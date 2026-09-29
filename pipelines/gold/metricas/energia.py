"""Aproveitamento, restrição e precisão da programação da geração eólica e solar."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from metricas.padrao_gold import SUBSISTEMA_ANALISE, TabelaGold

FONTES_RENOVAVEIS = ("Eólica", "Solar")
HORAS_POR_PATAMAR = 0.5  # a programação do ONS é por meia hora


def mes(coluna):
    return F.to_date(F.date_trunc("month", F.col(coluna)))


def _razao(numerador, denominador):
    return F.when(F.col(denominador) > 0, F.col(numerador) / F.col(denominador))


# --- 1. Quanto do potencial vira energia ----------------------------------------------------

COLUNAS_APROVEITAMENTO = [
    ("mes", "DATE", "Primeiro dia do mês"),
    ("uf", "STRING", "Sigla do estado"),
    ("tipo_usina", "STRING", "Eólica ou Solar"),
    ("usinas", "BIGINT", "Usinas e conjuntos com medição no mês"),
    ("geracao_verificada_mwh", "DOUBLE", "Energia gerada no mês, em MWh"),
    ("geracao_programada_mwh", "DOUBLE", "Energia programada pelo ONS no mês, em MWh"),
    ("capacidade_mwh", "DOUBLE", "Energia que a capacidade instalada geraria operando o mês inteiro a plena carga, em MWh"),
    ("fator_capacidade", "DOUBLE", "Geração verificada dividida pela capacidade (0 a 1)"),
    ("fator_capacidade_programado", "DOUBLE", "Geração programada dividida pela capacidade (0 a 1)"),
]


def aproveitamento_mensal(fator: DataFrame) -> DataFrame:
    # Valores horários em MW médios somados ao longo das horas = energia em MWh.
    return (
        fator.filter((F.col("id_subsistema") == SUBSISTEMA_ANALISE) & F.col("tipo_usina").isin(*FONTES_RENOVAVEIS))
        .groupBy(mes("data_hora").alias("mes"), "uf", "tipo_usina")
        .agg(
            F.countDistinct("id_ons").alias("usinas"),
            F.sum("geracao_verificada_mwmed").alias("geracao_verificada_mwh"),
            F.sum("geracao_programada_mwmed").alias("geracao_programada_mwh"),
            F.sum("capacidade_instalada_mw").alias("capacidade_mwh"),
        )
        .withColumn("fator_capacidade", _razao("geracao_verificada_mwh", "capacidade_mwh"))
        .withColumn("fator_capacidade_programado", _razao("geracao_programada_mwh", "capacidade_mwh"))
    )


# --- 3a. Quanto deixamos de gerar ----------------------------------------------------------

COLUNAS_RESTRICAO_DIARIA = [
    ("data", "DATE", "Dia da programação"),
    ("usinas", "BIGINT", "Usinas eólicas e solares programadas no dia"),
    ("previsto_mwh", "DOUBLE", "Energia que o ONS previu que as usinas poderiam gerar, em MWh"),
    ("programado_mwh", "DOUBLE", "Energia que o ONS programou para as usinas gerarem, em MWh"),
    ("restricao_mwh", "DOUBLE", "Energia prevista e não programada (soma das meias horas com programado abaixo do previsto), em MWh"),
    ("restricao_pct", "DOUBLE", "Restrição em relação ao previsto (0 a 1)"),
    ("meias_horas_com_restricao_pct", "DOUBLE", "Fração das meias horas por usina com programado abaixo do previsto (0 a 1)"),
]


def _com_restricao(programacao: DataFrame) -> DataFrame:
    diferenca = F.col("previsao_mwmed") - F.col("programado_mwmed")
    return programacao.withColumn("restricao_mwmed", F.greatest(diferenca, F.lit(0.0))).withColumn(
        "houve_restricao", (diferenca > 0.5).cast("int")  # tolerância de 0,5 MW para arredondamentos
    )


def restricao_diaria(programacao: DataFrame) -> DataFrame:
    return (
        _com_restricao(programacao)
        .groupBy("data")
        .agg(
            F.countDistinct("codigo_usina").alias("usinas"),
            (F.sum("previsao_mwmed") * HORAS_POR_PATAMAR).alias("previsto_mwh"),
            (F.sum("programado_mwmed") * HORAS_POR_PATAMAR).alias("programado_mwh"),
            (F.sum("restricao_mwmed") * HORAS_POR_PATAMAR).alias("restricao_mwh"),
            F.avg("houve_restricao").alias("meias_horas_com_restricao_pct"),
        )
        .withColumn("restricao_pct", _razao("restricao_mwh", "previsto_mwh"))
        .select(*[c for c, _, _ in COLUNAS_RESTRICAO_DIARIA])
    )


COLUNAS_RESTRICAO_USINA = [
    ("mes", "DATE", "Primeiro dia do mês"),
    ("codigo_usina", "STRING", "Código da usina na programação do ONS"),
    ("nome_usina", "STRING", "Nome da usina na programação do ONS"),
    ("previsto_mwh", "DOUBLE", "Energia prevista no mês, em MWh"),
    ("restricao_mwh", "DOUBLE", "Energia prevista e não programada no mês, em MWh"),
    ("restricao_pct", "DOUBLE", "Restrição em relação ao previsto (0 a 1)"),
]


def restricao_por_usina_mensal(programacao: DataFrame) -> DataFrame:
    return (
        _com_restricao(programacao)
        .groupBy(mes("data").alias("mes"), "codigo_usina")
        .agg(
            F.max("nome_usina").alias("nome_usina"),
            (F.sum("previsao_mwmed") * HORAS_POR_PATAMAR).alias("previsto_mwh"),
            (F.sum("restricao_mwmed") * HORAS_POR_PATAMAR).alias("restricao_mwh"),
        )
        .withColumn("restricao_pct", _razao("restricao_mwh", "previsto_mwh"))
    )


# --- 3b. O ONS acerta a programação --------------------------------------------------------

COLUNAS_PRECISAO_PROGRAMACAO = [
    ("mes", "DATE", "Primeiro dia do mês"),
    ("uf", "STRING", "Sigla do estado"),
    ("tipo_usina", "STRING", "Eólica ou Solar"),
    ("geracao_programada_mwh", "DOUBLE", "Energia programada pelo ONS, em MWh"),
    ("geracao_verificada_mwh", "DOUBLE", "Energia efetivamente gerada, em MWh"),
    ("erro_absoluto_mwh", "DOUBLE", "Soma, hora a hora e usina a usina, da diferença absoluta entre programado e verificado, em MWh"),
    ("erro_relativo", "DOUBLE", "Erro absoluto dividido pela geração verificada (WAPE)"),
    ("vies_relativo", "DOUBLE", "(verificado - programado) / programado; negativo = gerou menos que o programado"),
]


def precisao_programacao_mensal(fator: DataFrame) -> DataFrame:
    return (
        fator.filter((F.col("id_subsistema") == SUBSISTEMA_ANALISE) & F.col("tipo_usina").isin(*FONTES_RENOVAVEIS))
        .groupBy(mes("data_hora").alias("mes"), "uf", "tipo_usina")
        .agg(
            F.sum("geracao_programada_mwmed").alias("geracao_programada_mwh"),
            F.sum("geracao_verificada_mwmed").alias("geracao_verificada_mwh"),
            F.sum(F.abs(F.col("geracao_programada_mwmed") - F.col("geracao_verificada_mwmed"))).alias("erro_absoluto_mwh"),
        )
        .withColumn("erro_relativo", _razao("erro_absoluto_mwh", "geracao_verificada_mwh"))
        .withColumn(
            "vies_relativo",
            F.when(
                F.col("geracao_programada_mwh") > 0,
                (F.col("geracao_verificada_mwh") - F.col("geracao_programada_mwh")) / F.col("geracao_programada_mwh"),
            ),
        )
    )


TABELAS = (
    TabelaGold(
        nome="aproveitamento_mensal",
        pergunta="Quanto da capacidade eólica e solar instalada no Nordeste vira energia, ao longo do ano?",
        descricao="Fator de capacidade mensal das usinas eólicas e solares do Nordeste, por estado.",
        tabelas_silver=("fator_capacidade",),
        calcular=aproveitamento_mensal,
        colunas=COLUNAS_APROVEITAMENTO,
    ),
    TabelaGold(
        nome="restricao_geracao_diaria",
        pergunta="Quanto da geração eólica e solar prevista o ONS deixou de programar?",
        descricao="Geração prevista x programada pelo ONS por dia (Brasil; 91% da capacidade eólica fica no Nordeste).",
        tabelas_silver=("previsao_programado",),
        calcular=restricao_diaria,
        colunas=COLUNAS_RESTRICAO_DIARIA,
    ),
    TabelaGold(
        nome="restricao_geracao_por_usina",
        pergunta="Quais usinas tiveram mais geração prevista e não programada?",
        descricao="Geração prevista x programada pelo ONS por usina e mês.",
        tabelas_silver=("previsao_programado",),
        calcular=restricao_por_usina_mensal,
        colunas=COLUNAS_RESTRICAO_USINA,
    ),
    TabelaGold(
        nome="precisao_programacao_mensal",
        pergunta="Quanto a geração real das usinas eólicas e solares do Nordeste se afasta do que o ONS programou?",
        descricao="Erro e viés da programação do ONS em relação à geração verificada, por mês, estado e fonte.",
        tabelas_silver=("fator_capacidade",),
        calcular=precisao_programacao_mensal,
        colunas=COLUNAS_PRECISAO_PROGRAMACAO,
    ),
)
