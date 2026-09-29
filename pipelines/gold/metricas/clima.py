"""Clima x geração, precisão da previsão do tempo e carga x temperatura."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from metricas.energia import FONTES_RENOVAVEIS, mes
from metricas.padrao_gold import SUBSISTEMA_ANALISE, TabelaGold

KMH_POR_MS = 3.6
LARGURA_FAIXA = {"vento_100m_ms": 1.0, "radiacao_solar_wm2": 50.0}
VARIAVEL_PRINCIPAL = {"Eólica": "vento_100m_ms", "Solar": "radiacao_solar_wm2"}


# --- 2. O clima explica a geração ----------------------------------------------------------

COLUNAS_CLIMA_GERACAO = [
    ("data_hora", "TIMESTAMP_NTZ", "Hora local (sem fuso)"),
    ("uf", "STRING", "Sigla do estado"),
    ("tipo_usina", "STRING", "Eólica ou Solar"),
    ("ponto_id", "STRING", "Ponto de clima: centro das usinas da fonte no estado"),
    ("geracao_verificada_mwmed", "DOUBLE", "Geração das usinas da fonte no estado, em MW médios"),
    ("capacidade_mw", "DOUBLE", "Capacidade instalada das usinas da fonte no estado, em MW"),
    ("fator_capacidade", "DOUBLE", "Geração dividida pela capacidade (0 a 1)"),
    ("vento_100m_ms", "DOUBLE", "Velocidade do vento a 100 m no ponto, em m/s"),
    ("radiacao_solar_wm2", "DOUBLE", "Radiação solar no ponto, em W/m²"),
    ("cobertura_nuvens_pct", "DOUBLE", "Cobertura de nuvens no ponto, em %"),
    ("temperatura_c", "DOUBLE", "Temperatura do ar no ponto, em °C"),
    ("precipitacao_mm", "DOUBLE", "Precipitação na hora no ponto, em mm"),
]


def ponto_das_usinas(uf, tipo_usina):
    """Ponto de clima das usinas de uma fonte num estado (ex.: BA + Eólica -> ba-eolica)."""
    return F.concat(F.lower(uf), F.lit("-"), F.when(tipo_usina == "Eólica", "eolica").otherwise("solar"))


def clima_x_geracao_horaria(fator: DataFrame, clima_observado: DataFrame) -> DataFrame:
    geracao = (
        fator.filter((F.col("id_subsistema") == SUBSISTEMA_ANALISE) & F.col("tipo_usina").isin(*FONTES_RENOVAVEIS))
        .groupBy("data_hora", "uf", "tipo_usina")
        .agg(
            F.sum("geracao_verificada_mwmed").alias("geracao_verificada_mwmed"),
            F.sum("capacidade_instalada_mw").alias("capacidade_mw"),
        )
        .withColumn("fator_capacidade", F.when(F.col("capacidade_mw") > 0, F.col("geracao_verificada_mwmed") / F.col("capacidade_mw")))
        .withColumn("ponto_id", ponto_das_usinas(F.col("uf"), F.col("tipo_usina")))
    )
    clima = clima_observado.select(
        "ponto_id",
        "data_hora",
        (F.col("vento_velocidade_100m_kmh") / KMH_POR_MS).alias("vento_100m_ms"),
        "radiacao_solar_wm2",
        "cobertura_nuvens_pct",
        "temperatura_c",
        "precipitacao_mm",
    )
    return geracao.join(clima, ["ponto_id", "data_hora"]).select(*[c for c, _, _ in COLUNAS_CLIMA_GERACAO])


COLUNAS_CURVA = [
    ("tipo_usina", "STRING", "Eólica ou Solar"),
    ("variavel", "STRING", "vento_100m_ms (eólica) ou radiacao_solar_wm2 (solar)"),
    ("faixa_inicio", "DOUBLE", "Início da faixa da variável climática"),
    ("faixa_fim", "DOUBLE", "Fim da faixa da variável climática"),
    ("horas", "BIGINT", "Horas-estado observadas na faixa"),
    ("fator_capacidade", "DOUBLE", "Fator de capacidade na faixa (geração total / capacidade total)"),
    ("fator_capacidade_p25", "DOUBLE", "Percentil 25 do fator de capacidade horário na faixa"),
    ("fator_capacidade_p75", "DOUBLE", "Percentil 75 do fator de capacidade horário na faixa"),
]


def curva_de_potencia(clima_geracao: DataFrame) -> DataFrame:
    """Fator de capacidade por faixa de vento (eólica) ou de radiação (solar)."""
    variavel = F.when(F.col("tipo_usina") == "Eólica", "vento_100m_ms").otherwise("radiacao_solar_wm2")
    valor = F.when(F.col("tipo_usina") == "Eólica", F.col("vento_100m_ms")).otherwise(F.col("radiacao_solar_wm2"))
    largura = F.when(F.col("tipo_usina") == "Eólica", F.lit(LARGURA_FAIXA["vento_100m_ms"])).otherwise(F.lit(LARGURA_FAIXA["radiacao_solar_wm2"]))
    return (
        clima_geracao.filter(valor.isNotNull() & F.col("fator_capacidade").isNotNull())
        .withColumn("variavel", variavel)
        .withColumn("faixa_inicio", F.floor(valor / largura) * largura)
        .withColumn("faixa_fim", F.col("faixa_inicio") + largura)
        .groupBy("tipo_usina", "variavel", "faixa_inicio", "faixa_fim")
        .agg(
            F.count("*").alias("horas"),
            (F.sum("geracao_verificada_mwmed") / F.sum("capacidade_mw")).alias("fator_capacidade"),
            F.percentile_approx("fator_capacidade", 0.25).alias("fator_capacidade_p25"),
            F.percentile_approx("fator_capacidade", 0.75).alias("fator_capacidade_p75"),
        )
        .select(*[c for c, _, _ in COLUNAS_CURVA])
    )


COLUNAS_CORRELACAO = [
    ("uf", "STRING", "Sigla do estado"),
    ("tipo_usina", "STRING", "Eólica ou Solar"),
    ("variavel", "STRING", "Variável climática correlacionada com o fator de capacidade"),
    ("correlacao", "DOUBLE", "Correlação de Pearson entre a variável e o fator de capacidade horário (-1 a 1)"),
    ("horas", "BIGINT", "Horas usadas no cálculo (na solar, só horas com sol)"),
]


def correlacao(x, y):
    """Correlação de Pearson; nula (em vez de erro) quando uma das variáveis não varia."""
    return F.try_divide(F.covar_samp(x, y), F.stddev_samp(x) * F.stddev_samp(y))


def correlacao_clima_geracao(clima_geracao: DataFrame) -> DataFrame:
    """Correlação entre clima e geração por estado e fonte.

    Solar: só horas com radiação, senão a alternância dia/noite domina a correlação.
    """
    pares = [("Eólica", "vento_100m_ms"), ("Eólica", "temperatura_c"),
             ("Solar", "radiacao_solar_wm2"), ("Solar", "cobertura_nuvens_pct"), ("Solar", "temperatura_c")]
    partes = []
    for tipo, variavel in pares:
        base = clima_geracao.filter(F.col("tipo_usina") == tipo)
        if tipo == "Solar":
            base = base.filter(F.col("radiacao_solar_wm2") > 0)
        partes.append(
            base.groupBy("uf", "tipo_usina")
            .agg(correlacao(variavel, "fator_capacidade").alias("correlacao"), F.count("*").alias("horas"))
            .withColumn("variavel", F.lit(variavel))
        )
    resultado = partes[0]
    for parte in partes[1:]:
        resultado = resultado.unionByName(parte)
    return resultado.select(*[c for c, _, _ in COLUNAS_CORRELACAO])


# --- 4. A previsão do tempo acerta ---------------------------------------------------------

COLUNAS_PRECISAO_TEMPO = [
    ("mes", "DATE", "Primeiro dia do mês"),
    ("ponto_id", "STRING", "Ponto de clima"),
    ("uf", "STRING", "Sigla do estado do ponto"),
    ("tipo_ponto", "STRING", "eolica, solar ou capital"),
    ("horas", "BIGINT", "Horas com previsão D+1 e observação"),
    ("vento_100m_observado_kmh", "DOUBLE", "Vento a 100 m médio observado, em km/h"),
    ("erro_medio_absoluto_vento_100m_kmh", "DOUBLE", "Erro médio absoluto da previsão D+1 do vento a 100 m, em km/h"),
    ("vies_vento_100m_kmh", "DOUBLE", "Previsto menos observado, em média; positivo = previsão superestima o vento"),
    ("radiacao_observada_wm2", "DOUBLE", "Radiação solar média observada nas horas com sol, em W/m²"),
    ("erro_medio_absoluto_radiacao_wm2", "DOUBLE", "Erro médio absoluto da previsão D+1 da radiação nas horas com sol, em W/m²"),
    ("vies_radiacao_wm2", "DOUBLE", "Previsto menos observado nas horas com sol, em média"),
    ("erro_medio_absoluto_temperatura_c", "DOUBLE", "Erro médio absoluto da previsão D+1 da temperatura, em °C"),
]


def precisao_previsao_tempo(clima_previsao: DataFrame, clima_observado: DataFrame) -> DataFrame:
    previsto = clima_previsao.filter(F.col("horizonte_dias") == 1).select(
        "ponto_id", "uf", "tipo_ponto", "data_hora",
        F.col("vento_velocidade_100m_kmh").alias("vento_previsto"),
        F.col("radiacao_solar_wm2").alias("radiacao_prevista"),
        F.col("temperatura_c").alias("temperatura_prevista"),
    )
    observado = clima_observado.select(
        "ponto_id", "data_hora",
        F.col("vento_velocidade_100m_kmh").alias("vento_observado"),
        F.col("radiacao_solar_wm2").alias("radiacao_observada"),
        F.col("temperatura_c").alias("temperatura_observada"),
    )
    com_sol = F.col("radiacao_observada") > 0
    erro_radiacao = F.when(com_sol, F.col("radiacao_prevista") - F.col("radiacao_observada"))
    return (
        previsto.join(observado, ["ponto_id", "data_hora"])
        .groupBy(mes("data_hora").alias("mes"), "ponto_id", "uf", "tipo_ponto")
        .agg(
            F.count("*").alias("horas"),
            F.avg("vento_observado").alias("vento_100m_observado_kmh"),
            F.avg(F.abs(F.col("vento_previsto") - F.col("vento_observado"))).alias("erro_medio_absoluto_vento_100m_kmh"),
            F.avg(F.col("vento_previsto") - F.col("vento_observado")).alias("vies_vento_100m_kmh"),
            F.avg(F.when(com_sol, F.col("radiacao_observada"))).alias("radiacao_observada_wm2"),
            F.avg(F.abs(erro_radiacao)).alias("erro_medio_absoluto_radiacao_wm2"),
            F.avg(erro_radiacao).alias("vies_radiacao_wm2"),
            F.avg(F.abs(F.col("temperatura_prevista") - F.col("temperatura_observada"))).alias("erro_medio_absoluto_temperatura_c"),
        )
    )


# --- Bônus: a carga sobe com o calor ------------------------------------------------------

COLUNAS_CARGA_TEMPERATURA = [
    ("data", "DATE", "Dia"),
    ("carga_mwmed", "DOUBLE", "Carga média do dia no subsistema Nordeste, em MW médios"),
    ("temperatura_media_c", "DOUBLE", "Temperatura média do dia nas capitais (Fortaleza, Recife, Salvador), em °C"),
    ("temperatura_maxima_c", "DOUBLE", "Maior temperatura horária do dia entre as capitais, em °C"),
    ("dia_semana", "INT", "Dia da semana ISO: 1 = segunda-feira, 7 = domingo"),
    ("fim_de_semana", "BOOLEAN", "Sábado ou domingo (feriados não são identificados)"),
]


def carga_x_temperatura(carga: DataFrame, clima_observado: DataFrame) -> DataFrame:
    temperatura = (
        clima_observado.filter(F.col("tipo_ponto") == "capital")
        .groupBy(F.to_date("data_hora").alias("data"))
        .agg(F.avg("temperatura_c").alias("temperatura_media_c"), F.max("temperatura_c").alias("temperatura_maxima_c"))
    )
    dia_semana = ((F.dayofweek("data") + 5) % 7 + 1).cast("int")  # dayofweek: 1 = domingo
    return (
        carga.filter(F.col("id_subsistema") == SUBSISTEMA_ANALISE)
        .select("data", "carga_mwmed")
        .join(temperatura, "data")
        .withColumn("dia_semana", dia_semana)
        .withColumn("fim_de_semana", F.col("dia_semana") >= 6)
    )


TABELAS = (
    TabelaGold(
        nome="clima_x_geracao_horaria",
        pergunta="O clima no local das usinas explica a geração eólica e solar?",
        descricao="Fator de capacidade horário por estado e fonte no Nordeste, com o clima observado no ponto das usinas.",
        tabelas_silver=("fator_capacidade", "clima_observado"),
        calcular=clima_x_geracao_horaria,
        colunas=COLUNAS_CLIMA_GERACAO,
    ),
    TabelaGold(
        nome="curva_de_potencia",
        pergunta="Como o fator de capacidade responde ao vento (eólica) e à radiação (solar)?",
        descricao="Fator de capacidade por faixa de vento a 100 m ou de radiação solar, no Nordeste.",
        tabelas_silver=(),
        tabelas_gold=("clima_x_geracao_horaria",),
        calcular=curva_de_potencia,
        colunas=COLUNAS_CURVA,
    ),
    TabelaGold(
        nome="correlacao_clima_geracao",
        pergunta="Quanto cada variável climática se relaciona com a geração, estado a estado?",
        descricao="Correlação entre variáveis climáticas e o fator de capacidade horário, por estado e fonte.",
        tabelas_silver=(),
        tabelas_gold=("clima_x_geracao_horaria",),
        calcular=correlacao_clima_geracao,
        colunas=COLUNAS_CORRELACAO,
    ),
    TabelaGold(
        nome="precisao_previsao_tempo",
        pergunta="Quanto a previsão do tempo feita na véspera acerta o vento, a radiação e a temperatura?",
        descricao="Erro da previsão D+1 do Open-Meteo em relação ao observado, por mês e ponto.",
        tabelas_silver=("clima_previsao", "clima_observado"),
        calcular=precisao_previsao_tempo,
        colunas=COLUNAS_PRECISAO_TEMPO,
    ),
    TabelaGold(
        nome="carga_x_temperatura",
        pergunta="O consumo de energia do Nordeste sobe nos dias mais quentes?",
        descricao="Carga diária do Nordeste com a temperatura das capitais e o dia da semana.",
        tabelas_silver=("carga_energia", "clima_observado"),
        calcular=carga_x_temperatura,
        colunas=COLUNAS_CARGA_TEMPERATURA,
    ),
)
