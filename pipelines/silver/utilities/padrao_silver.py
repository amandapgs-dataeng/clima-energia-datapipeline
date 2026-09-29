"""Padrão das tabelas da silver.

Cada fonte descreve a sua tabela numa `TabelaSilver` (origem na bronze, tratamento, colunas,
chave, regras de qualidade). A partir dela, `declarar_silver` cria no pipeline:

    x_padronizada      view: o tratamento aplicado a tudo o que chega da bronze
    x_validada         view: x_padronizada com as regras de qualidade (descarta e alerta)
    x                  versão atual de cada chave (AUTO CDC, SCD tipo 1)
    x_quarentena       registros descartados, com as regras que violaram
    x_versoes          histórico com nomes de negócio (se a fonte revisa dados)
    x_historico_cdc    histórico técnico (AUTO CDC, SCD tipo 2; uso interno)
"""
from dataclasses import dataclass, field
from typing import Callable

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# Coluna que ordena as versões: o valor ingerido por último vence.
SEQUENCIA = "ingerido_em"
COLUNA_SEQUENCIA = (SEQUENCIA, "TIMESTAMP", "Quando o valor foi ingerido pela última vez (UTC)")

COLUNAS_CDC = [
    ("__START_AT", "TIMESTAMP", "Início da vigência da versão (coluna técnica do CDC)"),
    ("__END_AT", "TIMESTAMP", "Fim da vigência da versão; nulo = versão atual (coluna técnica do CDC)"),
]

COLUNAS_VERSAO = [
    ("numero_versao", "INT", "Ordem da versão para a chave (1 = primeira publicação)"),
    ("vigente_desde", "TIMESTAMP", "Quando esta versão passou a valer (ingestão em que o valor apareceu)"),
    ("vigente_ate", "TIMESTAMP", "Quando esta versão foi substituída; nulo se ainda é a atual"),
    ("versao_atual", "BOOLEAN", "Verdadeiro se esta é a versão vigente"),
    ("confirmado_em", "TIMESTAMP", "Última ingestão que trouxe este mesmo valor (UTC)"),
]

COLUNA_REGRAS_VIOLADAS = ("regras_violadas", "ARRAY<STRING>", "Regras de descarte que o registro violou")


@dataclass(frozen=True)
class TabelaSilver:
    nome: str
    fonte: str
    descricao: str
    tabelas_bronze: tuple  # relativas ao catalog da bronze, ex.: ("ons.carga_energia",)
    padronizar: Callable  # recebe um DataFrame por tabela da bronze, na mesma ordem
    colunas: list  # (nome, tipo, comentário), na ordem produzida por `padronizar`, sem a sequência
    chave: list
    regras_descarte: dict
    regras_alerta: dict = field(default_factory=dict)
    colunas_versionadas: list = field(default_factory=list)  # vazio = sem histórico de versões


def ddl(colunas):
    """Schema em DDL a partir de (nome, tipo, comentário)."""
    return ",\n".join(f"{nome} {tipo} COMMENT '{comentario}'" for nome, tipo, comentario in colunas)


def nomes(colunas):
    return [nome for nome, _, _ in colunas]


def propriedades_da_tabela(colunas, propriedades):
    """Propriedades da tabela, com os recursos do Delta que o schema exige.

    Colunas TIMESTAMP_NTZ exigem o recurso timestampNtz habilitado explicitamente.
    """
    if any(tipo.upper() == "TIMESTAMP_NTZ" for _, tipo, _ in colunas):
        return {**propriedades, "delta.feature.timestampNtz": "supported"}
    return dict(propriedades)


def regras_nulo_reprova(regras):
    """Regras em que um resultado nulo conta como violação (o padrão do SQL seria aprovar)."""
    return {nome: f"coalesce(({expressao}), false)" for nome, expressao in regras.items()}


def marcar_regras_violadas(df: DataFrame, regras) -> DataFrame:
    """Acrescenta `regras_violadas`: o nome de cada regra que o registro não cumpre."""
    violadas = F.array(*[F.when(~F.expr(expressao), F.lit(nome)) for nome, expressao in regras_nulo_reprova(regras).items()])
    return df.withColumn(COLUNA_REGRAS_VIOLADAS[0], F.filter(violadas, lambda regra: regra.isNotNull()))


def versoes_legiveis(historico_cdc: DataFrame, chave) -> DataFrame:
    """Histórico com nomes de negócio no lugar das colunas técnicas do CDC (__START_AT/__END_AT)."""
    tecnicas = {"__START_AT", "__END_AT", SEQUENCIA}
    dados = [coluna for coluna in historico_cdc.columns if coluna not in tecnicas]
    ordem = Window.partitionBy(*chave).orderBy("__START_AT")
    return historico_cdc.select(
        *dados,
        F.row_number().over(ordem).alias("numero_versao"),
        F.col("__START_AT").alias("vigente_desde"),
        F.col("__END_AT").alias("vigente_ate"),
        F.col("__END_AT").isNull().alias("versao_atual"),
        F.col(SEQUENCIA).alias("confirmado_em"),
    )


def declarar_silver(dp, spark, tabela: TabelaSilver, catalog_bronze):
    """Declara no pipeline todos os objetos de uma tabela da silver."""
    nome = tabela.nome
    propriedades = propriedades_da_tabela(
        tabela.colunas, {"camada": "silver", "fonte": tabela.fonte, "dataset": nome}
    )
    colunas_com_sequencia = tabela.colunas + [COLUNA_SEQUENCIA]

    @dp.temporary_view(name=f"{nome}_padronizada")
    def padronizada():
        entradas = [spark.readStream.table(f"{catalog_bronze}.{origem}") for origem in tabela.tabelas_bronze]
        return tabela.padronizar(*entradas)

    @dp.temporary_view(name=f"{nome}_validada")
    @dp.expect_all_or_drop(regras_nulo_reprova(tabela.regras_descarte))
    @dp.expect_all(tabela.regras_alerta)
    def validada():
        return spark.readStream.table(f"{nome}_padronizada")

    @dp.table(
        name=f"{nome}_quarentena",
        comment=f"{tabela.descricao}: registros descartados pelas regras de qualidade (continuam na bronze)",
        schema=ddl(colunas_com_sequencia + [COLUNA_REGRAS_VIOLADAS]),
        table_properties={**propriedades, "uso": "qualidade"},
    )
    def quarentena():
        entrada = marcar_regras_violadas(spark.readStream.table(f"{nome}_padronizada"), tabela.regras_descarte)
        return entrada.filter(F.size(COLUNA_REGRAS_VIOLADAS[0]) > 0)

    dp.create_streaming_table(
        name=nome,
        comment=f"{tabela.descricao}: versão atual",
        schema=ddl(colunas_com_sequencia),
        table_properties=propriedades,
    )
    dp.create_auto_cdc_flow(
        target=nome, source=f"{nome}_validada", keys=tabela.chave, sequence_by=SEQUENCIA, stored_as_scd_type=1
    )

    if not tabela.colunas_versionadas:
        return

    tabela_cdc = f"{nome}_historico_cdc"
    dp.create_streaming_table(
        name=tabela_cdc,
        comment=f"Tabela técnica do CDC (SCD tipo 2). Para consultar o histórico, use {nome}_versoes.",
        schema=ddl(colunas_com_sequencia + COLUNAS_CDC),
        table_properties={**propriedades, "uso": "interno"},
    )
    # Só mudanças nestas colunas criam versão: releituras idênticas (retries, janelas
    # que se sobrepõem) não geram versões; revisões da fonte geram.
    dp.create_auto_cdc_flow(
        target=tabela_cdc,
        source=f"{nome}_validada",
        keys=tabela.chave,
        sequence_by=SEQUENCIA,
        stored_as_scd_type=2,
        track_history_column_list=tabela.colunas_versionadas,
    )

    @dp.materialized_view(
        name=f"{nome}_versoes",
        comment=f"{tabela.descricao}: histórico de versões (cada revisão da fonte é uma versão)",
        schema=ddl(tabela.colunas + COLUNAS_VERSAO),
        table_properties=propriedades,
    )
    def versoes():
        return versoes_legiveis(spark.read.table(tabela_cdc), tabela.chave)
