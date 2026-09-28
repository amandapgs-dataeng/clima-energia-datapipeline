# Silver da carga de energia diária por subsistema (ONS).
#
# bronze.ons.carga_energia (append, com releituras)
#   -> carga_energia_padronizada    (view: tipagem, vocabulário, expectations)
#   -> carga_energia                (SCD tipo 1: versão atual de cada subsistema x dia)
#   -> carga_energia_historico_cdc  (SCD tipo 2, tabela técnica do CDC)
#   -> carga_energia_versoes        (o histórico com nomes de negócio, para consulta)
from pyspark import pipelines as dp

from utilities.carga_energia import (
    CHAVE,
    COLUNAS_VERSIONADAS,
    REGRAS_ALERTA,
    REGRAS_DESCARTE,
    SEQUENCIA,
    padronizar_carga_energia,
    versoes_legiveis,
)

CATALOG_BRONZE = spark.conf.get("clima_energia.catalog_bronze")  # noqa: F821 (spark é global no pipeline)
PROPRIEDADES = {"camada": "silver", "fonte": "ons", "dataset": "carga_energia"}

COLUNAS = """
    id_subsistema STRING COMMENT 'Código do subsistema: N, NE, S, SE',
    subsistema STRING COMMENT 'Nome padronizado do subsistema',
    data DATE COMMENT 'Dia da medição (horário local)',
    carga_mwmed DOUBLE COMMENT 'Carga média do dia, em MW médios',
    ingerido_em TIMESTAMP COMMENT 'Quando o valor foi ingerido pela última vez (UTC)'
"""


@dp.temporary_view(name="carga_energia_padronizada")
@dp.expect_all_or_drop(REGRAS_DESCARTE)
@dp.expect_all(REGRAS_ALERTA)
def carga_energia_padronizada():
    return padronizar_carga_energia(spark.readStream.table(f"{CATALOG_BRONZE}.ons.carga_energia"))  # noqa: F821


dp.create_streaming_table(
    name="carga_energia",
    comment="Carga de energia diária por subsistema: versão mais recente publicada pelo ONS",
    schema=COLUNAS,
    table_properties=PROPRIEDADES,
)
dp.create_auto_cdc_flow(
    target="carga_energia",
    source="carga_energia_padronizada",
    keys=CHAVE,
    sequence_by=SEQUENCIA,
    stored_as_scd_type=1,
)

dp.create_streaming_table(
    name="carga_energia_historico_cdc",
    comment="Tabela técnica do CDC (SCD tipo 2). Para consultar o histórico, use carga_energia_versoes.",
    schema=COLUNAS + """,
    __START_AT TIMESTAMP COMMENT 'Início da vigência da versão (coluna técnica do CDC)',
    __END_AT TIMESTAMP COMMENT 'Fim da vigência da versão; nulo = versão atual (coluna técnica do CDC)'
""",
    table_properties={**PROPRIEDADES, "uso": "interno"},
)
dp.create_auto_cdc_flow(
    target="carga_energia_historico_cdc",
    source="carga_energia_padronizada",
    keys=CHAVE,
    sequence_by=SEQUENCIA,
    stored_as_scd_type=2,
    track_history_column_list=COLUNAS_VERSIONADAS,
)


@dp.materialized_view(
    name="carga_energia_versoes",
    comment="Histórico de versões da carga de energia: cada revisão publicada pelo ONS é uma versão",
    schema="""
        id_subsistema STRING COMMENT 'Código do subsistema: N, NE, S, SE',
        data DATE COMMENT 'Dia da medição (horário local)',
        subsistema STRING COMMENT 'Nome padronizado do subsistema',
        carga_mwmed DOUBLE COMMENT 'Carga média do dia nesta versão, em MW médios',
        numero_versao INT COMMENT 'Ordem da versão para o subsistema e dia (1 = primeira publicação)',
        vigente_desde TIMESTAMP COMMENT 'Quando esta versão passou a valer (ingestão em que o valor apareceu)',
        vigente_ate TIMESTAMP COMMENT 'Quando esta versão foi substituída; nulo se ainda é a atual',
        versao_atual BOOLEAN COMMENT 'Verdadeiro se esta é a versão vigente',
        confirmado_em TIMESTAMP COMMENT 'Última ingestão que trouxe este mesmo valor (UTC)'
    """,
    table_properties=PROPRIEDADES,
)
def carga_energia_versoes():
    return versoes_legiveis(spark.read.table("carga_energia_historico_cdc"))  # noqa: F821
