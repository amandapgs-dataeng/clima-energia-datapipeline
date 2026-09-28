# Silver da carga de energia diária por subsistema (ONS).
#
# bronze.ons.carga_energia (append, com releituras)
#   -> carga_energia_padronizada  (view: tipagem, vocabulário, expectations)
#   -> carga_energia              (SCD tipo 1: versão atual de cada subsistema x dia)
#   -> carga_energia_historico    (SCD tipo 2: uma versão por valor diferente publicado pelo ONS)
from pyspark import pipelines as dp

from utilities.carga_energia import (
    CHAVE,
    COLUNAS_VERSIONADAS,
    REGRAS_ALERTA,
    REGRAS_DESCARTE,
    padronizar_carga_energia,
)

CATALOG_BRONZE = spark.conf.get("clima_energia.catalog_bronze")  # noqa: F821 (spark é global no pipeline)
PROPRIEDADES = {"camada": "silver", "fonte": "ons", "dataset": "carga_energia"}

COLUNAS = """
    id_subsistema STRING COMMENT 'Código do subsistema: N, NE, S, SE',
    subsistema STRING COMMENT 'Nome padronizado do subsistema',
    data DATE COMMENT 'Dia da medição (horário local)',
    carga_mwmed DOUBLE COMMENT 'Carga média do dia, em MW médios',
    ingestion_timestamp TIMESTAMP COMMENT 'Momento em que a versão foi ingerida na bronze (UTC)'
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
    sequence_by="ingestion_timestamp",
    stored_as_scd_type=1,
)

dp.create_streaming_table(
    name="carga_energia_historico",
    comment="Carga de energia diária por subsistema: histórico de versões (revisões do ONS)",
    schema=COLUNAS + """,
    __START_AT TIMESTAMP COMMENT 'Ingestão a partir da qual esta versão vale',
    __END_AT TIMESTAMP COMMENT 'Ingestão em que esta versão foi substituída (nulo = versão atual)'
""",
    table_properties=PROPRIEDADES,
)
dp.create_auto_cdc_flow(
    target="carga_energia_historico",
    source="carga_energia_padronizada",
    keys=CHAVE,
    sequence_by="ingestion_timestamp",
    stored_as_scd_type=2,
    track_history_column_list=COLUNAS_VERSIONADAS,
)
