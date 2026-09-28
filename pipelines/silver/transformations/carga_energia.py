# Silver da carga de energia diária por subsistema (ONS).
#
# ons.carga_energia
#   -> carga_energia_padronizada  (view: tipagem, vocabulário comum, regras de qualidade)
#   -> carga_energia  (versão atual)
#   -> carga_energia_versoes  (histórico de revisões da fonte, para consulta)
#   -> carga_energia_historico_cdc  (histórico técnico, uso interno)
from pyspark import pipelines as dp

from utilities.historico import declarar_tabelas
from utilities.carga_energia import (
    CHAVE,
    COLUNAS,
    COLUNAS_VERSIONADAS,
    REGRAS_ALERTA,
    REGRAS_DESCARTE,
    padronizar_carga_energia,
)

CATALOG_BRONZE = spark.conf.get("clima_energia.catalog_bronze")  # noqa: F821 (spark é global no pipeline)


@dp.temporary_view(name="carga_energia_padronizada")
@dp.expect_all_or_drop(REGRAS_DESCARTE)
@dp.expect_all(REGRAS_ALERTA)
def carga_energia_padronizada():
    return padronizar_carga_energia(spark.readStream.table(f"{CATALOG_BRONZE}.ons.carga_energia"))  # noqa: F821


declarar_tabelas(
    dp,
    spark,  # noqa: F821
    nome="carga_energia",
    origem="carga_energia_padronizada",
    chave=CHAVE,
    colunas=COLUNAS,
    descricao="Carga de energia diária por subsistema",
    propriedades={"camada": "silver", "fonte": "ons", "dataset": "carga_energia"},
    colunas_versionadas=COLUNAS_VERSIONADAS,
)
