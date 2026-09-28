# Silver da previsão x programação de usinas eólicas e solares (ONS).
#
# ons.previsao_programado_eolsol
#   -> previsao_programado_padronizada  (view: tipagem, vocabulário comum, regras de qualidade)
#   -> previsao_programado  (versão atual)
#   -> previsao_programado_versoes  (histórico de revisões da fonte, para consulta)
#   -> previsao_programado_historico_cdc  (histórico técnico, uso interno)
from pyspark import pipelines as dp

from utilities.historico import declarar_tabelas
from utilities.previsao_programado import (
    CHAVE,
    COLUNAS,
    COLUNAS_VERSIONADAS,
    REGRAS_ALERTA,
    REGRAS_DESCARTE,
    padronizar_previsao_programado,
)

CATALOG_BRONZE = spark.conf.get("clima_energia.catalog_bronze")  # noqa: F821 (spark é global no pipeline)


@dp.temporary_view(name="previsao_programado_padronizada")
@dp.expect_all_or_drop(REGRAS_DESCARTE)
@dp.expect_all(REGRAS_ALERTA)
def previsao_programado_padronizada():
    return padronizar_previsao_programado(spark.readStream.table(f"{CATALOG_BRONZE}.ons.previsao_programado_eolsol"))  # noqa: F821


declarar_tabelas(
    dp,
    spark,  # noqa: F821
    nome="previsao_programado",
    origem="previsao_programado_padronizada",
    chave=CHAVE,
    colunas=COLUNAS,
    descricao="Geração prevista e programada pelo ONS por usina eólica ou solar, a cada meia hora",
    propriedades={"camada": "silver", "fonte": "ons", "dataset": "previsao_programado"},
    colunas_versionadas=COLUNAS_VERSIONADAS,
)
