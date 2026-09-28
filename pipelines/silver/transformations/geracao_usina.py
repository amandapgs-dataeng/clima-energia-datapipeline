# Silver da geração horária por usina (ONS).
#
# ons.geracao_usina
#   -> geracao_usina_padronizada  (view: tipagem, vocabulário comum, regras de qualidade)
#   -> geracao_usina  (versão atual)
#   -> geracao_usina_versoes  (histórico de revisões da fonte, para consulta)
#   -> geracao_usina_historico_cdc  (histórico técnico, uso interno)
from pyspark import pipelines as dp

from utilities.historico import declarar_tabelas
from utilities.geracao_usina import (
    CHAVE,
    COLUNAS,
    COLUNAS_VERSIONADAS,
    REGRAS_ALERTA,
    REGRAS_DESCARTE,
    padronizar_geracao_usina,
)

CATALOG_BRONZE = spark.conf.get("clima_energia.catalog_bronze")  # noqa: F821 (spark é global no pipeline)


@dp.temporary_view(name="geracao_usina_padronizada")
@dp.expect_all_or_drop(REGRAS_DESCARTE)
@dp.expect_all(REGRAS_ALERTA)
def geracao_usina_padronizada():
    return padronizar_geracao_usina(spark.readStream.table(f"{CATALOG_BRONZE}.ons.geracao_usina"))  # noqa: F821


declarar_tabelas(
    dp,
    spark,  # noqa: F821
    nome="geracao_usina",
    origem="geracao_usina_padronizada",
    chave=CHAVE,
    colunas=COLUNAS,
    descricao="Geração horária por usina, todas as fontes e subsistemas",
    propriedades={"camada": "silver", "fonte": "ons", "dataset": "geracao_usina"},
    colunas_versionadas=COLUNAS_VERSIONADAS,
)
