# Silver do fator de capacidade horário de usinas eólicas e solares (ONS).
#
# ons.fator_capacidade
#   -> fator_capacidade_padronizada  (view: tipagem, vocabulário comum, regras de qualidade)
#   -> fator_capacidade  (versão atual)
#   -> fator_capacidade_versoes  (histórico de revisões da fonte, para consulta)
#   -> fator_capacidade_historico_cdc  (histórico técnico, uso interno)
from pyspark import pipelines as dp

from utilities.historico import declarar_tabelas
from utilities.fator_capacidade import (
    CHAVE,
    COLUNAS,
    COLUNAS_VERSIONADAS,
    REGRAS_ALERTA,
    REGRAS_DESCARTE,
    padronizar_fator_capacidade,
)

CATALOG_BRONZE = spark.conf.get("clima_energia.catalog_bronze")  # noqa: F821 (spark é global no pipeline)


@dp.temporary_view(name="fator_capacidade_padronizada")
@dp.expect_all_or_drop(REGRAS_DESCARTE)
@dp.expect_all(REGRAS_ALERTA)
def fator_capacidade_padronizada():
    return padronizar_fator_capacidade(spark.readStream.table(f"{CATALOG_BRONZE}.ons.fator_capacidade"))  # noqa: F821


declarar_tabelas(
    dp,
    spark,  # noqa: F821
    nome="fator_capacidade",
    origem="fator_capacidade_padronizada",
    chave=CHAVE,
    colunas=COLUNAS,
    descricao="Fator de capacidade horário de usinas e conjuntos eólicos e solares",
    propriedades={"camada": "silver", "fonte": "ons", "dataset": "fator_capacidade"},
    colunas_versionadas=COLUNAS_VERSIONADAS,
)
