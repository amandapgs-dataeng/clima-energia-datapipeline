# Silver do tempo observado em Fortaleza (Open-Meteo).
#
# open_meteo.historico_observado
#   -> clima_observado_padronizada  (view: tipagem, vocabulário comum, regras de qualidade)
#   -> clima_observado  (versão atual; sem histórico: janelas sobrepostas só se deduplicam)
from pyspark import pipelines as dp

from utilities.historico import declarar_tabelas
from utilities.clima import (
    CHAVE_OBSERVADO,
    COLUNAS_OBSERVADO,
    REGRAS_ALERTA,
    REGRAS_DESCARTE,
    padronizar_clima_observado,
)

CATALOG_BRONZE = spark.conf.get("clima_energia.catalog_bronze")  # noqa: F821 (spark é global no pipeline)


@dp.temporary_view(name="clima_observado_padronizada")
@dp.expect_all_or_drop(REGRAS_DESCARTE)
@dp.expect_all(REGRAS_ALERTA)
def clima_observado_padronizada():
    return padronizar_clima_observado(spark.readStream.table(f"{CATALOG_BRONZE}.open_meteo.historico_observado"))  # noqa: F821


declarar_tabelas(
    dp,
    spark,  # noqa: F821
    nome="clima_observado",
    origem="clima_observado_padronizada",
    chave=CHAVE_OBSERVADO,
    colunas=COLUNAS_OBSERVADO,
    descricao="Tempo observado hora a hora em Fortaleza",
    propriedades={"camada": "silver", "fonte": "open_meteo", "dataset": "clima_observado"},
)
