# Silver da previsão horária do tempo em Fortaleza (Open-Meteo).
#
# open_meteo.previsao_bruta
#   -> clima_previsao_padronizada  (view: tipagem, vocabulário comum, regras de qualidade)
#   -> clima_previsao  (versão atual; sem histórico: cada emissão é um fato novo, não uma revisão)
from pyspark import pipelines as dp

from utilities.historico import declarar_tabelas
from utilities.clima import (
    CHAVE_PREVISAO,
    COLUNAS_PREVISAO,
    REGRAS_ALERTA,
    REGRAS_DESCARTE,
    padronizar_clima_previsao,
)

CATALOG_BRONZE = spark.conf.get("clima_energia.catalog_bronze")  # noqa: F821 (spark é global no pipeline)


@dp.temporary_view(name="clima_previsao_padronizada")
@dp.expect_all_or_drop(REGRAS_DESCARTE)
@dp.expect_all(REGRAS_ALERTA)
def clima_previsao_padronizada():
    return padronizar_clima_previsao(spark.readStream.table(f"{CATALOG_BRONZE}.open_meteo.previsao_bruta"))  # noqa: F821


declarar_tabelas(
    dp,
    spark,  # noqa: F821
    nome="clima_previsao",
    origem="clima_previsao_padronizada",
    chave=CHAVE_PREVISAO,
    colunas=COLUNAS_PREVISAO,
    descricao="Previsão horária do tempo em Fortaleza, por dia de emissão",
    propriedades={"camada": "silver", "fonte": "open_meteo", "dataset": "clima_previsao"},
)
