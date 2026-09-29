# Gold: uma materialized view por tabela do catálogo (metricas/catalogo.py), calculada a partir
# da silver do mesmo ambiente. Recorte de negócio: subsistema Nordeste.
from pyspark import pipelines as dp

from metricas.catalogo import TABELAS
from metricas.padrao_gold import declarar_gold

CATALOG = spark.conf.get("clima_energia.catalog")  # noqa: F821 (spark é global no pipeline)

for tabela in TABELAS:
    declarar_gold(dp, spark, tabela, CATALOG)  # noqa: F821
