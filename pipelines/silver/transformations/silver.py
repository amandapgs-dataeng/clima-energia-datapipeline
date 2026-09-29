# Silver: uma declaração por tabela do catálogo (utilities/catalogo.py). Para cada tabela,
# o padrão (utilities/padrao_silver.py) cria a view padronizada, a validada, a versão atual,
# a quarentena e, quando a fonte revisa dados, o histórico de versões.
from pyspark import pipelines as dp

from utilities.catalogo import TABELAS
from utilities.padrao_silver import declarar_silver

CATALOG_BRONZE = spark.conf.get("clima_energia.catalog_bronze")  # noqa: F821 (spark é global no pipeline)

for tabela in TABELAS:
    declarar_silver(dp, spark, tabela, CATALOG_BRONZE)  # noqa: F821
