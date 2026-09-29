# Databricks notebook source
# Carga de energia diária (ONS, todos os subsistemas), reprocessando uma janela de 60 dias.
# Reprocessamento: informe janela_dias maior (ex.: 760 para dois anos).

# COMMAND ----------
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date

from utils.bronze_helpers import execucao_auditada, gravar_bronze, ler_parquet_remoto
from utils.date_helpers import FORMATO_DATA, anos_da_janela, janela_retroativa, resolver_data_referencia
from utils.fontes import url_ons
from utils.http_helpers import ArquivoIndisponivel
from utils.logging_helpers import obter_logger

PIPELINE = "05_carga_energia"
FONTE = "ons-carga-energia-weekly"
JANELA_DIAS_PADRAO = 60

dbutils.widgets.text("data_referencia", "")
dbutils.widgets.text("janela_dias", str(JANELA_DIAS_PADRAO))
dbutils.widgets.text("catalog", "clima_energia_bronze")

catalog = dbutils.widgets.get("catalog")
data_referencia = resolver_data_referencia(dbutils.widgets.get("data_referencia"))
janela_dias = int(dbutils.widgets.get("janela_dias") or JANELA_DIAS_PADRAO)
logger = obter_logger(PIPELINE)

# COMMAND ----------
def ler_carga_dos_anos(anos, ano_corrente):
    """Lê o arquivo anual de cada ano da janela.

    Só o arquivo do ano corrente pode faltar (o ONS publica com atraso no início
    do ano); qualquer outra falha interrompe o job.
    """
    frames = []
    for ano in anos:
        arquivo = f"CARGA_ENERGIA_{ano}.parquet"
        try:
            frames.append(ler_parquet_remoto(spark, url_ons("carga_energia_di", arquivo), arquivo))
        except ArquivoIndisponivel:
            if ano != ano_corrente:
                raise
            logger.warning("Arquivo de %s ainda não publicado pelo ONS; seguindo sem ele", ano)

    if not frames:
        raise RuntimeError(f"Nenhum arquivo de carga disponível para os anos {anos}")
    return reduce(DataFrame.unionByName, frames)

# COMMAND ----------
with execucao_auditada(spark, catalog, PIPELINE, data_referencia, logger) as execucao:
    data_inicio, data_fim = janela_retroativa(data_referencia, janela_dias)
    logger.info("Janela: %s até %s", data_inicio.strftime(FORMATO_DATA), data_fim.strftime(FORMATO_DATA))

    df_carga = ler_carga_dos_anos(anos_da_janela(data_inicio, data_fim), ano_corrente=data_fim.year)

    # Recorte só de coleta (quais dias reprocessar); nenhum filtro de negócio na bronze.
    df_janela = df_carga.filter(
        to_date(col("din_instante")).between(data_inicio.strftime(FORMATO_DATA), data_fim.strftime(FORMATO_DATA))
    )
    execucao.linhas_gravadas = gravar_bronze(df_janela, f"{catalog}.ons.carga_energia", FONTE, execucao.inicio)
