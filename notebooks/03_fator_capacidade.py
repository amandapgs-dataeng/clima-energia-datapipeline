# Databricks notebook source
# Fator de capacidade (ONS) do último mês fechado: usinas do Nordeste.

# COMMAND ----------
from pyspark.sql.functions import col, upper

from utils.bronze_helpers import execucao_auditada, gravar_bronze, ler_parquet_remoto
from utils.date_helpers import mes_anterior_fechado, resolver_data_referencia
from utils.fontes import url_ons
from utils.logging_helpers import obter_logger
from utils.normalization_helpers import SUBSISTEMA_ALVO

PIPELINE = "03_fator_capacidade"
FONTE = "ons-fator-capacidade-monthly"

dbutils.widgets.text("data_referencia", "")
dbutils.widgets.text("catalog", "clima_energia_dev")

catalog = dbutils.widgets.get("catalog")
data_referencia = resolver_data_referencia(dbutils.widgets.get("data_referencia"))
logger = obter_logger(PIPELINE)

# COMMAND ----------
with execucao_auditada(spark, catalog, PIPELINE, data_referencia, logger) as execucao:
    ano, mes = mes_anterior_fechado(data_referencia)
    logger.info("Mês fechado: %s-%02d", ano, mes)

    arquivo = f"FATOR_CAPACIDADE-2_{ano}_{mes:02d}.parquet"
    df_mes = ler_parquet_remoto(spark, url_ons("fator_capacidade_2_di", arquivo), arquivo)

    df_filtrado = df_mes.filter(upper(col("nom_subsistema")).contains(SUBSISTEMA_ALVO))
    execucao.linhas_gravadas = gravar_bronze(df_filtrado, f"{catalog}.bronze.fator_capacidade", FONTE, execucao.inicio)
