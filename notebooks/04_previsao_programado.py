# Databricks notebook source
# Programação x previsão de geração eólica e solar (ONS) do dia D-1.

# COMMAND ----------
from utils.bronze_helpers import execucao_auditada, gravar_bronze, ler_parquet_remoto
from utils.date_helpers import resolver_data_referencia
from utils.fontes import url_ons
from utils.logging_helpers import obter_logger

PIPELINE = "04_previsao_programado"
FONTE = "ons-previsao-programado-daily"

dbutils.widgets.text("data_referencia", "")
dbutils.widgets.text("catalog", "clima_energia_dev")

catalog = dbutils.widgets.get("catalog")
data_referencia = resolver_data_referencia(dbutils.widgets.get("data_referencia"), dias_defasagem=1)
logger = obter_logger(PIPELINE)

# COMMAND ----------
with execucao_auditada(spark, catalog, PIPELINE, data_referencia, logger) as execucao:
    arquivo = f"PROGRAMACAO_X_PREVISAO_{data_referencia:%Y_%m_%d}.parquet"
    df_dia = ler_parquet_remoto(spark, url_ons("programacao_x_previsao", arquivo), arquivo)

    execucao.linhas_gravadas = gravar_bronze(df_dia, f"{catalog}.bronze.previsao_programado_eolsol", FONTE, execucao.inicio)
