# Databricks notebook source
# Fator de capacidade (ONS) do último mês fechado, arquivo completo (todo o Brasil).

# COMMAND ----------
from utils.bronze_helpers import execucao_auditada, gravar_bronze, ler_parquet_remoto
from utils.date_helpers import mes_anterior_fechado, resolver_data_referencia
from utils.fontes import url_ons
from utils.logging_helpers import obter_logger

PIPELINE = "03_fator_capacidade"
FONTE = "ons-fator-capacidade-monthly"

dbutils.widgets.text("data_referencia", "")
dbutils.widgets.text("catalog", "clima_energia_bronze")

catalog = dbutils.widgets.get("catalog")
data_referencia = resolver_data_referencia(dbutils.widgets.get("data_referencia"))
logger = obter_logger(PIPELINE)

# COMMAND ----------
with execucao_auditada(spark, catalog, PIPELINE, data_referencia, logger) as execucao:
    ano, mes = mes_anterior_fechado(data_referencia)
    logger.info("Mês fechado: %s-%02d", ano, mes)

    arquivo = f"FATOR_CAPACIDADE-2_{ano}_{mes:02d}.parquet"
    df_mes = ler_parquet_remoto(spark, url_ons("fator_capacidade_2_di", arquivo), arquivo)
    execucao.linhas_gravadas = gravar_bronze(df_mes, f"{catalog}.ons.fator_capacidade", FONTE, execucao.inicio)
