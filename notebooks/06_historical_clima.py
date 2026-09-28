# Databricks notebook source
# Clima observado em Fortaleza (Open-Meteo, arquivo histórico) de 16 a 10 dias atrás.

# COMMAND ----------
import json

from utils.bronze_helpers import execucao_auditada, gravar_bronze
from utils.date_helpers import FORMATO_DATA, janela_historico, resolver_data_referencia
from utils.fontes import LATITUDE_FORTALEZA, LONGITUDE_FORTALEZA, OPEN_METEO_ARCHIVE_URL, params_open_meteo
from utils.http_helpers import buscar_json
from utils.logging_helpers import obter_logger

PIPELINE = "06_historical_clima"
FONTE = "open-meteo-historical-weekly"

dbutils.widgets.text("data_referencia", "")
dbutils.widgets.text("catalog", "clima_energia_dev")

catalog = dbutils.widgets.get("catalog")
data_referencia = resolver_data_referencia(dbutils.widgets.get("data_referencia"))
logger = obter_logger(PIPELINE)

# COMMAND ----------
with execucao_auditada(spark, catalog, PIPELINE, data_referencia, logger) as execucao:
    data_inicio, data_fim = janela_historico(data_referencia)
    logger.info("Janela: %s até %s", data_inicio.strftime(FORMATO_DATA), data_fim.strftime(FORMATO_DATA))

    resposta = buscar_json(OPEN_METEO_ARCHIVE_URL, params_open_meteo(data_inicio, data_fim))

    df = spark.createDataFrame([{
        "latitude_requested": LATITUDE_FORTALEZA,
        "longitude_requested": LONGITUDE_FORTALEZA,
        "start_date": data_inicio.strftime(FORMATO_DATA),
        "end_date": data_fim.strftime(FORMATO_DATA),
        "raw_response": json.dumps(resposta),
    }])
    execucao.linhas_gravadas = gravar_bronze(df, f"{catalog}.bronze.historico_observado", FONTE, execucao.inicio)
