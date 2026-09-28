# Databricks notebook source
# Previsão horária do tempo em Fortaleza (Open-Meteo) para a data de referência.

# COMMAND ----------
import json

from utils.bronze_helpers import execucao_auditada, gravar_bronze
from utils.date_helpers import FORMATO_DATA, resolver_data_referencia
from utils.fontes import LATITUDE_FORTALEZA, LONGITUDE_FORTALEZA, OPEN_METEO_FORECAST_URL, params_open_meteo
from utils.http_helpers import buscar_json
from utils.logging_helpers import obter_logger

PIPELINE = "01_forecast_clima"
FONTE = "open-meteo-forecast-daily"

dbutils.widgets.text("data_referencia", "")
dbutils.widgets.text("catalog", "clima_energia_dev")

catalog = dbutils.widgets.get("catalog")
data_referencia = resolver_data_referencia(dbutils.widgets.get("data_referencia"))
logger = obter_logger(PIPELINE)

# COMMAND ----------
with execucao_auditada(spark, catalog, PIPELINE, data_referencia, logger) as execucao:
    resposta = buscar_json(OPEN_METEO_FORECAST_URL, params_open_meteo(data_referencia, data_referencia))

    df = spark.createDataFrame([{
        "latitude_requested": LATITUDE_FORTALEZA,
        "longitude_requested": LONGITUDE_FORTALEZA,
        "data_referencia": data_referencia.strftime(FORMATO_DATA),
        "raw_response": json.dumps(resposta),
    }])
    execucao.linhas_gravadas = gravar_bronze(df, f"{catalog}.bronze.previsao_bruta", FONTE, execucao.inicio)
