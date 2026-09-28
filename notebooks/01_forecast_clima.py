# Databricks notebook source
# Previsão horária do tempo em Fortaleza (Open-Meteo) para o dia seguinte à data de referência.
# O job roda às 21h: buscar o dia seguinte é o que torna isto uma previsão de verdade
# (o dia corrente, a essa hora, já passou quase inteiro).

# COMMAND ----------
import json

from utils.bronze_helpers import execucao_auditada, gravar_bronze
from utils.date_helpers import FORMATO_DATA, data_prevista, resolver_data_referencia
from utils.fontes import LATITUDE_FORTALEZA, LONGITUDE_FORTALEZA, OPEN_METEO_FORECAST_URL, params_open_meteo
from utils.http_helpers import buscar_json
from utils.logging_helpers import obter_logger

PIPELINE = "01_forecast_clima"
FONTE = "open-meteo-forecast-daily"
HORIZONTE_DIAS = 1

dbutils.widgets.text("data_referencia", "")
dbutils.widgets.text("catalog", "clima_energia_bronze")

catalog = dbutils.widgets.get("catalog")
data_referencia = resolver_data_referencia(dbutils.widgets.get("data_referencia"))
logger = obter_logger(PIPELINE)

# COMMAND ----------
with execucao_auditada(spark, catalog, PIPELINE, data_referencia, logger) as execucao:
    dia_previsto = data_prevista(data_referencia, HORIZONTE_DIAS)
    logger.info("Emissão em %s, prevendo %s", data_referencia.strftime(FORMATO_DATA), dia_previsto.strftime(FORMATO_DATA))

    resposta = buscar_json(OPEN_METEO_FORECAST_URL, params_open_meteo(dia_previsto, dia_previsto))

    # data_referencia = dia da emissão; data_prevista = dia descrito pela previsão.
    # Linhas anteriores a esta mudança não têm data_prevista: previam o próprio data_referencia.
    df = spark.createDataFrame([{
        "latitude_requested": LATITUDE_FORTALEZA,
        "longitude_requested": LONGITUDE_FORTALEZA,
        "data_referencia": data_referencia.strftime(FORMATO_DATA),
        "data_prevista": dia_previsto.strftime(FORMATO_DATA),
        "horizonte_dias": HORIZONTE_DIAS,
        "raw_response": json.dumps(resposta),
    }])
    execucao.linhas_gravadas = gravar_bronze(df, f"{catalog}.open_meteo.previsao_bruta", FONTE, execucao.inicio)
