# Databricks notebook source
# Previsão horária do tempo (Open-Meteo) para o dia seguinte à data de referência, nos pontos
# das usinas e nas capitais do Nordeste (utils/pontos_clima.py).
# O job roda às 21h: buscar o dia seguinte é o que torna isto uma previsão de verdade.

# COMMAND ----------
from utils.bronze_helpers import execucao_auditada, gravar_bronze
from utils.date_helpers import FORMATO_DATA, data_prevista, resolver_data_referencia
from utils.fontes import OPEN_METEO_FORECAST_URL, params_open_meteo, registros_por_ponto, respostas_por_ponto
from utils.http_helpers import buscar_json
from utils.logging_helpers import obter_logger
from utils.pontos_clima import PONTOS_CLIMA

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
    logger.info("Emissão em %s, prevendo %s, %s pontos",
                data_referencia.strftime(FORMATO_DATA), dia_previsto.strftime(FORMATO_DATA), len(PONTOS_CLIMA))

    resposta = buscar_json(OPEN_METEO_FORECAST_URL, params_open_meteo(PONTOS_CLIMA, dia_previsto, dia_previsto))

    # data_referencia = dia da emissão; data_prevista = dia descrito pela previsão.
    df = spark.createDataFrame(registros_por_ponto(
        respostas_por_ponto(resposta, PONTOS_CLIMA),
        data_referencia=data_referencia.strftime(FORMATO_DATA),
        data_prevista=dia_previsto.strftime(FORMATO_DATA),
        horizonte_dias=HORIZONTE_DIAS,
    ))
    execucao.linhas_gravadas = gravar_bronze(df, f"{catalog}.open_meteo.previsao_bruta", FONTE, execucao.inicio)
