# Databricks notebook source
# Previsões do tempo emitidas na véspera (D+1) para um período passado, nos pontos das usinas e
# nas capitais do Nordeste (Open-Meteo, API de previsões anteriores).
# Serve ao reprocessamento: a coleta diária (01) só guarda previsões a partir de quando começou.

# COMMAND ----------
from datetime import datetime

from utils.bronze_helpers import execucao_auditada, gravar_bronze
from utils.date_helpers import FORMATO_DATA, meses_do_periodo, resolver_data_referencia
from utils.fontes import (
    OPEN_METEO_PREVIOUS_RUNS_URL,
    params_open_meteo,
    registros_por_ponto,
    respostas_por_ponto,
    variaveis_da_vespera,
)
from utils.http_helpers import buscar_json, criar_sessao
from utils.logging_helpers import obter_logger
from utils.pontos_clima import PONTOS_CLIMA

PIPELINE = "07_previsao_clima_historica"
FONTE = "open-meteo-previous-runs"
HORIZONTE_DIAS = 1

dbutils.widgets.text("data_inicio", "")
dbutils.widgets.text("data_fim", "")
dbutils.widgets.text("catalog", "clima_energia_bronze")

catalog = dbutils.widgets.get("catalog")
data_inicio, data_fim = (datetime.strptime(dbutils.widgets.get(w), FORMATO_DATA) for w in ("data_inicio", "data_fim"))
data_referencia = resolver_data_referencia("")
logger = obter_logger(PIPELINE)

# COMMAND ----------
with execucao_auditada(spark, catalog, PIPELINE, data_referencia, logger) as execucao:
    sessao = criar_sessao()
    variaveis = variaveis_da_vespera(HORIZONTE_DIAS)
    registros = []
    for inicio, fim in meses_do_periodo(data_inicio, data_fim):
        logger.info("Buscando previsões D+%s de %s até %s", HORIZONTE_DIAS, inicio.strftime(FORMATO_DATA), fim.strftime(FORMATO_DATA))
        resposta = buscar_json(OPEN_METEO_PREVIOUS_RUNS_URL, params_open_meteo(PONTOS_CLIMA, inicio, fim, variaveis), sessao=sessao)
        registros += registros_por_ponto(
            respostas_por_ponto(resposta, PONTOS_CLIMA),
            start_date=inicio.strftime(FORMATO_DATA),
            end_date=fim.strftime(FORMATO_DATA),
            horizonte_dias=HORIZONTE_DIAS,
        )

    execucao.linhas_gravadas = gravar_bronze(
        spark.createDataFrame(registros), f"{catalog}.open_meteo.previsao_historica_bruta", FONTE, execucao.inicio
    )
