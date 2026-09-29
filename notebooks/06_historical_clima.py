# Databricks notebook source
# Clima observado (Open-Meteo, arquivo histórico) nos pontos das usinas e nas capitais do Nordeste.
# Padrão: janela de 16 a 10 dias atrás (a API publica com atraso).
# Reprocessamento: informe data_inicio e data_fim; o período é buscado mês a mês.

# COMMAND ----------
from datetime import datetime

from utils.bronze_helpers import execucao_auditada, gravar_bronze
from utils.date_helpers import FORMATO_DATA, janela_historico, meses_do_periodo, resolver_data_referencia
from utils.fontes import OPEN_METEO_ARCHIVE_URL, params_open_meteo, registros_por_ponto, respostas_por_ponto
from utils.http_helpers import buscar_json, criar_sessao
from utils.logging_helpers import obter_logger
from utils.pontos_clima import PONTOS_CLIMA

PIPELINE = "06_historical_clima"
FONTE = "open-meteo-historical-weekly"

dbutils.widgets.text("data_referencia", "")
dbutils.widgets.text("data_inicio", "")
dbutils.widgets.text("data_fim", "")
dbutils.widgets.text("catalog", "clima_energia_bronze")

catalog = dbutils.widgets.get("catalog")
data_referencia = resolver_data_referencia(dbutils.widgets.get("data_referencia"))
logger = obter_logger(PIPELINE)

if dbutils.widgets.get("data_inicio") and dbutils.widgets.get("data_fim"):
    periodo = tuple(datetime.strptime(dbutils.widgets.get(w), FORMATO_DATA) for w in ("data_inicio", "data_fim"))
else:
    periodo = janela_historico(data_referencia)

# COMMAND ----------
with execucao_auditada(spark, catalog, PIPELINE, data_referencia, logger) as execucao:
    sessao = criar_sessao()
    registros = []
    for inicio, fim in meses_do_periodo(*periodo):
        logger.info("Buscando %s até %s, %s pontos", inicio.strftime(FORMATO_DATA), fim.strftime(FORMATO_DATA), len(PONTOS_CLIMA))
        resposta = buscar_json(OPEN_METEO_ARCHIVE_URL, params_open_meteo(PONTOS_CLIMA, inicio, fim), sessao=sessao)
        registros += registros_por_ponto(
            respostas_por_ponto(resposta, PONTOS_CLIMA),
            start_date=inicio.strftime(FORMATO_DATA),
            end_date=fim.strftime(FORMATO_DATA),
        )

    execucao.linhas_gravadas = gravar_bronze(
        spark.createDataFrame(registros), f"{catalog}.open_meteo.historico_observado", FONTE, execucao.inicio
    )
