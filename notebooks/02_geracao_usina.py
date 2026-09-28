# Databricks notebook source
# Geração horária por usina (ONS) do último mês fechado: eólica e solar do Nordeste.

# COMMAND ----------
from pyspark.sql.functions import col, upper

from utils.bronze_helpers import execucao_auditada, gravar_bronze, ler_parquet_remoto
from utils.date_helpers import mes_anterior_fechado, resolver_data_referencia
from utils.fontes import url_ons
from utils.logging_helpers import obter_logger
from utils.normalization_helpers import SUBSISTEMA_ALVO, TIPOS_USINA_ALVO

PIPELINE = "02_geracao_usina"
FONTE = "ons-geracao-usina-daily"

dbutils.widgets.text("data_referencia", "")
dbutils.widgets.text("catalog", "clima_energia_dev")

catalog = dbutils.widgets.get("catalog")
data_referencia = resolver_data_referencia(dbutils.widgets.get("data_referencia"))
logger = obter_logger(PIPELINE)

# COMMAND ----------
with execucao_auditada(spark, catalog, PIPELINE, data_referencia, logger) as execucao:
    ano, mes = mes_anterior_fechado(data_referencia)
    logger.info("Mês fechado: %s-%02d", ano, mes)

    arquivo = f"GERACAO_USINA-2_{ano}_{mes:02d}.parquet"
    df_mes = ler_parquet_remoto(spark, url_ons("geracao_usina_2_ho", arquivo), arquivo)

    df_filtrado = df_mes.filter(
        upper(col("nom_subsistema")).contains(SUBSISTEMA_ALVO)
        & col("nom_tipousina").isin(*TIPOS_USINA_ALVO)
    )
    execucao.linhas_gravadas = gravar_bronze(df_filtrado, f"{catalog}.bronze.geracao_usina", FONTE, execucao.inicio)
