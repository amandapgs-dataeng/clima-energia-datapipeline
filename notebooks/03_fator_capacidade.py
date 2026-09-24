# Databricks notebook source
# COMMAND ----------
import requests
from datetime import datetime, timezone
from pyspark.sql.functions import col, lit, upper

from utils.date_helpers import mes_anterior_fechado, resolver_data_referencia
from utils.normalization_helpers import SUBSISTEMA_ALVO

dbutils.widgets.text("data_referencia", "")
data_param = dbutils.widgets.get("data_referencia")
dbutils.widgets.text("catalog", "clima_energia_dev")
catalog = dbutils.widgets.get("catalog")

data_referencia = resolver_data_referencia(data_param)
ano, mes = mes_anterior_fechado(data_referencia)

print(f"Executando para o mês fechado: {ano}-{mes:02d}")

# COMMAND ----------
url = f"https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/fator_capacidade_2_di/FATOR_CAPACIDADE-2_{ano}_{mes:02d}.parquet"
local_path = f"/tmp/fc_{ano}_{mes:02d}.parquet"

resp = requests.get(url)
resp.raise_for_status()

with open(local_path, "wb") as f:
    f.write(resp.content)

df_mes = spark.read.parquet(f"file://{local_path}")

# COMMAND ----------
df_filtrado = df_mes.filter(upper(col("nom_subsistema")).contains(SUBSISTEMA_ALVO))

print(f"Linhas após filtro: {df_filtrado.count()}")

# COMMAND ----------
ingestion_timestamp_val = datetime.now(timezone.utc)

df_bronze = (
    df_filtrado
    .withColumn("ingestion_timestamp", lit(ingestion_timestamp_val))
    .withColumn("source", lit("ons-fator-capacidade-monthly"))
)

df_bronze.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(f"{catalog}.bronze.fator_capacidade")

linhas_gravadas = df_bronze.count()

audit_record = [{
    "pipeline_name": "03_fator_capacidade",
    "data_referencia": data_referencia.strftime("%Y-%m-%d"),
    "execution_timestamp": ingestion_timestamp_val,
    "status": "success",
    "linhas_gravadas": linhas_gravadas
}]
spark.createDataFrame(audit_record).write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(f"{catalog}.bronze._audit_log")

print(f"Gravado com sucesso: {linhas_gravadas} linhas para {ano}-{mes:02d}")