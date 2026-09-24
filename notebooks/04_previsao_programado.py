# Databricks notebook source
# COMMAND ----------
import requests
from datetime import datetime, timezone
from pyspark.sql.functions import lit

from utils.date_helpers import resolver_data_referencia

dbutils.widgets.text("data_referencia", "")
data_param = dbutils.widgets.get("data_referencia")
dbutils.widgets.text("catalog", "clima_energia_dev")
catalog = dbutils.widgets.get("catalog")

data_referencia = resolver_data_referencia(data_param, dias_defasagem=1)  # padrão: D-1

data_str = data_referencia.strftime("%Y_%m_%d")

print(f"Executando para: {data_referencia.strftime('%Y-%m-%d')}")

# COMMAND ----------
url = f"https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/programacao_x_previsao/PROGRAMACAO_X_PREVISAO_{data_str}.parquet"
local_path = f"/tmp/prog_{data_str}.parquet"

resp = requests.get(url)
resp.raise_for_status()

with open(local_path, "wb") as f:
    f.write(resp.content)

df_dia = spark.read.parquet(f"file://{local_path}")

print(f"Linhas encontradas: {df_dia.count()}")

# COMMAND ----------
ingestion_timestamp_val = datetime.now(timezone.utc)

df_bronze = (
    df_dia
    .withColumn("ingestion_timestamp", lit(ingestion_timestamp_val))
    .withColumn("source", lit("ons-previsao-programado-daily"))
)

df_bronze.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(f"{catalog}.bronze.previsao_programado_eolsol")

linhas_gravadas = df_bronze.count()

audit_record = [{
    "pipeline_name": "04_previsao_programado",
    "data_referencia": data_referencia.strftime("%Y-%m-%d"),
    "execution_timestamp": ingestion_timestamp_val,
    "status": "success",
    "linhas_gravadas": linhas_gravadas
}]
spark.createDataFrame(audit_record).write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(f"{catalog}.bronze._audit_log")

print(f"Gravado com sucesso: {linhas_gravadas} linhas para {data_referencia.strftime('%Y-%m-%d')}")