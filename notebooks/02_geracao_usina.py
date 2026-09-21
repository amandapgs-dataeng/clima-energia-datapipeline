# Databricks notebook source
# COMMAND ----------
import requests
from datetime import datetime, timedelta, timezone
from pyspark.sql.functions import col, lit, current_timestamp

dbutils.widgets.text("data_referencia", "")
data_param = dbutils.widgets.get("data_referencia")

if data_param == "":
    data_referencia = datetime.now()
else:
    data_referencia = datetime.strptime(data_param, "%Y-%m-%d")

mes_anterior = data_referencia.replace(day=1) - timedelta(days=1)
ano = mes_anterior.year
mes = mes_anterior.month

print(f"Executando para o mês fechado: {ano}-{mes:02d}")

# COMMAND ----------
url = f"https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/geracao_usina_2_ho/GERACAO_USINA-2_{ano}_{mes:02d}.parquet"
local_path = f"/tmp/geracao_{ano}_{mes:02d}.parquet"

resp = requests.get(url)
resp.raise_for_status()

with open(local_path, "wb") as f:
    f.write(resp.content)

df_mes = spark.read.parquet(f"file://{local_path}")

# COMMAND ----------
df_filtrado = df_mes.filter(
    (col("nom_subsistema").contains("NORDESTE")) &
    (col("nom_tipousina").isin("EOLIELÉTRICA", "FOTOVOLTAICA"))
)

print(f"Linhas após filtro: {df_filtrado.count()}")

# COMMAND ----------
ingestion_timestamp_val = datetime.now(timezone.utc)

df_bronze = (
    df_filtrado
    .withColumn("ingestion_timestamp", lit(ingestion_timestamp_val))
    .withColumn("source", lit("ons-geracao-usina-daily"))
)

df_bronze.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("clima_energia.bronze.geracao_usina")

linhas_gravadas = df_bronze.count()

audit_record = [{
    "pipeline_name": "02_geracao_usina",
    "data_referencia": data_referencia.strftime("%Y-%m-%d"),
    "execution_timestamp": ingestion_timestamp_val,
    "status": "success",
    "linhas_gravadas": linhas_gravadas
}]
spark.createDataFrame(audit_record).write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("clima_energia.bronze._audit_log")

print(f"Gravado com sucesso: {linhas_gravadas} linhas para {ano}-{mes:02d}")