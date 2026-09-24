# Databricks notebook source
# COMMAND ----------
import requests
from datetime import datetime, timedelta, timezone
from pyspark.sql.functions import col, lit, to_date
from functools import reduce

dbutils.widgets.text("data_referencia", "")
data_param = dbutils.widgets.get("data_referencia")
dbutils.widgets.text("catalog", "clima_energia_dev")
catalog = dbutils.widgets.get("catalog")

if data_param == "":
    data_referencia = datetime.now()
else:
    data_referencia = datetime.strptime(data_param, "%Y-%m-%d")

janela_dias = 60
data_fim = data_referencia
data_inicio = data_referencia - timedelta(days=janela_dias)

print(f"Janela de reprocessamento: {data_inicio.strftime('%Y-%m-%d')} até {data_fim.strftime('%Y-%m-%d')}")

# COMMAND ----------
anos_necessarios = sorted(set([data_inicio.year, data_fim.year]))

frames = []
for ano in anos_necessarios:
    url = f"https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/carga_energia_di/CARGA_ENERGIA_{ano}.parquet"
    local_path = f"/tmp/carga_energia_{ano}.parquet"
    try:
        resp = requests.get(url)
        resp.raise_for_status()
        with open(local_path, "wb") as f:
            f.write(resp.content)
        df_ano = spark.read.parquet(f"file://{local_path}")
        frames.append(df_ano)
        print(f"Ano {ano}: baixado com sucesso")
    except Exception as e:
        print(f"Falha ao baixar ano {ano}: {e}")

df_todos_anos = reduce(lambda a, b: a.unionByName(b), frames)

# COMMAND ----------
df_filtrado = df_todos_anos.filter(
    (col("id_subsistema") == "NE") &
    (to_date(col("din_instante")) >= data_inicio.strftime("%Y-%m-%d")) &
    (to_date(col("din_instante")) <= data_fim.strftime("%Y-%m-%d"))
)

print(f"Linhas após filtro: {df_filtrado.count()}")

# COMMAND ----------
ingestion_timestamp_val = datetime.now(timezone.utc)

df_bronze = (
    df_filtrado
    .withColumn("ingestion_timestamp", lit(ingestion_timestamp_val))
    .withColumn("source", lit("ons-carga-energia-weekly"))
)

df_bronze.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(f"{catalog}.bronze.carga_energia")

linhas_gravadas = df_bronze.count()

audit_record = [{
    "pipeline_name": "05_carga_energia",
    "data_referencia": data_referencia.strftime("%Y-%m-%d"),
    "execution_timestamp": ingestion_timestamp_val,
    "status": "success",
    "linhas_gravadas": linhas_gravadas
}]
spark.createDataFrame(audit_record).write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(f"{catalog}.bronze._audit_log")

print(f"Gravado com sucesso: {linhas_gravadas} linhas")