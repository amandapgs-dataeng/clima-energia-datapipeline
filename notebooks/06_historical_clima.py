# Databricks notebook source
# COMMAND ----------
import requests
import json
from datetime import datetime, timedelta, timezone

dbutils.widgets.text("data_referencia", "")
data_param = dbutils.widgets.get("data_referencia")

if data_param == "":
    data_referencia = datetime.now()
else:
    data_referencia = datetime.strptime(data_param, "%Y-%m-%d")

end_date = (data_referencia - timedelta(days=10)).strftime("%Y-%m-%d")
start_date = (data_referencia - timedelta(days=16)).strftime("%Y-%m-%d")

print(f"Janela: {start_date} até {end_date}")

# COMMAND ----------
latitude = -3.72
longitude = -38.54
historical_url = "https://archive-api.open-meteo.com/v1/archive"

hourly_vars = [
    "temperature_2m", "apparent_temperature", "relative_humidity_2m", "dew_point_2m",
    "precipitation", "rain", "showers",
    "weathercode", "pressure_msl", "surface_pressure",
    "cloud_cover", "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high",
    "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m",
    "shortwave_radiation", "uv_index", "visibility", "is_day"
]

params = {
    "latitude": latitude,
    "longitude": longitude,
    "start_date": start_date,
    "end_date": end_date,
    "hourly": ",".join(hourly_vars),
    "timezone": "America/Fortaleza"
}

response = requests.get(historical_url, params=params)
response.raise_for_status()
data = response.json()

# COMMAND ----------
ingestion_timestamp = datetime.now(timezone.utc)

record = [{
    "ingestion_timestamp": ingestion_timestamp,
    "source": "open-meteo-historical-weekly",
    "latitude_requested": latitude,
    "longitude_requested": longitude,
    "start_date": start_date,
    "end_date": end_date,
    "raw_response": json.dumps(data)
}]

df = spark.createDataFrame(record)
df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("clima_energia.bronze.historico_observado")

audit_record = [{
    "pipeline_name": "06_historical_clima",
    "data_referencia": data_referencia.strftime("%Y-%m-%d"),
    "execution_timestamp": ingestion_timestamp,
    "status": "success",
    "linhas_gravadas": df.count()
}]
spark.createDataFrame(audit_record).write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("clima_energia.bronze._audit_log")

print(f"Gravado com sucesso: {start_date} até {end_date}")