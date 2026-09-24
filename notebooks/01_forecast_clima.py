# Databricks notebook source
# COMMAND ----------
import requests
import json
from datetime import datetime, timedelta, timezone

dbutils.widgets.text("data_referencia", "")
data_param = dbutils.widgets.get("data_referencia")
dbutils.widgets.text("catalog", "clima_energia_dev")
catalog = dbutils.widgets.get("catalog")

if data_param == "":
    data_referencia = datetime.now()
else:
    data_referencia = datetime.strptime(data_param, "%Y-%m-%d")

print(f"Executando para: {data_referencia.strftime('%Y-%m-%d')}")

# COMMAND ----------
latitude = -3.72
longitude = -38.54
forecast_url = "https://api.open-meteo.com/v1/forecast"

hourly_vars = [
    "temperature_2m", "apparent_temperature", "relative_humidity_2m", "dew_point_2m",
    "precipitation", "rain", "showers",
    "weathercode", "pressure_msl", "surface_pressure",
    "cloud_cover", "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high",
    "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m",
    "shortwave_radiation", "uv_index", "visibility", "is_day"
]

# COMMAND ----------
params = {
    "latitude": latitude,
    "longitude": longitude,
    "start_date": data_referencia.strftime("%Y-%m-%d"),
    "end_date": data_referencia.strftime("%Y-%m-%d"),
    "hourly": ",".join(hourly_vars),
    "timezone": "America/Fortaleza"
}

response = requests.get(forecast_url, params=params)
response.raise_for_status()
data = response.json()

# COMMAND ----------
ingestion_timestamp = datetime.now(timezone.utc)

record = [{
    "ingestion_timestamp": ingestion_timestamp,
    "source": "open-meteo-forecast-daily",
    "latitude_requested": latitude,
    "longitude_requested": longitude,
    "data_referencia": data_referencia.strftime("%Y-%m-%d"),
    "raw_response": json.dumps(data)
}]

df = spark.createDataFrame(record)
df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(f"{catalog}.bronze.previsao_bruta")

audit_record = [{
    "pipeline_name": "01_forecast_clima",
    "data_referencia": data_referencia.strftime("%Y-%m-%d"),
    "execution_timestamp": ingestion_timestamp,
    "status": "success",
    "linhas_gravadas": df.count()
}]
spark.createDataFrame(audit_record).write.format("delta").mode("append").saveAsTable(f"{catalog}.bronze._audit_log")

print(f"Gravado com sucesso: {data_referencia.strftime('%Y-%m-%d')}")
# COMMAND ----------
