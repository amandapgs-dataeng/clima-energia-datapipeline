"""Endereços e parâmetros das fontes externas."""
from utils.date_helpers import FORMATO_DATA

ONS_BASE_URL = "https://ons-aws-prod-opendata.s3.amazonaws.com/dataset"

OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
OPEN_METEO_TIMEZONE = "America/Fortaleza"

LATITUDE_FORTALEZA = -3.72
LONGITUDE_FORTALEZA = -38.54

VARIAVEIS_HORARIAS = (
    "temperature_2m", "apparent_temperature", "relative_humidity_2m", "dew_point_2m",
    "precipitation", "rain", "showers",
    "weathercode", "pressure_msl", "surface_pressure",
    "cloud_cover", "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high",
    "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m",
    "shortwave_radiation", "uv_index", "visibility", "is_day",
)


def url_ons(dataset, arquivo):
    return f"{ONS_BASE_URL}/{dataset}/{arquivo}"


def params_open_meteo(data_inicio, data_fim):
    """Parâmetros da consulta horária do Open-Meteo para Fortaleza."""
    return {
        "latitude": LATITUDE_FORTALEZA,
        "longitude": LONGITUDE_FORTALEZA,
        "start_date": data_inicio.strftime(FORMATO_DATA),
        "end_date": data_fim.strftime(FORMATO_DATA),
        "hourly": ",".join(VARIAVEIS_HORARIAS),
        "timezone": OPEN_METEO_TIMEZONE,
    }
