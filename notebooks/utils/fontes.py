"""Endereços e parâmetros das fontes externas."""
import json

from utils.date_helpers import FORMATO_DATA

ONS_BASE_URL = "https://ons-aws-prod-opendata.s3.amazonaws.com/dataset"

OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
# Previsões emitidas em dias anteriores (ex.: *_previous_day1 = previsão feita na véspera).
OPEN_METEO_PREVIOUS_RUNS_URL = "https://previous-runs-api.open-meteo.com/v1/forecast"
OPEN_METEO_TIMEZONE = "America/Fortaleza"

VARIAVEIS_HORARIAS = (
    "temperature_2m", "apparent_temperature", "relative_humidity_2m", "dew_point_2m",
    "precipitation", "rain", "showers",
    "weathercode", "pressure_msl", "surface_pressure",
    "cloud_cover", "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high",
    "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m",
    "wind_speed_100m", "wind_direction_100m",  # altura aproximada do rotor das turbinas
    "shortwave_radiation", "uv_index", "visibility", "is_day",
)


def url_ons(dataset, arquivo):
    return f"{ONS_BASE_URL}/{dataset}/{arquivo}"


def variaveis_da_vespera(dias=1):
    """Nomes das variáveis na API de previsões anteriores (previsão emitida `dias` antes)."""
    return tuple(f"{variavel}_previous_day{dias}" for variavel in VARIAVEIS_HORARIAS)


def params_open_meteo(pontos, data_inicio, data_fim, variaveis=VARIAVEIS_HORARIAS):
    """Consulta horária do Open-Meteo para vários pontos numa única chamada."""
    return {
        "latitude": ",".join(str(ponto.latitude) for ponto in pontos),
        "longitude": ",".join(str(ponto.longitude) for ponto in pontos),
        "start_date": data_inicio.strftime(FORMATO_DATA),
        "end_date": data_fim.strftime(FORMATO_DATA),
        "hourly": ",".join(variaveis),
        "timezone": OPEN_METEO_TIMEZONE,
    }


def respostas_por_ponto(resposta, pontos):
    """Pares (ponto, resposta): a API devolve uma lista, na ordem dos pontos (ou um objeto, se for um só)."""
    respostas = resposta if isinstance(resposta, list) else [resposta]
    if len(respostas) != len(pontos):
        raise ValueError(f"A API devolveu {len(respostas)} respostas para {len(pontos)} pontos")
    return list(zip(pontos, respostas))


def registros_por_ponto(pares, **campos):
    """Uma linha da bronze por ponto: identificação do ponto, resposta JSON bruta e campos comuns."""
    return [
        {
            "ponto_id": ponto.id,
            "uf": ponto.uf,
            "tipo_ponto": ponto.tipo,
            "latitude_requested": ponto.latitude,
            "longitude_requested": ponto.longitude,
            **campos,
            "raw_response": json.dumps(resposta),
        }
        for ponto, resposta in pares
    ]
