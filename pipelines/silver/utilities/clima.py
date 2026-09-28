"""Silver do clima (Open-Meteo): cada resposta JSON da bronze vira uma linha por hora."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utilities.historico import SEQUENCIA

# (variável na API, coluna na silver, tipo, comentário). Unidades padrão do Open-Meteo.
VARIAVEIS = [
    ("temperature_2m", "temperatura_c", "DOUBLE", "Temperatura do ar a 2 m, em °C"),
    ("apparent_temperature", "sensacao_termica_c", "DOUBLE", "Sensação térmica, em °C"),
    ("relative_humidity_2m", "umidade_relativa_pct", "DOUBLE", "Umidade relativa a 2 m, em %"),
    ("dew_point_2m", "ponto_orvalho_c", "DOUBLE", "Ponto de orvalho a 2 m, em °C"),
    ("precipitation", "precipitacao_mm", "DOUBLE", "Precipitação total na hora, em mm"),
    ("rain", "chuva_mm", "DOUBLE", "Chuva na hora, em mm"),
    ("showers", "pancadas_mm", "DOUBLE", "Pancadas de chuva na hora, em mm"),
    ("weathercode", "codigo_tempo", "INT", "Código de condição do tempo (padrão WMO)"),
    ("pressure_msl", "pressao_nivel_mar_hpa", "DOUBLE", "Pressão ao nível do mar, em hPa"),
    ("surface_pressure", "pressao_superficie_hpa", "DOUBLE", "Pressão na superfície, em hPa"),
    ("cloud_cover", "cobertura_nuvens_pct", "DOUBLE", "Cobertura total de nuvens, em %"),
    ("cloud_cover_low", "nuvens_baixas_pct", "DOUBLE", "Cobertura de nuvens baixas, em %"),
    ("cloud_cover_mid", "nuvens_medias_pct", "DOUBLE", "Cobertura de nuvens médias, em %"),
    ("cloud_cover_high", "nuvens_altas_pct", "DOUBLE", "Cobertura de nuvens altas, em %"),
    ("wind_speed_10m", "vento_velocidade_kmh", "DOUBLE", "Velocidade do vento a 10 m, em km/h"),
    ("wind_direction_10m", "vento_direcao_graus", "DOUBLE", "Direção do vento a 10 m, em graus"),
    ("wind_gusts_10m", "vento_rajada_kmh", "DOUBLE", "Rajada de vento a 10 m, em km/h"),
    ("shortwave_radiation", "radiacao_solar_wm2", "DOUBLE", "Radiação solar de onda curta, em W/m²"),
    ("uv_index", "indice_uv", "DOUBLE", "Índice UV"),
    ("visibility", "visibilidade_m", "DOUBLE", "Visibilidade, em metros"),
    ("is_day", "periodo_diurno", "BOOLEAN", "Verdadeiro entre o nascer e o pôr do sol"),
]

ESQUEMA_RESPOSTA = (
    "latitude DOUBLE, longitude DOUBLE, hourly STRUCT<time: ARRAY<STRING>, "
    + ", ".join(f"{api}: ARRAY<DOUBLE>" for api, _, _, _ in VARIAVEIS)
    + ">"
)

COLUNAS_HORA = [("data_hora", "TIMESTAMP_NTZ", "Hora local de Fortaleza (sem fuso)")]
COLUNAS_VARIAVEIS = [(coluna, tipo, comentario) for _, coluna, tipo, comentario in VARIAVEIS]
COLUNAS_GRADE = [
    ("latitude_grade", "DOUBLE", "Latitude do ponto de grade usado pela API (o mais próximo do pedido)"),
    ("longitude_grade", "DOUBLE", "Longitude do ponto de grade usado pela API"),
]

REGRAS_DESCARTE = {
    "hora_presente": "data_hora IS NOT NULL",
}
# Faixas plausíveis para Fortaleza: fora delas, o valor é suspeito.
REGRAS_ALERTA = {
    "temperatura_plausivel": "temperatura_c BETWEEN 15 AND 45",
    "umidade_valida": "umidade_relativa_pct BETWEEN 0 AND 100",
    "radiacao_nao_negativa": "radiacao_solar_wm2 >= 0",
    "vento_nao_negativo": "vento_velocidade_kmh >= 0",
}


def horas_da_resposta(bronze: DataFrame, colunas_contexto) -> DataFrame:
    """Uma linha por hora da resposta, com uma coluna por variável.

    `colunas_contexto`: colunas (Column) calculadas a partir da linha da bronze, repetidas
    em todas as horas daquela resposta.
    """
    resposta = F.from_json("raw_response", ESQUEMA_RESPOSTA)
    por_resposta = bronze.select(*colunas_contexto, F.col("ingestion_timestamp"), resposta.alias("r"))
    por_hora = por_resposta.select("*", F.posexplode("r.hourly.time").alias("posicao", "hora"))

    def valor(api, tipo):
        bruto = F.get(F.col(f"r.hourly.{api}"), F.col("posicao"))
        return (bruto == 1) if tipo == "BOOLEAN" else bruto.cast(tipo.lower())

    return por_hora.select(
        *[F.col(c) for c in por_resposta.columns if c not in ("r", "ingestion_timestamp")],
        F.to_timestamp_ntz("hora", F.lit("yyyy-MM-dd'T'HH:mm")).alias("data_hora"),
        *[valor(api, tipo).alias(coluna) for api, coluna, tipo, _ in VARIAVEIS],
        F.col("r.latitude").alias("latitude_grade"),
        F.col("r.longitude").alias("longitude_grade"),
        F.col("ingestion_timestamp").alias(SEQUENCIA),
    )


# --- Previsão: uma emissão por dia, prevendo o dia seguinte -------------------------------

COLUNAS_PREVISAO = [
    ("data_emissao", "DATE", "Dia em que a previsão foi emitida (coletada)"),
    ("data_prevista", "DATE", "Dia que a previsão descreve"),
    ("horizonte_dias", "INT", "Dias entre a emissão e o dia previsto (0 = o próprio dia)"),
] + COLUNAS_HORA + COLUNAS_VARIAVEIS + COLUNAS_GRADE
CHAVE_PREVISAO = ["data_emissao", "data_hora"]


def _coluna_ou_nulo(bronze, nome):
    return F.col(nome) if nome in bronze.columns else F.lit(None)


def padronizar_clima_previsao(bronze: DataFrame) -> DataFrame:
    # Linhas anteriores a 28/09/2026 não têm data_prevista/horizonte_dias: previam o próprio dia.
    emissao = F.to_date("data_referencia")
    return horas_da_resposta(bronze, [
        emissao.alias("data_emissao"),
        F.coalesce(F.to_date(_coluna_ou_nulo(bronze, "data_prevista")), emissao).alias("data_prevista"),
        F.coalesce(_coluna_ou_nulo(bronze, "horizonte_dias").cast("int"), F.lit(0)).alias("horizonte_dias"),
    ])


# --- Observado: janelas semanais; a mais recente vence onde houver sobreposição ------------

COLUNAS_OBSERVADO = COLUNAS_HORA + COLUNAS_VARIAVEIS + COLUNAS_GRADE
CHAVE_OBSERVADO = ["data_hora"]


def padronizar_clima_observado(bronze: DataFrame) -> DataFrame:
    return horas_da_resposta(bronze, [])
