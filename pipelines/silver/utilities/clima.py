"""Silver do clima (Open-Meteo): cada resposta JSON da bronze vira uma linha por ponto e hora.

Pontos de coleta: centros das usinas eólicas e solares de cada estado do Nordeste e as
capitais (ver notebooks/utils/pontos_clima.py).
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utilities.padrao_silver import SEQUENCIA, TabelaSilver

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
    ("wind_speed_10m", "vento_velocidade_10m_kmh", "DOUBLE", "Velocidade do vento a 10 m, em km/h"),
    ("wind_direction_10m", "vento_direcao_10m_graus", "DOUBLE", "Direção do vento a 10 m, em graus"),
    ("wind_gusts_10m", "vento_rajada_10m_kmh", "DOUBLE", "Rajada de vento a 10 m, em km/h"),
    ("wind_speed_100m", "vento_velocidade_100m_kmh", "DOUBLE", "Velocidade do vento a 100 m (altura aproximada do rotor), em km/h"),
    ("wind_direction_100m", "vento_direcao_100m_graus", "DOUBLE", "Direção do vento a 100 m, em graus"),
    ("shortwave_radiation", "radiacao_solar_wm2", "DOUBLE", "Radiação solar de onda curta, em W/m²"),
    ("uv_index", "indice_uv", "DOUBLE", "Índice UV"),
    ("visibility", "visibilidade_m", "DOUBLE", "Visibilidade, em metros"),
    ("is_day", "periodo_diurno", "BOOLEAN", "Verdadeiro entre o nascer e o pôr do sol"),
]

# Coletas anteriores a 29/09/2026 eram só de Fortaleza e não identificavam o ponto.
PONTO_LEGADO = ("fortaleza", "CE", "capital")

COLUNAS_PONTO = [
    ("ponto_id", "STRING", "Ponto de coleta: centro das usinas de uma fonte num estado, ou uma capital"),
    ("uf", "STRING", "Sigla do estado do ponto"),
    ("tipo_ponto", "STRING", "eolica, solar ou capital"),
]
COLUNAS_HORA = [("data_hora", "TIMESTAMP_NTZ", "Hora local do Nordeste, UTC-3 (sem fuso)")]
COLUNAS_VARIAVEIS = [(coluna, tipo, comentario) for _, coluna, tipo, comentario in VARIAVEIS]
COLUNAS_GRADE = [
    ("latitude_grade", "DOUBLE", "Latitude do ponto de grade usado pela API (o mais próximo do pedido)"),
    ("longitude_grade", "DOUBLE", "Longitude do ponto de grade usado pela API"),
]

REGRAS_DESCARTE = {
    "chave_presente": "ponto_id IS NOT NULL AND data_hora IS NOT NULL",
}
# Faixas plausíveis para o Nordeste: fora delas, o valor é suspeito.
REGRAS_ALERTA = {
    "temperatura_plausivel": "temperatura_c BETWEEN 5 AND 48",
    "umidade_valida": "umidade_relativa_pct BETWEEN 0 AND 100",
    "radiacao_nao_negativa": "radiacao_solar_wm2 >= 0",
    "vento_nao_negativo": "vento_velocidade_100m_kmh >= 0",
}


def _coluna_ou_nulo(bronze, nome):
    return F.col(nome) if nome in bronze.columns else F.lit(None)


def colunas_do_ponto(bronze: DataFrame):
    ponto_id, uf, tipo = PONTO_LEGADO
    return [
        F.coalesce(_coluna_ou_nulo(bronze, "ponto_id"), F.lit(ponto_id)).alias("ponto_id"),
        F.coalesce(_coluna_ou_nulo(bronze, "uf"), F.lit(uf)).alias("uf"),
        F.coalesce(_coluna_ou_nulo(bronze, "tipo_ponto"), F.lit(tipo)).alias("tipo_ponto"),
    ]


def esquema_resposta(sufixo=""):
    variaveis = ", ".join(f"{api}{sufixo}: ARRAY<DOUBLE>" for api, _, _, _ in VARIAVEIS)
    return f"latitude DOUBLE, longitude DOUBLE, hourly STRUCT<time: ARRAY<STRING>, {variaveis}>"


def horas_da_resposta(bronze: DataFrame, colunas_contexto, sufixo="") -> DataFrame:
    """Uma linha por hora da resposta, com uma coluna por variável.

    `colunas_contexto`: colunas calculadas a partir da linha da bronze, repetidas em todas as
    horas daquela resposta. `sufixo`: sufixo dos nomes das variáveis na API (ex.: _previous_day1).
    """
    resposta = F.from_json("raw_response", esquema_resposta(sufixo))
    por_resposta = bronze.select(*colunas_contexto, F.col("ingestion_timestamp"), resposta.alias("r"))
    por_hora = por_resposta.select("*", F.posexplode("r.hourly.time").alias("posicao", "hora"))

    def valor(api, tipo):
        bruto = F.get(F.col(f"r.hourly.{api}{sufixo}"), F.col("posicao"))
        return (bruto == 1) if tipo == "BOOLEAN" else bruto.cast(tipo.lower())

    return por_hora.select(
        *[F.col(c) for c in por_resposta.columns if c not in ("r", "ingestion_timestamp")],
        F.to_timestamp_ntz("hora", F.lit("yyyy-MM-dd'T'HH:mm")).alias("data_hora"),
        *[valor(api, tipo).alias(coluna) for api, coluna, tipo, _ in VARIAVEIS],
        F.col("r.latitude").alias("latitude_grade"),
        F.col("r.longitude").alias("longitude_grade"),
        F.col("ingestion_timestamp").alias(SEQUENCIA),
    )


# --- Previsão -------------------------------------------------------------------------------
# Duas origens: a coleta diária (emitida às 21h para o dia seguinte) e a API de previsões
# anteriores (previsões D+1 de dias passados, usada no reprocessamento).

COLUNAS_PREVISAO = COLUNAS_PONTO + [
    ("data_emissao", "DATE", "Dia em que a previsão foi emitida"),
    ("data_prevista", "DATE", "Dia que a previsão descreve"),
    ("horizonte_dias", "INT", "Dias entre a emissão e o dia previsto (0 = o próprio dia)"),
    ("origem", "STRING", "coleta_diaria ou previsoes_anteriores (reprocessamento via API de previsões anteriores)"),
] + COLUNAS_HORA + COLUNAS_VARIAVEIS + COLUNAS_GRADE
CHAVE_PREVISAO = ["ponto_id", "data_emissao", "data_hora"]


def _previsao_coleta_diaria(bronze: DataFrame) -> DataFrame:
    # Linhas anteriores a 28/09/2026 não têm data_prevista/horizonte_dias: previam o próprio dia.
    emissao = F.to_date("data_referencia")
    return horas_da_resposta(bronze, colunas_do_ponto(bronze) + [
        emissao.alias("data_emissao"),
        F.coalesce(F.to_date(_coluna_ou_nulo(bronze, "data_prevista")), emissao).alias("data_prevista"),
        F.coalesce(_coluna_ou_nulo(bronze, "horizonte_dias").cast("int"), F.lit(0)).alias("horizonte_dias"),
        F.lit("coleta_diaria").alias("origem"),
    ])


def _previsao_anterior(bronze: DataFrame, horizonte=1) -> DataFrame:
    # A resposta cobre um período; cada hora foi prevista `horizonte` dias antes do seu dia.
    horas = horas_da_resposta(bronze, colunas_do_ponto(bronze) + [
        F.lit(None).cast("date").alias("data_emissao"),
        F.lit(None).cast("date").alias("data_prevista"),
        F.lit(horizonte).alias("horizonte_dias"),
        F.lit("previsoes_anteriores").alias("origem"),
    ], sufixo=f"_previous_day{horizonte}")
    dia = F.to_date("data_hora")
    return (
        horas.withColumn("data_prevista", dia)
        .withColumn("data_emissao", F.date_sub(dia, horizonte))
    )


def padronizar_clima_previsao(coleta_diaria: DataFrame, previsoes_anteriores: DataFrame) -> DataFrame:
    return _previsao_coleta_diaria(coleta_diaria).unionByName(_previsao_anterior(previsoes_anteriores))


# --- Observado ------------------------------------------------------------------------------
# Janelas de coleta podem se sobrepor: a ingestão mais recente vence.

COLUNAS_OBSERVADO = COLUNAS_PONTO + COLUNAS_HORA + COLUNAS_VARIAVEIS + COLUNAS_GRADE
CHAVE_OBSERVADO = ["ponto_id", "data_hora"]


def padronizar_clima_observado(bronze: DataFrame) -> DataFrame:
    return horas_da_resposta(bronze, colunas_do_ponto(bronze))


TABELA_PREVISAO = TabelaSilver(
    nome="clima_previsao",
    fonte="open_meteo",
    descricao="Previsão horária do tempo nos pontos das usinas e nas capitais do Nordeste, por dia de emissão",
    tabelas_bronze=("open_meteo.previsao_bruta", "open_meteo.previsao_historica_bruta"),
    padronizar=padronizar_clima_previsao,
    colunas=COLUNAS_PREVISAO,
    chave=CHAVE_PREVISAO,
    regras_descarte={**REGRAS_DESCARTE, "emissao_presente": "data_emissao IS NOT NULL"},
    regras_alerta=REGRAS_ALERTA,
)

TABELA_OBSERVADO = TabelaSilver(
    nome="clima_observado",
    fonte="open_meteo",
    descricao="Tempo observado hora a hora nos pontos das usinas e nas capitais do Nordeste",
    tabelas_bronze=("open_meteo.historico_observado",),
    padronizar=padronizar_clima_observado,
    colunas=COLUNAS_OBSERVADO,
    chave=CHAVE_OBSERVADO,
    regras_descarte=REGRAS_DESCARTE,
    regras_alerta=REGRAS_ALERTA,
)
