"""Silver da carga de energia: 1 linha = 1 subsistema x 1 dia."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utilities.historico import SEQUENCIA
from utilities.vocabulario import codigo_subsistema, nome_subsistema

COLUNAS = [
    ("id_subsistema", "STRING", "Código do subsistema: N, NE, S, SE"),
    ("subsistema", "STRING", "Nome padronizado do subsistema"),
    ("data", "DATE", "Dia da medição"),
    ("carga_mwmed", "DOUBLE", "Carga média do dia, em MW médios"),
]
CHAVE = ["id_subsistema", "data"]
COLUNAS_VERSIONADAS = ["carga_mwmed"]

REGRAS_DESCARTE = {
    "chave_presente": "id_subsistema IS NOT NULL AND data IS NOT NULL",
    "subsistema_conhecido": "subsistema IS NOT NULL",
    "carga_presente": "carga_mwmed IS NOT NULL",
}
REGRAS_ALERTA = {
    "carga_positiva": "carga_mwmed > 0",
}


def padronizar_carga_energia(bronze: DataFrame) -> DataFrame:
    codigo = codigo_subsistema("id_subsistema")
    return bronze.select(
        codigo.alias("id_subsistema"),
        nome_subsistema(codigo).alias("subsistema"),
        F.to_date("din_instante").alias("data"),  # 00:00 local rotulado como UTC -> o próprio dia
        F.col("val_cargaenergiamwmed").cast("double").alias("carga_mwmed"),
        F.col("ingestion_timestamp").alias(SEQUENCIA),
    )
