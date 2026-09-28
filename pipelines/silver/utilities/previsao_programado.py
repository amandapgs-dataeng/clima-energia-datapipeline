"""Silver da previsão x programação do ONS: 1 linha = 1 usina eólica/solar x 1 meia hora."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utilities.historico import SEQUENCIA
from utilities.vocabulario import texto_limpo

COLUNAS = [
    ("data", "DATE", "Dia da programação"),
    ("patamar", "INT", "Meia hora do dia, de 1 (00:00) a 48 (23:30)"),
    ("inicio_patamar", "TIMESTAMP_NTZ", "Início da meia hora, horário de Brasília (sem fuso)"),
    ("codigo_usina", "STRING", "Código da usina na programação do ONS (não corresponde a CEG nem a id_ons)"),
    ("nome_usina", "STRING", "Nome da usina na programação do ONS"),
    ("previsao_mwmed", "DOUBLE", "Geração prevista pelo ONS para a meia hora, em MW médios"),
    ("programado_mwmed", "DOUBLE", "Geração programada pelo ONS para a meia hora, em MW médios"),
]
CHAVE = ["data", "patamar", "codigo_usina"]
COLUNAS_VERSIONADAS = ["previsao_mwmed", "programado_mwmed"]

REGRAS_DESCARTE = {
    "chave_presente": "data IS NOT NULL AND codigo_usina IS NOT NULL",
    "patamar_valido": "patamar BETWEEN 1 AND 48",
}
REGRAS_ALERTA = {
    # Os valores chegam como texto: nulo aqui significa que o texto não era um número.
    "valores_numericos": "previsao_mwmed IS NOT NULL AND programado_mwmed IS NOT NULL",
    "valores_nao_negativos": "previsao_mwmed >= 0 AND programado_mwmed >= 0",
}


def padronizar_previsao_programado(bronze: DataFrame) -> DataFrame:
    data = F.to_date("dat_programacao", "yyyyMMdd")
    patamar = F.col("num_patamar").cast("int")
    minutos = (patamar - 1) * 30
    return bronze.select(
        data.alias("data"),
        patamar.alias("patamar"),
        F.make_timestamp_ntz(
            F.year(data), F.month(data), F.dayofmonth(data), F.floor(minutos / 60).cast("int"), (minutos % 60).cast("int"), F.lit(0)
        ).alias("inicio_patamar"),
        texto_limpo("cod_usinapdp").alias("codigo_usina"),  # campos de tamanho fixo: vêm com espaços
        texto_limpo("nom_usinapdp").alias("nome_usina"),
        F.col("val_previsao").try_cast("double").alias("previsao_mwmed"),
        F.col("val_programado").try_cast("double").alias("programado_mwmed"),
        F.col("ingestion_timestamp").alias(SEQUENCIA),
    )
