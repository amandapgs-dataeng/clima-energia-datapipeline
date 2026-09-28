"""Silver da geração por usina: 1 linha = 1 usina x 1 hora (todas as fontes, todo o Brasil)."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utilities.historico import SEQUENCIA
from utilities.vocabulario import (
    codigo_subsistema,
    hora_local_ons,
    modalidade_operacao,
    nome_subsistema,
    sem_codigo,
    texto_limpo,
    tipo_usina,
)

COLUNAS = [
    ("data_hora", "TIMESTAMP_NTZ", "Hora da medição, horário de Brasília (sem fuso)"),
    ("chave_usina", "STRING", "Identificador da usina na silver: CEG + nome (o CEG sozinho não é único)"),
    ("nome_usina", "STRING", "Nome da usina ou do conjunto de usinas"),
    ("ceg", "STRING", "Código ANEEL do empreendimento; nulo em usinas agregadas"),
    ("id_ons", "STRING", "Código da usina no ONS; nulo em pequenas usinas"),
    ("tipo_usina", "STRING", "Fonte: Hidrelétrica, Térmica, Eólica, Solar ou Nuclear"),
    ("combustivel", "STRING", "Combustível (usinas térmicas)"),
    ("modalidade_operacao", "STRING", "Modalidade de operação no ONS (Tipo I, Tipo II-A/B/C, Tipo III, conjuntos, pequenas usinas)"),
    ("id_subsistema", "STRING", "Código do subsistema: N, NE, S, SE"),
    ("subsistema", "STRING", "Nome padronizado do subsistema"),
    ("uf", "STRING", "Sigla do estado"),
    ("geracao_mwmed", "DOUBLE", "Geração na hora, em MW médios; nulo quando o ONS não mede a usina hora a hora"),
]
CHAVE = ["data_hora", "chave_usina"]
COLUNAS_VERSIONADAS = ["geracao_mwmed"]

REGRAS_DESCARTE = {
    "chave_presente": "data_hora IS NOT NULL AND nome_usina IS NOT NULL",
    "subsistema_conhecido": "subsistema IS NOT NULL",
}
REGRAS_ALERTA = {
    "tipo_usina_conhecido": "tipo_usina IS NOT NULL",
    # Pequenos negativos são o consumo interno de usinas paradas; abaixo disso é suspeito.
    "geracao_plausivel": "geracao_mwmed IS NULL OR geracao_mwmed >= -10",
}


def padronizar_geracao_usina(bronze: DataFrame) -> DataFrame:
    codigo = codigo_subsistema("id_subsistema")
    return bronze.select(
        hora_local_ons("din_instante").alias("data_hora"),
        F.concat_ws(" | ", F.coalesce(F.trim("ceg"), F.lit("-")), F.trim("nom_usina")).alias("chave_usina"),
        texto_limpo("nom_usina").alias("nome_usina"),
        sem_codigo("ceg").alias("ceg"),
        sem_codigo("id_ons").alias("id_ons"),
        tipo_usina("nom_tipousina").alias("tipo_usina"),
        texto_limpo("nom_tipocombustivel").alias("combustivel"),
        modalidade_operacao("cod_modalidadeoperacao").alias("modalidade_operacao"),
        codigo.alias("id_subsistema"),
        nome_subsistema(codigo).alias("subsistema"),
        F.upper(F.trim("id_estado")).alias("uf"),
        F.col("val_geracao").cast("double").alias("geracao_mwmed"),
        F.col("ingestion_timestamp").alias(SEQUENCIA),
    )
