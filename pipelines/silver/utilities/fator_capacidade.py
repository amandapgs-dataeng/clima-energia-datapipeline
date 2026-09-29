"""Silver do fator de capacidade: 1 linha = 1 usina/conjunto eólico ou solar x 1 hora.

Conjuntos híbridos (eólico + solar) têm uma linha por tipo; id_ons distingue as duas.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utilities.padrao_silver import SEQUENCIA, TabelaSilver
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
    ("id_ons", "STRING", "Código da usina ou conjunto no ONS (distingue as partes de um conjunto híbrido)"),
    ("nome_usina", "STRING", "Nome da usina ou do conjunto de usinas"),
    ("ceg", "STRING", "Código ANEEL do empreendimento; nulo em conjuntos"),
    ("tipo_usina", "STRING", "Fonte: Eólica ou Solar"),
    ("modalidade_operacao", "STRING", "Modalidade de operação no ONS"),
    ("id_subsistema", "STRING", "Código do subsistema: N, NE, S, SE"),
    ("subsistema", "STRING", "Nome padronizado do subsistema"),
    ("uf", "STRING", "Sigla do estado"),
    ("codigo_ponto_conexao", "STRING", "Código do ponto de conexão à rede"),
    ("nome_ponto_conexao", "STRING", "Nome do ponto de conexão à rede"),
    ("localizacao", "STRING", "Localização informada pelo ONS"),
    ("latitude_coletora", "DOUBLE", "Latitude da subestação coletora"),
    ("longitude_coletora", "DOUBLE", "Longitude da subestação coletora"),
    ("latitude_ponto_conexao", "DOUBLE", "Latitude do ponto de conexão"),
    ("longitude_ponto_conexao", "DOUBLE", "Longitude do ponto de conexão"),
    ("geracao_programada_mwmed", "DOUBLE", "Geração programada na hora, em MW médios"),
    ("geracao_verificada_mwmed", "DOUBLE", "Geração verificada na hora, em MW médios"),
    ("capacidade_instalada_mw", "DOUBLE", "Capacidade instalada, em MW"),
    ("fator_capacidade", "DOUBLE", "Geração verificada dividida pela capacidade instalada (0 a 1)"),
]
CHAVE = ["data_hora", "id_ons"]
COLUNAS_VERSIONADAS = [
    "geracao_programada_mwmed",
    "geracao_verificada_mwmed",
    "capacidade_instalada_mw",
    "fator_capacidade",
]

REGRAS_DESCARTE = {
    "chave_presente": "data_hora IS NOT NULL AND id_ons IS NOT NULL",
    "subsistema_conhecido": "subsistema IS NOT NULL",
}
REGRAS_ALERTA = {
    "tipo_usina_conhecido": "tipo_usina IN ('Eólica', 'Solar')",
    # Os valores chegam como texto: nulo aqui significa que o texto não era um número.
    "valores_numericos": "geracao_verificada_mwmed IS NOT NULL AND capacidade_instalada_mw IS NOT NULL",
    # Levemente negativo é consumo interno à noite; acima de 1 seria gerar além da capacidade.
    "fator_plausivel": "fator_capacidade BETWEEN -0.05 AND 1",
}


def padronizar_fator_capacidade(bronze: DataFrame) -> DataFrame:
    codigo = codigo_subsistema("id_subsistema")
    return bronze.select(
        hora_local_ons("din_instante").alias("data_hora"),
        sem_codigo("id_ons").alias("id_ons"),
        texto_limpo("nom_usina_conjunto").alias("nome_usina"),
        sem_codigo("ceg").alias("ceg"),
        tipo_usina("nom_tipousina").alias("tipo_usina"),
        modalidade_operacao("nom_modalidadeoperacao").alias("modalidade_operacao"),
        codigo.alias("id_subsistema"),
        nome_subsistema(codigo).alias("subsistema"),
        F.upper(F.trim("id_estado")).alias("uf"),
        texto_limpo("cod_pontoconexao").alias("codigo_ponto_conexao"),
        texto_limpo("nom_pontoconexao").alias("nome_ponto_conexao"),
        texto_limpo("nom_localizacao").alias("localizacao"),
        F.col("val_latitudesecoletora").cast("double").alias("latitude_coletora"),
        F.col("val_longitudesecoletora").cast("double").alias("longitude_coletora"),
        F.col("val_latitudepontoconexao").cast("double").alias("latitude_ponto_conexao"),
        F.col("val_longitudepontoconexao").cast("double").alias("longitude_ponto_conexao"),
        F.col("val_geracaoprogramada").try_cast("double").alias("geracao_programada_mwmed"),
        F.col("val_geracaoverificada").try_cast("double").alias("geracao_verificada_mwmed"),
        F.col("val_capacidadeinstalada").try_cast("double").alias("capacidade_instalada_mw"),
        F.col("val_fatorcapacidade").cast("double").alias("fator_capacidade"),
        F.col("ingestion_timestamp").alias(SEQUENCIA),
    )


TABELA = TabelaSilver(
    nome="fator_capacidade",
    fonte="ons",
    descricao="Fator de capacidade horário de usinas e conjuntos eólicos e solares",
    tabelas_bronze=("ons.fator_capacidade",),
    padronizar=padronizar_fator_capacidade,
    colunas=COLUNAS,
    chave=CHAVE,
    regras_descarte=REGRAS_DESCARTE,
    regras_alerta=REGRAS_ALERTA,
    colunas_versionadas=COLUNAS_VERSIONADAS,
)
