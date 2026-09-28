"""Regras da silver de carga de energia (funções puras, testadas sem o Databricks)."""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from utilities.vocabulario import codigo_subsistema, nome_subsistema

# 1 linha = 1 subsistema x 1 dia.
CHAVE = ["id_subsistema", "data"]

# Coluna que ordena as versões: a mais recente ingerida vence.
SEQUENCIA = "ingerido_em"

# Só uma mudança nestas colunas cria uma nova versão no histórico: releituras idênticas
# (retries, a janela semanal de 60 dias) não geram versões; revisões do ONS geram.
COLUNAS_VERSIONADAS = ["carga_mwmed"]

# Registros que violam estas regras são descartados (e contados nas métricas do pipeline).
REGRAS_DESCARTE = {
    "chave_presente": "id_subsistema IS NOT NULL AND data IS NOT NULL",
    "subsistema_conhecido": "subsistema IS NOT NULL",
    "carga_presente": "carga_mwmed IS NOT NULL",
}

# Registros que violam estas regras seguem, mas a violação fica registrada.
REGRAS_ALERTA = {
    "carga_positiva": "carga_mwmed > 0",
}


def padronizar_carga_energia(bronze: DataFrame) -> DataFrame:
    """Colunas tipadas e com nomes padronizados a partir da bronze.

    `din_instante` é horário local rotulado como UTC (ver docs/dicionario_dados.md);
    com a sessão em UTC, `to_date` devolve o dia local correto.
    """
    codigo = codigo_subsistema("id_subsistema")
    return bronze.select(
        codigo.alias("id_subsistema"),
        nome_subsistema(codigo).alias("subsistema"),
        F.to_date("din_instante").alias("data"),
        F.col("val_cargaenergiamwmed").cast("double").alias("carga_mwmed"),
        F.col("ingestion_timestamp").alias(SEQUENCIA),
    )


def versoes_legiveis(historico_cdc: DataFrame) -> DataFrame:
    """Histórico com nomes de negócio no lugar das colunas técnicas do CDC (__START_AT/__END_AT)."""
    ordem = Window.partitionBy(*CHAVE).orderBy("__START_AT")
    return historico_cdc.select(
        *CHAVE,
        "subsistema",
        "carga_mwmed",
        F.row_number().over(ordem).alias("numero_versao"),
        F.col("__START_AT").alias("vigente_desde"),
        F.col("__END_AT").alias("vigente_ate"),
        F.col("__END_AT").isNull().alias("versao_atual"),
        F.col(SEQUENCIA).alias("confirmado_em"),
    )
