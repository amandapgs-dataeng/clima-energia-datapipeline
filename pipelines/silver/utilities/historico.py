"""Padrão das tabelas da silver: versão atual, e opcionalmente o histórico de versões.

Para uma tabela `x`:
    x                  versão atual de cada chave (AUTO CDC, SCD tipo 1)
    x_historico_cdc    histórico técnico (AUTO CDC, SCD tipo 2; uso interno)
    x_versoes          o histórico com nomes de negócio, para consulta
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# Coluna que ordena as versões: o valor ingerido por último vence.
SEQUENCIA = "ingerido_em"
COLUNA_SEQUENCIA = (SEQUENCIA, "TIMESTAMP", "Quando o valor foi ingerido pela última vez (UTC)")

COLUNAS_CDC = [
    ("__START_AT", "TIMESTAMP", "Início da vigência da versão (coluna técnica do CDC)"),
    ("__END_AT", "TIMESTAMP", "Fim da vigência da versão; nulo = versão atual (coluna técnica do CDC)"),
]

COLUNAS_VERSAO = [
    ("numero_versao", "INT", "Ordem da versão para a chave (1 = primeira publicação)"),
    ("vigente_desde", "TIMESTAMP", "Quando esta versão passou a valer (ingestão em que o valor apareceu)"),
    ("vigente_ate", "TIMESTAMP", "Quando esta versão foi substituída; nulo se ainda é a atual"),
    ("versao_atual", "BOOLEAN", "Verdadeiro se esta é a versão vigente"),
    ("confirmado_em", "TIMESTAMP", "Última ingestão que trouxe este mesmo valor (UTC)"),
]


def ddl(colunas):
    """Schema em DDL a partir de (nome, tipo, comentário)."""
    return ",\n".join(f"{nome} {tipo} COMMENT '{comentario}'" for nome, tipo, comentario in colunas)


def nomes(colunas):
    return [nome for nome, _, _ in colunas]


def versoes_legiveis(historico_cdc: DataFrame, chave) -> DataFrame:
    """Histórico com nomes de negócio no lugar das colunas técnicas do CDC (__START_AT/__END_AT)."""
    tecnicas = {"__START_AT", "__END_AT", SEQUENCIA}
    dados = [coluna for coluna in historico_cdc.columns if coluna not in tecnicas]
    ordem = Window.partitionBy(*chave).orderBy("__START_AT")
    return historico_cdc.select(
        *dados,
        F.row_number().over(ordem).alias("numero_versao"),
        F.col("__START_AT").alias("vigente_desde"),
        F.col("__END_AT").alias("vigente_ate"),
        F.col("__END_AT").isNull().alias("versao_atual"),
        F.col(SEQUENCIA).alias("confirmado_em"),
    )


def declarar_tabelas(dp, spark, *, nome, origem, chave, colunas, descricao, propriedades, colunas_versionadas=None):
    """Declara no pipeline a tabela atual e, se houver colunas versionadas, o histórico.

    `colunas`: colunas de dados como (nome, tipo, comentário), na ordem em que a view
    `origem` as produz, sem a coluna de sequência (adicionada aqui).
    """
    dp.create_streaming_table(
        name=nome,
        comment=f"{descricao}: versão atual",
        schema=ddl(colunas + [COLUNA_SEQUENCIA]),
        table_properties=propriedades,
    )
    dp.create_auto_cdc_flow(target=nome, source=origem, keys=chave, sequence_by=SEQUENCIA, stored_as_scd_type=1)

    if not colunas_versionadas:
        return

    tabela_cdc = f"{nome}_historico_cdc"
    dp.create_streaming_table(
        name=tabela_cdc,
        comment=f"Tabela técnica do CDC (SCD tipo 2). Para consultar o histórico, use {nome}_versoes.",
        schema=ddl(colunas + [COLUNA_SEQUENCIA] + COLUNAS_CDC),
        table_properties={**propriedades, "uso": "interno"},
    )
    # Só mudanças nestas colunas criam versão: releituras idênticas (retries, janelas
    # que se sobrepõem) não geram versões; revisões da fonte geram.
    dp.create_auto_cdc_flow(
        target=tabela_cdc,
        source=origem,
        keys=chave,
        sequence_by=SEQUENCIA,
        stored_as_scd_type=2,
        track_history_column_list=colunas_versionadas,
    )

    @dp.materialized_view(
        name=f"{nome}_versoes",
        comment=f"{descricao}: histórico de versões (cada revisão da fonte é uma versão)",
        schema=ddl(colunas + COLUNAS_VERSAO),
        table_properties=propriedades,
    )
    def versoes():
        return versoes_legiveis(spark.read.table(tabela_cdc), chave)
