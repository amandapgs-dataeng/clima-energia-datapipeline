"""Compatibilização de schema entre versões de um arquivo da fonte e a tabela da bronze.

O ONS já mudou o tipo de colunas ao longo do tempo (ex.: val_geracao era texto, como
"58.91700000", "0E-8" ou vazio, e passou a número). A bronze mantém um tipo por coluna: o
valor que chega com outro tipo é convertido, desde que a conversão não perca informação.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class ConversaoComPerda(Exception):
    """Um valor da fonte não pôde ser convertido para o tipo da coluna na bronze."""


def colunas_com_tipo_diferente(df: DataFrame, schema_destino):
    """{coluna: tipo no destino} para as colunas que existem nos dois lados com tipos diferentes."""
    destino = {campo.name: campo.dataType for campo in schema_destino}
    return {
        campo.name: destino[campo.name]
        for campo in df.schema
        if campo.name in destino and campo.dataType != destino[campo.name]
    }


def alinhar_ao_schema(df: DataFrame, schema_destino) -> DataFrame:
    """Converte as colunas de `df` para os tipos de `schema_destino`.

    Vazio vira nulo (é ausência de valor). Qualquer outro valor que não converta interrompe a
    gravação com ConversaoComPerda, em vez de virar nulo em silêncio.
    """
    conversoes = colunas_com_tipo_diferente(df, schema_destino)
    if not conversoes:
        return df

    convertidas = {coluna: F.col(coluna).try_cast(tipo) for coluna, tipo in conversoes.items()}
    perdas = df.select(*[
        F.sum(
            (F.col(coluna).isNotNull() & (F.trim(F.col(coluna).cast("string")) != "") & convertida.isNull()).cast("int")
        ).alias(coluna)
        for coluna, convertida in convertidas.items()
    ]).first().asDict()

    perdidas = {coluna: quantidade for coluna, quantidade in perdas.items() if quantidade}
    if perdidas:
        raise ConversaoComPerda(f"Valores que não convertem para o tipo da bronze: {perdidas}")

    for coluna, convertida in convertidas.items():
        df = df.withColumn(coluna, convertida)
    return df
