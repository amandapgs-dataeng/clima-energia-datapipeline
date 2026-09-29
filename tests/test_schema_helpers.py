import pytest
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from utils.schema_helpers import ConversaoComPerda, alinhar_ao_schema, colunas_com_tipo_diferente

DESTINO = StructType([StructField("usina", StringType()), StructField("val_geracao", DoubleType())])


def arquivo_antigo(spark, valores):
    # Até 2025, o ONS publicava val_geracao como texto.
    return spark.createDataFrame([(f"u{i}", v) for i, v in enumerate(valores)], "usina string, val_geracao string")


def test_detecta_colunas_com_tipo_diferente(spark):
    assert colunas_com_tipo_diferente(arquivo_antigo(spark, ["1.0"]), DESTINO) == {"val_geracao": DoubleType()}


def test_texto_numerico_vira_numero(spark):
    df = alinhar_ao_schema(arquivo_antigo(spark, ["58.91700000", "0E-8", "-1.4155"]), DESTINO)
    assert df.schema["val_geracao"].dataType == DoubleType()
    assert [r.val_geracao for r in df.orderBy("usina").collect()] == [58.917, 0.0, -1.4155]


def test_vazio_vira_nulo(spark):
    df = alinhar_ao_schema(arquivo_antigo(spark, ["", "  ", None, "10"]), DESTINO)
    assert [r.val_geracao for r in df.orderBy("usina").collect()] == [None, None, None, 10.0]


@pytest.mark.parametrize("valor", ["12,5", "n/d", "abc"])
def test_valor_que_nao_converte_interrompe_em_vez_de_perder_dado(spark, valor):
    with pytest.raises(ConversaoComPerda, match="val_geracao"):
        alinhar_ao_schema(arquivo_antigo(spark, ["1.0", valor]), DESTINO)


def test_schema_igual_nao_mexe(spark):
    df = spark.createDataFrame([("u", 1.0)], "usina string, val_geracao double")
    assert alinhar_ao_schema(df, DESTINO) is df


def test_coluna_nova_na_fonte_passa_intacta(spark):
    df = spark.createDataFrame([("u", "1.0", "x")], "usina string, val_geracao string, coluna_nova string")
    assert alinhar_ao_schema(df, DESTINO).columns == ["usina", "val_geracao", "coluna_nova"]
