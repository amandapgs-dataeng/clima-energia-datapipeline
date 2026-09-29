import subprocess

import pytest

# Schemas da bronze (como estão em clima_energia_bronze), para montar dados de teste.
SCHEMAS_BRONZE = {
    "carga_energia": "id_subsistema string, nom_subsistema string, din_instante timestamp, val_cargaenergiamwmed double, ingestion_timestamp timestamp",
    "geracao_usina": (
        "din_instante timestamp, id_subsistema string, nom_subsistema string, id_estado string, nom_estado string, "
        "cod_modalidadeoperacao string, nom_tipousina string, nom_tipocombustivel string, nom_usina string, "
        "id_ons string, ceg string, val_geracao double, ingestion_timestamp timestamp"
    ),
    "fator_capacidade": (
        "id_subsistema string, nom_subsistema string, id_estado string, nom_estado string, cod_pontoconexao string, "
        "nom_pontoconexao string, nom_localizacao string, val_latitudesecoletora double, val_longitudesecoletora double, "
        "val_latitudepontoconexao double, val_longitudepontoconexao double, nom_modalidadeoperacao string, "
        "nom_tipousina string, nom_usina_conjunto string, id_ons string, ceg string, din_instante timestamp, "
        "val_geracaoprogramada string, val_geracaoverificada string, val_capacidadeinstalada string, "
        "val_fatorcapacidade double, ingestion_timestamp timestamp"
    ),
    "previsao_programado": (
        "dat_programacao string, num_patamar int, cod_usinapdp string, nom_usinapdp string, "
        "val_previsao string, val_programado string, ingestion_timestamp timestamp"
    ),
    "clima_previsao": (
        "data_referencia string, data_prevista string, horizonte_dias long, latitude_requested double, "
        "longitude_requested double, raw_response string, ingestion_timestamp timestamp"
    ),
    "clima_observado": (
        "start_date string, end_date string, latitude_requested double, longitude_requested double, "
        "raw_response string, ingestion_timestamp timestamp"
    ),
}


def _java_disponivel():
    try:
        return subprocess.run(["java", "-version"], capture_output=True).returncode == 0
    except FileNotFoundError:
        return False


@pytest.fixture(scope="session")
def spark():
    """SparkSession local com a mesma configuração de fuso do pipeline (UTC)."""
    if not _java_disponivel():
        pytest.skip("Java não encontrado: os testes de Spark rodam no CI")
    from pyspark.sql import SparkSession

    try:
        sessao = (
            SparkSession.builder.master("local[1]")
            .appName("testes-silver")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    except Exception as erro:  # ex.: pyspark do databricks-connect, que não roda Spark local
        pytest.skip(f"Spark local indisponível: {erro}")
    yield sessao
    sessao.stop()


@pytest.fixture
def bronze(spark):
    """bronze("geracao_usina", [linhas]) -> DataFrame com o schema da bronze."""
    def criar(dataset, linhas):
        return spark.createDataFrame(linhas, SCHEMAS_BRONZE[dataset])
    return criar
