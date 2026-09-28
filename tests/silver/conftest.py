import subprocess

import pytest


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
