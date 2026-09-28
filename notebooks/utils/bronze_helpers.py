"""Leitura das fontes, gravação na bronze e log de auditoria (depende de Spark)."""
import os
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from utils.date_helpers import FORMATO_DATA
from utils.http_helpers import baixar_arquivo

TAMANHO_MAX_ERRO = 1000


@dataclass
class Execucao:
    inicio: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    linhas_gravadas: int = 0


def ler_parquet_remoto(spark, url, nome_arquivo):
    """Baixa um parquet para o disco local do driver e o lê com Spark."""
    destino = os.path.join(tempfile.gettempdir(), nome_arquivo)
    baixar_arquivo(url, destino)
    return spark.read.parquet(f"file://{destino}")


def gravar_bronze(df, tabela, fonte, ingestion_timestamp):
    """Acrescenta metadados de ingestão, grava em `tabela` e devolve as linhas gravadas."""
    (
        df.withColumn("ingestion_timestamp", F.lit(ingestion_timestamp))
        .withColumn("source", F.lit(fonte))
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(tabela)
    )
    # Contagem pelas métricas do commit Delta: evita reprocessar o DataFrame só para contar.
    metricas = DeltaTable.forName(df.sparkSession, tabela).history(1).first()["operationMetrics"]
    return int(metricas.get("numOutputRows", 0))


def _registrar_auditoria(spark, catalog, pipeline, data_referencia, execucao, status, erro=None):
    registro = {
        "pipeline_name": pipeline,
        "data_referencia": data_referencia.strftime(FORMATO_DATA),
        "execution_timestamp": execucao.inicio,
        "status": status,
        "linhas_gravadas": execucao.linhas_gravadas,
        "duracao_segundos": round(time.time() - execucao.inicio.timestamp(), 1),
        "erro": erro[:TAMANHO_MAX_ERRO] if erro else None,
    }
    (
        spark.createDataFrame([registro], schema=(
            "pipeline_name string, data_referencia string, execution_timestamp timestamp, "
            "status string, linhas_gravadas long, duracao_segundos double, erro string"
        ))
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(f"{catalog}.bronze._audit_log")
    )


@contextmanager
def execucao_auditada(spark, catalog, pipeline, data_referencia, logger):
    """Registra sucesso ou falha no `_audit_log`; em caso de falha, relança o erro para o job falhar."""
    execucao = Execucao()
    logger.info("Início | data_referencia=%s | catalog=%s", data_referencia.strftime(FORMATO_DATA), catalog)
    try:
        yield execucao
    except Exception as erro:
        logger.exception("Falha na execução")
        _registrar_auditoria(spark, catalog, pipeline, data_referencia, execucao, "failed", repr(erro))
        raise
    _registrar_auditoria(spark, catalog, pipeline, data_referencia, execucao, "success")
    logger.info("Sucesso | %s linhas gravadas", execucao.linhas_gravadas)
