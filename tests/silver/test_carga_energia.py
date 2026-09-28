from datetime import date, datetime

import pytest

from utilities.carga_energia import (
    CHAVE,
    COLUNAS_VERSIONADAS,
    REGRAS_ALERTA,
    REGRAS_DESCARTE,
    padronizar_carga_energia,
)

COLUNAS_BRONZE = "id_subsistema string, nom_subsistema string, din_instante timestamp, val_cargaenergiamwmed double, ingestion_timestamp timestamp"
INGESTAO = datetime(2026, 9, 28, 14, 43)


def bronze(spark, linhas):
    return spark.createDataFrame(linhas, COLUNAS_BRONZE)


def aplicar_regras(df, regras):
    for expressao in regras.values():
        df = df.filter(expressao)
    return df


class TestPadronizar:
    def test_colunas_e_tipos(self, spark):
        df = padronizar_carga_energia(bronze(spark, [("NE", "Nordeste", datetime(2026, 9, 1), 13928.0, INGESTAO)]))
        assert [(c.name, c.dataType.simpleString()) for c in df.schema] == [
            ("id_subsistema", "string"),
            ("subsistema", "string"),
            ("data", "date"),
            ("carga_mwmed", "double"),
            ("ingestion_timestamp", "timestamp"),
        ]

    def test_meia_noite_rotulada_utc_vira_o_proprio_dia(self, spark):
        # O ONS grava o dia como 00:00 local, rotulado como UTC.
        linha = padronizar_carga_energia(bronze(spark, [("NE", "Nordeste", datetime(2026, 9, 1, 0, 0), 1.0, INGESTAO)])).first()
        assert linha.data == date(2026, 9, 1)

    @pytest.mark.parametrize(
        "codigo_bronze, nome_bronze, codigo, nome",
        [
            ("NE", "NORDESTE", "NE", "Nordeste"),
            (" ne ", "Nordeste", "NE", "Nordeste"),
            ("SE", "Sudeste/Centro-Oeste", "SE", "Sudeste/Centro-Oeste"),
            ("N", "NORTE", "N", "Norte"),
            ("S", "Sul", "S", "Sul"),
        ],
    )
    def test_vocabulario_de_subsistema(self, spark, codigo_bronze, nome_bronze, codigo, nome):
        linha = padronizar_carga_energia(bronze(spark, [(codigo_bronze, nome_bronze, datetime(2026, 9, 1), 1.0, INGESTAO)])).first()
        assert (linha.id_subsistema, linha.subsistema) == (codigo, nome)

    def test_subsistema_desconhecido_fica_sem_nome(self, spark):
        linha = padronizar_carga_energia(bronze(spark, [("XX", "?", datetime(2026, 9, 1), 1.0, INGESTAO)])).first()
        assert linha.subsistema is None


class TestRegrasDeQualidade:
    def test_descarta_registros_invalidos(self, spark):
        df = padronizar_carga_energia(bronze(spark, [
            ("NE", "Nordeste", datetime(2026, 9, 1), 13928.0, INGESTAO),  # válido
            (None, None, datetime(2026, 9, 1), 100.0, INGESTAO),           # sem subsistema
            ("NE", "Nordeste", None, 100.0, INGESTAO),                     # sem data
            ("XX", "?", datetime(2026, 9, 1), 100.0, INGESTAO),            # subsistema desconhecido
            ("S", "Sul", datetime(2026, 9, 1), None, INGESTAO),            # sem valor
        ]))
        restantes = aplicar_regras(df, REGRAS_DESCARTE).collect()
        assert [(r.id_subsistema, r.carga_mwmed) for r in restantes] == [("NE", 13928.0)]

    def test_carga_nao_positiva_so_gera_alerta(self, spark):
        df = padronizar_carga_energia(bronze(spark, [("N", "Norte", datetime(2026, 9, 1), 0.0, INGESTAO)]))
        assert aplicar_regras(df, REGRAS_DESCARTE).count() == 1
        assert aplicar_regras(df, REGRAS_ALERTA).count() == 0

    def test_regras_sao_sql_valido(self, spark):
        df = padronizar_carga_energia(bronze(spark, [("NE", "Nordeste", datetime(2026, 9, 1), 1.0, INGESTAO)]))
        for nome, expressao in {**REGRAS_DESCARTE, **REGRAS_ALERTA}.items():
            df.filter(expressao).count()  # falha se a expressão ou uma coluna não existir


class TestContratoDoHistorico:
    def test_chave_e_colunas_versionadas_existem_na_saida(self, spark):
        colunas = padronizar_carga_energia(bronze(spark, [])).columns
        assert set(CHAVE) <= set(colunas)
        assert set(COLUNAS_VERSIONADAS) <= set(colunas)
        assert not set(CHAVE) & set(COLUNAS_VERSIONADAS)

    def test_ingestion_timestamp_nao_versiona(self):
        # Se versionasse, cada releitura idêntica criaria uma versão nova no histórico.
        assert "ingestion_timestamp" not in COLUNAS_VERSIONADAS
