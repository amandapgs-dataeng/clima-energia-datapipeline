from datetime import date, datetime

import pytest

from utilities.carga_energia import (
    CHAVE,
    COLUNAS_VERSIONADAS,
    REGRAS_ALERTA,
    REGRAS_DESCARTE,
    SEQUENCIA,
    padronizar_carga_energia,
    versoes_legiveis,
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
            ("ingerido_em", "timestamp"),
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

    def test_momento_da_ingestao_nao_versiona(self):
        # Se versionasse, cada releitura idêntica criaria uma versão nova no histórico.
        assert SEQUENCIA not in COLUNAS_VERSIONADAS


COLUNAS_CDC = "id_subsistema string, subsistema string, data date, carga_mwmed double, ingerido_em timestamp, __START_AT timestamp, __END_AT timestamp"


class TestVersoesLegiveis:
    def historico(self, spark):
        # NE em 25/09: publicado em 28/09 e revisado em 04/10. SE em 25/09: nunca revisado.
        return spark.createDataFrame([
            ("NE", "Nordeste", date(2026, 9, 25), 15109.0, datetime(2026, 9, 28, 14, 44), datetime(2026, 9, 28, 14, 43), datetime(2026, 10, 4, 21, 5)),
            ("NE", "Nordeste", date(2026, 9, 25), 15187.4, datetime(2026, 10, 4, 21, 5), datetime(2026, 10, 4, 21, 5), None),
            ("SE", "Sudeste/Centro-Oeste", date(2026, 9, 25), 46000.0, datetime(2026, 9, 28, 14, 44), datetime(2026, 9, 28, 14, 43), None),
        ], COLUNAS_CDC)

    def test_sem_colunas_tecnicas_do_cdc(self, spark):
        colunas = versoes_legiveis(self.historico(spark)).columns
        assert not [c for c in colunas if c.startswith("__")]
        assert {"vigente_desde", "vigente_ate", "versao_atual", "numero_versao", "confirmado_em"} <= set(colunas)

    def test_numera_as_versoes_e_marca_a_atual(self, spark):
        linhas = versoes_legiveis(self.historico(spark)).filter("id_subsistema = 'NE'").orderBy("numero_versao").collect()
        assert [(l.numero_versao, l.carga_mwmed, l.versao_atual) for l in linhas] == [(1, 15109.0, False), (2, 15187.4, True)]
        assert linhas[0].vigente_ate == linhas[1].vigente_desde

    def test_valor_nunca_revisado_tem_uma_versao_atual(self, spark):
        linha = versoes_legiveis(self.historico(spark)).filter("id_subsistema = 'SE'").first()
        assert (linha.numero_versao, linha.versao_atual, linha.vigente_ate) == (1, True, None)
