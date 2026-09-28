from datetime import date, datetime, timezone

import pytest
from pyspark.sql import functions as F

from utilities.historico import COLUNAS_VERSAO, ddl, nomes, versoes_legiveis
from utilities.vocabulario import (
    codigo_subsistema,
    hora_local_ons,
    modalidade_operacao,
    nome_subsistema,
    sem_codigo,
    texto_limpo,
    tipo_usina,
)


def aplicar(spark, funcao, valor, tipo="string"):
    return spark.createDataFrame([(valor,)], f"v {tipo}").select(funcao("v").alias("r")).first().r


class TestVocabulario:
    @pytest.mark.parametrize("bruto, esperado", [
        ("EOLIELÉTRICA", "Eólica"), ("Eólica", "Eólica"), ("eolica", "Eólica"),
        ("FOTOVOLTAICA", "Solar"), ("Solar", "Solar"),
        ("HIDROELÉTRICA", "Hidrelétrica"), ("TÉRMICA", "Térmica"), ("NUCLEAR", "Nuclear"),
        (" Eólica ", "Eólica"), ("BIOMASSA", None),
    ])
    def test_tipo_usina_unifica_os_nomes_das_fontes(self, spark, bruto, esperado):
        assert aplicar(spark, tipo_usina, bruto) == esperado

    @pytest.mark.parametrize("bruto, esperado", [
        ("TIPO I", "Tipo I"), ("Tipo I", "Tipo I"), ("TIPO II-B", "Tipo II-B"), ("Tipo II-B", "Tipo II-B"),
        ("Conjunto de Usinas", "Conjunto de Usinas"), ("Pequenas Usinas (Tipo III)", "Pequenas Usinas (Tipo III)"),
    ])
    def test_modalidade_operacao(self, spark, bruto, esperado):
        assert aplicar(spark, modalidade_operacao, bruto) == esperado

    @pytest.mark.parametrize("bruto, esperado", [("-", None), ("", None), (" UHE.PH.PA.030354-2.01 ", "UHE.PH.PA.030354-2.01")])
    def test_sem_codigo(self, spark, bruto, esperado):
        assert aplicar(spark, sem_codigo, bruto) == esperado

    def test_texto_limpo_remove_espacos_de_campo_fixo(self, spark):
        assert aplicar(spark, texto_limpo, "I4ALBA      ") == "I4ALBA"
        assert aplicar(spark, texto_limpo, "   ") is None

    @pytest.mark.parametrize("bruto, esperado", [(" ne ", "Nordeste"), ("SE", "Sudeste/Centro-Oeste"), ("XX", None)])
    def test_subsistema(self, spark, bruto, esperado):
        assert aplicar(spark, lambda c: nome_subsistema(codigo_subsistema(c)), bruto) == esperado

    def test_hora_do_ons_mantem_o_relogio_publicado(self, spark):
        # 06:00 publicado pelo ONS (horário de Brasília rotulado UTC) continua 06:00, sem fuso.
        hora = aplicar(spark, hora_local_ons, datetime(2026, 8, 1, 6, 0, tzinfo=timezone.utc), "timestamp")
        assert hora == datetime(2026, 8, 1, 6, 0)


class TestHistorico:
    def test_ddl(self):
        assert ddl([("a", "INT", "coluna a"), ("b", "DATE", "coluna b")]) == "a INT COMMENT 'coluna a',\nb DATE COMMENT 'coluna b'"

    def cdc(self, spark):
        # NE/25-09 publicado em 28/09 e revisado em 04/10; SE/25-09 nunca revisado.
        return spark.createDataFrame([
            ("NE", date(2026, 9, 25), 15109.0, datetime(2026, 9, 28, 14, 44), datetime(2026, 9, 28, 14, 43), datetime(2026, 10, 4, 21, 5)),
            ("NE", date(2026, 9, 25), 15187.4, datetime(2026, 10, 4, 21, 5), datetime(2026, 10, 4, 21, 5), None),
            ("SE", date(2026, 9, 25), 46000.0, datetime(2026, 9, 28, 14, 44), datetime(2026, 9, 28, 14, 43), None),
        ], "id_subsistema string, data date, carga double, ingerido_em timestamp, __START_AT timestamp, __END_AT timestamp")

    def test_colunas_de_negocio_no_lugar_das_tecnicas(self, spark):
        colunas = versoes_legiveis(self.cdc(spark), ["id_subsistema", "data"]).columns
        assert colunas == ["id_subsistema", "data", "carga"] + nomes(COLUNAS_VERSAO)

    def test_numera_versoes_e_marca_a_atual(self, spark):
        linhas = (versoes_legiveis(self.cdc(spark), ["id_subsistema", "data"])
                  .filter("id_subsistema = 'NE'").orderBy("numero_versao").collect())
        assert [(l.numero_versao, l.carga, l.versao_atual) for l in linhas] == [(1, 15109.0, False), (2, 15187.4, True)]
        assert linhas[0].vigente_ate == linhas[1].vigente_desde

    def test_valor_nunca_revisado(self, spark):
        linha = versoes_legiveis(self.cdc(spark), ["id_subsistema", "data"]).filter("id_subsistema = 'SE'").first()
        assert (linha.numero_versao, linha.versao_atual, linha.vigente_ate) == (1, True, None)
