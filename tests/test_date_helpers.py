from datetime import datetime, timezone

import pytest

from utils.date_helpers import (
    agora_fortaleza,
    anos_da_janela,
    janela_historico,
    janela_retroativa,
    mes_anterior_fechado,
    resolver_data_referencia,
)


class TestAgoraFortaleza:
    def test_meia_noite_utc_ainda_e_o_dia_anterior_em_fortaleza(self):
        # Horário em que os jobs disparam: 21h em Fortaleza = 00h UTC do dia seguinte.
        agora_utc = datetime(2026, 9, 25, 0, 0, tzinfo=timezone.utc)
        assert agora_fortaleza(agora_utc) == datetime(2026, 9, 24, 21, 0)

    def test_virada_de_ano_em_utc_ainda_e_31_de_dezembro_em_fortaleza(self):
        agora_utc = datetime(2027, 1, 1, 0, 0, tzinfo=timezone.utc)
        assert agora_fortaleza(agora_utc) == datetime(2026, 12, 31, 21, 0)

    def test_retorna_sem_fuso_para_combinar_com_datas_do_widget(self):
        assert agora_fortaleza().tzinfo is None


class TestResolverDataReferencia:
    def test_usa_data_do_widget(self):
        assert resolver_data_referencia("2026-03-15") == datetime(2026, 3, 15)

    def test_ignora_espacos_no_widget(self):
        assert resolver_data_referencia("  2026-03-15 ") == datetime(2026, 3, 15)

    @pytest.mark.parametrize("vazio", ["", "   ", None])
    def test_widget_vazio_usa_hoje(self, vazio):
        hoje = datetime(2026, 9, 24, 21, 0)
        assert resolver_data_referencia(vazio, hoje=hoje) == hoje

    def test_defasagem_d_menos_1(self):
        hoje = datetime(2026, 9, 24)
        assert resolver_data_referencia("", hoje=hoje, dias_defasagem=1) == datetime(2026, 9, 23)

    def test_defasagem_d_menos_1_na_virada_de_ano(self):
        hoje = datetime(2027, 1, 1)
        assert resolver_data_referencia("", hoje=hoje, dias_defasagem=1) == datetime(2026, 12, 31)

    def test_defasagem_nao_se_aplica_a_data_informada(self):
        assert resolver_data_referencia("2026-03-15", dias_defasagem=1) == datetime(2026, 3, 15)

    def test_formato_invalido_falha(self):
        with pytest.raises(ValueError):
            resolver_data_referencia("15/03/2026")


class TestMesAnteriorFechado:
    @pytest.mark.parametrize(
        "data_referencia, esperado",
        [
            (datetime(2026, 9, 24), (2026, 8)),
            (datetime(2026, 9, 1), (2026, 8)),
            (datetime(2026, 9, 30), (2026, 8)),
            (datetime(2026, 3, 2), (2026, 2)),
            (datetime(2024, 3, 2), (2024, 2)),  # fevereiro de ano bissexto
        ],
    )
    def test_meses_comuns(self, data_referencia, esperado):
        assert mes_anterior_fechado(data_referencia) == esperado

    @pytest.mark.parametrize("dia", [1, 2, 15, 31])
    def test_janeiro_volta_para_dezembro_do_ano_anterior(self, dia):
        assert mes_anterior_fechado(datetime(2027, 1, dia)) == (2026, 12)


class TestJanelaRetroativa:
    def test_janela_de_60_dias(self):
        inicio, fim = janela_retroativa(datetime(2026, 9, 24), 60)
        assert inicio == datetime(2026, 7, 26)
        assert fim == datetime(2026, 9, 24)

    def test_janela_atravessa_virada_de_ano(self):
        inicio, fim = janela_retroativa(datetime(2027, 1, 20), 60)
        assert inicio == datetime(2026, 11, 21)
        assert fim == datetime(2027, 1, 20)


class TestAnosDaJanela:
    def test_janela_dentro_do_mesmo_ano(self):
        assert anos_da_janela(datetime(2026, 7, 26), datetime(2026, 9, 24)) == [2026]

    def test_janela_na_virada_de_ano_inclui_os_dois_anos(self):
        assert anos_da_janela(datetime(2026, 11, 21), datetime(2027, 1, 20)) == [2026, 2027]

    def test_janela_maior_que_um_ano_inclui_anos_do_meio(self):
        assert anos_da_janela(datetime(2025, 12, 1), datetime(2027, 1, 5)) == [2025, 2026, 2027]

    def test_combinado_com_janela_retroativa_de_60_dias_em_janeiro(self):
        inicio, fim = janela_retroativa(datetime(2027, 1, 10), 60)
        assert anos_da_janela(inicio, fim) == [2026, 2027]


class TestJanelaHistorico:
    def test_janela_padrao_de_16_a_10_dias_atras(self):
        inicio, fim = janela_historico(datetime(2026, 9, 24))
        assert inicio == datetime(2026, 9, 8)
        assert fim == datetime(2026, 9, 14)

    def test_janela_cobre_7_dias(self):
        inicio, fim = janela_historico(datetime(2026, 9, 24))
        assert (fim - inicio).days == 6  # 7 dias contando as duas pontas

    def test_janela_na_virada_de_ano(self):
        inicio, fim = janela_historico(datetime(2027, 1, 5))
        assert inicio == datetime(2026, 12, 20)
        assert fim == datetime(2026, 12, 26)
