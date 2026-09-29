"""Regras de cálculo da gold, com dados pequenos e resultados conferidos à mão."""
from datetime import date, datetime

import pytest

from metricas.clima import (
    carga_x_temperatura,
    clima_x_geracao_horaria,
    correlacao_clima_geracao,
    curva_de_potencia,
    precisao_previsao_tempo,
)
from metricas.energia import (
    aproveitamento_mensal,
    precisao_programacao_mensal,
    restricao_diaria,
    restricao_por_usina_mensal,
)

FATOR = ("data_hora timestamp_ntz, id_ons string, uf string, tipo_usina string, id_subsistema string, "
         "geracao_programada_mwmed double, geracao_verificada_mwmed double, capacidade_instalada_mw double")
H = datetime(2026, 8, 1, 12, 0)


def fator(spark, linhas):
    return spark.createDataFrame(linhas, FATOR)


class TestAproveitamento:
    def test_fator_mensal_pondera_pela_capacidade(self, spark):
        df = aproveitamento_mensal(fator(spark, [
            (H, "A", "BA", "Eólica", "NE", 50.0, 40.0, 100.0),
            (H, "B", "BA", "Eólica", "NE", 10.0, 20.0, 300.0),
        ])).first()
        assert (df.mes, df.usinas, df.geracao_verificada_mwh, df.capacidade_mwh) == (date(2026, 8, 1), 2, 60.0, 400.0)
        assert df.fator_capacidade == pytest.approx(0.15)          # 60 / 400
        assert df.fator_capacidade_programado == pytest.approx(0.15)  # 60 / 400

    def test_so_nordeste_e_so_renovaveis(self, spark):
        df = aproveitamento_mensal(fator(spark, [
            (H, "A", "BA", "Eólica", "NE", 1.0, 1.0, 10.0),
            (H, "B", "MG", "Solar", "SE", 1.0, 1.0, 10.0),
            (H, "C", "BA", "Térmica", "NE", 1.0, 1.0, 10.0),
        ]))
        assert [(r.uf, r.tipo_usina) for r in df.collect()] == [("BA", "Eólica")]


class TestPrecisaoProgramacao:
    def test_erro_relativo_e_vies(self, spark):
        r = precisao_programacao_mensal(fator(spark, [
            (H, "A", "RN", "Eólica", "NE", 100.0, 80.0, 200.0),   # gerou 20 a menos
            (H, "B", "RN", "Eólica", "NE", 50.0, 70.0, 200.0),    # gerou 20 a mais
        ])).first()
        assert r.erro_absoluto_mwh == 40.0
        assert r.erro_relativo == pytest.approx(40 / 150)
        assert r.vies_relativo == pytest.approx(0.0)  # os erros se compensam no total


PROGRAMACAO = "data date, patamar int, codigo_usina string, nome_usina string, previsao_mwmed double, programado_mwmed double"


class TestRestricao:
    def programacao(self, spark):
        return spark.createDataFrame([
            (date(2026, 9, 1), 1, "U1", "Usina 1", 100.0, 60.0),   # 40 MW não programados na meia hora
            (date(2026, 9, 1), 2, "U1", "Usina 1", 100.0, 100.0),
            (date(2026, 9, 1), 1, "U2", "Usina 2", 50.0, 60.0),    # programado acima do previsto: não é restrição
            (date(2026, 9, 1), 2, "U2", "Usina 2", 50.0, 49.8),    # diferença abaixo da tolerância
        ], PROGRAMACAO)

    def test_restricao_diaria_em_energia(self, spark):
        r = restricao_diaria(self.programacao(spark)).first()
        assert (r.previsto_mwh, r.programado_mwh) == (150.0, pytest.approx(134.9))  # meia hora = 0,5 h
        assert r.restricao_mwh == pytest.approx(20.1)  # (40 + 0,2) x 0,5
        assert r.restricao_pct == pytest.approx(20.1 / 150)
        assert r.meias_horas_com_restricao_pct == 0.25  # só U1 patamar 1 passa da tolerância

    def test_restricao_por_usina(self, spark):
        linhas = {r.codigo_usina: r for r in restricao_por_usina_mensal(self.programacao(spark)).collect()}
        assert linhas["U1"].restricao_mwh == 20.0 and linhas["U1"].restricao_pct == pytest.approx(0.2)
        assert linhas["U2"].mes == date(2026, 9, 1)


CLIMA_OBS = ("ponto_id string, data_hora timestamp_ntz, vento_velocidade_100m_kmh double, radiacao_solar_wm2 double, "
             "cobertura_nuvens_pct double, temperatura_c double, precipitacao_mm double, tipo_ponto string")


def clima(spark, linhas):
    return spark.createDataFrame(linhas, CLIMA_OBS)


class TestClimaXGeracao:
    def test_junta_a_geracao_ao_ponto_das_usinas_da_fonte_no_estado(self, spark):
        df = clima_x_geracao_horaria(
            fator(spark, [(H, "A", "BA", "Eólica", "NE", 0.0, 50.0, 100.0), (H, "B", "BA", "Solar", "NE", 0.0, 30.0, 60.0)]),
            clima(spark, [("ba-eolica", H, 36.0, 800.0, 10.0, 30.0, 0.0, "eolica"),
                          ("ba-solar", H, 18.0, 900.0, 5.0, 31.0, 0.0, "solar")]),
        )
        linhas = {r.tipo_usina: r for r in df.collect()}
        assert linhas["Eólica"].ponto_id == "ba-eolica" and linhas["Eólica"].vento_100m_ms == pytest.approx(10.0)
        assert linhas["Eólica"].fator_capacidade == pytest.approx(0.5)
        assert linhas["Solar"].radiacao_solar_wm2 == 900.0 and linhas["Solar"].fator_capacidade == pytest.approx(0.5)


CLIMA_GERACAO = ("data_hora timestamp_ntz, uf string, tipo_usina string, ponto_id string, geracao_verificada_mwmed double, "
                 "capacidade_mw double, fator_capacidade double, vento_100m_ms double, radiacao_solar_wm2 double, "
                 "cobertura_nuvens_pct double, temperatura_c double, precipitacao_mm double")


class TestCurvaECorrelacao:
    def dados(self, spark):
        linhas = []
        for i, vento in enumerate([3.2, 3.8, 8.1, 8.9, 12.5]):
            linhas.append((datetime(2026, 8, 1, i), "BA", "Eólica", "ba-eolica", vento * 10, 100.0, vento / 10, vento, 0.0, 0.0, 25.0, 0.0))
        for i, rad in enumerate([0.0, 0.0, 120.0, 480.0, 910.0]):
            linhas.append((datetime(2026, 8, 1, i), "BA", "Solar", "ba-solar", rad / 20, 50.0, rad / 1000, 5.0, rad, 100 - rad / 10, 25.0, 0.0))
        return spark.createDataFrame(linhas, CLIMA_GERACAO)

    def test_curva_agrupa_vento_em_faixas_de_1_ms(self, spark):
        curva = curva_de_potencia(self.dados(spark)).filter("tipo_usina = 'Eólica'").orderBy("faixa_inicio").collect()
        assert [(c.faixa_inicio, c.faixa_fim, c.horas) for c in curva] == [(3.0, 4.0, 2), (8.0, 9.0, 2), (12.0, 13.0, 1)]
        assert curva[0].fator_capacidade == pytest.approx((32 + 38) / 200)

    def test_curva_solar_em_faixas_de_50_wm2(self, spark):
        faixas = [c.faixa_inicio for c in curva_de_potencia(self.dados(spark)).filter("tipo_usina = 'Solar'").orderBy("faixa_inicio").collect()]
        assert faixas == [0.0, 100.0, 450.0, 900.0]

    def test_correlacao_solar_usa_so_horas_com_sol(self, spark):
        corr = {(r.tipo_usina, r.variavel): r for r in correlacao_clima_geracao(self.dados(spark)).collect()}
        assert corr[("Solar", "radiacao_solar_wm2")].horas == 3
        assert corr[("Eólica", "vento_100m_ms")].correlacao == pytest.approx(1.0)
        assert corr[("Solar", "cobertura_nuvens_pct")].correlacao == pytest.approx(-1.0)

    def test_variavel_constante_da_correlacao_nula_em_vez_de_erro(self, spark):
        # A temperatura é 25 °C em todas as horas dos dados de teste.
        corr = {(r.tipo_usina, r.variavel): r for r in correlacao_clima_geracao(self.dados(spark)).collect()}
        assert corr[("Eólica", "temperatura_c")].correlacao is None


class TestPrecisaoPrevisaoTempo:
    def test_erro_do_d1_e_radiacao_so_com_sol(self, spark):
        previsao = spark.createDataFrame([
            ("rn-eolica", "RN", "eolica", datetime(2026, 8, 1, 3), 1, 40.0, 0.0, 25.0),
            ("rn-eolica", "RN", "eolica", datetime(2026, 8, 1, 12), 1, 30.0, 700.0, 30.0),
            ("rn-eolica", "RN", "eolica", datetime(2026, 8, 1, 12), 0, 99.0, 999.0, 99.0),  # horizonte 0: fora
        ], "ponto_id string, uf string, tipo_ponto string, data_hora timestamp_ntz, horizonte_dias int, "
           "vento_velocidade_100m_kmh double, radiacao_solar_wm2 double, temperatura_c double")
        observado = clima(spark, [
            ("rn-eolica", datetime(2026, 8, 1, 3), 36.0, 0.0, 0.0, 24.0, 0.0, "eolica"),
            ("rn-eolica", datetime(2026, 8, 1, 12), 36.0, 800.0, 0.0, 31.0, 0.0, "eolica"),
        ])
        r = precisao_previsao_tempo(previsao, observado).first()
        assert r.horas == 2
        assert r.erro_medio_absoluto_vento_100m_kmh == pytest.approx(5.0)   # |40-36| e |30-36|
        assert r.vies_vento_100m_kmh == pytest.approx(-1.0)                 # (+4 - 6) / 2
        assert r.erro_medio_absoluto_radiacao_wm2 == pytest.approx(100.0)   # só a hora com sol
        assert r.erro_medio_absoluto_temperatura_c == pytest.approx(1.0)


class TestCargaXTemperatura:
    def test_media_das_capitais_e_dia_da_semana(self, spark):
        carga = spark.createDataFrame([
            ("NE", date(2026, 9, 26), 14000.0), ("NE", date(2026, 9, 28), 15000.0), ("SE", date(2026, 9, 28), 45000.0),
        ], "id_subsistema string, data date, carga_mwmed double")
        observado = clima(spark, [
            ("fortaleza", datetime(2026, 9, 28, 14), 0.0, 0.0, 0.0, 30.0, 0.0, "capital"),
            ("recife", datetime(2026, 9, 28, 14), 0.0, 0.0, 0.0, 28.0, 0.0, "capital"),
            ("ba-eolica", datetime(2026, 9, 28, 14), 0.0, 0.0, 0.0, 40.0, 0.0, "eolica"),  # não é capital
            ("salvador", datetime(2026, 9, 26, 14), 0.0, 0.0, 0.0, 27.0, 0.0, "capital"),
        ])
        linhas = {r.data: r for r in carga_x_temperatura(carga, observado).collect()}
        segunda = linhas[date(2026, 9, 28)]
        assert (segunda.carga_mwmed, segunda.temperatura_media_c, segunda.temperatura_maxima_c) == (15000.0, 29.0, 30.0)
        assert (segunda.dia_semana, segunda.fim_de_semana) == (1, False)
        assert (linhas[date(2026, 9, 26)].dia_semana, linhas[date(2026, 9, 26)].fim_de_semana) == (6, True)
