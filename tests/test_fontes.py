import json
from datetime import datetime

import pytest

from utils.fontes import (
    ONS_BASE_URL,
    VARIAVEIS_HORARIAS,
    params_open_meteo,
    registros_por_ponto,
    respostas_por_ponto,
    url_ons,
    variaveis_da_vespera,
)
from utils.pontos_clima import PONTOS_CLIMA, PontoClima

A = PontoClima("a", "BA", "eolica", -11.6, -41.7, "ponto A")
B = PontoClima("b", "RN", "solar", -5.5, -37.0, "ponto B")


def test_url_ons():
    assert url_ons("carga_energia_di", "CARGA_ENERGIA_2026.parquet") == (
        f"{ONS_BASE_URL}/carga_energia_di/CARGA_ENERGIA_2026.parquet"
    )


class TestParamsOpenMeteo:
    def test_varios_pontos_numa_chamada(self):
        params = params_open_meteo([A, B], datetime(2026, 9, 8), datetime(2026, 9, 14, 21, 30))
        assert (params["latitude"], params["longitude"]) == ("-11.6,-5.5", "-41.7,-37.0")
        assert (params["start_date"], params["end_date"]) == ("2026-09-08", "2026-09-14")
        assert params["timezone"] == "America/Fortaleza"

    def test_pede_todas_as_variaveis_incluindo_vento_a_100m(self):
        params = params_open_meteo([A], datetime(2026, 9, 8), datetime(2026, 9, 8))
        assert params["hourly"].split(",") == list(VARIAVEIS_HORARIAS)
        assert "wind_speed_100m" in VARIAVEIS_HORARIAS

    def test_variaveis_da_vespera(self):
        assert variaveis_da_vespera(1)[0] == "temperature_2m_previous_day1"
        assert len(variaveis_da_vespera(1)) == len(VARIAVEIS_HORARIAS)


class TestRespostasPorPonto:
    def test_lista_na_ordem_dos_pontos(self):
        assert respostas_por_ponto([{"x": 1}, {"x": 2}], [A, B]) == [(A, {"x": 1}), (B, {"x": 2})]

    def test_objeto_unico_para_um_ponto(self):
        assert respostas_por_ponto({"x": 1}, [A]) == [(A, {"x": 1})]

    def test_quantidade_diferente_falha(self):
        with pytest.raises(ValueError):
            respostas_por_ponto([{"x": 1}], [A, B])


def test_registros_por_ponto():
    registros = registros_por_ponto([(A, {"hourly": {}}), (B, {"hourly": {}})], start_date="2026-09-01")
    assert [r["ponto_id"] for r in registros] == ["a", "b"]
    assert registros[0] == {
        "ponto_id": "a", "uf": "BA", "tipo_ponto": "eolica",
        "latitude_requested": -11.6, "longitude_requested": -41.7,
        "start_date": "2026-09-01", "raw_response": json.dumps({"hourly": {}}),
    }


class TestPontosClima:
    def test_ids_unicos(self):
        ids = [p.id for p in PONTOS_CLIMA]
        assert len(ids) == len(set(ids))

    def test_uma_eolica_e_uma_solar_por_estado_e_tres_capitais(self):
        tipos = [p.tipo for p in PONTOS_CLIMA]
        assert (tipos.count("eolica"), tipos.count("solar"), tipos.count("capital")) == (6, 6, 3)

    def test_todos_no_nordeste(self):
        for ponto in PONTOS_CLIMA:
            assert ponto.uf in {"BA", "RN", "PI", "CE", "PB", "PE", "MA", "AL", "SE"}
            assert -18 < ponto.latitude < -1 and -48 < ponto.longitude < -34, ponto.id
