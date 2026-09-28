from datetime import datetime

from utils.fontes import ONS_BASE_URL, VARIAVEIS_HORARIAS, params_open_meteo, url_ons


def test_url_ons():
    assert url_ons("carga_energia_di", "CARGA_ENERGIA_2026.parquet") == (
        f"{ONS_BASE_URL}/carga_energia_di/CARGA_ENERGIA_2026.parquet"
    )


def test_params_open_meteo_formata_datas_e_fixa_fortaleza():
    params = params_open_meteo(datetime(2026, 9, 8), datetime(2026, 9, 14, 21, 30))
    assert params["start_date"] == "2026-09-08"
    assert params["end_date"] == "2026-09-14"
    assert (params["latitude"], params["longitude"]) == (-3.72, -38.54)
    assert params["timezone"] == "America/Fortaleza"


def test_params_open_meteo_pede_todas_as_variaveis_horarias():
    params = params_open_meteo(datetime(2026, 9, 8), datetime(2026, 9, 8))
    assert params["hourly"].split(",") == list(VARIAVEIS_HORARIAS)
