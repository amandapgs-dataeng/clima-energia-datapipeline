"""Cálculos de datas usados pelos notebooks de ingestão.

Funções puras (sem Spark, rede ou credenciais) para poderem ser testadas com pytest.
"""
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

FORMATO_DATA = "%Y-%m-%d"
FUSO_PROJETO = ZoneInfo("America/Fortaleza")


def agora_fortaleza(agora_utc=None):
    """Data/hora local de Fortaleza (sem tzinfo).

    O cluster roda em UTC: às 21h de Fortaleza, `datetime.now()` já é 00h do
    dia seguinte, e os notebooks processariam a data errada.
    """
    agora_utc = agora_utc or datetime.now(timezone.utc)
    return agora_utc.astimezone(FUSO_PROJETO).replace(tzinfo=None)


def resolver_data_referencia(data_param, hoje=None, dias_defasagem=0):
    """Data do widget `data_referencia` ou, se vazio, hoje (em Fortaleza) menos `dias_defasagem`."""
    if data_param and data_param.strip():
        return datetime.strptime(data_param.strip(), FORMATO_DATA)
    hoje = hoje or agora_fortaleza()
    return hoje - timedelta(days=dias_defasagem)


def mes_anterior_fechado(data_referencia):
    """(ano, mês) do último mês completo antes de `data_referencia`."""
    ultimo_dia_mes_anterior = data_referencia.replace(day=1) - timedelta(days=1)
    return ultimo_dia_mes_anterior.year, ultimo_dia_mes_anterior.month


def janela_retroativa(data_referencia, dias):
    """(início, fim) de uma janela de `dias` terminando em `data_referencia`."""
    return data_referencia - timedelta(days=dias), data_referencia


def anos_da_janela(data_inicio, data_fim):
    """Todos os anos tocados pela janela, em ordem (cobre a virada de ano)."""
    return list(range(data_inicio.year, data_fim.year + 1))


def janela_historico(data_referencia, dias_inicio=16, dias_fim=10):
    """(início, fim) da janela do histórico observado, que tem atraso de publicação."""
    return (
        data_referencia - timedelta(days=dias_inicio),
        data_referencia - timedelta(days=dias_fim),
    )
