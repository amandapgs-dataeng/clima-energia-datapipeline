"""Todas as tabelas da silver. O pipeline, os testes de contrato e o dicionário partem daqui."""
from utilities import carga_energia, clima, fator_capacidade, geracao_usina, previsao_programado

TABELAS = (
    carga_energia.TABELA,
    geracao_usina.TABELA,
    fator_capacidade.TABELA,
    previsao_programado.TABELA,
    clima.TABELA_PREVISAO,
    clima.TABELA_OBSERVADO,
)
