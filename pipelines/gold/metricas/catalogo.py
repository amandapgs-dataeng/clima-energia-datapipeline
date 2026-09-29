"""Todas as tabelas da gold. O pipeline, os testes de contrato e o dicionário partem daqui."""
from metricas import clima, energia

TABELAS = energia.TABELAS + clima.TABELAS
