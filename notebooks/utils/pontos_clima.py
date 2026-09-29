"""Pontos onde o clima é coletado.

Usinas: um ponto por estado do Nordeste e por fonte, no centro das usinas ponderado pela
capacidade instalada (silver.fator_capacidade, 08/2026). Os 12 pontos cobrem 100% da
capacidade eólica e solar do subsistema Nordeste.

Capitais: clima das maiores cidades do Nordeste, para comparar com a carga (consumo).
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class PontoClima:
    id: str
    uf: str
    tipo: str  # "eolica", "solar" ou "capital"
    latitude: float
    longitude: float
    descricao: str


PONTOS_CLIMA = (
    PontoClima("ba-eolica", "BA", "eolica", -11.644, -41.736, "Usinas eólicas da Bahia (36,3% da capacidade eólica do NE)"),
    PontoClima("rn-eolica", "RN", "eolica", -5.561, -36.232, "Usinas eólicas do Rio Grande do Norte (35,3%)"),
    PontoClima("pi-eolica", "PI", "eolica", -8.308, -41.149, "Usinas eólicas do Piauí (14,9%)"),
    PontoClima("ce-eolica", "CE", "eolica", -3.607, -39.245, "Usinas eólicas do Ceará (7,9%)"),
    PontoClima("pb-eolica", "PB", "eolica", -6.968, -36.830, "Usinas eólicas da Paraíba (3,0%)"),
    PontoClima("pe-eolica", "PE", "eolica", -8.885, -37.045, "Usinas eólicas de Pernambuco (2,6%)"),
    PontoClima("ba-solar", "BA", "solar", -11.264, -42.279, "Usinas solares da Bahia (26,2% da capacidade solar do NE)"),
    PontoClima("pi-solar", "PI", "solar", -8.011, -43.623, "Usinas solares do Piauí (20,8%)"),
    PontoClima("ce-solar", "CE", "solar", -5.352, -38.514, "Usinas solares do Ceará (20,6%)"),
    PontoClima("rn-solar", "RN", "solar", -5.475, -37.008, "Usinas solares do Rio Grande do Norte (16,7%)"),
    PontoClima("pe-solar", "PE", "solar", -8.069, -38.483, "Usinas solares de Pernambuco (10,6%)"),
    PontoClima("pb-solar", "PB", "solar", -6.918, -37.343, "Usinas solares da Paraíba (5,2%)"),
    PontoClima("fortaleza", "CE", "capital", -3.720, -38.540, "Fortaleza"),
    PontoClima("recife", "PE", "capital", -8.050, -34.900, "Recife"),
    PontoClima("salvador", "BA", "capital", -12.970, -38.500, "Salvador"),
)
