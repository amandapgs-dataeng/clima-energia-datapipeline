"""Padrão das tabelas da gold: materialized views calculadas a partir da silver.

Cada tabela é descrita numa `TabelaGold` (tabelas da silver de origem, cálculo, colunas). O
pipeline, os testes de contrato e o dicionário partem dessas definições.
"""
from dataclasses import dataclass
from typing import Callable

# Recorte de negócio do projeto: o subsistema Nordeste.
SUBSISTEMA_ANALISE = "NE"


@dataclass(frozen=True)
class TabelaGold:
    nome: str
    pergunta: str  # a pergunta de negócio que a tabela responde
    descricao: str
    tabelas_silver: tuple  # relativas ao schema silver, ex.: ("fator_capacidade",)
    calcular: Callable  # recebe um DataFrame por tabela da silver (ou gold, ver abaixo), na mesma ordem
    colunas: list  # (nome, tipo, comentário), na ordem produzida por `calcular`
    tabelas_gold: tuple = ()  # outras tabelas da gold usadas como origem (lidas depois das da silver)


def ddl(colunas):
    """Schema em DDL a partir de (nome, tipo, comentário)."""
    return ",\n".join(f"{nome} {tipo} COMMENT '{comentario}'" for nome, tipo, comentario in colunas)


def nomes(colunas):
    return [nome for nome, _, _ in colunas]


def propriedades_da_tabela(tabela):
    propriedades = {"camada": "gold", "dataset": tabela.nome}
    if any(tipo.upper() == "TIMESTAMP_NTZ" for _, tipo, _ in tabela.colunas):
        propriedades["delta.feature.timestampNtz"] = "supported"
    return propriedades


def declarar_gold(dp, spark, tabela: TabelaGold, catalog):
    """Declara a tabela como materialized view no pipeline da gold."""

    @dp.materialized_view(
        name=tabela.nome,
        comment=f"{tabela.descricao} Pergunta: {tabela.pergunta}",
        schema=ddl(tabela.colunas),
        table_properties=propriedades_da_tabela(tabela),
    )
    def calcular():
        origens = [spark.read.table(f"{catalog}.silver.{nome}") for nome in tabela.tabelas_silver]
        origens += [spark.read.table(nome) for nome in tabela.tabelas_gold]
        return tabela.calcular(*origens)
