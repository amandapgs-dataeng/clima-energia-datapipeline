"""Contrato que toda tabela do catálogo da silver cumpre: a view padronizada produz exatamente
as colunas e tipos declarados (o schema das tabelas do pipeline), e as regras e chaves
referenciam colunas que existem."""
import pytest

from utilities.catalogo import TABELAS
from utilities.padrao_silver import COLUNA_REGRAS_VIOLADAS, SEQUENCIA, marcar_regras_violadas


@pytest.fixture(params=TABELAS, ids=[t.nome for t in TABELAS])
def tabela(request):
    return request.param


@pytest.fixture
def saida(tabela, bronze):
    return tabela.padronizar(*[bronze(origem, []) for origem in tabela.tabelas_bronze])


def test_origens_existem_na_bronze(tabela, schemas_bronze):
    assert set(tabela.tabelas_bronze) <= set(schemas_bronze)


def test_colunas_e_tipos_iguais_ao_declarado(tabela, saida):
    esperado = [(nome, tipo.lower()) for nome, tipo, _ in tabela.colunas] + [(SEQUENCIA, "timestamp")]
    obtido = [(campo.name, campo.dataType.simpleString()) for campo in saida.schema]
    assert obtido == esperado


def test_chave_e_colunas_versionadas_existem(tabela, saida):
    assert set(tabela.chave) <= set(saida.columns)
    assert set(tabela.colunas_versionadas) <= set(saida.columns)
    assert not set(tabela.chave) & set(tabela.colunas_versionadas)
    assert SEQUENCIA not in tabela.colunas_versionadas  # senão toda releitura idêntica viraria uma versão


def test_regras_sao_sql_valido(tabela, saida):
    for expressao in {**tabela.regras_descarte, **tabela.regras_alerta}.values():
        saida.filter(expressao).count()  # falha se a expressão ou uma coluna não existir


def test_quarentena_tem_o_schema_declarado(tabela, saida):
    quarentena = marcar_regras_violadas(saida, tabela.regras_descarte)
    assert quarentena.columns[-1] == COLUNA_REGRAS_VIOLADAS[0]
    assert quarentena.schema[-1].dataType.simpleString() == "array<string>"


def test_comentarios_sem_aspas_simples(tabela):
    # O schema vira DDL: uma aspa simples num comentário quebraria a declaração da tabela.
    assert all("'" not in comentario for _, _, comentario in tabela.colunas)


def test_nomes_unicos():
    nomes = [t.nome for t in TABELAS]
    assert len(nomes) == len(set(nomes))
