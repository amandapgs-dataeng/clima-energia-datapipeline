"""Contrato da gold: cada tabela, calculada a partir de tabelas com o schema real da silver,
produz exatamente as colunas e tipos declarados. Se a silver mudar uma coluna que a gold usa,
este teste falha."""
import pytest

from metricas.catalogo import TABELAS
from metricas.padrao_gold import ddl
from utilities.catalogo import TABELAS as TABELAS_SILVER
from utilities.padrao_silver import COLUNA_SEQUENCIA

SCHEMA_SILVER = {t.nome: ddl(t.colunas + [COLUNA_SEQUENCIA]).replace("\n", " ") for t in TABELAS_SILVER}
SCHEMA_GOLD = {t.nome: ddl(t.colunas).replace("\n", " ") for t in TABELAS}


def vazio(spark, schema_ddl):
    return spark.createDataFrame([], schema_ddl)


@pytest.fixture(params=TABELAS, ids=[t.nome for t in TABELAS])
def tabela(request):
    return request.param


def test_origens_existem(tabela):
    assert set(tabela.tabelas_silver) <= set(SCHEMA_SILVER)
    assert set(tabela.tabelas_gold) <= set(SCHEMA_GOLD)


def test_colunas_e_tipos_iguais_ao_declarado(spark, tabela):
    origens = [vazio(spark, SCHEMA_SILVER[n]) for n in tabela.tabelas_silver]
    origens += [vazio(spark, SCHEMA_GOLD[n]) for n in tabela.tabelas_gold]
    saida = tabela.calcular(*origens)
    esperado = [(nome, tipo.lower()) for nome, tipo, _ in tabela.colunas]
    assert [(c.name, c.dataType.simpleString()) for c in saida.schema] == esperado


def test_comentarios_sem_aspas_simples(tabela):
    assert all("'" not in comentario for _, _, comentario in tabela.colunas)


def test_nomes_unicos_e_sem_colisao_com_a_silver():
    nomes = [t.nome for t in TABELAS]
    assert len(nomes) == len(set(nomes))
    assert not set(nomes) & set(SCHEMA_SILVER)
