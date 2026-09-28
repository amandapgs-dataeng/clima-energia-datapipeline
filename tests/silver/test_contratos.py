"""Contrato que toda tabela da silver cumpre: a view padronizada produz exatamente as
colunas e tipos declarados (o schema das tabelas do pipeline), e as regras e chaves
referenciam colunas que existem."""
import pytest

from utilities import carga_energia, clima, fator_capacidade, geracao_usina, previsao_programado
from utilities.historico import SEQUENCIA, nomes

# (dataset da bronze, função, colunas declaradas, chave, colunas versionadas, regras)
TABELAS = {
    "carga_energia": ("carga_energia", carga_energia.padronizar_carga_energia, carga_energia.COLUNAS,
                      carga_energia.CHAVE, carga_energia.COLUNAS_VERSIONADAS,
                      {**carga_energia.REGRAS_DESCARTE, **carga_energia.REGRAS_ALERTA}),
    "geracao_usina": ("geracao_usina", geracao_usina.padronizar_geracao_usina, geracao_usina.COLUNAS,
                      geracao_usina.CHAVE, geracao_usina.COLUNAS_VERSIONADAS,
                      {**geracao_usina.REGRAS_DESCARTE, **geracao_usina.REGRAS_ALERTA}),
    "fator_capacidade": ("fator_capacidade", fator_capacidade.padronizar_fator_capacidade, fator_capacidade.COLUNAS,
                         fator_capacidade.CHAVE, fator_capacidade.COLUNAS_VERSIONADAS,
                         {**fator_capacidade.REGRAS_DESCARTE, **fator_capacidade.REGRAS_ALERTA}),
    "previsao_programado": ("previsao_programado", previsao_programado.padronizar_previsao_programado,
                            previsao_programado.COLUNAS, previsao_programado.CHAVE,
                            previsao_programado.COLUNAS_VERSIONADAS,
                            {**previsao_programado.REGRAS_DESCARTE, **previsao_programado.REGRAS_ALERTA}),
    "clima_previsao": ("clima_previsao", clima.padronizar_clima_previsao, clima.COLUNAS_PREVISAO,
                       clima.CHAVE_PREVISAO, [], {**clima.REGRAS_DESCARTE, **clima.REGRAS_ALERTA}),
    "clima_observado": ("clima_observado", clima.padronizar_clima_observado, clima.COLUNAS_OBSERVADO,
                        clima.CHAVE_OBSERVADO, [], {**clima.REGRAS_DESCARTE, **clima.REGRAS_ALERTA}),
}


@pytest.fixture(params=list(TABELAS), ids=list(TABELAS))
def tabela(request, bronze):
    dataset, funcao, colunas, chave, versionadas, regras = TABELAS[request.param]
    return funcao(bronze(dataset, [])), colunas, chave, versionadas, regras


def test_colunas_e_tipos_iguais_ao_declarado(tabela):
    saida, colunas, *_ = tabela
    esperado = [(nome, tipo.lower()) for nome, tipo, _ in colunas] + [(SEQUENCIA, "timestamp")]
    obtido = [(campo.name, campo.dataType.simpleString()) for campo in saida.schema]
    assert obtido == esperado


def test_chave_e_colunas_versionadas_existem(tabela):
    saida, _, chave, versionadas, _ = tabela
    assert set(chave) <= set(saida.columns)
    assert set(versionadas) <= set(saida.columns)
    assert not set(chave) & set(versionadas)
    assert SEQUENCIA not in versionadas  # senão toda releitura idêntica viraria uma versão


def test_regras_sao_sql_valido(tabela):
    saida, *_, regras = tabela
    for expressao in regras.values():
        saida.filter(expressao).count()  # falha se a expressão ou uma coluna não existir


def test_comentarios_sem_aspas_simples():
    # O schema vira DDL: uma aspa simples num comentário quebraria a declaração da tabela.
    for _, _, colunas, *_ in TABELAS.values():
        assert all("'" not in comentario for _, _, comentario in colunas)
    assert nomes(carga_energia.COLUNAS)[0] == "id_subsistema"
