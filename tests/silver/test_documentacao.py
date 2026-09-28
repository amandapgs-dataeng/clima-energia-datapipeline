"""O dicionário da silver documenta toda coluna declarada no código (evita documentação defasada)."""
from pathlib import Path

import pytest

from utilities import carga_energia, clima, fator_capacidade, geracao_usina, previsao_programado
from utilities.historico import COLUNA_SEQUENCIA, COLUNAS_VERSAO, nomes

DICIONARIO = (Path(__file__).parents[2] / "docs" / "dicionario_silver.md").read_text(encoding="utf-8")

COLUNAS_POR_TABELA = {
    "carga_energia": carga_energia.COLUNAS,
    "geracao_usina": geracao_usina.COLUNAS,
    "fator_capacidade": fator_capacidade.COLUNAS,
    "previsao_programado": previsao_programado.COLUNAS,
    "clima_previsao": clima.COLUNAS_PREVISAO,
    "clima_observado": clima.COLUNAS_OBSERVADO,
}


@pytest.mark.parametrize("tabela", list(COLUNAS_POR_TABELA))
def test_tabela_documentada(tabela):
    assert f"### `{tabela}`" in DICIONARIO


@pytest.mark.parametrize("coluna", sorted({n for cols in COLUNAS_POR_TABELA.values() for n in nomes(cols)}
                                          | set(nomes(COLUNAS_VERSAO)) | {COLUNA_SEQUENCIA[0]}))
def test_coluna_documentada(coluna):
    assert f"`{coluna}`" in DICIONARIO
