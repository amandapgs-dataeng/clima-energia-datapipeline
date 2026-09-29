"""O dicionário da silver documenta toda coluna declarada no código (evita documentação defasada)."""
from pathlib import Path

import pytest

from utilities.catalogo import TABELAS
from utilities.padrao_silver import COLUNA_REGRAS_VIOLADAS, COLUNA_SEQUENCIA, COLUNAS_VERSAO, nomes

DICIONARIO = (Path(__file__).parents[2] / "docs" / "dicionario_silver.md").read_text(encoding="utf-8")

COLUNAS_POR_TABELA = {tabela.nome: tabela.colunas for tabela in TABELAS}


@pytest.mark.parametrize("tabela", list(COLUNAS_POR_TABELA))
def test_tabela_documentada(tabela):
    assert f"### `{tabela}`" in DICIONARIO


@pytest.mark.parametrize("coluna", sorted({n for cols in COLUNAS_POR_TABELA.values() for n in nomes(cols)}
                                          | set(nomes(COLUNAS_VERSAO)) | {COLUNA_SEQUENCIA[0], COLUNA_REGRAS_VIOLADAS[0]}))
def test_coluna_documentada(coluna):
    assert f"`{coluna}`" in DICIONARIO
