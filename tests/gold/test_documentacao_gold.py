"""O dicionário da gold documenta toda tabela e coluna declarada no código."""
from pathlib import Path

import pytest

from metricas.catalogo import TABELAS
from metricas.padrao_gold import nomes

DICIONARIO = (Path(__file__).parents[2] / "docs" / "dicionario_gold.md").read_text(encoding="utf-8")


@pytest.mark.parametrize("tabela", [t.nome for t in TABELAS])
def test_tabela_documentada(tabela):
    assert f"### `{tabela}`" in DICIONARIO


@pytest.mark.parametrize("coluna", sorted({n for t in TABELAS for n in nomes(t.colunas)}))
def test_coluna_documentada(coluna):
    assert f"`{coluna}`" in DICIONARIO
