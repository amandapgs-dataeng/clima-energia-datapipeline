"""Vocabulário comum entre as fontes (o mesmo conceito aparece com nomes diferentes)."""
from itertools import chain

from pyspark.sql import Column
from pyspark.sql import functions as F

# id_subsistema é consistente entre os datasets do ONS; os nomes não são
# ("NORDESTE" x "Nordeste", "SUDESTE" x "Sudeste/Centro-Oeste").
SUBSISTEMAS = {
    "N": "Norte",
    "NE": "Nordeste",
    "S": "Sul",
    "SE": "Sudeste/Centro-Oeste",
}


def codigo_subsistema(coluna):
    """Código do subsistema sem espaços e em maiúsculas."""
    return F.upper(F.trim(F.col(coluna)))


def nome_subsistema(codigo: Column) -> Column:
    """Nome padronizado do subsistema; nulo para códigos desconhecidos."""
    mapa = F.create_map(*[F.lit(valor) for valor in chain.from_iterable(SUBSISTEMAS.items())])
    return mapa[codigo]
