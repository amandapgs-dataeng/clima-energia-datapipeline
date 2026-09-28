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

# Tipo de usina, já sem acentos e em maiúsculas -> nome padronizado.
# geracao_usina usa EOLIELÉTRICA/FOTOVOLTAICA; fator_capacidade usa Eólica/Solar.
TIPOS_USINA = {
    "HIDROELETRICA": "Hidrelétrica",
    "HIDRELETRICA": "Hidrelétrica",
    "TERMICA": "Térmica",
    "EOLIELETRICA": "Eólica",
    "EOLICA": "Eólica",
    "FOTOVOLTAICA": "Solar",
    "SOLAR": "Solar",
    "NUCLEAR": "Nuclear",
}

_COM_ACENTO = "ÁÀÂÃÉÈÊÍÌÎÓÒÔÕÚÙÛÇ"
_SEM_ACENTO = "AAAAEEEIIIOOOOUUUC"


def _mapa(dicionario):
    return F.create_map(*[F.lit(valor) for valor in chain.from_iterable(dicionario.items())])


def texto_limpo(coluna):
    """Texto sem espaços nas pontas; vazio vira nulo."""
    valor = F.trim(F.col(coluna))
    return F.when(valor != "", valor)


def sem_codigo(coluna):
    """Códigos com marcador de ausência ('-' ou vazio) viram nulo."""
    valor = F.trim(F.col(coluna))
    return F.when(~valor.isin("-", ""), valor)


def codigo_subsistema(coluna):
    """Código do subsistema sem espaços e em maiúsculas."""
    return F.upper(F.trim(F.col(coluna)))


def nome_subsistema(codigo: Column) -> Column:
    """Nome padronizado do subsistema; nulo para códigos desconhecidos."""
    return _mapa(SUBSISTEMAS)[codigo]


def tipo_usina(coluna):
    """Tipo de usina padronizado (Hidrelétrica, Térmica, Eólica, Solar, Nuclear); nulo se desconhecido."""
    chave = F.translate(F.upper(F.trim(F.col(coluna))), _COM_ACENTO, _SEM_ACENTO)
    return _mapa(TIPOS_USINA)[chave]


def modalidade_operacao(coluna):
    """'TIPO II-B' e 'Tipo II-B' viram 'Tipo II-B'; as demais modalidades ficam como vieram."""
    valor = F.trim(F.col(coluna))
    return (
        F.when(F.upper(valor).startswith("TIPO "), F.concat(F.lit("Tipo "), F.upper(F.substring(valor, 6, 50))))
        .otherwise(valor)
    )


def hora_local_ons(coluna):
    """Hora do ONS como horário local, sem fuso (TIMESTAMP_NTZ).

    O ONS grava horário de Brasília rotulado como UTC (ver docs/dicionario_dados.md). Com a
    sessão em UTC, o cast devolve exatamente o relógio publicado, sem deslocamento.
    """
    return F.col(coluna).cast("timestamp_ntz")
