"""Normalização dos nomes que aparecem nos datasets do ONS.

Os filtros Spark dos notebooks usam as mesmas constantes daqui; as funções
são a referência testada da regra (e servem para a camada Silver).
"""
import unicodedata

SUBSISTEMA_ALVO = "NORDESTE"
ID_SUBSISTEMA_ALVO = "NE"

# Nomes oficiais do ONS: a eólica é "EOLIELÉTRICA", não "EÓLICA".
TIPO_EOLICA = "EOLIELÉTRICA"
TIPO_SOLAR = "FOTOVOLTAICA"
TIPOS_USINA_ALVO = (TIPO_EOLICA, TIPO_SOLAR)

_SINONIMOS_TIPO_USINA = {
    "EOLIELETRICA": TIPO_EOLICA,
    "EOLICA": TIPO_EOLICA,
    "FOTOVOLTAICA": TIPO_SOLAR,
}


def normalizar_texto(valor):
    """Maiúsculas, sem espaços nas pontas e com espaços internos únicos."""
    if valor is None:
        return ""
    return " ".join(str(valor).split()).upper()


def remover_acentos(valor):
    decomposto = unicodedata.normalize("NFKD", valor)
    return "".join(c for c in decomposto if not unicodedata.combining(c))


def eh_subsistema_alvo(nome_subsistema):
    """True para "NORDESTE", "Nordeste", "SME NORDESTE" etc."""
    return SUBSISTEMA_ALVO in normalizar_texto(nome_subsistema)


def normalizar_tipo_usina(nome_tipo):
    """Converte variações ("Eólica", "EOLIELETRICA"...) para o nome oficial do ONS."""
    texto = normalizar_texto(nome_tipo)
    return _SINONIMOS_TIPO_USINA.get(remover_acentos(texto), texto)


def eh_tipo_usina_alvo(nome_tipo):
    return normalizar_tipo_usina(nome_tipo) in TIPOS_USINA_ALVO
