"""Chamadas HTTP às fontes externas (ONS e Open-Meteo) com retry e timeout."""
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

TIMEOUT_SEGUNDOS = (10, 120)  # (conexão, leitura)
STATUS_TRANSITORIOS = (429, 500, 502, 503, 504)
TAMANHO_BLOCO = 1024 * 1024


class ArquivoIndisponivel(Exception):
    """A fonte respondeu 404: o arquivo não existe (ou ainda não foi publicado)."""


def criar_sessao(tentativas=5, backoff=2.0):
    """Sessão que repete falhas transitórias com espera exponencial (2s, 4s, 8s, 16s...)."""
    retry = Retry(
        total=tentativas,
        backoff_factor=backoff,
        status_forcelist=STATUS_TRANSITORIOS,
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    sessao = requests.Session()
    sessao.mount("https://", adapter)
    sessao.mount("http://", adapter)
    return sessao


def buscar_json(url, params=None, sessao=None):
    sessao = sessao or criar_sessao()
    resposta = sessao.get(url, params=params, timeout=TIMEOUT_SEGUNDOS)
    resposta.raise_for_status()
    return resposta.json()


def baixar_arquivo(url, destino, sessao=None):
    """Baixa `url` para `destino` em blocos, sem carregar o arquivo inteiro em memória."""
    sessao = sessao or criar_sessao()
    with sessao.get(url, stream=True, timeout=TIMEOUT_SEGUNDOS) as resposta:
        if resposta.status_code == 404:
            raise ArquivoIndisponivel(url)
        resposta.raise_for_status()
        with open(destino, "wb") as arquivo:
            for bloco in resposta.iter_content(chunk_size=TAMANHO_BLOCO):
                arquivo.write(bloco)
    return destino
