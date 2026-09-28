import pytest
import requests

from utils.http_helpers import (
    STATUS_TRANSITORIOS,
    TIMEOUT_SEGUNDOS,
    ArquivoIndisponivel,
    baixar_arquivo,
    buscar_json,
    criar_sessao,
)


class RespostaFalsa:
    def __init__(self, status_code=200, conteudo=b"", json_data=None):
        self.status_code = status_code
        self._conteudo = conteudo
        self._json = json_data

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} Error", response=self)

    def iter_content(self, chunk_size):
        for i in range(0, len(self._conteudo), chunk_size):
            yield self._conteudo[i:i + chunk_size]

    def json(self):
        return self._json

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class SessaoFalsa:
    def __init__(self, resposta):
        self.resposta = resposta
        self.chamadas = []

    def get(self, url, **kwargs):
        self.chamadas.append((url, kwargs))
        return self.resposta


class TestCriarSessao:
    def test_repete_falhas_transitorias_com_backoff(self):
        retry = criar_sessao().get_adapter("https://ons.example").max_retries
        assert retry.total == 5
        assert retry.backoff_factor == 2.0
        assert set(retry.status_forcelist) == set(STATUS_TRANSITORIOS)

    def test_nao_repete_erros_do_cliente(self):
        retry = criar_sessao().get_adapter("https://ons.example").max_retries
        assert 404 not in retry.status_forcelist
        assert 400 not in retry.status_forcelist

    def test_retry_vale_para_http_e_https(self):
        sessao = criar_sessao(tentativas=3)
        assert sessao.get_adapter("http://x").max_retries.total == 3
        assert sessao.get_adapter("https://x").max_retries.total == 3


class TestBuscarJson:
    def test_devolve_json_e_usa_timeout(self):
        sessao = SessaoFalsa(RespostaFalsa(json_data={"hourly": {}}))
        assert buscar_json("https://api", params={"a": 1}, sessao=sessao) == {"hourly": {}}
        _, kwargs = sessao.chamadas[0]
        assert kwargs["params"] == {"a": 1}
        assert kwargs["timeout"] == TIMEOUT_SEGUNDOS

    def test_erro_http_propaga(self):
        with pytest.raises(requests.HTTPError):
            buscar_json("https://api", sessao=SessaoFalsa(RespostaFalsa(status_code=500)))


class TestBaixarArquivo:
    def test_grava_conteudo_no_destino(self, tmp_path):
        destino = tmp_path / "arquivo.parquet"
        conteudo = b"x" * (3 * 1024 * 1024 + 10)  # mais de um bloco
        baixar_arquivo("https://ons/a.parquet", destino, sessao=SessaoFalsa(RespostaFalsa(conteudo=conteudo)))
        assert destino.read_bytes() == conteudo

    def test_usa_streaming_e_timeout(self, tmp_path):
        sessao = SessaoFalsa(RespostaFalsa(conteudo=b"abc"))
        baixar_arquivo("https://ons/a.parquet", tmp_path / "a", sessao=sessao)
        _, kwargs = sessao.chamadas[0]
        assert kwargs["stream"] is True
        assert kwargs["timeout"] == TIMEOUT_SEGUNDOS

    def test_404_vira_arquivo_indisponivel(self, tmp_path):
        with pytest.raises(ArquivoIndisponivel):
            baixar_arquivo("https://ons/a.parquet", tmp_path / "a", sessao=SessaoFalsa(RespostaFalsa(status_code=404)))

    def test_outros_erros_propagam_como_http_error(self, tmp_path):
        destino = tmp_path / "a"
        with pytest.raises(requests.HTTPError):
            baixar_arquivo("https://ons/a.parquet", destino, sessao=SessaoFalsa(RespostaFalsa(status_code=503)))
        assert not destino.exists()
