import pytest

from utils.normalization_helpers import (
    TIPO_EOLICA,
    TIPO_SOLAR,
    TIPOS_USINA_ALVO,
    eh_subsistema_alvo,
    eh_tipo_usina_alvo,
    normalizar_texto,
    normalizar_tipo_usina,
)


class TestNormalizarTexto:
    def test_maiusculas_e_espacos(self):
        assert normalizar_texto("  sme   Nordeste ") == "SME NORDESTE"

    def test_none_vira_vazio(self):
        assert normalizar_texto(None) == ""


class TestSubsistemaAlvo:
    @pytest.mark.parametrize("nome", ["NORDESTE", "Nordeste", "nordeste", "SME NORDESTE", " Nordeste "])
    def test_variacoes_do_nordeste(self, nome):
        assert eh_subsistema_alvo(nome)

    @pytest.mark.parametrize("nome", ["NORTE", "SUDESTE", "SUDESTE/CENTRO-OESTE", "SUL", "", None])
    def test_outros_subsistemas(self, nome):
        assert not eh_subsistema_alvo(nome)


class TestTipoUsina:
    def test_nome_oficial_da_eolica_e_eolieletrica(self):
        assert TIPO_EOLICA == "EOLIELÉTRICA"
        assert TIPOS_USINA_ALVO == ("EOLIELÉTRICA", "FOTOVOLTAICA")

    @pytest.mark.parametrize("nome", ["EOLIELÉTRICA", "Eolielétrica", "EOLIELETRICA", "EÓLICA", "eolica"])
    def test_variacoes_da_eolica(self, nome):
        assert normalizar_tipo_usina(nome) == TIPO_EOLICA

    @pytest.mark.parametrize("nome", ["FOTOVOLTAICA", "Fotovoltaica", " fotovoltaica "])
    def test_variacoes_da_solar(self, nome):
        assert normalizar_tipo_usina(nome) == TIPO_SOLAR

    @pytest.mark.parametrize("nome", ["HIDROELÉTRICA", "TÉRMICA", "NUCLEAR"])
    def test_outros_tipos_ficam_como_estao_e_nao_sao_alvo(self, nome):
        assert normalizar_tipo_usina(nome) == nome
        assert not eh_tipo_usina_alvo(nome)

    @pytest.mark.parametrize("nome", ["EÓLICA", "EOLIELÉTRICA", "Fotovoltaica"])
    def test_renovaveis_sao_alvo(self, nome):
        assert eh_tipo_usina_alvo(nome)
