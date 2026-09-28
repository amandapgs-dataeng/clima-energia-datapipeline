import logging

from utils.logging_helpers import obter_logger


def test_chamar_duas_vezes_nao_duplica_mensagens():
    obter_logger("teste_duplicado")
    logger = obter_logger("teste_duplicado")
    assert len(logger.handlers) == 1


def test_formato_inclui_nivel_e_nome(capsys):
    obter_logger("teste_formato").info("olá")
    saida = capsys.readouterr().out
    assert "| INFO    | teste_formato | olá" in saida


def test_mostra_avisos_de_retry_do_urllib3(capsys):
    obter_logger("teste_urllib3")
    obter_logger("teste_urllib3_de_novo")
    logging.getLogger("urllib3.connectionpool").warning("Retrying (Retry(total=4))")
    logging.getLogger("urllib3.connectionpool").debug("detalhe interno")
    saida = capsys.readouterr().out
    assert saida.count("Retrying") == 1
    assert "detalhe interno" not in saida
