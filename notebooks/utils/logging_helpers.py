"""Logger padronizado para os notebooks (aparece na saída do job no Databricks)."""
import logging
import sys

FORMATO = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
FORMATO_HORA = "%Y-%m-%d %H:%M:%S"


class _HandlerDoProjeto(logging.StreamHandler):
    """Handler que adicionamos (o tipo evita duplicá-lo).

    Escreve sempre no `sys.stdout` atual, mesmo que ele seja trocado depois
    (o Databricks e o pytest redirecionam a saída).
    """

    def emit(self, record):
        self.stream = sys.stdout
        super().emit(record)


def _configurar(logger, nivel):
    if not any(isinstance(h, _HandlerDoProjeto) for h in logger.handlers):
        handler = _HandlerDoProjeto()
        handler.setFormatter(logging.Formatter(FORMATO, datefmt=FORMATO_HORA))
        logger.addHandler(handler)
    logger.setLevel(nivel)
    logger.propagate = False


def obter_logger(nome, nivel=logging.INFO):
    """Logger com saída no stdout; chamar de novo não duplica as mensagens."""
    logger = logging.getLogger(nome)
    _configurar(logger, nivel)
    # Avisos de retry do urllib3 ("Retrying (Retry(total=4...") também ficam visíveis.
    # (O urllib3 já vem com um NullHandler, por isso a checagem é pelo tipo do handler.)
    _configurar(logging.getLogger("urllib3"), logging.WARNING)
    return logger
