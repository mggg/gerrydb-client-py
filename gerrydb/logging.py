import logging
import colorlog
import os

log = logging.getLogger("gerrydb")


def setup_logging(level=logging.INFO):
    if log.hasHandlers():
        return

    handler = colorlog.StreamHandler()
    handler.setFormatter(
        colorlog.ColoredFormatter(
            "%(log_color)s%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            log_colors={
                "DEBUG": "cyan",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "bold_red",
            },
        )
    )
    log.setLevel(level)
    log.addHandler(handler)
    log.propagate = False

    log.debug("gerrydb logger is configured.")


if os.getenv("GERRYDB_DEBUG"):
    setup_logging(level=logging.DEBUG)
else:
    setup_logging(level=logging.INFO)
