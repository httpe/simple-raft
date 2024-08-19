import logging
from enum import Enum

from uvicorn.config import LOGGING_CONFIG

logger = logging.getLogger("uvicorn.error")


class LogLevel(Enum):
    debug = "debug"
    info = "info"
    warning = "warning"
    error = "error"
    critical = "critical"
    fatal = "fatal"


def get_uvicorn_log_config():
    log_config = LOGGING_CONFIG
    # Add timestamp to logging
    log_config["formatters"]["default"][
        "fmt"
    ] = "%(asctime)s %(levelprefix)s %(message)s"
    log_config["formatters"]["access"][
        "fmt"
    ] = "%(asctime)s %(levelprefix)s %(message)s"
    return log_config


def attach_log_file(path: str):
    # create file handler which logs even debug messages
    fh = logging.FileHandler(path)
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    fh.setFormatter(formatter)
    logger.addHandler(fh)
