import logging
from enum import Enum

from uvicorn.config import LOGGING_CONFIG


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


logger = logging.getLogger("uvicorn.error")
