from .engine.executor import Executor
from .engine.scheduler import Scheduler
from .engine.faasr_payload import FaaSrPayload
from .s3_api import faasr_log
from .config.logger_classes import JsonFormatter, FaaSrFilter
from .config.debug_config import global_config
from .config.s3_log_sender import S3LogSender
from .helpers.faasr_start_invoke_helper import (
    faasr_func_dependancy_install,
    faasr_get_github_raw,
)


import sys
import logging
import json


logger = logging.getLogger()
logger.setLevel(logging.NOTSET)

# clear existing handlers
logger.handlers.clear()

# add a new handler to log to stdout -- [LEVEL] [FILENAME] [TIMESTAMP] [MESSAGE]
stdout_handler = logging.StreamHandler(sys.stdout)
if global_config.READABLE_LOGS:
    formatter = logging.Formatter("[%(levelname)s] [%(filename)s] %(message)s")
else:
    formatter = JsonFormatter()
stdout_handler.setFormatter(formatter)
stdout_handler.setLevel(logging.INFO)
stdout_handler.addFilter(FaaSrFilter())
logger.addHandler(stdout_handler)


__all__ = [
    "FaaSr",
    "Scheduler",
    "Executor",
    "faasr_log",
    "debug_config",
    "faasr_replace_values",
]
