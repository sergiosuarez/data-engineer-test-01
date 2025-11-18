"""Centralized logging utilities for the Airbnb analytics pipeline."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
DEFAULT_LOG_FILE = "pipeline_execution.log"


def _ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def get_logger(
    name: str,
    log_level: Optional[str] = None,
    log_dir: Optional[str] = None,
    propagate: bool = False,
) -> logging.Logger:
    """
    Return a logger configured with console + file handlers.

    Parameters
    ----------
    name : str
        Logger name, usually ``__name__`` of the caller.
    log_level : Optional[str]
        Override for the log level (INFO, DEBUG, ...). Falls back to LOG_LEVEL env var.
    log_dir : Optional[str]
        Directory where the log file will be stored. Defaults to LOG_DIR env var or ./logs.
    propagate : bool
        Whether log records should bubble up to parent loggers (defaults to False).
    """

    level_name = (log_level or os.getenv("LOG_LEVEL", "INFO")).upper()
    level = getattr(logging, level_name, logging.INFO)
    log_directory = Path(log_dir or os.getenv("LOG_DIR", "logs"))
    _ensure_directory(log_directory)
    log_file = log_directory / os.getenv("LOG_FILE", DEFAULT_LOG_FILE)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = propagate

    if not logger.handlers:
        formatter = logging.Formatter(LOG_FORMAT, DATE_FORMAT)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        stream_handler.setLevel(level)

        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        file_handler.setLevel(level)

        logger.addHandler(stream_handler)
        logger.addHandler(file_handler)

    return logger
