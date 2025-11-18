"""Simple orchestrator to run Extract → Validate → Transform → Load sequentially."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

from src.pipeline.extract import extract_sources
from src.pipeline.load import load_all
from src.pipeline.transform import transform_datasets
from src.pipeline.validate import validate_dataframes
from src.utils.db_connector import DBConnector
from src.utils.logger import get_logger

LOGGER = get_logger(__name__)


def run_pipeline(config_path: Optional[str] = None, limit: Optional[int] = None) -> None:
    LOGGER.info("Pipeline started")

    extraction = extract_sources(config_path=config_path, limit=limit)
    LOGGER.info("Extraction completed")

    validate_dataframes(extraction.dataframes)
    LOGGER.info("Validation completed")

    transformation = transform_datasets(extraction)
    LOGGER.info("Transformation completed")

    connector = DBConnector(config_path=config_path)
    load_all(connector, transformation)
    LOGGER.info("Load completed. Pipeline finished successfully.")


def main(config_path: Optional[str] = None, limit: Optional[int] = None) -> None:
    try:
        run_pipeline(config_path=config_path, limit=limit)
    except Exception as exc:
        LOGGER.exception("Pipeline failed: %s", exc)
        raise


if __name__ == "__main__":
    main()  # pragma: no cover
