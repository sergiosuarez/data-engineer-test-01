"""Extraction utilities that read raw CSV sources defined in config.yaml."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from src.utils.db_connector import load_pipeline_config
from src.utils.logger import get_logger

LOGGER = get_logger(__name__)


@dataclass
class ExtractionResult:
    """Container for extracted DataFrames and metadata."""

    dataframes: Dict[str, pd.DataFrame]
    metadata: Dict[str, Dict[str, Any]]


def _read_source(
    name: str,
    source_cfg: Dict[str, Any],
    base_dir: Path,
    limit: Optional[int] = None,
) -> ExtractionResult:
    file_name = source_cfg.get("file")
    if not file_name:
        raise ValueError(f"Source `{name}` is missing the `file` attribute in config.yaml.")

    file_path = base_dir / file_name
    if not file_path.exists():
        raise FileNotFoundError(f"Source `{name}` file not found: {file_path}")

    date_col = source_cfg.get("date_column")

    LOGGER.info("Reading source `%s` from %s", name, file_path)
    df = pd.read_csv(file_path)
    if date_col and date_col in df.columns:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    if limit:
        df = df.head(limit)

    metadata = {
        "path": str(file_path),
        "row_count": int(len(df)),
        "columns": df.columns.tolist(),
        "primary_key": source_cfg.get("primary_key"),
        "date_column": date_col,
    }
    return ExtractionResult({name: df}, {name: metadata})


def extract_sources(
    config_path: Optional[str] = None,
    limit: Optional[int] = None,
) -> ExtractionResult:
    """
    Read every configured raw source into memory.

    Parameters
    ----------
    config_path : Optional[str]
        Alternative path to the YAML configuration.
    limit : Optional[int]
        Row limit applied uniformly to each dataset (useful for tests).
    """

    config = load_pipeline_config(config_path)
    base_dir = Path(config.get("paths", {}).get("raw_data_dir", "data"))
    if not base_dir.exists():
        raise FileNotFoundError(f"Raw data directory does not exist: {base_dir}")

    dataframes: Dict[str, pd.DataFrame] = {}
    metadata: Dict[str, Dict[str, Any]] = {}

    for source_name, source_cfg in config.get("sources", {}).items():
        result = _read_source(source_name, source_cfg, base_dir, limit)
        dataframes.update(result.dataframes)
        metadata.update(result.metadata)

    LOGGER.info("Extraction completed for %d sources", len(dataframes))
    return ExtractionResult(dataframes=dataframes, metadata=metadata)


def main(config_path: Optional[str] = None, limit: Optional[int] = None) -> ExtractionResult:
    """CLI-friendly entrypoint."""

    return extract_sources(config_path=config_path, limit=limit)


if __name__ == "__main__":
    main()  # pragma: no cover
