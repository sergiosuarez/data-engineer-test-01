"""Data validation utilities powered by Pandera."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import pandera as pa
from pandera import Check, Column, DataFrameSchema
from pandera.errors import SchemaErrors

from src.utils.logger import get_logger

LOGGER = get_logger(__name__)

ROOM_TYPES = [
    "Entire home/apt",
    "Private room",
    "Shared room",
    "Hotel room",
]

LISTINGS_SCHEMA = DataFrameSchema(
    {
        "id": Column(pa.Int64, Check.gt(0), nullable=False, unique=True),
        "name": Column(pa.String, nullable=False),
        "host_id": Column(pa.Int64, Check.gt(0), nullable=False),
        "host_name": Column(pa.String, nullable=True),
        "neighbourhood": Column(pa.String, nullable=True),
        "room_type": Column(pa.String, Check.isin(ROOM_TYPES), nullable=False),
        "price": Column(pa.Float64, Check.ge(0)),
        "minimum_nights": Column(pa.Int64, Check.ge(1)),
        "availability_365": Column(pa.Int64, Check.in_range(0, 365)),
        "number_of_reviews": Column(pa.Int64, Check.ge(0)),
        "reviews_per_month": Column(pa.Float64, Check.ge(0), nullable=True),
        "calculated_host_listings_count": Column(pa.Int64, Check.ge(0)),
        "last_review": Column(pa.DateTime, nullable=True),
    },
    coerce=True,
    strict=False,
)

REVIEWS_SCHEMA = DataFrameSchema(
    {
        "listing_id": Column(pa.Int64, Check.gt(0)),
        "date": Column(pa.DateTime),
    },
    coerce=True,
    strict=False,
)

SCHEMA_REGISTRY = {
    "listings": LISTINGS_SCHEMA,
    "reviews": REVIEWS_SCHEMA,
}


@dataclass
class DatasetValidationResult:
    name: str
    passed: bool
    row_count: int
    error_details: List[Dict[str, object]]


def _validate_dataset(name: str, dataframe: pd.DataFrame) -> DatasetValidationResult:
    schema = SCHEMA_REGISTRY.get(name)
    if schema is None:
        LOGGER.warning("No schema registered for dataset `%s`. Skipping validation.", name)
        return DatasetValidationResult(name=name, passed=True, row_count=len(dataframe), error_details=[])

    try:
        schema.validate(dataframe, lazy=True)
        LOGGER.info("Validation passed for dataset `%s` (%s rows)", name, len(dataframe))
        return DatasetValidationResult(name=name, passed=True, row_count=len(dataframe), error_details=[])
    except SchemaErrors as err:
        failure_cases = err.failure_cases.copy()
        failure_cases["dataset"] = name
        details = failure_cases.to_dict(orient="records")
        LOGGER.error("Validation failed for dataset `%s` with %s issues", name, len(details))
        return DatasetValidationResult(name=name, passed=False, row_count=len(dataframe), error_details=details)


def _write_report(report_path: Path, payload: Dict[str, object]) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    LOGGER.info("Data quality report stored at %s", report_path)


def validate_dataframes(
    datasets: Dict[str, pd.DataFrame],
    report_path: Optional[str] = "output/data_quality_report.json",
) -> Dict[str, object]:
    """
    Validate the provided datasets and optionally persist a JSON report.

    Parameters
    ----------
    datasets : Dict[str, pandas.DataFrame]
        Mapping of dataset name to dataframe (typically output from extract step).
    report_path : Optional[str]
        Path to write the data quality report. If None, no file is written.
    """

    results = [_validate_dataset(name, df) for name, df in datasets.items()]
    summary = {
        "validated_datasets": len(results),
        "valid_datasets": sum(1 for result in results if result.passed),
        "invalid_datasets": sum(1 for result in results if not result.passed),
    }

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": summary,
        "datasets": [
            {
                "name": result.name,
                "row_count": result.row_count,
                "passed": result.passed,
                "issues": result.error_details,
            }
            for result in results
        ],
    }

    if report_path:
        _write_report(Path(report_path), payload)

    return payload


def main(report_path: Optional[str] = "output/data_quality_report.json") -> Dict[str, object]:
    """Convenience CLI entrypoint that runs extract + validate."""

    from src.pipeline.extract import extract_sources  # Local import to avoid circular dependencies

    extraction = extract_sources()
    return validate_dataframes(extraction.dataframes, report_path=report_path)


if __name__ == "__main__":
    main()  # pragma: no cover
