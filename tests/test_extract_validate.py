import json
from pathlib import Path

import pandas as pd
import pytest
import yaml

from src.pipeline.extract import extract_sources
from src.pipeline.validate import validate_dataframes


@pytest.fixture()
def sample_config(tmp_path: Path) -> Path:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()

    listings_csv = """id,name,host_id,host_name,neighbourhood,room_type,price,minimum_nights,availability_365,number_of_reviews,reviews_per_month,calculated_host_listings_count,last_review
1,Loft,101,Alice,Downtown,Entire home/apt,150,2,200,42,1.2,2,2024-01-01
2,Studio,202,Bob,Midtown,Private room,85,1,150,5,0.4,1,2024-02-15
"""
    reviews_csv = """listing_id,date
1,2024-01-03
2,2024-02-20
"""
    (raw_dir / "listings.csv").write_text(listings_csv, encoding="utf-8")
    (raw_dir / "reviews.csv").write_text(reviews_csv, encoding="utf-8")

    config = {
        "paths": {"raw_data_dir": str(raw_dir)},
        "sources": {
            "listings": {"file": "listings.csv", "primary_key": "id", "date_column": "last_review"},
            "reviews": {"file": "reviews.csv", "primary_key": "listing_id", "date_column": "date"},
        },
    }

    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return config_path


def test_extract_sources_reads_all_configured_files(sample_config: Path):
    result = extract_sources(config_path=str(sample_config))

    assert set(result.dataframes.keys()) == {"listings", "reviews"}
    listings_df = result.dataframes["listings"]
    reviews_df = result.dataframes["reviews"]

    assert len(listings_df) == 2
    assert "last_review" in listings_df.columns
    assert pd.api.types.is_datetime64_any_dtype(listings_df["last_review"])

    assert len(reviews_df) == 2
    assert result.metadata["listings"]["row_count"] == 2


def test_validate_dataframes_writes_quality_report(tmp_path: Path, sample_config: Path):
    extraction = extract_sources(config_path=str(sample_config))
    report_path = tmp_path / "report.json"

    payload = validate_dataframes(extraction.dataframes, report_path=str(report_path))

    assert payload["summary"]["invalid_datasets"] == 0
    assert report_path.exists()

    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["summary"]["valid_datasets"] == 2
    assert all(dataset["passed"] for dataset in report["datasets"])
