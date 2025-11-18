import pandas as pd

from src.pipeline.extract import ExtractionResult
from src.pipeline.transform import transform_datasets


def _build_sample_extraction() -> ExtractionResult:
    listings = pd.DataFrame(
        {
            "id": [1, 2],
            "name": ["Loft", "Studio"],
            "host_id": [100, 200],
            "host_name": ["Alice", "Bob"],
            "host_since": ["2020-01-01", "2019-06-15"],
            "host_response_rate": ["90%", "75%"],
            "host_is_superhost": ["t", "f"],
            "host_listings_count": [3, 1],
            "calculated_host_listings_count": [4, 1],
            "host_identity_verified": ["yes", "no"],
            "neighbourhood": ["Centro", "Norte"],
            "neighbourhood_group": ["City", "City"],
            "room_type": ["Entire home/apt", "Private room"],
            "price": ["$120", "85"],
            "minimum_nights": [2, 1],
            "availability_365": [200, 150],
            "amenities": ["['Wifi','TV']", "['Wifi']"],
            "last_review": ["2024-01-10", "2024-02-20"],
        }
    )
    reviews = pd.DataFrame({"listing_id": [1], "date": ["2024-01-15"]})
    return ExtractionResult(dataframes={"listings": listings, "reviews": reviews}, metadata={})


def test_transform_creates_expected_dimensions_and_facts():
    extraction = _build_sample_extraction()
    result = transform_datasets(extraction)

    assert set(result.dimensions.keys()) == {
        "dim_host",
        "dim_listing",
        "dim_neighborhood",
        "dim_property_type",
        "dim_date",
    }
    host_dim = result.dimensions["dim_host"]
    assert bool(host_dim.loc[host_dim["host_id"] == 100, "host_is_superhost"].iloc[0]) is True

    fact_listing = result.facts["fact_listing_daily_metrics"]
    assert "estimated_revenue" in fact_listing.columns
    loft_row = fact_listing[fact_listing["listing_id"] == 1].iloc[0]
    assert loft_row["price_tier"] == "standard"
    assert round(loft_row["occupancy_rate"], 4) == round((365 - 200) / 365, 4)


def test_dim_date_captures_all_observed_dates():
    extraction = _build_sample_extraction()
    dim_date = transform_datasets(extraction).dimensions["dim_date"]
    assert set(dim_date["date_key"].tolist()) == {20240110, 20240220, 20240115}
    assert len(dim_date) == 3
