"""Transformation logic to build dimensional and fact dataframes."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional

import pandas as pd

from src.pipeline.extract import ExtractionResult, extract_sources
from src.utils.logger import get_logger

LOGGER = get_logger(__name__)


PRICE_TIERS = [
    (0, 100, "budget"),
    (100, 200, "standard"),
    (200, 400, "premium"),
    (400, math.inf, "luxury"),
]


def _clean_price(price_series: pd.Series) -> pd.Series:
    normalized = (
        price_series.astype(str)
        .str.replace(r"[^\d.-]", "", regex=True)
    )
    return pd.to_numeric(normalized, errors="coerce").fillna(0).clip(lower=0)


def _booleanize(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.strip()
        .str.lower()
        .isin(["true", "t", "1", "yes", "y"])
    )


def _price_tier(price: float) -> str:
    for lower, upper, label in PRICE_TIERS:
        if lower <= price < upper:
            return label
    return "unknown"


def _calculate_occupancy_rate(listing_df: pd.DataFrame) -> pd.Series:
    availability = listing_df.get("availability_365", pd.Series([0] * len(listing_df)))
    return ((365 - availability) / 365).clip(lower=0, upper=1).round(4)


def _calculate_estimated_revenue(prices: pd.Series, occupancy_rates: pd.Series) -> pd.Series:
    return (prices * occupancy_rates * 30).round(2)


def _transform_dim_host(listings: pd.DataFrame) -> pd.DataFrame:
    host_cols = [
        "host_id",
        "host_name",
        "host_since",
        "host_response_time",
        "host_response_rate",
        "host_is_superhost",
        "host_listings_count",
        "calculated_host_listings_count",
        "host_identity_verified",
    ]
    available_cols = [col for col in host_cols if col in listings.columns]
    host_df = listings[available_cols].drop_duplicates(subset=["host_id"]).copy()
    host_df["host_is_superhost"] = _booleanize(host_df.get("host_is_superhost", False))
    host_df["host_identity_verified"] = _booleanize(host_df.get("host_identity_verified", False))
    if "host_response_rate" in host_df.columns:
        host_df["host_response_rate"] = (
            host_df["host_response_rate"]
            .astype(str)
            .str.replace("%", "", regex=False)
        )
        host_df["host_response_rate"] = pd.to_numeric(host_df["host_response_rate"], errors="coerce")
    if "host_verifications" not in host_df.columns:
        host_df["host_verifications"] = ""
    host_df["host_since"] = pd.to_datetime(host_df.get("host_since"), errors="coerce")
    host_df.rename(columns={"calculated_host_listings_count": "host_total_listings"}, inplace=True)
    return host_df


def _transform_dim_listing(listings: pd.DataFrame) -> pd.DataFrame:
    listing_df = listings.copy()
    if "id" in listing_df.columns:
        listing_df.rename(columns={"id": "listing_id"}, inplace=True)
    if "name" in listing_df.columns:
        listing_df.rename(columns={"name": "listing_name"}, inplace=True)
    listing_df["price"] = _clean_price(listing_df.get("price", 0))
    for column in ["instant_bookable", "has_availability"]:
        if column in listing_df.columns:
            listing_df[column] = _booleanize(listing_df[column])
        else:
            listing_df[column] = False
    listing_df["amenities_hash"] = listing_df.get("amenities", "").astype(str).str.lower()
    if "neighbourhood" in listing_df.columns:
        listing_df.rename(columns={"neighbourhood": "neighborhood"}, inplace=True)
    if "neighbourhood_group" in listing_df.columns:
        listing_df.rename(columns={"neighbourhood_group": "neighborhood_group"}, inplace=True)
    for numeric_col in ["bathrooms", "bedrooms", "beds", "accommodates", "maximum_nights"]:
        if numeric_col not in listing_df.columns:
            listing_df[numeric_col] = pd.NA
    if "cancellation_policy" not in listing_df.columns:
        listing_df["cancellation_policy"] = "not_specified"
    return listing_df


def _transform_dim_neighborhood(listings: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    if "neighbourhood" in listings.columns:
        df["neighborhood_name"] = listings["neighbourhood"]
    else:
        df["neighborhood_name"] = pd.Series(dtype="string")
    if "neighbourhood_group" in listings.columns:
        df["city"] = listings["neighbourhood_group"]
    else:
        df["city"] = ""
    df["state"] = ""
    df["country"] = ""
    df["geo_hash"] = ""
    df = df.drop_duplicates(subset=["neighborhood_name"])
    return df


def _transform_dim_property_type(listings: pd.DataFrame) -> pd.DataFrame:
    if "property_type" in listings.columns:
        df = listings[["property_type", "room_type"]].drop_duplicates().copy()
        df.rename(columns={"property_type": "property_type_name", "room_type": "property_category"}, inplace=True)
    else:
        df = listings[["room_type"]].drop_duplicates().copy()
        df.rename(columns={"room_type": "property_type_name"}, inplace=True)
        df["property_category"] = df["property_type_name"]
    df["description"] = ""
    df["is_active"] = True
    return df


def _build_fact_listing_daily_metrics(listings: pd.DataFrame) -> pd.DataFrame:
    fact_df = listings.copy()
    if "id" in fact_df.columns:
        fact_df.rename(columns={"id": "listing_id"}, inplace=True)
    fact_df["price"] = _clean_price(fact_df.get("price", 0))
    for monetary_col in ["cleaning_fee", "security_deposit"]:
        if monetary_col not in fact_df.columns:
            fact_df[monetary_col] = 0
        else:
            fact_df[monetary_col] = _clean_price(fact_df[monetary_col])
    fact_df["minimum_nights"] = pd.to_numeric(fact_df.get("minimum_nights", 1), errors="coerce").fillna(1).astype(int)
    if "maximum_nights" not in fact_df.columns:
        fact_df["maximum_nights"] = fact_df["minimum_nights"]
    fact_df["occupancy_rate"] = _calculate_occupancy_rate(fact_df)
    fact_df["estimated_revenue"] = _calculate_estimated_revenue(fact_df["price"], fact_df["occupancy_rate"])
    fact_df["price_tier"] = fact_df["price"].apply(_price_tier)
    fact_df["date_key"] = pd.to_datetime(fact_df.get("last_review")).dt.strftime("%Y%m%d").astype("Int64")
    return fact_df


def _build_fact_review(reviews: pd.DataFrame) -> pd.DataFrame:
    fact_reviews = reviews.copy()
    fact_reviews["date_key"] = pd.to_datetime(fact_reviews["date"], errors="coerce").dt.strftime("%Y%m%d").astype("Int64")
    if "id" in fact_reviews.columns:
        fact_reviews["review_id"] = fact_reviews["id"]
    else:
        fact_reviews["review_id"] = pd.RangeIndex(start=1, stop=len(fact_reviews) + 1)
    return fact_reviews


def _build_dim_date(listings: pd.DataFrame, reviews: pd.DataFrame) -> pd.DataFrame:
    date_series = []
    if "last_review" in listings.columns:
        date_series.append(pd.to_datetime(listings["last_review"], errors="coerce"))
    if "date" in reviews.columns:
        date_series.append(pd.to_datetime(reviews["date"], errors="coerce"))
    if not date_series:
        return pd.DataFrame(
            columns=[
                "date_key",
                "full_date",
                "day_of_week",
                "day_name",
                "week_of_year",
                "month",
                "month_name",
                "quarter",
                "year",
                "is_weekend",
                "created_at",
            ]
        )
    dates = pd.concat(date_series).dropna().dt.normalize().drop_duplicates().sort_values()
    dim_date = pd.DataFrame({"full_date": dates})
    dim_date["date_key"] = dim_date["full_date"].dt.strftime("%Y%m%d").astype(int)
    dim_date["day_of_week"] = dim_date["full_date"].dt.weekday + 1
    dim_date["day_name"] = dim_date["full_date"].dt.day_name()
    dim_date["week_of_year"] = dim_date["full_date"].dt.isocalendar().week.astype(int)
    dim_date["month"] = dim_date["full_date"].dt.month
    dim_date["month_name"] = dim_date["full_date"].dt.month_name()
    dim_date["quarter"] = dim_date["full_date"].dt.quarter
    dim_date["year"] = dim_date["full_date"].dt.year
    dim_date["is_weekend"] = dim_date["day_of_week"].isin([6, 7])
    dim_date["created_at"] = pd.Timestamp.utcnow()
    return dim_date


@dataclass
class TransformationResult:
    dimensions: Dict[str, pd.DataFrame]
    facts: Dict[str, pd.DataFrame]


def transform_datasets(extraction: ExtractionResult) -> TransformationResult:
    listings = extraction.dataframes["listings"]
    reviews = extraction.dataframes.get("reviews", pd.DataFrame(columns=["listing_id", "date"]))

    dim_host = _transform_dim_host(listings)
    dim_listing = _transform_dim_listing(listings)
    dim_neighborhood = _transform_dim_neighborhood(listings)
    dim_property_type = _transform_dim_property_type(listings)
    dim_date = _build_dim_date(listings, reviews)

    fact_listing = _build_fact_listing_daily_metrics(listings)
    fact_reviews = _build_fact_review(reviews)

    dimensions = {
        "dim_host": dim_host,
        "dim_listing": dim_listing,
        "dim_neighborhood": dim_neighborhood,
        "dim_property_type": dim_property_type,
        "dim_date": dim_date,
    }
    facts = {
        "fact_listing_daily_metrics": fact_listing,
        "fact_review": fact_reviews,
    }

    LOGGER.info(
        "Transform generated %s dimensions and %s facts",
        len(dimensions),
        len(facts),
    )
    return TransformationResult(dimensions=dimensions, facts=facts)


def main(config_path: Optional[str] = None, limit: Optional[int] = None) -> TransformationResult:
    extraction = extract_sources(config_path=config_path, limit=limit)
    return transform_datasets(extraction)


if __name__ == "__main__":
    main()  # pragma: no cover
