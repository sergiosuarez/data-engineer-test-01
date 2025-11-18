"""Load utilities that persist dimension and fact tables with SCD2 support."""

from __future__ import annotations

from typing import Iterable, List, Optional

import pandas as pd

from src.pipeline.extract import extract_sources
from src.pipeline.transform import TransformationResult, transform_datasets
from src.utils.db_connector import DBConnector
from src.utils.logger import get_logger

LOGGER = get_logger(__name__)


def _ensure_columns(dataframe: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    df = dataframe.copy()
    for column in columns:
        if column not in df.columns:
            df[column] = pd.NA
    return df[columns]


def _ensure_schema(connector: DBConnector, schema: str) -> None:
    connector.run_query(f"CREATE SCHEMA IF NOT EXISTS {schema}")


def _diff_condition(alias_left: str, alias_right: str, columns: Iterable[str]) -> str:
    return " OR ".join(
        [
            f"COALESCE({alias_left}.{col}::text, '') IS DISTINCT FROM COALESCE({alias_right}.{col}::text, '')"
            for col in columns
        ]
    )


def _stage_dataframe(
    connector: DBConnector,
    dataframe: pd.DataFrame,
    staging_schema: str,
    staging_table: str,
) -> None:
    _ensure_schema(connector, staging_schema)
    dataframe = dataframe.copy()
    connector.load_dataframe(
        dataframe,
        table_name=staging_table,
        schema=staging_schema,
        if_exists="replace",
    )


def scd2_upsert(
    connector: DBConnector,
    dataframe: pd.DataFrame,
    table_name: str,
    natural_key: str,
    tracked_columns: List[str],
    schema: Optional[str] = None,
    columns: Optional[List[str]] = None,
) -> None:
    """
    Perform a Slowly Changing Dimension Type 2 upsert using staging tables.

    Parameters
    ----------
    connector : DBConnector
        Database connector aware of schemas and credentials.
    dataframe : pandas.DataFrame
        Latest snapshot of the dimension with the natural key and tracked columns.
    table_name : str
        Target dimension table (e.g., dim_host).
    natural_key : str
        Column used to uniquely identify business entities (host_id, listing_id).
    tracked_columns : List[str]
        Columns that trigger a new version when changes are detected.
    schema : Optional[str]
        Warehouse schema override. Defaults to connector.db_config.schema.
    """

    if dataframe.empty:
        LOGGER.warning("No records supplied for %s; skipping SCD2 upsert.", table_name)
        return

    target_schema = schema or connector.db_config.schema
    staging_schema = connector.db_config.staging_schema or target_schema
    staging_table = f"{table_name}_staging"

    now_ts = pd.Timestamp.utcnow()
    staged_df = dataframe.copy()
    if columns:
        staged_df = staged_df[columns]
    staged_df = staged_df.drop_duplicates(subset=[natural_key]).copy()
    if natural_key not in staged_df.columns:
        raise KeyError(f"Natural key `{natural_key}` not present in dataframe for {table_name}.")
    if not tracked_columns:
        tracked_columns = [
            col
            for col in staged_df.columns
            if col not in {natural_key, "effective_from", "effective_to", "is_current"}
        ]
    for column in tracked_columns:
        if column not in staged_df.columns:
            staged_df[column] = pd.NA
    staged_df["effective_from"] = now_ts
    staged_df["effective_to"] = pd.NaT
    staged_df["is_current"] = True

    _stage_dataframe(connector, staged_df, staging_schema, staging_table)

    diff = _diff_condition("dim", "s", tracked_columns)

    update_sql = f"""
        WITH staged AS (
            SELECT * FROM {staging_schema}.{staging_table}
        )
        UPDATE {target_schema}.{table_name} AS dim
        SET effective_to = NOW(),
            is_current = FALSE
        FROM staged s
        WHERE dim.is_current = TRUE
          AND dim.{natural_key} = s.{natural_key}
          AND ({diff});
    """
    connector.run_query(update_sql)

    columns = [col for col in staged_df.columns if col not in {"host_key", "listing_key"}]
    column_list = ", ".join(columns)
    select_list = ", ".join([f"s.{col}" for col in columns])

    insert_sql = f"""
        WITH staged AS (
            SELECT * FROM {staging_schema}.{staging_table}
        ), changed AS (
            SELECT s.*
            FROM staged s
            LEFT JOIN {target_schema}.{table_name} dim
              ON dim.{natural_key} = s.{natural_key}
             AND dim.is_current = TRUE
            WHERE dim.{natural_key} IS NULL
               OR ({diff})
        )
        INSERT INTO {target_schema}.{table_name} ({column_list})
        SELECT {select_list} FROM changed;
    """
    connector.run_query(insert_sql)


def replace_dimension_snapshot(
    connector: DBConnector,
    dataframe: pd.DataFrame,
    table_name: str,
    schema: Optional[str] = None,
) -> None:
    """Replace Type 1 dimensions (date, neighborhood, property_type) using truncate + insert."""

    if dataframe.empty:
        LOGGER.warning("No rows provided for %s; skipping.", table_name)
        return

    target_schema = schema or connector.db_config.schema
    truncate_sql = f"TRUNCATE TABLE {target_schema}.{table_name} RESTART IDENTITY CASCADE;"
    connector.run_query(truncate_sql)
    connector.load_dataframe(
        dataframe,
        table_name=table_name,
        schema=target_schema,
        if_exists="append",
    )


def append_fact(
    connector: DBConnector,
    dataframe: pd.DataFrame,
    table_name: str,
    schema: Optional[str] = None,
) -> None:
    """Append fact records (append-only)."""

    if dataframe.empty:
        LOGGER.warning("No fact rows provided for %s; skipping append.", table_name)
        return
    target_schema = schema or connector.db_config.schema
    connector.load_dataframe(
        dataframe,
        table_name=table_name,
        schema=target_schema,
        if_exists="append",
    )


def load_all(
    connector: DBConnector,
    transformation: TransformationResult,
) -> None:
    """Load every dimension and fact DataFrame produced by the transform stage."""

    target_schema = connector.db_config.schema
    _ensure_schema(connector, target_schema)
    if connector.db_config.staging_schema:
        _ensure_schema(connector, connector.db_config.staging_schema)

    scd2_upsert(
        connector,
        _ensure_columns(
            transformation.dimensions["dim_host"],
            [
                "host_id",
                "host_name",
                "host_since",
                "host_response_time",
                "host_response_rate",
                "host_is_superhost",
                "host_listings_count",
                "host_total_listings",
                "host_verifications",
                "host_identity_verified",
            ],
        ),
        table_name="dim_host",
        natural_key="host_id",
        tracked_columns=[
            "host_name",
            "host_since",
            "host_response_time",
            "host_response_rate",
            "host_is_superhost",
            "host_listings_count",
            "host_total_listings",
            "host_verifications",
            "host_identity_verified",
        ],
        columns=[
            "host_id",
            "host_name",
            "host_since",
            "host_response_time",
            "host_response_rate",
            "host_is_superhost",
            "host_listings_count",
            "host_total_listings",
            "host_verifications",
            "host_identity_verified",
        ],
    )

    scd2_upsert(
        connector,
        _ensure_columns(
            transformation.dimensions["dim_listing"],
            [
                "listing_id",
                "host_id",
                "listing_name",
                "room_type",
                "accommodates",
                "bathrooms",
                "bedrooms",
                "beds",
                "amenities_hash",
                "cancellation_policy",
                "minimum_nights",
                "maximum_nights",
                "instant_bookable",
                "neighborhood",
            ],
        ),
        table_name="dim_listing",
        natural_key="listing_id",
        tracked_columns=[
            "listing_name",
            "room_type",
            "accommodates",
            "bathrooms",
            "bedrooms",
            "beds",
            "amenities_hash",
            "cancellation_policy",
            "minimum_nights",
            "maximum_nights",
            "instant_bookable",
            "neighborhood",
        ],
        columns=[
            "listing_id",
            "host_id",
            "listing_name",
            "room_type",
            "accommodates",
            "bathrooms",
            "bedrooms",
            "beds",
            "amenities_hash",
            "cancellation_policy",
            "minimum_nights",
            "maximum_nights",
            "instant_bookable",
            "neighborhood",
        ],
    )

    replace_dimension_snapshot(connector, transformation.dimensions["dim_neighborhood"], "dim_neighborhood")
    replace_dimension_snapshot(connector, transformation.dimensions["dim_property_type"], "dim_property_type")
    if "dim_date" in transformation.dimensions:
        replace_dimension_snapshot(connector, transformation.dimensions["dim_date"], "dim_date")

    append_fact(connector, transformation.facts["fact_listing_daily_metrics"], "fact_listing_daily_metrics")
    append_fact(connector, transformation.facts["fact_review"], "fact_review")


def main(config_path: Optional[str] = None, limit: Optional[int] = None) -> None:
    extraction = extract_sources(config_path=config_path, limit=limit)
    transformation = transform_datasets(extraction)
    connector = DBConnector(config_path=config_path)
    load_all(connector, transformation)


if __name__ == "__main__":
    main()  # pragma: no cover
