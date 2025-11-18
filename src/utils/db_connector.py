"""
Database connector utilities for the Airbnb analytics pipeline.

This module centralizes configuration loading, SQLAlchemy engine creation,
and convenience helpers for running ad-hoc queries or persisting pandas
DataFrames. The implementation is intentionally lightweight so that it can
be reused both by local scripts and by orchestrated jobs (Airflow, Prefect, etc.).
"""

from __future__ import annotations

import logging
import os
import re
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Generator, Optional

import yaml
from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine, Result, create_engine

load_dotenv()

LOGGER = logging.getLogger(__name__)
DEFAULT_CONFIG_PATH = Path(os.getenv("PIPELINE_CONFIG", "src/config/config.yaml"))
ENV_PATTERN = re.compile(r"\$\{([^}:]+)(:-([^}]+))?\}")


@dataclass
class DatabaseConfig:
    """Resolved DB configuration used to build SQLAlchemy engines."""

    uri: str
    schema: str = "analytics"
    staging_schema: Optional[str] = None
    load_batch_size: int = 5000
    echo: bool = False


def _resolve_env_in_value(value: Any) -> Any:
    """Replace ${VAR:-default} style placeholders with environment values."""

    if not isinstance(value, str):
        return value

    def replacer(match: re.Match[str]) -> str:
        env_var = match.group(1)
        default = match.group(3) or ""
        return os.getenv(env_var, default)

    return ENV_PATTERN.sub(replacer, value)


def _resolve_env_in_structure(data: Any) -> Any:
    if isinstance(data, dict):
        return {k: _resolve_env_in_structure(v) for k, v in data.items()}
    if isinstance(data, list):
        return [_resolve_env_in_structure(item) for item in data]
    return _resolve_env_in_value(data)


def load_pipeline_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load YAML configuration with environment-variable expansion.

    Parameters
    ----------
    config_path: Optional[str]
        Path to the YAML config. Defaults to PIPELINE_CONFIG env var or src/config/config.yaml.
    """

    path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with path.open("r", encoding="utf-8") as cfg:
        raw = yaml.safe_load(cfg) or {}

    return _resolve_env_in_structure(raw)


def build_database_config(config: Optional[Dict[str, Any]] = None) -> DatabaseConfig:
    """Create a ``DatabaseConfig`` dataclass from loaded configuration."""

    cfg = config or load_pipeline_config()
    warehouse_cfg = cfg.get("warehouse", {})
    return DatabaseConfig(
        uri=warehouse_cfg.get("uri"),
        schema=warehouse_cfg.get("schema", "analytics"),
        staging_schema=warehouse_cfg.get("staging_schema"),
        load_batch_size=int(warehouse_cfg.get("load_batch_size", 5000)),
        echo=bool(warehouse_cfg.get("echo", False)),
    )


class DBConnector:
    """
    High-level helper that wraps SQLAlchemy engine creation and basic operations.

    Example
    -------
    >>> connector = DBConnector()
    >>> connector.run_query(\"\"\"SELECT count(*) FROM analytics.dim_listing\"\"\")
    """

    def __init__(
        self,
        config_path: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.logger = logger or LOGGER
        cfg = load_pipeline_config(config_path)
        self.db_config = build_database_config(cfg)
        if not self.db_config.uri:
            raise ValueError("Warehouse URI is missing in config.yaml or environment variables.")
        self.engine: Engine = create_engine(
            self.db_config.uri,
            future=True,
            echo=self.db_config.echo,
            pool_pre_ping=True,
        )
        self.logger.debug(
            "Initialized DB engine for %s (schema=%s)",
            self.db_config.uri,
            self.db_config.schema,
        )

    @contextmanager
    def connect(self) -> Generator[Connection, None, None]:
        """Yield a SQLAlchemy connection with automatic close."""

        connection = self.engine.connect()
        try:
            yield connection
        finally:
            connection.close()

    def run_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> Result:
        """Execute a query using SQLAlchemy ``text`` bindings and return the Result."""

        self.logger.debug("Executing query: %s | params=%s", query, params)
        with self.connect() as conn:
            return conn.execute(text(query), params or {})

    def load_dataframe(
        self,
        dataframe,
        table_name: str,
        schema: Optional[str] = None,
        if_exists: str = "append",
        chunksize: Optional[int] = None,
        method: str = "multi",
    ) -> None:
        """
        Persist a pandas DataFrame into the target warehouse using ``to_sql``.

        Parameters
        ----------
        dataframe : pandas.DataFrame
            Data to load.
        table_name : str
            Destination table name.
        schema : Optional[str]
            Schema override (defaults to warehouse.schema).
        if_exists : str
            Behaviour passed to pandas.to_sql (default append).
        chunksize : Optional[int]
            Optional chunk size for batch inserts.
        method : str
            Pandas insert method (multi for efficient batched inserts).
        """

        try:
            import pandas as pd  # type: ignore
        except ImportError as exc:  # pragma: no cover - defensive in case pandas missing
            raise ImportError("pandas is required for load_dataframe") from exc

        if not isinstance(dataframe, pd.DataFrame):
            raise TypeError("Dataframe argument must be a pandas DataFrame.")

        target_schema = schema or self.db_config.schema
        self.logger.info(
            "Loading dataframe into %s.%s (%s rows)",
            target_schema,
            table_name,
            len(dataframe),
        )
        dataframe.to_sql(
            table_name,
            con=self.engine,
            schema=target_schema,
            if_exists=if_exists,
            index=False,
            chunksize=chunksize or self.db_config.load_batch_size,
            method=method,
        )


def get_engine(config_path: Optional[str] = None) -> Engine:
    """Utility shortcut to obtain a configured SQLAlchemy engine."""

    connector = DBConnector(config_path=config_path)
    return connector.engine
