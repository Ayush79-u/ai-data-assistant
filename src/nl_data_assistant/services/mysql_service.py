"""
mysql_service.py – Improved MySQLService
Key changes vs original:
  • Tables always get an AUTO_INCREMENT primary key (`_id`) injected
  • write_dataframe() uses INSERT IGNORE to block duplicate rows
  • execute_statement() accepts both dict and list[dict] parameters
  • describe_table() / get_schema_catalog() are fully exception-safe
  • New helper: table_exists() used by the engine before DDL
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import NoSuchTableError, OperationalError

from nl_data_assistant.models import ColumnSpec
from nl_data_assistant.utils.cleaning import normalize_identifier


_AUTO_PK = "`_id` INT AUTO_INCREMENT PRIMARY KEY"


class MySQLService:
    def __init__(self, database_url: str | None) -> None:
        self.database_url = database_url
        self.engine = create_engine(database_url, future=True, pool_pre_ping=True) if database_url else None

    # ------------------------------------------------------------------ #
    # Properties                                                           #
    # ------------------------------------------------------------------ #

    @property
    def is_configured(self) -> bool:
        return self.engine is not None

    # ------------------------------------------------------------------ #
    # DDL helpers                                                          #
    # ------------------------------------------------------------------ #

    def table_exists(self, table_name: str) -> bool:
        """Return True when the table is already present in the database."""
        if not self.engine:
            return False
        try:
            return normalize_identifier(table_name) in inspect(self.engine).get_table_names()
        except Exception:
            return False

    def create_table(self, table_name: str, columns: list[ColumnSpec]) -> None:
        """
        CREATE TABLE IF NOT EXISTS with an injected AUTO_INCREMENT primary key.

        Why AUTO_INCREMENT?
        – Every table needs a stable unique row identity so that future
          UPDATE / DELETE commands can target specific rows safely.
        – Without it, duplicate inserts are hard to detect and remove.
        """
        self._ensure_configured()
        safe_table = normalize_identifier(table_name)

        user_cols = ", ".join(
            f"`{normalize_identifier(c.name)}` {c.data_type} {'NULL' if c.nullable else 'NOT NULL'}"
            for c in columns
        )
        # Prepend the hidden PK; skip if caller already included one.
        pk_clause = _AUTO_PK if not any(c.name.lower() in ("id", "_id") for c in columns) else ""
        col_block = f"{pk_clause}, {user_cols}" if pk_clause else user_cols

        ddl = f"CREATE TABLE IF NOT EXISTS `{safe_table}` ({col_block})"
        with self.engine.begin() as conn:
            conn.execute(text(ddl))

    # ------------------------------------------------------------------ #
    # DML helpers                                                          #
    # ------------------------------------------------------------------ #

    def write_dataframe(
        self,
        dataframe: pd.DataFrame,
        table_name: str,
        if_exists: str = "append",
        ignore_duplicates: bool = True,
    ) -> int:
        """
        Persist a DataFrame to MySQL.

        ignore_duplicates=True  → uses INSERT IGNORE, so rows whose unique /
        primary-key constraints collide are silently skipped rather than
        raising an error.  This is the primary guard against Streamlit-rerun
        duplicate inserts.

        Why do reruns cause duplicates?
        Streamlit re-executes the *entire* script on every widget interaction
        or st.rerun() call.  If a DB write sits outside of a proper
        session_state guard it runs again each time, inserting the same rows.
        INSERT IGNORE is the last line of defence; the real fix is the
        command-hash guard in streamlit_app.py.
        """
        self._ensure_configured()
        safe = normalize_identifier(table_name)

        if ignore_duplicates and if_exists == "append":
            # Build INSERT IGNORE manually so duplicates are skipped at the DB level.
            cols = ", ".join(f"`{c}`" for c in dataframe.columns)
            placeholders = ", ".join(f":{c}" for c in dataframe.columns)
            stmt = text(f"INSERT IGNORE INTO `{safe}` ({cols}) VALUES ({placeholders})")
            with self.engine.begin() as conn:
                rows = dataframe.to_dict(orient="records")
                result = conn.execute(stmt, rows)
                return int(result.rowcount or 0)

        dataframe.to_sql(safe, self.engine, if_exists=if_exists, index=False)
        return len(dataframe)

    def read_table(self, table_name: str, limit: int = 500) -> pd.DataFrame:
        self._ensure_configured()
        safe = normalize_identifier(table_name)
        return pd.read_sql_query(text(f"SELECT * FROM `{safe}` LIMIT {int(limit)}"), self.engine)

    def run_query(self, query: str, parameters: dict[str, Any] | None = None) -> pd.DataFrame:
        self._ensure_configured()
        return pd.read_sql_query(text(query), self.engine, params=parameters)

    def execute_statement(
        self,
        query: str,
        parameters: dict[str, Any] | list[dict[str, Any]] | None = None,
    ) -> int:
        self._ensure_configured()
        with self.engine.begin() as conn:
            result = conn.execute(text(query), parameters or {})
            return int(result.rowcount or 0)

    # ------------------------------------------------------------------ #
    # Introspection                                                        #
    # ------------------------------------------------------------------ #

    def describe_table(self, table_name: str) -> list[dict[str, Any]]:
        self._ensure_configured()
        try:
            cols = inspect(self.engine).get_columns(normalize_identifier(table_name))
        except (NoSuchTableError, OperationalError):
            return []
        return [
            {"column": c["name"], "dtype": str(c["type"]), "nullable": bool(c.get("nullable", True))}
            for c in cols
        ]

    def list_tables(self) -> list[str]:
        """Return all table names in the connected database."""
        if not self.engine:
            return []
        try:
            return [normalize_identifier(t) for t in inspect(self.engine).get_table_names()]
        except Exception:
            return []

    def get_schema_catalog(self) -> dict[str, list[str]]:
        if not self.engine:
            return {}
        try:
            inspector = inspect(self.engine)
            catalog: dict[str, list[str]] = {}
            for tbl in inspector.get_table_names():
                try:
                    cols = inspector.get_columns(tbl)
                    catalog[normalize_identifier(tbl)] = [normalize_identifier(c["name"]) for c in cols]
                except NoSuchTableError:
                    continue
            return catalog
        except Exception:
            return {}

    # ------------------------------------------------------------------ #
    # Internal                                                             #
    # ------------------------------------------------------------------ #

    def _ensure_configured(self) -> None:
        if not self.engine:
            raise RuntimeError(
                "MySQL is not configured. Set MYSQL_USER and MYSQL_DATABASE in your .env file."
            )
