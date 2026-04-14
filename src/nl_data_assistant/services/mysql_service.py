from __future__ import annotations

from typing import Any

import pandas as pd
from sqlalchemy import create_engine, inspect, text

from nl_data_assistant.models import ColumnSpec
from nl_data_assistant.utils.cleaning import normalize_identifier


class MySQLService:
    def __init__(self, database_url: str | None) -> None:
        self.database_url = database_url
        self.engine = create_engine(database_url, future=True) if database_url else None

    @property
    def is_configured(self) -> bool:
        return self.engine is not None

    def create_table(self, table_name: str, columns: list[ColumnSpec]) -> None:
        self._ensure_configured()
        safe_table = normalize_identifier(table_name)
        definitions = ", ".join(
            f"`{normalize_identifier(column.name)}` {column.data_type} {'NULL' if column.nullable else 'NOT NULL'}"
            for column in columns
        ) or "`id` INT"
        query = f"CREATE TABLE IF NOT EXISTS `{safe_table}` ({definitions})"
        with self.engine.begin() as connection:
            connection.execute(text(query))

    def write_dataframe(self, dataframe: pd.DataFrame, table_name: str, if_exists: str = "replace") -> None:
        self._ensure_configured()
        dataframe.to_sql(normalize_identifier(table_name), self.engine, if_exists=if_exists, index=False)

    def read_table(self, table_name: str, limit: int = 200) -> pd.DataFrame:
        self._ensure_configured()
        safe_table = normalize_identifier(table_name)
        query = text(f"SELECT * FROM `{safe_table}` LIMIT {int(limit)}")
        return pd.read_sql_query(query, self.engine)

    def run_query(self, query: str) -> pd.DataFrame:
        self._ensure_configured()
        return pd.read_sql_query(text(query), self.engine)

    def describe_table(self, table_name: str) -> list[dict[str, Any]]:
        self._ensure_configured()
        inspector = inspect(self.engine)
        columns = inspector.get_columns(normalize_identifier(table_name))
        rows = []
        for column in columns:
            rows.append(
                {
                    "column": column["name"],
                    "dtype": str(column["type"]),
                    "nullable": bool(column.get("nullable", True)),
                }
            )
        return rows

    def _ensure_configured(self) -> None:
        if not self.engine:
            raise RuntimeError("MySQL is not configured. Set MYSQL_USER and MYSQL_DATABASE in .env.")

