"""
mysql_service.py — All MySQL I/O via SQLAlchemy.
Queries are always executed with bound parameters — never raw f-strings.
"""
from __future__ import annotations

import logging
import re
from contextlib import contextmanager
from typing import Any, Generator

import pandas as pd
from sqlalchemy import Engine, create_engine, inspect, text
from sqlalchemy.exc import OperationalError, SQLAlchemyError

from nl_data_assistant.config import settings
from nl_data_assistant.models import ActionPlan, ExecutionResult
from nl_data_assistant.nlp.mysql_query_generator import MySQLQueryGenerator

log = logging.getLogger(__name__)

_ALLOWED_SQL_TYPES = {
    "INT",
    "BIGINT",
    "FLOAT",
    "DOUBLE",
    "DECIMAL(10,2)",
    "VARCHAR(255)",
    "TEXT",
    "DATETIME",
    "DATE",
    "TINYINT(1)",
    "INT AUTO_INCREMENT",
}


def _sanitize_identifier(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_]+", "_", value.strip().lower()).strip("_")
    if not cleaned:
        raise ValueError("A table or column name is required.")
    return cleaned


def _normalise_sql_type(value: str) -> str:
    sql_type = value.strip().upper()
    if sql_type not in _ALLOWED_SQL_TYPES:
        raise ValueError(
            f"Unsupported SQL type '{value}'. Allowed types: {sorted(_ALLOWED_SQL_TYPES)}"
        )
    return sql_type


class MySQLService:
    def __init__(self, engine: Engine | None = None):
        self._engine = engine or create_engine(
            settings.mysql_url,
            pool_pre_ping=True,
            pool_recycle=3600,
        )
        self._generator = MySQLQueryGenerator(self._engine)

    # ── Connection check ──────────────────────────────────────────────────────

    def ping(self) -> bool:
        """Return True if the DB is reachable."""
        try:
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except OperationalError:
            return False

    # ── Schema introspection ──────────────────────────────────────────────────

    def get_table_names(self) -> list[str]:
        return inspect(self._engine).get_table_names()

    def table_exists(self, table_name: str) -> bool:
        safe_name = _sanitize_identifier(table_name)
        return safe_name in {name.lower() for name in self.get_table_names()}

    def get_table_columns(self, table_name: str) -> list[dict[str, Any]]:
        safe_name = self._validate_existing_table(table_name)
        return inspect(self._engine).get_columns(safe_name)

    def get_schema_summary(self) -> str:
        """Return a compact schema string for local parser context and debugging."""
        insp = inspect(self._engine)
        parts: list[str] = []
        for table in insp.get_table_names():
            cols = ", ".join(
                f"{c['name']} {c['type']}" for c in insp.get_columns(table)
            )
            parts.append(f"{table}({cols})")
        return "tables: " + "; ".join(parts) if parts else "(no tables yet)"

    def fetch_table(self, table_name: str, limit: int = 500) -> pd.DataFrame:
        safe_name = self._validate_existing_table(table_name)
        sql = f"SELECT * FROM `{safe_name}` LIMIT :limit;"
        with self._engine.begin() as conn:
            result = conn.execute(text(sql), {"limit": max(1, min(limit, 10_000))})
            rows = result.fetchall()
            columns = list(result.keys())
        return pd.DataFrame(rows, columns=columns)

    def create_table_from_blueprint(
        self,
        blueprint: dict[str, Any],
        *,
        recreate: bool = False,
    ) -> ExecutionResult:
        try:
            table_name = _sanitize_identifier(str(blueprint.get("table_name", "")))
            columns = blueprint.get("columns") or []
            if not columns:
                raise ValueError("The blueprint does not contain any columns.")

            sql = self._build_create_table_sql(table_name, columns)
            statements: list[str] = []

            with self._engine.begin() as conn:
                if recreate:
                    conn.execute(text(f"DROP TABLE IF EXISTS `{table_name}`;"))
                    statements.append(f"DROP TABLE IF EXISTS `{table_name}`;")
                conn.execute(text(sql))
                statements.append(sql)

            return ExecutionResult(
                success=True,
                sql_executed="\n".join(statements),
                message=f"Created table `{table_name}`.",
            )
        except (ValueError, SQLAlchemyError) as exc:
            log.error("Create table failed: %s", exc)
            return ExecutionResult(success=False, error=str(exc))

    def replace_table_data(self, table_name: str, df: pd.DataFrame) -> ExecutionResult:
        try:
            safe_name = self._validate_existing_table(table_name)
            db_columns = self.get_table_columns(safe_name)
            if not db_columns:
                raise ValueError(f"Table '{safe_name}' has no columns.")

            writable_columns: list[str] = []
            for column in db_columns:
                column_name = column["name"]
                autoincrement_value = str(column.get("autoincrement", "")).lower()
                is_auto_id = column_name.lower() == "id" and (
                    column.get("primary_key")
                    or autoincrement_value in {"true", "auto", "auto_increment"}
                    or "auto_increment" in autoincrement_value
                )
                if is_auto_id:
                    continue
                writable_columns.append(column_name)

            clean_df = df.copy()
            clean_df.columns = [str(col).strip() for col in clean_df.columns]

            missing = [name for name in writable_columns if name not in clean_df.columns]
            for column_name in missing:
                clean_df[column_name] = None

            extra_columns = [name for name in clean_df.columns if name not in writable_columns]
            if extra_columns:
                clean_df = clean_df.drop(columns=extra_columns)

            clean_df = clean_df[writable_columns]

            # remove rows where every editable column is blank
            clean_df = clean_df.dropna(how="all")

            # convert NaN to None for MySQL NULL support
            clean_df = clean_df.where(pd.notnull(clean_df), None)

            delete_sql = f"DELETE FROM `{safe_name}`;"
            statements = [delete_sql]

            with self._engine.begin() as conn:
                conn.execute(text(delete_sql))
                if not clean_df.empty and writable_columns:
                    column_sql = ", ".join(f"`{name}`" for name in writable_columns)
                    value_sql = ", ".join(f":{name}" for name in writable_columns)
                    insert_sql = f"INSERT INTO `{safe_name}` ({column_sql}) VALUES ({value_sql});"
                    conn.execute(text(insert_sql), clean_df.to_dict(orient="records"))
                    statements.append(insert_sql)

            return ExecutionResult(
                success=True,
                sql_executed="\n".join(statements),
                rows_affected=len(clean_df),
                message=f"Saved {len(clean_df)} row(s) to `{safe_name}`.",
            )
        except (ValueError, SQLAlchemyError) as exc:
            log.error("Save table data failed: %s", exc)
            return ExecutionResult(success=False, error=str(exc))

    # ── Query execution ───────────────────────────────────────────────────────

    def execute_plan(self, plan: ActionPlan) -> ExecutionResult:
        try:
            sql, params = self._generator.generate(plan)
            return self._run(sql, params)
        except ValueError as exc:
            return ExecutionResult(success=False, error=str(exc))
        except SQLAlchemyError as exc:
            log.error("DB error: %s", exc)
            return ExecutionResult(success=False, error=str(exc))

    def execute_raw(self, sql: str, params: dict[str, Any] | None = None) -> ExecutionResult:
        """Execute arbitrary SQL — caller is responsible for safety checks."""
        try:
            return self._run(sql, params or {})
        except SQLAlchemyError as exc:
            log.error("Raw SQL execution failed: %s", exc)
            return ExecutionResult(success=False, error=str(exc), sql_executed=sql)

    def _run(self, sql: str, params: dict[str, Any]) -> ExecutionResult:
        normalized = sql.strip().upper()
        is_select = normalized.startswith("SELECT") or normalized.startswith("DESCRIBE")
        with self._engine.begin() as conn:
            result = conn.execute(text(sql), params)
            if is_select:
                rows = result.fetchall()
                columns = list(result.keys())
                df = pd.DataFrame(rows, columns=columns)
                return ExecutionResult(
                    success=True,
                    data=df,
                    sql_executed=sql,
                    rows_affected=len(df),
                    message=f"Returned {len(df)} row(s).",
                )
            else:
                return ExecutionResult(
                    success=True,
                    sql_executed=sql,
                    rows_affected=result.rowcount,
                    message=f"OK — {result.rowcount} row(s) affected.",
                )

    # ── Context manager support ───────────────────────────────────────────────

    def _validate_existing_table(self, table_name: str) -> str:
        safe_name = _sanitize_identifier(table_name)
        tables = {name.lower(): name for name in self.get_table_names()}
        if safe_name not in tables:
            raise ValueError(f"Unknown table '{table_name}'.")
        return tables[safe_name]

    def _build_create_table_sql(self, table_name: str, columns: list[dict[str, Any]]) -> str:
        column_lines: list[str] = []
        primary_key_columns: list[str] = []

        for column in columns:
            column_name = _sanitize_identifier(str(column.get("name", "")))
            column_type = _normalise_sql_type(str(column.get("type", "VARCHAR(255)")))
            is_primary = bool(column.get("primary_key", False))
            line = f"`{column_name}` {column_type}"
            if is_primary and "AUTO_INCREMENT" not in column_type:
                line += " NOT NULL"
            column_lines.append(line)
            if is_primary:
                primary_key_columns.append(column_name)

        if primary_key_columns:
            joined_keys = ", ".join(f"`{name}`" for name in primary_key_columns)
            column_lines.append(f"PRIMARY KEY ({joined_keys})")

        return (
            f"CREATE TABLE IF NOT EXISTS `{table_name}` (\n  "
            + ",\n  ".join(column_lines)
            + "\n);"
        )

    @contextmanager
    def transaction(self) -> Generator:
        with self._engine.begin() as conn:
            yield conn
