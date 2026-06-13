"""
mysql_session_service.py - MySQL session-aware SQL execution and schema helpers.

This service connects at the server level so commands such as:
- CREATE DATABASE
- SHOW DATABASES
- USE database_name
- SHOW TABLES
- DESCRIBE
- CREATE / INSERT / SELECT / UPDATE / DELETE / ALTER / TRUNCATE / DROP

behave much closer to a real MySQL console.

Adds:
- update_rows_by_id() for safe partial saves of edited query results.
"""
from __future__ import annotations

import logging
import re
import time
from contextlib import contextmanager
from typing import Any, Generator

import pandas as pd
from sqlalchemy import Engine, create_engine, inspect, text
from sqlalchemy.exc import OperationalError, SQLAlchemyError

from nl_data_assistant.config import settings
from nl_data_assistant.models import ActionPlan, ExecutionResult
from nl_data_assistant.nlp.mysql_query_generator import MySQLQueryGenerator
from nl_data_assistant.nlp.table_blueprint import TableBlueprint
from nl_data_assistant.nlp.schema_context import (
    SchemaContext,
    build_context_str,
)

log = logging.getLogger(__name__)

try:
    import sqlparse
except ImportError:  # pragma: no cover - lightweight fallback for local envs
    sqlparse = None

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

_ROW_RETURNING_PREFIXES = ("SELECT", "SHOW", "DESCRIBE", "EXPLAIN", "WITH")
_SERVER_LEVEL_PREFIXES = (
    "CREATE DATABASE",
    "DROP DATABASE",
    "ALTER DATABASE",
    "SHOW DATABASES",
    "USE ",
)


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


class MySQLSessionService:
    _SCHEMA_CACHE_TTL_SECONDS = 120
    _AI_MAX_TABLES = 6
    _AI_SAMPLE_ROWS = 5
    _AI_DISTINCT_VALUE_LIMIT = 6
    _AI_LOW_CARDINALITY_THRESHOLD = 12
    _AI_MAX_VALUE_CHARS = 48

    def __init__(
        self,
        server_engine: Engine | None = None,
        *,
        default_database: str = "",
    ):
        self._server_engine = server_engine or create_engine(
            settings.mysql_server_url,
            connect_args=settings.ssl_connect_args,
            pool_pre_ping=True,
            pool_recycle=3600,
        )
        self._database_engines: dict[str, Engine] = {}
        self._schema_context_cache: dict[tuple[Any, ...], tuple[float, dict[str, Any]]] = {}
        # Set the current database lazily — validated on first use, not at startup
        # to avoid an unnecessary DB round-trip during cold start.
        self._current_database = default_database.strip() or settings.default_database

    @property
    def current_database(self) -> str:
        return self._current_database

    def ping(self) -> bool:
        try:
            with self._server_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except OperationalError:
            return False

    def get_database_names(self) -> list[str]:
        with self._server_engine.begin() as conn:
            result = conn.exec_driver_sql("SHOW DATABASES;")
            rows = result.fetchall()
        return [str(row[0]) for row in rows]

    def use_database(self, database_name: str) -> ExecutionResult:
        safe_name = _sanitize_identifier(database_name)
        if safe_name not in {name.lower() for name in self.get_database_names()}:
            return ExecutionResult(success=False, error=f"Unknown database '{database_name}'.")

        real_name = self._match_database_name(safe_name)
        self._current_database = real_name
        return ExecutionResult(success=True, message=f"Database changed to `{real_name}`.")

    def clear_context(self) -> None:
        default_database = settings.default_database.strip()
        if default_database and default_database in self.get_database_names():
            self._current_database = default_database
        else:
            self._current_database = ""

    def get_table_names(self, database: str | None = None) -> list[str]:
        target = self._resolve_database(database)
        if not target:
            return []
        return inspect(self._database_engine(target)).get_table_names()

    def table_exists(self, table_name: str, database: str | None = None) -> bool:
        safe_name = _sanitize_identifier(table_name)
        return safe_name in {name.lower() for name in self.get_table_names(database)}

    def get_table_columns(
        self,
        table_name: str,
        database: str | None = None,
    ) -> list[dict[str, Any]]:
        target_db = self._require_database(database)
        real_table = self._match_table_name(table_name, target_db)
        return inspect(self._database_engine(target_db)).get_columns(real_table)

    def get_schema_summary(self, database: str | None = None) -> str:
        target = self._resolve_database(database)
        if not target:
            return "(no database selected)"

        insp = inspect(self._database_engine(target))
        parts: list[str] = []
        for table in insp.get_table_names():
            cols = ", ".join(f"{col['name']} {col['type']}" for col in insp.get_columns(table))
            parts.append(f"{table}({cols})")
        return f"database: {target}; tables: " + "; ".join(parts) if parts else f"database: {target}; (no tables yet)"

    # ------------------------------------------------------------------
    # Schema-aware AI context pipeline
    # ------------------------------------------------------------------

    def build_ai_schema_context(
        self,
        database: str | None = None,
        table_hints: list[str] | None = None,
    ) -> SchemaContext:
        """
        Build a rich SchemaContext for AI SQL generation.

        Scans up to _AI_MAX_TABLES tables and fetches:
        - Column names + types (DESCRIBE)
        - Sample rows (SELECT * LIMIT _AI_SAMPLE_ROWS)
        - Distinct values for low-cardinality text columns

        Results are cached for _SCHEMA_CACHE_TTL_SECONDS to avoid
        repeated DB round-trips on each Streamlit rerun.

        Parameters
        ----------
        database    : target database (defaults to current)
        table_hints : if provided, these tables are listed first
        """
        target = self._resolve_database(database)
        if not target:
            return SchemaContext()

        hint_key = tuple(sorted(table_hints or []))
        cache_key = (target, hint_key)
        now = time.time()

        cached = self._schema_context_cache.get(cache_key)
        if cached:
            cached_at, cached_ctx = cached
            if now - cached_at < self._SCHEMA_CACHE_TTL_SECONDS:
                log.debug("schema_context cache hit for %s", cache_key)
                return cached_ctx  # type: ignore[return-value]

        log.debug("Building AI schema context for database=%s hints=%s", target, hint_key)

        all_tables = self.get_table_names(target)

        # Prioritise hinted tables so the active table is always included
        ordered: list[str] = []
        if table_hints:
            for hint in table_hints:
                real = next((t for t in all_tables if t.lower() == hint.lower()), None)
                if real and real not in ordered:
                    ordered.append(real)
        for t in all_tables:
            if t not in ordered:
                ordered.append(t)

        tables_to_scan = ordered[: self._AI_MAX_TABLES]

        tables_data: list[dict[str, Any]] = []
        column_map: dict[str, list[str]] = {}
        categorical_map: dict[str, dict[str, list[str]]] = {}

        for tbl in tables_to_scan:
            tbl_data = self._fetch_table_schema_data(tbl, target)
            tables_data.append(tbl_data)
            column_map[tbl] = [c["name"] for c in tbl_data["columns"]]
            categorical_map[tbl] = tbl_data["categoricals"]

        context_str = build_context_str(target, tables_data)

        ctx = SchemaContext(
            database=target,
            context_str=context_str,
            table_names=tables_to_scan,
            column_map=column_map,
            categorical_map=categorical_map,
        )

        self._schema_context_cache[cache_key] = (now, ctx)  # type: ignore[assignment]
        return ctx

    def _fetch_table_schema_data(self, table_name: str, database: str) -> dict[str, Any]:
        """Fetch column info, sample rows, and categorical values for one table."""
        engine = self._database_engine(database)
        insp   = inspect(engine)

        raw_cols = insp.get_columns(table_name)
        columns  = [{"name": c["name"], "type": str(c["type"])} for c in raw_cols]

        # Sample rows
        sample_rows: list[dict[str, Any]] = []
        try:
            with engine.connect() as conn:
                result = conn.execute(
                    text(f"SELECT * FROM `{table_name}` LIMIT :n;"),
                    {"n": self._AI_SAMPLE_ROWS},
                )
                keys = list(result.keys())
                for row in result.fetchall():
                    sample_rows.append(dict(zip(keys, row)))
        except Exception as exc:  # pragma: no cover
            log.warning("Could not fetch sample rows for %s: %s", table_name, exc)

        # Distinct values for low-cardinality text columns
        categoricals = self._fetch_distinct_values(table_name, columns, database)

        return {
            "name":        table_name,
            "columns":     columns,
            "sample_rows": sample_rows,
            "categoricals": categoricals,
        }

    def _fetch_distinct_values(
        self,
        table_name: str,
        columns: list[dict[str, Any]],
        database: str,
    ) -> dict[str, list[str]]:
        """
        For text-like columns, fetch distinct values if cardinality is low.
        Skips columns whose names match the sensitive-column list.
        """
        from nl_data_assistant.nlp.schema_context import _is_sensitive  # local import avoids circular

        engine = self._database_engine(database)
        result_map: dict[str, list[str]] = {}

        text_type_prefixes = ("varchar", "char", "text", "enum", "set")

        with engine.connect() as conn:
            for col in columns:
                col_name = col["name"]
                col_type = col["type"].lower()

                if _is_sensitive(col_name):
                    continue

                if not any(col_type.startswith(p) for p in text_type_prefixes):
                    continue

                try:
                    # Check cardinality first (cheap COUNT DISTINCT)
                    count_result = conn.execute(
                        text(
                            f"SELECT COUNT(DISTINCT `{col_name}`) "
                            f"FROM `{table_name}` WHERE `{col_name}` IS NOT NULL;"
                        )
                    ).scalar()
                    cardinality = int(count_result or 0)

                    if cardinality == 0 or cardinality > self._AI_LOW_CARDINALITY_THRESHOLD:
                        continue

                    vals_result = conn.execute(
                        text(
                            f"SELECT DISTINCT `{col_name}` "
                            f"FROM `{table_name}` "
                            f"WHERE `{col_name}` IS NOT NULL "
                            f"ORDER BY `{col_name}` "
                            f"LIMIT :lim;"
                        ),
                        {"lim": self._AI_DISTINCT_VALUE_LIMIT},
                    )
                    vals = [str(row[0]) for row in vals_result.fetchall() if row[0] is not None]
                    if vals:
                        result_map[col_name] = vals

                except Exception as exc:  # pragma: no cover
                    log.debug("Skipping distinct values for %s.%s: %s", table_name, col_name, exc)

        return result_map

    def invalidate_schema_cache(self, database: str | None = None) -> None:
        """Clear cached schema context for one database (or all)."""
        if database:
            self._schema_context_cache = {
                k: v for k, v in self._schema_context_cache.items()
                if k[0] != database
            }
        else:
            self._schema_context_cache.clear()

    def fetch_table(
        self,
        table_name: str,
        *,
        database: str | None = None,
        limit: int = 500,
    ) -> pd.DataFrame:
        target_db = self._require_database(database)
        real_table = self._match_table_name(table_name, target_db)
        sql = f"SELECT * FROM `{real_table}` LIMIT :limit;"
        with self._database_engine(target_db).begin() as conn:
            result = conn.execute(text(sql), {"limit": max(1, min(limit, 10_000))})
            rows = result.fetchall()
            columns = list(result.keys())
        return pd.DataFrame(rows, columns=columns)

    def create_table_from_blueprint(
        self,
        blueprint: dict[str, Any],
        *,
        recreate: bool = False,
        database: str | None = None,
    ) -> ExecutionResult:
        target_db = self._require_database(database)
        table_name = _sanitize_identifier(str(blueprint.get("table_name", "")))
        columns = blueprint.get("columns") or []
        if not columns:
            return ExecutionResult(success=False, error="The blueprint does not contain any columns.")

        sql = self._build_create_table_sql(table_name, columns)
        statements = [f"USE `{target_db}`;"]
        try:
            with self._server_engine.begin() as conn:
                conn.exec_driver_sql(f"USE `{target_db}`;")
                if recreate:
                    drop_sql = f"DROP TABLE IF EXISTS `{table_name}`;"
                    conn.exec_driver_sql(drop_sql)
                    statements.append(drop_sql)
                conn.exec_driver_sql(sql)
                statements.append(sql)
            return ExecutionResult(
                success=True,
                sql_executed="\n".join(statements),
                message=f"Table `{table_name}` created in `{target_db}`.",
            )
        except SQLAlchemyError as exc:
            log.error("Create table failed: %s", exc)
            return ExecutionResult(success=False, error=str(exc))

    def replace_table_data(
        self,
        table_name: str,
        df: pd.DataFrame,
        *,
        database: str | None = None,
    ) -> ExecutionResult:
        target_db = self._require_database(database)
        real_table = self._match_table_name(table_name, target_db)
        db_columns = self.get_table_columns(real_table, target_db)
        if not db_columns:
            return ExecutionResult(success=False, error=f"Table '{real_table}' has no columns.")

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
        clean_df.columns = [str(column).strip() for column in clean_df.columns]

        rename_map: dict[str, str] = {}
        for column_name in clean_df.columns:
            if column_name not in writable_columns:
                rename_map[column_name] = _sanitize_identifier(column_name)
        if rename_map:
            clean_df = clean_df.rename(columns=rename_map)

        duplicated_columns = clean_df.columns[clean_df.columns.duplicated()].tolist()
        if duplicated_columns:
            duplicates = ", ".join(sorted(set(map(str, duplicated_columns))))
            return ExecutionResult(
                success=False,
                error=f"Duplicate column names found after cleaning: {duplicates}.",
            )

        for missing in [name for name in writable_columns if name not in clean_df.columns]:
            clean_df[missing] = None

        extra_columns = [name for name in clean_df.columns if name not in writable_columns]
        final_columns = list(writable_columns) + list(extra_columns)
        clean_df = clean_df[final_columns]
        clean_df = clean_df.where(pd.notnull(clean_df), None)

        delete_sql = f"DELETE FROM `{real_table}`;"
        statements = [f"USE `{target_db}`;"]
        try:
            with self._database_engine(target_db).begin() as conn:
                for column_name in extra_columns:
                    sql_type = self._infer_series_sql_type(column_name, clean_df[column_name])
                    alter_sql = (
                        f"ALTER TABLE `{real_table}` "
                        f"ADD COLUMN `{column_name}` {sql_type};"
                    )
                    conn.exec_driver_sql(alter_sql)
                    statements.append(alter_sql)

                statements.append(delete_sql)
                conn.execute(text(delete_sql))
                if not clean_df.empty and final_columns:
                    column_sql = ", ".join(f"`{name}`" for name in final_columns)
                    value_sql = ", ".join(f":{name}" for name in final_columns)
                    insert_sql = (
                        f"INSERT INTO `{real_table}` ({column_sql}) VALUES ({value_sql});"
                    )
                    conn.execute(text(insert_sql), clean_df.to_dict(orient="records"))
                    statements.append(insert_sql)

            return ExecutionResult(
                success=True,
                sql_executed="\n".join(statements),
                rows_affected=len(clean_df),
                message=f"Saved {len(clean_df)} row(s) to `{real_table}`.",
            )
        except SQLAlchemyError as exc:
            log.error("Save table data failed: %s", exc)
            return ExecutionResult(success=False, error=str(exc))

    def update_rows_by_id(
        self,
        table_name: str,
        df: pd.DataFrame,
        *,
        database: str | None = None,
    ) -> ExecutionResult:
        """Update or insert the rows present in ``df`` using a safe matching column."""
        target_db = self._require_database(database)
        real_table = self._match_table_name(table_name, target_db)
        db_columns = self.get_table_columns(real_table, target_db)

        if not db_columns:
            return ExecutionResult(success=False, error=f"Table '{real_table}' has no columns.")
        clean_df = self._prepare_editor_dataframe(df, db_columns)
        if clean_df.empty:
            return ExecutionResult(success=True, message="Nothing to update.", rows_affected=0)

        statements = [f"USE `{target_db}`;"]
        try:
            updated = 0
            inserted = 0
            skipped = 0

            with self._database_engine(target_db).begin() as conn:
                db_columns = self._ensure_dataframe_columns(conn, real_table, clean_df, db_columns)
                db_column_names = {column["name"] for column in db_columns}
                match_column = self._choose_match_column(conn, real_table, db_columns, clean_df)

                if not match_column:
                    return ExecutionResult(
                        success=False,
                        error=(
                            f"Table `{real_table}` has no safe matching column yet. "
                            "Add a unique id-like column such as `id`, `empid`, or "
                            f"`{real_table}_id`, or use 'Save to MySQL (replace all)'."
                        ),
                    )

                key_meta = next(
                    (column for column in db_columns if column["name"] == match_column),
                    {"name": match_column},
                )
                key_is_auto = self._is_auto_increment_column(key_meta)

                existing_key_rows = conn.execute(
                    text(
                        f"SELECT `{match_column}` FROM `{real_table}` "
                        f"WHERE `{match_column}` IS NOT NULL;"
                    )
                ).fetchall()
                existing_keys = {row[0] for row in existing_key_rows}

                writable_columns = [
                    column_name
                    for column_name in clean_df.columns
                    if column_name != match_column and column_name in db_column_names
                ]
                insert_columns = [
                    column_name
                    for column_name in clean_df.columns
                    if column_name in db_column_names
                    and (column_name != match_column or not key_is_auto)
                ]

                update_sql = ""
                if writable_columns:
                    set_clause = ", ".join(
                        f"`{column_name}` = :{column_name}"
                        for column_name in writable_columns
                    )
                    update_sql = (
                        f"UPDATE `{real_table}` SET {set_clause} "
                        f"WHERE `{match_column}` = :{match_column};"
                    )
                    statements.append(update_sql)

                insert_sql = ""
                if insert_columns:
                    column_sql = ", ".join(f"`{column_name}`" for column_name in insert_columns)
                    value_sql = ", ".join(f":{column_name}" for column_name in insert_columns)
                    insert_sql = (
                        f"INSERT INTO `{real_table}` ({column_sql}) VALUES ({value_sql});"
                    )
                    statements.append(insert_sql)

                for record in clean_df.to_dict(orient="records"):
                    row_values = {
                        column_name: record.get(column_name)
                        for column_name in clean_df.columns
                        if column_name in db_column_names
                    }
                    if not any(not self._is_blank_value(value) for value in row_values.values()):
                        continue

                    key_value = record.get(match_column)

                    if not self._is_blank_value(key_value) and key_value in existing_keys:
                        if not update_sql:
                            skipped += 1
                            continue
                        params = {column_name: record.get(column_name) for column_name in writable_columns}
                        params[match_column] = key_value
                        result = conn.execute(text(update_sql), params)
                        updated += result.rowcount or 0
                        continue

                    if not insert_sql:
                        skipped += 1
                        continue

                    if self._is_blank_value(key_value) and not key_is_auto:
                        skipped += 1
                        continue

                    insert_payload = {
                        column_name: record.get(column_name)
                        for column_name in insert_columns
                    }
                    if key_is_auto and match_column in insert_payload:
                        insert_payload.pop(match_column, None)

                    if not any(
                        not self._is_blank_value(value)
                        for column_name, value in insert_payload.items()
                        if column_name != match_column
                    ):
                        continue

                    result = conn.execute(text(insert_sql), insert_payload)
                    inserted += result.rowcount or 1
                    if not self._is_blank_value(key_value):
                        existing_keys.add(key_value)

            total_changed = updated + inserted
            summary = (
                f"Saved editor changes to `{real_table}` using `{match_column}`. "
                f"Updated {updated} row(s), inserted {inserted} row(s)."
            )
            if skipped:
                summary += f" Skipped {skipped} row(s) that could not be matched safely."

            return ExecutionResult(
                success=True,
                sql_executed="\n".join(statements),
                rows_affected=total_changed,
                message=summary,
            )
        except (SQLAlchemyError, ValueError) as exc:
            log.error("Update rows failed: %s", exc)
            return ExecutionResult(success=False, error=str(exc))

    def _prepare_editor_dataframe(
        self,
        df: pd.DataFrame,
        db_columns: list[dict[str, Any]],
    ) -> pd.DataFrame:
        clean_df = df.dropna(how="all").copy()
        clean_df.columns = [str(column).strip() for column in clean_df.columns]
        db_name_map = {
            _sanitize_identifier(str(column["name"])): str(column["name"])
            for column in db_columns
        }

        rename_map: dict[str, str] = {}
        for column_name in clean_df.columns:
            safe_name = _sanitize_identifier(column_name)
            rename_map[column_name] = db_name_map.get(safe_name, safe_name)

        if rename_map:
            clean_df = clean_df.rename(columns=rename_map)

        duplicated_columns = clean_df.columns[clean_df.columns.duplicated()].tolist()
        if duplicated_columns:
            duplicates = ", ".join(sorted(set(map(str, duplicated_columns))))
            raise ValueError(f"Duplicate column names found after cleaning: {duplicates}.")

        return clean_df.where(pd.notnull(clean_df), None)

    def _ensure_dataframe_columns(
        self,
        conn,
        table_name: str,
        clean_df: pd.DataFrame,
        db_columns: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        db_column_names = {column["name"] for column in db_columns}
        extra_columns = [name for name in clean_df.columns if name not in db_column_names]

        for column_name in extra_columns:
            sql_type = self._infer_series_sql_type(column_name, clean_df[column_name])
            alter_sql = f"ALTER TABLE `{table_name}` ADD COLUMN `{column_name}` {sql_type};"
            conn.exec_driver_sql(alter_sql)
            db_columns.append({"name": column_name, "type": sql_type})

        return db_columns

    def _choose_match_column(
        self,
        conn,
        table_name: str,
        db_columns: list[dict[str, Any]],
        clean_df: pd.DataFrame,
    ) -> str:
        shared_columns = {
            column["name"]
            for column in db_columns
            if column["name"] in clean_df.columns
        }
        if not shared_columns:
            return ""

        primary_key = next(
            (
                column["name"]
                for column in db_columns
                if column.get("primary_key") and column["name"] in shared_columns
            ),
            "",
        )
        if primary_key:
            return primary_key

        safe_table = _sanitize_identifier(table_name)
        ranked_candidates: list[tuple[int, str]] = []
        for column in db_columns:
            name = column["name"]
            if name not in shared_columns:
                continue
            lowered = name.lower()
            if lowered == "id":
                ranked_candidates.append((0, name))
            elif lowered in {f"{safe_table}id", f"{safe_table}_id"}:
                ranked_candidates.append((1, name))
            elif lowered.endswith("_id"):
                ranked_candidates.append((2, name))
            elif lowered.endswith("id"):
                ranked_candidates.append((3, name))
            elif clean_df[name].dropna().is_unique:
                ranked_candidates.append((4, name))

        for _score, candidate in sorted(ranked_candidates, key=lambda item: (item[0], item[1])):
            if self._column_has_unique_db_values(conn, table_name, candidate):
                return candidate

        return ""

    @staticmethod
    def _is_auto_increment_column(column: dict[str, Any]) -> bool:
        autoincrement_value = str(column.get("autoincrement", "")).lower()
        return bool(
            autoincrement_value in {"true", "auto", "auto_increment"}
            or "auto_increment" in autoincrement_value
        )

    @staticmethod
    def _is_blank_value(value: Any) -> bool:
        if value is None:
            return True
        if isinstance(value, str):
            return value.strip() == ""
        return False

    def _column_has_unique_db_values(self, conn, table_name: str, column_name: str) -> bool:
        result = conn.execute(
            text(
                f"SELECT COUNT(`{column_name}`), COUNT(DISTINCT `{column_name}`) "
                f"FROM `{table_name}` WHERE `{column_name}` IS NOT NULL;"
            )
        ).first()
        if result is None:
            return False
        total_values = int(result[0] or 0)
        distinct_values = int(result[1] or 0)
        return total_values > 0 and total_values == distinct_values

    def _infer_series_sql_type(self, column_name: str, series: pd.Series) -> str:
        non_null = series.dropna()
        if non_null.empty:
            return TableBlueprint()._infer_type(column_name)

        if pd.api.types.is_bool_dtype(non_null):
            return "TINYINT(1)"
        if pd.api.types.is_integer_dtype(non_null):
            return "INT"
        if pd.api.types.is_float_dtype(non_null):
            return "FLOAT"
        if pd.api.types.is_datetime64_any_dtype(non_null):
            return "DATETIME"

        as_text = non_null.astype(str).str.strip()
        if not as_text.empty and as_text.str.fullmatch(r"-?\d+").all():
            return "INT"
        if not as_text.empty and as_text.str.fullmatch(r"-?\d+(\.\d+)?").all():
            return "FLOAT"
        if not as_text.empty and as_text.str.fullmatch(
            r"\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2})?"
        ).all():
            return "DATETIME"

        return TableBlueprint()._infer_type(column_name)

    def import_dataframe(
        self,
        table_name: str,
        df: pd.DataFrame,
        *,
        database: str | None = None,
        if_exists: str = "replace",
    ) -> ExecutionResult:
        target_db = self._require_database(database)
        safe_table = _sanitize_identifier(table_name)
        try:
            with self._database_engine(target_db).begin() as conn:
                df.to_sql(safe_table, conn, if_exists=if_exists, index=False)
            statement = (
                f"-- Imported DataFrame into `{target_db}`.`{safe_table}` using pandas.to_sql "
                f"with if_exists='{if_exists}'"
            )
            return ExecutionResult(
                success=True,
                sql_executed=statement,
                rows_affected=len(df),
                message=f"Imported {len(df)} row(s) into `{safe_table}`.",
            )
        except (ValueError, SQLAlchemyError) as exc:
            log.error("DataFrame import failed: %s", exc)
            return ExecutionResult(success=False, error=str(exc))

    def rename_table(
        self,
        current_table_name: str,
        new_table_name: str,
        *,
        database: str | None = None,
    ) -> ExecutionResult:
        target_db = self._require_database(database)
        real_current = self._match_table_name(current_table_name, target_db)
        safe_new = _sanitize_identifier(new_table_name)
        current_tables = {name.lower() for name in self.get_table_names(target_db)}

        if safe_new == real_current.lower():
            return ExecutionResult(
                success=True,
                message=f"Table `{real_current}` already has that name.",
            )

        if safe_new in current_tables:
            return ExecutionResult(
                success=False,
                error=f"Table `{safe_new}` already exists.",
            )

        sql = f"RENAME TABLE `{real_current}` TO `{safe_new}`;"
        statements = [f"USE `{target_db}`;", sql]
        try:
            with self._server_engine.begin() as conn:
                conn.exec_driver_sql(f"USE `{target_db}`;")
                conn.exec_driver_sql(sql)
            return ExecutionResult(
                success=True,
                sql_executed="\n".join(statements),
                message=f"Renamed table `{real_current}` to `{safe_new}`.",
            )
        except SQLAlchemyError as exc:
            log.error("Rename table failed: %s", exc)
            return ExecutionResult(success=False, error=str(exc))

    def drop_table(
        self,
        table_name: str,
        *,
        database: str | None = None,
    ) -> ExecutionResult:
        target_db = self._require_database(database)
        real_table = self._match_table_name(table_name, target_db)
        sql = f"DROP TABLE `{real_table}`;"
        statements = [f"USE `{target_db}`;", sql]
        try:
            with self._server_engine.begin() as conn:
                conn.exec_driver_sql(f"USE `{target_db}`;")
                conn.exec_driver_sql(sql)
            return ExecutionResult(
                success=True,
                sql_executed="\n".join(statements),
                message=f"Deleted table `{real_table}`.",
            )
        except SQLAlchemyError as exc:
            log.error("Drop table failed: %s", exc)
            return ExecutionResult(success=False, error=str(exc))

    def execute_plan(self, plan: ActionPlan) -> ExecutionResult:
        try:
            target_db = self._require_database()
            generator = MySQLQueryGenerator(self._database_engine(target_db))
            sql, params = generator.generate(plan)
            return self._run_database_sql(sql, params=params, database=target_db)
        except (ValueError, SQLAlchemyError) as exc:
            log.error("DB error: %s", exc)
            return ExecutionResult(success=False, error=str(exc))

    def preview_plan_sql(
        self,
        plan: ActionPlan,
        *,
        database: str | None = None,
    ) -> str:
        target_db = self._require_database(database)
        generator = MySQLQueryGenerator(self._database_engine(target_db))
        sql, _params = generator.generate(plan)
        return sql

    def execute_sql(self, sql: str) -> ExecutionResult:
        from nl_data_assistant.nlp.ai_sql_generator import is_safe_sql  # avoid circular import
        raw_statements = sqlparse.split(sql) if sqlparse else sql.split(";")
        statements = [statement.strip() for statement in raw_statements if statement.strip()]
        if not statements:
            return ExecutionResult(success=False, error="Enter a SQL command.")

        # Defence-in-depth: block dangerous statements regardless of the caller
        for statement in statements:
            if not is_safe_sql(statement):
                return ExecutionResult(
                    success=False,
                    error="Blocked: this statement type is not permitted.",
                    sql_executed=statement,
                )

        combined_sql: list[str] = []
        last_data: pd.DataFrame | None = None
        messages: list[str] = []
        rows_affected = 0

        for statement in statements:
            result = self._execute_statement(statement)
            combined_sql.append(result.sql_executed or statement)
            if not result.success:
                return ExecutionResult(
                    success=False,
                    error=result.error or result.message,
                    sql_executed="\n".join(combined_sql),
                    data=last_data,
                    rows_affected=rows_affected,
                )
            if isinstance(result.data, pd.DataFrame):
                last_data = result.data
            rows_affected = result.rows_affected or rows_affected
            if result.message:
                messages.append(result.message)

        return ExecutionResult(
            success=True,
            data=last_data,
            sql_executed="\n".join(combined_sql),
            rows_affected=rows_affected,
            message="\n".join(messages) if messages else "Query OK.",
        )

    def execute_raw(self, sql: str, params: dict[str, Any] | None = None) -> ExecutionResult:
        if params:
            try:
                target_db = self._resolve_database(require=False)
                if target_db:
                    return self._run_database_sql(sql, params=params, database=target_db)
                return self._run_server_sql(sql, params=params)
            except SQLAlchemyError as exc:
                log.error("Raw SQL execution failed: %s", exc)
                return ExecutionResult(success=False, error=str(exc), sql_executed=sql)
        return self.execute_sql(sql)

    @contextmanager
    def transaction(self) -> Generator:
        target_db = self._require_database()
        with self._database_engine(target_db).begin() as conn:
            yield conn

    def _execute_statement(self, statement: str) -> ExecutionResult:
        normalized = statement.strip().rstrip(";")
        upper = normalized.upper()

        if upper.startswith("USE "):
            match = re.match(r"USE\s+`?([a-zA-Z0-9_]+)`?$", normalized, re.IGNORECASE)
            if not match:
                return ExecutionResult(success=False, error="Invalid USE statement.")
            return self.use_database(match.group(1))

        if upper.startswith("DROP DATABASE"):
            match = re.search(r"DROP\s+DATABASE(?:\s+IF\s+EXISTS)?\s+`?([a-zA-Z0-9_]+)`?", normalized, re.IGNORECASE)
            dropped = match.group(1) if match else ""
            result = self._run_server_sql(f"{normalized};")
            if result.success and dropped and self._current_database.lower() == dropped.lower():
                self._current_database = ""
            return result

        if upper.startswith(_SERVER_LEVEL_PREFIXES):
            return self._run_server_sql(f"{normalized};")

        show_tables_match = re.search(
            r"SHOW\s+TABLES(?:\s+(?:FROM|IN)\s+`?([a-zA-Z0-9_]+)`?)?",
            normalized,
            re.IGNORECASE,
        )
        if show_tables_match:
            hinted_database = show_tables_match.group(1)
            target_db = self._require_database(hinted_database)
            return self._run_database_sql(f"{normalized};", database=target_db)

        hinted_database = self._extract_database_hint(normalized)
        target_db = self._require_database(hinted_database)
        return self._run_database_sql(f"{normalized};", database=target_db)

    def _run_database_sql(
        self,
        sql: str,
        *,
        params: dict[str, Any] | None = None,
        database: str,
    ) -> ExecutionResult:
        with self._server_engine.begin() as conn:
            conn.exec_driver_sql(f"USE `{database}`;")
            return self._execute_with_connection(conn, sql, params=params, database=database)

    def _run_server_sql(
        self,
        sql: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> ExecutionResult:
        with self._server_engine.begin() as conn:
            return self._execute_with_connection(conn, sql, params=params, database="")

    @staticmethod
    def _escape_literal_percents_for_driver_sql(sql: str) -> str:
        """
        Escape literal percent signs for raw DBAPI execution.

        PyMySQL treats `%` in raw SQL strings as Python formatting markers when
        `exec_driver_sql()` ultimately routes to the DBAPI cursor without bound
        parameters. We only escape lone percent signs here; already-escaped `%%`
        sequences are preserved as-is.
        """
        if "%" not in sql:
            return sql

        escaped: list[str] = []
        index = 0
        while index < len(sql):
            char = sql[index]
            if char != "%":
                escaped.append(char)
                index += 1
                continue

            escaped.append("%%")
            if index + 1 < len(sql) and sql[index + 1] == "%":
                index += 2
            else:
                index += 1

        return "".join(escaped)

    def _execute_with_connection(
        self,
        conn,
        sql: str,
        *,
        params: dict[str, Any] | None = None,
        database: str,
    ) -> ExecutionResult:
        try:
            if params:
                result = conn.execute(text(sql), params)
            else:
                driver_sql = self._escape_literal_percents_for_driver_sql(sql)
                result = conn.exec_driver_sql(driver_sql)

            if result.returns_rows or sql.strip().upper().startswith(_ROW_RETURNING_PREFIXES):
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

            return ExecutionResult(
                success=True,
                sql_executed=sql,
                rows_affected=max(result.rowcount, 0),
                message=self._format_query_ok(sql, result.rowcount, database),
            )
        except SQLAlchemyError as exc:
            log.error("SQL execution failed: %s", exc)
            return ExecutionResult(success=False, error=str(exc), sql_executed=sql)

    def _format_query_ok(self, sql: str, rowcount: int, database: str) -> str:
        normalized = sql.strip().upper()
        affected = max(rowcount, 0)
        if normalized.startswith("CREATE DATABASE"):
            match = re.search(r"CREATE\s+DATABASE(?:\s+IF\s+NOT\s+EXISTS)?\s+`?([a-zA-Z0-9_]+)`?", sql, re.IGNORECASE)
            created = match.group(1) if match else "database"
            return f"Query OK. Database `{created}` created."
        if normalized.startswith("CREATE TABLE"):
            return f"Query OK. Table created in `{database}`."
        if normalized.startswith("ALTER TABLE"):
            return "Query OK. Table altered."
        if normalized.startswith("TRUNCATE TABLE"):
            return "Query OK. Table truncated."
        if normalized.startswith("DROP TABLE"):
            return "Query OK. Table dropped."
        if normalized.startswith("INSERT"):
            return f"Query OK. {affected} row(s) inserted."
        if normalized.startswith("UPDATE"):
            return f"Query OK. {affected} row(s) updated."
        if normalized.startswith("DELETE"):
            return f"Query OK. {affected} row(s) deleted."
        if normalized.startswith("DROP DATABASE"):
            return "Query OK. Database dropped."
        return f"Query OK. {affected} row(s) affected."

    def _resolve_database(self, database: str | None = None, *, require: bool = False) -> str:
        target = (database or self._current_database or settings.default_database).strip()
        if not target and require:
            raise ValueError("No database selected. Run USE database_name first.")
        databases = {name.lower(): name for name in self.get_database_names()}
        if target and target.lower() not in databases:
            if require:
                raise ValueError(f"Unknown database '{target}'.")
            return ""
        return databases.get(target.lower(), target) if target else ""

    def _require_database(self, database: str | None = None) -> str:
        return self._resolve_database(database, require=True)

    def _database_engine(self, database: str) -> Engine:
        if database not in self._database_engines:
            self._database_engines[database] = create_engine(
                settings.mysql_url_for(database),
                connect_args=settings.ssl_connect_args,
                pool_pre_ping=True,
                pool_recycle=3600,
            )
        return self._database_engines[database]

    def _match_database_name(self, database_name: str) -> str:
        databases = {name.lower(): name for name in self.get_database_names()}
        if database_name not in databases:
            raise ValueError(f"Unknown database '{database_name}'.")
        return databases[database_name]

    def _match_table_name(self, table_name: str, database: str) -> str:
        safe_name = _sanitize_identifier(table_name)
        tables = {name.lower(): name for name in self.get_table_names(database)}
        if safe_name not in tables:
            raise ValueError(f"Unknown table '{table_name}'.")
        return tables[safe_name]

    def _extract_database_hint(self, sql: str) -> str | None:
        patterns = [
            r"(?:FROM|INTO|UPDATE|JOIN|TABLE|DESCRIBE)\s+`?([a-zA-Z0-9_]+)`?\.`?([a-zA-Z0-9_]+)`?",
        ]
        for pattern in patterns:
            match = re.search(pattern, sql, re.IGNORECASE)
            if match:
                return match.group(1)
        return None

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
