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
from nl_data_assistant.nlp.table_blueprint import TableBlueprint

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
    def __init__(
        self,
        server_engine: Engine | None = None,
        *,
        default_database: str = "",
    ):
        self._server_engine = server_engine or create_engine(
            settings.mysql_server_url,
            pool_pre_ping=True,
            pool_recycle=3600,
        )
        self._database_engines: dict[str, Engine] = {}
        self._current_database = default_database.strip() or settings.default_database
        if self._current_database and self._current_database not in self.get_database_names():
            self._current_database = ""

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
            if sql.strip().upper().startswith("SELECT") or sql.strip().upper().startswith("DESCRIBE"):
                return self._run_database_sql(sql, params=params, database=target_db)
            return self._run_database_sql(sql, params=params, database=target_db)
        except (ValueError, SQLAlchemyError) as exc:
            log.error("DB error: %s", exc)
            return ExecutionResult(success=False, error=str(exc))

    def execute_sql(self, sql: str) -> ExecutionResult:
        raw_statements = sqlparse.split(sql) if sqlparse else sql.split(";")
        statements = [statement.strip() for statement in raw_statements if statement.strip()]
        if not statements:
            return ExecutionResult(success=False, error="Enter a SQL command.")

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
                result = conn.exec_driver_sql(sql)

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
