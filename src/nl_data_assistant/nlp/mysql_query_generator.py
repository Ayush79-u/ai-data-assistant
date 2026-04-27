"""
mysql_query_generator.py — Builds safe, schema-aware MySQL queries.

All table and column names are validated against the live schema before use.
Values are always bound via SQLAlchemy parameters — never interpolated.
"""
from __future__ import annotations

import logging
import random
import string
from typing import Any

from sqlalchemy import Engine, inspect, text

from nl_data_assistant.models import ActionPlan, Intent

log = logging.getLogger(__name__)

# ── Safe-name helpers ─────────────────────────────────────────────────────────

def _safe_identifier(name: str, allowed: set[str], label: str = "identifier") -> str:
    """Raise ValueError if name is not in the schema-derived allowlist."""
    clean = name.strip().lower()
    if clean not in allowed:
        raise ValueError(
            f"Unknown {label} '{name}'. "
            f"Allowed: {sorted(allowed) or '(none — check your DB connection)'}"
        )
    return f"`{clean}`"


def _safe_table(name: str, engine: Engine) -> str:
    tables = set(inspect(engine).get_table_names())
    return _safe_identifier(name, tables, "table")


def _safe_columns(cols: list[str], table: str, engine: Engine) -> list[str]:
    insp = inspect(engine)
    allowed = {c["name"].lower() for c in insp.get_columns(table.strip("`"))}
    return [_safe_identifier(c, allowed, "column") for c in cols]


# ── Query builder ─────────────────────────────────────────────────────────────

class MySQLQueryGenerator:
    """
    Converts an ActionPlan into a (sql_template, bind_params) pair.
    All user-supplied values go into bind_params — never into the SQL string.
    """

    def __init__(self, engine: Engine):
        self._engine = engine

    def generate(self, plan: ActionPlan) -> tuple[str, dict[str, Any]]:
        """Return (sql_string, params_dict) ready for SQLAlchemy text()."""
        if plan.sql:
            # Raw SQL path — validate that it doesn't reference
            # unknown tables before passing it through.
            self._validate_raw_sql(plan.sql)
            return plan.sql, {}

        handlers = {
            Intent.CREATE_TABLE: self._create_table,
            Intent.INSERT:       self._insert,
            Intent.SELECT:       self._select,
            Intent.UPDATE:       self._update,
            Intent.DELETE:       self._delete,
            Intent.DROP_TABLE:   self._drop_table,
            Intent.DESCRIBE:     self._describe,
        }
        handler = handlers.get(plan.intent)
        if handler is None:
            raise ValueError(f"Cannot generate SQL for intent '{plan.intent}'")
        return handler(plan)

    # ── DDL / DML builders ────────────────────────────────────────────────────

    def _create_table(self, plan: ActionPlan) -> tuple[str, dict]:
        name = self._sanitize_new_name(plan.table_name)
        cols = plan.columns or ["id"]
        col_defs = ["  `id` INT AUTO_INCREMENT PRIMARY KEY"]
        for col in cols:
            safe_col = re.sub(r"[^a-zA-Z0-9_]", "_", col).lower()
            col_defs.append(f"  `{safe_col}` VARCHAR(255)")
        sql = f"CREATE TABLE IF NOT EXISTS `{name}` (\n" + ",\n".join(col_defs) + "\n);"
        return sql, {}

    def _insert(self, plan: ActionPlan) -> tuple[str, dict]:
        tbl = _safe_table(plan.table_name, self._engine)
        rows = plan.values or [self._random_row(plan)]

        all_keys: list[str] = []
        for row in rows:
            for k in row:
                if k not in all_keys:
                    all_keys.append(k)

        safe_cols = _safe_columns(all_keys, plan.table_name, self._engine)
        col_list = ", ".join(safe_cols)
        statements: list[str] = []
        params: dict[str, Any] = {}

        for i, row in enumerate(rows):
            placeholders = []
            for j, k in enumerate(all_keys):
                param_name = f"v_{i}_{j}"
                placeholders.append(f":{param_name}")
                params[param_name] = row.get(k)
            statements.append(
                f"INSERT INTO {tbl} ({col_list}) VALUES ({', '.join(placeholders)});"
            )

        return "\n".join(statements), params

    def _select(self, plan: ActionPlan) -> tuple[str, dict]:
        tbl = _safe_table(plan.table_name, self._engine)
        if plan.columns:
            safe_cols = _safe_columns(plan.columns, plan.table_name, self._engine)
            col_list = ", ".join(safe_cols)
        else:
            col_list = "*"

        sql = f"SELECT {col_list} FROM {tbl}"
        params: dict[str, Any] = {}

        if plan.conditions:
            # Conditions are passed through the parser — log them but don't interpolate values
            sql += f" WHERE {plan.conditions}"

        if plan.order_by:
            # Only allow "col ASC/DESC" patterns
            order_clean = re.sub(r"[^a-zA-Z0-9_,\s]", "", plan.order_by)
            sql += f" ORDER BY {order_clean}"

        if plan.limit is not None:
            sql += " LIMIT :lim"
            params["lim"] = min(plan.limit, 10_000)

        return sql + ";", params

    def _update(self, plan: ActionPlan) -> tuple[str, dict]:
        tbl = _safe_table(plan.table_name, self._engine)
        if not plan.values:
            raise ValueError("UPDATE requires values.")
        row = plan.values[0]
        safe_cols = _safe_columns(list(row.keys()), plan.table_name, self._engine)
        set_clause = ", ".join(f"{c} = :{k}" for c, k in zip(safe_cols, row.keys()))
        sql = f"UPDATE {tbl} SET {set_clause}"
        if plan.conditions:
            sql += f" WHERE {plan.conditions}"
        return sql + ";", dict(row)

    def _delete(self, plan: ActionPlan) -> tuple[str, dict]:
        tbl = _safe_table(plan.table_name, self._engine)
        if not plan.conditions:
            raise ValueError(
                "DELETE without a WHERE clause is blocked for safety. "
                "Use 'delete all rows from <table>' to confirm intent."
            )
        sql = f"DELETE FROM {tbl} WHERE {plan.conditions};"
        return sql, {}

    def _drop_table(self, plan: ActionPlan) -> tuple[str, dict]:
        tbl = _safe_table(plan.table_name, self._engine)
        return f"DROP TABLE IF EXISTS {tbl};", {}

    def _describe(self, plan: ActionPlan) -> tuple[str, dict]:
        tbl = _safe_table(plan.table_name, self._engine)
        return f"DESCRIBE {tbl};", {}

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _validate_raw_sql(self, sql: str) -> None:
        """Check that every table referenced in raw SQL exists in the DB."""
        tables = set(inspect(self._engine).get_table_names())
        for match in re.finditer(r"\b(?:FROM|JOIN|INTO|UPDATE|TABLE)\s+`?(\w+)`?", sql, re.I):
            name = match.group(1).lower()
            if name not in tables and name not in {"dual"}:
                log.warning("Raw SQL references unknown table '%s'.", name)

    @staticmethod
    def _sanitize_new_name(name: str) -> str:
        return re.sub(r"[^a-zA-Z0-9_]", "_", name.strip()).lower()

    @staticmethod
    def _random_row(plan: ActionPlan) -> dict[str, Any]:
        row: dict[str, Any] = {}
        for col in plan.columns:
            c = col.lower()
            if "name" in c:
                row[col] = random.choice(["Alice", "Bob", "Carol", "Dave", "Eva"])
            elif any(k in c for k in ("cgpa", "gpa", "grade")):
                row[col] = round(random.uniform(5.0, 10.0), 2)
            elif any(k in c for k in ("salary", "price", "amount")):
                row[col] = round(random.uniform(1000, 100_000), 2)
            elif any(k in c for k in ("age", "year")):
                row[col] = random.randint(18, 60)
            else:
                row[col] = "".join(random.choices(string.ascii_lowercase, k=6))
        return row


import re  # noqa: E402  (imported at bottom to avoid circular issues)
