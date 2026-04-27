"""
utils/schema.py — DB schema introspection helpers.
"""
from __future__ import annotations

import pandas as pd
from sqlalchemy import Engine, inspect, text


def get_schema_summary(engine: Engine) -> str:
    """
    Return a compact, human-readable schema string suitable for
    injecting into a Claude system prompt.

    Example output:
      tables: students(id INT, name VARCHAR, cgpa FLOAT); expenses(id INT, month VARCHAR, amount DECIMAL)
    """
    insp = inspect(engine)
    parts: list[str] = []
    for table in insp.get_table_names():
        cols = ", ".join(
            f"{c['name']} {c['type']}" for c in insp.get_columns(table)
        )
        parts.append(f"{table}({cols})")
    return "tables: " + "; ".join(parts) if parts else "(no tables yet)"


def get_table_info(engine: Engine, table_name: str) -> pd.DataFrame:
    """Return column info for a single table as a DataFrame."""
    insp = inspect(engine)
    cols = insp.get_columns(table_name)
    return pd.DataFrame([
        {
            "column": c["name"],
            "type": str(c["type"]),
            "nullable": c.get("nullable", True),
            "default": c.get("default"),
        }
        for c in cols
    ])


def table_exists(engine: Engine, table_name: str) -> bool:
    return table_name.lower() in {t.lower() for t in inspect(engine).get_table_names()}


def get_row_count(engine: Engine, table_name: str) -> int:
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM `{table_name}`"))
        return result.scalar() or 0
