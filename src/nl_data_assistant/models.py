"""
models.py — shared data models for the NL data assistant pipeline.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class Intent(str, Enum):
    CREATE_TABLE = "create_table"
    INSERT = "insert"
    SELECT = "select"
    UPDATE = "update"
    DELETE = "delete"
    DROP_TABLE = "drop_table"
    IMPORT_EXCEL = "import_excel"
    EXPORT_EXCEL = "export_excel"
    CREATE_EXCEL = "create_excel"
    SHOW_EXCEL = "show_excel"
    VISUALIZE = "visualize"
    DESCRIBE = "describe"
    UNKNOWN = "unknown"


# Intents that modify or destroy data — require user confirmation in the UI.
DESTRUCTIVE_INTENTS: frozenset[Intent] = frozenset(
    {Intent.DELETE, Intent.DROP_TABLE, Intent.UPDATE}
)

DESTRUCTIVE_SQL_KEYWORDS: frozenset[str] = frozenset(
    {"drop", "delete", "truncate", "alter"}
)


def is_destructive_sql(sql: str) -> bool:
    first_word = sql.strip().split()[0].lower() if sql.strip() else ""
    return first_word in DESTRUCTIVE_SQL_KEYWORDS


@dataclass
class ActionPlan:
    intent: Intent = Intent.UNKNOWN
    table_name: str = ""
    columns: list[str] = field(default_factory=list)
    values: list[dict[str, Any]] = field(default_factory=list)
    conditions: str = ""
    order_by: str = ""
    limit: int | None = None
    file_path: str = ""
    chart_type: str = ""
    sql: str = ""                  # raw SQL when provided directly
    raw_command: str = ""          # original user text
    error: str = ""

    @property
    def is_destructive(self) -> bool:
        return self.intent in DESTRUCTIVE_INTENTS or is_destructive_sql(self.sql)


@dataclass
class ExecutionResult:
    success: bool
    message: str = ""
    data: Any = None              # DataFrame, list of dicts, or chart figure
    sql_executed: str = ""
    rows_affected: int = 0
    error: str = ""
