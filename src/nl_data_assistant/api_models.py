"""
api_models.py — Pydantic request / response schemas for the FastAPI layer.

Security notes:
  - RunRequest no longer exposes skip_confirmation (removed — security risk)
  - ExecuteRequest no longer accepts a raw 'sql' field (SQL injection vector removed)
"""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


# ── Requests ──────────────────────────────────────────────────────────────────

class RunRequest(BaseModel):
    command: str = Field(..., min_length=1, max_length=2000,
                         description="Natural-language command (max 2000 chars)")
    # skip_confirmation intentionally removed from the public API.
    # Destructive operations always require the two-step parse→confirm→execute flow.


class ExecuteRequest(BaseModel):
    """Execute a pre-parsed plan — use the output of /parse.
    The 'sql' field has been removed. Callers cannot submit raw SQL directly.
    """
    intent: str
    table_name: str = ""
    columns: list[str] = []
    values: list[dict[str, Any]] = []
    conditions: str = ""
    # sql: str = ""  ← REMOVED: was a SQL injection vector
    raw_command: str = ""


class BlueprintRequest(BaseModel):
    command: str = Field(..., description="e.g. 'Create a students table with name and cgpa'")


# ── Responses ─────────────────────────────────────────────────────────────────

class ParseResponse(BaseModel):
    intent: str
    table_name: str
    columns: list[str]
    sql: str
    is_destructive: bool
    raw_command: str


class RunResponse(BaseModel):
    success: bool
    message: str = ""
    sql_executed: str = ""
    rows_affected: int = 0
    data: list[dict[str, Any]] | None = None
    error: str = ""
