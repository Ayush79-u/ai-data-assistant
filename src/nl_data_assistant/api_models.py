"""
api_models.py — Pydantic request / response schemas for the FastAPI layer.
"""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


# ── Requests ──────────────────────────────────────────────────────────────────

class RunRequest(BaseModel):
    command: str = Field(..., min_length=1, description="Natural-language command")
    skip_confirmation: bool = Field(
        False,
        description="Set to True to execute destructive ops without a confirmation round-trip.",
    )


class ExecuteRequest(BaseModel):
    """Execute a pre-parsed plan — use the output of /parse."""
    intent: str
    table_name: str = ""
    columns: list[str] = []
    values: list[dict[str, Any]] = []
    conditions: str = ""
    sql: str = ""
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
