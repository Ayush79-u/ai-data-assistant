from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class CommandRequest(BaseModel):
    command: str = Field(..., min_length=1)
    target: Literal["auto", "excel", "mysql"] = "auto"
    uploaded_file_path: str | None = None


class BlueprintRequest(BaseModel):
    command: str = Field(..., min_length=1)
    sample_rows: int = Field(default=3, ge=3, le=5)


class BlueprintColumn(BaseModel):
    name: str
    type: str


class BlueprintResponse(BaseModel):
    table_name: str
    columns: list[BlueprintColumn]
    sample_data: list[dict[str, Any]]


class ParseResponse(BaseModel):
    success: bool
    plan: dict[str, Any]
    sql: str | None = None
    notes: list[str] = Field(default_factory=list)


class ExecuteResponse(BaseModel):
    success: bool
    message: str
    plan: dict[str, Any]
    file_path: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    data_preview: list[dict[str, Any]] = Field(default_factory=list)
    chart: dict[str, Any] | None = None


class HealthResponse(BaseModel):
    status: str
    mysql_configured: bool
    llm_enabled: bool
