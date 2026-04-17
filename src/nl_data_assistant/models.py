from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(slots=True)
class ColumnSpec:
    name: str
    data_type: str = "VARCHAR(255)"
    nullable: bool = True


@dataclass(slots=True)
class ActionPlan:
    action: str
    target: str | None = None
    table_name: str | None = None
    sheet_name: str | None = None
    workbook_path: str | None = None
    source_path: str | None = None
    destination_path: str | None = None
    columns: list[ColumnSpec] = field(default_factory=list)
    query: str | None = None
    parameters: Any = None
    chart_type: str | None = None
    x_column: str | None = None
    y_column: str | None = None
    title: str | None = None
    limit: int = 200
    use_last_result: bool = False
    entities: dict[str, Any] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ExecutionResult:
    success: bool
    message: str
    plan: ActionPlan
    dataframe: Any = None
    figure: Any = None
    file_path: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
