from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from nl_data_assistant.models import ColumnSpec
from nl_data_assistant.nlp.local_parser import detect_intent, extract_entities
from nl_data_assistant.utils.schema import SchemaMapper


NAME_SAMPLES = ["Ayush", "Riya", "Karan", "Neha", "Aman"]
PRODUCT_SAMPLES = ["Notebook", "Keyboard", "Bottle", "Monitor", "Headphones"]
CATEGORY_SAMPLES = ["Electronics", "Stationery", "Home", "Travel", "Health"]
DEPARTMENT_SAMPLES = ["Sales", "HR", "Finance", "Engineering", "Support"]
EMAIL_SAMPLES = [
    "ayush@example.com",
    "riya@example.com",
    "karan@example.com",
    "neha@example.com",
    "aman@example.com",
]


@dataclass(slots=True)
class TableBlueprint:
    table_name: str
    columns: list[dict[str, str]]
    sample_data: list[dict[str, Any]]

    def as_dict(self) -> dict[str, Any]:
        return {
            "table_name": self.table_name,
            "columns": self.columns,
            "sample_data": self.sample_data,
        }


def command_to_blueprint(
    text: str,
    sample_rows: int = 3,
    schema_mapper: SchemaMapper | None = None,
) -> dict[str, Any]:
    schema_mapper = schema_mapper or SchemaMapper()
    intent = detect_intent(text)
    if intent != "create_table":
        raise ValueError("Blueprint generation currently supports table-creation style commands only.")

    entities = extract_entities(text, intent="create_table", schema_mapper=schema_mapper)
    table_name = entities.get("table_name") or "new_table"
    column_specs: list[ColumnSpec] = entities.get("column_specs") or [ColumnSpec(name="id", data_type="INT")]

    safe_row_count = max(3, min(5, int(sample_rows)))
    blueprint = TableBlueprint(
        table_name=table_name,
        columns=[{"name": column.name, "type": column.data_type} for column in column_specs],
        sample_data=build_sample_rows(column_specs, safe_row_count),
    )
    return blueprint.as_dict()


def build_sample_rows(columns: list[ColumnSpec], row_count: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index in range(row_count):
        row = {}
        for column in columns:
            row[column.name] = sample_value(column.name, column.data_type, index)
        rows.append(row)
    return rows


def sample_value(column_name: str, sql_type: str, index: int) -> Any:
    lowered = column_name.lower()
    sql_upper = sql_type.upper()

    if "name" in lowered:
        return NAME_SAMPLES[index % len(NAME_SAMPLES)]
    if "product" in lowered:
        return PRODUCT_SAMPLES[index % len(PRODUCT_SAMPLES)]
    if "category" in lowered:
        return CATEGORY_SAMPLES[index % len(CATEGORY_SAMPLES)]
    if "department" in lowered:
        return DEPARTMENT_SAMPLES[index % len(DEPARTMENT_SAMPLES)]
    if "email" in lowered:
        return EMAIL_SAMPLES[index % len(EMAIL_SAMPLES)]

    if sql_upper.startswith("INT") or sql_upper.startswith("BIGINT"):
        if "quantity" in lowered:
            return [12, 20, 8, 15, 30][index % 5]
        return [1, 2, 3, 4, 5][index % 5]

    if sql_upper.startswith("FLOAT") or sql_upper.startswith("DOUBLE") or sql_upper.startswith("DECIMAL"):
        if "cgpa" in lowered or "gpa" in lowered:
            return [8.5, 9.1, 7.8, 8.2, 9.4][index % 5]
        if "salary" in lowered:
            return [45000.0, 52000.0, 61000.0, 58000.0, 67000.0][index % 5]
        if "price" in lowered:
            return [199.99, 349.5, 99.0, 499.0, 249.75][index % 5]
        return [10.5, 20.75, 30.25, 40.0, 50.5][index % 5]

    if "DATE" in sql_upper or "TIME" in sql_upper:
        base = datetime(2026, 4, 1, 10, 0, 0) + timedelta(days=index)
        return base.strftime("%Y-%m-%d %H:%M:%S")

    return f"{column_name}_{index + 1}"

