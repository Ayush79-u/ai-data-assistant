from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from nl_data_assistant.nlp.local_parser import GeneratedSQL, generate_sql
from nl_data_assistant.utils.cleaning import normalize_identifier
from nl_data_assistant.utils.schema import SchemaMapper


SYSTEM_COLUMNS = {"id", "created_at", "updated_at"}


@dataclass(slots=True)
class SchemaCatalog:
    tables: dict[str, list[str]]

    @classmethod
    def from_snapshot(cls, snapshot: dict[str, list[str]] | None) -> "SchemaCatalog":
        normalized: dict[str, list[str]] = {}
        for table_name, columns in (snapshot or {}).items():
            normalized_table = normalize_identifier(table_name)
            normalized[normalized_table] = [normalize_identifier(column) for column in columns]
        return cls(tables=normalized)

    def resolve_table(self, candidate: str | None) -> str | None:
        if not candidate:
            return None
        normalized = normalize_identifier(candidate)
        if normalized in self.tables:
            return normalized

        singular = normalized.rstrip("s")
        plural = f"{normalized}s"
        for option in (singular, plural):
            if option in self.tables:
                return option

        for table_name in self.tables:
            if table_name.startswith(normalized) or normalized.startswith(table_name):
                return table_name
        return normalized

    def columns_for(self, table_name: str | None) -> list[str]:
        if not table_name:
            return []
        return list(self.tables.get(self.resolve_table(table_name) or "", []))

    def resolve_column(self, table_name: str | None, candidate: str | None) -> str | None:
        if not candidate:
            return None

        normalized = normalize_identifier(candidate)
        available_columns = self.columns_for(table_name)
        if not available_columns:
            return normalized
        if normalized in available_columns:
            return normalized

        singular = normalized.rstrip("s")
        plural = f"{normalized}s"
        for option in (singular, plural):
            if option in available_columns:
                return option

        compressed = normalized.replace("_", "")
        for column in available_columns:
            if column.replace("_", "") == compressed:
                return column
            if normalized in column.split("_"):
                return column
            if column.startswith(normalized) or normalized.startswith(column):
                return column
        return normalized


class MySQLQueryGenerator:
    def __init__(self, schema_snapshot: dict[str, list[str]] | None = None, schema_mapper: SchemaMapper | None = None) -> None:
        self.catalog = SchemaCatalog.from_snapshot(schema_snapshot)
        self.schema_mapper = schema_mapper or SchemaMapper()

    def generate(self, intent: str, entities: dict[str, Any]) -> GeneratedSQL:
        refined = self.refine_entities(entities, intent)
        return generate_sql(intent, refined, schema_mapper=self.schema_mapper)

    def refine_entities(self, entities: dict[str, Any], intent: str) -> dict[str, Any]:
        refined = dict(entities)
        resolved_table = self.catalog.resolve_table(refined.get("table_name"))
        refined["table_name"] = resolved_table

        available_columns = self.catalog.columns_for(resolved_table)
        if available_columns:
            refined["columns"] = self._resolve_column_list(resolved_table, refined.get("columns", []))
            refined["selected_columns"] = self._resolve_column_list(resolved_table, refined.get("selected_columns", []))
            refined["random_fields"] = self._resolve_column_list(resolved_table, refined.get("random_fields", []))
            refined["explicit_values"] = self._resolve_mapping(resolved_table, refined.get("explicit_values", {}))
            refined["assignments"] = self._resolve_mapping(resolved_table, refined.get("assignments", {}))
            refined["conditions"] = self._resolve_conditions(resolved_table, refined.get("conditions", []))
            refined["order_by"] = self.catalog.resolve_column(resolved_table, refined.get("order_by"))

            if intent == "insert":
                refined["rows"] = [self._resolve_mapping(resolved_table, row) for row in refined.get("rows", [])]
                if not refined.get("columns"):
                    refined["columns"] = [column for column in available_columns if column not in SYSTEM_COLUMNS] or available_columns

                allowed_columns = [column for column in available_columns if column not in SYSTEM_COLUMNS] or available_columns
                filtered_columns = [column for column in allowed_columns if column in set(refined["columns"])]
                refined["columns"] = filtered_columns or refined["columns"]
                refined["rows"] = [
                    {column: row.get(column) for column in refined["columns"] if column in row}
                    for row in refined.get("rows", [])
                ]

        return refined

    def _resolve_mapping(self, table_name: str | None, values: dict[str, Any]) -> dict[str, Any]:
        resolved: dict[str, Any] = {}
        for column, value in values.items():
            key = self.catalog.resolve_column(table_name, column)
            if key:
                resolved[key] = value
        return resolved

    def _resolve_conditions(self, table_name: str | None, conditions: list[dict[str, Any]]) -> list[dict[str, Any]]:
        resolved = []
        for condition in conditions:
            column = self.catalog.resolve_column(table_name, condition.get("column"))
            if column:
                resolved.append({**condition, "column": column})
        return resolved

    def _resolve_column_list(self, table_name: str | None, columns: list[str]) -> list[str]:
        resolved = []
        for column in columns:
            mapped = self.catalog.resolve_column(table_name, column)
            if mapped:
                resolved.append(mapped)
        return list(dict.fromkeys(resolved))
