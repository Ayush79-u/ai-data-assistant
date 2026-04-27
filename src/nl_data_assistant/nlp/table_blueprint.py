"""
table_blueprint.py — Generate a JSON table schema + sample data locally.
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Any


NAME_SAMPLES = ["Ayush", "Riya", "Karan", "Neha", "Aman"]
TEXT_SAMPLES = ["sample_1", "sample_2", "sample_3", "sample_4", "sample_5"]


class TableBlueprint:
    def generate(self, command: str) -> dict:
        table_name = self._extract_table_name(command)
        columns = self._extract_columns(command)
        if not columns:
            columns = [{"name": "value", "type": "VARCHAR(255)", "primary_key": False}]

        full_columns = [{"name": "id", "type": "INT AUTO_INCREMENT", "primary_key": True}] + columns
        sample_data = self._sample_rows(columns, row_count=3)
        return {
            "table_name": table_name,
            "columns": full_columns,
            "sample_data": sample_data,
            "create_sql": self._build_create_sql(table_name, full_columns),
        }

    def _extract_table_name(self, command: str) -> str:
        patterns = [
            r"(?:create|make|build)(?:\s+a|\s+an)?\s+([a-zA-Z_][\w]*)\s+table",
            r"(?:create|make|build)(?:\s+a|\s+an)?\s+table(?:\s+of|\s+named|\s+called)?\s+([a-zA-Z_][\w]*)",
        ]
        for pattern in patterns:
            match = re.search(pattern, command, re.IGNORECASE)
            if match:
                return self._safe_name(match.group(1))
        return "new_table"

    def _extract_columns(self, command: str) -> list[dict[str, Any]]:
        match = re.search(r"\bwith\b\s+(.+)$", command, re.IGNORECASE)
        if not match:
            return []

        raw_columns = re.split(r",|\band\b", match.group(1), flags=re.IGNORECASE)
        results = []
        for raw in raw_columns:
            name = self._safe_name(raw.strip())
            if not name:
                continue
            results.append({"name": name, "type": self._infer_type(name), "primary_key": False})
        return results

    def _infer_type(self, column_name: str) -> str:
        name = column_name.lower()
        if any(token in name for token in ("id", "count", "qty", "quantity", "age", "year")):
            return "INT"
        if any(token in name for token in ("cgpa", "gpa", "salary", "price", "amount", "rate", "score")):
            return "FLOAT"
        if any(token in name for token in ("date", "time", "joined", "created")):
            return "DATETIME"
        if any(token in name for token in ("is_", "has_", "active", "enabled")):
            return "TINYINT(1)"
        return "VARCHAR(255)"

    def _sample_rows(self, columns: list[dict[str, Any]], row_count: int) -> list[dict[str, Any]]:
        rows = []
        for row_index in range(row_count):
            row = {}
            for column in columns:
                row[column["name"]] = self._sample_value(column["name"], column["type"], row_index)
            rows.append(row)
        return rows

    def _sample_value(self, name: str, sql_type: str, index: int) -> Any:
        lowered = name.lower()
        upper = sql_type.upper()

        if "name" in lowered:
            return NAME_SAMPLES[index % len(NAME_SAMPLES)]
        if upper.startswith("INT"):
            if "quantity" in lowered or "qty" in lowered:
                return [10, 20, 15, 8, 12][index % 5]
            return index + 1
        if upper.startswith("FLOAT") or upper.startswith("DECIMAL") or upper.startswith("DOUBLE"):
            if "cgpa" in lowered or "gpa" in lowered:
                return [8.5, 9.1, 7.8, 8.2, 9.4][index % 5]
            if "salary" in lowered:
                return [45000.0, 52000.0, 61000.0, 58000.0, 67000.0][index % 5]
            if "price" in lowered or "amount" in lowered:
                return [199.99, 349.5, 99.0, 499.0, 249.75][index % 5]
            return [10.5, 20.25, 30.75, 40.0, 50.5][index % 5]
        if upper.startswith("DATETIME"):
            value = datetime(2026, 4, 1, 9, 0, 0) + timedelta(days=index)
            return value.strftime("%Y-%m-%d %H:%M:%S")
        if upper.startswith("TINYINT"):
            return [1, 0, 1, 1, 0][index % 5]
        return TEXT_SAMPLES[index % len(TEXT_SAMPLES)] if "name" not in lowered else NAME_SAMPLES[index % len(NAME_SAMPLES)]

    def _build_create_sql(self, table_name: str, columns: list[dict[str, Any]]) -> str:
        lines = []
        for column in columns:
            if column.get("primary_key"):
                lines.append(f"  `{column['name']}` {column['type']} PRIMARY KEY")
            else:
                lines.append(f"  `{column['name']}` {column['type']}")
        return f"CREATE TABLE IF NOT EXISTS `{table_name}` (\n" + ",\n".join(lines) + "\n);"

    def _safe_name(self, value: str) -> str:
        return re.sub(r"[^a-zA-Z0-9_]+", "_", value.strip().lower()).strip("_")
