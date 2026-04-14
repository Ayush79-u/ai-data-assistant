from __future__ import annotations

import re

import pandas as pd

from nl_data_assistant.models import ColumnSpec
from nl_data_assistant.utils.cleaning import normalize_identifier


class SchemaMapper:
    def parse_columns_from_text(self, phrase: str) -> list[ColumnSpec]:
        if not phrase.strip():
            return []

        cleaned_phrase = re.sub(r"\b(columns?|fields?)\b", "", phrase, flags=re.IGNORECASE)
        raw_parts = re.split(r",| and ", cleaned_phrase)
        columns = []
        for raw_name in raw_parts:
            value = raw_name.strip(" .")
            if not value:
                continue
            safe_name = normalize_identifier(value)
            columns.append(ColumnSpec(name=safe_name, data_type=self.guess_type_from_name(value)))
        return columns

    def guess_type_from_name(self, value: str) -> str:
        lowered = value.lower()
        if any(token in lowered for token in ("id", "roll", "age", "year", "count", "qty", "quantity")):
            return "INT"
        if any(
            token in lowered
            for token in ("cgpa", "gpa", "price", "amount", "cost", "score", "salary", "rate", "percent")
        ):
            return "FLOAT"
        if any(token in lowered for token in ("date", "time", "month")):
            return "DATETIME"
        return "VARCHAR(255)"

    def dataframe_to_columns(self, dataframe: pd.DataFrame) -> list[ColumnSpec]:
        columns: list[ColumnSpec] = []
        for name, dtype in dataframe.dtypes.items():
            columns.append(ColumnSpec(name=normalize_identifier(str(name)), data_type=self.map_dtype(dtype)))
        return columns

    def map_dtype(self, dtype: object) -> str:
        if pd.api.types.is_integer_dtype(dtype):
            return "BIGINT"
        if pd.api.types.is_float_dtype(dtype):
            return "FLOAT"
        if pd.api.types.is_bool_dtype(dtype):
            return "BOOLEAN"
        if pd.api.types.is_datetime64_any_dtype(dtype):
            return "DATETIME"
        return "VARCHAR(255)"

    def ensure_safe_name(self, value: str | None, fallback: str) -> str:
        return normalize_identifier(value or fallback)

