from __future__ import annotations

import re

import pandas as pd


def normalize_identifier(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", str(value).strip().lower()).strip("_")
    return cleaned or "column"


class DataCleaner:
    """Normalizes headers, trims strings, and gently coerces data types."""

    def clean_dataframe(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        cleaned = dataframe.copy()
        cleaned.columns = [normalize_identifier(column) for column in cleaned.columns]
        cleaned = cleaned.replace(r"^\s*$", pd.NA, regex=True)
        cleaned = cleaned.dropna(how="all")
        cleaned = cleaned.dropna(axis=1, how="all")

        for column in cleaned.columns:
            series = cleaned[column]
            if pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series):
                cleaned[column] = series.astype("string").str.strip()
            cleaned[column] = self._coerce_series(cleaned[column], column)

        return cleaned.reset_index(drop=True)

    def _coerce_series(self, series: pd.Series, column_name: str) -> pd.Series:
        if series.dropna().empty:
            return series

        numeric = pd.to_numeric(series, errors="coerce")
        if numeric.notna().sum() >= max(1, int(series.notna().sum() * 0.7)):
            return numeric

        if any(token in column_name for token in ("date", "time", "month", "year")):
            parsed = pd.to_datetime(series, errors="coerce", format="mixed")
            if parsed.notna().sum() >= max(1, int(series.notna().sum() * 0.7)):
                return parsed

        if self._looks_temporal(series):
            parsed = pd.to_datetime(series, errors="coerce", format="mixed")
            if parsed.notna().sum() >= max(1, int(series.notna().sum() * 0.8)):
                return parsed

        return series

    def _looks_temporal(self, series: pd.Series) -> bool:
        sample = series.dropna().astype(str).head(10)
        if sample.empty:
            return False

        temporal_pattern = (
            r"^\d{4}-\d{1,2}-\d{1,2}$|^\d{1,2}/\d{1,2}/\d{2,4}$|"
            r"^\d{1,2}-\d{1,2}-\d{2,4}$|^\d{1,2}:\d{2}(:\d{2})?$"
        )
        hits = sample.str.match(temporal_pattern, na=False).sum()
        return hits >= max(1, int(len(sample) * 0.6))
