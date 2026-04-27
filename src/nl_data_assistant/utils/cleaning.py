"""
utils/cleaning.py — DataFrame normalization utilities.
"""
from __future__ import annotations

import re

import pandas as pd


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize a raw DataFrame for MySQL import:
    - Strip + lowercase column names, replace spaces/dashes with underscores
    - Remove completely empty rows and columns
    - Strip leading/trailing whitespace from string cells
    - Coerce numeric-looking strings to numbers
    - Parse date-looking strings to datetime
    """
    df = df.copy()

    # Clean column names
    df.columns = _clean_column_names(df.columns)

    # Drop fully empty rows and columns
    df = df.dropna(how="all").dropna(axis=1, how="all")

    # Strip string values
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda s: s.str.strip() if hasattr(s, "str") else s)

    # Coerce numeric strings
    for col in str_cols:
        df[col] = _try_numeric(df[col])

    # Coerce date strings
    for col in df.select_dtypes(include="object").columns:
        df[col] = _try_datetime(df[col])

    return df.reset_index(drop=True)


def _clean_column_names(columns: pd.Index) -> pd.Index:
    cleaned = (
        columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(r"[\s\-/\\\.]+", "_", regex=True)
        .str.replace(r"[^a-z0-9_]", "", regex=True)
        .str.replace(r"^(\d)", r"col_\1", regex=True)  # can't start with digit
    )
    # Deduplicate
    seen: dict[str, int] = {}
    result = []
    for name in cleaned:
        if name in seen:
            seen[name] += 1
            result.append(f"{name}_{seen[name]}")
        else:
            seen[name] = 0
            result.append(name)
    return pd.Index(result)


def _try_numeric(series: pd.Series) -> pd.Series:
    try:
        converted = pd.to_numeric(series, errors="coerce")
        # Only convert if >50 % of non-null values parsed successfully
        if converted.notna().sum() / max(series.notna().sum(), 1) > 0.5:
            return converted
    except Exception:
        pass
    return series


def _try_datetime(series: pd.Series) -> pd.Series:
    try:
        converted = pd.to_datetime(series, infer_datetime_format=True, errors="coerce")
        if converted.notna().sum() / max(series.notna().sum(), 1) > 0.5:
            return converted
    except Exception:
        pass
    return series
