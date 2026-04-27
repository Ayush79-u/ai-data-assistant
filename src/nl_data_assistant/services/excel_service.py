"""
excel_service.py — Read, write, and inspect Excel workbooks.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

_EXCEL_ENGINE = "openpyxl"


class ExcelService:
    def __init__(self, default_path: Path | str | None = None):
        self._default_path = Path(default_path) if default_path else None

    # ── Read ──────────────────────────────────────────────────────────────────

    def read_sheet(
        self,
        path: Path | str | None = None,
        sheet: str | int = 0,
    ) -> pd.DataFrame:
        p = self._resolve(path)
        df = pd.read_excel(p, sheet_name=sheet, engine=_EXCEL_ENGINE)
        return self._clean_headers(df)

    def list_sheets(self, path: Path | str | None = None) -> list[str]:
        p = self._resolve(path)
        xl = pd.ExcelFile(p, engine=_EXCEL_ENGINE)
        return xl.sheet_names

    def read_all_sheets(self, path: Path | str | None = None) -> dict[str, pd.DataFrame]:
        p = self._resolve(path)
        xl = pd.ExcelFile(p, engine=_EXCEL_ENGINE)
        return {
            name: self._clean_headers(xl.parse(name))
            for name in xl.sheet_names
        }

    # ── Write ─────────────────────────────────────────────────────────────────

    def write_sheet(
        self,
        df: pd.DataFrame,
        path: Path | str | None = None,
        sheet: str = "Sheet1",
        mode: str = "w",
    ) -> Path:
        """Write a DataFrame to a sheet. mode='a' appends without overwriting others."""
        p = self._resolve(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        kw = {"if_sheet_exists": "replace"} if mode == "a" and p.exists() else {}
        with pd.ExcelWriter(p, engine=_EXCEL_ENGINE, mode=mode, **kw) as writer:
            df.to_excel(writer, sheet_name=sheet, index=False)
        log.info("Wrote %d rows to %s[%s]", len(df), p, sheet)
        return p

    def create_blank(
        self,
        columns: list[str],
        path: Path | str | None = None,
        sheet: str = "Sheet1",
    ) -> Path:
        df = pd.DataFrame(columns=columns)
        return self.write_sheet(df, path, sheet)

    # ── Schema ────────────────────────────────────────────────────────────────

    def infer_schema(self, path: Path | str | None = None, sheet: str | int = 0) -> dict:
        """Return column names and inferred MySQL types."""
        df = self.read_sheet(path, sheet)
        return {col: _infer_mysql_type(df[col]) for col in df.columns}

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _resolve(self, path: Path | str | None) -> Path:
        p = path or self._default_path
        if p is None:
            raise ValueError("No Excel file path provided.")
        return Path(p)

    @staticmethod
    def _clean_headers(df: pd.DataFrame) -> pd.DataFrame:
        df.columns = (
            df.columns.astype(str)
            .str.strip()
            .str.lower()
            .str.replace(r"[\s\-/]+", "_", regex=True)
            .str.replace(r"[^a-z0-9_]", "", regex=True)
        )
        # Drop completely empty rows and columns
        df = df.dropna(how="all").dropna(axis=1, how="all")
        return df.reset_index(drop=True)


# ── Type inference ────────────────────────────────────────────────────────────

def _infer_mysql_type(series: pd.Series) -> str:
    """Infer a MySQL column type from the full column, not just the first row."""
    s = series.dropna()
    if s.empty:
        return "VARCHAR(255)"

    if pd.api.types.is_bool_dtype(s):
        return "TINYINT(1)"
    if pd.api.types.is_integer_dtype(s):
        mx = s.abs().max()
        if mx <= 127:
            return "TINYINT"
        if mx <= 32_767:
            return "SMALLINT"
        if mx <= 2_147_483_647:
            return "INT"
        return "BIGINT"
    if pd.api.types.is_float_dtype(s):
        return "DECIMAL(15,4)"
    if pd.api.types.is_datetime64_any_dtype(s):
        return "DATETIME"

    # Try parsing as datetime
    try:
        pd.to_datetime(s, infer_datetime_format=True)
        return "DATE"
    except (ValueError, TypeError):
        pass

    max_len = int(s.astype(str).str.len().max())
    if max_len > 255:
        return "TEXT"
    return f"VARCHAR({min(max_len * 2 + 10, 255)})"
