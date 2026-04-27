"""
sync_service.py — Bidirectional sync between Excel and MySQL.
"""
from __future__ import annotations

import logging
import re
from pathlib import Path

import pandas as pd
from sqlalchemy import Engine, text

from nl_data_assistant.services.excel_service import ExcelService, _infer_mysql_type

log = logging.getLogger(__name__)


class SyncService:
    def __init__(self, engine: Engine, excel: ExcelService | None = None):
        self._engine = engine
        self._excel = excel or ExcelService()

    # ── Excel → MySQL ─────────────────────────────────────────────────────────

    def excel_to_mysql(
        self,
        file_path: Path | str,
        table_name: str,
        sheet: str | int = 0,
        if_exists: str = "replace",   # "replace" | "append" | "fail"
        chunk_size: int = 500,
    ) -> int:
        """
        Import an Excel sheet into MySQL.
        Returns the number of rows inserted.
        """
        df = self._excel.read_sheet(file_path, sheet)
        if df.empty:
            raise ValueError(f"The sheet '{sheet}' in '{file_path}' is empty.")

        safe_name = _sanitize_identifier(table_name)

        # Create table first with correct column types
        if if_exists == "replace":
            self._create_table_from_df(df, safe_name)

        total = 0
        for chunk_df in _chunked(df, chunk_size):
            chunk_df.to_sql(
                safe_name,
                self._engine,
                if_exists="append",
                index=False,
                method="multi",
            )
            total += len(chunk_df)
            log.debug("Inserted chunk: %d rows so far", total)

        log.info("Imported %d rows into `%s`", total, safe_name)
        return total

    def _create_table_from_df(self, df: pd.DataFrame, table_name: str) -> None:
        col_defs = ["  `id` INT AUTO_INCREMENT PRIMARY KEY"]
        for col in df.columns:
            mysql_type = _infer_mysql_type(df[col])
            col_defs.append(f"  `{col}` {mysql_type}")
        ddl = (
            f"DROP TABLE IF EXISTS `{table_name}`;\n"
            f"CREATE TABLE `{table_name}` (\n"
            + ",\n".join(col_defs)
            + "\n);"
        )
        with self._engine.begin() as conn:
            for stmt in ddl.split(";"):
                stmt = stmt.strip()
                if stmt:
                    conn.execute(text(stmt))

    # ── MySQL → Excel ─────────────────────────────────────────────────────────

    def mysql_to_excel(
        self,
        table_name: str,
        file_path: Path | str,
        sheet_name: str | None = None,
        conditions: str = "",
        limit: int | None = None,
    ) -> Path:
        """
        Export a MySQL table to an Excel file.
        Returns the path to the written file.
        """
        safe_name = _sanitize_identifier(table_name)
        sql = f"SELECT * FROM `{safe_name}`"
        if conditions:
            sql += f" WHERE {conditions}"
        if limit:
            sql += f" LIMIT {int(limit)}"

        with self._engine.connect() as conn:
            df = pd.read_sql(text(sql), conn)

        if df.empty:
            log.warning("mysql_to_excel: query returned 0 rows.")

        out = Path(file_path)
        sheet = sheet_name or table_name
        self._excel.write_sheet(df, out, sheet=sheet)
        log.info("Exported %d rows from `%s` to %s", len(df), safe_name, out)
        return out


# ── Utilities ─────────────────────────────────────────────────────────────────

def _sanitize_identifier(name: str) -> str:
    clean = re.sub(r"[^a-zA-Z0-9_]", "_", name.strip()).lower()
    if not clean or clean[0].isdigit():
        clean = "t_" + clean
    return clean


def _chunked(df: pd.DataFrame, size: int):
    for i in range(0, len(df), size):
        yield df.iloc[i : i + size]
