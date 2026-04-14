from __future__ import annotations

from pathlib import Path

import pandas as pd

from nl_data_assistant.models import ColumnSpec


class ExcelService:
    def create_sheet(self, workbook_path: str | Path, sheet_name: str, columns: list[ColumnSpec]) -> Path:
        path = Path(workbook_path).resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        headers = [column.name for column in columns] or ["id"]
        dataframe = pd.DataFrame(columns=headers)
        self.write_dataframe(path, dataframe, sheet_name=sheet_name)
        return path

    def write_dataframe(self, workbook_path: str | Path, dataframe: pd.DataFrame, sheet_name: str) -> Path:
        path = Path(workbook_path).resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            with pd.ExcelWriter(path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
                dataframe.to_excel(writer, index=False, sheet_name=sheet_name[:31] or "Sheet1")
        else:
            with pd.ExcelWriter(path, engine="openpyxl", mode="w") as writer:
                dataframe.to_excel(writer, index=False, sheet_name=sheet_name[:31] or "Sheet1")
        return path

    def read_sheet(self, workbook_path: str | Path, sheet_name: str | int | None = None) -> pd.DataFrame:
        path = Path(workbook_path).resolve()
        selected_sheet = 0 if sheet_name in (None, "") else sheet_name
        return pd.read_excel(path, sheet_name=selected_sheet)

    def list_sheets(self, workbook_path: str | Path) -> list[str]:
        workbook = pd.ExcelFile(Path(workbook_path).resolve())
        return workbook.sheet_names

    def describe_sheet(self, workbook_path: str | Path, sheet_name: str | int | None = None) -> list[dict[str, str]]:
        dataframe = self.read_sheet(workbook_path, sheet_name=sheet_name)
        rows = []
        for name, dtype in dataframe.dtypes.items():
            rows.append({"column": str(name), "dtype": str(dtype)})
        return rows

