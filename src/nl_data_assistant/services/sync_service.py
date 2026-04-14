from __future__ import annotations

from pathlib import Path

import pandas as pd

from nl_data_assistant.services.excel_service import ExcelService
from nl_data_assistant.services.mysql_service import MySQLService
from nl_data_assistant.utils.cleaning import DataCleaner
from nl_data_assistant.utils.schema import SchemaMapper


class SyncService:
    def __init__(
        self,
        excel_service: ExcelService,
        mysql_service: MySQLService,
        cleaner: DataCleaner,
        schema_mapper: SchemaMapper,
    ) -> None:
        self.excel_service = excel_service
        self.mysql_service = mysql_service
        self.cleaner = cleaner
        self.schema_mapper = schema_mapper

    def excel_to_mysql(
        self,
        source_path: str | Path,
        table_name: str,
        sheet_name: str | int | None = None,
        if_exists: str = "replace",
    ) -> tuple[pd.DataFrame, list[dict[str, str]]]:
        dataframe = self.excel_service.read_sheet(source_path, sheet_name=sheet_name)
        cleaned = self.cleaner.clean_dataframe(dataframe)
        self.mysql_service.write_dataframe(cleaned, table_name=table_name, if_exists=if_exists)
        mapping = [
            {"column": column.name, "mysql_type": column.data_type}
            for column in self.schema_mapper.dataframe_to_columns(cleaned)
        ]
        return cleaned, mapping

    def mysql_to_excel(
        self,
        table_name: str,
        destination_path: str | Path,
        sheet_name: str | None = None,
        limit: int = 100000,
    ) -> tuple[pd.DataFrame, Path]:
        dataframe = self.mysql_service.read_table(table_name, limit=limit)
        cleaned = self.cleaner.clean_dataframe(dataframe)
        output_path = self.excel_service.write_dataframe(destination_path, cleaned, sheet_name=sheet_name or table_name)
        return cleaned, output_path

