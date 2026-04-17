from __future__ import annotations

from pathlib import Path

import pandas as pd

from nl_data_assistant.config import AppConfig
from nl_data_assistant.models import ActionPlan, ColumnSpec, ExecutionResult
from nl_data_assistant.nlp.interpreter import CommandInterpreter
from nl_data_assistant.services.excel_service import ExcelService
from nl_data_assistant.services.mysql_service import MySQLService
from nl_data_assistant.services.sync_service import SyncService
from nl_data_assistant.services.visualization_service import VisualizationService
from nl_data_assistant.utils.cleaning import DataCleaner
from nl_data_assistant.utils.schema import SchemaMapper


class DataAssistantEngine:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.cleaner = DataCleaner()
        self.schema_mapper = SchemaMapper()
        self.interpreter = CommandInterpreter(config.openai_api_key, config.openai_model)
        self.excel_service = ExcelService()
        self.mysql_service = MySQLService(config.mysql_url)
        self.sync_service = SyncService(
            excel_service=self.excel_service,
            mysql_service=self.mysql_service,
            cleaner=self.cleaner,
            schema_mapper=self.schema_mapper,
        )
        self.visualization_service = VisualizationService()

    def run(
        self,
        command: str,
        context: list[dict[str, str]] | None = None,
        default_target: str | None = None,
        uploaded_file_path: str | None = None,
        current_dataframe: pd.DataFrame | None = None,
        current_table: str | None = None,
    ) -> ExecutionResult:
        resolved_command = self._resolve_command_with_context(command, context=context, current_table=current_table)
        return self.execute(
            command=resolved_command,
            default_target=default_target,
            uploaded_file_path=uploaded_file_path,
            current_dataframe=current_dataframe,
        )

    def execute(
        self,
        command: str,
        default_target: str | None = None,
        uploaded_file_path: str | None = None,
        current_dataframe: pd.DataFrame | None = None,
    ) -> ExecutionResult:
        effective_target = None if default_target == "auto" else (default_target or self.config.default_target)
        plan = self.parse_command(command, default_target=effective_target)
        self._hydrate_plan_from_context(plan, uploaded_file_path)

        try:
            if plan.action == "create_table":
                return self._create_table(plan)
            if plan.action in {"insert", "update", "delete"}:
                return self._mutate_rows(plan)
            if plan.action == "query":
                return self._query(plan)
            if plan.action == "visualize":
                return self._visualize(plan, current_dataframe=current_dataframe)
            if plan.action == "excel_to_mysql":
                return self._excel_to_mysql(plan)
            if plan.action == "mysql_to_excel":
                return self._mysql_to_excel(plan)
            if plan.action == "clean_data":
                return self._clean_data(plan)
            if plan.action == "describe_schema":
                return self._describe_schema(plan)
            message = plan.notes[0] if plan.notes else "I could not map that request to a supported action."
            return ExecutionResult(False, message, plan)
        except Exception as exc:
            return ExecutionResult(False, f"{type(exc).__name__}: {exc}", plan)

    def parse_command(self, command: str, default_target: str | None = None) -> ActionPlan:
        mysql_schema = self._get_mysql_schema_snapshot(default_target)
        return self.interpreter.interpret(command, default_target=default_target, mysql_schema=mysql_schema)

    def get_mysql_schema_catalog(self) -> dict[str, list[str]]:
        return self.mysql_service.get_schema_catalog()

    def _get_mysql_schema_snapshot(self, default_target: str | None) -> dict[str, list[str]]:
        if default_target == "excel":
            return {}
        return self.mysql_service.get_schema_catalog()

    def _resolve_command_with_context(
        self,
        command: str,
        context: list[dict[str, str]] | None = None,
        current_table: str | None = None,
    ) -> str:
        text = command.strip()
        lowered = text.lower()
        active_table = current_table or self._extract_recent_table_from_context(context)

        if not active_table:
            return text

        if any(phrase in lowered for phrase in ("show data", "show the data", "show rows", "show table", "show all data")):
            return f"show all {active_table}"

        if any(phrase in lowered for phrase in ("add rows", "add row", "insert rows", "insert row")):
            return self._rewrite_row_insert_command(text, active_table)

        if lowered.startswith("add ") and "table" not in lowered and active_table not in lowered:
            return self._rewrite_row_insert_command(text, active_table)

        if lowered.startswith("insert ") and active_table not in lowered and " into " not in lowered:
            return self._rewrite_row_insert_command(text, active_table)

        return text

    def _rewrite_row_insert_command(self, command: str, current_table: str) -> str:
        lowered = command.lower().strip()
        if current_table in lowered:
            return command
        cleaned = (
            command.replace("rows", "")
            .replace("row", "")
            .replace("Rows", "")
            .replace("Row", "")
            .strip()
        )
        if "random" in lowered:
            base = cleaned if cleaned else command.strip()
            return f"{base} {current_table}"
        if lowered.startswith("add "):
            return cleaned.replace("add", "insert", 1).strip() + f" {current_table}"
        if lowered.startswith("insert "):
            return f"{cleaned.strip()} {current_table}"
        return command

    def _extract_recent_table_from_context(self, context: list[dict[str, str]] | None) -> str | None:
        if not context:
            return None
        for message in reversed(context):
            metadata = message.get("metadata", {})
            if isinstance(metadata, dict) and metadata.get("table_name"):
                return str(metadata["table_name"])
        return None

    def _hydrate_plan_from_context(self, plan: ActionPlan, uploaded_file_path: str | None) -> None:
        if uploaded_file_path and not plan.source_path and plan.target == "excel":
            plan.source_path = uploaded_file_path
        if plan.target == "excel" and not plan.workbook_path:
            plan.workbook_path = str(Path(uploaded_file_path).resolve()) if uploaded_file_path else str(self.config.default_workbook)
        if plan.target == "excel" and plan.table_name and not plan.sheet_name:
            plan.sheet_name = plan.table_name
        if plan.target == "mysql" and plan.sheet_name and not plan.table_name:
            plan.table_name = plan.sheet_name

    def _create_table(self, plan: ActionPlan) -> ExecutionResult:
        columns = plan.columns or [ColumnSpec(name="id", data_type="INT")]
        if plan.target == "excel":
            sheet_name = plan.sheet_name or "sheet1"
            workbook_path = plan.workbook_path or str(self.config.default_workbook)
            path = self.excel_service.create_sheet(workbook_path, sheet_name, columns)
            return ExecutionResult(
                True,
                f"Created Excel sheet '{sheet_name}' with {len(columns)} column(s).",
                plan,
                file_path=str(path),
            )

        table_name = plan.table_name or "new_table"
        if plan.query:
            self.mysql_service.execute_statement(plan.query, plan.parameters)
        else:
            self.mysql_service.create_table(table_name, columns)
        return ExecutionResult(
            True,
            f"Created MySQL table '{table_name}' with {len(columns)} column(s).",
            plan,
            metadata={"table_name": table_name, "sql": plan.query},
        )

    def _query(self, plan: ActionPlan) -> ExecutionResult:
        if plan.target == "excel":
            source = plan.source_path or plan.workbook_path or str(self.config.default_workbook)
            dataframe = self.cleaner.clean_dataframe(
                self.excel_service.read_sheet(source, sheet_name=plan.sheet_name or 0)
            )
            return ExecutionResult(
                True,
                f"Loaded {len(dataframe)} row(s) from Excel.",
                plan,
                dataframe=dataframe,
                file_path=str(Path(source).resolve()),
            )

        if plan.query:
            query_parameters = plan.parameters if isinstance(plan.parameters, dict) else None
            dataframe = self.mysql_service.run_query(plan.query, query_parameters)
            message = f"Executed query and returned {len(dataframe)} row(s)."
        else:
            table_name = plan.table_name or "data"
            dataframe = self.mysql_service.read_table(table_name, limit=plan.limit)
            message = f"Loaded {len(dataframe)} row(s) from MySQL table '{table_name}'."
        return ExecutionResult(True, message, plan, dataframe=dataframe, metadata={"sql": plan.query})

    def _mutate_rows(self, plan: ActionPlan) -> ExecutionResult:
        if not plan.query:
            raise ValueError("No SQL statement was generated for this command.")

        rows_affected = self.mysql_service.execute_statement(plan.query, plan.parameters)
        preview = pd.DataFrame(plan.entities.get("rows", [])) if plan.entities.get("rows") else None

        if plan.action == "insert":
            inserted_count = len(plan.parameters) if isinstance(plan.parameters, list) else max(rows_affected, 1)
            message = f"Inserted {inserted_count} row(s) into '{plan.table_name}'."
        elif plan.action == "update":
            message = f"Updated {rows_affected} row(s) in '{plan.table_name}'."
        else:
            message = f"Deleted {rows_affected} row(s) from '{plan.table_name}'."

        return ExecutionResult(
            True,
            message,
            plan,
            dataframe=preview,
            metadata={"rows_affected": rows_affected, "sql": plan.query, "parameters": plan.parameters},
        )

    def _visualize(self, plan: ActionPlan, current_dataframe: pd.DataFrame | None) -> ExecutionResult:
        dataframe: pd.DataFrame
        if current_dataframe is not None and (plan.use_last_result or not plan.table_name):
            dataframe = current_dataframe
        elif plan.target == "excel":
            source = plan.source_path or plan.workbook_path or str(self.config.default_workbook)
            dataframe = self.cleaner.clean_dataframe(
                self.excel_service.read_sheet(source, sheet_name=plan.sheet_name or 0)
            )
        else:
            if not plan.table_name:
                raise ValueError("No table was provided for visualization. Query some data first or name a table.")
            dataframe = self.mysql_service.read_table(plan.table_name, limit=plan.limit)

        figure, path, axes = self.visualization_service.create_chart(
            dataframe=dataframe,
            chart_type=plan.chart_type or "bar",
            output_dir=self.config.output_dir,
            x_column=plan.x_column,
            y_column=plan.y_column,
            title=plan.title,
        )
        return ExecutionResult(
            True,
            f"Generated a {plan.chart_type or 'bar'} chart.",
            plan,
            dataframe=dataframe,
            figure=figure,
            file_path=str(path),
            metadata=axes,
        )

    def _excel_to_mysql(self, plan: ActionPlan) -> ExecutionResult:
        if not plan.source_path:
            raise ValueError("Please upload or provide an Excel file path before importing to MySQL.")
        source = Path(plan.source_path).resolve()
        dataframe, mapping = self.sync_service.excel_to_mysql(
            source_path=source,
            table_name=plan.table_name or source.stem,
            sheet_name=plan.sheet_name or 0,
        )
        return ExecutionResult(
            True,
            f"Imported {len(dataframe)} row(s) from Excel into MySQL table '{plan.table_name or source.stem}'.",
            plan,
            dataframe=dataframe,
            file_path=str(source),
            metadata={"mapping": mapping},
        )

    def _mysql_to_excel(self, plan: ActionPlan) -> ExecutionResult:
        table_name = plan.table_name or "export_data"
        destination = Path(plan.destination_path).resolve() if plan.destination_path else self.config.output_dir / f"{table_name}.xlsx"
        dataframe, path = self.sync_service.mysql_to_excel(table_name=table_name, destination_path=destination)
        return ExecutionResult(
            True,
            f"Exported MySQL table '{table_name}' to Excel.",
            plan,
            dataframe=dataframe,
            file_path=str(path),
        )

    def _clean_data(self, plan: ActionPlan) -> ExecutionResult:
        if plan.target == "mysql" and plan.table_name:
            dataframe = self.mysql_service.read_table(plan.table_name, limit=100000)
            cleaned = self.cleaner.clean_dataframe(dataframe)
            cleaned_table = f"{plan.table_name}_cleaned"
            self.mysql_service.write_dataframe(cleaned, cleaned_table, if_exists="replace")
            return ExecutionResult(
                True,
                f"Cleaned data was written to MySQL table '{cleaned_table}'.",
                plan,
                dataframe=cleaned,
                metadata={"table_name": cleaned_table},
            )

        source = plan.source_path or plan.workbook_path or str(self.config.default_workbook)
        dataframe = self.excel_service.read_sheet(source, sheet_name=plan.sheet_name or 0)
        cleaned = self.cleaner.clean_dataframe(dataframe)
        destination = self.config.output_dir / f"cleaned_{Path(source).stem}.xlsx"
        path = self.excel_service.write_dataframe(destination, cleaned, sheet_name=plan.sheet_name or "cleaned_data")
        return ExecutionResult(
            True,
            f"Cleaned Excel data and saved it to '{path.name}'.",
            plan,
            dataframe=cleaned,
            file_path=str(path),
        )

    def _describe_schema(self, plan: ActionPlan) -> ExecutionResult:
        if plan.target == "excel":
            source = plan.source_path or plan.workbook_path or str(self.config.default_workbook)
            rows = self.excel_service.describe_sheet(source, sheet_name=plan.sheet_name or 0)
            dataframe = pd.DataFrame(rows)
            return ExecutionResult(True, "Read Excel schema successfully.", plan, dataframe=dataframe, file_path=str(source))

        rows = self.mysql_service.describe_table(plan.table_name or "data")
        dataframe = pd.DataFrame(rows)
        return ExecutionResult(True, "Read MySQL schema successfully.", plan, dataframe=dataframe)
