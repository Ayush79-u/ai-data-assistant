"""
engine.py — DataAssistantEngine: central dispatcher.

Flow:
  1. NL command → interpreter.interpret() → ActionPlan
  2. If destructive, return plan flagged for UI confirmation before executing
  3. ActionPlan → correct service → ExecutionResult
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

from nl_data_assistant.config import settings
from nl_data_assistant.models import ActionPlan, ExecutionResult, Intent
from nl_data_assistant.nlp.interpreter import interpret
from nl_data_assistant.nlp.table_blueprint import TableBlueprint
from nl_data_assistant.services.excel_service import ExcelService
from nl_data_assistant.services.mysql_session_service import MySQLSessionService
from nl_data_assistant.services.sync_service import SyncService
from nl_data_assistant.services.visualization_service import VisualizationService

log = logging.getLogger(__name__)


class DataAssistantEngine:
    def __init__(self):
        sync_engine = create_engine(
            settings.mysql_url_for(settings.default_database) if settings.default_database else settings.mysql_server_url,
            pool_pre_ping=True,
            pool_recycle=3600,
        )
        self.mysql = MySQLSessionService(default_database=settings.default_database)
        self.excel = ExcelService(settings.default_workbook)
        self.sync = SyncService(sync_engine, self.excel)
        self.viz = VisualizationService()

        # Multi-turn conversation history shared across all calls
        self._history: list[dict] = []

    # ── Public API ────────────────────────────────────────────────────────────

    def parse(self, command: str, *, default_table: str = "") -> ActionPlan:
        """Parse without executing. Useful for previewing / confirmation step."""
        schema = self.mysql.get_schema_summary()
        plan = interpret(command, schema_summary=schema, history=self._history)
        plan = plan.__class__(**{**plan.__dict__, "raw_command": command})
        plan = self._apply_default_table(plan, default_table)
        # Append to history so the next call has context
        self._history.append({"role": "user", "content": command})
        return plan

    def execute(self, plan: ActionPlan) -> ExecutionResult:
        """Execute a pre-parsed (and optionally confirmed) ActionPlan."""
        try:
            result = self._dispatch(plan)
            # Keep assistant turn in history so follow-up commands make sense
            self._history.append({
                "role": "assistant",
                "content": result.message or result.sql_executed,
            })
            return result
        except Exception as exc:
            log.exception("Engine dispatch error")
            return ExecutionResult(success=False, error=str(exc))

    def run(
        self,
        command: str,
        *,
        skip_confirmation: bool = False,
        default_table: str = "",
    ) -> ExecutionResult:
        """
        Parse and execute in one step.
        If the plan is destructive and skip_confirmation=False, returns an
        ExecutionResult with success=False and a confirmation prompt in .message.
        The caller (Streamlit / CLI) should display this and re-call with
        skip_confirmation=True after the user confirms.
        """
        plan = self.parse(command, default_table=default_table)

        if plan.is_destructive and not skip_confirmation:
            return ExecutionResult(
                success=False,
                message=(
                    f"⚠️ This will execute: {plan.intent.value.upper()} "
                    f"on '{plan.table_name}'. Confirm to proceed."
                ),
                data=plan,        # caller can inspect the plan
            )

        return self.execute(plan)

    def clear_history(self) -> None:
        self._history.clear()

    def execute_raw(self, sql: str) -> ExecutionResult:
        return self.mysql.execute_sql(sql)

    # ── Dispatcher ────────────────────────────────────────────────────────────

    def _dispatch(self, plan: ActionPlan) -> ExecutionResult:
        intent = plan.intent

        # ── MySQL operations ──────────────────────────────────────────────────
        if intent == Intent.CREATE_TABLE:
            return self._create_table_from_command(plan)

        if intent in (
            Intent.INSERT,
            Intent.SELECT,
            Intent.UPDATE,
            Intent.DELETE,
            Intent.DROP_TABLE,
            Intent.DESCRIBE,
        ):
            return self.mysql.execute_plan(plan)

        # ── Excel operations ──────────────────────────────────────────────────
        if intent == Intent.CREATE_EXCEL:
            path = Path(plan.file_path) if plan.file_path else settings.default_workbook
            written = self.excel.create_blank(plan.columns, path)
            return ExecutionResult(success=True, message=f"Created {written}")

        if intent == Intent.SHOW_EXCEL:
            path = Path(plan.file_path) if plan.file_path else settings.default_workbook
            df = self.excel.read_sheet(path)
            return ExecutionResult(
                success=True,
                data=df,
                message=f"Read {len(df)} rows from {path.name}",
            )

        # ── Sync operations ───────────────────────────────────────────────────
        if intent == Intent.IMPORT_EXCEL:
            n = self.sync.excel_to_mysql(plan.file_path, plan.table_name)
            return ExecutionResult(success=True, message=f"Imported {n} rows → `{plan.table_name}`")

        if intent == Intent.EXPORT_EXCEL:
            out = self.sync.mysql_to_excel(
                plan.table_name,
                plan.file_path or settings.default_workbook,
            )
            return ExecutionResult(success=True, message=f"Exported to {out}")

        # ── Visualisation ─────────────────────────────────────────────────────
        if intent == Intent.VISUALIZE:
            result = self.mysql.execute_plan(plan.__class__(
                **{**plan.__dict__, "intent": Intent.SELECT}
            ))
            if not result.success or result.data is None:
                return result
            fig = self.viz.plot(result.data, plan.chart_type or "bar", plan.table_name)
            return ExecutionResult(success=True, data=fig, message="Chart ready.")

        return ExecutionResult(
            success=False,
            error=f"I don't know how to handle intent '{intent}'. Try rephrasing.",
        )

    def _apply_default_table(self, plan: ActionPlan, default_table: str) -> ActionPlan:
        if not default_table:
            return plan

        available_tables = {name.lower() for name in self.mysql.get_table_names()}
        safe_default = default_table.strip().lower()
        if safe_default not in available_tables:
            return plan

        intents_using_current_table = {
            Intent.INSERT,
            Intent.SELECT,
            Intent.UPDATE,
            Intent.DELETE,
            Intent.VISUALIZE,
            Intent.DESCRIBE,
            Intent.EXPORT_EXCEL,
        }
        current_table = (plan.table_name or "").strip().lower()
        if plan.intent in intents_using_current_table and current_table not in available_tables:
            plan.table_name = safe_default
        return plan

    def _create_table_from_command(self, plan: ActionPlan) -> ExecutionResult:
        blueprint = TableBlueprint().generate(plan.raw_command)
        if plan.table_name:
            blueprint["table_name"] = plan.table_name
        if plan.columns:
            blueprint["columns"] = self._merge_blueprint_columns(blueprint, plan.columns)

        result = self.mysql.create_table_from_blueprint(blueprint)
        if not result.success:
            return result

        sample_df = pd.DataFrame(blueprint.get("sample_data") or [])
        return ExecutionResult(
            success=True,
            data=sample_df,
            sql_executed=result.sql_executed or blueprint.get("create_sql", ""),
            message=(
                f"Created table `{blueprint['table_name']}`. "
                "You can edit the starter rows and save them to MySQL."
            ),
        )

    @staticmethod
    def _merge_blueprint_columns(
        blueprint: dict,
        requested_columns: list[str],
    ) -> list[dict]:
        requested = [column.strip().lower() for column in requested_columns if column.strip()]
        if not requested:
            return blueprint.get("columns", [])

        existing = {
            str(column.get("name", "")).lower(): column
            for column in blueprint.get("columns", [])
        }
        merged: list[dict] = []

        if "id" in existing:
            merged.append(existing["id"])

        generator = TableBlueprint()
        for name in requested:
            if name == "id":
                continue
            merged.append(
                existing.get(
                    name,
                    {
                        "name": name,
                        "type": generator._infer_type(name),
                        "primary_key": False,
                    },
                )
            )
        return merged
