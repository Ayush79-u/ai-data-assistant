from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI

from nl_data_assistant.api_models import (
    BlueprintRequest,
    BlueprintResponse,
    CommandRequest,
    ExecuteResponse,
    HealthResponse,
    ParseResponse,
)
from nl_data_assistant.config import AppConfig
from nl_data_assistant.examples import EXAMPLE_COMMANDS
from nl_data_assistant.nlp.table_blueprint import command_to_blueprint
from nl_data_assistant.services.engine import DataAssistantEngine


def create_api_app(config: AppConfig | None = None) -> FastAPI:
    resolved_config = config or AppConfig.from_env(Path.cwd())
    engine = DataAssistantEngine(resolved_config)

    app = FastAPI(
        title="AI Data Assistant API",
        version="0.2.0",
        description="Local natural-language backend for Excel, MySQL, and visual analytics.",
    )

    @app.get("/health", response_model=HealthResponse)
    def health() -> HealthResponse:
        return HealthResponse(
            status="ok",
            mysql_configured=engine.mysql_service.is_configured,
            llm_enabled=resolved_config.llm_enabled,
        )

    @app.get("/examples")
    def examples() -> dict[str, list[str]]:
        return {"examples": EXAMPLE_COMMANDS}

    @app.get("/schema")
    def schema() -> dict[str, Any]:
        return {
            "configured": engine.mysql_service.is_configured,
            "tables": engine.get_mysql_schema_catalog(),
        }

    @app.post("/parse", response_model=ParseResponse)
    def parse_command(payload: CommandRequest) -> ParseResponse:
        effective_target = None if payload.target == "auto" else payload.target
        plan = engine.parse_command(payload.command, default_target=effective_target)
        return ParseResponse(
            success=plan.action != "unknown",
            plan=plan.as_dict(),
            sql=plan.query,
            notes=list(plan.notes),
        )

    @app.post("/blueprint", response_model=BlueprintResponse)
    def blueprint(payload: BlueprintRequest) -> BlueprintResponse:
        data = command_to_blueprint(payload.command, sample_rows=payload.sample_rows)
        return BlueprintResponse(**data)

    @app.post("/execute", response_model=ExecuteResponse)
    def execute_command(payload: CommandRequest) -> ExecuteResponse:
        result = engine.execute(
            command=payload.command,
            default_target=payload.target,
            uploaded_file_path=payload.uploaded_file_path,
        )
        return ExecuteResponse(
            success=result.success,
            message=result.message,
            plan=result.plan.as_dict(),
            file_path=result.file_path,
            metadata=_sanitize_json(result.metadata),
            data_preview=_dataframe_preview(result.dataframe),
            chart=result.figure.to_dict() if result.figure is not None else None,
        )

    return app


def _dataframe_preview(dataframe: Any) -> list[dict[str, Any]]:
    if dataframe is None:
        return []
    try:
        preview = dataframe.head(50)
        return _sanitize_json(preview.to_dict(orient="records"))
    except Exception:
        return []


def _sanitize_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _sanitize_json(inner) for key, inner in value.items()}
    if isinstance(value, list):
        return [_sanitize_json(item) for item in value]
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


app = create_api_app()
