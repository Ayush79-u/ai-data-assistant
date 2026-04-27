"""
api.py — FastAPI backend.

Endpoints:
  GET  /health        — liveness probe
  GET  /schema        — live DB schema summary
  POST /parse         — NL → ActionPlan (no execution)
  POST /execute       — execute a pre-parsed ActionPlan
  POST /run           — parse + execute in one step
  POST /blueprint     — generate a JSON table blueprint
  POST /excel/import  — upload Excel, import to MySQL
  GET  /excel/export  — export MySQL table as Excel download
"""
from __future__ import annotations

import io
from contextlib import asynccontextmanager

from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from nl_data_assistant.api_models import (
    BlueprintRequest,
    ExecuteRequest,
    ParseResponse,
    RunRequest,
    RunResponse,
)
from nl_data_assistant.services.engine import DataAssistantEngine

# ── Lifespan ──────────────────────────────────────────────────────────────────

_engine: DataAssistantEngine | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _engine
    _engine = DataAssistantEngine()
    if not _engine.mysql.ping():
        raise RuntimeError("Cannot reach MySQL on startup — check .env credentials.")
    yield
    _engine = None


app = FastAPI(
    title="AI Data Assistant API",
    version="0.2.0",
    description="Natural-language MySQL + Excel assistant running fully locally.",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # tighten in production
    allow_methods=["*"],
    allow_headers=["*"],
)


def _get_engine() -> DataAssistantEngine:
    if _engine is None:
        raise HTTPException(503, "Engine not initialised.")
    return _engine


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health", tags=["meta"])
def health():
    eng = _get_engine()
    return {
        "status": "ok",
        "mysql": eng.mysql.ping(),
        "tables": eng.mysql.get_table_names(),
    }


# ── Schema ────────────────────────────────────────────────────────────────────

@app.get("/schema", tags=["meta"])
def schema():
    eng = _get_engine()
    return {"schema": eng.mysql.get_schema_summary()}


# ── NL endpoints ──────────────────────────────────────────────────────────────

@app.post("/parse", response_model=ParseResponse, tags=["nl"])
def parse(body: RunRequest):
    """Convert natural language to an ActionPlan without executing."""
    eng = _get_engine()
    plan = eng.parse(body.command)
    return ParseResponse(
        intent=plan.intent.value,
        table_name=plan.table_name,
        columns=plan.columns,
        sql=plan.sql,
        is_destructive=plan.is_destructive,
        raw_command=plan.raw_command,
    )


@app.post("/execute", response_model=RunResponse, tags=["nl"])
def execute(body: ExecuteRequest):
    """Execute a pre-parsed plan (as JSON). Use after /parse + user confirmation."""
    from nl_data_assistant.models import ActionPlan, Intent
    eng = _get_engine()
    plan = ActionPlan(
        intent=Intent(body.intent),
        table_name=body.table_name,
        columns=body.columns,
        values=body.values,
        conditions=body.conditions,
        sql=body.sql,
        raw_command=body.raw_command,
    )
    result = eng.execute(plan)
    return _to_run_response(result)


@app.post("/run", response_model=RunResponse, tags=["nl"])
def run(body: RunRequest):
    """Parse + execute in one step. Returns a confirmation prompt for destructive ops."""
    eng = _get_engine()
    result = eng.run(body.command, skip_confirmation=body.skip_confirmation)
    return _to_run_response(result)


# ── Blueprint ─────────────────────────────────────────────────────────────────

@app.post("/blueprint", tags=["nl"])
def blueprint(body: BlueprintRequest):
    """Return a JSON schema + sample data for a create-table command."""
    from nl_data_assistant.nlp.table_blueprint import TableBlueprint
    bp = TableBlueprint()
    return bp.generate(body.command)


# ── Excel ─────────────────────────────────────────────────────────────────────

@app.post("/excel/import", tags=["excel"])
async def excel_import(
    file: UploadFile = File(...),
    table_name: str = Query(..., description="Target MySQL table name"),
    sheet: str = Query("0", description="Sheet name or 0-based index"),
):
    eng = _get_engine()
    contents = await file.read()
    import tempfile, pathlib
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        tmp.write(contents)
        tmp_path = pathlib.Path(tmp.name)

    sheet_arg: str | int = int(sheet) if sheet.isdigit() else sheet
    n = eng.sync.excel_to_mysql(tmp_path, table_name, sheet_arg)
    tmp_path.unlink(missing_ok=True)
    return {"imported_rows": n, "table": table_name}


@app.get("/excel/export", tags=["excel"])
def excel_export(
    table: str = Query(..., description="MySQL table to export"),
    conditions: str = Query("", description="Optional WHERE clause"),
):
    eng = _get_engine()
    import tempfile, pathlib
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        tmp_path = pathlib.Path(tmp.name)

    eng.sync.mysql_to_excel(table, tmp_path, conditions=conditions)
    data = tmp_path.read_bytes()
    tmp_path.unlink(missing_ok=True)

    return StreamingResponse(
        io.BytesIO(data),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{table}.xlsx"'},
    )


# ── Helpers ───────────────────────────────────────────────────────────────────

def _to_run_response(result) -> RunResponse:
    data = None
    if hasattr(result.data, "to_dict"):       # DataFrame
        data = result.data.to_dict(orient="records")
    return RunResponse(
        success=result.success,
        message=result.message,
        sql_executed=result.sql_executed,
        rows_affected=result.rows_affected,
        data=data,
        error=result.error,
    )
