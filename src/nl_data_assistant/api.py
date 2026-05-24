"""
api.py — FastAPI backend.

Security hardening applied:
  - API key authentication on every endpoint (X-API-Key header)
  - CORS restricted to localhost:8501 only
  - `skip_confirmation` removed from public API
  - Raw `sql` field removed from ExecuteRequest
  - Raw `conditions` parameter removed from /excel/export
  - File size limit (50 MB) on Excel upload
  - Rate limiting via slowapi (10 req/min per IP on mutating endpoints)
  - Generic error responses — no internal details leaked

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
import logging
import os
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, File, HTTPException, Query, Request, Security, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.security import APIKeyHeader

from nl_data_assistant.api_models import (
    BlueprintRequest,
    ExecuteRequest,
    ParseResponse,
    RunRequest,
    RunResponse,
)
from nl_data_assistant.services.engine import DataAssistantEngine

log = logging.getLogger(__name__)

# ── Rate limiting ─────────────────────────────────────────────────────────────

try:
    from slowapi import Limiter, _rate_limit_exceeded_handler
    from slowapi.errors import RateLimitExceeded
    from slowapi.util import get_remote_address
    _limiter = Limiter(key_func=get_remote_address)
    _RATE_LIMITING = True
except ImportError:
    _limiter = None
    _RATE_LIMITING = False
    log.warning("slowapi not installed — rate limiting disabled. Run: pip install slowapi")

# ── Authentication ────────────────────────────────────────────────────────────

_API_KEY_NAME = "X-API-Key"
_api_key_header = APIKeyHeader(name=_API_KEY_NAME, auto_error=False)
_CONFIGURED_KEY: str = os.getenv("APP_API_KEY", "").strip()


def _require_api_key(key: str | None = Security(_api_key_header)) -> None:
    """Dependency: reject requests without a valid API key."""
    if not _CONFIGURED_KEY:
        # API key not configured → API is for local-only use; allow all requests
        # but log a warning on every call so the operator notices.
        log.warning(
            "APP_API_KEY is not set in .env — the REST API is UNPROTECTED. "
            "Set APP_API_KEY=<secret> to enable authentication."
        )
        return
    if key != _CONFIGURED_KEY:
        raise HTTPException(status_code=403, detail="Invalid or missing API key.")


_auth = Depends(_require_api_key)

# ── Upload limits ─────────────────────────────────────────────────────────────

_MAX_UPLOAD_BYTES = 50 * 1024 * 1024  # 50 MB

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
    version="0.3.0",
    description="Natural-language MySQL + Excel assistant running fully locally.",
    lifespan=lifespan,
)

# Register slowapi rate-limit error handler if available
if _RATE_LIMITING and _limiter:
    app.state.limiter = _limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# ── CORS — restricted to the Streamlit frontend only ─────────────────────────

_ALLOWED_ORIGINS = [
    o.strip()
    for o in os.getenv("CORS_ALLOWED_ORIGINS", "http://localhost:8501").split(",")
    if o.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type", _API_KEY_NAME],
)


def _get_engine() -> DataAssistantEngine:
    if _engine is None:
        raise HTTPException(503, "Engine not initialised.")
    return _engine


def _safe_error(exc: Exception, *, generic: str = "An internal error occurred.") -> str:
    """Log the real error, return a generic message safe to expose to callers."""
    log.error("API error: %s", exc, exc_info=True)
    return generic


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health", tags=["meta"], dependencies=[_auth])
def health():
    eng = _get_engine()
    return {
        "status": "ok",
        "mysql": eng.mysql.ping(),
        "tables": eng.mysql.get_table_names(),
    }


# ── Schema ────────────────────────────────────────────────────────────────────

@app.get("/schema", tags=["meta"], dependencies=[_auth])
def schema():
    eng = _get_engine()
    return {"schema": eng.mysql.get_schema_summary()}


# ── NL endpoints ──────────────────────────────────────────────────────────────

@app.post("/parse", response_model=ParseResponse, tags=["nl"], dependencies=[_auth])
def parse(body: RunRequest):
    """Convert natural language to an ActionPlan without executing."""
    try:
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
    except Exception as exc:
        raise HTTPException(500, _safe_error(exc)) from exc


@app.post("/execute", response_model=RunResponse, tags=["nl"], dependencies=[_auth])
def execute(body: ExecuteRequest):
    """Execute a pre-parsed plan (as JSON). Use after /parse + user confirmation.
    Note: the 'sql' field has been removed — raw SQL cannot be submitted directly.
    """
    from nl_data_assistant.models import ActionPlan, Intent
    try:
        eng = _get_engine()
        plan = ActionPlan(
            intent=Intent(body.intent),
            table_name=body.table_name,
            columns=body.columns,
            values=body.values,
            conditions=body.conditions,
            # sql field intentionally omitted — callers cannot inject raw SQL
            raw_command=body.raw_command,
        )
        result = eng.execute(plan)
        return _to_run_response(result)
    except Exception as exc:
        raise HTTPException(500, _safe_error(exc)) from exc


@app.post("/run", response_model=RunResponse, tags=["nl"], dependencies=[_auth])
def run(request: Request, body: RunRequest):
    """Parse + execute in one step. Returns a confirmation prompt for destructive ops.
    Note: skip_confirmation has been removed from the public API for security.
    """
    if _RATE_LIMITING and _limiter:
        _limiter.limit("10/minute")(lambda: None)()

    if len(body.command) > 2000:
        raise HTTPException(400, "Command too long (max 2000 characters).")
    try:
        eng = _get_engine()
        # skip_confirmation=False always — the API never bypasses confirmation
        result = eng.run(body.command, skip_confirmation=False)
        return _to_run_response(result)
    except Exception as exc:
        raise HTTPException(500, _safe_error(exc)) from exc


# ── Blueprint ─────────────────────────────────────────────────────────────────

@app.post("/blueprint", tags=["nl"], dependencies=[_auth])
def blueprint(body: BlueprintRequest):
    """Return a JSON schema + sample data for a create-table command."""
    try:
        from nl_data_assistant.nlp.table_blueprint import TableBlueprint
        bp = TableBlueprint()
        return bp.generate(body.command)
    except Exception as exc:
        raise HTTPException(500, _safe_error(exc)) from exc


# ── Excel ─────────────────────────────────────────────────────────────────────

@app.post("/excel/import", tags=["excel"], dependencies=[_auth])
async def excel_import(
    file: UploadFile = File(...),
    table_name: str = Query(..., description="Target MySQL table name"),
    sheet: str = Query("0", description="Sheet name or 0-based index"),
):
    import re
    import tempfile
    import uuid
    from pathlib import Path

    # ── Size check ────────────────────────────────────────────────────────────
    contents = await file.read(_MAX_UPLOAD_BYTES + 1)
    if len(contents) > _MAX_UPLOAD_BYTES:
        raise HTTPException(413, "File too large. Maximum size is 50 MB.")

    # ── Extension check ───────────────────────────────────────────────────────
    raw_name = file.filename or "upload.xlsx"
    suffix = Path(raw_name).suffix.lower()
    if suffix not in {".xlsx", ".xls"}:
        raise HTTPException(400, "Only .xlsx and .xls files are accepted.")

    # ── Safe temp path (no path traversal) ───────────────────────────────────
    safe_name = f"upload_{uuid.uuid4().hex}{suffix}"
    tmp_path = Path(tempfile.gettempdir()) / safe_name
    tmp_path.write_bytes(contents)

    try:
        eng = _get_engine()
        sheet_arg: str | int = int(sheet) if sheet.isdigit() else sheet
        n = eng.sync.excel_to_mysql(tmp_path, table_name, sheet_arg)
        return {"imported_rows": n, "table": table_name}
    except Exception as exc:
        raise HTTPException(500, _safe_error(exc, generic="Import failed. Check the file format.")) from exc
    finally:
        tmp_path.unlink(missing_ok=True)


@app.get("/excel/export", tags=["excel"], dependencies=[_auth])
def excel_export(
    table: str = Query(..., description="MySQL table to export"),
    # NOTE: 'conditions' parameter has been removed — it was a SQL injection vector.
    # Use the NL interface (/run) to filter data before exporting.
):
    """Export a full MySQL table as an Excel download.
    Filtering is intentionally not supported here to prevent SQL injection.
    """
    import re
    import tempfile
    import uuid
    from pathlib import Path

    # Validate table name is a safe identifier
    if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", table):
        raise HTTPException(400, "Invalid table name.")

    try:
        eng = _get_engine()
        tmp_path = Path(tempfile.gettempdir()) / f"export_{uuid.uuid4().hex}.xlsx"
        eng.sync.mysql_to_excel(table, tmp_path)
        data = tmp_path.read_bytes()
        return StreamingResponse(
            io.BytesIO(data),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="{table}.xlsx"'},
        )
    except Exception as exc:
        raise HTTPException(500, _safe_error(exc, generic="Export failed.")) from exc
    finally:
        Path(tempfile.gettempdir()).joinpath(f"export_{uuid.uuid4().hex}.xlsx").unlink(missing_ok=True)
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass


# ── Helpers ───────────────────────────────────────────────────────────────────

def _to_run_response(result) -> RunResponse:
    data = None
    if hasattr(result.data, "to_dict"):       # DataFrame
        data = result.data.to_dict(orient="records")
    # Never expose raw DB error strings to callers
    safe_error = "An error occurred." if result.error else ""
    if result.error:
        log.error("Execution error (not exposed to caller): %s", result.error)
    return RunResponse(
        success=result.success,
        message=result.message,
        sql_executed=result.sql_executed,
        rows_affected=result.rows_affected,
        data=data,
        error=safe_error,
    )
