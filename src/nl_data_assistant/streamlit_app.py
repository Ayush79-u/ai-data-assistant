"""
streamlit_app.py – Refactored AI Data Assistant
================================================
Key changes vs original
------------------------
1. DUPLICATE-INSERT GUARD
   Each user command is hashed into a short key that is stored in
   st.session_state.executed_commands (a set).  Before any DB write the
   engine checks this set; if the key is already present the write is
   skipped.  This stops Streamlit reruns from inserting the same rows twice.

2. CHAT-BASED INTERFACE
   st.chat_input() + st.chat_message() give a proper conversation UI.
   Messages are stored in st.session_state.messages and re-rendered on
   every rerun.

3. DYNAMIC MULTI-TABLE SYSTEM
   st.session_state.current_table tracks which table is "active".
   "create table …" updates it; follow-up commands like "add rows" and
   "show data" automatically reuse it.
   A sidebar dropdown lets users switch between existing MySQL tables.
   "show tables" in chat lists all tables.

4. EDITABLE TABLE UI
   st.data_editor() renders the current DataFrame inline.
   A "Save to MySQL" button commits edits.  The save is guarded by a
   per-session save_token to prevent double-saves on rerun.

5. ADAPTIVE CSS
   All colours use CSS custom properties that Streamlit sets differently in
   light vs dark mode, so the UI is readable in both themes.
"""

from __future__ import annotations

import hashlib
import time
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from nl_data_assistant.config import AppConfig
from nl_data_assistant.examples import EXAMPLE_COMMANDS
from nl_data_assistant.models import ColumnSpec, ExecutionResult
from nl_data_assistant.nlp.local_parser import extract_count
from nl_data_assistant.nlp.table_blueprint import build_sample_rows, command_to_blueprint
from nl_data_assistant.services.engine import DataAssistantEngine
from nl_data_assistant.utils.cleaning import normalize_identifier


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def run_streamlit_app() -> None:
    st.set_page_config(page_title="Data Assistant", layout="wide", page_icon="🗃️")
    _inject_styles()
    _bootstrap_state()

    base_config = AppConfig.from_env(Path.cwd())
    config, default_target = _render_sidebar(base_config)
    engine = DataAssistantEngine(config)

    uploaded_file_path = _render_header(config, engine)
    render_chat_ui(engine, default_target, uploaded_file_path)


# ─────────────────────────────────────────────────────────────────────────────
# Chat UI
# ─────────────────────────────────────────────────────────────────────────────

def render_chat_ui(
    engine: DataAssistantEngine,
    default_target: str,
    uploaded_file_path: str | None,
) -> None:
    chat_col, side_col = st.columns([2.2, 1], gap="large")

    with chat_col:
        _render_messages()

        user_input = st.chat_input("Ask me anything… e.g. 'Create a students table with name and grade'")
        if user_input:
            _handle_user_input(engine, user_input, default_target, uploaded_file_path)
            st.rerun()

        # Editable grid lives below the chat stream
        if st.session_state.blueprint is not None:
            _render_editor_panel(engine)

    with side_col:
        _render_context_panel(engine, default_target, uploaded_file_path)


# ─────────────────────────────────────────────────────────────────────────────
# Command handling  –  the THREE-LAYER GUARD against duplicate execution
#
#  Layer 1 (this function): command-hash check in session_state
#  Layer 2 (mysql_service):  INSERT IGNORE at the SQL level
#  Layer 3 (mysql_service):  CREATE TABLE IF NOT EXISTS  (DDL is idempotent)
# ─────────────────────────────────────────────────────────────────────────────

def _handle_user_input(
    engine: DataAssistantEngine,
    user_input: str,
    default_target: str,
    uploaded_file_path: str | None,
) -> None:
    cleaned = user_input.strip()
    if not cleaned:
        return

    # ── Layer 1: hash guard ──────────────────────────────────────────────────
    # We combine the command text with a short time-bucket (10-second window).
    # This means the same command typed twice within 10 s is treated as one,
    # but the same command typed a minute later is allowed again.
    time_bucket = str(int(time.time()) // 10)
    cmd_hash = hashlib.md5(f"{cleaned}{time_bucket}".encode()).hexdigest()[:12]

    if cmd_hash in st.session_state.executed_commands:
        # Streamlit triggered a rerun but the command was already processed.
        return

    st.session_state.executed_commands.add(cmd_hash)
    st.session_state.messages.append({"role": "user", "content": cleaned})

    response = _process_command(engine, cleaned, default_target, uploaded_file_path)
    st.session_state.messages.append(response)


def _process_command(
    engine: DataAssistantEngine,
    user_input: str,
    default_target: str,
    uploaded_file_path: str | None,
) -> dict[str, Any]:

    lowered = user_input.lower().strip()

    # 🟢 Always ensure file path is available
    uploaded_file_path = uploaded_file_path or st.session_state.get("uploaded_file_path")

    # ── CLEAR CHAT ─────────────────────────────────────────
    if any(t in lowered for t in ("clear chat", "reset chat")):
        _reset_chat_state(keep_config=True)
        return _msg("Chat cleared. Starting fresh.", status="info")

    # ── EXCEL IMPORT (FIXED) ───────────────────────────────
    if uploaded_file_path and any(word in lowered for word in ["excel", "file", "upload", "import"]):
        try:
            df = pd.read_excel(uploaded_file_path)
            st.session_state.current_dataframe = df
            st.session_state.editor_df = df.copy()
            st.session_state.blueprint = {
                "table_name": "excel_data",
                "columns": [{"name": col, "type": "text"} for col in df.columns],
            }
            st.session_state.current_table = "excel_data"

            return _msg(
                f"✅ Excel file loaded successfully with {len(df)} rows.",
                metadata={"dataframe": df},
                show_editor=True,
                status="success",
            )
        except Exception as e:
            return _msg(f"❌ Failed to load Excel: {e}", status="error")

    # ── EXPORT TO EXCEL (NEW FEATURE) ──────────────────────
    if any(word in lowered for word in ["export", "download", "save excel"]):
        if st.session_state.current_dataframe is None:
            return _msg("No data available to export.", status="error")

        try:
            file_name = f"{st.session_state.current_table or 'data'}.xlsx"
            st.session_state.current_dataframe.to_excel(file_name, index=False)

            return _msg(
                f"✅ Data exported to Excel: {file_name}",
                metadata={"file_path": file_name},
                status="success",
            )
        except Exception as e:
            return _msg(f"❌ Export failed: {e}", status="error")

    # ── SHOW TABLES ────────────────────────────────────────
    if lowered in ("show tables", "list tables"):
        tables = engine.mysql_service.list_tables()
        return _msg("\n".join(tables) if tables else "No tables found.", status="info")

    # ── SWITCH TABLE ───────────────────────────────────────
    if lowered.startswith("use table "):
        table_name = lowered.replace("use table", "").strip()
        st.session_state.current_table = table_name
        return _msg(f"Switched to table `{table_name}`.", status="success")

    # ── CREATE TABLE ───────────────────────────────────────
    if _is_create_table_request(user_input):
        return _handle_blueprint_creation(user_input)

    # ── ADD ROWS ───────────────────────────────────────────
    if _is_add_rows_request(lowered) and st.session_state.blueprint is not None:
        return _handle_add_rows_to_blueprint(user_input)

    # ── SHOW DATA ──────────────────────────────────────────
    if _is_show_data_request(lowered) and st.session_state.blueprint is not None:
        return _msg(
            f"Showing data for `{st.session_state.current_table}`",
            show_editor=True,
            status="success",
        )

    # ── SAVE TO MYSQL ──────────────────────────────────────
    if _is_save_request(lowered):
        return _save_to_mysql(engine)

    # ── DEFAULT ENGINE (IMPORTANT) ─────────────────────────
    result = engine.run(
        user_input,
        context=st.session_state.messages,
        default_target=default_target,
        uploaded_file_path=uploaded_file_path,
        current_dataframe=st.session_state.current_dataframe,
        current_table=st.session_state.current_table,
    )

    return _msg_from_result(result)


# ─────────────────────────────────────────────────────────────────────────────
# Blueprint helpers
# ─────────────────────────────────────────────────────────────────────────────

def _handle_blueprint_creation(user_input: str) -> dict[str, Any]:
    try:
        blueprint = command_to_blueprint(user_input)
    except Exception as exc:
        return _msg(f"Could not build a table blueprint: {exc}", status="error")

    df = pd.DataFrame(blueprint["sample_data"])
    st.session_state.blueprint = blueprint
    st.session_state.current_table = blueprint["table_name"]
    st.session_state.current_dataframe = df
    st.session_state.editor_df = df.copy()

    summary = (
        f"Blueprint ready for **`{blueprint['table_name']}`** — "
        f"{len(blueprint['columns'])} column(s), {len(df)} sample row(s).\n\n"
        "Edit the data in the grid below, then press **Save to MySQL**."
    )
    return _msg(
        summary,
        metadata={"table_name": blueprint["table_name"], "blueprint": blueprint},
        blueprint=blueprint,
        show_editor=True,
        status="success",
    )


def _handle_add_rows_to_blueprint(user_input: str) -> dict[str, Any]:
    blueprint = st.session_state.blueprint
    count = extract_count(user_input)
    columns = [ColumnSpec(name=c["name"], data_type=c["type"], nullable=True) for c in blueprint["columns"]]
    new_rows = build_sample_rows(columns, count)
    new_df = pd.concat([st.session_state.editor_df, pd.DataFrame(new_rows)], ignore_index=True)
    st.session_state.editor_df = new_df
    st.session_state.current_dataframe = new_df
    return _msg(
        f"Added {count} new row(s) to `{blueprint['table_name']}`. Edit them below, then save.",
        metadata={"table_name": blueprint["table_name"], "rows_added": count},
        show_editor=True,
        status="success",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Save to MySQL  –  guarded by a save_token to prevent double-saves on rerun
# ─────────────────────────────────────────────────────────────────────────────

def _save_to_mysql(engine: DataAssistantEngine) -> dict[str, Any]:
    if st.session_state.blueprint is None or st.session_state.editor_df is None:
        return _msg("No table to save. Create a table first.", status="error")
    if not engine.mysql_service.is_configured:
        return _msg("MySQL is not configured. Fill in the connection details in the sidebar.", status="error")

    # ── Save-token guard: prevents the same save from running twice on rerun ─
    save_token = hashlib.md5(
        (st.session_state.blueprint["table_name"] + str(time.time() // 10)).encode()
    ).hexdigest()[:12]
    if save_token in st.session_state.executed_commands:
        return _msg("Already saved — no duplicate insert.", status="info")
    st.session_state.executed_commands.add(save_token)

    df = st.session_state.editor_df.copy()
    blueprint = st.session_state.blueprint
    columns = [
        ColumnSpec(name=c["name"], data_type=c["type"], nullable=True)
        for c in blueprint["columns"]
    ]

    try:
        # CREATE TABLE IF NOT EXISTS → idempotent, safe on rerun
        engine.mysql_service.create_table(blueprint["table_name"], columns)
        # INSERT IGNORE → duplicates silently skipped at DB level
        inserted = engine.mysql_service.write_dataframe(
            df, blueprint["table_name"], if_exists="append", ignore_duplicates=True
        )
        st.session_state.current_dataframe = df
        return _msg(
            f"✅ Saved **{inserted}** row(s) to MySQL table `{blueprint['table_name']}`.",
            metadata={"table_name": blueprint["table_name"], "rows": inserted},
            status="success",
        )
    except Exception as exc:
        return _msg(f"Could not save to MySQL: {exc}", status="error")


# ─────────────────────────────────────────────────────────────────────────────
# Message rendering
# ─────────────────────────────────────────────────────────────────────────────

def _render_messages() -> None:
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            if message["role"] == "assistant":
                _render_status_badge(message.get("status", "info"))
            st.markdown(message["content"])

            if message["role"] == "assistant":
                meta = message.get("metadata", {})
                if meta.get("dataframe") is not None:
                    st.dataframe(meta["dataframe"], use_container_width=True)
                if meta.get("chart") is not None:
                    st.plotly_chart(meta["chart"], use_container_width=True)
                if meta.get("sql"):
                    st.code(meta["sql"], language="sql")
                if meta.get("file_path"):
                    st.caption(f"📁 {meta['file_path']}")
                if message.get("show_editor") and st.session_state.blueprint is not None:
                    st.caption("↓ Editable grid below")


# ─────────────────────────────────────────────────────────────────────────────
# Editable grid panel
# ─────────────────────────────────────────────────────────────────────────────

def _render_editor_panel(engine: DataAssistantEngine) -> None:
    st.markdown("---")
    tname = st.session_state.blueprint.get("table_name", "table")
    st.markdown(f"### 📝 Editing `{tname}`")
    st.caption("Modify rows directly, then click **Save to MySQL** below.")

    edited_df = st.data_editor(
        st.session_state.editor_df,
        use_container_width=True,
        num_rows="dynamic",
        key="table_editor",
    )
    # Keep session state in sync with whatever the user edited
    st.session_state.editor_df = edited_df
    st.session_state.current_dataframe = edited_df

    col_save, col_discard = st.columns([1, 1], gap="small")
    with col_save:
        if st.button("💾 Save to MySQL", use_container_width=True, type="primary"):
            response = _save_to_mysql(engine)
            st.session_state.messages.append(response)
            st.rerun()
    with col_discard:
        if st.button("🗑️ Discard blueprint", use_container_width=True):
            st.session_state.blueprint = None
            st.session_state.editor_df = None
            st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────────────────────────────────────────

def _render_sidebar(base_config: AppConfig) -> tuple[AppConfig, str]:
    with st.sidebar:
        st.markdown("## ⚙️ Connection")
        mysql_host = st.text_input("MySQL host", value=base_config.mysql_host)
        mysql_port = st.number_input("MySQL port", value=base_config.mysql_port, step=1)
        mysql_user = st.text_input("MySQL user", value=base_config.mysql_user)
        mysql_password = st.text_input("MySQL password", value=base_config.mysql_password, type="password")
        mysql_database = st.text_input("MySQL database", value=base_config.mysql_database)
        default_target = st.selectbox("Default target", ["mysql", "excel", "auto"], index=0)

        st.markdown("---")

        # ── Table switcher ─────────────────────────────────────────────────
        st.markdown("## 🗂️ Switch Table")
        _render_table_switcher(base_config, mysql_host, mysql_port, mysql_user, mysql_password, mysql_database)

        st.markdown("---")
        st.markdown("## 💡 Quick prompts")
        for example in EXAMPLE_COMMANDS[:5]:
            if st.button(example, key=f"ex_{example}", use_container_width=True):
                # Quick-prompt clicks are also hash-guarded via _handle_user_input
                engine_temp = DataAssistantEngine(_build_config(
                    base_config, mysql_host, int(mysql_port),
                    mysql_user, mysql_password, mysql_database
                ))
                _handle_user_input(
                    engine_temp, example, default_target,
                    st.session_state.get("uploaded_file_path")
                )
                st.rerun()

    config = _build_config(base_config, mysql_host, int(mysql_port), mysql_user, mysql_password, mysql_database)
    st.session_state.default_target = default_target
    st.session_state.config_values = {
        "mysql_host": mysql_host, "mysql_port": int(mysql_port),
        "mysql_user": mysql_user, "mysql_password": mysql_password,
        "mysql_database": mysql_database,
    }
    return config, default_target


def _render_table_switcher(
    base_config: AppConfig,
    host: str, port: int, user: str, pw: str, db: str,
) -> None:
    """Dropdown that lets users jump to any existing MySQL table."""
    cfg = _build_config(base_config, host, int(port), user, pw, db)
    from nl_data_assistant.services.mysql_service import MySQLService
    svc = MySQLService(cfg.mysql_url)
    tables = svc.list_tables()

    if not tables:
        st.caption("No tables yet — create one in chat.")
        return

    options = ["— select —"] + tables
    current_idx = 0
    if st.session_state.current_table in tables:
        current_idx = tables.index(st.session_state.current_table) + 1

    chosen = st.selectbox("Active table", options, index=current_idx, key="table_switcher_dd")
    if chosen != "— select —" and chosen != st.session_state.current_table:
        st.session_state.current_table = chosen
        st.session_state.blueprint = None  # discard blueprint on switch
        st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# Header
# ─────────────────────────────────────────────────────────────────────────────

def _render_header(config: AppConfig, engine: DataAssistantEngine) -> str | None:
    st.title("🗃️ AI Data Assistant")
    st.caption("Create tables, edit rows, query data, and visualise — all in natural language.")

    left, right = st.columns([1.4, 1], gap="large")
    uploaded_file_path = None

    with left:
        uploaded_file = st.file_uploader("Upload an Excel file (optional)", type=["xlsx", "xls"])
        if uploaded_file:
            saved = _save_uploaded_file(uploaded_file, config.upload_dir)
            uploaded_file_path = str(saved)
            st.success(f"Workbook ready: {saved.name}")

    with right:
        mysql_ok = engine.mysql_service.is_configured
        st.metric("MySQL", "✅ Connected" if mysql_ok else "⚠️ Not configured")
        st.metric("Tables", len(engine.get_mysql_schema_catalog()))
        if st.session_state.current_table:
            st.metric("Active table", st.session_state.current_table)

    st.session_state.uploaded_file_path = uploaded_file_path
    return uploaded_file_path


# ─────────────────────────────────────────────────────────────────────────────
# Context / memory panel
# ─────────────────────────────────────────────────────────────────────────────

def _render_context_panel(
    engine: DataAssistantEngine,
    default_target: str,
    uploaded_file_path: str | None,
) -> None:
    st.markdown("### 🧠 Session Memory")
    st.metric("Active table", st.session_state.current_table or "None")
    st.metric("Messages", len(st.session_state.messages))
    row_count = 0 if st.session_state.editor_df is None else len(st.session_state.editor_df)
    st.metric("Editable rows", row_count)

    if st.button("🗑️ Clear chat", use_container_width=True):
        _reset_chat_state(keep_config=True)
        st.rerun()

    if st.session_state.blueprint is not None:
        with st.expander("Current blueprint (JSON)"):
            st.json(st.session_state.blueprint)


# ─────────────────────────────────────────────────────────────────────────────
# Message factory helpers
# ─────────────────────────────────────────────────────────────────────────────

def _msg_from_result(result: ExecutionResult) -> dict[str, Any]:
    if result.dataframe is not None:
        st.session_state.current_dataframe = result.dataframe
    if result.plan.table_name:
        st.session_state.current_table = result.plan.table_name

    meta: dict[str, Any] = dict(result.metadata)
    if result.plan.table_name:
        meta["table_name"] = result.plan.table_name
    if result.figure is not None:
        meta["chart"] = result.figure
    if result.dataframe is not None:
        meta["dataframe"] = result.dataframe
    if result.file_path:
        meta["file_path"] = result.file_path

    return _msg(result.message, metadata=meta, status="success" if result.success else "error")


def _msg(
    content: str,
    metadata: dict[str, Any] | None = None,
    blueprint: dict[str, Any] | None = None,
    show_editor: bool = False,
    status: str = "info",
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "role": "assistant",
        "content": content,
        "status": status,
        "metadata": metadata or {},
    }
    if blueprint is not None:
        payload["blueprint"] = blueprint
    if show_editor:
        payload["show_editor"] = True
    return payload


# ─────────────────────────────────────────────────────────────────────────────
# Status badge
# ─────────────────────────────────────────────────────────────────────────────

def _render_status_badge(status: str) -> None:
    normalized = status if status in {"success", "error", "info"} else "info"
    bg, fg = {
        "success": ("rgba(22,163,74,0.15)", "#15803d"),
        "error":   ("rgba(220,38,38,0.13)", "#b91c1c"),
        "info":    ("rgba(15,118,110,0.13)", "#0f766e"),
    }[normalized]
    st.markdown(
        f"<span style='display:inline-block;padding:0.15rem 0.5rem;"
        f"border-radius:999px;background:{bg};color:{fg};"
        f"font-size:0.74rem;margin-bottom:0.3rem;font-weight:600;'>"
        f"{normalized.upper()}</span>",
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Session state bootstrap & reset
# ─────────────────────────────────────────────────────────────────────────────

def _bootstrap_state() -> None:
    defaults = {
        "messages": [],
        "current_table": None,
        "blueprint": None,
        "current_dataframe": None,
        "editor_df": None,
        "uploaded_file_path": None,
        "default_target": "mysql",
        "config_values": {},
        "executed_commands": set(),   # ← the duplicate-guard set
    }
    for key, value in defaults.items():
        st.session_state.setdefault(key, value)

    if not st.session_state.messages:
        st.session_state.messages = [
            {
                "role": "assistant",
                "content": (
                    "👋 Ready! Try:\n"
                    "- `Create a students table with name, age, and CGPA`\n"
                    "- `add 5 rows`\n"
                    "- `show data`\n"
                    "- `save to mysql`\n"
                    "- `show tables`"
                ),
                "status": "info",
                "metadata": {},
            }
        ]


def _reset_chat_state(keep_config: bool = True) -> None:
    cfg   = st.session_state.get("config_values", {}) if keep_config else {}
    tgt   = st.session_state.get("default_target", "mysql") if keep_config else "mysql"
    fpath = st.session_state.get("uploaded_file_path") if keep_config else None

    for key in ("messages", "current_table", "blueprint", "current_dataframe",
                "editor_df", "executed_commands"):
        st.session_state.pop(key, None)

    _bootstrap_state()
    st.session_state.config_values      = cfg
    st.session_state.default_target     = tgt
    st.session_state.uploaded_file_path = fpath


# ─────────────────────────────────────────────────────────────────────────────
# Intent helpers (pure functions – easy to unit-test)
# ─────────────────────────────────────────────────────────────────────────────

def _is_create_table_request(command: str) -> bool:
    low = command.lower()
    return "table" in low and any(t in low for t in ("create", "make", "build"))


def _is_add_rows_request(low: str) -> bool:
    return any(p in low for p in ("add rows", "add row", "insert rows", "insert row")) or (
        low.startswith("add ") and "table" not in low
    )


def _is_show_data_request(low: str) -> bool:
    return low in {"show data", "show the data", "show rows", "show table", "show all data"}


def _is_save_request(low: str) -> bool:
    return normalize_identifier(low) in {"save_to_mysql", "save_mysql", "save_to_database", "save_data"}


# ─────────────────────────────────────────────────────────────────────────────
# Config factory
# ─────────────────────────────────────────────────────────────────────────────

def _build_config(
    base: AppConfig,
    host: str, port: int, user: str, pw: str, db: str,
) -> AppConfig:
    cfg = AppConfig.from_env(Path.cwd())
    cfg.mysql_host     = host
    cfg.mysql_port     = port
    cfg.mysql_user     = user
    cfg.mysql_password = pw
    cfg.mysql_database = db
    cfg.openai_api_key = ""
    cfg.openai_model   = ""
    return cfg


# ─────────────────────────────────────────────────────────────────────────────
# File upload helper
# ─────────────────────────────────────────────────────────────────────────────

def _save_uploaded_file(
    uploaded_file: "st.runtime.uploaded_file_manager.UploadedFile",
    upload_dir: Path,
) -> Path:
    upload_dir.mkdir(parents=True, exist_ok=True)
    dest = upload_dir / uploaded_file.name
    dest.write_bytes(uploaded_file.getbuffer())
    return dest


# ─────────────────────────────────────────────────────────────────────────────
# Adaptive CSS  –  light AND dark mode compatible
#
# Strategy: use Streamlit's own CSS custom properties (--text-color,
# --background-color, etc.) rather than hard-coded hex values.
# Where we need our own accent colours we define a small palette of
# semi-transparent overlays that look good on any background.
# ─────────────────────────────────────────────────────────────────────────────

def _inject_styles() -> None:
    st.markdown(
        """
        <style>
        /* ── Layout ─────────────────────────────────────────────────────── */
        .block-container {
            max-width: 1380px;
            padding-top: 1.2rem;
            padding-bottom: 2rem;
        }

        /* ── Typography ─────────────────────────────────────────────────── */
        h1, h2, h3 {
            font-family: "Georgia", "Times New Roman", serif;
            letter-spacing: -0.01em;
        }

        /* ── App background: subtle gradient that adapts ─────────────────
           We use a very light overlay so it works in both modes.          */
        .stApp {
            background:
                radial-gradient(ellipse at top left,  rgba(15,118,110,0.08) 0%, transparent 55%),
                radial-gradient(ellipse at top right, rgba(180,83,9,0.06)   0%, transparent 45%);
        }

        /* ── Chat messages ───────────────────────────────────────────────── */
        [data-testid="stChatMessage"] {
            border-radius: 12px;
            padding: 0.6rem 0.8rem;
            margin-bottom: 0.5rem;
            /* Use Streamlit's own secondary background – correct in both themes */
            background: var(--secondary-background-color, rgba(128,128,128,0.07));
            border: 1px solid rgba(128,128,128,0.12);
        }

        /* User bubble: slightly different shade */
        [data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-user"]) {
            background: rgba(15,118,110,0.08);
            border-color: rgba(15,118,110,0.18);
        }

        /* ── Chat input bar ──────────────────────────────────────────────── */
        [data-testid="stChatInput"] textarea {
            border-radius: 24px !important;
            font-size: 0.95rem;
        }

        /* ── Metric cards ────────────────────────────────────────────────── */
        [data-testid="metric-container"] {
            background: var(--secondary-background-color, rgba(128,128,128,0.07));
            border: 1px solid rgba(128,128,128,0.12);
            border-radius: 10px;
            padding: 0.5rem 0.8rem;
        }

        /* ── Data editor / dataframe ─────────────────────────────────────── */
        [data-testid="stDataFrame"], [data-testid="stDataEditor"] {
            border-radius: 8px;
            overflow: hidden;
        }

        /* ── Sidebar ─────────────────────────────────────────────────────── */
        [data-testid="stSidebar"] {
            border-right: 1px solid rgba(128,128,128,0.15);
        }

        /* ── Buttons ─────────────────────────────────────────────────────── */
        button[kind="primary"] {
            border-radius: 8px !important;
        }

        /* ── Code blocks ─────────────────────────────────────────────────── */
        pre {
            border-radius: 8px;
            font-size: 0.82rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
