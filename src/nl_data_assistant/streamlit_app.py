"""
streamlit_app.py — Full Streamlit UI for the AI Data Assistant.

Features:
- Multi-turn chat with persistent history
- Live schema sidebar
- Destructive operation confirmation dialogs
- Query history export as .sql
- Excel upload / download
- Plotly chart rendering
"""
from __future__ import annotations

import io
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from nl_data_assistant.models import ExecutionResult, Intent
from nl_data_assistant.nlp.table_blueprint import TableBlueprint
from nl_data_assistant.services.engine import DataAssistantEngine
from nl_data_assistant.streamlit_workspace_app import run_streamlit_app as _workspace_run_streamlit_app


# ── Page config ───────────────────────────────────────────────────────────────

def run_streamlit_app() -> None:
    st.set_page_config(
        page_title="AI Data Assistant",
        page_icon="🗄️",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    _inject_css()
    _init_session()

    st.title("🗄️ AI Data Assistant")
    st.caption("Talk naturally to your MySQL database and Excel sheets locally.")

    # Layout
    sidebar_col, main_col = st.columns([1, 3], gap="large")

    with sidebar_col:
        _render_sidebar()

    with main_col:
        tab_mysql, tab_excel, tab_history = st.tabs(["💬 MySQL Chat", "📊 Excel", "📜 Query History"])
        with tab_mysql:
            _render_mysql_tab()
        with tab_excel:
            _render_excel_tab()
        with tab_history:
            _render_history_tab()


# ── Session state ─────────────────────────────────────────────────────────────

def _init_session() -> None:
    if "engine" not in st.session_state:
        st.session_state.engine = DataAssistantEngine()
    if "chat" not in st.session_state:
        st.session_state.chat = []       # list of {role, content, result}
    if "messages" not in st.session_state:
        st.session_state.messages = st.session_state.chat
    if "pending_plan" not in st.session_state:
        st.session_state.pending_plan = None
    if "query_log" not in st.session_state:
        st.session_state.query_log = []  # list of {ts, sql, ok}
    if "current_table" not in st.session_state:
        st.session_state.current_table = ""
    if "blueprint" not in st.session_state:
        st.session_state.blueprint = None
    if "table_editor_df" not in st.session_state:
        st.session_state.table_editor_df = pd.DataFrame()
    if "table_editor_table" not in st.session_state:
        st.session_state.table_editor_table = ""
    if "table_editor_version" not in st.session_state:
        st.session_state.table_editor_version = 0
    if "sql_editor_text" not in st.session_state:
        st.session_state.sql_editor_text = ""
    if "sql_editor_widget" not in st.session_state:
        st.session_state.sql_editor_widget = ""
    if "pending_sql_editor_text" not in st.session_state:
        st.session_state.pending_sql_editor_text = ""
    if "sql_result" not in st.session_state:
        st.session_state.sql_result = None
    st.session_state.messages = st.session_state.chat


def _engine() -> DataAssistantEngine:
    return st.session_state.engine


# ── Sidebar ───────────────────────────────────────────────────────────────────

def _render_sidebar() -> None:
    st.subheader("Database")

    eng = _engine()
    if eng.mysql.ping():
        st.success("MySQL connected", icon="✅")
    else:
        st.error("MySQL unreachable — check .env", icon="🔴")
        st.stop()

    # Live schema
    with st.expander("📋 Schema", expanded=True):
        tables = eng.mysql.get_table_names()
        if tables:
            for t in tables:
                st.markdown(f"**`{t}`**")
        else:
            st.info("No tables yet — create one!")

    if st.button("🔄 Refresh schema"):
        st.rerun()

    tables = eng.mysql.get_table_names()
    if tables:
        selected_table = st.selectbox(
            "Switch current table",
            options=[""] + tables,
            index=([""] + tables).index(st.session_state.current_table)
            if st.session_state.current_table in tables
            else 0,
            key="sidebar_current_table",
        )
        if selected_table and selected_table != st.session_state.current_table:
            st.session_state.current_table = selected_table
            _load_table_into_editor(selected_table)
            st.rerun()

    with st.expander("Add your own table", expanded=not tables):
        with st.form("create_table_form"):
            table_name = st.text_input("Table name", placeholder="students")
            columns_text = st.text_input(
                "Columns",
                placeholder="name, cgpa, branch",
                help="Comma-separated column names. Types are inferred locally.",
            )
            recreate = st.checkbox("Replace table if it already exists")
            create_table = st.form_submit_button("Create table", use_container_width=True)

        if create_table:
            blueprint = _blueprint_from_inputs(table_name, columns_text)
            if blueprint is None:
                st.error("Enter a table name and at least one column.")
            else:
                result = eng.mysql.create_table_from_blueprint(blueprint, recreate=recreate)
                if result.success:
                    st.session_state.current_table = blueprint["table_name"]
                    st.session_state.blueprint = blueprint
                    _set_sql_editor(result.sql_executed or blueprint.get("create_sql", ""))
                    _set_table_editor(
                        blueprint["table_name"],
                        pd.DataFrame(blueprint.get("sample_data") or []),
                    )
                    _log_query(result)
                    _append_turn(
                        "assistant",
                        (
                            f"Created table `{blueprint['table_name']}` from the table builder. "
                            "Edit the starter rows in the table editor and save when ready."
                        ),
                        result,
                    )
                    st.rerun()
                else:
                    st.error(result.error or "Could not create the table.")

    st.divider()

    # Example prompts
    st.subheader("Try these")
    examples = [
        "Create a students table with name, cgpa, and branch",
        "Insert 5 students with random data",
        "Show all students ordered by cgpa desc",
        "Delete students with cgpa less than 6",
        "Show me a bar chart of students",
        "Export students to Excel",
    ]
    for ex in examples:
        if st.button(ex, use_container_width=True):
            st.session_state["_prefill"] = ex
            st.rerun()

    st.divider()
    if st.button("🗑️ Clear chat history"):
        st.session_state.chat.clear()
        st.session_state.messages = st.session_state.chat
        _engine().clear_history()
        st.session_state.pending_plan = None
        st.session_state.sql_editor_text = ""
        st.session_state.sql_editor_widget = ""
        st.session_state.pending_sql_editor_text = ""
        st.session_state.sql_result = None
        st.rerun()


# ── MySQL Chat tab ────────────────────────────────────────────────────────────

def _render_mysql_tab() -> None:
    _render_current_table_panel()
    _render_sql_editor_panel()

    # Render chat history
    for turn in st.session_state.chat:
        with st.chat_message(turn["role"]):
            st.markdown(turn["content"])
            result: ExecutionResult | None = turn.get("result")
            if result:
                _render_result(result)

    # Handle pending destructive confirmation
    if st.session_state.pending_plan is not None:
        plan = st.session_state.pending_plan
        st.warning(
            f"⚠️ This will **{plan.intent.value.upper()}** on `{plan.table_name}`. "
            "Are you sure?",
            icon="⚠️",
        )
        col_yes, col_no = st.columns(2)
        if col_yes.button("✅ Yes, proceed", type="primary"):
            result = _engine().execute(plan)
            _handle_execution_result(plan, result)
            st.session_state.pending_plan = None
            st.rerun()
        if col_no.button("❌ Cancel"):
            st.session_state.pending_plan = None
            st.rerun()
        return  # block input until confirmed

    # Chat input
    prefill = st.session_state.pop("_prefill", "")
    prompt = st.chat_input("Ask anything — e.g. 'show all students ordered by cgpa'")
    if prefill:
        prompt = prefill

    if prompt:
        _append_turn("user", prompt)
        with st.spinner("Thinking…"):
            plan = _engine().parse(prompt, default_table=st.session_state.current_table)

        if plan.is_destructive:
            # Destructive — needs confirmation
            st.session_state.pending_plan = plan
            _append_turn(
                "assistant",
                (
                    f"This will {plan.intent.value.replace('_', ' ')} "
                    f"on `{plan.table_name or st.session_state.current_table or 'the selected table'}`. "
                    "Please confirm to continue."
                ),
            )
        else:
            with st.spinner("Running command..."):
                result = _engine().execute(plan)
            _handle_execution_result(plan, result)

        st.rerun()


def _handle_execution_result(plan, result: ExecutionResult) -> None:
    _log_query(result)
    _append_turn("assistant", _result_summary(result), result)

    if result.sql_executed:
        _set_sql_editor(result.sql_executed)

    if not result.success:
        return

    if plan.table_name:
        st.session_state.current_table = plan.table_name

    if plan.intent == Intent.CREATE_TABLE:
        st.session_state.blueprint = _blueprint_from_command(plan.raw_command, plan.table_name)
        sample_df = result.data if isinstance(result.data, pd.DataFrame) else pd.DataFrame()
        _set_table_editor(plan.table_name, sample_df)
        return

    if plan.intent in {
        Intent.SELECT,
        Intent.INSERT,
        Intent.UPDATE,
        Intent.DELETE,
        Intent.DESCRIBE,
    } and st.session_state.current_table:
        _load_table_into_editor(st.session_state.current_table)


def _render_current_table_panel() -> None:
    with st.expander("Table editor", expanded=bool(st.session_state.current_table)):
        current_table = st.session_state.current_table
        if not current_table:
            st.info("Select or create a table to edit it here.")
            return

        st.caption(f"Editing `{current_table}`")

        if st.session_state.table_editor_table != current_table:
            _load_table_into_editor(current_table)

        col_reload, col_save = st.columns(2)
        if col_reload.button("Reload table", use_container_width=True):
            _load_table_into_editor(current_table)
            st.rerun()

        editor_key = f"table_editor_widget_{current_table}_{st.session_state.table_editor_version}"
        edited_df = st.data_editor(
            st.session_state.table_editor_df,
            key=editor_key,
            num_rows="dynamic",
            use_container_width=True,
        )
        st.session_state.table_editor_df = edited_df

        if col_save.button("Save table to MySQL", type="primary", use_container_width=True):
            save_result = _save_editor_to_mysql(current_table, edited_df)
            if save_result.success:
                st.success(save_result.message)
                st.rerun()
            else:
                st.error(save_result.error or save_result.message)


def _render_sql_editor_panel() -> None:
    with st.expander("SQL editor", expanded=bool(st.session_state.sql_editor_text)):
        if not st.session_state.sql_editor_text:
            st.info("Run a command first, then edit the generated SQL here.")
            return

        if st.session_state.pending_sql_editor_text:
            st.session_state.sql_editor_widget = st.session_state.pending_sql_editor_text
            st.session_state.pending_sql_editor_text = ""
        elif not st.session_state.sql_editor_widget:
            st.session_state.sql_editor_widget = st.session_state.sql_editor_text

        st.text_area(
            "Editable SQL",
            key="sql_editor_widget",
            height=180,
            help="You can modify the generated SQL and run it again.",
        )
        st.session_state.sql_editor_text = st.session_state.sql_editor_widget
        col_run, col_clear = st.columns(2)
        if col_run.button("Run edited SQL", type="primary", use_container_width=True):
            result = _engine().execute_raw(st.session_state.sql_editor_widget)
            st.session_state.sql_result = result
            _log_query(result)
            _append_turn("assistant", _result_summary(result), result)
            if st.session_state.current_table:
                try:
                    _load_table_into_editor(st.session_state.current_table)
                except Exception:
                    pass
            st.rerun()
        if col_clear.button("Clear SQL", use_container_width=True):
            st.session_state.sql_editor_text = ""
            st.session_state.sql_editor_widget = ""
            st.session_state.pending_sql_editor_text = ""
            st.session_state.sql_result = None
            st.rerun()


def _render_result(result: ExecutionResult) -> None:
    if result.error:
        st.error(result.error)
        return
    if result.data is None:
        return

    import plotly.graph_objects as go
    if isinstance(result.data, pd.DataFrame):
        st.dataframe(result.data, use_container_width=True)
        _download_button(result.data, "result.xlsx")
    elif isinstance(result.data, go.Figure):
        st.plotly_chart(result.data, use_container_width=True)


def _result_summary(result: ExecutionResult) -> str:
    if not result.success:
        return result.message or result.error
    if result.sql_executed:
        return f"```sql\n{result.sql_executed}\n```\n\n{result.message}"
    return result.message or "Done."


def _append_turn(role: str, content: str, result: ExecutionResult | None = None) -> None:
    st.session_state.chat.append({"role": role, "content": content, "result": result})
    st.session_state.messages = st.session_state.chat


def _log_query(result: ExecutionResult) -> None:
    if result.sql_executed:
        st.session_state.query_log.append({
            "ts": datetime.now().strftime("%H:%M:%S"),
            "sql": result.sql_executed,
            "ok": result.success,
        })


# ── Excel tab ─────────────────────────────────────────────────────────────────

def _render_excel_tab() -> None:
    st.subheader("Excel Manager")

    uploaded = st.file_uploader("Upload an Excel file", type=["xlsx", "xls"])
    if uploaded:
        excel_svc = _engine().excel
        # Save to a temp path so ExcelService can read it
        tmp = Path(tempfile.gettempdir()) / uploaded.name
        tmp.write_bytes(uploaded.read())

        sheets = excel_svc.list_sheets(tmp)
        sheet = st.selectbox("Sheet", sheets)
        df = excel_svc.read_sheet(tmp, sheet)
        st.dataframe(df, use_container_width=True)

        col_a, col_b = st.columns(2)
        with col_a:
            _download_button(df, f"{Path(uploaded.name).stem}_{sheet}.xlsx")
        with col_b:
            table_name = st.text_input("Import to MySQL table", value=sheet.lower())
            if st.button("⬆️ Import to MySQL"):
                with st.spinner("Importing…"):
                    n = _engine().sync.excel_to_mysql(tmp, table_name, sheet)
                st.success(f"Imported {n} rows into `{table_name}`")

    st.divider()
    st.subheader("Export MySQL table → Excel")
    tables = _engine().mysql.get_table_names()
    if tables:
        tbl = st.selectbox("Table", tables, key="export_tbl")
        if st.button("⬇️ Export to Excel"):
            with st.spinner("Exporting…"):
                out_path = Path(tempfile.gettempdir()) / f"{tbl}_export.xlsx"
                _engine().sync.mysql_to_excel(tbl, out_path)
            with open(out_path, "rb") as f:
                st.download_button(
                    label=f"Download {tbl}.xlsx",
                    data=f.read(),
                    file_name=f"{tbl}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
    else:
        st.info("No MySQL tables found to export.")


# ── Query history tab ─────────────────────────────────────────────────────────

def _render_history_tab() -> None:
    log = st.session_state.query_log
    if not log:
        st.info("No queries executed yet.")
        return

    st.subheader(f"Query Log — {len(log)} queries")

    # Export as .sql
    sql_dump = "\n\n".join(
        f"-- {entry['ts']} {'OK' if entry['ok'] else 'FAILED'}\n{entry['sql']}"
        for entry in log
    )
    st.download_button(
        "⬇️ Download as .sql",
        data=sql_dump,
        file_name="session_queries.sql",
        mime="text/plain",
    )

    st.divider()

    for entry in reversed(log):
        icon = "✅" if entry["ok"] else "❌"
        with st.expander(f"{icon} {entry['ts']} — {entry['sql'][:60]}…"):
            st.code(entry["sql"], language="sql")
            if st.button(
                "Edit this SQL",
                key=f"edit_{entry['ts']}_{entry['sql']}",
                use_container_width=True,
            ):
                _set_sql_editor(entry["sql"])
                st.rerun()
            if st.button("▶️ Re-run", key=entry["sql"] + entry["ts"]):
                result = _engine().execute_raw(entry["sql"])
                st.session_state.sql_result = result
                _log_query(result)
                _append_turn("assistant", _result_summary(result), result)
                if st.session_state.current_table:
                    try:
                        _load_table_into_editor(st.session_state.current_table)
                    except Exception:
                        pass
                st.rerun()


# ── Shared helpers ────────────────────────────────────────────────────────────

def _blueprint_from_command(command: str, fallback_table: str = "") -> dict:
    blueprint = TableBlueprint().generate(command)
    if fallback_table:
        blueprint["table_name"] = fallback_table
    return blueprint


def _blueprint_from_inputs(table_name: str, columns_text: str) -> dict | None:
    clean_name = table_name.strip()
    clean_columns = columns_text.strip()
    if not clean_name or not clean_columns:
        return None
    return _blueprint_from_command(
        f"create a table of {clean_name} with {clean_columns}",
        fallback_table=clean_name,
    )


def _set_table_editor(table_name: str, df: pd.DataFrame) -> None:
    st.session_state.current_table = table_name
    st.session_state.table_editor_table = table_name
    st.session_state.table_editor_df = df.copy()
    st.session_state.table_editor_version += 1


def _load_table_into_editor(table_name: str) -> None:
    df = _engine().mysql.fetch_table(table_name)
    _set_table_editor(table_name, df)


def _save_editor_to_mysql(table_name: str, df: pd.DataFrame) -> ExecutionResult:
    result = _engine().mysql.replace_table_data(table_name, df)
    if result.success:
        _log_query(result)
        _set_sql_editor(result.sql_executed)
        _append_turn(
            "assistant",
            f"Saved the edited rows back to `{table_name}`.\n\n```sql\n{result.sql_executed}\n```",
            result,
        )
        _load_table_into_editor(table_name)
    return result


def _set_sql_editor(sql: str) -> None:
    st.session_state.sql_editor_text = sql
    st.session_state.pending_sql_editor_text = sql


def _download_button(df: pd.DataFrame, filename: str) -> None:
    buf = io.BytesIO()
    df.to_excel(buf, index=False)
    st.download_button(
        label=f"⬇️ Download {filename}",
        data=buf.getvalue(),
        file_name=filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


def _inject_css() -> None:
    st.markdown("""
    <style>
        .stChatMessage { border-radius: 12px; }
        .stButton > button { border-radius: 8px; }
        code { font-size: 13px; }
    </style>
    """, unsafe_allow_html=True)


run_streamlit_app = _workspace_run_streamlit_app
