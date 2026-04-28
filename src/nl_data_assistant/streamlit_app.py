"""
streamlit_app.py — Simple, friendly UI for the AI Data Assistant.

Key fixes vs old version:
- BUG FIXED: `run_streamlit_app = _workspace_run_streamlit_app` at the bottom
  was overriding the entire 575-line function. That line is GONE.
- Chat is the main focus
- Excel upload is always visible in the sidebar (not buried in a tab)
- "Save Chat" button always in the header
- SQL shown collapsed so non-technical users aren't overwhelmed
- Friendly, conversational reply text
- All original features kept: table editor, SQL editor, query history,
  blueprint builder, Excel import/export, destructive confirmation
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


# ── Page config ───────────────────────────────────────────────────────────────

def run_streamlit_app() -> None:
    st.set_page_config(
        page_title="Data Assistant",
        page_icon="🧠",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    _inject_css()
    _init_session()
    _render_header()
    _render_body()


# ── Session init ──────────────────────────────────────────────────────────────

def _init_session() -> None:
    defaults = {
        "engine":               DataAssistantEngine(),
        "chat":                 [],
        "pending_plan":         None,
        "query_log":            [],
        "current_table":        "",
        "table_editor_df":      pd.DataFrame(),
        "table_editor_table":   "",
        "table_editor_version": 0,
        "sql_editor_text":      "",
        "sql_result":           None,
        "prefill":              "",
        "blueprint":            None,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def _eng() -> DataAssistantEngine:
    return st.session_state.engine


# ── Header ────────────────────────────────────────────────────────────────────

def _render_header() -> None:
    col_title, col_db, col_save, col_clear = st.columns([4, 3, 1.5, 1])

    with col_title:
        st.markdown("## 🧠 Data Assistant")
        st.caption("Just talk — I'll handle the SQL.")

    with col_db:
        if _eng().mysql.ping():
            tables = _eng().mysql.get_table_names()
            st.success(
                f"Connected · {len(tables)} table{'s' if len(tables) != 1 else ''}",
                icon="✅",
            )
        else:
            st.error("MySQL unreachable — check your .env file", icon="🔴")
            st.stop()

    with col_save:
        if st.session_state.chat:
            st.download_button(
                "💾 Save chat",
                data=_build_chat_export(),
                file_name=f"chat_{datetime.now().strftime('%Y%m%d_%H%M')}.txt",
                mime="text/plain",
                use_container_width=True,
            )

    with col_clear:
        if st.button("🗑️ Clear", use_container_width=True):
            _clear_all()
            st.rerun()

    st.divider()


# ── Body layout ───────────────────────────────────────────────────────────────

def _render_body() -> None:
    sidebar_col, chat_col = st.columns([1, 3], gap="large")
    with sidebar_col:
        _render_sidebar()
    with chat_col:
        _render_chat_area()
        _render_editors()
        _render_chat_input()


# ── Sidebar ───────────────────────────────────────────────────────────────────

def _render_sidebar() -> None:

    # Quick examples
    st.markdown("#### 💡 Try these")
    examples = [
        ("📋 List all tables",       "Show all tables"),
        ("🏗️ Create students table", "Create a students table with name, cgpa, and branch"),
        ("➕ Add sample rows",       "Insert 5 students with random data"),
        ("🔍 Show students",         "Show all students ordered by cgpa desc"),
        ("📊 Bar chart",             "Show me a bar chart of students"),
        ("🗑️ Delete low CGPA",      "Delete students with cgpa less than 6"),
        ("📝 Describe table",        "Describe the schema of students"),
    ]
    for label, cmd in examples:
        if st.button(label, use_container_width=True, key=f"ex_{cmd}"):
            st.session_state.prefill = cmd
            st.rerun()

    st.divider()

    # Your tables
    st.markdown("#### 🗄️ Your tables")
    tables = _eng().mysql.get_table_names()
    if tables:
        for t in tables:
            if st.button(f"  📁 {t}", use_container_width=True, key=f"tbl_{t}"):
                st.session_state.prefill = f"Show all {t}"
                st.rerun()
        if st.button("🔄 Refresh", use_container_width=True):
            st.rerun()
    else:
        st.caption("No tables yet. Create one by asking!")

    st.divider()

    # Build a table form
    with st.expander("🏗️ Build a table", expanded=not tables):
        with st.form("create_table_form", clear_on_submit=True):
            tname = st.text_input("Table name", placeholder="students")
            tcols = st.text_input("Columns", placeholder="name, cgpa, branch")
            recreate = st.checkbox("Replace if already exists")
            submitted = st.form_submit_button(
                "Create", use_container_width=True, type="primary"
            )
        if submitted:
            blueprint = _blueprint_from_inputs(tname, tcols)
            if blueprint is None:
                st.error("Fill in both a name and at least one column.")
            else:
                result = _eng().mysql.create_table_from_blueprint(
                    blueprint, recreate=recreate
                )
                if result.success:
                    st.session_state.current_table = blueprint["table_name"]
                    st.session_state.blueprint = blueprint
                    _set_sql_editor(result.sql_executed or blueprint.get("create_sql", ""))
                    _set_table_editor(
                        blueprint["table_name"],
                        pd.DataFrame(blueprint.get("sample_data") or []),
                    )
                    _log_query(result)
                    _append(
                        "assistant",
                        f"Done! Created table `{blueprint['table_name']}`. "
                        "You can edit rows in the Table Editor below and save when ready.",
                        result,
                    )
                    st.rerun()
                else:
                    st.error(result.error or "Couldn't create the table.")

    st.divider()

    # Excel section — always visible
    st.markdown("#### 📂 Excel")
    uploaded = st.file_uploader(
        "Upload an Excel file",
        type=["xlsx", "xls"],
        label_visibility="collapsed",
        key="excel_upload",
    )

    if uploaded:
        tmp = Path(tempfile.gettempdir()) / uploaded.name
        tmp.write_bytes(uploaded.read())
        excel_svc = _eng().excel
        sheets = excel_svc.list_sheets(tmp)
        sheet = st.selectbox("Sheet", sheets, key="excel_sheet")
        df = excel_svc.read_sheet(tmp, sheet)

        st.caption(f"{len(df)} rows · {len(df.columns)} cols")

        with st.expander("Preview", expanded=False):
            st.dataframe(df.head(8), hide_index=True, use_container_width=True)

        buf = io.BytesIO()
        df.to_excel(buf, index=False)
        st.download_button(
            "⬇️ Download",
            data=buf.getvalue(),
            file_name=f"{Path(uploaded.name).stem}_{sheet}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

        tbl_name = st.text_input(
            "Save to MySQL table as", value=sheet.lower(), key="excel_tbl"
        )
        if st.button("⬆️ Import to MySQL", use_container_width=True, type="primary"):
            with st.spinner("Importing…"):
                n = _eng().sync.excel_to_mysql(tmp, tbl_name, sheet)
            st.success(f"Imported {n} rows into `{tbl_name}`!")
            _append(
                "assistant",
                f"Imported **{n} rows** from `{uploaded.name}` into MySQL table `{tbl_name}`.",
            )
            st.rerun()

    # Export MySQL → Excel
    if tables:
        with st.expander("Export table → Excel", expanded=False):
            tbl = st.selectbox("Table to export", tables, key="export_tbl")
            if st.button("⬇️ Export", use_container_width=True):
                out = Path(tempfile.gettempdir()) / f"{tbl}_export.xlsx"
                _eng().sync.mysql_to_excel(tbl, out)
                st.download_button(
                    f"Download {tbl}.xlsx",
                    data=out.read_bytes(),
                    file_name=f"{tbl}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )

    st.divider()

    # Query history (compact, in sidebar)
    log = st.session_state.query_log
    if log:
        with st.expander(f"📜 Query history ({len(log)})", expanded=False):
            sql_dump = "\n\n".join(
                f"-- {e['ts']} {'OK' if e['ok'] else 'FAILED'}\n{e['sql']}"
                for e in log
            )
            st.download_button(
                "⬇️ Download .sql",
                data=sql_dump,
                file_name="session_queries.sql",
                mime="text/plain",
                use_container_width=True,
            )
            for entry in reversed(log[-20:]):
                icon = "✅" if entry["ok"] else "❌"
                with st.expander(
                    f"{icon} {entry['ts']} — {entry['sql'][:45]}…", expanded=False
                ):
                    st.code(entry["sql"], language="sql")
                    c1, c2 = st.columns(2)
                    if c1.button(
                        "✏️ Edit",
                        key=f"edit_{entry['ts']}_{entry['sql'][:8]}",
                        use_container_width=True,
                    ):
                        _set_sql_editor(entry["sql"])
                        st.rerun()
                    if c2.button(
                        "▶️ Re-run",
                        key=f"rerun_{entry['ts']}_{entry['sql'][:8]}",
                        use_container_width=True,
                    ):
                        result = _eng().execute_raw(entry["sql"])
                        _log_query(result)
                        _append("assistant", _result_summary(result), result)
                        st.rerun()


# ── Chat area ─────────────────────────────────────────────────────────────────

def _render_chat_area() -> None:
    import plotly.graph_objects as go

    if not st.session_state.chat:
        st.markdown(
            """
            <div style="text-align:center;padding:50px 0 20px;color:#9aa0a6;">
                <div style="font-size:52px;">🗄️</div>
                <div style="font-size:20px;font-weight:600;margin-top:10px;color:#3c4043;">
                    Ask me anything about your data
                </div>
                <div style="font-size:14px;margin-top:6px;">
                    Try "show all students" · "create a sales table" · "bar chart of expenses"
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    for turn in st.session_state.chat:
        with st.chat_message(turn["role"]):
            st.markdown(turn["content"])
            result: ExecutionResult | None = turn.get("result")
            if result and result.data is not None:
                if isinstance(result.data, pd.DataFrame) and not result.data.empty:
                    st.dataframe(result.data, use_container_width=True, hide_index=True)
                    buf = io.BytesIO()
                    result.data.to_excel(buf, index=False)
                    st.download_button(
                        "⬇️ Download as Excel",
                        data=buf.getvalue(),
                        file_name="result.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key=f"dl_{turn['ts']}_{id(result)}",
                    )
                elif isinstance(result.data, go.Figure):
                    st.plotly_chart(result.data, use_container_width=True)
            if result and result.error:
                st.error(result.error)

    # Destructive confirmation
    if st.session_state.pending_plan is not None:
        plan = st.session_state.pending_plan
        st.warning(
            f"⚠️ This will **{plan.intent.value.upper()}** on `{plan.table_name}`. "
            "Are you sure? This can't be undone.",
            icon="⚠️",
        )
        yes_col, no_col = st.columns(2)
        if yes_col.button("✅ Yes, go ahead", type="primary", use_container_width=True):
            result = _eng().execute(plan)
            _handle_result(plan, result)
            st.session_state.pending_plan = None
            st.rerun()
        if no_col.button("❌ Cancel", use_container_width=True):
            _append("assistant", "OK, cancelled. Nothing was changed.")
            st.session_state.pending_plan = None
            st.rerun()


# ── Editors ───────────────────────────────────────────────────────────────────

def _render_editors() -> None:
    current = st.session_state.current_table

    # Table editor
    with st.expander(
        f"📝 Table Editor{f'  —  `{current}`' if current else ''}",
        expanded=bool(current),
    ):
        if not current:
            st.info("Select or create a table to edit rows directly here.")
        else:
            if st.session_state.table_editor_table != current:
                _load_table_editor(current)

            c_reload, c_save = st.columns(2)
            if c_reload.button("🔄 Reload from DB", use_container_width=True):
                _load_table_editor(current)
                st.rerun()

            editor_key = f"te_{current}_{st.session_state.table_editor_version}"
            edited = st.data_editor(
                st.session_state.table_editor_df,
                key=editor_key,
                num_rows="dynamic",
                use_container_width=True,
            )
            st.session_state.table_editor_df = edited

            if c_save.button("💾 Save to MySQL", type="primary", use_container_width=True):
                save_result = _eng().mysql.replace_table_data(current, edited)
                _log_query(save_result)
                if save_result.success:
                    _set_sql_editor(save_result.sql_executed)
                    _append(
                        "assistant",
                        f"Saved your edits back to `{current}` ✔",
                        save_result,
                    )
                    _load_table_editor(current)
                    st.rerun()
                else:
                    st.error(save_result.error or "Save failed.")

    # SQL editor (only shown after a command runs)
    if st.session_state.sql_editor_text:
        with st.expander("🔧 SQL Editor — tweak and re-run", expanded=False):
            sql = st.text_area(
                "SQL",
                value=st.session_state.sql_editor_text,
                height=160,
                key="sql_editor_widget",
                label_visibility="collapsed",
                help="You can edit the generated SQL and run it again.",
            )
            c1, c2 = st.columns(2)
            if c1.button("▶️ Run SQL", type="primary", use_container_width=True):
                result = _eng().execute_raw(sql)
                _log_query(result)
                _append("assistant", _result_summary(result), result)
                _set_sql_editor(sql)
                st.rerun()
            if c2.button("✖ Clear", use_container_width=True):
                st.session_state.sql_editor_text = ""
                st.rerun()


# ── Chat input ────────────────────────────────────────────────────────────────

def _render_chat_input() -> None:
    prefill = st.session_state.pop("prefill", "") or ""
    blocked = st.session_state.pending_plan is not None

    prompt = st.chat_input(
        "Ask anything — e.g. 'show all students' or 'make a bar chart of expenses'",
        disabled=blocked,
    )

    if prefill and not blocked:
        _process_command(prefill)
        st.rerun()

    if prompt and not blocked:
        _process_command(prompt)
        st.rerun()


# ── Command processing ────────────────────────────────────────────────────────

def _process_command(command: str) -> None:
    _append("user", command)

    with st.spinner("Thinking…"):
        try:
            plan = _eng().parse(command, default_table=st.session_state.current_table)
        except TypeError:
            plan = _eng().parse(command)

    if plan.is_destructive:
        st.session_state.pending_plan = plan
        _append(
            "assistant",
            f"Heads up — this will **{plan.intent.value.replace('_', ' ')}** "
            f"on `{plan.table_name or st.session_state.current_table or 'the table'}`. "
            "Confirm below if you want to proceed.",
        )
        return

    with st.spinner("Running…"):
        result = _eng().execute(plan)

    _handle_result(plan, result)


def _handle_result(plan, result: ExecutionResult) -> None:
    _log_query(result)
    _append("assistant", _friendly_reply(result), result)

    if result.sql_executed:
        _set_sql_editor(result.sql_executed)

    if not result.success:
        return

    if plan.table_name:
        st.session_state.current_table = plan.table_name

    if plan.intent == Intent.CREATE_TABLE:
        bp = _blueprint_from_command(plan.raw_command, plan.table_name)
        st.session_state.blueprint = bp
        sample = result.data if isinstance(result.data, pd.DataFrame) else pd.DataFrame()
        _set_table_editor(plan.table_name, sample)
        return

    if plan.intent in {
        Intent.SELECT, Intent.INSERT, Intent.UPDATE,
        Intent.DELETE, Intent.DESCRIBE,
    }:
        if st.session_state.current_table:
            try:
                _load_table_editor(st.session_state.current_table)
            except Exception:
                pass


def _friendly_reply(result: ExecutionResult) -> str:
    import plotly.graph_objects as go

    if not result.success:
        return f"Something went wrong: {result.error or result.message}"

    if isinstance(result.data, pd.DataFrame):
        n = len(result.data)
        if n == 0:
            return "Query ran fine, but got no rows back — the table might be empty or your filter didn't match anything."
        return f"Here you go — {n} row{'s' if n != 1 else ''} found."

    if isinstance(result.data, go.Figure):
        return "Here's your chart! 📊"

    if result.rows_affected:
        return f"Done! {result.rows_affected} row{'s' if result.rows_affected != 1 else ''} affected."

    return result.message or "Done! ✔"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _append(role: str, content: str, result: ExecutionResult | None = None) -> None:
    st.session_state.chat.append({
        "role": role,
        "content": content,
        "result": result,
        "ts": datetime.now().strftime("%H:%M:%S"),
    })


def _log_query(result: ExecutionResult) -> None:
    if result.sql_executed:
        st.session_state.query_log.append({
            "ts": datetime.now().strftime("%H:%M:%S"),
            "sql": result.sql_executed,
            "ok": result.success,
        })


def _set_sql_editor(sql: str) -> None:
    st.session_state.sql_editor_text = sql


def _set_table_editor(table_name: str, df: pd.DataFrame) -> None:
    st.session_state.current_table = table_name
    st.session_state.table_editor_table = table_name
    st.session_state.table_editor_df = df.copy()
    st.session_state.table_editor_version += 1


def _load_table_editor(table_name: str) -> None:
    df = _eng().mysql.fetch_table(table_name)
    _set_table_editor(table_name, df)


def _result_summary(result: ExecutionResult) -> str:
    if not result.success:
        return result.message or result.error or "Failed."
    msg = _friendly_reply(result)
    if result.sql_executed:
        return f"```sql\n{result.sql_executed}\n```\n\n{msg}"
    return msg


def _blueprint_from_command(command: str, fallback_table: str = "") -> dict:
    bp = TableBlueprint().generate(command)
    if fallback_table:
        bp["table_name"] = fallback_table
    return bp


def _blueprint_from_inputs(table_name: str, columns_text: str) -> dict | None:
    name = table_name.strip()
    cols = columns_text.strip()
    if not name or not cols:
        return None
    return _blueprint_from_command(
        f"create a table of {name} with {cols}", fallback_table=name
    )


def _build_chat_export() -> str:
    lines = [
        f"Chat saved on {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "=" * 50,
        "",
    ]
    for turn in st.session_state.chat:
        who = "You" if turn["role"] == "user" else "Assistant"
        lines.append(f"[{turn.get('ts', '')}] {who}:")
        lines.append(f"  {turn['content']}")
        result = turn.get("result")
        if result and result.sql_executed:
            lines.append(f"  SQL: {result.sql_executed}")
        lines.append("")
    return "\n".join(lines)


def _clear_all() -> None:
    st.session_state.chat.clear()
    st.session_state.pending_plan = None
    st.session_state.sql_editor_text = ""
    st.session_state.sql_result = None
    st.session_state.query_log.clear()
    _eng().clear_history()


def _inject_css() -> None:
    st.markdown(
        """
        <style>
            [data-testid="stChatMessage"] { border-radius: 12px; padding: 4px 0; }
            .stButton > button { border-radius: 8px; font-size: 13px; }
            [data-testid="stDownloadButton"] > button { border-radius: 8px; font-size: 13px; }
            code { font-size: 12px; }
            #MainMenu, footer { visibility: hidden; }
            [data-testid="stSidebar"] .block-container { padding-top: 1rem; }
        </style>
        """,
        unsafe_allow_html=True,
    )