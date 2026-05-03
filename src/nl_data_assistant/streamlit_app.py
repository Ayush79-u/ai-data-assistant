"""
streamlit_app.py — Simple, friendly UI for the AI Data Assistant.

Changes vs previous version:
- Removed the "Table options" sidebar block (per request).
- Added "Generate SQL with AI" (Qwen) inside the existing SQL Editor.
- Added DROP/TRUNCATE safety guard before Run SQL.
- All other features preserved: chat, table editor, Excel import/export,
  query history, blueprint builder, destructive confirmation, etc.
"""
from __future__ import annotations

import io
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from nl_data_assistant.models import ExecutionResult, Intent
from nl_data_assistant.nlp.ai_sql_generator import generate_sql, is_safe_sql
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
        "top_table_selector":   "",
        "rename_table_name":    "",
        "rename_table_source":  "",
        "new_column_name":      "",
        "new_column_source":    "",
        "confirm_delete_table": False,
        "ai_nl_input":          "",
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
        st.markdown("## Data Assistant")
        st.caption("Talk in simple English and work on the selected table.")

    with col_db:
        if _eng().mysql.ping():
            tables = _eng().mysql.get_table_names()
            st.success(
                f"Connected · {len(tables)} table{'s' if len(tables) != 1 else ''}",
                icon="✅",
            )
        else:
            st.error("MySQL unreachable - check your .env file", icon="🔴")
            st.stop()

    with col_save:
        if st.session_state.chat:
            st.download_button(
                "Save chat",
                data=_build_chat_export(),
                file_name=f"chat_{datetime.now().strftime('%Y%m%d_%H%M')}.txt",
                mime="text/plain",
                use_container_width=True,
            )

    with col_clear:
        if st.button("End conversation", use_container_width=True):
            _end_conversation()
            st.rerun()

    st.divider()


def _render_body() -> None:
    sidebar_col, chat_col = st.columns([1, 3], gap="large")
    with sidebar_col:
        _render_sidebar()
    with chat_col:
        _render_table_toolbar()
        _render_chat_area()
        _render_chat_input()
        _render_editors()


# ── Sidebar ───────────────────────────────────────────────────────────────────

def _render_sidebar() -> None:
    tables = _eng().mysql.get_table_names()

    # NOTE: "Table options" section removed per request.
    # Table selection still available via the toolbar selectbox in the main area.

    with st.expander("Build a table", expanded=not tables):
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

    # Quick examples
    st.markdown("#### Try these")
    examples = [
        ("List all tables",       "Show all tables"),
        ("Create students table", "Create a students table with name, cgpa, and branch"),
        ("Add sample rows",       "Insert 5 students with random data"),
        ("Show students",         "Show all students ordered by cgpa desc"),
        ("Bar chart",             "Show me a bar chart of students"),
        ("Delete low CGPA",       "Delete students with cgpa less than 6"),
        ("Describe table",        "Describe the schema of students"),
    ]
    for label, cmd in examples:
        if st.button(label, use_container_width=True, key=f"ex_{cmd}"):
            st.session_state.prefill = cmd
            st.rerun()

    st.divider()

    # Excel section - always visible
    st.markdown("#### Excel")
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
            "Download",
            data=buf.getvalue(),
            file_name=f"{Path(uploaded.name).stem}_{sheet}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

        tbl_name = st.text_input(
            "Save to MySQL table as", value=sheet.lower(), key="excel_tbl"
        )
        if st.button("Import to MySQL", use_container_width=True, type="primary"):
            with st.spinner("Importing..."):
                n = _eng().sync.excel_to_mysql(tmp, tbl_name, sheet)
            st.success(f"Imported {n} rows into `{tbl_name}`!")
            _append(
                "assistant",
                f"Imported **{n} rows** from `{uploaded.name}` into MySQL table `{tbl_name}`.",
            )
            st.rerun()

    if tables:
        with st.expander("Export table to Excel", expanded=False):
            tbl = st.selectbox("Table to export", tables, key="export_tbl")
            if st.button("Export", use_container_width=True):
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

    log = st.session_state.query_log
    if log:
        with st.expander(f"Query history ({len(log)})", expanded=False):
            sql_dump = "\n\n".join(
                f"-- {e['ts']} {'OK' if e['ok'] else 'FAILED'}\n{e['sql']}"
                for e in log
            )
            st.download_button(
                "Download .sql",
                data=sql_dump,
                file_name="session_queries.sql",
                mime="text/plain",
                use_container_width=True,
            )
            for entry in reversed(log[-20:]):
                icon = "OK" if entry["ok"] else "FAIL"
                with st.expander(
                    f"{icon} {entry['ts']} - {entry['sql'][:45]}...", expanded=False
                ):
                    st.code(entry["sql"], language="sql")
                    c1, c2 = st.columns(2)
                    if c1.button(
                        "Edit",
                        key=f"edit_{entry['ts']}_{entry['sql'][:8]}",
                        use_container_width=True,
                    ):
                        _set_sql_editor(entry["sql"])
                        st.rerun()
                    if c2.button(
                        "Re-run",
                        key=f"rerun_{entry['ts']}_{entry['sql'][:8]}",
                        use_container_width=True,
                    ):
                        if not is_safe_sql(entry["sql"]):
                            st.error("Blocked: query contains DROP or TRUNCATE.")
                        else:
                            result = _eng().execute_raw(entry["sql"])
                            _log_query(result)
                            _append("assistant", _result_summary(result), result)
                            st.rerun()


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

def _render_table_toolbar() -> None:
    tables = _eng().mysql.get_table_names()
    if not tables:
        return

    current = st.session_state.current_table
    if current and st.session_state.table_editor_table != current:
        _open_table(current, announce=False)

    selector_options = [""] + tables
    if st.session_state.top_table_selector not in selector_options:
        st.session_state.top_table_selector = current if current in tables else ""

    if current and st.session_state.rename_table_source != current:
        st.session_state.rename_table_name = current
        st.session_state.rename_table_source = current

    selected_index = selector_options.index(st.session_state.top_table_selector)
    toolbar_col, save_col, reload_col = st.columns([4, 1.4, 1.2])

    with toolbar_col:
        st.selectbox(
            "Current table",
            options=selector_options,
            index=selected_index,
            key="top_table_selector",
            on_change=_select_current_table,
            format_func=lambda name: "Select a table" if name == "" else name,
        )

    with save_col:
        if st.button(
            "Save to MySQL",
            type="primary",
            use_container_width=True,
            key="toolbar_save_to_mysql",
        ):
            result = _save_current_table()
            if result.success:
                st.rerun()
            st.error(result.error or "Save failed.")

    with reload_col:
        if st.button("Reload table", use_container_width=True, key="reload_table_toolbar"):
            _open_table(st.session_state.top_table_selector, announce=False)
            st.rerun()

    st.caption(
        "Selecting a table opens it immediately. Save to MySQL before ending "
        "the conversation if you want to keep your edits."
    )

    with st.expander("Manage current table", expanded=False):
        st.text_input("Rename table to", key="rename_table_name")
        rename_col, delete_col = st.columns(2)
        if rename_col.button(
            "Rename table",
            use_container_width=True,
            key="rename_current_table_button",
        ):
            result = _rename_current_table(st.session_state.rename_table_name)
            if result.success:
                st.rerun()
            st.error(result.error or "Rename failed.")

        st.checkbox(
            "I understand deleting a table cannot be undone.",
            key="confirm_delete_table",
        )
        if delete_col.button(
            "Delete table",
            use_container_width=True,
            disabled=not st.session_state.confirm_delete_table,
            key="delete_current_table_button",
        ):
            result = _delete_current_table()
            if result.success:
                st.rerun()
            st.error(result.error or "Delete failed.")

    st.divider()


def _render_editors() -> None:
    current = st.session_state.current_table

    with st.expander(
        f"Table Editor{f'  -  `{current}`' if current else ''}",
        expanded=bool(current),
    ):
        if not current:
            st.info("Select or create a table to edit rows directly here.")
        else:
            if st.session_state.table_editor_table != current:
                _load_table_editor(current)

            if st.session_state.get("new_column_source", "") != current:
                st.session_state.new_column_name = ""
                st.session_state.new_column_source = current

            c_reload, c_add_row, c_save = st.columns(3)
            if c_reload.button(
                "Reload from DB",
                use_container_width=True,
                key=f"reload_editor_{current}",
            ):
                _load_table_editor(current)
                st.rerun()
            if c_add_row.button(
                "Add blank row",
                use_container_width=True,
                key=f"add_blank_row_{current}",
            ):
                _add_blank_row(current)
                st.rerun()

            column_name_col, column_button_col = st.columns([3, 1.2])
            column_name_col.text_input(
                "New column name",
                key="new_column_name",
                placeholder="branch",
            )
            if column_button_col.button(
                "Add column",
                use_container_width=True,
                key=f"add_column_{current}",
            ):
                error_message = _add_blank_column(current, st.session_state.new_column_name)
                if error_message:
                    st.error(error_message)
                else:
                    st.rerun()

            editor_key = f"te_{current}_{st.session_state.table_editor_version}"
            edited = st.data_editor(
                st.session_state.table_editor_df,
                key=editor_key,
                num_rows="dynamic",
                use_container_width=True,
            )
            st.session_state.table_editor_df = edited

            if c_save.button(
                "Save to MySQL",
                type="primary",
                use_container_width=True,
                key=f"save_editor_{current}",
            ):
                save_result = _save_current_table()
                if save_result.success:
                    st.rerun()
                st.error(save_result.error or "Save failed.")

    # SQL Editor — always available (also exposes the AI generator)
    if st.session_state.sql_editor_text or st.session_state.current_table:
        with st.expander("SQL Editor - tweak and re-run", expanded=False):

            # ── AI SQL generation (Qwen) ──────────────────────────────
            st.markdown("**🤖 Generate SQL with AI**")
            ai_col_input, ai_col_btn = st.columns([4, 1.4])
            ai_col_input.text_input(
                "Ask in English",
                key="ai_nl_input",
                placeholder="e.g. show top 5 rows ordered by cgpa desc",
                label_visibility="collapsed",
            )
            if ai_col_btn.button(
                "Generate SQL with AI",
                use_container_width=True,
                key="ai_generate_sql_button",
            ):
                _generate_sql_with_ai(st.session_state.get("ai_nl_input", ""))
                st.rerun()
            st.caption("Generated SQL appears below. Review it, then click Run SQL.")
            # ──────────────────────────────────────────────────────────

            sql = st.text_area(
                "SQL",
                value=st.session_state.sql_editor_text,
                height=160,
                key="sql_editor_widget",
                label_visibility="collapsed",
                help="You can edit the generated SQL and run it again.",
            )
            c1, c2 = st.columns(2)
            if c1.button(
                "Run SQL",
                type="primary",
                use_container_width=True,
                key="run_sql_editor_button",
            ):
                # Safety check before execution
                if not is_safe_sql(sql):
                    st.error("Blocked: queries containing DROP or TRUNCATE are not allowed.")
                else:
                    result = _eng().execute_raw(sql)
                    _log_query(result)
                    _append("assistant", _result_summary(result), result)
                    _set_sql_editor(sql)
                    st.rerun()
            if c2.button("Clear SQL", use_container_width=True, key="clear_sql_editor_button"):
                st.session_state.sql_editor_text = ""
                st.rerun()


def _render_chat_input() -> None:
    prefill = st.session_state.pop("prefill", "") or ""
    blocked = st.session_state.pending_plan is not None
    current = st.session_state.current_table

    prompt = st.chat_input(
        (
            f"Ask in simple English about `{current}` - try 'add 3 rows' or 'show all data'"
            if current
            else "Ask anything - e.g. 'show all students' or 'make a bar chart of expenses'"
        ),
        disabled=blocked,
    )

    if prefill and not blocked:
        _process_command(prefill)
        st.rerun()

    if prompt and not blocked:
        _process_command(prompt)
        st.rerun()


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


# ── AI SQL helper ─────────────────────────────────────────────────────────────

def _generate_sql_with_ai(nl_input: str) -> None:
    """Call Qwen and inject the generated SQL into the existing editor."""
    if not nl_input.strip():
        st.warning("Type what you want in plain English first.")
        return

    current = st.session_state.current_table

    # Detect data source: Excel if a workbook is uploaded this session, else MySQL.
    data_source = "excel" if st.session_state.get("excel_upload") is not None else "mysql"

    # Build a compact schema string for the active table (or whole DB).
    schema_str = ""
    try:
        if current:
            cols = _eng().mysql.get_table_columns(current)
            schema_str = ", ".join(f"{c['name']} {c['type']}" for c in cols)
        else:
            schema_str = _eng().mysql.get_schema_summary()
    except Exception as exc:
        schema_str = f"(could not load schema: {exc})"

    try:
        with st.spinner("Asking Qwen…"):
            sql = generate_sql(
                user_input=nl_input,
                schema=schema_str,
                table_name=current,
                data_source=data_source,
            )
    except Exception as exc:
        st.error(f"AI generation failed: {exc}")
        return

    _set_sql_editor(sql)
    _append(
        "assistant",
        "Generated SQL from your request. Review it in the SQL Editor and "
        f"click **Run SQL** when ready.\n\n```sql\n{sql}\n```",
    )
    st.success("SQL inserted into the editor.")


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
    st.session_state.top_table_selector = table_name
    st.session_state.rename_table_name = table_name
    st.session_state.rename_table_source = table_name


def _open_table(table_name: str, *, announce: bool = False) -> None:
    try:
        df = _eng().mysql.fetch_table(table_name)
    except Exception as exc:
        result = ExecutionResult(
            success=False,
            error=str(exc),
            message=f"Couldn't open table `{table_name}`.",
        )
        _append("assistant", result.message, result)
        return

    sql = f"SELECT * FROM `{table_name}` LIMIT 500;"
    result = ExecutionResult(
        success=True,
        message=f"Opened table `{table_name}`.",
        data=df,
        sql_executed=sql,
        rows_affected=len(df),
    )
    _set_table_editor(table_name, df)
    _set_sql_editor(sql)
    if announce:
        _log_query(result)
        _append("assistant", result.message, result)


def _select_current_table() -> None:
    table_name = st.session_state.top_table_selector
    if table_name:
        _open_table(table_name, announce=True)
    else:
        st.session_state.current_table = ""
        st.session_state.table_editor_table = ""
        st.session_state.table_editor_df = pd.DataFrame()
        st.session_state.table_editor_version += 1
        st.session_state.sql_editor_text = ""
        st.session_state.blueprint = None


def _save_current_table() -> ExecutionResult:
    current = st.session_state.current_table
    if not current:
        return ExecutionResult(success=False, error="No table is currently open.")

    save_result = _eng().mysql.replace_table_data(
        current,
        st.session_state.table_editor_df,
    )
    _log_query(save_result)
    if save_result.success:
        _set_sql_editor(save_result.sql_executed)
        _append(
            "assistant",
            f"Saved your edits back to `{current}`.",
            save_result,
        )
        _load_table_editor(current)
    return save_result


def _add_blank_row(table_name: str) -> None:
    df = st.session_state.table_editor_df.copy()
    columns = list(df.columns)
    if not columns:
        columns = [
            column["name"]
            for column in _eng().mysql.get_table_columns(table_name)
            if column["name"].lower() != "id"
        ]
    blank_row = {column: None for column in columns}
    st.session_state.table_editor_df = pd.concat(
        [df, pd.DataFrame([blank_row])],
        ignore_index=True,
    )
    st.session_state.table_editor_version += 1


def _add_blank_column(table_name: str, column_name: str) -> str:
    del table_name  # reserved for future schema-aware column suggestions

    raw_name = column_name.strip().lower()
    safe_name = TableBlueprint()._safe_name(raw_name)
    if not safe_name:
        return "Enter a column name first."

    df = st.session_state.table_editor_df.copy()
    if safe_name in df.columns:
        return f"Column `{safe_name}` already exists."

    df[safe_name] = None
    st.session_state.table_editor_df = df
    st.session_state.table_editor_version += 1
    st.session_state.new_column_name = ""
    return ""


def _rename_current_table(new_name: str) -> ExecutionResult:
    current = st.session_state.current_table
    if not current:
        return ExecutionResult(success=False, error="No table is currently open.")

    result = _eng().mysql.rename_table(current, new_name)
    _log_query(result)
    if result.success:
        _append("assistant", result.message or "Table renamed.", result)
        _open_table(new_name, announce=False)
    return result


def _delete_current_table() -> ExecutionResult:
    current = st.session_state.current_table
    if not current:
        return ExecutionResult(success=False, error="No table is currently open.")

    result = _eng().mysql.drop_table(current)
    _log_query(result)
    if result.success:
        _append("assistant", result.message or "Table deleted.", result)
        st.session_state.current_table = ""
        st.session_state.table_editor_table = ""
        st.session_state.table_editor_df = pd.DataFrame()
        st.session_state.table_editor_version += 1
        st.session_state.sql_editor_text = ""
        st.session_state.blueprint = None
        st.session_state.confirm_delete_table = False
        remaining_tables = _eng().mysql.get_table_names()
        if remaining_tables:
            _open_table(remaining_tables[0], announce=False)
        else:
            st.session_state.top_table_selector = ""
            st.session_state.rename_table_name = ""
            st.session_state.rename_table_source = ""
    return result


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


def _end_conversation() -> None:
    _clear_all()
    st.session_state.current_table = ""
    st.session_state.table_editor_table = ""
    st.session_state.table_editor_df = pd.DataFrame()
    st.session_state.table_editor_version += 1
    st.session_state.prefill = ""
    st.session_state.blueprint = None
    st.session_state.top_table_selector = ""
    st.session_state.rename_table_name = ""
    st.session_state.rename_table_source = ""
    st.session_state.confirm_delete_table = False
    st.session_state.ai_nl_input = ""
    _eng().mysql.clear_context()


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