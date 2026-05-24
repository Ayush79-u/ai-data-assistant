"""
streamlit_app.py — Simple, friendly UI for the AI Data Assistant.

Features:
- Removed "Table options" sidebar block.
- Local-first SQL generation with Groq `llama-3.3-70b-versatile` fallback inside the SQL Editor.
- DROP/TRUNCATE safety guard before any SQL run.
- "✏️ Edit these results" button on chat result tables — loads them into
  the Table Editor for inline editing.
- Two save modes in the Table Editor:
    • "💾 Save edits only"  — updates rows by id (safe for filtered results)
    • "Save to MySQL (replace all)" — old replace-all behavior
- All other features preserved: chat, Excel import/export, blueprint builder,
  query history, destructive confirmation, etc.
"""
from __future__ import annotations

import io
import logging
import os
import re
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

log = logging.getLogger(__name__)

from nl_data_assistant.models import ExecutionResult, Intent
from nl_data_assistant.nlp.ai_sql_generator import generate_sql, is_safe_sql
from nl_data_assistant.nlp.schema_context import SchemaContext
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
        "pending_table_select": None,
        "rename_table_name":    "",
        "rename_table_source":  "",
        "new_column_name":      "",
        "new_column_source":    "",
        "confirm_delete_table": False,
        "ai_nl_input":          "",
        "auto_run_generated_sql": True,
        "schema_ctx":           None,   # cached SchemaContext for AI generation
        "schema_ctx_table":     "",     # which table the cache is for
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
    # Table selection is available via the toolbar selectbox in the main area.

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
                    _queue_table_selection(blueprint["table_name"])
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
                import uuid as _uuid2
                out = Path(tempfile.gettempdir()) / ("export_" + _uuid2.uuid4().hex + ".xlsx")
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

    for idx, turn in enumerate(st.session_state.chat):
        with st.chat_message(turn["role"]):
            st.markdown(turn["content"])
            result: ExecutionResult | None = turn.get("result")

            if result and result.data is not None:

                if isinstance(result.data, pd.DataFrame) and not result.data.empty:

                    # ✅ FIX: use stable unique keys
                    edit_key = f"edit_mode_{idx}"
                    editor_key = f"editor_{idx}"

                    # =========================
                    # ✏️ EDIT MODE
                    # =========================
                    if st.session_state.get(edit_key, False):

                        edited_df = st.data_editor(
                            result.data,
                            key=editor_key,
                            use_container_width=True,
                            num_rows="dynamic"
                        )



                        col1, col2, col3 = st.columns(3)

                        # 💾 Save edits only (DB update)
                        if col1.button("💾 Save edits only", key=f"save_{idx}"):

                            target_tbl = _infer_table_from_sql(result.sql_executed or "")

                            if target_tbl:
                                save_result = _eng().mysql.update_rows_by_id(
                                    target_tbl,
                                    edited_df,

                                )

                                if save_result.success:
                                    st.success(save_result.message or "Saved your changes.")
                                    st.rerun()
                                else:
                                    st.error(save_result.error or "Save failed")

                        # 🚀 Replace all
                        if col2.button("🚀 Save to MySQL (replace all)", key=f"replace_{idx}"):

                            target_tbl = _infer_table_from_sql(result.sql_executed or "")

                            if target_tbl:
                                save_result = _eng().mysql.replace_table_data(target_tbl, edited_df)

                                if save_result.success:
                                    st.success("✅ Table fully replaced in MySQL")
                                    st.rerun()
                                else:
                                    st.error(save_result.error or "Replace failed")

                        # ❌ Cancel
                        if col3.button("❌ Cancel", key=f"cancel_{idx}"):
                            st.session_state[edit_key] = False
                            st.rerun()

                    # =========================
                    # 📊 NORMAL VIEW
                    # =========================
                    else:
                        st.dataframe(result.data, use_container_width=True, hide_index=True)

                    # =========================
                    # DOWNLOAD + EDIT BUTTONS
                    # =========================
                    btn_col1, btn_col2 = st.columns(2)

                    buf = io.BytesIO()
                    result.data.to_excel(buf, index=False)

                    btn_col1.download_button(
                        "⬇️ Download as Excel",
                        data=buf.getvalue(),
                        file_name="result.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key=f"dl_{idx}",
                        use_container_width=True,
                    )

                    target_tbl = _infer_table_from_sql(result.sql_executed or "")

                    if target_tbl:
                        if btn_col2.button(
                            "✏️ Edit these results",
                            key=f"edit_btn_{idx}",
                            use_container_width=True,
                        ):
                            st.session_state[edit_key] = True
                            st.rerun()

                elif isinstance(result.data, go.Figure):
                    st.plotly_chart(result.data, use_container_width=True)

            if result and result.error:
                log.error("Result error (not shown to user): %s", result.error)
                st.error("An error occurred. Please try again.")

    # =========================
    # DESTRUCTIVE CONFIRMATION
    # =========================
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
    _apply_pending_table_selection(
        selector_options,
        fallback=current if current in tables else "",
    )

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

            c_reload, c_add_row, c_save_edits, c_save_all = st.columns(4)
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

            if c_save_edits.button(
                "💾 Save edits only",
                use_container_width=True,
                key=f"save_edits_{current}",
                help="Updates the rows shown here using a safe unique column and "
                     "can insert new rows when the table has a usable identifier.",
            ):
                save_result = _save_edits_only()
                if save_result.success:
                    st.success(save_result.message)
                    st.rerun()
                st.error(save_result.error or "Save failed.")

            if c_save_all.button(
                "Save to MySQL (replace all)",
                type="primary",
                use_container_width=True,
                key=f"save_editor_{current}",
                help="⚠️ Replaces ALL rows in the table with what's in the editor.",
            ):
                save_result = _save_current_table()
                if save_result.success:
                    st.rerun()
                st.error(save_result.error or "Save failed.")

    # SQL Editor - always available (also exposes the Groq fallback generator)
    if st.session_state.sql_editor_text or st.session_state.current_table:
        with st.expander("SQL Editor - tweak and re-run", expanded=False):

            st.markdown("**Generate SQL from English**")
            ai_col_input, ai_col_btn, ai_col_toggle = st.columns([4, 1.4, 1.3])
            ai_col_input.text_input(
                "Ask in English",
                key="ai_nl_input",
                placeholder="e.g. show top 5 rows ordered by cgpa desc",
                label_visibility="collapsed",
            )
            if ai_col_btn.button(
                "Generate SQL",
                use_container_width=True,
                key="ai_generate_sql_button",
            ):
                should_rerun = _generate_sql_with_ai(
                    st.session_state.get("ai_nl_input", ""),
                    auto_run=st.session_state.auto_run_generated_sql,
                )
                if should_rerun:
                    st.rerun()
            ai_col_toggle.checkbox(
                "Auto-run",
                key="auto_run_generated_sql",
                help="Automatically execute generated read-only SQL such as SELECT and SHOW.",
            )
            st.caption(
                "Local generation runs first and fills the SQL editor. "
                "Read-only SQL can be executed automatically."
            )

            # Schema context badge ──────────────────────────────────────────
            cached_ctx: SchemaContext | None = st.session_state.get("schema_ctx")
            if cached_ctx and not cached_ctx.is_empty():
                tbl_list = ", ".join(f"`{t}`" for t in cached_ctx.table_names[:4])
                if len(cached_ctx.table_names) > 4:
                    tbl_list += f" +{len(cached_ctx.table_names) - 4} more"
                st.success(
                    f"🔍 Schema loaded — {len(cached_ctx.table_names)} table(s): {tbl_list}",
                    icon="✅",
                )
            elif st.session_state.current_table:
                st.info("Schema will be loaded when you click Generate SQL.", icon="ℹ️")

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
                result = _execute_sql_from_text(
                    sql,
                    intro="Executed the SQL from the editor.",
                )
                if result.success:
                    st.rerun()
                st.error(result.error or "SQL execution failed.")
            if c2.button("Clear SQL", use_container_width=True, key="clear_sql_editor_button"):
                st.session_state.sql_editor_text = ""
                st.rerun()


# ── Chat input + processing ───────────────────────────────────────────────────

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

def _generate_sql_with_ai(nl_input: str, *, auto_run: bool = False) -> bool:
    """Generate SQL locally first, then optionally auto-run safe read-only queries."""
    if not nl_input.strip():
        st.warning("Type what you want in plain English first.")
        return False

    # Security: limit input length to prevent API abuse / ReDoS
    if len(nl_input) > 2000:
        st.error("Input too long. Please keep your request under 2000 characters.")
        return False

    try:
        sql, generation_note = _generate_sql_from_request(nl_input)
    except Exception as exc:
        st.error(f"SQL generation failed: {exc}")
        return False

    _set_sql_editor(sql)

    if auto_run and _is_read_only_sql(sql):
        intro = "Generated SQL from your request and executed it automatically."
        if generation_note:
            intro += f" {generation_note}"
        result = _execute_sql_from_text(
            sql,
            intro=intro,
        )
        if not result.success:
            st.error(result.error or "SQL execution failed.")
        return True

    message = "Generated SQL from your request."
    if auto_run and not _is_read_only_sql(sql):
        message += " It changes data, so it was added to the SQL Editor for manual review."
    else:
        message += " Review it in the SQL Editor and click **Run SQL** when ready."
    if generation_note:
        message += f"\n\n_{generation_note}_"

    _append("assistant", f"{message}\n\n```sql\n{sql}\n```")
    st.success("SQL inserted into the editor.")
    return True


def _generate_sql_from_request(nl_input: str) -> tuple[str, str]:
    """
    Generate SQL from a natural-language request.

    Priority order (schema-aware first):
      1. Build SchemaContext from the active DB (cached, fast on repeat calls).
      2. If Groq API key is present  -> call Groq with the FULL schema context
         so it sees exact column names, types, sample values, and categorical
         values.  This is the primary path for all natural-language queries.
      3. If Groq fails or is unavailable -> fall back to the local rule-based
         parser (structural queries like SHOW / DESCRIBE still work offline).

    Why this order?
    The local parser does NOT know actual categorical values (e.g. 'HR', 'IT'),
    so it produces weak SQL like  LOWER(dept) = 'hr department'  instead of
    the correct  WHERE Department = 'HR'.  Groq with schema context produces
    accurate, case-matched SQL.
    """
    current = st.session_state.current_table
    data_source = "excel" if st.session_state.get("excel_upload") is not None else "mysql"
    groq_api_key = os.getenv("GROQ_API_KEY", "").strip()

    # ── 1. Build schema context (session-level + service-level TTL cache) ──────
    schema_ctx: SchemaContext | None = None
    try:
        cached_ctx: SchemaContext | None = st.session_state.get("schema_ctx")
        cached_tbl: str = st.session_state.get("schema_ctx_table", "")

        if cached_ctx is not None and cached_tbl == current and not cached_ctx.is_empty():
            schema_ctx = cached_ctx
        else:
            hints = [current] if current else None
            schema_ctx = _eng().mysql.build_ai_schema_context(table_hints=hints)
            st.session_state.schema_ctx       = schema_ctx
            st.session_state.schema_ctx_table = current
    except Exception as exc:
        log.warning("Could not build schema context: %s", exc)
        schema_ctx = None

    # ── 2. Groq with full schema context (PRIMARY path) ────────────────────
    if groq_api_key:
        try:
            sql = generate_sql(
                user_input=nl_input,
                schema=schema_ctx,
                table_name=current,
                data_source=data_source,
            )
            tbl_count = len(schema_ctx.table_names) if schema_ctx and not schema_ctx.is_empty() else 0
            note = (
                f"Used schema-aware AI generation ({tbl_count} table(s) in context)."
                if tbl_count
                else "Used AI generation (no schema context available)."
            )
            return sql, note
        except ValueError as exc:
            # Hard block (hallucination guard / safety) — surface immediately
            raise ValueError(str(exc)) from exc
        except Exception as exc:
            log.warning("Groq generation failed, falling back to local parser: %s", exc)
            groq_error = str(exc)
    else:
        groq_error = "GROQ_API_KEY not set."

    # -- 3. Local rule-based parser (FALLBACK when Groq unavailable/failed) ---
    local_sql = ""
    local_error = ""
    try:
        plan = _eng().preview_plan(nl_input, default_table=current)
        if plan.intent != Intent.UNKNOWN:
            local_sql = _eng().mysql.preview_plan_sql(plan)
    except Exception as local_exc:
        local_error = str(local_exc)

    if local_sql:
        note = "Used the local SQL generator."
        if groq_error:
            note += f" (Groq unavailable: {groq_error})"
        return local_sql, note

    # Both paths failed - raise a helpful error
    raise RuntimeError(
        f"Could not generate SQL. "
        "", # error details logged, not shown  #
        f"Local parser: {local_error or 'could not understand the request'}. "
        "Please try rephrasing your request."
    )


# ── Inline-edit-results helpers ───────────────────────────────────────────────

def _infer_table_from_sql(sql: str) -> str:
    """Extract the first referenced table name if it exists in MySQL."""
    if not sql:
        return ""
    match = re.search(
        r"\b(?:FROM|INTO|UPDATE|DESCRIBE|TABLE)\s+`?([a-zA-Z_][a-zA-Z0-9_]*)`?",
        sql,
        re.IGNORECASE,
    )
    if not match:
        return ""
    table = match.group(1).lower()
    try:
        existing = {t.lower() for t in _eng().mysql.get_table_names()}
        return table if table in existing else ""
    except Exception:
        return ""


def _is_read_only_sql(sql: str) -> bool:
    first_word = sql.strip().split()[0].upper() if sql.strip() else ""
    return first_word in {"SELECT", "SHOW", "DESCRIBE", "EXPLAIN", "WITH"}


def _execute_sql_from_text(sql: str, *, intro: str = "") -> ExecutionResult:
    sql = (sql or "").strip()
    if not sql:
        return ExecutionResult(success=False, error="Enter a SQL command first.")

    if not is_safe_sql(sql):
        return ExecutionResult(
            success=False,
            error="Blocked: queries containing DROP or TRUNCATE are not allowed.",
            sql_executed=sql,
        )

    result = _eng().execute_raw(sql)
    _log_query(result)
    _set_sql_editor(sql)
    _sync_after_sql_execution(sql, result)

    message_parts: list[str] = []
    if intro:
        message_parts.append(intro)
    message_parts.append(f"```sql\n{sql}\n```")
    if result.success:
        message_parts.append(_friendly_reply(result))
    else:
        log.error("SQL execution error (not shown to user): %s", result.error or result.message)
        message_parts.append("Something went wrong. Please check your input and try again.")
    _append("assistant", "\n\n".join(message_parts), result)
    return result


def _sync_after_sql_execution(sql: str, result: ExecutionResult) -> None:
    if not result.success:
        return

    target_table = _infer_table_from_sql(result.sql_executed or sql)
    if not target_table:
        return

    st.session_state.current_table = target_table
    _queue_table_selection(target_table)
    try:
        _load_table_editor(target_table)
    except Exception:
        pass


def _edit_query_results(table_name: str, df: pd.DataFrame) -> None:
    """Load query results into the Table Editor for inline editing."""
    _set_table_editor(table_name, df)
    _append(
        "assistant",
        f"Loaded these rows into the **Table Editor** below for `{table_name}`. "
        "Edit any cell, then choose how to save:\n\n"
        "• **💾 Save edits only** — updates the rows you see here using a safe unique column. "
        "Use this when you filtered (e.g. only HR department).\n"
        "• **Save to MySQL (replace all)** — replaces ALL rows in the table with the editor contents. "
        "Use this only when the editor holds the full table.",
    )


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


def _queue_table_selection(table_name: str) -> None:
    st.session_state.pending_table_select = table_name or ""


def _apply_pending_table_selection(options: list[str], *, fallback: str = "") -> None:
    """
    Apply any pending programmatic table selection before the selectbox is created.

    This keeps Streamlit widget state in sync without mutating the widget key
    later in the render cycle.
    """
    desired = st.session_state.get("pending_table_select")
    if desired is None:
        desired = st.session_state.get("top_table_selector", "")
        if desired not in options:
            desired = fallback if fallback in options else ""
    else:
        desired = desired if desired in options else (fallback if fallback in options else "")

    st.session_state.top_table_selector = desired
    st.session_state.pending_table_select = None


def _set_table_editor(table_name: str, df: pd.DataFrame) -> None:
    st.session_state.current_table = table_name
    st.session_state.table_editor_table = table_name
    st.session_state.table_editor_df = df.copy()
    st.session_state.table_editor_version += 1


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
    _queue_table_selection(table_name)
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
    """Replace all rows of the current table with the editor contents."""
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
            f"Saved your edits back to `{current}` (replace mode).",
            save_result,
        )
        _load_table_editor(current)
    return save_result


def _save_edits_only() -> ExecutionResult:
    """Update or insert only the rows currently shown in the editor."""
    current = st.session_state.current_table
    if not current:
        return ExecutionResult(success=False, error="No table is currently open.")

    save_result = _eng().mysql.update_rows_by_id(
        current,
        st.session_state.table_editor_df,
    )
    _log_query(save_result)
    if save_result.success:
        _set_sql_editor(save_result.sql_executed)
        _append(
            "assistant",
            save_result.message or f"Saved editor changes to `{current}`.",
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
        _queue_table_selection(new_name)
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
            _queue_table_selection("")
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
    _queue_table_selection("")
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
