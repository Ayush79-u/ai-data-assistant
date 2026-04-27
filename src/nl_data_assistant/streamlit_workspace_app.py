"""
streamlit_workspace_app.py - MySQL terminal + assistant workspace.
"""
from __future__ import annotations

import io
import re
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from nl_data_assistant.models import ExecutionResult, Intent
from nl_data_assistant.services.engine import DataAssistantEngine

try:
    from streamlit_ace import st_ace
except ImportError:  # pragma: no cover - optional local dependency
    st_ace = None


SQL_KEYWORDS = [
    "SELECT",
    "SHOW",
    "INSERT INTO",
    "UPDATE",
    "DELETE FROM",
    "CREATE DATABASE",
    "CREATE TABLE",
    "ALTER TABLE",
    "DROP TABLE",
    "DROP DATABASE",
    "TRUNCATE TABLE",
    "DESCRIBE",
    "USE",
    "WHERE",
    "ORDER BY",
    "GROUP BY",
    "LIMIT",
    "JOIN",
]

SQL_COMMAND_PREFIXES = (
    "SELECT",
    "SHOW",
    "INSERT",
    "UPDATE",
    "DELETE",
    "CREATE",
    "ALTER",
    "DROP",
    "TRUNCATE",
    "DESCRIBE",
    "USE",
    "EXPLAIN",
    "WITH",
)

SYSTEM_DATABASES = {"information_schema", "mysql", "performance_schema", "sys"}


def run_streamlit_app() -> None:
    st.set_page_config(
        page_title="Local MySQL Workspace",
        page_icon="🗃️",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    _inject_css()
    _init_session()
    _render_status_banner()

    st.title("🗃️ Local MySQL Workspace")
    st.caption("A MySQL-style SQL console with persistent chat, table editing, and local session memory.")

    sidebar_col, main_col = st.columns([0.8, 2.2], gap="large")
    with sidebar_col:
        _render_sidebar()

    with main_col:
        sql_col, chat_col = st.columns([1.55, 1], gap="large")
        with sql_col:
            _render_sql_workspace()
        with chat_col:
            _render_chat_workspace()

        lower_tab_history, lower_tab_excel = st.tabs(["Command History", "Excel Tools"])
        with lower_tab_history:
            _render_query_history_panel()
        with lower_tab_excel:
            _render_excel_tools()


def _init_session() -> None:
    if "engine" not in st.session_state:
        st.session_state.engine = DataAssistantEngine()
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "query_log" not in st.session_state:
        st.session_state.query_log = []
    if "sql_history" not in st.session_state:
        st.session_state.sql_history = []
    if "sql_history_index" not in st.session_state:
        st.session_state.sql_history_index = -1
    if "sql_console_text" not in st.session_state:
        st.session_state.sql_console_text = ""
    if "sql_console_version" not in st.session_state:
        st.session_state.sql_console_version = 0
    if "last_query_result" not in st.session_state:
        st.session_state.last_query_result = None
    if "last_query_sql" not in st.session_state:
        st.session_state.last_query_sql = ""
    if "pending_plan" not in st.session_state:
        st.session_state.pending_plan = None
    if "current_database" not in st.session_state:
        st.session_state.current_database = _engine().mysql.current_database
    if "current_table" not in st.session_state:
        st.session_state.current_table = ""
    if "table_editor_df" not in st.session_state:
        st.session_state.table_editor_df = pd.DataFrame()
    if "table_editor_table" not in st.session_state:
        st.session_state.table_editor_table = ""
    if "table_editor_version" not in st.session_state:
        st.session_state.table_editor_version = 0
    if "status_banner" not in st.session_state:
        st.session_state.status_banner = None
    _sync_session_context()


def _engine() -> DataAssistantEngine:
    return st.session_state.engine


def _sync_session_context() -> None:
    mysql = _engine().mysql
    st.session_state.current_database = mysql.current_database
    current_table = st.session_state.current_table.strip()
    available_tables = {name.lower() for name in mysql.get_table_names()}
    if current_table and current_table.lower() not in available_tables:
        st.session_state.current_table = ""
        st.session_state.table_editor_table = ""
        st.session_state.table_editor_df = pd.DataFrame()


def _render_status_banner() -> None:
    banner = st.session_state.status_banner
    if not banner:
        return

    kind = banner.get("kind", "info")
    message = banner.get("message", "")
    if kind == "success":
        st.success(message)
    elif kind == "error":
        st.error(message)
    else:
        st.info(message)
    st.session_state.status_banner = None


def _render_sidebar() -> None:
    st.subheader("MySQL Explorer")
    mysql = _engine().mysql

    if mysql.ping():
        st.success("Connected to MySQL", icon="✅")
    else:
        st.error("MySQL connection failed. Check your `.env` settings.", icon="🔴")
        st.stop()

    col_db, col_refresh = st.columns([3, 1])
    with col_db:
        databases = mysql.get_database_names()
        current_db = st.session_state.current_database
        selected_database = st.selectbox(
            "Databases",
            options=[""] + databases,
            index=([""] + databases).index(current_db) if current_db in databases else 0,
            key="sidebar_database_select",
        )
    with col_refresh:
        if st.button("↻", use_container_width=True, help="Refresh databases and tables"):
            _sync_session_context()
            st.rerun()

    if selected_database and selected_database != current_db:
        result = mysql.use_database(selected_database)
        if result.success:
            st.session_state.current_database = mysql.current_database
            st.session_state.current_table = ""
            st.session_state.table_editor_table = ""
            st.session_state.table_editor_df = pd.DataFrame()
            _set_status("success", result.message)
        else:
            _set_status("error", result.error or result.message)
        st.rerun()

    current_db = st.session_state.current_database
    if current_db:
        st.caption(f"Current database: `{current_db}`")
        tables = mysql.get_table_names()
        selected_table = st.selectbox(
            "Tables",
            options=[""] + tables,
            index=([""] + tables).index(st.session_state.current_table)
            if st.session_state.current_table in tables
            else 0,
            key="sidebar_table_select",
        )
        if selected_table and selected_table != st.session_state.current_table:
            st.session_state.current_table = selected_table
            _refresh_table_editor(selected_table)
            st.rerun()

        if st.session_state.current_table:
            with st.expander("Selected table schema", expanded=False):
                for column in mysql.get_table_columns(st.session_state.current_table):
                    st.markdown(f"- `{column['name']}`: `{column['type']}`")

            _render_export_buttons(st.session_state.current_table)
    else:
        st.info("Run `CREATE DATABASE ...` or `USE database_name` in the SQL console to start.")

    with st.expander("Quick SQL examples", expanded=False):
        examples = [
            "CREATE DATABASE school;",
            "USE school;",
            "CREATE TABLE students (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), cgpa FLOAT);",
            "INSERT INTO students (name, cgpa) VALUES ('Ayush', 8.7);",
            "SELECT * FROM students ORDER BY cgpa DESC;",
            "SHOW TABLES;",
        ]
        for example in examples:
            if st.button(example, key=f"sidebar_example_{example}", use_container_width=True):
                _load_sql_into_console(example)
                st.rerun()


def _render_sql_workspace() -> None:
    st.subheader("SQL Console")

    if st.session_state.current_database:
        st.caption(f"Connected context: `{st.session_state.current_database}`")
    else:
        st.caption("No database selected yet. You can still run `SHOW DATABASES;` or `CREATE DATABASE ...`.")

    _render_sql_console_panel()
    _render_result_panel()
    _render_table_editor_panel()


def _render_sql_console_panel() -> None:
    col_run, col_prev, col_next, col_clear = st.columns([1.3, 0.9, 0.9, 1.1])
    if col_run.button("Run SQL", type="primary", use_container_width=True):
        _run_console_sql()
    if col_prev.button("↑ Prev", use_container_width=True):
        _load_previous_command()
        st.rerun()
    if col_next.button("↓ Next", use_container_width=True):
        _load_next_command()
        st.rerun()
    if col_clear.button("Clear SQL", use_container_width=True):
        _load_sql_into_console("")
        st.rerun()

    editor_key = f"sql_console_editor_{st.session_state.sql_console_version}"
    if st_ace is not None:
        ace_theme = "tomorrow_night_bright" if st.get_option("theme.base") == "dark" else "chrome"
        editor_text = st_ace(
            value=st.session_state.sql_console_text,
            language="sql",
            theme=ace_theme,
            key=editor_key,
            height=220,
            auto_update=True,
            font_size=14,
            wrap=True,
            show_gutter=True,
            keybinding="vscode",
        )
        st.session_state.sql_console_text = editor_text or st.session_state.sql_console_text
    else:
        st.info("Install `streamlit-ace` for syntax highlighting and a richer SQL editor. Using the built-in console for now.")
        st.session_state.sql_console_text = st.text_area(
            "SQL command console",
            value=st.session_state.sql_console_text,
            key=editor_key,
            height=220,
            placeholder="Type MySQL commands here, for example:\nSHOW DATABASES;\nUSE school;\nSELECT * FROM students;",
        )
        if st.session_state.sql_console_text.strip():
            st.code(st.session_state.sql_console_text, language="sql")

    suggestions = _suggest_sql_keywords(st.session_state.sql_console_text)
    if suggestions:
        st.caption("Suggestions")
        suggestion_cols = st.columns(min(len(suggestions), 4))
        for index, keyword in enumerate(suggestions[:8]):
            with suggestion_cols[index % len(suggestion_cols)]:
                if st.button(keyword, key=f"sql_suggestion_{keyword}_{index}", use_container_width=True):
                    _append_sql_keyword(keyword)
                    st.rerun()

    st.caption("History buttons and the command log below let you quickly reuse earlier SQL.")


def _render_result_panel() -> None:
    result: ExecutionResult | None = st.session_state.last_query_result
    if result is None:
        st.info("Run a SQL command to see results here.")
        return

    st.markdown("**Result**")
    if result.error:
        st.error(result.error)
    elif result.message:
        st.success(result.message)

    if result.sql_executed:
        st.code(result.sql_executed, language="sql")

    _render_execution_result(result)


def _render_table_editor_panel() -> None:
    with st.expander("Table Editor", expanded=bool(st.session_state.current_table)):
        current_table = st.session_state.current_table
        if not current_table:
            st.info("Pick a table from the sidebar or run a query that targets a table to edit data here.")
            return

        if st.session_state.table_editor_table != current_table:
            _refresh_table_editor(current_table)

        st.caption(f"Editing `{current_table}` in `{st.session_state.current_database}`")
        col_reload, col_save = st.columns([1, 1])
        if col_reload.button("Reload table", key="reload_editor_table", use_container_width=True):
            _refresh_table_editor(current_table)
            st.rerun()

        editor_key = f"table_editor_widget_{current_table}_{st.session_state.table_editor_version}"
        edited_df = st.data_editor(
            st.session_state.table_editor_df,
            key=editor_key,
            num_rows="dynamic",
            use_container_width=True,
        )
        st.session_state.table_editor_df = edited_df

        if col_save.button("Save table changes", key="save_editor_table", type="primary", use_container_width=True):
            result = _engine().mysql.replace_table_data(current_table, edited_df)
            if result.success:
                _log_query(result)
                st.session_state.last_query_result = result
                st.session_state.last_query_sql = result.sql_executed
                _append_message(
                    "assistant",
                    f"Saved the edited rows back to `{current_table}`.",
                    result,
                )
                _refresh_table_editor(current_table)
                _set_status("success", result.message)
                st.rerun()
            else:
                st.error(result.error or result.message)


def _render_chat_workspace() -> None:
    st.subheader("Assistant Chat")

    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
            result = message.get("result")
            if isinstance(result, ExecutionResult):
                _render_execution_result(result)

    if st.session_state.pending_plan is not None:
        plan = st.session_state.pending_plan
        st.warning(
            f"This will {plan.intent.value.replace('_', ' ')} on `{plan.table_name or st.session_state.current_table or 'the active table'}`.",
            icon="⚠️",
        )
        confirm_col, cancel_col = st.columns(2)
        if confirm_col.button("Confirm action", key="confirm_pending_plan", type="primary", use_container_width=True):
            result = _engine().execute(plan)
            _handle_nl_result(plan, result)
            st.session_state.pending_plan = None
            st.rerun()
        if cancel_col.button("Cancel", key="cancel_pending_plan", use_container_width=True):
            st.session_state.pending_plan = None
            st.rerun()

    control_col, info_col = st.columns([1.15, 1.85])
    with control_col:
        with st.popover("Clear Chat History"):
            choice = st.radio(
                "What should happen to MySQL data?",
                options=["Keep current MySQL data", "Reset current database data"],
                key="clear_chat_choice",
            )
            if st.button("Confirm clear", key="confirm_clear_chat", type="primary", use_container_width=True):
                _clear_chat_history(reset_sql_data=choice == "Reset current database data")
                st.rerun()
    with info_col:
        if st.session_state.current_database:
            st.caption(
                f"Chat context stays connected to `{st.session_state.current_database}`"
                + (f" → `{st.session_state.current_table}`" if st.session_state.current_table else "")
            )
        else:
            st.caption("Chat memory is active. SQL context will continue until you clear it.")

    prompt = st.chat_input("Ask for SQL help, or paste a MySQL command directly.")
    if not prompt:
        return

    _append_message("user", prompt)

    if _looks_like_sql(prompt):
        result = _execute_sql_command(prompt)
        _append_message("assistant", _result_summary(result), result)
        st.rerun()

    plan = _engine().parse(prompt, default_table=st.session_state.current_table)
    if plan.is_destructive:
        st.session_state.pending_plan = plan
        _append_message(
            "assistant",
            (
                f"I parsed this as `{plan.intent.value}` for "
                f"`{plan.table_name or st.session_state.current_table or 'the current table'}`. "
                "Please confirm the action below."
            ),
        )
        st.rerun()

    result = _engine().execute(plan)
    _handle_nl_result(plan, result)
    st.rerun()


def _render_query_history_panel() -> None:
    if not st.session_state.sql_history:
        st.info("No SQL commands run yet.")
        return

    st.markdown("**SQL Command History**")
    for index, command in enumerate(reversed(st.session_state.sql_history[-25:]), start=1):
        entry_key = f"history_{index}_{command}"
        with st.expander(command[:100] + ("..." if len(command) > 100 else ""), expanded=False):
            st.code(command, language="sql")
            col_load, col_run = st.columns(2)
            if col_load.button("Load into console", key=f"load_{entry_key}", use_container_width=True):
                _load_sql_into_console(command)
                st.rerun()
            if col_run.button("Run again", key=f"rerun_{entry_key}", use_container_width=True):
                result = _execute_sql_command(command)
                _append_message("assistant", _result_summary(result), result)
                st.rerun()

    sql_dump = "\n\n".join(
        f"-- {entry['ts']} {'OK' if entry['ok'] else 'FAILED'}\n{entry['sql']}"
        for entry in st.session_state.query_log
    )
    st.download_button(
        "Download command log as .sql",
        data=sql_dump,
        file_name="mysql_workspace_history.sql",
        mime="text/plain",
        use_container_width=True,
    )
def _execute_sql_command(sql: str) -> ExecutionResult:
    result = _engine().execute_raw(sql)
    _push_sql_history(sql)
    _log_query(result)
    st.session_state.last_query_result = result
    st.session_state.last_query_sql = sql
    _sync_context_after_sql(sql, result)
    return result


def _handle_nl_result(plan, result: ExecutionResult) -> None:
    _append_message("assistant", _result_summary(result), result)
    if result.sql_executed:
        _load_sql_into_console(result.sql_executed)
        _log_query(result)
        st.session_state.last_query_sql = result.sql_executed
    st.session_state.last_query_result = result

    if result.success and plan.table_name:
        st.session_state.current_table = plan.table_name
        if plan.intent == Intent.CREATE_TABLE and isinstance(result.data, pd.DataFrame):
            st.session_state.table_editor_df = result.data.copy()
            st.session_state.table_editor_table = plan.table_name
            st.session_state.table_editor_version += 1
        elif st.session_state.current_database and _engine().mysql.table_exists(plan.table_name):
            _refresh_table_editor(plan.table_name)
    _sync_session_context()


def _render_execution_result(result: ExecutionResult) -> None:
    if result is None or result.data is None:
        return
    if isinstance(result.data, pd.DataFrame):
        st.dataframe(result.data, use_container_width=True)
        _render_result_downloads(result.data)
        return

    try:
        import plotly.graph_objects as go
    except ImportError:  # pragma: no cover
        return
    if isinstance(result.data, go.Figure):
        st.plotly_chart(result.data, use_container_width=True)


def _result_summary(result: ExecutionResult) -> str:
    if not result.success:
        return result.error or result.message or "The command failed."
    if result.message:
        return result.message
    if result.sql_executed:
        return "SQL command executed."
    return "Done."


def _append_message(role: str, content: str, result: ExecutionResult | None = None) -> None:
    st.session_state.messages.append(
        {
            "role": role,
            "content": content,
            "result": result,
        }
    )


def _log_query(result: ExecutionResult) -> None:
    if not result.sql_executed:
        return
    st.session_state.query_log.append(
        {
            "ts": datetime.now().strftime("%H:%M:%S"),
            "sql": result.sql_executed,
            "ok": result.success,
        }
    )


def _push_sql_history(sql: str) -> None:
    command = sql.strip()
    if not command:
        return
    history = st.session_state.sql_history
    if not history or history[-1] != command:
        history.append(command)
    st.session_state.sql_history_index = len(history)


def _load_previous_command() -> None:
    history = st.session_state.sql_history
    if not history:
        return
    next_index = st.session_state.sql_history_index - 1
    if next_index < 0:
        next_index = 0
    st.session_state.sql_history_index = next_index
    _load_sql_into_console(history[next_index])


def _load_next_command() -> None:
    history = st.session_state.sql_history
    if not history:
        _load_sql_into_console("")
        return
    next_index = st.session_state.sql_history_index + 1
    if next_index >= len(history):
        st.session_state.sql_history_index = len(history)
        _load_sql_into_console("")
        return
    st.session_state.sql_history_index = next_index
    _load_sql_into_console(history[next_index])


def _load_sql_into_console(sql: str) -> None:
    st.session_state.sql_console_text = sql
    st.session_state.sql_console_version += 1


def _append_sql_keyword(keyword: str) -> None:
    current = st.session_state.sql_console_text.rstrip()
    spacer = " " if current and not current.endswith((" ", "\n")) else ""
    _load_sql_into_console(f"{current}{spacer}{keyword}")


def _looks_like_sql(text: str) -> bool:
    stripped = text.strip()
    if not stripped:
        return False
    upper = stripped.upper()
    return upper.startswith(SQL_COMMAND_PREFIXES)


def _suggest_sql_keywords(text: str) -> list[str]:
    stripped = text.strip()
    if not stripped:
        return SQL_KEYWORDS[:8]
    token_match = re.findall(r"[A-Za-z_]+", stripped.upper())
    prefix = ""
    if token_match and not stripped.endswith((" ", "\n")):
        prefix = token_match[-1]
    if not prefix:
        return SQL_KEYWORDS[:8]
    matches = [keyword for keyword in SQL_KEYWORDS if keyword.startswith(prefix)]
    return matches[:8] or SQL_KEYWORDS[:4]


def _sync_context_after_sql(sql: str, result: ExecutionResult) -> None:
    _sync_session_context()
    st.session_state.current_database = _engine().mysql.current_database

    normalized = sql.strip().upper()
    if normalized.startswith("USE "):
        st.session_state.current_table = ""
        st.session_state.table_editor_table = ""
        st.session_state.table_editor_df = pd.DataFrame()
        return

    table_name = _extract_table_name_from_sql(sql)
    if not table_name:
        return

    if normalized.startswith("DROP TABLE"):
        if st.session_state.current_table.lower() == table_name.lower():
            st.session_state.current_table = ""
            st.session_state.table_editor_table = ""
            st.session_state.table_editor_df = pd.DataFrame()
        return

    if result.success and st.session_state.current_database and _engine().mysql.table_exists(table_name):
        st.session_state.current_table = table_name
        try:
            _refresh_table_editor(table_name)
        except Exception:
            pass


def _extract_table_name_from_sql(sql: str) -> str:
    patterns = [
        r"CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+`?([a-zA-Z0-9_]+)`?",
        r"INSERT\s+INTO\s+`?([a-zA-Z0-9_]+)`?",
        r"UPDATE\s+`?([a-zA-Z0-9_]+)`?",
        r"DELETE\s+FROM\s+`?([a-zA-Z0-9_]+)`?",
        r"ALTER\s+TABLE\s+`?([a-zA-Z0-9_]+)`?",
        r"TRUNCATE\s+TABLE\s+`?([a-zA-Z0-9_]+)`?",
        r"DROP\s+TABLE(?:\s+IF\s+EXISTS)?\s+`?([a-zA-Z0-9_]+)`?",
        r"DESCRIBE\s+`?([a-zA-Z0-9_]+)`?",
        r"FROM\s+`?([a-zA-Z0-9_]+)`?",
        r"JOIN\s+`?([a-zA-Z0-9_]+)`?",
    ]
    for pattern in patterns:
        match = re.search(pattern, sql, re.IGNORECASE)
        if match:
            return match.group(1)
    return ""


def _refresh_table_editor(table_name: str) -> None:
    df = _engine().mysql.fetch_table(table_name)
    st.session_state.current_table = table_name
    st.session_state.table_editor_table = table_name
    st.session_state.table_editor_df = df.copy()
    st.session_state.table_editor_version += 1


def _clear_chat_history(*, reset_sql_data: bool) -> None:
    st.session_state.messages = []
    st.session_state.pending_plan = None
    _engine().clear_history()

    if reset_sql_data:
        result = _reset_current_database_data()
        if result.success:
            _set_status("success", result.message)
        else:
            _set_status("error", result.error or result.message)
    else:
        _set_status("success", "Chat history cleared. SQL data was kept.")


def _reset_current_database_data() -> ExecutionResult:
    current_db = st.session_state.current_database.strip()
    if not current_db:
        st.session_state.current_table = ""
        st.session_state.table_editor_table = ""
        st.session_state.table_editor_df = pd.DataFrame()
        return ExecutionResult(success=True, message="Chat history cleared. There was no active database to reset.")

    if current_db.lower() in SYSTEM_DATABASES:
        _engine().mysql.clear_context()
        st.session_state.current_database = _engine().mysql.current_database
        st.session_state.current_table = ""
        st.session_state.table_editor_table = ""
        st.session_state.table_editor_df = pd.DataFrame()
        return ExecutionResult(
            success=True,
            message=f"Chat history cleared. System database `{current_db}` was kept for safety.",
        )

    result = _engine().mysql.execute_sql(f"DROP DATABASE `{current_db}`;")
    if result.success:
        _engine().mysql.clear_context()
        st.session_state.current_database = _engine().mysql.current_database
        st.session_state.current_table = ""
        st.session_state.table_editor_table = ""
        st.session_state.table_editor_df = pd.DataFrame()
        st.session_state.last_query_result = None
        st.session_state.last_query_sql = ""
        st.session_state.sql_history = []
        st.session_state.sql_history_index = -1
        _load_sql_into_console("")
        return ExecutionResult(
            success=True,
            message=f"Chat history cleared and database `{current_db}` was dropped.",
        )
    return result


def _render_export_buttons(table_name: str) -> None:
    try:
        df = _engine().mysql.fetch_table(table_name)
    except Exception as exc:
        st.error(str(exc))
        return

    csv_data = df.to_csv(index=False).encode("utf-8")
    excel_buffer = io.BytesIO()
    df.to_excel(excel_buffer, index=False)
    st.download_button(
        "Download CSV",
        data=csv_data,
        file_name=f"{table_name}.csv",
        mime="text/csv",
        use_container_width=True,
        key=f"download_csv_{table_name}",
    )
    st.download_button(
        "Download Excel",
        data=excel_buffer.getvalue(),
        file_name=f"{table_name}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
        key=f"download_excel_{table_name}",
    )


def _render_result_downloads(df: pd.DataFrame) -> None:
    col_csv, col_excel = st.columns(2)
    with col_csv:
        st.download_button(
            "Result CSV",
            data=df.to_csv(index=False).encode("utf-8"),
            file_name="query_result.csv",
            mime="text/csv",
            use_container_width=True,
            key=f"result_csv_{len(df)}_{list(df.columns)}",
        )
    with col_excel:
        buffer = io.BytesIO()
        df.to_excel(buffer, index=False)
        st.download_button(
            "Result Excel",
            data=buffer.getvalue(),
            file_name="query_result.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            key=f"result_excel_{len(df)}_{list(df.columns)}",
        )


def _run_console_sql() -> None:
    command = st.session_state.sql_console_text.strip()
    if not command:
        _set_status("error", "Type a SQL command first.")
        return
    result = _execute_sql_command(command)
    if result.success:
        _set_status("success", result.message)
    else:
        _set_status("error", result.error or result.message)


def _set_status(kind: str, message: str) -> None:
    st.session_state.status_banner = {"kind": kind, "message": message}


def _render_excel_tools() -> None:
    st.markdown("**Excel Import / Export**")

    uploaded = st.file_uploader("Upload an Excel file", type=["xlsx", "xls"])
    if uploaded and st.session_state.current_database:
        excel_service = _engine().excel
        tmp = Path(tempfile.gettempdir()) / uploaded.name
        tmp.write_bytes(uploaded.read())

        sheets = excel_service.list_sheets(tmp)
        sheet = st.selectbox("Sheet", sheets, key="excel_sheet_select")
        df = excel_service.read_sheet(tmp, sheet)
        st.dataframe(df, use_container_width=True)

        table_name = st.text_input("Import into table", value=sheet.lower(), key="excel_import_table")
        if st.button("Import sheet into current database", key="excel_import_btn", use_container_width=True):
            with st.spinner("Importing..."):
                result = _engine().mysql.import_dataframe(table_name, df, if_exists="replace")
            if result.success:
                _log_query(result)
                st.session_state.last_query_result = result
                st.session_state.last_query_sql = result.sql_executed
                _set_status("success", result.message)
            else:
                _set_status("error", result.error or result.message)
            st.rerun()

    if not st.session_state.current_database:
        st.info("Select a database first if you want to import Excel sheets into MySQL.")


def _inject_css() -> None:
    st.markdown(
        """
        <style>
            .stButton > button {
                border-radius: 10px;
                border: 1px solid rgba(128, 128, 128, 0.28);
            }
            .stChatMessage {
                border-radius: 14px;
                border: 1px solid rgba(128, 128, 128, 0.18);
            }
            code {
                font-size: 13px;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )
