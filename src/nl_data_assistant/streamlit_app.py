from __future__ import annotations

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


def run_streamlit_app() -> None:
    st.set_page_config(page_title="AI Data Assistant Chat", layout="wide")
    _inject_styles()
    _bootstrap_state()

    base_config = AppConfig.from_env(Path.cwd())
    config, default_target = _render_sidebar(base_config)
    engine = DataAssistantEngine(config)

    uploaded_file_path = _render_header(config, engine)
    render_chat_ui(engine, default_target, uploaded_file_path)


def render_chat_ui(engine: DataAssistantEngine, default_target: str, uploaded_file_path: str | None) -> None:
    chat_col, side_col = st.columns([2.1, 1], gap="large")

    with chat_col:
        _render_messages()
        user_input = st.chat_input("Ask me to create a table, add rows, show data, or save to MySQL")
        if user_input:
            handle_user_input(engine, user_input, default_target, uploaded_file_path)
            st.rerun()

        if st.session_state.blueprint is not None:
            _render_editor_panel()

    with side_col:
        _render_context_panel(engine)


def handle_user_input(
    engine: DataAssistantEngine,
    user_input: str,
    default_target: str,
    uploaded_file_path: str | None,
) -> None:
    cleaned_input = user_input.strip()
    if not cleaned_input:
        return

    st.session_state.messages.append({"role": "user", "content": cleaned_input})
    response = process_command(engine, cleaned_input, default_target, uploaded_file_path)
    st.session_state.messages.append(response)


def process_command(
    engine: DataAssistantEngine,
    user_input: str,
    default_target: str,
    uploaded_file_path: str | None,
) -> dict[str, Any]:
    lowered = user_input.lower().strip()

    if any(token in lowered for token in ("clear chat", "reset chat")):
        _reset_chat_state(keep_config=True)
        return _assistant_message("Chat cleared. We can start fresh.", status="success")

    if _is_create_table_request(user_input):
        return _handle_blueprint_creation(user_input)

    if _is_add_rows_request(lowered) and st.session_state.blueprint is not None:
        return _handle_add_rows_to_blueprint(user_input)

    if _is_show_data_request(lowered) and st.session_state.blueprint is not None:
        return _assistant_message(
            f"Here is the current editable data for `{st.session_state.current_table}`.",
            metadata={"table_name": st.session_state.current_table},
            show_editor=True,
            status="success",
        )

    if _is_save_request(lowered):
        return save_to_mysql(engine)

    result = engine.run(
        user_input,
        context=st.session_state.messages,
        default_target=default_target,
        uploaded_file_path=uploaded_file_path,
        current_dataframe=st.session_state.current_dataframe,
        current_table=st.session_state.current_table,
    )
    return _assistant_message_from_result(result)


def save_to_mysql(engine: DataAssistantEngine) -> dict[str, Any]:
    if st.session_state.blueprint is None or st.session_state.editor_df is None:
        return _assistant_message("There is no editable table to save yet. Create a table first.", status="error")

    if not engine.mysql_service.is_configured:
        return _assistant_message("MySQL is not configured. Fill in the connection settings in the sidebar first.", status="error")

    dataframe = st.session_state.editor_df.copy()
    blueprint = st.session_state.blueprint
    columns = [
        ColumnSpec(name=column["name"], data_type=column["type"], nullable=True)
        for column in blueprint["columns"]
    ]

    try:
        engine.mysql_service.create_table(blueprint["table_name"], columns)
        engine.mysql_service.write_dataframe(dataframe, blueprint["table_name"], if_exists="append")
        st.session_state.current_dataframe = dataframe
        return _assistant_message(
            f"Saved {len(dataframe)} row(s) to MySQL table `{blueprint['table_name']}`.",
            metadata={"table_name": blueprint["table_name"], "rows": len(dataframe)},
            status="success",
        )
    except Exception as exc:
        return _assistant_message(f"Could not save to MySQL: {exc}", status="error")


def _handle_blueprint_creation(user_input: str) -> dict[str, Any]:
    try:
        blueprint = command_to_blueprint(user_input)
    except Exception as exc:
        return _assistant_message(f"I could not build a table blueprint from that request: {exc}", status="error")

    dataframe = pd.DataFrame(blueprint["sample_data"])
    st.session_state.blueprint = blueprint
    st.session_state.current_table = blueprint["table_name"]
    st.session_state.current_dataframe = dataframe
    st.session_state.editor_df = dataframe.copy()

    summary = (
        f"Prepared a table blueprint for `{blueprint['table_name']}` with "
        f"{len(blueprint['columns'])} column(s) and {len(blueprint['sample_data'])} sample row(s). "
        "You can edit the data below and then save it to MySQL."
    )
    return _assistant_message(
        summary,
        metadata={"table_name": blueprint["table_name"], "blueprint": blueprint},
        blueprint=blueprint,
        show_editor=True,
        status="success",
    )


def _handle_add_rows_to_blueprint(user_input: str) -> dict[str, Any]:
    blueprint = st.session_state.blueprint
    count = extract_count(user_input)
    columns = [ColumnSpec(name=column["name"], data_type=column["type"], nullable=True) for column in blueprint["columns"]]
    new_rows = build_sample_rows(columns, count)
    new_dataframe = pd.concat([st.session_state.editor_df, pd.DataFrame(new_rows)], ignore_index=True)
    st.session_state.editor_df = new_dataframe
    st.session_state.current_dataframe = new_dataframe

    return _assistant_message(
        f"Added {count} new sample row(s) to `{blueprint['table_name']}`. You can edit them before saving.",
        metadata={"table_name": blueprint["table_name"], "rows_added": count},
        show_editor=True,
        status="success",
    )


def _assistant_message_from_result(result: ExecutionResult) -> dict[str, Any]:
    if result.dataframe is not None:
        st.session_state.current_dataframe = result.dataframe
    if result.plan.table_name:
        st.session_state.current_table = result.plan.table_name

    metadata = dict(result.metadata)
    if result.plan.table_name:
        metadata["table_name"] = result.plan.table_name
    if result.figure is not None:
        metadata["chart"] = result.figure
    if result.dataframe is not None:
        metadata["dataframe"] = result.dataframe
    if result.file_path:
        metadata["file_path"] = result.file_path

    return _assistant_message(
        result.message,
        metadata=metadata,
        status="success" if result.success else "error",
    )


def _assistant_message(
    content: str,
    metadata: dict[str, Any] | None = None,
    blueprint: dict[str, Any] | None = None,
    show_editor: bool = False,
    status: str = "info",
) -> dict[str, Any]:
    payload = {
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


def _render_messages() -> None:
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            _render_status_badge(message.get("status", "info"))
            st.markdown(message["content"])

            if message["role"] == "assistant":
                if message.get("blueprint"):
                    st.json(message["blueprint"])
                metadata = message.get("metadata", {})
                if metadata.get("dataframe") is not None:
                    st.dataframe(metadata["dataframe"], use_container_width=True)
                if metadata.get("chart") is not None:
                    st.plotly_chart(metadata["chart"], use_container_width=True)
                if metadata.get("sql"):
                    st.code(metadata["sql"], language="sql")
                if metadata.get("file_path"):
                    st.caption(metadata["file_path"])
                if message.get("show_editor") and st.session_state.blueprint is not None:
                    st.caption("The editable table appears below the chat.")


def _render_editor_panel() -> None:
    st.markdown("### Editable Table")
    st.caption("Edit the generated rows, then save them into MySQL when you are happy with the data.")

    edited_df = st.data_editor(
        st.session_state.editor_df,
        use_container_width=True,
        num_rows="dynamic",
        key="table_editor",
    )
    st.session_state.editor_df = edited_df
    st.session_state.current_dataframe = edited_df


def _render_context_panel(engine: DataAssistantEngine) -> None:
    st.markdown("### Session Memory")
    current_table = st.session_state.current_table or "None"
    st.metric("Current Table", current_table)
    st.metric("Messages", len(st.session_state.messages))
    row_count = 0 if st.session_state.editor_df is None else len(st.session_state.editor_df)
    st.metric("Editable Rows", row_count)

    button_col, clear_col = st.columns(2, gap="small")
    with button_col:
        if st.button("Save to MySQL", use_container_width=True, disabled=st.session_state.blueprint is None):
            response = save_to_mysql(engine)
            st.session_state.messages.append(response)
            st.rerun()
    with clear_col:
        if st.button("Clear Chat", use_container_width=True):
            _reset_chat_state(keep_config=True)
            st.rerun()

    if st.session_state.blueprint is not None:
        st.markdown("### Current Blueprint")
        st.json(st.session_state.blueprint)

    st.markdown("### Quick Prompts")
    for example in EXAMPLE_COMMANDS[:6]:
        if st.button(example, key=f"example_{example}", use_container_width=True):
            handle_user_input(
                DataAssistantEngine(_config_from_session()),
                example,
                st.session_state.default_target,
                st.session_state.uploaded_file_path,
            )
            st.rerun()


def _render_sidebar(base_config: AppConfig) -> tuple[AppConfig, str]:
    with st.sidebar:
        st.markdown("## Connection")
        mysql_host = st.text_input("MySQL host", value=base_config.mysql_host)
        mysql_port = st.number_input("MySQL port", value=base_config.mysql_port, step=1)
        mysql_user = st.text_input("MySQL user", value=base_config.mysql_user)
        mysql_password = st.text_input("MySQL password", value=base_config.mysql_password, type="password")
        mysql_database = st.text_input("MySQL database", value=base_config.mysql_database)
        default_target = st.selectbox("Default target", ["mysql", "excel", "auto"], index=0)
        st.caption("This chat workflow stays local and uses the built-in parser and blueprint system.")

    config = AppConfig.from_env(Path.cwd())
    config.mysql_host = mysql_host
    config.mysql_port = int(mysql_port)
    config.mysql_user = mysql_user
    config.mysql_password = mysql_password
    config.mysql_database = mysql_database
    config.openai_api_key = ""
    config.openai_model = ""

    st.session_state.default_target = default_target
    st.session_state.config_values = {
        "mysql_host": mysql_host,
        "mysql_port": int(mysql_port),
        "mysql_user": mysql_user,
        "mysql_password": mysql_password,
        "mysql_database": mysql_database,
    }
    return config, default_target


def _render_header(config: AppConfig, engine: DataAssistantEngine) -> str | None:
    st.title("AI Data Assistant Chat")
    st.caption("Chat with your data workflow. Create blueprints, edit rows, save to MySQL, and run follow-up commands in the same session.")

    header_left, header_right = st.columns([1.3, 1], gap="large")
    uploaded_file_path = None

    with header_left:
        uploaded_file = st.file_uploader("Upload an Excel file for context", type=["xlsx", "xls"])
        if uploaded_file:
            saved = _save_uploaded_file(uploaded_file, config.upload_dir)
            uploaded_file_path = str(saved)
            st.success(f"Workbook ready: {saved.name}")

    with header_right:
        st.metric("MySQL", "Configured" if engine.mysql_service.is_configured else "Not configured")
        st.metric("Known tables", len(engine.get_mysql_schema_catalog()))

    st.session_state.uploaded_file_path = uploaded_file_path
    return uploaded_file_path


def _bootstrap_state() -> None:
    st.session_state.setdefault("messages", [])
    st.session_state.setdefault("current_table", None)
    st.session_state.setdefault("blueprint", None)
    st.session_state.setdefault("current_dataframe", None)
    st.session_state.setdefault("editor_df", None)
    st.session_state.setdefault("uploaded_file_path", None)
    st.session_state.setdefault("default_target", "mysql")
    st.session_state.setdefault("config_values", {})

    if not st.session_state.messages:
        st.session_state.messages = [
            {
                "role": "assistant",
                "content": (
                    "I'm ready. Try something like `Create a table of students with name and CGPA`, "
                    "`add 3 rows`, `show data`, or `save to mysql`."
                ),
                "status": "info",
                "metadata": {},
            }
        ]


def _reset_chat_state(keep_config: bool = True) -> None:
    config_values = st.session_state.get("config_values", {}) if keep_config else {}
    default_target = st.session_state.get("default_target", "mysql") if keep_config else "mysql"
    uploaded_file_path = st.session_state.get("uploaded_file_path") if keep_config else None

    for key in ("messages", "current_table", "blueprint", "current_dataframe", "editor_df"):
        if key in st.session_state:
            del st.session_state[key]

    _bootstrap_state()
    st.session_state.config_values = config_values
    st.session_state.default_target = default_target
    st.session_state.uploaded_file_path = uploaded_file_path


def _config_from_session() -> AppConfig:
    config = AppConfig.from_env(Path.cwd())
    values = st.session_state.get("config_values", {})
    config.mysql_host = values.get("mysql_host", config.mysql_host)
    config.mysql_port = values.get("mysql_port", config.mysql_port)
    config.mysql_user = values.get("mysql_user", config.mysql_user)
    config.mysql_password = values.get("mysql_password", config.mysql_password)
    config.mysql_database = values.get("mysql_database", config.mysql_database)
    config.openai_api_key = ""
    config.openai_model = ""
    return config


def _render_status_badge(status: str) -> None:
    normalized = status if status in {"success", "error", "info"} else "info"
    badge_color = {
        "success": ("rgba(22,163,74,0.14)", "#166534"),
        "error": ("rgba(220,38,38,0.12)", "#991b1b"),
        "info": ("rgba(15,118,110,0.12)", "#0f766e"),
    }[normalized]
    st.markdown(
        (
            f"<span style='display:inline-block;padding:0.18rem 0.55rem;border-radius:999px;"
            f"background:{badge_color[0]};color:{badge_color[1]};font-size:0.76rem;margin-bottom:0.4rem;'>"
            f"{normalized.title()}</span>"
        ),
        unsafe_allow_html=True,
    )


def _is_create_table_request(command: str) -> bool:
    lowered = command.lower()
    return "table" in lowered and any(token in lowered for token in ("create", "make", "build"))


def _is_add_rows_request(lowered: str) -> bool:
    return any(phrase in lowered for phrase in ("add rows", "add row", "insert rows", "insert row")) or (
        lowered.startswith("add ") and "table" not in lowered
    )


def _is_show_data_request(lowered: str) -> bool:
    return lowered in {"show data", "show the data", "show rows", "show table", "show all data"}


def _is_save_request(lowered: str) -> bool:
    normalized = normalize_identifier(lowered)
    return normalized in {"save_to_mysql", "save_mysql", "save_to_database", "save_data"}


def _inject_styles() -> None:
    st.markdown(
        """
        <style>
        .stApp {
            background:
                radial-gradient(circle at top left, rgba(15, 118, 110, 0.14), transparent 28%),
                radial-gradient(circle at top right, rgba(180, 83, 9, 0.12), transparent 24%),
                linear-gradient(180deg, #f7f3ea 0%, #efe8db 100%);
        }
        .block-container {
            max-width: 1360px;
            padding-top: 1rem;
            padding-bottom: 2rem;
        }
        h1, h2, h3 {
            font-family: Georgia, "Times New Roman", serif;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _save_uploaded_file(uploaded_file: st.runtime.uploaded_file_manager.UploadedFile, upload_dir: Path) -> Path:
    upload_dir.mkdir(parents=True, exist_ok=True)
    destination = upload_dir / uploaded_file.name
    destination.write_bytes(uploaded_file.getbuffer())
    return destination
