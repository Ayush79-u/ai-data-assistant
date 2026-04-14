from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from nl_data_assistant.config import AppConfig
from nl_data_assistant.services.engine import DataAssistantEngine


EXAMPLE_COMMANDS = [
    "Create a table of students with name and CGPA",
    "Create an Excel sheet of monthly expenses with month, category and amount",
    "Import expenses.xlsx to MySQL as monthly_expenses",
    "Show monthly_expenses from MySQL",
    "Show me a bar chart of monthly expenses",
    "Export MySQL table students to Excel",
    "Clean expenses.xlsx",
]


def run_streamlit_app() -> None:
    st.set_page_config(page_title="AI Data Assistant", layout="wide")
    st.title("AI-Powered Excel + MySQL Data Assistant")
    st.caption("Type natural-language commands and let the app translate them into spreadsheet, database, and chart actions.")

    base_config = AppConfig.from_env(Path.cwd())

    with st.sidebar:
        st.header("Configuration")
        mysql_host = st.text_input("MySQL host", value=base_config.mysql_host)
        mysql_port = st.number_input("MySQL port", value=base_config.mysql_port, step=1)
        mysql_user = st.text_input("MySQL user", value=base_config.mysql_user)
        mysql_password = st.text_input("MySQL password", value=base_config.mysql_password, type="password")
        mysql_database = st.text_input("MySQL database", value=base_config.mysql_database)
        default_target = st.selectbox("Default target", ["auto", "mysql", "excel"], index=1)
        use_llm = st.toggle("Use OpenAI parser", value=base_config.llm_enabled)
        openai_api_key = st.text_input("OpenAI API key", value=base_config.openai_api_key if use_llm else "", type="password")
        openai_model = st.text_input("OpenAI model", value=base_config.openai_model if use_llm else "")

        st.markdown("**Examples**")
        for example in EXAMPLE_COMMANDS:
            st.code(example)

    config = AppConfig.from_env(Path.cwd())
    config.mysql_host = mysql_host
    config.mysql_port = int(mysql_port)
    config.mysql_user = mysql_user
    config.mysql_password = mysql_password
    config.mysql_database = mysql_database
    config.openai_api_key = openai_api_key if use_llm else ""
    config.openai_model = openai_model if use_llm else ""

    engine = DataAssistantEngine(config)

    uploaded_file = st.file_uploader("Upload an Excel file for context", type=["xlsx", "xls"])
    uploaded_file_path = None
    if uploaded_file:
        uploaded_file_path = _save_uploaded_file(uploaded_file, config.upload_dir)
        st.success(f"Loaded workbook: {uploaded_file_path.name}")

    command = st.text_area(
        "Natural language command",
        height=120,
        placeholder="Example: Create a table of students with name and CGPA",
    )

    if "last_dataframe" not in st.session_state:
        st.session_state.last_dataframe = None

    if st.button("Run Command", type="primary", use_container_width=True):
        if not command.strip():
            st.warning("Enter a command first.")
        else:
            result = engine.execute(
                command=command,
                default_target=default_target,
                uploaded_file_path=str(uploaded_file_path) if uploaded_file_path else None,
                current_dataframe=st.session_state.last_dataframe,
            )

            if result.success:
                st.success(result.message)
            else:
                st.error(result.message)

            with st.expander("Parsed action plan", expanded=True):
                st.json(result.plan.as_dict())

            if result.file_path:
                st.code(result.file_path)

            if result.metadata:
                st.write(result.metadata)

            if result.dataframe is not None:
                st.session_state.last_dataframe = result.dataframe
                st.subheader("Result Preview")
                st.dataframe(result.dataframe, use_container_width=True)
                st.caption(f"{len(result.dataframe)} rows x {len(result.dataframe.columns)} columns")

            if result.figure is not None:
                st.subheader("Visualization")
                st.plotly_chart(result.figure, use_container_width=True)


def _save_uploaded_file(uploaded_file: st.runtime.uploaded_file_manager.UploadedFile, upload_dir: Path) -> Path:
    upload_dir.mkdir(parents=True, exist_ok=True)
    destination = upload_dir / uploaded_file.name
    destination.write_bytes(uploaded_file.getbuffer())
    return destination
