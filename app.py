"""
app.py - Streamlit entry point.
Run with: streamlit run app.py
"""

import os
from pathlib import Path
import sys

from dotenv import find_dotenv, load_dotenv

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


def _load_streamlit_secrets() -> None:
    """
    Bridge Streamlit Cloud secrets → os.environ.

    On Streamlit Community Cloud, credentials are stored in st.secrets
    (set via the cloud dashboard). This function copies them into os.environ
    so the rest of the app (config.py, etc.) can read them normally.
    Silently skips if st.secrets is unavailable (e.g., local dev with .env).
    """
    try:
        import streamlit as st
        for key, value in st.secrets.items():
            if isinstance(value, str):
                os.environ.setdefault(key, value)
    except Exception:
        pass  # Not on Streamlit Cloud, or secrets not configured yet


def _bootstrap_environment() -> None:
    # 1. Load Streamlit Cloud secrets first (cloud deployment)
    _load_streamlit_secrets()
    # 2. Fall back to .env file (local development)
    env_path = find_dotenv()
    if not env_path:
        env_path = str(ROOT / ".env")
    load_dotenv(env_path, override=False)  # Don't override secrets already loaded


_bootstrap_environment()

# Launch the main Streamlit UI that includes table creation,
# table editing, SQL editing, and the chat workflow.
from nl_data_assistant.streamlit_app import run_streamlit_app


def safe_validate() -> None:
    """Validate MySQL config when available, without hard-failing the UI."""
    try:
        from nl_data_assistant.config import validate_config

        validate_config()
        print("Config loaded successfully.")
    except Exception as exc:
        print("Running in local mode with the current configuration.")
        print(f"Reason: {exc}")


if __name__ == "__main__":
    safe_validate()
    run_streamlit_app()

