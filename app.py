"""
app.py - Streamlit entry point.
Run with: streamlit run app.py
"""

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

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