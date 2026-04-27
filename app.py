"""
app.py — Streamlit entry point.
Run with: streamlit run app.py
"""

from pathlib import Path
import sys
import os

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

# Import safely
from nl_data_assistant.streamlit_workspace_app import run_streamlit_app

# Try config validation but don't crash
def safe_validate():
    try:
        from nl_data_assistant.config import validate_config
        validate_config()
        print("✅ Config loaded (AI mode enabled)")
    except Exception as e:
        print("⚠️ Running in LOCAL mode (no AI)")
        print(f"Reason: {e}")

if __name__ == "__main__":
    safe_validate()   # no more hard crash
    run_streamlit_app()
