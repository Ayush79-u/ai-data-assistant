from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from nl_data_assistant.streamlit_app import run_streamlit_app


if __name__ == "__main__":
    run_streamlit_app()

import pandas as pd

print("AI Data System Started 🚀")