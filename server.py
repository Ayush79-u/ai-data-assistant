"""
server.py — FastAPI entry point.
Run with: uvicorn server:app --reload
"""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from nl_data_assistant.config import validate_config

validate_config()   # crash here with a clear message, not mid-request

from nl_data_assistant.api import app  # noqa: E402  (must come after sys.path patch)

__all__ = ["app"]
