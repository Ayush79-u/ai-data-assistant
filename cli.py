"""
cli.py — Interactive command-line interface.
Run with: python cli.py
Or single command: python cli.py --command "show all students"
"""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from nl_data_assistant.main import main

if __name__ == "__main__":
    raise SystemExit(main())
