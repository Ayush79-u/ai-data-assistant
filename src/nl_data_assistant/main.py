from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from nl_data_assistant.config import AppConfig
from nl_data_assistant.services.engine import DataAssistantEngine


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Natural-language Excel/MySQL assistant")
    parser.add_argument("--command", required=True, help="Natural language command to execute.")
    parser.add_argument(
        "--target",
        default="auto",
        choices=["auto", "excel", "mysql"],
        help="Default target if the command does not explicitly mention Excel or MySQL.",
    )
    parser.add_argument("--file", help="Optional Excel file path to use as context.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    config = AppConfig.from_env(Path.cwd())
    engine = DataAssistantEngine(config)
    result = engine.execute(args.command, default_target=args.target, uploaded_file_path=args.file)

    print(result.message)
    print(json.dumps(result.plan.as_dict(), indent=2, default=str))
    if result.dataframe is not None:
        print(result.dataframe.head(20).to_string(index=False))
    if result.file_path:
        print(f"Output: {result.file_path}")
    return 0 if result.success else 1


if __name__ == "__main__":
    sys.exit(main())

