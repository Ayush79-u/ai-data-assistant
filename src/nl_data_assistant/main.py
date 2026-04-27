"""
main.py — CLI entry point.

Usage:
  python cli.py                              # interactive REPL
  python cli.py --command "show all students"
  python cli.py --command "..." --yes        # skip destructive confirmation
"""
from __future__ import annotations

import argparse
import sys

from nl_data_assistant.config import validate_config


# ANSI colours (disabled on Windows without ANSI support)
_GREEN = "\033[92m"
_RED   = "\033[91m"
_CYAN  = "\033[96m"
_GREY  = "\033[90m"
_BOLD  = "\033[1m"
_RESET = "\033[0m"

_BANNER = f"""
{_BOLD}{_CYAN}╔══════════════════════════════════════╗
║      AI Data Assistant (Local)       ║
╚══════════════════════════════════════╝{_RESET}
Type a command in plain English, or:
  {_GREY}help{_RESET}    — show examples
  {_GREY}schema{_RESET}  — show live DB schema
  {_GREY}history{_RESET} — show executed SQL
  {_GREY}clear{_RESET}   — clear conversation context
  {_GREY}exit{_RESET}    — quit
"""

_EXAMPLES = [
    "Create a students table with name, cgpa, and branch",
    "Insert 5 students with random data",
    "Show all students ordered by cgpa desc",
    "Delete students with cgpa less than 6",
    "Show me a bar chart of monthly_expenses",
    "Import expenses.xlsx to MySQL as monthly_expenses",
    "Export students to Excel",
    "Describe the schema of students",
]


def main() -> int:
    parser = argparse.ArgumentParser(description="AI Data Assistant CLI")
    parser.add_argument("--command", "-c", help="Single command to execute then exit")
    parser.add_argument("--yes", "-y", action="store_true",
                        help="Auto-confirm destructive operations")
    args = parser.parse_args()

    try:
        validate_config()
    except EnvironmentError as exc:
        _error(str(exc))
        return 1

    # Import here so config validation happens first
    from nl_data_assistant.services.engine import DataAssistantEngine
    engine = DataAssistantEngine()

    if not engine.mysql.ping():
        _error("Cannot reach MySQL — check your .env credentials.")
        return 1

    if args.command:
        return _run_once(engine, args.command, skip_confirmation=args.yes)

    # Interactive REPL
    print(_BANNER)
    return _repl(engine, auto_confirm=args.yes)


def _repl(engine, auto_confirm: bool = False) -> int:
    sql_log: list[str] = []

    while True:
        try:
            command = input(f"{_BOLD}{_CYAN}> {_RESET}").strip()
        except (KeyboardInterrupt, EOFError):
            print("\nBye!")
            return 0

        if not command:
            continue

        low = command.lower()

        if low in ("exit", "quit", "q"):
            print("Bye!")
            return 0

        if low == "help":
            print(f"\n{_GREY}Example commands:{_RESET}")
            for ex in _EXAMPLES:
                print(f"  {_GREY}•{_RESET} {ex}")
            print()
            continue

        if low == "schema":
            print(f"\n{_GREY}{engine.mysql.get_schema_summary()}{_RESET}\n")
            continue

        if low == "history":
            if not sql_log:
                print(f"{_GREY}(no queries yet){_RESET}\n")
            else:
                for i, sql in enumerate(sql_log, 1):
                    print(f"{_GREY}{i:>3}.{_RESET} {sql}")
            print()
            continue

        if low == "clear":
            engine.clear_history()
            print(f"{_GREY}Conversation context cleared.{_RESET}\n")
            continue

        result = engine.run(command, skip_confirmation=auto_confirm)

        # Destructive confirmation round-trip
        if not result.success and hasattr(result.data, "intent"):
            print(f"\n{_RED}⚠️  {result.message}{_RESET}")
            ans = input("Type 'yes' to confirm: ").strip().lower()
            if ans == "yes":
                result = engine.execute(result.data)
            else:
                print(f"{_GREY}Cancelled.{_RESET}\n")
                continue

        _print_result(result)
        if result.sql_executed:
            sql_log.append(result.sql_executed)

    return 0


def _run_once(engine, command: str, skip_confirmation: bool) -> int:
    result = engine.run(command, skip_confirmation=skip_confirmation)
    _print_result(result)
    return 0 if result.success else 1


def _print_result(result) -> None:
    import pandas as pd
    if not result.success:
        _error(result.error or result.message)
        return

    if result.sql_executed:
        print(f"\n{_GREY}{result.sql_executed}{_RESET}")

    print(f"{_GREEN}✓ {result.message}{_RESET}")

    if isinstance(result.data, pd.DataFrame) and not result.data.empty:
        print(result.data.to_string(index=False))

    print()


def _error(msg: str) -> None:
    print(f"{_RED}✗ {msg}{_RESET}", file=sys.stderr)
