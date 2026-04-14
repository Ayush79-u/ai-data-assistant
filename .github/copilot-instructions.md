# Copilot Instructions for this repo

- This project is an NL assistant for Excel + MySQL, with two entrypoints:
  - `app.py` starts the Streamlit UI via `src/nl_data_assistant/streamlit_app.py`
  - `cli.py` runs commands using `src/nl_data_assistant/main.py`

- Core runtime flow lives in `src/nl_data_assistant/services/engine.py`.
  - `DataAssistantEngine.execute()` interprets a command, hydrates Excel context, and dispatches actions.
  - `ActionPlan` in `src/nl_data_assistant/models.py` is the canonical command plan.

- Command parsing is rule-first in `src/nl_data_assistant/nlp/interpreter.py`.
  - Supported actions include `create_table`, `query`, `visualize`, `excel_to_mysql`, `mysql_to_excel`, `clean_data`, `describe_schema`.
  - Use explicit keywords like `import`, `export`, `clean`, `describe`, `show`, `create`, and chart types `bar|line|pie|scatter|histogram|dashboard`.
  - Target detection is keyword-based: Excel if command mentions `excel|sheet|workbook|.xlsx|.xls`; MySQL if it mentions `mysql|database|db|sql`.
  - OpenAI parsing is optional and only used if `OPENAI_API_KEY` and `OPENAI_MODEL` are set.

- Service boundaries:
  - `ExcelService` handles workbook read/write/describe operations.
  - `MySQLService` handles SQLAlchemy-backed table creation, reads, queries, and schema inspection.
  - `SyncService` glues Excel <-> MySQL imports/exports and always applies `DataCleaner`.
  - `VisualizationService` builds Plotly charts and dashboards from cleaned DataFrames.

- Config and environment:
  - `src/nl_data_assistant/config.py` loads env vars and creates `data/`, `data/uploads/`, and `outputs/` automatically.
  - MySQL is only enabled when both `MYSQL_USER` and `MYSQL_DATABASE` are provided.
  - `default_target` is configurable and `auto` means the interpreter chooses based on the command.

- Important repo-specific conventions:
  - Names are normalized with `normalize_identifier()` before using as sheet/table names.
  - `clean_data` writes cleaned Excel output to `outputs/cleaned_<name>.xlsx` and cleaned MySQL to `<table>_cleaned`.
  - The Streamlit app preserves the last dataframe in session state and reuses it for visualization if no explicit table is given.
  - `DataAssistantEngine._hydrate_plan_from_context()` maps uploaded file context into plan fields like `source_path`, `workbook_path`, and `sheet_name`.

- Quick workflows:
  - Install: `pip install -r requirements.txt` and `pip install -e .`
  - Run UI: `streamlit run app.py`
  - Run CLI: `python cli.py --command "Create a table of students with name and CGPA" --target mysql`
  - Unit tests live under `tests/` and validate interpreter rules and cleaning behavior.

- When editing:
  - Keep the rule-based parser and `tests/test_interpreter.py` in sync.
  - Do not assume OpenAI is required; the rule parser is the primary command path.
  - Preserve the distinction between Excel and MySQL targets, especially when actions are inferred by keywords.
