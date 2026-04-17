# AI-Powered Excel + MySQL Data Assistant

This project gives you a Python starter system for interacting with Excel sheets and MySQL databases using natural language.

Examples:

- `Create a table of students with name and CGPA`
- `Import expenses.xlsx to MySQL as monthly_expenses`
- `Show monthly_expenses from MySQL`
- `Show me a bar chart of monthly expenses`
- `Export MySQL table students to Excel`

The app translates commands into structured actions, cleans data, maps schemas, syncs Excel and MySQL in both directions, and generates charts or dashboards from the result.

## Features

- Natural-language command interpretation
- Local structured parser for `create`, `insert`, `select`, `update`, and `delete`
- Schema-aware MySQL query generator with table and column resolution
- JSON table blueprint generator for schema + sample data output
- Excel sheet creation, reading, writing, and schema inspection
- MySQL table creation, querying, export, and schema inspection
- Excel-to-MySQL import with cleaning and inferred schema mapping
- MySQL-to-Excel export
- Plotly-based bar, line, pie, scatter, histogram, and dashboard views
- Streamlit workbench UI for interactive use
- FastAPI backend for parse and execute endpoints
- CLI mode for quick testing
- Optional OpenAI-powered parsing when you provide `OPENAI_API_KEY` and `OPENAI_MODEL`

## Project Structure

```text
.
|-- app.py
|-- cli.py
|-- server.py
|-- requirements.txt
|-- README.md
|-- src/
|   `-- nl_data_assistant/
|       |-- api.py
|       |-- api_models.py
|       |-- config.py
|       |-- examples.py
|       |-- models.py
|       |-- streamlit_app.py
|       |-- main.py
|       |-- nlp/
|       |   `-- interpreter.py
|       |   `-- local_parser.py
|       |   `-- mysql_query_generator.py
|       |   `-- table_blueprint.py
|       |-- services/
|       |   |-- engine.py
|       |   |-- excel_service.py
|       |   |-- mysql_service.py
|       |   |-- sync_service.py
|       |   `-- visualization_service.py
|       `-- utils/
|           |-- cleaning.py
|           `-- schema.py
`-- tests/
```

## How To Run In PyCharm

1. Open the folder `C:\Users\AYUSH THAKUR\OneDrive\Documents\New project` in PyCharm.
2. Create a virtual environment with Python 3.10 or newer.
3. Install dependencies:

```bash
pip install -r requirements.txt
pip install -e .
```

4. Copy `.env.example` to `.env` and fill in your MySQL connection details.
5. Optional: add `OPENAI_API_KEY` and `OPENAI_MODEL` if you want LLM-based parsing.
6. Start the UI:

```bash
streamlit run app.py
```

7. Start the FastAPI backend:

```bash
uvicorn server:app --reload
```

8. Or use the CLI:

```bash
python cli.py --command "Create a table of students with name and CGPA"
```

## Example Commands

- `Create a table of students with name and CGPA`
- `Insert 5 students with random CGPA`
- `Add 3 students`
- `Show all students`
- `Show names and cgpa of students order by cgpa desc`
- `Delete students with CGPA less than 6`
- `Create employee table with name, salary`
- `Make table of products with price and quantity`
- `Create an Excel sheet of monthly expenses with month, category and amount`
- `Import expenses.xlsx to MySQL as monthly_expenses`
- `Show monthly_expenses from Excel`
- `Show me a line chart of monthly_expenses`
- `Describe schema of students`
- `Clean expenses.xlsx`
- `Export MySQL table monthly_expenses to Excel`

## Architecture

- `local_parser.py` handles intent detection, entity extraction, random row generation, and SQL generation without any external AI API
- `mysql_query_generator.py` refines parsed entities with real MySQL schema information so table and column names resolve more accurately
- `table_blueprint.py` converts create-table commands into JSON containing table name, MySQL-compatible columns, and sample data
- `CommandInterpreter` converts natural language into an `ActionPlan` and can still fall back to OpenAI if you enable it
- `DataAssistantEngine` dispatches the plan to the correct service
- `ExcelService` handles workbook and sheet operations
- `MySQLService` handles SQL operations through SQLAlchemy
- `SyncService` moves data between Excel and MySQL
- `DataCleaner` normalizes headers, trims strings, drops empty rows/columns, and infers types
- `VisualizationService` creates interactive Plotly charts and dashboards
- `api.py` exposes the same parser and execution engine through FastAPI endpoints such as `/parse`, `/blueprint`, `/execute`, `/health`, and `/schema`

## Important Notes

- The default parser is rule-based and safe for the included commands.
- If you enable OpenAI parsing, the app falls back to the rule-based parser when the API response is unavailable or invalid.
- The project does not auto-create a MySQL server. You still need a running MySQL instance and valid credentials.
- If MySQL is configured, the parser now tries to inspect table schemas and use them to generate cleaner SQL.
- In this OneDrive-backed folder, Python bytecode caching may be restricted, so some verification tools that write `.pyc` files can fail even when source code is valid.
