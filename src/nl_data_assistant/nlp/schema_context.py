"""
schema_context.py — SchemaContext dataclass and AI prompt builder.

Holds the rich database schema extracted by MySQLSessionService and
exposes a single helper that turns it into the system prompt block
injected before every AI SQL generation call.
"""
from __future__ import annotations

from dataclasses import dataclass, field

# ---------------------------------------------------------------------------
# Constants (shared with MySQLSessionService)
# ---------------------------------------------------------------------------

MAX_CONTEXT_CHARS = 8_000   # hard cap on total prompt size
MAX_VALUE_CHARS   = 48      # max chars per sample cell value

# Columns whose sample values should never appear in the AI prompt
_SENSITIVE_COLUMN_NAMES: frozenset[str] = frozenset({
    "password", "passwd", "pwd",
    "token", "secret", "api_key", "apikey",
    "ssn", "social_security",
    "credit_card", "card_number", "cvv",
    "pin",
})


def _is_sensitive(col_name: str) -> bool:
    return col_name.lower().strip() in _SENSITIVE_COLUMN_NAMES


# ---------------------------------------------------------------------------
# SchemaContext dataclass
# ---------------------------------------------------------------------------

@dataclass
class SchemaContext:
    """
    Rich database schema context used to build AI prompts.

    Attributes
    ----------
    database      : name of the active database
    context_str   : pre-formatted prompt block (ready to inject)
    table_names   : list of real table names in the DB
    column_map    : {table: [col_name, ...]}
    categorical_map : {table: {col: [distinct_val, ...]}} — low-cardinality cols
    """
    database:        str                               = ""
    context_str:     str                               = ""
    table_names:     list[str]                         = field(default_factory=list)
    column_map:      dict[str, list[str]]              = field(default_factory=dict)
    categorical_map: dict[str, dict[str, list[str]]]  = field(default_factory=dict)

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------

    def is_empty(self) -> bool:
        return not self.table_names

    def known_tables(self) -> frozenset[str]:
        return frozenset(t.lower() for t in self.table_names)

    def known_columns(self) -> frozenset[str]:
        all_cols: set[str] = set()
        for cols in self.column_map.values():
            all_cols.update(c.lower() for c in cols)
        return frozenset(all_cols)

    def columns_for(self, table: str) -> list[str]:
        return self.column_map.get(table, [])


# ---------------------------------------------------------------------------
# Prompt builder
# ---------------------------------------------------------------------------

_SYSTEM_BASE = """\
You are an expert MySQL SQL generator for a production web application.

Rules:
1. Return ONLY executable MySQL SQL — no explanations, no markdown code fences.
2. NEVER invent table or column names that are not listed in the schema below.
3. When filtering text columns, use the EXACT casing shown in the [values: ...]
   list. Example: if the user says "hr department" and values show `HR`, write
   WHERE Department = 'HR'  NOT  WHERE Department = 'hr department'.
4. Avoid dangerous statements: DROP, TRUNCATE.
5. Always include a WHERE clause with UPDATE and DELETE.
6. Prefer readable column aliases in SELECT when helpful.
7. For JOIN queries, only join tables that exist in the schema.
"""

_SCHEMA_HEADER = "\n=== DATABASE SCHEMA ===\nDatabase: {database}\n"
_SCHEMA_FOOTER = "\n=== END SCHEMA ===\n"
_USER_REQUEST_INTRO = (
    "\nGenerate SQL for the following request, using ONLY the tables and "
    "columns listed above:\n\n"
)


def build_system_prompt(schema_ctx: "SchemaContext") -> str:
    """
    Build the full system prompt string to send to the LLM.
    Includes base rules + full schema block.
    """
    if schema_ctx.is_empty():
        return _SYSTEM_BASE + "\n(No schema available — use your best judgement.)\n"

    parts = [_SYSTEM_BASE]
    parts.append(_SCHEMA_HEADER.format(database=schema_ctx.database))
    parts.append(schema_ctx.context_str)
    parts.append(_SCHEMA_FOOTER)
    return "".join(parts)


def build_user_message(
    user_input: str,
    *,
    table_name: str = "",
    data_source: str = "mysql",
) -> str:
    """Build the user-turn message passed to the LLM."""
    lines = []
    if data_source and data_source != "mysql":
        lines.append(f"Data source: {data_source}")
    if table_name:
        lines.append(f"Primary table of interest: {table_name}")
    lines.append(_USER_REQUEST_INTRO.strip())
    lines.append(user_input)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Compact schema string builder (called by MySQLSessionService)
# ---------------------------------------------------------------------------

def build_context_str(
    database: str,
    tables_data: list[dict],
    *,
    max_chars: int = MAX_CONTEXT_CHARS,
) -> str:
    """
    Format the schema metadata into a compact, LLM-readable string.

    Parameters
    ----------
    database    : database name
    tables_data : list of dicts, each with keys:
                  'name'        : str
                  'columns'     : list of {'name': str, 'type': str}
                  'sample_rows' : list of dicts (column → value)
                  'categoricals': dict of {col_name: [val, ...]}
    max_chars   : hard character cap for the whole block
    """
    lines: list[str] = []

    for tbl in tables_data:
        tbl_name = tbl["name"]
        cols     = tbl.get("columns", [])
        samples  = tbl.get("sample_rows", [])
        cats     = tbl.get("categoricals", {})

        lines.append(f"\nTABLE: {tbl_name}")
        lines.append("  Columns:")

        for col in cols:
            col_name = col["name"]
            col_type = str(col["type"])
            cat_vals = cats.get(col_name, [])
            if cat_vals and not _is_sensitive(col_name):
                formatted_vals = ", ".join(str(v) for v in cat_vals)
                lines.append(f"    - {col_name:<20} {col_type:<20} [values: {formatted_vals}]")
            else:
                lines.append(f"    - {col_name:<20} {col_type}")

        if samples and cols:
            # Build a simple pipe-delimited table
            col_names = [c["name"] for c in cols if not _is_sensitive(c["name"])]
            if col_names:
                lines.append("")
                lines.append("  Sample rows:")
                header = " | ".join(col_names)
                lines.append(f"    {header}")
                lines.append(f"    {'-' * len(header)}")
                for row in samples:
                    cells = []
                    for cn in col_names:
                        val = row.get(cn, "")
                        cell = str(val) if val is not None else "NULL"
                        if len(cell) > MAX_VALUE_CHARS:
                            cell = cell[:MAX_VALUE_CHARS] + "…"
                        cells.append(cell)
                    lines.append(f"    {' | '.join(cells)}")

        lines.append("")   # blank line between tables

    context = "\n".join(lines)

    # Hard cap: if we exceed max_chars, truncate at last full table boundary
    if len(context) > max_chars:
        context = context[:max_chars]
        # Try to trim at a clean TABLE boundary
        last_table = context.rfind("\nTABLE:")
        if last_table > 0:
            context = context[:last_table]
        context += "\n  ... (schema truncated for prompt size) ..."

    return context
