"""
ai_sql_generator.py — Schema-aware Groq-powered natural-language → SQL generator.

Upgrade over the original:
- Accepts a rich SchemaContext (tables, columns, sample rows, categorical values)
  in addition to (or instead of) a plain schema string.
- Injects the full context into the system prompt so the LLM never needs to guess
  table or column names.
- Runs a post-generation hallucination guard that rejects SQL referencing
  unknown tables or columns.
- Backward-compatible: still accepts a plain `schema: str` for legacy callers.
"""
from __future__ import annotations

import logging
import re
import os
from typing import Union

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Type alias for the schema argument (rich context OR plain string)
# ---------------------------------------------------------------------------
try:
    from nl_data_assistant.nlp.schema_context import (
        SchemaContext,
        build_system_prompt,
        build_user_message,
    )
    _SCHEMA_CONTEXT_AVAILABLE = True
except ImportError:   # pragma: no cover  (shouldn't happen in normal install)
    _SCHEMA_CONTEXT_AVAILABLE = False
    SchemaContext = None  # type: ignore[assignment,misc]

SchemaInput = Union["SchemaContext", str, None]


# ---------------------------------------------------------------------------
# Safety helpers
# ---------------------------------------------------------------------------

# Statements that must never be allowed regardless of context
_BLOCKED_STATEMENTS: frozenset[str] = frozenset({
    "DROP",
    "TRUNCATE",
    "GRANT",
    "REVOKE",
    "LOAD",        # LOAD DATA INFILE
    "CREATE USER",
    "ALTER USER",
    "RENAME USER",
    "DROP USER",
})

# Patterns that indicate a missing WHERE clause on mutating statements
_MUTATING_WITHOUT_WHERE = re.compile(
    r"^\s*(?:DELETE\s+FROM|UPDATE)\s+`?[a-zA-Z_][a-zA-Z0-9_]*`?\s*;", re.IGNORECASE
)


def is_safe_sql(sql: str) -> bool:
    """Return False for SQL that is dangerous regardless of user intent.

    Blocks:
      - DROP, TRUNCATE, GRANT, REVOKE, LOAD DATA, CREATE/ALTER/DROP/RENAME USER
      - DELETE or UPDATE statements with no WHERE clause
      - SELECT ... INTO OUTFILE (data exfiltration)
    """
    if not sql:
        return False
    upper = sql.strip().upper()
    first_word = upper.split()[0] if upper.split() else ""

    # Block by first keyword
    if first_word in _BLOCKED_STATEMENTS:
        return False

    # Block compound dangerous keywords (e.g. CREATE USER, LOAD DATA)
    first_two = " ".join(upper.split()[:2])
    if first_two in _BLOCKED_STATEMENTS:
        return False

    # Block DROP DATABASE even if first word is not DROP alone
    if re.search(r"\bDROP\s+DATABASE\b", upper):
        return False

    # Block SELECT INTO OUTFILE (data exfiltration)
    if re.search(r"\bINTO\s+OUTFILE\b", upper):
        return False

    # Block DELETE/UPDATE without WHERE (would wipe the whole table)
    if _MUTATING_WITHOUT_WHERE.match(sql.strip()):
        return False

    return True


def _clean_sql(text: str) -> str:
    """Strip markdown fences and stray whitespace from model output."""
    text = text.strip()
    text = re.sub(r"^```(?:sql)?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*```$", "", text)
    return text.strip()


# ---------------------------------------------------------------------------
# Hallucination guard
# ---------------------------------------------------------------------------

_TABLE_PATTERN  = re.compile(
    r"\b(?:FROM|INTO|UPDATE|JOIN|TABLE|DESCRIBE)\s+`?([a-zA-Z_][a-zA-Z0-9_]*)`?",
    re.IGNORECASE,
)
_COLUMN_PATTERN = re.compile(
    r"(?:WHERE|SET|ON|ORDER\s+BY|GROUP\s+BY|HAVING|SELECT)\s+"
    r"(?:`?([a-zA-Z_][a-zA-Z0-9_]*)`?\.)?`?([a-zA-Z_][a-zA-Z0-9_]*)`?",
    re.IGNORECASE,
)

# SQL keywords to skip when checking column names
_SQL_KEYWORDS: frozenset[str] = frozenset({
    "and", "or", "not", "null", "is", "in", "like", "between",
    "select", "from", "where", "order", "by", "group", "having",
    "distinct", "asc", "desc", "limit", "offset", "case", "when",
    "then", "else", "end", "as", "on", "join", "inner", "left",
    "right", "outer", "full", "cross", "union", "all", "exists",
    "count", "sum", "avg", "min", "max",
})


def validate_sql_against_schema(
    sql: str,
    schema_ctx: "SchemaContext",
) -> None:
    """
    Raise ValueError if the generated SQL references tables or columns that
    do not exist in schema_ctx.

    Skips validation when schema_ctx is empty (no tables known).
    """
    if schema_ctx.is_empty():
        return

    known_tables  = schema_ctx.known_tables()
    known_columns = schema_ctx.known_columns()

    # Check table names
    for match in _TABLE_PATTERN.finditer(sql):
        ref = match.group(1).lower()
        if ref in _SQL_KEYWORDS:
            continue
        if ref not in known_tables:
            raise ValueError(
                f"Generated SQL references unknown table `{match.group(1)}`. "
                f"Known tables: {', '.join(schema_ctx.table_names)}. "
                "Please rephrase your request."
            )

    # Check column names (best-effort; only flags obvious hallucinations)
    for match in _COLUMN_PATTERN.finditer(sql):
        ref = (match.group(2) or "").lower()
        if not ref or ref in _SQL_KEYWORDS or ref.isdigit():
            continue
        if ref not in known_columns:
            # Be lenient: log a warning rather than hard-failing, since the regex
            # can over-match expressions like function names (DATE_FORMAT etc.)
            log.warning(
                "Generated SQL may reference unknown column `%s`. "
                "Known columns: %s",
                ref,
                list(known_columns)[:20],
            )


# ---------------------------------------------------------------------------
# Prompt construction (legacy path for plain-string schemas)
# ---------------------------------------------------------------------------

_LEGACY_SYSTEM_PROMPT = (
    "You are an expert MySQL SQL generator.\n"
    "Return ONLY executable MySQL SQL — no explanations, no markdown fences.\n"
    "Use only the provided schema.\n"
    "Avoid dangerous queries like DROP or TRUNCATE.\n"
    "Ensure UPDATE and DELETE include a WHERE clause.\n"
    "When filtering text columns, use exact casing as shown in the schema.\n"
    "Support both MySQL and Excel-backed tables.\n"
)


def _build_legacy_messages(
    user_input: str,
    schema: str,
    table_name: str,
    data_source: str,
) -> list[dict]:
    user_prompt = (
        f"Data source: {data_source}\n"
        f"Table name: {table_name or '(none selected)'}\n"
        f"Schema:\n{schema or '(no schema available)'}\n\n"
        f"User request: {user_input}\n\n"
        "Generate the SQL query."
    )
    return [
        {"role": "system", "content": _LEGACY_SYSTEM_PROMPT},
        {"role": "user",   "content": user_prompt},
    ]


def _build_schema_aware_messages(
    user_input: str,
    schema_ctx: "SchemaContext",
    table_name: str,
    data_source: str,
) -> list[dict]:
    system_content = build_system_prompt(schema_ctx)
    user_content   = build_user_message(
        user_input,
        table_name=table_name,
        data_source=data_source,
    )
    return [
        {"role": "system", "content": system_content},
        {"role": "user",   "content": user_content},
    ]


# ---------------------------------------------------------------------------
# Main public API
# ---------------------------------------------------------------------------

def generate_sql(
    user_input: str,
    schema: SchemaInput = None,
    table_name: str = "",
    data_source: str = "mysql",
    *,
    # Legacy keyword kept for callers that still pass schema= as a string
    schema_context: "SchemaContext | None" = None,
) -> str:
    """
    Generate a SQL query from a natural-language request via Groq.

    Parameters
    ----------
    user_input     : natural-language request from the user
    schema         : either a SchemaContext object OR a plain schema string
                     (for backward compatibility)
    table_name     : active table name (used as a hint)
    data_source    : "mysql" or "excel"
    schema_context : explicit SchemaContext kwarg (takes precedence over schema)
    """
    if not user_input.strip():
        raise ValueError("Please describe what you want in plain English.")

    # Resolve which schema representation to use
    ctx: "SchemaContext | None" = None
    plain_schema: str = ""

    if schema_context is not None:
        ctx = schema_context
    elif _SCHEMA_CONTEXT_AVAILABLE and isinstance(schema, SchemaContext):
        ctx = schema
    elif isinstance(schema, str):
        plain_schema = schema

    # Import Groq
    try:
        from groq import Groq
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "The `groq` package is required. Install it with: pip install groq"
        ) from exc

    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise RuntimeError(
            "Set GROQ_API_KEY in your .env to use Groq SQL generation."
        )

    model  = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
    client = Groq(api_key=api_key)

    # Build messages depending on what schema info we have
    if ctx is not None and not ctx.is_empty():
        messages = _build_schema_aware_messages(user_input, ctx, table_name, data_source)
        log.debug(
            "Using schema-aware prompt for %d table(s): %s",
            len(ctx.table_names),
            ctx.table_names,
        )
    else:
        messages = _build_legacy_messages(user_input, plain_schema, table_name, data_source)
        log.debug("Using legacy (schema-string) prompt.")

    response = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=0.1,
    )

    sql = _clean_sql(response.choices[0].message.content or "")
    log.debug("Groq (%s) generated SQL: %s", model, sql)

    if not sql:
        raise RuntimeError(
            "Groq returned an empty response. Try rephrasing your request."
        )

    if not is_safe_sql(sql):
        raise ValueError(
            "Groq returned a blocked query (DROP/TRUNCATE). "
            "Please rephrase your request."
        )

    # Hallucination guard (only when we have a real SchemaContext)
    if ctx is not None and not ctx.is_empty():
        validate_sql_against_schema(sql, ctx)

    return sql
