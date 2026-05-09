"""
ai_sql_generator.py - Groq-powered natural-language to SQL generator.

Uses the official Groq Python SDK with `llama-3.3-70b-versatile` as the
default fallback model. The Streamlit UI injects generated SQL into the SQL
editor, and safe read-only queries can be auto-executed by the app.
"""
from __future__ import annotations

import logging
import re
import os

log = logging.getLogger(__name__)

SYSTEM_PROMPT = (
    "You are an expert SQL generator.\n"
    "Return ONLY SQL.\n"
    "Do not add explanations.\n"
    "Use only the provided schema.\n"
    "Avoid dangerous queries like DROP or TRUNCATE.\n"
    "Ensure UPDATE and DELETE include a WHERE clause.\n"
    "Support both MySQL and Excel-backed tables.\n"
)


def is_safe_sql(sql: str) -> bool:
    """Block DROP and TRUNCATE before any execution path."""
    if not sql:
        return False
    upper = sql.upper()
    return not re.search(r"\b(?:DROP|TRUNCATE)\b", upper)


def _clean_sql(text: str) -> str:
    """Strip markdown fences and stray whitespace from model output."""
    text = text.strip()
    text = re.sub(r"^```(?:sql)?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*```$", "", text)
    return text.strip()


def generate_sql(
    user_input: str,
    schema: str,
    table_name: str,
    data_source: str,
) -> str:
    """
    Generate a SQL query from a natural-language request via Groq.

    Parameters
    ----------
    user_input  : natural-language request from the user
    schema      : compact schema description
    table_name  : active table name
    data_source : "mysql" or "excel"
    """
    if not user_input.strip():
        raise ValueError("Please describe what you want in plain English.")

    try:
        from groq import Groq
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "The `groq` package is required for SQL generation with "
            "`llama-3.3-70b-versatile`. Install it with: pip install groq"
        ) from exc

    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise RuntimeError(
            "Set GROQ_API_KEY in your .env to use Groq SQL generation."
        )

    model = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
    client = Groq(api_key=api_key)

    user_prompt = (
        f"Data source: {data_source}\n"
        f"Table name: {table_name or '(none selected)'}\n"
        f"Schema:\n{schema or '(no schema available)'}\n\n"
        f"User request: {user_input}\n\n"
        "Generate the SQL query."
    )

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.1,
    )

    sql = _clean_sql(response.choices[0].message.content or "")
    log.debug("Groq (%s) generated SQL: %s", model, sql)

    if not sql:
        raise RuntimeError("Groq returned an empty response. Try rephrasing your request.")

    if not is_safe_sql(sql):
        raise ValueError(
            "Groq returned a blocked query (DROP/TRUNCATE). Please rephrase your request."
        )

    return sql
