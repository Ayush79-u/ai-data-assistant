"""
ai_sql_generator.py — OpenRouter-powered natural-language → SQL generator.

Uses OpenRouter (https://openrouter.ai) which provides a unified OpenAI-compatible
API for many models including Qwen, Claude, GPT, Llama, etc.

Used by the Streamlit UI's "Generate SQL with AI" button. The generated
SQL is injected into the existing SQL editor; nothing executes until the
user clicks "Run SQL", at which point the existing safety checks apply.
"""
from __future__ import annotations

import logging
import os
import re

log = logging.getLogger(__name__)

SYSTEM_PROMPT = (
    "You are an expert SQL generator.\n"
    "Return ONLY SQL query.\n"
    "No explanation.\n"
    "Use only given schema.\n"
    "Avoid dangerous queries like DROP or TRUNCATE.\n"
    "Ensure UPDATE and DELETE have WHERE clause.\n"
    "Support both MySQL and Excel tables."
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
    Generate a SQL query from a natural-language request via OpenRouter.

    Parameters
    ----------
    user_input  : natural-language request from the user
    schema      : compact schema description (e.g. "name VARCHAR, cgpa FLOAT")
    table_name  : the active table the user is working on
    data_source : "mysql" or "excel"

    Returns
    -------
    SQL query string only (no commentary, no markdown).
    """
    if not user_input.strip():
        raise ValueError("Please describe what you want in plain English.")

    # Lazy import so the rest of the app still works if `openai` isn't installed.
    try:
        from openai import OpenAI
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "The `openai` package is required for AI SQL generation. "
            "Install it with: pip install openai"
        ) from exc

    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError(
            "Set OPENROUTER_API_KEY in your .env to use AI SQL generation. "
            "Get a key at https://openrouter.ai/keys"
        )

    base_url = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
    # Default to a free Qwen model on OpenRouter; override via .env if you like.
    model = os.getenv("OPENROUTER_MODEL", "qwen/qwen-2.5-coder-32b-instruct")

    client = OpenAI(api_key=api_key, base_url=base_url)

    user_prompt = (
        f"Data source: {data_source}\n"
        f"Table name: {table_name or '(none selected)'}\n"
        f"Schema:\n{schema or '(no schema available)'}\n\n"
        f"User request: {user_input}\n\n"
        f"Generate the SQL query."
    )

    # OpenRouter recommends sending these optional headers for analytics/ranking.
    extra_headers = {
        "HTTP-Referer": os.getenv("OPENROUTER_REFERER", "http://localhost:8501"),
        "X-Title": os.getenv("OPENROUTER_APP_NAME", "AI Data Assistant"),
    }

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.1,
        extra_headers=extra_headers,
    )

    sql = _clean_sql(response.choices[0].message.content or "")
    log.debug("OpenRouter (%s) generated SQL: %s", model, sql)

    if not sql:
        raise RuntimeError("AI returned an empty response. Try rephrasing your request.")

    if not is_safe_sql(sql):
        raise ValueError(
            "AI returned a blocked query (DROP/TRUNCATE). Please rephrase your request."
        )

    return sql