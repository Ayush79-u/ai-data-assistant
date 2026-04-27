"""
interpreter.py — Converts natural language into an ActionPlan using LOCAL parser.

Fully offline, no API required.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from nl_data_assistant.models import ActionPlan

if TYPE_CHECKING:
    pass

log = logging.getLogger(__name__)


def interpret(
    command: str,
    schema_summary: str = "",
    history: list[dict] | None = None,
) -> ActionPlan:
    """
    Parse a natural-language command into an ActionPlan using LOCAL parser.

    Parameters
    ----------
    command:        user's raw text input
    schema_summary: (unused for now)
    history:        (unused for now)
    """
    from nl_data_assistant.nlp.local_parser import LocalParser

    parser = LocalParser()
    plan = parser.parse(command)

    log.debug("Local parser parsed %r → intent=%s", command, plan.intent)

    return plan