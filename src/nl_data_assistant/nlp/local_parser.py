"""
local_parser.py — Rule-based local parser.
"""
from __future__ import annotations

import re

from nl_data_assistant.models import ActionPlan, Intent


class LocalParser:
    """Keyword / regex intent detector with broad synonym coverage."""

    # Maps synonym verbs → canonical Intent
    _INTENT_MAP: list[tuple[re.Pattern, Intent]] = [
        (re.compile(r"\b(create|make|build|set\s+up|add)\b.*\btable\b", re.I), Intent.CREATE_TABLE),
        (re.compile(r"\b(create|make|new|add)\b.*\b(excel|sheet|workbook)\b", re.I), Intent.CREATE_EXCEL),
        (re.compile(r"\b(import|load|upload|bring)\b.*\b(xlsx?|excel|sheet)\b", re.I), Intent.IMPORT_EXCEL),
        (re.compile(r"\b(export|download|save|write)\b.*\b(xlsx?|excel|sheet)\b", re.I), Intent.EXPORT_EXCEL),
        (re.compile(r"\b(show|display|list|view|open|read)\b.*\b(xlsx?|excel|sheet)\b", re.I), Intent.SHOW_EXCEL),
        (re.compile(r"\b(insert|add|put|create)\b.+\brow\b", re.I), Intent.INSERT),
        (re.compile(r"\b(insert|add|append)\b.+\b\d+\b", re.I), Intent.INSERT),
        (re.compile(r"\b(show|select|get|fetch|list|find|display|pull|retrieve)\b", re.I), Intent.SELECT),
        (re.compile(r"\b(update|change|modify|edit|set)\b", re.I), Intent.UPDATE),
        (re.compile(r"\b(delete|remove|drop\s+rows?|erase)\b", re.I), Intent.DELETE),
        (re.compile(r"\b(drop\s+table|destroy\s+table)\b", re.I), Intent.DROP_TABLE),
        (re.compile(r"\b(chart|graph|plot|visuali[sz]e|bar\s+chart|line\s+chart|pie)\b", re.I), Intent.VISUALIZE),
        (re.compile(r"\b(describe|schema|structure|columns|info)\b", re.I), Intent.DESCRIBE),
    ]

    _CHART_MAP: list[tuple[re.Pattern, str]] = [
        (re.compile(r"\bbar\b", re.I), "bar"),
        (re.compile(r"\bline\b", re.I), "line"),
        (re.compile(r"\bpie\b", re.I), "pie"),
        (re.compile(r"\bscatter\b", re.I), "scatter"),
        (re.compile(r"\bhist(ogram)?\b", re.I), "histogram"),
    ]

    def parse(self, command: str) -> ActionPlan:
        plan = ActionPlan(raw_command=command)
        plan.intent = self._detect_intent(command)
        plan.table_name = self._extract_table(command)
        plan.columns = self._extract_columns(command)
        plan.conditions = self._extract_conditions(command)
        plan.limit = self._extract_limit(command)
        plan.file_path = self._extract_file(command)
        plan.chart_type = self._detect_chart_type(command)
        return plan

    # ── helpers ───────────────────────────────────────────────────────────────

    def _detect_intent(self, cmd: str) -> Intent:
        for pattern, intent in self._INTENT_MAP:
            if pattern.search(cmd):
                return intent
        return Intent.UNKNOWN

    def _extract_table(self, cmd: str) -> str:
        """Pull the first noun-like token after a table-related keyword."""
        match = re.search(
            r"\b(?:table|of|from|into|for)\b\s+([a-zA-Z_][a-zA-Z0-9_]*)",
            cmd, re.I
        )
        if match:
            return match.group(1).lower()
        # fallback: last bare word that looks like a table name
        words = re.findall(r"[a-zA-Z_][a-zA-Z0-9_]{2,}", cmd)
        stopwords = {
            "create", "make", "show", "insert", "delete", "update", "select",
            "from", "into", "with", "and", "the", "all", "some", "table",
            "chart", "graph", "excel", "mysql", "order", "by", "desc", "asc",
        }
        candidates = [w.lower() for w in words if w.lower() not in stopwords]
        return candidates[-1] if candidates else ""

    def _extract_columns(self, cmd: str) -> list[str]:
        """Extract comma-separated column names after 'with' keyword."""
        match = re.search(r"\bwith\b\s+(.+?)(?:\s+(?:and|order|where|limit)|$)", cmd, re.I)
        if not match:
            return []
        raw = match.group(1)
        cols = re.split(r"[,\s]+(?:and\s+)?", raw)
        return [c.strip().lower().replace(" ", "_") for c in cols if c.strip()]

    def _extract_conditions(self, cmd: str) -> str:
        match = re.search(r"\b(?:where|with\s+condition|that\s+have?)\b\s+(.+?)(?:\s+(?:order|limit)|$)", cmd, re.I)
        return match.group(1).strip() if match else ""

    def _extract_limit(self, cmd: str) -> int | None:
        match = re.search(r"\b(\d+)\b", cmd)
        if match:
            n = int(match.group(1))
            return n if n < 10_000 else None
        return None

    def _extract_file(self, cmd: str) -> str:
        match = re.search(r"(\S+\.xlsx?)", cmd, re.I)
        return match.group(1) if match else ""

    def _detect_chart_type(self, cmd: str) -> str:
        for pattern, chart in self._CHART_MAP:
            if pattern.search(cmd):
                return chart
        return "bar"
