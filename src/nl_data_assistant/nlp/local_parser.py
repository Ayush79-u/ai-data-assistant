"""
local_parser.py - Rule-based local parser.
"""
from __future__ import annotations

import re

from nl_data_assistant.models import ActionPlan, Intent


class LocalParser:
    """Keyword and regex intent detector with broad synonym coverage."""

    _SELECT_VERBS = r"(?:show|select|get|fetch|list|find|display|pull|retrieve)"

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
        plan.table_name = self._extract_table(command, plan.intent)
        plan.columns = self._extract_columns(command, plan.intent)
        plan.conditions = self._extract_conditions(command, plan.intent)
        plan.limit = self._extract_limit(command, plan.intent)
        plan.file_path = self._extract_file(command)
        plan.chart_type = self._detect_chart_type(command)
        return plan

    def _detect_intent(self, cmd: str) -> Intent:
        for pattern, intent in self._INTENT_MAP:
            if pattern.search(cmd):
                return intent
        return Intent.UNKNOWN

    def _extract_table(self, cmd: str, intent: Intent) -> str:
        match = re.search(
            r"\b(?:table|of|from|into|for)\b\s+([a-zA-Z_][a-zA-Z0-9_]*)",
            cmd,
            re.I,
        )
        if match:
            return match.group(1).lower()

        if intent == Intent.DELETE:
            delete_match = re.search(
                r"\b(?:delete|remove|erase|drop\s+rows?)\b\s+([a-zA-Z_][a-zA-Z0-9_]*)",
                cmd,
                re.I,
            )
            if delete_match:
                return delete_match.group(1).lower()

        if intent == Intent.INSERT:
            insert_match = re.search(
                r"\b(?:insert|add|append)\b(?:\s+\d+)?\s+([a-zA-Z_][a-zA-Z0-9_]*)",
                cmd,
                re.I,
            )
            if insert_match:
                return insert_match.group(1).lower()

        if intent == Intent.SELECT:
            select_match = re.search(
                rf"\b{self._SELECT_VERBS}\b\s+all\s+([a-zA-Z_][a-zA-Z0-9_]*)",
                cmd,
                re.I,
            )
            if select_match:
                return select_match.group(1).lower()
            if re.search(
                r"\b([a-zA-Z_][a-zA-Z0-9_]*)\s+"
                r"(?:starting|starts|ending|ends|contains|containing)\b",
                cmd,
                re.I,
            ):
                return ""
            if re.search(r"\bnames?\s+with\b", cmd, re.I):
                return ""
            if re.search(
                r"\b(?:people|employees|workers?)\s+(?:working\s+in|working\s+before|working\s+after|working\s+on|joined\s+before|joined\s+after|hired\s+before|hired\s+after)\b",
                cmd,
                re.I,
            ):
                return ""

        words = re.findall(r"[a-zA-Z_][a-zA-Z0-9_]{2,}", cmd)
        stopwords = {
            "create", "make", "show", "insert", "delete", "update", "select",
            "from", "into", "with", "and", "the", "all", "some", "table",
            "chart", "graph", "excel", "mysql", "order", "by", "desc", "asc",
            "starting", "starts", "ending", "ends", "contains", "containing",
            "where", "rows", "data", "less", "greater", "than", "equal",
            "equals", "starting_with", "limit", "top", "first",
        }
        candidates = [w.lower() for w in words if w.lower() not in stopwords]
        return candidates[-1] if candidates else ""

    def _extract_columns(self, cmd: str, intent: Intent) -> list[str]:
        if intent == Intent.CREATE_TABLE:
            match = re.search(r"\bwith\b\s+(.+?)(?:\s+(?:and|order|where|limit)|$)", cmd, re.I)
            if not match:
                return []
            raw = match.group(1)
            cols = re.split(r",|\band\b", raw, flags=re.I)
            return [self._normalize_identifier(c) for c in cols if self._normalize_identifier(c)]

        if intent in {Intent.SELECT, Intent.VISUALIZE}:
            explicit_from = re.search(
                rf"\b{self._SELECT_VERBS}\b\s+(.+?)\s+\b(?:from|of)\b",
                cmd,
                re.I,
            )
            if explicit_from:
                raw = explicit_from.group(1).strip().lower()
                if raw not in {"all", "all data", "data", "everything", "rows"}:
                    cols = re.split(r",|\band\b", explicit_from.group(1), flags=re.I)
                    normalized = [
                        self._normalize_identifier(col)
                        for col in cols
                        if self._normalize_identifier(col)
                    ]
                    return normalized

            text_filter = re.search(
                r"\b([a-zA-Z_][a-zA-Z0-9_]*)\s+"
                r"(?:starting|starts|ending|ends|contains|containing)\b",
                cmd,
                re.I,
            )
            if text_filter:
                return [self._normalize_identifier(text_filter.group(1))]

            names_with = re.search(
                r"\bnames?\s+with\s+(.+?)(?:\s+(?:order|limit)\b|$)",
                cmd,
                re.I,
            )
            if names_with:
                return ["name"]

        return []

    def _extract_conditions(self, cmd: str, intent: Intent) -> str:
        del intent

        text_patterns = [
            (
                r"\b([a-zA-Z_][a-zA-Z0-9_]*)\s+(?:starting|starts)\s+with\s+(.+?)(?:\s+(?:order|limit)\b|$)",
                lambda col, val: f"LOWER(`{col}`) LIKE {self._sql_literal(val.lower() + '%')}",
            ),
            (
                r"\b([a-zA-Z_][a-zA-Z0-9_]*)\s+(?:ending|ends)\s+with\s+(.+?)(?:\s+(?:order|limit)\b|$)",
                lambda col, val: f"LOWER(`{col}`) LIKE {self._sql_literal('%' + val.lower())}",
            ),
            (
                r"\b([a-zA-Z_][a-zA-Z0-9_]*)\s+(?:contains|containing)\s+(.+?)(?:\s+(?:order|limit)\b|$)",
                lambda col, val: f"LOWER(`{col}`) LIKE {self._sql_literal('%' + val.lower() + '%')}",
            ),
        ]
        for pattern, builder in text_patterns:
            match = re.search(pattern, cmd, re.I)
            if match:
                column = self._normalize_identifier(match.group(1))
                value = self._clean_value(match.group(2))
                if column and value:
                    return builder(column, value)

        names_with = re.search(
            r"\bnames?\s+with\s+(.+?)(?:\s+(?:order|limit)\b|$)",
            cmd,
            re.I,
        )
        if names_with:
            value = self._clean_value(names_with.group(1))
            if value:
                return f"LOWER(`name`) LIKE {self._sql_literal('%' + value.lower() + '%')}"

        department_match = re.search(
            r"\b(?:people|employees|workers?)\s+working\s+in\s+(.+?)(?:\s+(?:order|limit)\b|$)",
            cmd,
            re.I,
        )
        if department_match:
            value = self._clean_value(department_match.group(1))
            if value:
                return f"LOWER(`department`) = {self._sql_literal(value.lower())}"

        date_patterns = [
            (r"\b(?:people|employees|workers?)\s+(?:working|joined|hired)\s+before\s+(\d{4}-\d{2}-\d{2})\b", "<"),
            (r"\b(?:people|employees|workers?)\s+(?:working|joined|hired)\s+after\s+(\d{4}-\d{2}-\d{2})\b", ">"),
            (r"\b(?:people|employees|workers?)\s+(?:working|joined|hired)\s+on\s+(\d{4}-\d{2}-\d{2})\b", "="),
        ]
        for pattern, operator in date_patterns:
            match = re.search(pattern, cmd, re.I)
            if match:
                return f"DATE(`hiredate`) {operator} {self._sql_literal(match.group(1))}"

        comparison_patterns = [
            (r"(?:where|with|that\s+have?\s+)?\s*([a-zA-Z_][a-zA-Z0-9_]*)\s+(?:is\s+)?less than or equal to\s+(.+?)(?:\s+(?:order|limit)\b|$)", "<="),
            (r"(?:where|with|that\s+have?\s+)?\s*([a-zA-Z_][a-zA-Z0-9_]*)\s+(?:is\s+)?greater than or equal to\s+(.+?)(?:\s+(?:order|limit)\b|$)", ">="),
            (r"(?:where|with|that\s+have?\s+)?\s*([a-zA-Z_][a-zA-Z0-9_]*)\s+(?:is\s+)?less than\s+(.+?)(?:\s+(?:order|limit)\b|$)", "<"),
            (r"(?:where|with|that\s+have?\s+)?\s*([a-zA-Z_][a-zA-Z0-9_]*)\s+(?:is\s+)?greater than\s+(.+?)(?:\s+(?:order|limit)\b|$)", ">"),
            (r"(?:where|with|that\s+have?\s+)?\s*([a-zA-Z_][a-zA-Z0-9_]*)\s+(?:is\s+)?(?:equal to|equals?)\s+(.+?)(?:\s+(?:order|limit)\b|$)", "="),
            (r"(?:where|with|that\s+have?\s+)?\s*([a-zA-Z_][a-zA-Z0-9_]*)\s+(?:is\s+)?(?:not equal to|not equals?)\s+(.+?)(?:\s+(?:order|limit)\b|$)", "!="),
        ]
        for pattern, operator in comparison_patterns:
            match = re.search(pattern, cmd, re.I)
            if match:
                column = self._normalize_identifier(match.group(1))
                value = self._clean_value(match.group(2))
                if column and value:
                    return f"`{column}` {operator} {self._sql_literal(value)}"

        match = re.search(
            r"\b(?:where|with\s+condition|that\s+have?)\b\s+(.+?)(?:\s+(?:order|limit)|$)",
            cmd,
            re.I,
        )
        return match.group(1).strip() if match else ""

    def _extract_limit(self, cmd: str, intent: Intent) -> int | None:
        if intent not in {Intent.SELECT, Intent.VISUALIZE}:
            return None

        match = re.search(r"\blimit\s+(\d+)\b", cmd, re.I)
        if not match:
            match = re.search(r"\b(?:top|first)\s+(\d+)\b", cmd, re.I)
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

    @staticmethod
    def _normalize_identifier(value: str) -> str:
        return re.sub(r"[^a-zA-Z0-9_]+", "_", value.strip().lower()).strip("_")

    @staticmethod
    def _clean_value(value: str) -> str:
        cleaned = value.strip().strip("'\"")
        cleaned = re.sub(r"\s+(?:order|limit)\b.*$", "", cleaned, flags=re.I)
        return cleaned.strip()

    @staticmethod
    def _sql_literal(value: str) -> str:
        stripped = value.strip()
        if re.fullmatch(r"-?\d+", stripped):
            return stripped
        if re.fullmatch(r"-?\d+\.\d+", stripped):
            return stripped
        escaped = stripped.replace("\\", "\\\\").replace("'", "''")
        return f"'{escaped}'"
