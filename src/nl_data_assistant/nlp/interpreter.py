from __future__ import annotations

import json
import re
from dataclasses import dataclass

from nl_data_assistant.models import ActionPlan, ColumnSpec
from nl_data_assistant.nlp.local_parser import detect_intent, extract_entities
from nl_data_assistant.nlp.mysql_query_generator import MySQLQueryGenerator
from nl_data_assistant.utils.cleaning import normalize_identifier
from nl_data_assistant.utils.schema import SchemaMapper


def _match_group(pattern: str, text: str, group: str) -> str | None:
    match = re.search(pattern, text, flags=re.IGNORECASE)
    if not match:
        return None
    return match.group(group).strip()


@dataclass(slots=True)
class LLMParser:
    api_key: str
    model: str

    def parse(self, command: str) -> ActionPlan | None:
        try:
            from openai import OpenAI
        except ImportError:
            return None

        prompt = f"""
You convert natural language data commands into JSON.
Return only valid JSON with these keys:
action, target, table_name, sheet_name, workbook_path, source_path, destination_path,
columns, query, parameters, entities, chart_type, x_column, y_column, title, limit, use_last_result, notes.

Command: {command}
"""
        try:
            client = OpenAI(api_key=self.api_key)
            response = client.responses.create(model=self.model, input=prompt)
            payload = json.loads(response.output_text)
            columns = [
                ColumnSpec(
                    name=str(item.get("name", "column")),
                    data_type=str(item.get("data_type", "VARCHAR(255)")),
                    nullable=bool(item.get("nullable", True)),
                )
                for item in payload.get("columns", [])
                if isinstance(item, dict)
            ]
            return ActionPlan(
                action=payload.get("action", "unknown"),
                target=payload.get("target"),
                table_name=payload.get("table_name"),
                sheet_name=payload.get("sheet_name"),
                workbook_path=payload.get("workbook_path"),
                source_path=payload.get("source_path"),
                destination_path=payload.get("destination_path"),
                columns=columns,
                query=payload.get("query"),
                parameters=payload.get("parameters"),
                chart_type=payload.get("chart_type"),
                x_column=payload.get("x_column"),
                y_column=payload.get("y_column"),
                title=payload.get("title"),
                limit=int(payload.get("limit", 200)),
                use_last_result=bool(payload.get("use_last_result", False)),
                entities=dict(payload.get("entities", {})),
                notes=list(payload.get("notes", [])),
            )
        except Exception:
            return None


class RuleBasedInterpreter:
    def __init__(self, schema_mapper: SchemaMapper) -> None:
        self.schema_mapper = schema_mapper

    def parse(self, command: str, default_target: str | None = None, mysql_schema: dict[str, list[str]] | None = None) -> ActionPlan:
        text = command.strip()
        lowered = text.lower()
        target = self._detect_target(lowered, default_target)

        if lowered.startswith("select "):
            return ActionPlan(action="query", target="mysql", query=text, notes=["Raw SQL query detected."])

        if any(keyword in lowered for keyword in ("import", "load", "transfer")) and "mysql" in lowered:
            source_path = _match_group(r"(?P<path>[^\s]+\.xlsx?)", text, "path")
            table_name = _match_group(r"\bas\s+(?P<name>[\w\s]+)$", text, "name")
            if not table_name:
                table_name = _match_group(r"\bto mysql(?: table)?\s+(?P<name>[\w\s]+)", text, "name")
            return ActionPlan(
                action="excel_to_mysql",
                target="mysql",
                source_path=source_path,
                table_name=normalize_identifier(table_name or "imported_data"),
            )

        if "export" in lowered and "excel" in lowered:
            table_name = _match_group(r"(?:table|from)\s+(?P<name>[\w\s]+?)(?:\s+to|\s*$)", text, "name")
            destination_path = _match_group(r"(?P<path>[^\s]+\.xlsx?)", text, "path")
            return ActionPlan(
                action="mysql_to_excel",
                target="mysql",
                table_name=normalize_identifier(table_name or "export_data"),
                destination_path=destination_path,
            )

        if any(keyword in lowered for keyword in ("clean", "sanitize", "normalize")):
            source_path = _match_group(r"(?P<path>[^\s]+\.xlsx?)", text, "path")
            table_name = _match_group(r"(?:table|sheet)\s+(?P<name>[\w\s]+)", text, "name")
            return ActionPlan(
                action="clean_data",
                target=target,
                source_path=source_path,
                table_name=normalize_identifier(table_name) if table_name else None,
                sheet_name=normalize_identifier(table_name) if target == "excel" and table_name else None,
            )

        if any(keyword in lowered for keyword in ("schema", "structure", "describe")):
            name = _match_group(r"(?:of|for)\s+(?P<name>[\w\s]+)$", text, "name")
            safe_name = normalize_identifier(name or "data")
            return ActionPlan(
                action="describe_schema",
                target=target,
                table_name=safe_name,
                sheet_name=safe_name if target == "excel" else None,
            )

        chart_type = self._detect_chart_type(lowered)
        if chart_type:
            entity = _match_group(r"(?:of|for)\s+(?P<name>[\w\s]+?)(?:\s+from|\s+in|\s+using|\s*$)", text, "name")
            x_column = _match_group(r"\bx\s+(?:axis\s+)?(?:as|=|:)?\s*(?P<name>[\w_]+)", text, "name")
            y_column = _match_group(r"\by\s+(?:axis\s+)?(?:as|=|:)?\s*(?P<name>[\w_]+)", text, "name")
            safe_entity = normalize_identifier(entity or "")
            return ActionPlan(
                action="visualize",
                target=target,
                table_name=safe_entity or None,
                sheet_name=safe_entity if (target == "excel" and safe_entity) else None,
                chart_type=chart_type,
                x_column=normalize_identifier(x_column) if x_column else None,
                y_column=normalize_identifier(y_column) if y_column else None,
                title=text,
                use_last_result=not bool(safe_entity),
            )

        local_intent = detect_intent(text)
        if local_intent and target != "excel" and not any(token in lowered for token in ("sheet", "worksheet", "workbook")):
            local_plan = self._build_local_sql_plan(text, local_intent, mysql_schema=mysql_schema)
            if local_plan:
                return local_plan

        create_match = re.search(
            r"create(?:\s+(?:a|an))?\s+(?P<object>excel sheet|sheet|worksheet)"
            r"(?:\s+of|\s+named)?\s*(?P<name>[\w\s]+?)(?:\s+with\s+(?P<columns>.+))?$",
            text,
            flags=re.IGNORECASE,
        )
        if create_match:
            object_type = create_match.group("object").lower()
            inferred_target = "excel" if "sheet" in object_type else "mysql"
            name = create_match.group("name")
            columns_phrase = create_match.group("columns") or ""
            safe_name = normalize_identifier(name or ("sheet1" if inferred_target == "excel" else "new_table"))
            columns = self.schema_mapper.parse_columns_from_text(columns_phrase)
            return ActionPlan(
                action="create_table",
                target=target or inferred_target,
                table_name=safe_name if (target or inferred_target) == "mysql" else None,
                sheet_name=safe_name if (target or inferred_target) == "excel" else None,
                columns=columns,
                notes=["Column types were inferred from the natural-language request."],
            )

        if target == "excel" and any(keyword in lowered for keyword in ("show", "list", "display", "get", "fetch")):
            name = _match_group(r"(?:show|list|display|get|fetch)(?: me)?\s+(?P<name>[\w\s]+?)(?:\s+from|\s+in|\s*$)", text, "name")
            safe_name = normalize_identifier(name or "data")
            return ActionPlan(
                action="query",
                target=target,
                table_name=safe_name if target != "excel" else None,
                sheet_name=safe_name if target == "excel" else None,
                limit=200,
            )

        return ActionPlan(
            action="unknown",
            target=target,
            notes=[
                "The command could not be mapped to a supported action.",
                "Try create, show, import, export, clean, describe, or chart requests.",
            ],
        )

    def _build_local_sql_plan(self, text: str, intent: str, mysql_schema: dict[str, list[str]] | None = None) -> ActionPlan | None:
        entities = extract_entities(text, intent=intent, schema_mapper=self.schema_mapper)
        if not entities.get("table_name"):
            return None

        generator = MySQLQueryGenerator(schema_snapshot=mysql_schema, schema_mapper=self.schema_mapper)
        try:
            refined_entities = generator.refine_entities(entities, intent)
            generated = generator.generate(intent, refined_entities)
        except ValueError as exc:
            return ActionPlan(
                action="unknown",
                target="mysql",
                table_name=entities.get("table_name"),
                entities=entities,
                notes=[str(exc)],
            )

        action_map = {
            "create_table": "create_table",
            "insert": "insert",
            "select": "query",
            "update": "update",
            "delete": "delete",
        }
        notes = [f"Local parser detected intent '{intent}' and generated parameterized SQL."]
        if generated.preview_rows:
            notes.append(f"Prepared {len(generated.preview_rows)} row(s) for insert.")

        return ActionPlan(
            action=action_map[intent],
            target="mysql",
            table_name=refined_entities.get("table_name"),
            columns=refined_entities.get("column_specs", []),
            query=generated.statement,
            parameters=generated.parameters,
            entities=refined_entities,
            limit=int(refined_entities.get("limit", 200) or 200),
            notes=notes,
        )

    def _detect_target(self, lowered: str, default_target: str | None) -> str | None:
        if any(token in lowered for token in ("excel", "sheet", "workbook", ".xlsx", ".xls")):
            return "excel"
        if any(token in lowered for token in ("mysql", "database", "db", "sql")):
            return "mysql"
        return default_target

    def _detect_chart_type(self, lowered: str) -> str | None:
        for chart_type in ("bar", "line", "pie", "scatter", "histogram", "dashboard"):
            if chart_type in lowered:
                return chart_type
        if "chart" in lowered or "plot" in lowered or "graph" in lowered:
            return "bar"
        return None


class CommandInterpreter:
    def __init__(self, openai_api_key: str = "", openai_model: str = "") -> None:
        self.schema_mapper = SchemaMapper()
        self.rule_parser = RuleBasedInterpreter(self.schema_mapper)
        self.llm_parser = LLMParser(openai_api_key, openai_model) if (openai_api_key and openai_model) else None

    def interpret(
        self,
        command: str,
        default_target: str | None = None,
        mysql_schema: dict[str, list[str]] | None = None,
    ) -> ActionPlan:
        rule_plan = self.rule_parser.parse(command, default_target=default_target, mysql_schema=mysql_schema)
        if rule_plan.action != "unknown":
            return rule_plan
        if self.llm_parser:
            llm_plan = self.llm_parser.parse(command)
            if llm_plan:
                return llm_plan
        return rule_plan
