from __future__ import annotations

import random
import re
from dataclasses import dataclass, field
from typing import Any

from nl_data_assistant.models import ColumnSpec
from nl_data_assistant.utils.cleaning import normalize_identifier
from nl_data_assistant.utils.schema import SchemaMapper


NUMBER_WORDS = {
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
    "single": 1,
    "multiple": 3,
    "few": 3,
    "several": 4,
    "many": 5,
}

TABLE_PROFILES = {
    "student": ["name", "cgpa"],
    "students": ["name", "cgpa"],
    "expense": ["month", "category", "amount"],
    "expenses": ["month", "category", "amount"],
}

PLURAL_FIELD_MAP = {
    "names": "name",
    "students": "name",
    "cgpas": "cgpa",
    "amounts": "amount",
    "months": "month",
    "categories": "category",
}

DEFAULT_NAMES = [
    "Ayush",
    "Riya",
    "Aman",
    "Neha",
    "Karan",
    "Priya",
    "Arjun",
    "Sanya",
    "Vikram",
    "Ishita",
]
DEFAULT_MONTHS = ["January", "February", "March", "April", "May", "June"]
DEFAULT_CATEGORIES = ["Food", "Travel", "Rent", "Books", "Health", "Utilities"]
STOPWORDS = {
    "table",
    "tables",
    "record",
    "records",
    "row",
    "rows",
    "entry",
    "entries",
    "data",
    "values",
    "value",
    "random",
    "multiple",
    "few",
    "several",
    "many",
    "all",
    "with",
    "where",
    "from",
    "into",
    "set",
}
SELECT_LEAD_WORDS = ("show", "list", "display", "fetch", "get", "view", "select")


@dataclass(slots=True)
class GeneratedSQL:
    statement: str
    parameters: dict[str, Any] | list[dict[str, Any]] | None = None
    preview_rows: list[dict[str, Any]] = field(default_factory=list)


def detect_intent(text: str) -> str | None:
    lowered = text.strip().lower()
    if not lowered:
        return None
    if lowered.startswith("select "):
        return "select"
    if any(keyword in lowered for keyword in ("delete", "remove")):
        return "delete"
    if any(keyword in lowered for keyword in ("update", "change", "modify")):
        return "update"
    if any(keyword in lowered for keyword in ("insert", "add", "append", "populate")):
        return "insert"
    if ("create" in lowered or "make" in lowered or "build" in lowered) and "table" in lowered:
        return "create_table"
    if any(keyword in lowered for keyword in ("show", "list", "display", "fetch", "get", "view", "select")):
        return "select"
    return None


def extract_entities(
    text: str,
    intent: str | None = None,
    schema_mapper: SchemaMapper | None = None,
) -> dict[str, Any]:
    schema_mapper = schema_mapper or SchemaMapper()
    intent = intent or detect_intent(text)
    lowered = text.lower()

    entities: dict[str, Any] = {
        "raw_text": text,
        "table_name": extract_table_name(text, intent),
        "count": 1,
        "columns": [],
        "column_specs": [],
        "rows": [],
        "explicit_values": {},
        "assignments": {},
        "conditions": [],
        "random_fields": [],
        "selected_columns": [],
        "order_by": None,
        "order_direction": "ASC",
        "limit": extract_limit(text),
        "delete_all": bool(re.search(r"\b(?:delete|remove)\s+all\b", lowered)),
        "apply_to_all": bool(re.search(r"\bupdate\s+all\b", lowered)),
    }

    if intent == "create_table":
        column_specs = extract_column_specs(text, schema_mapper, entities["table_name"])
        entities["column_specs"] = column_specs
        entities["columns"] = [column.name for column in column_specs]
        return entities

    if intent == "insert":
        entities["count"] = extract_count(text)
        entities["explicit_values"] = extract_insert_values(text)
        entities["random_fields"] = extract_random_fields(text)
        entities["columns"] = resolve_insert_columns(
            table_name=entities["table_name"],
            explicit_values=entities["explicit_values"],
            random_fields=entities["random_fields"],
        )
        entities["rows"] = generate_random_rows(entities)
        return entities

    if intent == "select":
        entities["conditions"] = extract_conditions(text)
        entities["selected_columns"] = extract_selected_columns(text, entities["table_name"])
        entities["order_by"], entities["order_direction"] = extract_ordering(text)
        return entities

    if intent == "update":
        entities["assignments"] = extract_assignments(text)
        entities["conditions"] = extract_conditions(text, trigger_words=("where", "for"))
        return entities

    if intent == "delete":
        entities["conditions"] = extract_conditions(text)
        return entities

    return entities


def generate_random_rows(entities: dict[str, Any]) -> list[dict[str, Any]]:
    count = max(1, int(entities.get("count") or 1))
    columns = list(entities.get("columns") or infer_default_columns(entities.get("table_name")))
    explicit_values = dict(entities.get("explicit_values", {}))
    random_fields = set(entities.get("random_fields", []))
    should_generate_defaults = count > 1 or not explicit_values or bool(random_fields)

    rows: list[dict[str, Any]] = []
    for index in range(count):
        row: dict[str, Any] = {}
        for column in columns:
            if column in explicit_values and column not in random_fields:
                row[column] = explicit_values[column]
            elif should_generate_defaults:
                row[column] = generate_value_for_column(column, index)
        rows.append(row)
    return rows


def generate_sql(
    intent: str,
    entities: dict[str, Any],
    schema_mapper: SchemaMapper | None = None,
) -> GeneratedSQL:
    schema_mapper = schema_mapper or SchemaMapper()
    table_name = normalize_identifier(entities.get("table_name") or "records")

    if intent == "create_table":
        column_specs: list[ColumnSpec] = entities.get("column_specs") or [ColumnSpec("id", "INT")]
        definitions = ", ".join(
            f"`{normalize_identifier(column.name)}` {column.data_type} {'NULL' if column.nullable else 'NOT NULL'}"
            for column in column_specs
        )
        return GeneratedSQL(statement=f"CREATE TABLE IF NOT EXISTS `{table_name}` ({definitions})")

    if intent == "insert":
        rows = entities.get("rows") or generate_random_rows(entities)
        if not rows:
            raise ValueError("I could not build any rows for the insert request.")
        columns = list(entities.get("columns") or rows[0].keys())
        placeholders = ", ".join(f":{column}" for column in columns)
        statement = (
            f"INSERT INTO `{table_name}` "
            f"({', '.join(f'`{column}`' for column in columns)}) "
            f"VALUES ({placeholders})"
        )
        parameters = [{column: row.get(column) for column in columns} for row in rows]
        return GeneratedSQL(statement=statement, parameters=parameters, preview_rows=parameters)

    if intent == "select":
        where_sql, parameters = build_where_clause(entities.get("conditions", []))
        limit = int(entities.get("limit") or 200)
        selected_columns = entities.get("selected_columns") or []
        select_clause = ", ".join(f"`{normalize_identifier(column)}`" for column in selected_columns) if selected_columns else "*"
        statement = f"SELECT {select_clause} FROM `{table_name}`"
        if where_sql:
            statement += f" WHERE {where_sql}"
        if entities.get("order_by"):
            statement += f" ORDER BY `{normalize_identifier(entities['order_by'])}` {entities.get('order_direction', 'ASC')}"
        statement += f" LIMIT {limit}"
        return GeneratedSQL(statement=statement, parameters=parameters)

    if intent == "update":
        assignments = entities.get("assignments", {})
        if not assignments:
            raise ValueError("Update commands need at least one column assignment, for example: set cgpa to 9.2")
        conditions = entities.get("conditions", [])
        if not conditions and not entities.get("apply_to_all"):
            raise ValueError("For safety, update commands need a filter, for example: where name is Ayush")

        set_sql = []
        parameters: dict[str, Any] = {}
        for column, value in assignments.items():
            key = f"set_{column}"
            set_sql.append(f"`{normalize_identifier(column)}` = :{key}")
            parameters[key] = value

        where_sql, where_params = build_where_clause(conditions)
        parameters.update(where_params)
        statement = f"UPDATE `{table_name}` SET {', '.join(set_sql)}"
        if where_sql:
            statement += f" WHERE {where_sql}"
        return GeneratedSQL(statement=statement, parameters=parameters)

    if intent == "delete":
        conditions = entities.get("conditions", [])
        if not conditions and not entities.get("delete_all"):
            raise ValueError("For safety, delete commands need a filter, for example: delete students with cgpa less than 6")
        where_sql, parameters = build_where_clause(conditions)
        statement = f"DELETE FROM `{table_name}`"
        if where_sql:
            statement += f" WHERE {where_sql}"
        return GeneratedSQL(statement=statement, parameters=parameters)

    raise ValueError(f"Unsupported intent: {intent}")


def extract_table_name(text: str, intent: str | None) -> str | None:
    patterns_by_intent = {
        "create_table": [
            r"(?:create|make|build)(?:\s+(?:a|an))?\s+table(?:\s+of|\s+named|\s+called)?\s*(?P<name>[a-zA-Z_][\w]*)",
            r"(?:create|make|build)(?:\s+(?:a|an))?\s+(?P<name>(?!table\b)[a-zA-Z_][\w]*)\s+table",
        ],
        "insert": [
            r"\binto\s+(?P<name>[a-zA-Z_][\w]*)",
            r"(?:insert|add|append|populate)\s+(?:(?:\d+|[a-z]+)\s+)?(?P<name>[a-zA-Z_][\w]*)",
        ],
        "select": [
            r"\b(?:of|from|in)\s+(?P<name>[a-zA-Z_][\w]*)",
            r"(?:show|list|display|fetch|get|view)\s+(?:all\s+)?(?P<name>[a-zA-Z_][\w]*)",
            r"select\s+\*\s+from\s+(?P<name>[a-zA-Z_][\w]*)",
        ],
        "update": [r"(?:update|change|modify)\s+(?P<name>[a-zA-Z_][\w]*)"],
        "delete": [
            r"(?:delete|remove)\s+(?:all\s+)?(?P<name>[a-zA-Z_][\w]*)",
            r"\bfrom\s+(?P<name>[a-zA-Z_][\w]*)",
        ],
    }

    for pattern in patterns_by_intent.get(intent or "", []):
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if not match:
            continue
        candidate = normalize_identifier(match.group("name"))
        if candidate and candidate not in STOPWORDS:
            return candidate
    return None


def extract_count(text: str) -> int:
    lowered = text.lower()
    match = re.search(r"\b(?:insert|add|append|populate)\s+(?P<count>\d+|[a-z]+)\b", lowered)
    if match:
        return coerce_count_token(match.group("count"))

    any_number = re.search(r"\b(?P<count>\d+)\b", lowered)
    if any_number:
        return int(any_number.group("count"))
    return 1


def extract_limit(text: str) -> int:
    lowered = text.lower()
    for pattern in (r"\blimit\s+(?P<limit>\d+)", r"\btop\s+(?P<limit>\d+)", r"\bfirst\s+(?P<limit>\d+)"):
        match = re.search(pattern, lowered)
        if match:
            return int(match.group("limit"))
    return 200


def extract_column_specs(
    text: str,
    schema_mapper: SchemaMapper,
    table_name: str | None,
) -> list[ColumnSpec]:
    with_match = re.search(r"\bwith\b\s+(?P<body>.+)$", text, flags=re.IGNORECASE)
    if with_match:
        column_specs = schema_mapper.parse_columns_from_text(with_match.group("body"))
        if column_specs:
            return column_specs

    inferred_columns = infer_default_columns(table_name)
    return [ColumnSpec(name=column, data_type=schema_mapper.guess_type_from_name(column)) for column in inferred_columns]


def extract_insert_values(text: str) -> dict[str, Any]:
    match = re.search(r"\bwith\b\s+(?P<body>.+)$", text, flags=re.IGNORECASE)
    if not match:
        return {}

    values: dict[str, Any] = {}
    body = match.group("body")
    for fragment in split_fragments(body):
        if "random" in fragment.lower():
            continue
        parsed = parse_assignment_fragment(fragment)
        if parsed:
            column, value = parsed
            values[column] = value
    return values


def extract_assignments(text: str) -> dict[str, Any]:
    match = re.search(r"\bset\b\s+(?P<body>.+?)(?:\bwhere\b|\bfor\b|$)", text, flags=re.IGNORECASE)
    if not match:
        match = re.search(
            r"\b(?:update|change|modify)\b\s+[a-zA-Z_][\w]*\s+(?P<body>.+?)(?:\bwhere\b|\bfor\b|$)",
            text,
            flags=re.IGNORECASE,
        )
    if not match:
        return {}

    assignments: dict[str, Any] = {}
    for fragment in split_fragments(match.group("body")):
        parsed = parse_assignment_fragment(fragment)
        if parsed:
            column, value = parsed
            assignments[column] = value
    return assignments


def extract_conditions(text: str, trigger_words: tuple[str, ...] = ("where", "with", "having", "whose")) -> list[dict[str, Any]]:
    pattern = r"\b(?:" + "|".join(trigger_words) + r")\b\s+(?P<body>.+)$"
    match = re.search(pattern, text, flags=re.IGNORECASE)
    if not match:
        return []

    conditions: list[dict[str, Any]] = []
    for fragment in split_fragments(match.group("body")):
        condition = parse_condition_fragment(fragment)
        if condition:
            conditions.append(condition)
    return conditions


def extract_selected_columns(text: str, table_name: str | None) -> list[str]:
    lowered = text.lower().strip()
    if table_name and re.search(rf"\b(?:all|{re.escape(table_name)})\b", lowered):
        if any(pattern in lowered for pattern in ("show all", "list all", "display all", "view all")):
            return []

    lead_pattern = r"^(?:" + "|".join(SELECT_LEAD_WORDS) + r")\b(?:\s+me)?\s+"
    remainder = re.sub(lead_pattern, "", text.strip(), flags=re.IGNORECASE)
    split_match = re.search(r"\b(of|from|in)\b", remainder, flags=re.IGNORECASE)
    subject = remainder
    if split_match:
        subject = remainder[: split_match.start()]

    subject = subject.strip(" .")
    if not subject:
        return []

    normalized_subject = normalize_identifier(subject)
    if normalized_subject in {"all", normalize_identifier(table_name or "")}:
        return []

    if normalized_subject.startswith("all_"):
        return []

    columns = []
    for fragment in split_fragments(subject):
        column = normalize_random_field(fragment)
        if column and column not in STOPWORDS and column != normalize_identifier(table_name or ""):
            columns.append(column)
    return list(dict.fromkeys(columns))


def extract_ordering(text: str) -> tuple[str | None, str]:
    order_match = re.search(r"\border by\s+(?P<column>[a-zA-Z_][\w\s]*)(?:\s+(?P<direction>asc|desc))?", text, flags=re.IGNORECASE)
    if order_match:
        direction = (order_match.group("direction") or "ASC").upper()
        return normalize_identifier(order_match.group("column")), direction

    highest_match = re.search(r"\b(?:highest|top|largest|max(?:imum)?)\s+(?P<column>[a-zA-Z_][\w\s]*)", text, flags=re.IGNORECASE)
    if highest_match:
        return normalize_identifier(highest_match.group("column")), "DESC"

    lowest_match = re.search(r"\b(?:lowest|smallest|min(?:imum)?)\s+(?P<column>[a-zA-Z_][\w\s]*)", text, flags=re.IGNORECASE)
    if lowest_match:
        return normalize_identifier(lowest_match.group("column")), "ASC"

    return None, "ASC"


def extract_random_fields(text: str) -> list[str]:
    lowered = text.lower()
    fields = []
    for field in re.findall(r"random\s+([a-zA-Z_]+)", lowered):
        normalized = normalize_random_field(field)
        if normalized not in {"data", "value"}:
            fields.append(normalized)
    return list(dict.fromkeys(fields))


def resolve_insert_columns(
    table_name: str | None,
    explicit_values: dict[str, Any],
    random_fields: list[str],
) -> list[str]:
    columns = list(explicit_values.keys())
    for field in random_fields:
        if field not in columns:
            columns.append(field)

    for profile_column in infer_default_columns(table_name):
        if profile_column not in columns:
            columns.append(profile_column)

    return columns or ["name"]


def build_where_clause(conditions: list[dict[str, Any]]) -> tuple[str, dict[str, Any]]:
    if not conditions:
        return "", {}

    clauses = []
    parameters: dict[str, Any] = {}
    for index, condition in enumerate(conditions):
        key = f"condition_{index}"
        clauses.append(f"`{normalize_identifier(condition['column'])}` {condition['operator']} :{key}")
        parameters[key] = condition["value"]
    return " AND ".join(clauses), parameters


def generate_value_for_column(column: str, index: int) -> Any:
    lowered = column.lower()
    if lowered == "name":
        return random.choice(DEFAULT_NAMES)
    if lowered in {"cgpa", "gpa", "score", "rating"}:
        return round(random.uniform(5.0, 10.0), 2)
    if lowered in {"amount", "price", "cost", "salary"}:
        return round(random.uniform(100.0, 5000.0), 2)
    if lowered in {"age", "year", "count", "quantity"}:
        return random.randint(18, 30)
    if lowered == "month":
        return random.choice(DEFAULT_MONTHS)
    if lowered == "category":
        return random.choice(DEFAULT_CATEGORIES)
    if lowered == "email":
        return f"user{index + 1}@example.com"
    return f"{column}_{index + 1}"


def infer_default_columns(table_name: str | None) -> list[str]:
    if not table_name:
        return ["name"]
    return TABLE_PROFILES.get(table_name, TABLE_PROFILES.get(table_name.rstrip("s"), ["name"]))


def parse_assignment_fragment(fragment: str) -> tuple[str, Any] | None:
    cleaned = fragment.strip(" ,.")
    cleaned = re.sub(r"^(with|set)\s+", "", cleaned, flags=re.IGNORECASE)
    if not cleaned:
        return None

    patterns = [
        r"(?P<column>[a-zA-Z_][\w\s]*)\s*(?:=|to|is)\s*(?P<value>.+)",
        r"(?P<column>[a-zA-Z_][\w\s]*)\s+(?P<value>.+)",
    ]
    for pattern in patterns:
        match = re.match(pattern, cleaned, flags=re.IGNORECASE)
        if not match:
            continue
        column = normalize_identifier(match.group("column"))
        value = coerce_scalar(match.group("value"))
        if column and column not in STOPWORDS:
            return column, value
    return None


def parse_condition_fragment(fragment: str) -> dict[str, Any] | None:
    cleaned = fragment.strip(" ,.")
    if not cleaned:
        return None

    pattern_map = [
        (r"(?P<column>[a-zA-Z_][\w\s]*)\s+less than or equal to\s+(?P<value>.+)", "<="),
        (r"(?P<column>[a-zA-Z_][\w\s]*)\s+greater than or equal to\s+(?P<value>.+)", ">="),
        (r"(?P<column>[a-zA-Z_][\w\s]*)\s+not equal to\s+(?P<value>.+)", "!="),
        (r"(?P<column>[a-zA-Z_][\w\s]*)\s+less than\s+(?P<value>.+)", "<"),
        (r"(?P<column>[a-zA-Z_][\w\s]*)\s+greater than\s+(?P<value>.+)", ">"),
        (r"(?P<column>[a-zA-Z_][\w\s]*)\s+(?:below|under)\s+(?P<value>.+)", "<"),
        (r"(?P<column>[a-zA-Z_][\w\s]*)\s+(?:above|over)\s+(?P<value>.+)", ">"),
        (r"(?P<column>[a-zA-Z_][\w\s]*)\s+(?:equals|equal to|is)\s+(?P<value>.+)", "="),
    ]

    for pattern, operator in pattern_map:
        match = re.match(pattern, cleaned, flags=re.IGNORECASE)
        if match:
            return {
                "column": normalize_identifier(match.group("column")),
                "operator": operator,
                "value": coerce_scalar(match.group("value")),
            }

    direct_operator_match = re.match(
        r"(?P<column>[a-zA-Z_][\w\s]*)\s*(?P<operator><=|>=|!=|=|<|>)\s*(?P<value>.+)",
        cleaned,
        flags=re.IGNORECASE,
    )
    if direct_operator_match:
        return {
            "column": normalize_identifier(direct_operator_match.group("column")),
            "operator": direct_operator_match.group("operator"),
            "value": coerce_scalar(direct_operator_match.group("value")),
        }
    return None


def split_fragments(text: str) -> list[str]:
    return [fragment.strip() for fragment in re.split(r",|\band\b", text, flags=re.IGNORECASE) if fragment.strip()]


def coerce_count_token(token: str) -> int:
    if token.isdigit():
        return int(token)
    return NUMBER_WORDS.get(token.lower(), 1)


def coerce_scalar(value: str) -> Any:
    cleaned = value.strip().strip("'\"")
    if re.fullmatch(r"-?\d+", cleaned):
        return int(cleaned)
    if re.fullmatch(r"-?\d+\.\d+", cleaned):
        return float(cleaned)
    return cleaned


def normalize_random_field(field: str) -> str:
    lowered = field.strip().lower()
    lowered = PLURAL_FIELD_MAP.get(lowered, lowered.rstrip("s"))
    return normalize_identifier(lowered)
