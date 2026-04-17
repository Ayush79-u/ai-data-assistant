import random

from nl_data_assistant.nlp.local_parser import detect_intent, extract_entities, generate_sql
from nl_data_assistant.utils.schema import SchemaMapper


def test_detect_intent_handles_flexible_phrases():
    assert detect_intent("add 3 students") == "insert"
    assert detect_intent("show all students") == "select"
    assert detect_intent("delete students with cgpa less than 6") == "delete"


def test_extract_entities_for_random_student_insert():
    random.seed(7)
    entities = extract_entities("insert 5 students with random CGPA", intent="insert", schema_mapper=SchemaMapper())

    assert entities["table_name"] == "students"
    assert entities["count"] == 5
    assert "cgpa" in entities["columns"]
    assert "name" in entities["columns"]
    assert len(entities["rows"]) == 5
    assert all("cgpa" in row for row in entities["rows"])


def test_generate_sql_for_show_all_students():
    entities = extract_entities("show all students", intent="select", schema_mapper=SchemaMapper())
    generated = generate_sql("select", entities, schema_mapper=SchemaMapper())

    assert generated.statement == "SELECT * FROM `students` LIMIT 200"
    assert generated.parameters == {}


def test_generate_sql_for_delete_with_condition():
    entities = extract_entities(
        "delete students with CGPA less than 6",
        intent="delete",
        schema_mapper=SchemaMapper(),
    )
    generated = generate_sql("delete", entities, schema_mapper=SchemaMapper())

    assert generated.statement == "DELETE FROM `students` WHERE `cgpa` < :condition_0"
    assert generated.parameters == {"condition_0": 6}
