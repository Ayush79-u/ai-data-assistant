from nl_data_assistant.nlp.local_parser import extract_entities
from nl_data_assistant.nlp.mysql_query_generator import MySQLQueryGenerator
from nl_data_assistant.utils.schema import SchemaMapper


def test_schema_aware_generator_resolves_table_and_columns():
    generator = MySQLQueryGenerator(
        schema_snapshot={"students": ["student_name", "cgpa", "semester"]},
        schema_mapper=SchemaMapper(),
    )
    entities = extract_entities("show names and cgpa of student", intent="select", schema_mapper=SchemaMapper())

    refined = generator.refine_entities(entities, "select")
    generated = generator.generate("select", entities)

    assert refined["table_name"] == "students"
    assert refined["selected_columns"] == ["student_name", "cgpa"]
    assert generated.statement == "SELECT `student_name`, `cgpa` FROM `students` LIMIT 200"


def test_schema_aware_insert_ignores_system_columns():
    generator = MySQLQueryGenerator(
        schema_snapshot={"students": ["id", "name", "cgpa", "created_at"]},
        schema_mapper=SchemaMapper(),
    )
    entities = extract_entities("insert 2 students with random cgpa", intent="insert", schema_mapper=SchemaMapper())

    refined = generator.refine_entities(entities, "insert")

    assert "id" not in refined["columns"]
    assert "created_at" not in refined["columns"]
    assert refined["columns"] == ["name", "cgpa"]
