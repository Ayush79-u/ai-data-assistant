from nl_data_assistant.nlp.interpreter import CommandInterpreter


def test_create_table_command_maps_to_mysql_with_inferred_columns():
    interpreter = CommandInterpreter()
    plan = interpreter.interpret("Create a table of students with name and CGPA", default_target="mysql")

    assert plan.action == "create_table"
    assert plan.target == "mysql"
    assert plan.table_name == "students"
    assert [column.name for column in plan.columns] == ["name", "cgpa"]


def test_visualization_command_maps_to_chart_request():
    interpreter = CommandInterpreter()
    plan = interpreter.interpret("Show me a bar chart of monthly expenses", default_target="mysql")

    assert plan.action == "visualize"
    assert plan.chart_type == "bar"
    assert plan.table_name == "monthly_expenses"


def test_excel_import_command_maps_to_sync_action():
    interpreter = CommandInterpreter()
    plan = interpreter.interpret("Import expenses.xlsx to MySQL as monthly_expenses", default_target="mysql")

    assert plan.action == "excel_to_mysql"
    assert plan.source_path == "expenses.xlsx"
    assert plan.table_name == "monthly_expenses"

