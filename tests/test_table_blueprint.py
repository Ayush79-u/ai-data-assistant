from nl_data_assistant.nlp.table_blueprint import command_to_blueprint


def test_command_to_blueprint_returns_mysql_friendly_json():
    blueprint = command_to_blueprint("Create a table of students with name and CGPA", sample_rows=3)

    assert blueprint["table_name"] == "students"
    assert blueprint["columns"] == [
        {"name": "name", "type": "VARCHAR(255)"},
        {"name": "cgpa", "type": "FLOAT"},
    ]
    assert len(blueprint["sample_data"]) == 3
    assert isinstance(blueprint["sample_data"][0]["name"], str)
    assert isinstance(blueprint["sample_data"][0]["cgpa"], float)


def test_command_to_blueprint_handles_product_style_command():
    blueprint = command_to_blueprint("make table of products with price and quantity", sample_rows=4)

    assert blueprint["table_name"] == "products"
    assert blueprint["columns"] == [
        {"name": "price", "type": "FLOAT"},
        {"name": "quantity", "type": "INT"},
    ]
    assert len(blueprint["sample_data"]) == 4
