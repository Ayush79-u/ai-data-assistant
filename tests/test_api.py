from pathlib import Path

from fastapi.testclient import TestClient

from nl_data_assistant.api import create_api_app
from nl_data_assistant.config import AppConfig


def test_api_health_and_parse_endpoints():
    config = AppConfig.from_env(Path.cwd())
    app = create_api_app(config)
    client = TestClient(app)

    health = client.get("/health")
    assert health.status_code == 200
    assert health.json()["status"] == "ok"

    parsed = client.post("/parse", json={"command": "show all students", "target": "mysql"})
    assert parsed.status_code == 200
    payload = parsed.json()
    assert payload["success"] is True
    assert payload["sql"] == "SELECT * FROM `students` LIMIT 200"

    blueprint = client.post("/blueprint", json={"command": "create employee table with name, salary", "sample_rows": 3})
    assert blueprint.status_code == 200
    blueprint_payload = blueprint.json()
    assert blueprint_payload["table_name"] == "employee"
    assert blueprint_payload["columns"] == [
        {"name": "name", "type": "VARCHAR(255)"},
        {"name": "salary", "type": "FLOAT"},
    ]
