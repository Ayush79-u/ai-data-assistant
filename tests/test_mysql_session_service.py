from nl_data_assistant.services.mysql_session_service import MySQLSessionService


def test_escape_literal_percents_for_driver_sql_handles_like_patterns():
    raw_sql = "SELECT `name` FROM `employee` WHERE LOWER(`name`) LIKE '%e%';"

    escaped_sql = MySQLSessionService._escape_literal_percents_for_driver_sql(raw_sql)

    assert escaped_sql == "SELECT `name` FROM `employee` WHERE LOWER(`name`) LIKE '%%e%%';"


def test_escape_literal_percents_for_driver_sql_preserves_existing_double_percents():
    raw_sql = "SELECT '%% already escaped', `name` FROM `employee` WHERE `score` LIKE 'e%%';"

    escaped_sql = MySQLSessionService._escape_literal_percents_for_driver_sql(raw_sql)

    assert escaped_sql == raw_sql
