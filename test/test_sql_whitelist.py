import re

BLOCKED = ["drop", "delete", "update", "insert", "alter", "truncate"]

def is_safe_sql(sql: str) -> bool:
    s = sql.lower()
    if not s.strip().startswith("select"):
        return False
    return not any(word in s for word in BLOCKED)

def test_blocks_delete():
    assert is_safe_sql("DELETE FROM orders") is False

def test_allows_select():
    assert is_safe_sql("SELECT * FROM orders") is True