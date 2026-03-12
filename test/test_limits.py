def limit_ok(n: int) -> bool:
    return 1 <= n <= 20

def test_limit_low():
    assert limit_ok(0) is False

def test_limit_high():
    assert limit_ok(999) is False

def test_limit_ok():
    assert limit_ok(5) is True