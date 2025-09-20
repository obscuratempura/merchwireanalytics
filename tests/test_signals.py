from app.logic import signals


def test_percent_change_basic():
    assert signals.percent_change(120, 100) == 0.2
    assert signals.percent_change(None, 100) is None
    assert signals.percent_change(100, None) is None


def test_discount_percentage():
    assert signals.discount_percentage(80, 100) == 0.2
    assert signals.discount_percentage(100, 100) == 0.0
    assert signals.discount_percentage(None, 100) == 0.0


def test_discount_spike():
    assert signals.discount_spike(0.1, 0.25)
    assert not signals.discount_spike(0.1, 0.15)


def test_ad_surge():
    assert signals.ad_surge(20, [5, 5, 5])
    assert not signals.ad_surge(6, [5, 5, 5])


def test_normalized():
    assert signals.normalized([1, 2, 3]) == [0.0, 0.5, 1.0]
    assert signals.normalized([1, 1, 1]) == [0.0, 0.0, 0.0]
