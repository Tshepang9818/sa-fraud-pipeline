from main import detect_fraud

def test_high_amount_flagged():
    tx = {"amount_zar": 25000, "timestamp": "2024-01-01T10:00:00", "card_number": "4111-****-****-0001"}
    assert "HIGH_AMOUNT" in detect_fraud(tx)

def test_normal_amount_not_flagged():
    tx = {"amount_zar": 250, "timestamp": "2024-01-01T10:00:00", "card_number": "4111-****-****-0002"}
    assert "HIGH_AMOUNT" not in detect_fraud(tx)

def test_odd_hours_flagged():
    tx = {"amount_zar": 100, "timestamp": "2024-01-01T02:30:00", "card_number": "4111-****-****-0003"}
    assert "ODD_HOURS" in detect_fraud(tx)
