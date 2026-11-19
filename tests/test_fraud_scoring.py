from consumer.processing.fraud_scoring import score_transaction

BASE_EVENT = {
    "amount": 100.0,
    "merchant_category": "grocery",
    "country_mismatch": False,
    "customer_risk_profile": "normal",
}


def test_ordinary_transaction_is_low():
    result = score_transaction(dict(BASE_EVENT))
    assert result["risk_score"] == 0
    assert result["risk_level"] == "LOW"
    assert result["is_fraud_flag"] is False


def test_high_amount_alone_is_medium():
    result = score_transaction(dict(BASE_EVENT, amount=6000))
    assert result["risk_score"] == 30
    assert result["risk_level"] == "MEDIUM"
    assert result["rule_reasons"] == ["high_amount"]


def test_high_amount_plus_country_mismatch_is_high():
    result = score_transaction(dict(BASE_EVENT, amount=6000, country_mismatch=True))
    assert result["risk_score"] == 60
    assert result["risk_level"] == "HIGH"
    assert result["is_fraud_flag"] is True
