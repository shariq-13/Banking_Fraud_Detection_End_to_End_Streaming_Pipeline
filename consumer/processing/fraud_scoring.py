"""Explainable, rule-based risk score.

This is a heuristic risk indicator, NOT a probability of fraud.
"""

SUSPICIOUS_CATEGORIES = {"cash_advance", "electronics"}
HIGH_AMOUNT_THRESHOLD = 5000
HIGH_BAND_MIN = 60
MEDIUM_BAND_MIN = 30


def score_transaction(event):
    score = 0
    reasons = []

    if event["amount"] >= HIGH_AMOUNT_THRESHOLD:
        score += 30
        reasons.append("high_amount")

    if event["country_mismatch"]:
        score += 30
        reasons.append("country_mismatch")

    if event["merchant_category"] in SUSPICIOUS_CATEGORIES:
        score += 20
        reasons.append("sensitive_merchant_category")

    if event["customer_risk_profile"] == "high":
        score += 20
        reasons.append("high_customer_risk_profile")

    score = min(score, 100)

    if score >= HIGH_BAND_MIN:
        level = "HIGH"
    elif score >= MEDIUM_BAND_MIN:
        level = "MEDIUM"
    else:
        level = "LOW"

    return {
        **event,
        "risk_score": score,
        "risk_level": level,
        "is_fraud_flag": score >= HIGH_BAND_MIN,
        "rule_reasons": reasons,
    }
