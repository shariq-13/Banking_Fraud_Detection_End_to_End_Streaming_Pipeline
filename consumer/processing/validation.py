"""Structural checks for incoming transaction events."""
from datetime import datetime

REQUIRED_FIELDS = {
    "transaction_id", "customer_id", "account_id", "amount", "currency",
    "merchant_category", "country", "transaction_type", "transaction_timestamp",
}


def validate_transaction(event):
    """Raise ValueError if the event is not usable. Return the event otherwise."""
    if not isinstance(event, dict):
        raise ValueError("event must be a JSON object")

    missing = REQUIRED_FIELDS - event.keys()
    if missing:
        raise ValueError(f"Missing fields: {sorted(missing)}")

    amount = event["amount"]
    if isinstance(amount, bool) or not isinstance(amount, (int, float)) or amount <= 0:
        raise ValueError("amount must be a positive number")

    if not event["transaction_id"] or not event["customer_id"]:
        raise ValueError("transaction_id and customer_id are required")

    datetime.fromisoformat(event["transaction_timestamp"].replace("Z", "+00:00"))
    return event
