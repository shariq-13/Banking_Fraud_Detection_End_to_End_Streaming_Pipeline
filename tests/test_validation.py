import pytest

from consumer.processing.validation import validate_transaction

VALID_EVENT = {
    "transaction_id": "txn-000001",
    "customer_id": "cust-0012",
    "account_id": "acct-0042",
    "amount": 825.50,
    "currency": "USD",
    "merchant_category": "electronics",
    "country": "PK",
    "transaction_type": "card_purchase",
    "transaction_timestamp": "2026-11-17T18:30:00+00:00",
}


def test_valid_event_passes():
    assert validate_transaction(dict(VALID_EVENT))["transaction_id"] == "txn-000001"


def test_missing_field_is_rejected():
    event = dict(VALID_EVENT)
    del event["customer_id"]
    with pytest.raises(ValueError, match="Missing fields"):
        validate_transaction(event)


def test_non_positive_amount_is_rejected():
    event = dict(VALID_EVENT, amount=0)
    with pytest.raises(ValueError, match="positive"):
        validate_transaction(event)


def test_bad_timestamp_is_rejected():
    event = dict(VALID_EVENT, transaction_timestamp="not-a-date")
    with pytest.raises(ValueError):
        validate_transaction(event)
