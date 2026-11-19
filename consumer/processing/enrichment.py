"""Add a small synthetic customer profile to each event.

Later you can replace CUSTOMERS with a lookup against a Snowflake table.
"""

DEFAULT_PROFILE = {"home_country": "PK", "risk_profile": "normal"}

CUSTOMERS = {
    "cust-0001": {"home_country": "PK", "risk_profile": "normal"},
    "cust-0002": {"home_country": "PK", "risk_profile": "high"},
}


def enrich_transaction(event):
    profile = CUSTOMERS.get(event["customer_id"], DEFAULT_PROFILE)
    return {
        **event,
        "home_country": profile["home_country"],
        "customer_risk_profile": profile["risk_profile"],
        "country_mismatch": event["country"] != profile["home_country"],
    }
