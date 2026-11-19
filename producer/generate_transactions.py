"""Generate synthetic banking transactions and publish them to Kafka.

Run from the project root:
    python -m producer.generate_transactions
"""
import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

from config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_TRANSACTIONS

COUNTRIES = ["PK", "PK", "PK", "AE", "GB"]
CATEGORIES = ["grocery", "fuel", "restaurant", "electronics", "travel"]


def make_transaction():
    """Return one synthetic transaction. About 12% are 'suspicious' by design."""
    suspicious = random.random() < 0.12
    return {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": f"cust-{random.randint(1, 100):04d}",
        "account_id": f"acct-{random.randint(1, 120):04d}",
        "amount": round(
            random.uniform(6000, 12000) if suspicious else random.uniform(5, 1500), 2
        ),
        "currency": "USD",
        "merchant_category": random.choice(
            ["electronics", "cash_advance"] if suspicious else CATEGORIES
        ),
        "country": random.choice(["AE", "GB"] if suspicious else COUNTRIES),
        "transaction_type": random.choice(["card_purchase", "online_payment"]),
        "transaction_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5,
    )
    print(f"Producing to {KAFKA_TOPIC_TRANSACTIONS}. Press Ctrl+C to stop.")
    try:
        while True:
            event = make_transaction()
            producer.send(KAFKA_TOPIC_TRANSACTIONS, event)
            producer.flush()
            print("Produced:", event["transaction_id"], event["amount"])
            time.sleep(1)
    except KeyboardInterrupt:
        print("Producer stopped")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
