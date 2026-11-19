"""Kafka consumer: validate, enrich, score, then write batches to S3.

Offsets are committed only AFTER a batch is written to S3, so a crash
replays events instead of losing them. The loader is idempotent on
transaction_id, so replays do not create duplicate warehouse rows.

Run from the project root:
    python -m consumer.fraud_consumer
"""
import json
import time

from kafka import KafkaConsumer, KafkaProducer

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_GROUP_ID,
    KAFKA_TOPIC_INVALID,
    KAFKA_TOPIC_TRANSACTIONS,
)
from consumer.processing.enrichment import enrich_transaction
from consumer.processing.fraud_scoring import score_transaction
from consumer.processing.validation import validate_transaction
from consumer.storage.s3_writer import write_batch

BATCH_SIZE = 25
FLUSH_INTERVAL_SECONDS = 10


def build_consumer():
    return KafkaConsumer(
        KAFKA_TOPIC_TRANSACTIONS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )


def build_dlq_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
    )


def main():
    consumer = build_consumer()
    dlq_producer = build_dlq_producer()

    raw_batch, processed_batch = [], []
    pending = 0  # messages handled since the last commit (valid + invalid)
    last_flush = time.time()

    def flush():
        write_batch(raw_batch, processed_batch)
        raw_batch.clear()
        processed_batch.clear()
        dlq_producer.flush()
        consumer.commit()

    print(f"Fraud consumer running on {KAFKA_TOPIC_TRANSACTIONS}. Ctrl+C to stop.")
    try:
        while True:
            records = consumer.poll(timeout_ms=1000)
            for _partition, messages in records.items():
                for message in messages:
                    pending += 1
                    event = message.value
                    try:
                        validate_transaction(event)
                        result = score_transaction(enrich_transaction(event))
                    except Exception as exc:
                        dlq_producer.send(
                            KAFKA_TOPIC_INVALID, {"error": str(exc), "event": event}
                        )
                        continue

                    raw_batch.append(event)
                    processed_batch.append(result)
                    print(
                        result["transaction_id"],
                        result["risk_level"],
                        result["risk_score"],
                        result["rule_reasons"],
                    )
                    if result["is_fraud_flag"]:
                        print("FRAUD ALERT:", result["transaction_id"])

            due = time.time() - last_flush >= FLUSH_INTERVAL_SECONDS
            if pending >= BATCH_SIZE or (pending and due):
                flush()
                pending = 0
                last_flush = time.time()
    except KeyboardInterrupt:
        print("Stopping consumer")
    finally:
        if pending:
            flush()
        consumer.close()
        dlq_producer.close()


if __name__ == "__main__":
    main()
