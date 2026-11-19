#!/usr/bin/env bash
# Create the two Kafka topics used by the project.
# Usage: ./scripts/create_kafka_topics.sh
# Set KAFKA_HOME if Kafka is not installed at ~/kafka.
set -euo pipefail

KAFKA_HOME="${KAFKA_HOME:-$HOME/kafka}"
BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"

for topic in banking.transactions banking.invalid-transactions; do
  "$KAFKA_HOME/bin/kafka-topics.sh" \
    --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$topic" --partitions 1 --replication-factor 1
done

echo "Topics on $BOOTSTRAP:"
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list
