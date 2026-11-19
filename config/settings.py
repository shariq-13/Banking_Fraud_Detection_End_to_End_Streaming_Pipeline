"""Central configuration. Every folder imports settings from here.

Values are read from the .env file in the project root.
"""
import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

# AWS
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET", "")

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_TRANSACTIONS = os.getenv("KAFKA_TOPIC_TRANSACTIONS", "banking.transactions")
KAFKA_TOPIC_INVALID = os.getenv("KAFKA_TOPIC_INVALID", "banking.invalid-transactions")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "fraud-detection-consumer")

# Snowflake
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "FRAUD_APP")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD", "")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "FRAUD_ROLE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "FRAUD_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "BANKING_FRAUD")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "RAW")
