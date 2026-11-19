# Real-Time Banking Fraud Detection Pipeline

A production-style learning project that streams **synthetic** banking transactions through Apache Kafka, scores each transaction with an explainable rule set, stores raw and curated data in AWS S3, loads the curated data into **Snowflake**, and builds tested analytics models with **dbt Core**.

> **Disclaimer:** This project uses synthetic data only. The risk score is a demonstration heuristic, not a production fraud model.

---

## Table of contents

1. [Tech stack](#1-tech-stack)
2. [Architecture](#2-architecture)
3. [Prerequisites](#3-prerequisites)
4. [Project structure](#4-project-structure)
5. [Create the project](#5-create-the-project)
6. [Installation and configuration](#6-installation-and-configuration)
7. [Running the pipeline](#7-running-the-pipeline)
8. [Testing](#8-testing)
9. [Risk scoring rules](#9-risk-scoring-rules)
10. [Design decisions](#10-design-decisions)
11. [Troubleshooting](#11-troubleshooting)
12. [Known limitations](#12-known-limitations)
13. [Future improvements](#13-future-improvements)
14. [Security](#14-security)
15. [Author](#15-author)

---

## 1. Tech stack

| Layer | Technology |
|---|---|
| Language | Python 3.10+ |
| Streaming | Apache Kafka (KRaft mode, single broker) |
| Data lake | AWS S3 (Bronze: JSON Lines, Silver: Parquet) |
| Warehouse | Snowflake |
| Transformation and testing | dbt Core with `dbt-snowflake` |
| Dashboard (optional) | Apache Superset |
| Unit testing | pytest |

## 2. Architecture

```
producer/            generate_transactions.py
   |
   v
Kafka topic: banking.transactions
   |
   v
consumer/            validate -> enrich -> score -> write batches
   |   \
   |    +--> Kafka topic: banking.invalid-transactions   (rejected events)
   v
S3 Bronze (raw JSONL)   and   S3 Silver (scored Parquet)
   |
   v
loader/              load_s3_to_snowflake.py   (MERGE, idempotent)
   |
   v
Snowflake   BANKING_FRAUD.RAW.TRANSACTIONS
   |
   v
dbt_fraud/           stg_transactions (view) -> mart_daily_fraud_summary (table)
   |
   v
Superset dashboard   (optional)
```

| Component | Responsibility |
|---|---|
| Producer | Generates synthetic transactions and publishes them to Kafka |
| Kafka | Buffers and delivers events; holds invalid events separately |
| Consumer | Validates, enriches and scores each event; writes batches to S3 |
| S3 | Stores Bronze (raw) and Silver (curated) files |
| Loader | Reads Silver files and merges them into Snowflake |
| Snowflake | Stores the queryable warehouse tables |
| dbt | Builds staging and mart models and runs data tests |
| Superset | Displays fraud KPIs (optional) |

## 3. Prerequisites

| Requirement | Version | Verify with |
|---|---|---|
| Windows 10/11 with Git Bash (Git for Windows) | — | `uname -a` in Git Bash |
| Python | 3.10 or later | `python --version` |
| Java (for Kafka) | 17 or later | `java -version` |
| Git | any | `git --version` |
| AWS CLI | v2 | `aws --version` |
| Snowflake account | trial or paid | Log in to Snowsight |
| AWS account | — | Permission to create a private S3 bucket |

Install the system packages once:

Install these once from the official installers (Git Bash does not use `apt`):

- Git for Windows (includes Git Bash): https://git-scm.com/downloads
- Python 3.10+: https://www.python.org/downloads/ (tick **Add python.exe to PATH**)
- Java 17+ (Temurin): https://adoptium.net/

```bash
python --version
java -version
git --version
```

Cost note: S3 and Snowflake are metered services. Use a few thousand synthetic events, keep one bucket, and keep the Snowflake warehouse set to auto-suspend.

## 4. Project structure

```
realtime-banking-fraud-detection/
├── config/                       Shared settings loaded from .env
│   ├── __init__.py
│   └── settings.py
├── producer/                     Kafka producer (synthetic data)
│   ├── __init__.py
│   ├── generate_transactions.py
│   └── requirements.txt
├── consumer/                     Kafka consumer (validate, enrich, score)
│   ├── __init__.py
│   ├── fraud_consumer.py
│   ├── requirements.txt
│   ├── processing/
│   │   ├── __init__.py
│   │   ├── validation.py
│   │   ├── enrichment.py
│   │   └── fraud_scoring.py
│   └── storage/
│       ├── __init__.py
│       └── s3_writer.py
├── loader/                       S3 Silver to Snowflake loader
│   ├── __init__.py
│   ├── load_s3_to_snowflake.py
│   └── requirements.txt
├── snowflake/                    Snowflake setup SQL
│   ├── 01_create_objects.sql
│   └── 02_security_roles.sql
├── dbt_fraud/                    dbt project (Snowflake adapter)
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── requirements.txt
│   └── models/
│       ├── staging/
│       │   ├── sources.yml
│       │   ├── stg_transactions.sql
│       │   └── stg_transactions.yml
│       └── marts/
│           ├── mart_daily_fraud_summary.sql
│           └── mart_daily_fraud_summary.yml
├── scripts/                      Helper scripts and checks
│   ├── __init__.py
│   ├── check_snowflake.py
│   ├── create_kafka_topics.sh
│   └── run_dbt.sh
├── tests/                        pytest unit tests
│   ├── requirements.txt
│   ├── test_validation.py
│   └── test_fraud_scoring.py
├── docs/
│   └── screenshots/
├── .env.example
├── .gitignore
├── requirements.txt              Root Python dependencies
├── README.md
└── SETUP_GUIDE.md
```

## 5. Create the project

### 5.1 Create the folders

```bash
mkdir -p ~/projects/realtime-banking-fraud-detection
cd ~/projects/realtime-banking-fraud-detection

mkdir -p config producer consumer/processing consumer/storage loader snowflake
mkdir -p dbt_fraud/models/staging dbt_fraud/models/marts
mkdir -p scripts tests docs/screenshots
```

### 5.2 Create the files

```bash
touch config/__init__.py config/settings.py
touch producer/__init__.py producer/generate_transactions.py producer/requirements.txt
touch consumer/__init__.py consumer/fraud_consumer.py consumer/requirements.txt
touch consumer/processing/__init__.py consumer/processing/validation.py \
      consumer/processing/enrichment.py consumer/processing/fraud_scoring.py
touch consumer/storage/__init__.py consumer/storage/s3_writer.py
touch loader/__init__.py loader/load_s3_to_snowflake.py loader/requirements.txt
touch snowflake/01_create_objects.sql snowflake/02_security_roles.sql
touch dbt_fraud/dbt_project.yml dbt_fraud/profiles.yml dbt_fraud/requirements.txt
touch dbt_fraud/models/staging/sources.yml dbt_fraud/models/staging/stg_transactions.sql \
      dbt_fraud/models/staging/stg_transactions.yml
touch dbt_fraud/models/marts/mart_daily_fraud_summary.sql \
      dbt_fraud/models/marts/mart_daily_fraud_summary.yml
touch scripts/__init__.py scripts/check_snowflake.py scripts/create_kafka_topics.sh scripts/run_dbt.sh
touch tests/requirements.txt tests/test_validation.py tests/test_fraud_scoring.py
touch docs/screenshots/.gitkeep
touch requirements.txt .env.example .gitignore README.md SETUP_GUIDE.md

chmod +x scripts/create_kafka_topics.sh scripts/run_dbt.sh
```

Each file is filled with the code from the project archive (`realtime-banking-fraud-detection.zip`). To use the archive instead of creating files by hand:

```bash
powershell.exe -Command "Expand-Archive -Path realtime-banking-fraud-detection.zip -DestinationPath $HOME/projects -Force"
cd ~/projects/realtime-banking-fraud-detection
```

### 5.3 Initialise Git (recommended)

```bash
git init
git add .
git commit -m "Initial commit: Kafka, S3 and Snowflake fraud pipeline"
```

Check the result:

```bash
find . -path ./.venv -prune -o -type f -print | sort
```

## 6. Installation and configuration

### 6.1 Python virtual environment

```bash
cd ~/projects/realtime-banking-fraud-detection
python -m venv .venv
source .venv/Scripts/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Verify:

```bash
python -c "import kafka, boto3, pandas, pyarrow, snowflake.connector, pytest; print('Dependencies OK')"
```

Expected output: `Dependencies OK`

### 6.2 Environment file

```bash
cp .env.example .env
nano .env
```

| Variable | Value |
|---|---|
| `AWS_REGION` | `ap-south-1` (or your region) |
| `S3_BUCKET` | Your globally unique bucket name |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `SNOWFLAKE_ACCOUNT` | Your Snowflake account identifier |
| `SNOWFLAKE_USER` | `FRAUD_APP` |
| `SNOWFLAKE_PASSWORD` | The password for `FRAUD_APP` |
| `SNOWFLAKE_ROLE` | `FRAUD_ROLE` |
| `SNOWFLAKE_WAREHOUSE` | `FRAUD_WH` |
| `SNOWFLAKE_DATABASE` | `BANKING_FRAUD` |
| `DBT_SCHEMA` | `ANALYTICS` |

Password rule: use letters, numbers and underscores only. The dbt wrapper reads `.env` through the shell, and characters such as `$`, `#`, quotes or spaces break it.

Verify the settings load:

```bash
python -c "from config.settings import S3_BUCKET, SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER; print(S3_BUCKET, SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER)"
```

### 6.3 Snowflake setup

1. Sign up at https://signup.snowflake.com, or use an existing account. Choose AWS and Asia Pacific (Mumbai) `ap-south-1` if offered.
2. Find your account identifier in Snowsight (**Admin > Accounts**) and put it in `.env` as `SNOWFLAKE_ACCOUNT`.
3. Open a Snowsight worksheet and run these two files in order:
   - `snowflake/01_create_objects.sql` as SYSADMIN (creates the warehouse, database, schemas and tables)
   - `snowflake/02_security_roles.sql` as ACCOUNTADMIN (creates `FRAUD_ROLE` and `FRAUD_APP`)

   Before running `02_security_roles.sql`, replace `REPLACE_WITH_STRONG_PASSWORD` with your password. Use the same value in `.env`.

Or run them from the terminal with SnowSQL:

```bash
snowsql -a YOUR_ACCOUNT_IDENTIFIER -u YOUR_ADMIN_USER -r SYSADMIN -f snowflake/01_create_objects.sql
snowsql -a YOUR_ACCOUNT_IDENTIFIER -u YOUR_ADMIN_USER -r ACCOUNTADMIN -f snowflake/02_security_roles.sql
```

Verify the connection:

```bash
python -m scripts.check_snowflake
```

Expected output (values may differ):

```
Connected as FRAUD_APP | role=FRAUD_ROLE | warehouse=FRAUD_WH
Database.schema = BANKING_FRAUD.RAW
Rows in RAW.TRANSACTIONS: 0
```

### 6.4 AWS S3 setup

1. In the S3 console, create a bucket named something like `banking-fraud-lake-yourname-1234`.
2. Region: `Asia Pacific (Mumbai) ap-south-1`.
3. Object Ownership: Bucket owner enforced.
4. Block all public access: enabled.
5. Default encryption: SSE-S3.
6. Versioning: disabled for this version.
7. Copy the bucket name into `S3_BUCKET` in `.env`.

Install and configure the AWS CLI:

```bash
curl "https://awscli.amazonaws.com/AWSCLIV2.msi" -o AWSCLIV2.msi
msiexec.exe /i AWSCLIV2.msi
```

Close and reopen Git Bash, then check the version:

```bash
aws --version

aws configure              # access key profile
# or, for SSO:
aws configure sso
aws sso login
```

Verify access:

```bash
aws sts get-caller-identity
aws s3 ls s3://YOUR_BUCKET_NAME
```

An empty bucket should still list without error. If you see `AccessDenied`, check the identity, bucket name, region and IAM policy. Never make the bucket public to fix an access error.

### 6.5 Kafka setup

Kafka runs locally in KRaft mode (no ZooKeeper).

Download the binary from https://kafka.apache.org/downloads and extract it to `~/kafka`:

```bash
cd ~
tar -xzf kafka_2.13-*.tgz
mv kafka_2.13-* kafka
cd ~/kafka
```

Check the config file path for your version. Kafka 3.x uses `config/kraft/server.properties`; Kafka 4.x uses `config/server.properties`:

```bash
ls config/kraft/server.properties config/server.properties 2>/dev/null
```

Format the storage **once** (this step erases local Kafka data if repeated):

```bash
cd ~/kafka
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format -t "$KAFKA_CLUSTER_ID" -c config/server.properties
```

Start the broker in a dedicated terminal and leave it running:

```bash
cd ~/kafka
bin/kafka-server-start.sh config/server.properties
```

Create the topics from a second terminal:

```bash
cd ~/projects/realtime-banking-fraud-detection
export KAFKA_HOME=$HOME/kafka
./scripts/create_kafka_topics.sh
```

Expected output: `banking.invalid-transactions` and `banking.transactions` are listed.

### 6.6 dbt environment

dbt uses its own virtual environment because its dependencies differ from the application code.

```bash
python -m venv ~/venvs/dbt-fraud
source ~/venvs/dbt-fraud/Scripts/activate
pip install --upgrade pip
pip install -r dbt_fraud/requirements.txt
dbt --version
deactivate
```

## 7. Running the pipeline

Each step must pass its check before you move to the next one.

| Step | Action | Check |
|---|---|---|
| 1 | Start Kafka broker (Terminal 1) | Broker log shows it is started |
| 2 | Create Kafka topics | `./scripts/create_kafka_topics.sh` lists both topics |
| 3 | Start the consumer (Terminal 2) | Risk levels print for each event |
| 4 | Start the producer (Terminal 3) | `Produced: <id> <amount>` prints about once per second |
| 5 | Confirm S3 files | `aws s3 ls s3://$S3_BUCKET/ --recursive` shows Bronze and Silver objects |
| 6 | Load Snowflake | `python -m loader.load_s3_to_snowflake` reports rows loaded |
| 7 | Build dbt models | `./scripts/run_dbt.sh build` passes |
| 8 | Run tests | `python -m pytest -q` passes |

### 7.1 Start the streaming pipeline

Terminal 2, consumer:

```bash
cd ~/projects/realtime-banking-fraud-detection
source .venv/Scripts/activate
python -m consumer.fraud_consumer
```

Terminal 3, producer:

```bash
cd ~/projects/realtime-banking-fraud-detection
source .venv/Scripts/activate
python -m producer.generate_transactions
```

Stop each process with `Ctrl+C`. The consumer writes a batch to S3 every 25 events or every 10 seconds, whichever comes first.

### 7.2 Confirm S3 objects

```bash
cd ~/projects/realtime-banking-fraud-detection
set -a; source .env; set +a
aws s3 ls s3://$S3_BUCKET/ --recursive
```

Expected layout:

```
bronze/transactions/year=YYYY/month=MM/day=DD/batch-....jsonl
silver/transactions/year=YYYY/month=MM/day=DD/batch-....parquet
```

### 7.3 Load Silver data into Snowflake

```bash
source .venv/Scripts/activate
python -m loader.load_s3_to_snowflake
```

Expected output:

```
N Silver files found, N new to load
Loaded 25 rows from silver/transactions/...
Load finished
```

Run the command again. It should report `0 new to load`, which confirms that loading is idempotent.

Verify the warehouse in Snowsight:

```sql
USE WAREHOUSE FRAUD_WH;
SELECT COUNT(*) FROM BANKING_FRAUD.RAW.TRANSACTIONS;
SELECT RISK_LEVEL, COUNT(*) AS N FROM BANKING_FRAUD.RAW.TRANSACTIONS GROUP BY RISK_LEVEL;
SELECT * FROM BANKING_FRAUD.RAW.LOADED_FILES ORDER BY LOADED_AT DESC;
```

### 7.4 Build dbt models and tests

```bash
source ~/venvs/dbt-fraud/Scripts/activate
cd ~/projects/realtime-banking-fraud-detection

./scripts/run_dbt.sh debug     # test the connection
./scripts/run_dbt.sh build     # run models and tests
```

Other options:

```bash
./scripts/run_dbt.sh run       # models only
./scripts/run_dbt.sh test      # tests only
```

Query the mart:

```sql
SELECT * FROM BANKING_FRAUD.ANALYTICS.MART_DAILY_FRAUD_SUMMARY ORDER BY TRANSACTION_DATE DESC;
```

## 8. Testing

Run the unit tests from the project root:

```bash
source .venv/Scripts/activate
python -m pytest -q
```

Run with verbose output:

```bash
python -m pytest -v
```

The tests cover event validation (required fields, positive amount, timestamp format) and risk scoring (LOW, MEDIUM and HIGH bands).

## 9. Risk scoring rules

| Rule | Points | Reason code |
|---|---|---|
| Amount ≥ 5000 | 30 | `high_amount` |
| Country differs from the customer's home country | 30 | `country_mismatch` |
| Category is `cash_advance` or `electronics` | 20 | `sensitive_merchant_category` |
| Customer risk profile is `high` | 20 | `high_customer_risk_profile` |

Risk bands: LOW 0–29, MEDIUM 30–59, HIGH 60–100. An event with the HIGH band sets `is_fraud_flag = TRUE`.

The score is a heuristic indicator, not a probability of fraud.

## 10. Design decisions

- **Snowflake as the warehouse.** The loader uses `write_pandas` and `MERGE`. dbt uses the `dbt-snowflake` adapter. Snowflake does not support the PostgreSQL `FILTER` clause, so the mart uses `COUNT_IF` and `IFF`.
- **Idempotent loading.** `RAW.TRANSACTIONS` is keyed on `TRANSACTION_ID`. `RAW.LOADED_FILES` records every S3 file loaded, so reruns do not duplicate data.
- **Offsets are committed after S3 writes.** A crash replays events instead of losing them.
- **Batches, not single objects.** One S3 object per batch keeps request counts and cost low.
- **Least privilege.** The application uses `FRAUD_APP` with `FRAUD_ROLE`. ACCOUNTADMIN is used only for the one-time setup.
- **UTC everywhere.** Timestamps are stored as `TIMESTAMP_NTZ` in UTC.

## 11. Troubleshooting

| Symptom | Likely cause and fix |
|---|---|
| `Kafka connection refused` / `NoBrokersAvailable` | Broker is not running. Start it in Terminal 1 and confirm `localhost:9092`. |
| `Topic does not exist` | Run `./scripts/create_kafka_topics.sh` with `KAFKA_HOME` set. |
| `AccessDenied` on S3 | Check the active AWS identity, bucket name, region and IAM policy. |
| No S3 files | The consumer has not flushed. Wait 10 seconds and check its output. Confirm `S3_BUCKET` in `.env`. |
| `Incorrect username or password` (Snowflake) | `SNOWFLAKE_PASSWORD` in `.env` must match the password in `02_security_roles.sql`. |
| `Account not found` | `SNOWFLAKE_ACCOUNT` must be the account identifier, not the Snowsight URL. |
| `Insufficient privileges` in the loader | Re-run `02_security_roles.sql` as ACCOUNTADMIN. |
| `Object 'RAW.TRANSACTIONS' does not exist` | Run `01_create_objects.sql`. |
| dbt `Profile not found` or `Env var required` | Run dbt through `./scripts/run_dbt.sh`, and confirm `.env` contains all `SNOWFLAKE_*` values. |
| Shell errors while loading `.env` | Remove special characters from the password. Use letters, numbers and underscores. |
| Warehouse credits used up | `ALTER WAREHOUSE FRAUD_WH SUSPEND;`. Keep `AUTO_SUSPEND = 60`. |

## 12. Known limitations

- Single Kafka broker and single partition. This is a development setup, not a high-availability cluster.
- Customer profiles come from a small in-code dictionary, not a customer table.
- The rules are fixed. There is no velocity rule (for example, transactions per five minutes).
- A non-JSON message stops the consumer. Add a parser guard before using this beyond learning.
- The loader lists all Silver files on each run. It skips files already loaded, but the listing grows with the bucket.

## 13. Future improvements

- Velocity rules using Kafka Streams or a Python sliding-window state.
- Orchestration with Airflow once the manual pipeline is stable.
- Snowpipe or an external stage on S3 instead of the Python loader.
- A customer profile table in Snowflake, joined in dbt.
- dbt snapshots and freshness tests on `RAW.TRANSACTIONS`.

## 14. Security

- Use synthetic data only.
- Keep the S3 bucket private, with Block Public Access enabled and SSE-S3 encryption.
- Never commit `.env`, AWS keys or Snowflake passwords. `.gitignore` already excludes `.env`.
- Run the application as `FRAUD_APP`. Use ACCOUNTADMIN only for the setup in `snowflake/`.

## 15. Author

**Hamza Tufail**
BS Software Engineering, Sindh Madressatul Islam University
GitHub: [github.com/hamzatufai](https://github.com/hamzatufai)
