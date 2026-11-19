# Setup Guide: Real-Time Banking Fraud Detection (Snowflake Edition)

This guide sets up the project from zero. Each step ends with a check. Do not start the next step until the current check passes.

This version uses **Snowflake** as the warehouse. PostgreSQL is not used.

---

## 0. What you need

| Tool | Version | Check command |
|---|---|---|
| Linux (Ubuntu 24.04 recommended) | — | `lsb_release -a` |
| Python | 3.10 or later | `python3 --version` |
| Java (for Kafka) | 17 or later | `java -version` |
| Git | any | `git --version` |
| AWS CLI v2 | v2 | `aws --version` |
| Snowflake account | trial or paid | Snowsight login |
| Kafka | 3.x or 4.x binary | `ls ~/kafka/bin` |

Install the Linux packages once:

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip build-essential git openjdk-17-jdk-headless unzip
```

**Low-memory machine?** Run Kafka and the consumer only. Run Superset (step 12) only when you need the dashboard.

---

## 1. Put the project in place

If you downloaded the zip:

```bash
mkdir -p ~/projects
unzip realtime-banking-fraud-detection.zip -d ~/projects
cd ~/projects/realtime-banking-fraud-detection
```

Or create a Git repository from the folder:

```bash
cd ~/projects/realtime-banking-fraud-detection
git init
git add .
git commit -m "Initial project structure"
```

Check the structure:

```bash
find . -path ./.venv -prune -o -type f -print | sort
```

You should see `config/`, `producer/`, `consumer/`, `loader/`, `snowflake/`, `dbt_fraud/`, `scripts/`, `tests/`, `docs/`, plus `README.md`, `.env.example`, `.gitignore` and `requirements.txt`.

---

## 2. Create the Python virtual environment

```bash
cd ~/projects/realtime-banking-fraud-detection
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Check:

```bash
python -c "import kafka, boto3, pandas, pyarrow, snowflake.connector, pytest; print('Dependencies OK')"
```

Expected output: `Dependencies OK`

> Open a new terminal later? Run `cd ~/projects/realtime-banking-fraud-detection && source .venv/bin/activate` first.

---

## 3. Create your `.env` file

```bash
cp .env.example .env
nano .env
```

Fill in these values now. You will get the Snowflake account identifier in step 5 and the S3 bucket name in step 6.

| Variable | What to put |
|---|---|
| `S3_BUCKET` | Your globally unique bucket name (step 6) |
| `SNOWFLAKE_ACCOUNT` | Your account identifier (step 5) |
| `SNOWFLAKE_PASSWORD` | The password you set for `FRAUD_APP` (step 5) |
| Everything else | Keep the defaults |

**Password rule:** use letters, numbers and underscores only in the password. The `.env` file is read by the shell in `scripts/run_dbt.sh`, so characters like `$`, `#`, spaces or quotes can break it.

`.env` is listed in `.gitignore`. Never commit it.

---

## 4. Verify the `.env` loads

```bash
python -c "from config.settings import AWS_REGION, S3_BUCKET, SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER; print(AWS_REGION, S3_BUCKET, SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER)"
```

You should see your region, bucket, account and user. Empty values mean `.env` is missing or misnamed.

---

## 5. Set up Snowflake

### 5a. Create or open an account

1. Sign up at https://signup.snowflake.com (a 30-day trial includes credits) or use an existing account.
2. Choose **AWS** and **Asia Pacific (Mumbai)** (`ap-south-1`) if it is offered, so the warehouse sits close to your S3 bucket.
3. Log in to **Snowsight** in your browser.

### 5b. Find your account identifier

In Snowsight, open the account menu, then **Account** and copy the **account identifier**. It looks like `orgname-accountname` or `abc12345.ap-south-1`.

Put it in `.env`:

```
SNOWFLAKE_ACCOUNT=orgname-accountname
```

### 5c. Run the setup SQL (once)

Open a **Worksheet** in Snowsight. Paste the contents of `snowflake/01_create_objects.sql` and run it (**Run All**). Then do the same with `snowflake/02_security_roles.sql`.

Before running `02_security_roles.sql`, replace `REPLACE_WITH_STRONG_PASSWORD` with the password you chose, and copy the same value into `SNOWFLAKE_PASSWORD` in `.env`.

**Or, from the terminal with SnowSQL** (optional):

```bash
# Install SnowSQL: https://docs.snowflake.com/en/user-guide/snowsql-install-config
snowsql -a your-account-identifier -u <your_admin_user> -r ACCOUNTADMIN -f snowflake/01_create_objects.sql
snowsql -a your-account-identifier -u <your_admin_user> -r ACCOUNTADMIN -f snowflake/02_security_roles.sql
```

### 5d. Verify

```bash
python -m scripts.check_snowflake
```

Expected output (values may differ):

```
Connected as FRAUD_APP | role=FRAUD_ROLE | warehouse=FRAUD_WH
Database.schema = BANKING_FRAUD.RAW
Rows in RAW.TRANSACTIONS: 0
```

If you get `Incorrect username or password`, check `SNOWFLAKE_PASSWORD`. If you get `Account not found`, check `SNOWFLAKE_ACCOUNT`.

---

## 6. Create the S3 bucket

1. Open the **S3 console**, then **Create bucket**.
2. Name: a globally unique name such as `banking-fraud-lake-yourname-1234`.
3. Region: `Asia Pacific (Mumbai) ap-south-1`.
4. Object Ownership: **Bucket owner enforced**.
5. Block all public access: **keep enabled**.
6. Default encryption: **SSE-S3**.
7. Versioning: **disabled** for this version.
8. Create the bucket. Copy its exact name into `S3_BUCKET` in `.env`.

Cost note: S3 is metered. Check your billing page. Start with a few thousand synthetic events. Do not create one object per transaction.

---

## 7. Install and verify AWS CLI

```bash
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o awscliv2.zip
unzip -q awscliv2.zip
sudo ./aws/install
aws --version
```

Configure credentials. For an access key:

```bash
aws configure
```

For SSO (preferred where available):

```bash
aws configure sso
aws sso login
```

Verify:

```bash
aws sts get-caller-identity
aws s3 ls s3://YOUR_BUCKET_NAME
```

The second command should succeed even if the bucket is empty. `AccessDenied` means the identity, bucket name, region or IAM policy is wrong. Never make the bucket public to fix it.

The identity needs `s3:ListBucket` on the bucket, and `s3:GetObject` and `s3:PutObject` on objects in it.

---

## 8. Install and start Kafka

Kafka runs locally in KRaft mode (no ZooKeeper). It is a single broker, for development only.

Download the current binary from https://kafka.apache.org/downloads and extract it:

```bash
cd ~
tar -xzf kafka_2.13-*.tgz   # adjust the file name to your download
mv kafka_2.13-* kafka
cd ~/kafka
```

**Config path:** Kafka 3.x uses `config/kraft/server.properties`. Kafka 4.x uses `config/server.properties`. Check which exists:

```bash
ls config/kraft/server.properties config/server.properties 2>/dev/null
```

Use the one that exists below. The examples use `config/server.properties`.

Format storage **once**:

```bash
cd ~/kafka
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format -t "$KAFKA_CLUSTER_ID" -c config/server.properties
```

> Do not run `format` again on the same data directory. It can erase local Kafka data.

Start the broker in **Terminal 1** and leave it running:

```bash
cd ~/kafka
bin/kafka-server-start.sh config/server.properties
```

Create the topics in **a second terminal**:

```bash
cd ~/projects/realtime-banking-fraud-detection
export KAFKA_HOME=$HOME/kafka
./scripts/create_kafka_topics.sh
```

Expected output: `banking.invalid-transactions` and `banking.transactions` appear in the list.

---

## 9. Install dbt in its own environment

dbt has a different dependency set, so it gets its own virtual environment.

```bash
python3 -m venv ~/venvs/dbt-fraud
source ~/venvs/dbt-fraud/bin/activate
pip install --upgrade pip
pip install -r ~/projects/realtime-banking-fraud-detection/dbt_fraud/requirements.txt
dbt --version
deactivate
```

You do not need to keep this environment active. The wrapper script `scripts/run_dbt.sh` runs dbt for you, but it needs `dbt` on your PATH. Activate it when you run dbt:

```bash
source ~/venvs/dbt-fraud/bin/activate
```

---

## 10. Run the streaming pipeline

Use **three terminals**. In each one, go to the project and activate the main venv.

```bash
cd ~/projects/realtime-banking-fraud-detection
source .venv/bin/activate
```

**Terminal 1 (Kafka):** already running from step 8.

**Terminal 2 (consumer):**

```bash
python -m consumer.fraud_consumer
```

You should see `Fraud consumer running on banking.transactions`.

**Terminal 3 (producer):**

```bash
python -m producer.generate_transactions
```

You should see `Produced: <id> <amount>` about once per second.

Back in Terminal 2, you should see risk scores printed. Batches upload to S3 every 25 events or every 10 seconds, whichever comes first. Stop the producer and consumer with **Ctrl+C**.

### Check the invalid-event topic

To test the dead-letter path, send a bad event. Run this in a new terminal:

```bash
source ~/projects/realtime-banking-fraud-detection/.venv/bin/activate
cd ~/projects/realtime-banking-fraud-detection
python -c "
import json
from kafka import KafkaProducer
from config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_TRANSACTIONS
p = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, value_serializer=lambda v: json.dumps(v).encode())
p.send(KAFKA_TOPIC_TRANSACTIONS, {'transaction_id': 'bad-1', 'amount': -5})
p.flush(); print('Sent one invalid event')
"
```

Read the invalid topic:

```bash
~/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic banking.invalid-transactions --from-beginning --max-messages 1
```

You should see the error message and the bad event.

---

## 11. Verify S3 objects and load Snowflake

### 11a. Check S3

```bash
source .venv/bin/activate
set -a; source .env; set +a
aws s3 ls s3://$S3_BUCKET/ --recursive
```

You should see:

```
bronze/transactions/year=YYYY/month=MM/day=DD/batch-....jsonl
silver/transactions/year=YYYY/month=MM/day=DD/batch-....parquet
```

If the list is empty, the consumer has not flushed yet. Wait 10 seconds and try again, or check the consumer output for upload messages.

### 11b. Load Silver into Snowflake

```bash
python -m loader.load_s3_to_snowflake
```

Expected output:

```
N Silver files found, N new to load
Loaded 25 rows from silver/transactions/...
Load finished
```

Run it a second time. It should report `0 new to load`. This shows the loader is idempotent.

### 11c. Check the data in Snowflake

In a Snowsight worksheet (or with SnowSQL):

```sql
USE WAREHOUSE FRAUD_WH;
USE DATABASE BANKING_FRAUD;
USE SCHEMA RAW;

SELECT COUNT(*) FROM RAW.TRANSACTIONS;

SELECT RISK_LEVEL, COUNT(*) AS N
FROM RAW.TRANSACTIONS
GROUP BY RISK_LEVEL
ORDER BY RISK_LEVEL;

SELECT TRANSACTION_ID, CUSTOMER_ID, AMOUNT, COUNTRY, RISK_SCORE, RISK_LEVEL, RULE_REASONS
FROM RAW.TRANSACTIONS
WHERE IS_FRAUD_FLAG = TRUE
ORDER BY TRANSACTION_TIMESTAMP DESC
LIMIT 20;

SELECT * FROM RAW.LOADED_FILES ORDER BY LOADED_AT DESC;
```

Or from the terminal:

```bash
python -m scripts.check_snowflake
```

**Warehouse cost:** `FRAUD_WH` auto-suspends after 60 seconds of inactivity. Queries in Snowsight wake it up. Check **Admin > Cost management** to see credit usage.

---

## 12. Build dbt models and tests

### 12a. Test the connection

```bash
source ~/venvs/dbt-fraud/bin/activate
cd ~/projects/realtime-banking-fraud-detection
./scripts/run_dbt.sh debug
```

Expected: `All checks passed!`

If it fails, check that `.env` has the Snowflake values and that the password has no special characters.

### 12b. Build

```bash
./scripts/run_dbt.sh build
```

This runs the models and all tests. Expected: `stg_transactions` and `mart_daily_fraud_summary` build, and all tests pass.

To run in steps:

```bash
./scripts/run_dbt.sh run     # models only
./scripts/run_dbt.sh test    # tests only
```

### 12c. Check the mart

```sql
SELECT * FROM ANALYTICS.MART_DAILY_FRAUD_SUMMARY ORDER BY TRANSACTION_DATE DESC;
```

### Why `COUNT_IF` and `IFF`?

The PostgreSQL version used `count(*) filter (where ...)`. Snowflake does not support the `FILTER` clause, so the mart uses `count_if(...)` and `iff(...)`. This is the main SQL difference when moving from PostgreSQL.

---

## 13. Run the unit tests

```bash
source ~/projects/realtime-banking-fraud-detection/.venv/bin/activate
cd ~/projects/realtime-banking-fraud-detection
python -m pytest -q
```

Expected: all tests pass. Run `python -m pytest -v` to see each test.

---

## 14. Optional: Superset dashboard

Superset is resource-heavy. Install it only after the pipeline works. Follow the official guide at https://superset.apache.org/docs/installation/installing-superset-using-pip.

Use this SQLAlchemy connection string, replacing the placeholders:

```
snowflake://FRAUD_APP:YOUR_PASSWORD@YOUR_ACCOUNT_IDENTIFIER/BANKING_FRAUD/ANALYTICS?warehouse=FRAUD_WH&role=FRAUD_ROLE
```

Install the driver in the Superset environment first: `pip install snowflake-sqlalchemy`.

Add the dataset `ANALYTICS.MART_DAILY_FRAUD_SUMMARY` and build these charts:

1. Total transactions per day (line chart).
2. Flagged transactions per day (bar chart).
3. Flagged transaction percentage (big number or line).
4. Total amount versus flagged amount (bar chart).
5. Transaction count by risk level (use the `RAW.TRANSACTIONS` dataset).

Name the dashboard **Banking Fraud Monitoring** and add a note that the data is synthetic and the scores are rule-based.

---

## 15. End-to-end checklist

| Check | Command or action | Expected |
|---|---|---|
| Python | `python -c "import snowflake.connector"` | No error |
| Snowflake | `python -m scripts.check_snowflake` | Connected as FRAUD_APP |
| Kafka | `./scripts/create_kafka_topics.sh` | Both topics listed |
| Producer | `python -m producer.generate_transactions` | Transaction IDs print |
| Consumer | `python -m consumer.fraud_consumer` | Risk levels print |
| Invalid events | Send an event with a negative amount | Appears in invalid topic |
| S3 Bronze | `aws s3 ls s3://$S3_BUCKET/bronze/ --recursive` | JSONL objects |
| S3 Silver | `aws s3 ls s3://$S3_BUCKET/silver/ --recursive` | Parquet objects |
| Loader | `python -m loader.load_s3_to_snowflake` | Rows loaded; second run loads 0 |
| Warehouse | `SELECT COUNT(*) FROM RAW.TRANSACTIONS` | Count is above 0 |
| dbt | `./scripts/run_dbt.sh build` | All models and tests pass |
| Unit tests | `python -m pytest -q` | All pass |
| Dashboard | Superset (optional) | Charts show data |

---

## 16. Troubleshooting

| Problem | What to check |
|---|---|
| `Kafka connection refused` | Broker is running in Terminal 1; `KAFKA_BOOTSTRAP_SERVERS=localhost:9092` |
| `Topic does not exist` | Run `./scripts/create_kafka_topics.sh` with `KAFKA_HOME` set |
| `NoBrokersAvailable` | Same as above; check the broker log for startup errors |
| `AccessDenied` (S3) | Active AWS identity; bucket name; region `ap-south-1`; IAM policy |
| No S3 files | Consumer is running and printed upload lines; `.env` bucket name is exact |
| `Incorrect username or password` (Snowflake) | `SNOWFLAKE_PASSWORD` in `.env` matches the one in `02_security_roles.sql` |
| `Account not found` / `Could not connect` | `SNOWFLAKE_ACCOUNT` is the account identifier, not the Snowsight URL |
| `Insufficient privileges` in loader | Re-run `02_security_roles.sql` as ACCOUNTADMIN; check grants |
| `Object 'RAW.TRANSACTIONS' does not exist` | Run `01_create_objects.sql` |
| dbt `Profile not found` | Run from `scripts/run_dbt.sh`; it sets `DBT_PROFILES_DIR` |
| dbt `Env var required but not provided` | `.env` is missing a `SNOWFLAKE_*` value |
| Password breaks `.env` | Use letters, numbers and underscores only |
| `Duplicate rows` | Not expected. The MERGE prevents duplicates. Check `RAW.LOADED_FILES` |
| Memory pressure | Stop Superset and extra apps; keep Kafka and the consumer |
| Warehouse credits used up | `ALTER WAREHOUSE FRAUD_WH SUSPEND;`; keep `AUTO_SUSPEND = 60` |

---

## 17. Security checklist

- [ ] Synthetic data only.
- [ ] S3 bucket private, Block Public Access on, SSE-S3 on.
- [ ] `.env` is not committed (`git status` does not show it).
- [ ] App uses `FRAUD_ROLE` and `FRAUD_APP`, not ACCOUNTADMIN.
- [ ] Snowflake password is not in any committed file.
- [ ] Risk score is documented as a heuristic.
- [ ] Single-broker, single-partition limitation is documented (see README).

---

## 18. Portfolio evidence

Commit these to GitHub:

- `README.md` with architecture and rules table.
- Screenshots in `docs/screenshots/`: producer and consumer output, S3 listing, Snowflake query results, `dbt build` output, pytest output, dashboard.
- A sample event (step 10 shows the format).

Suggested resume entry:

> **Real-Time Banking Fraud Detection Pipeline | Python, Apache Kafka, AWS S3, Snowflake, dbt Core, Apache Superset**
> Built a streaming transaction pipeline with Python and Kafka to validate, enrich and score synthetic banking transactions using explainable rules. Stored raw JSONL and curated Parquet in a private AWS S3 data lake, loaded records idempotently into Snowflake with MERGE, and built tested dbt models for daily fraud KPIs.

---

## 19. Build order

Do not move to the next milestone until the current one is verified.

| # | Milestone | Complete when |
|---|---|---|
| 1 | Local foundation | venv installed; `.env` loads |
| 2 | Snowflake | `check_snowflake` connects; tables exist |
| 3 | Kafka | Broker running; both topics exist |
| 4 | Streaming | Producer sends; consumer prints scores; invalid event lands in DLQ |
| 5 | S3 | Bronze JSONL and Silver Parquet appear |
| 6 | Warehouse load | Loader inserts rows; second run loads 0 |
| 7 | dbt | `build` passes |
| 8 | Dashboard | Superset shows the mart (optional) |
| 9 | Portfolio | README, screenshots and tests committed |

---

## Official documentation

- Apache Kafka: https://kafka.apache.org/documentation/
- Snowflake Python connector: https://docs.snowflake.com/en/developer-guide/python-connector/python-connector
- Snowflake `write_pandas`: https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-pandas
- dbt Snowflake setup: https://docs.getdbt.com/docs/core/connect-data-platform/snowflake-setup
- Amazon S3 user guide: https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html
- AWS CLI installation: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html
- Apache Superset: https://superset.apache.org/docs/intro
