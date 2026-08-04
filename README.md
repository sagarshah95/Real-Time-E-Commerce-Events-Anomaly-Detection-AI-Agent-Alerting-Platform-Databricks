# Real-Time E-Commerce Events Anomaly Detection & AI Agent Alerting Platform

## Overview

This project demonstrates an end-to-end real-time data engineering and AI-powered monitoring platform built using both a local open-source stack and Databricks Lakehouse.

The platform simulates an e-commerce clickstream environment, processes events through a Medallion Architecture (Bronze → Silver → Gold), detects anomalies in near real time, and generates AI-powered incident summaries that are delivered automatically to a Discord channel.

The project was implemented in two environments:

1. **Local Streaming Platform**

   * Kafka
   * Spark Structured Streaming
   * Delta Lake
   * MinIO (S3-compatible storage)
   * Docker

2. **Databricks Lakehouse Platform**

   * Unity Catalog
   * Volumes
   * Delta Tables
   * Databricks Jobs
   * OpenAI Integration
   * Discord Notifications

---

## Business Problem

Modern digital platforms generate millions of events every day. Revenue-impacting issues such as checkout failures, payment gateway outages, tracking failures, or sudden traffic drops often remain unnoticed until significant business impact occurs.

This project aims to:

* Detect anomalies automatically
* Provide context-aware incident analysis
* Reduce time to detection
* Improve operational visibility
* Demonstrate modern Data Engineering + AI Agent patterns

---

# Repository Structure

The following reflects the **actual** files in this repository:

```text
Real-Time-E-Commerce-Events-Anomaly-Detection-AI-Agent-Alerting-Platform/
│
├── README.md                       # This document
├── docker-compose.yaml             # Local infra: Kafka + MinIO + Spark master/worker
├── .gitignore                      # Ignores .env, venv, __pycache__, etc.
│
├── local_version/                  # Self-hosted open-source streaming stack
│   ├── Dockerfile                  # Spark image + Delta/Kafka/S3 JARs
│   ├── producer/
│   │   └── producer.py             # Kafka event generator (host-side Python)
│   └── spark_jobs/
│       ├── bronze_ingest.py        # Kafka  → Bronze Delta (raw)
│       ├── silver_clean.py         # Bronze → Silver Delta (cleaned/validated)
│       ├── gold_aggregate.py       # Silver → Gold Delta (5-min KPI windows)
│       └── gold_test.py            # Batch reader to verify row counts per layer
│
└── databricks_version/             # Databricks Lakehouse implementation
    ├── notebooks/
    │   ├── 01_generate_raw_data.ipynb    # Writes JSON event batches to a Volume
    │   ├── 02_bronze_ingest.ipynb        # Auto Loader → Bronze Delta table
    │   ├── 03_silver_clean_data.ipynb    # Cleansing/validation → Silver table
    │   ├── 04_gold_aggregate.ipynb       # KPI windowing → Gold table
    │   └── 05_anomalies_detection.ipynb  # Rules + OpenAI + Discord alerting
    ├── workflow/
    │   └── databricks_pipeline.py        # Databricks Job (task DAG) definition
    └── sql/
        └── create_table.sql              # DDL for the alert-history table
```

> **Note:** `docker-compose.yaml` currently sits at the repository root, but its build context (`build: .`) and volume mount (`./spark_jobs`) expect the `Dockerfile` and `spark_jobs/` that actually live in `local_version/`. Run Compose from inside `local_version/` (move the file there) so the paths resolve. See [Setup — Local Docker Stack](#a-local-docker-stack).

---

# Architecture

## Local Streaming Architecture

```text
Event Generator (producer.py)
       │
       ▼
    Kafka  (topic: ecommerce-events)
       │
       ▼
Bronze Layer (bronze_ingest.py)      raw Kafka value bytes → Delta
       │
       ▼
Silver Layer (silver_clean.py)       parse JSON, validate, dedup → Delta
       │
       ▼
Gold Layer (gold_aggregate.py)       5-minute KPI windows → Delta
       │
       ▼
(Verification: gold_test.py)
```

Storage for all three layers is Delta on MinIO under the `lakehouse` bucket:
`s3a://lakehouse/{bronze,silver,gold}/...` with checkpoints under `s3a://lakehouse/checkpoints/...`.

## Databricks Lakehouse Architecture

```text
JSON Event Generator (01_generate_raw_data)
         │
         ▼
Databricks Volume  (/Volumes/.../raw_events/*.json)
         │
         ▼
Bronze Delta Table (02) — Auto Loader (cloudFiles)
         │
         ▼
Silver Delta Table (03) — validate + dedup + watermark
         │
         ▼
Gold KPI Table (04) — 5-minute tumbling windows
         │
         ▼
AI Alerting Agent (05) — rules → OpenAI → Discord → alert-history
```

---

# File-by-File Code Walkthrough

This section explains **every file**, what it does, the key code, and example inputs/outputs.

---

## Root files

### `docker-compose.yaml`

Defines the entire local infrastructure on one bridge network (`lakehouse-net`). Four services:

| Service | Image | Purpose | Host ports |
|---|---|---|---|
| `kafka` | `confluentinc/cp-kafka:7.8.3` | KRaft-mode Kafka broker (no ZooKeeper) | `29092` (host), `9092` internal |
| `minio` | `minio/minio` | S3-compatible object store for Delta | `9000` (API), `9001` (console) |
| `minio-init` | `minio/mc` | One-shot job that creates the `lakehouse` bucket | — |
| `spark-master` / `spark-worker` | built from `Dockerfile` | Spark standalone cluster | `8080` (UI), `7077` (submit) |

**Key detail — Kafka dual listeners:**

```yaml
KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,PLAINTEXT_HOST://0.0.0.0:29092
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092
```

* `kafka:9092` is used **inside** the Docker network (Spark connects here).
* `localhost:29092` is exposed to the **host** (the Python producer connects here).

**Key detail — bucket bootstrap:** `minio-init` waits for MinIO to be healthy, then runs:

```sh
mc alias set local http://minio:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD
mc mb local/lakehouse --ignore-existing
```

so the `lakehouse` bucket exists before any Spark job writes to it.

Spark services read credentials from an `.env` file (`env_file: - .env`) and mount `./spark_jobs` into `/opt/spark_jobs`.

### `.gitignore`

Excludes secrets and local artifacts: `.env`, `PROJECT_PLAN.md`, `venv`, `__pycache__`, `docker-compose.yml`. Because `.env` is ignored, **you must create it yourself** (template in the setup section).

---

## `local_version/Dockerfile`

Builds the Spark image used by both master and worker. It starts from the official Spark image and adds the JARs Spark needs to talk to Delta, Kafka, and S3/MinIO:

```dockerfile
FROM apache/spark:3.5.1
USER root
RUN pip install python-dotenv
RUN cd /opt/spark/jars && \
    wget -q .../delta-spark_2.12-3.2.0.jar && \
    wget -q .../delta-storage-3.2.0.jar && \
    wget -q .../spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    wget -q .../spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    wget -q .../kafka-clients-3.4.1.jar && \
    wget -q .../commons-pool2-2.11.1.jar && \
    wget -q .../hadoop-aws-3.3.4.jar && \
    wget -q .../aws-java-sdk-bundle-1.12.262.jar
USER spark
```

Why each group matters:

* **delta-spark / delta-storage** — Delta Lake read/write support.
* **spark-sql-kafka / kafka-clients / commons-pool2** — the Structured Streaming Kafka source.
* **hadoop-aws / aws-java-sdk-bundle** — the `s3a://` filesystem used to store Delta tables on MinIO.

Pinning `spark-sql-kafka` to the same version as Spark (`3.5.1`) avoids classpath conflicts.

---

## `local_version/producer/producer.py`

A standalone (host-side) Python script that generates fake e-commerce events and streams them into Kafka once per second.

```python
BOOTSTRAP_SERVER = os.getenv("BOOTSTRAP_SERVER")   # e.g. localhost:29092
TOPIC            = os.getenv("TOPIC")              # e.g. ecommerce-events
producer = Producer({'bootstrap.servers': BOOTSTRAP_SERVER})

def generate_events():
    return {
        "user_id":    fake.uuid4(),
        "event_type": fake.random_element(["login", "click_nav", "purchase", "logout"]),
        "product_id": fake.pyint(min_value=1000, max_value=9999),
        "amount":     fake.random_number(digits=4),
        "event_timestamp": fake.iso8601(),
    }

def stream_events():
    while True:
        event = generate_events()
        producer.produce(TOPIC, value=json.dumps(event).encode("utf-8"),
                         callback=delivery_report)
        producer.poll(0)   # serve delivery callbacks
        time.sleep(1)
```

* `producer.poll(0)` lets the client fire the `delivery_report` callback without blocking.
* On `Ctrl+C`, `producer.flush()` drains any buffered messages before exiting.

**Example message published to Kafka:**

```json
{"user_id": "b1f2...-...-...", "event_type": "purchase", "product_id": 4821, "amount": 3947, "event_timestamp": "2026-06-24T12:00:01"}
```

**Dependencies (host):** `confluent-kafka`, `faker`, `python-dotenv`.

> The local event schema (`user_id, event_type, product_id, amount, event_timestamp`) is intentionally simpler than the Databricks one, which also carries `session_id`, `product_category`, and `page`.

---

## `local_version/spark_jobs/` — the streaming pipeline

All four jobs share the same `SparkSession` boilerplate that wires up Delta and the MinIO S3A filesystem:

```python
spark = (SparkSession.builder.appName("...")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint",   MINIO_ENDPOINT)     # http://minio:9000
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")      # required by MinIO
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate())
```

`path.style.access=true` is essential for MinIO (it uses `endpoint/bucket` paths rather than AWS-style `bucket.endpoint`).

### `bronze_ingest.py` — Kafka → Bronze

Reads the Kafka topic as a stream and writes the **raw** records straight to Delta with no transformation (true Bronze / raw ingestion for replayability).

```python
kafka_options = {
    "kafka.bootstrap.servers": KAFKA_BOOTSTRAP_SERVER,  # kafka:9092
    "subscribe": TOPIC,
    "startingOffsets": "earliest",
    "failOnDataLoss": "false",
}

raw_df = spark.readStream.format("kafka").options(**kafka_options).load()

raw_df.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", "s3a://lakehouse/checkpoints/bronze")
    .trigger(processingTime="10 seconds")
    .start("s3a://lakehouse/bronze/ecommerce-events")
```

* The Kafka source yields columns like `key`, `value` (bytes), `topic`, `partition`, `offset`, `timestamp`. Bronze stores them as-is; the JSON body lives in `value`.
* `startingOffsets=earliest` replays from the start of the topic; `failOnDataLoss=false` tolerates offset gaps.
* A checkpoint under `checkpoints/bronze` makes the stream restartable and exactly-once for the Delta sink.

### `silver_clean.py` — Bronze → Silver

Parses the JSON `value`, enforces a schema, validates, deduplicates, and adds a processing timestamp.

```python
schema = StructType([
    StructField("user_id", StringType()),
    StructField("event_type", StringType()),
    StructField("product_id", IntegerType()),
    StructField("amount", DoubleType()),
    StructField("event_timestamp", StringType()),
])

df = (spark.readStream.format("delta").load("s3a://lakehouse/bronze/ecommerce-events")
        .select(F.from_json(F.col("value").cast("string"), schema).alias("data"))
        .select("data.*"))

df = (df.withColumn("event_timestamp", F.col("event_timestamp").cast("timestamp"))
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("event_timestamp").isNotNull())
        .withWatermark("event_timestamp", "5 minutes")                 # bound late data
        .dropDuplicates(["user_id", "event_type", "event_timestamp"])  # stateful dedup
        .withColumn("processed_at", F.current_timestamp()))
```

* `from_json` turns the raw Kafka bytes into typed columns.
* `withWatermark(..., "5 minutes")` tells Spark how long to keep dedup state; events later than the watermark are dropped, keeping state bounded.
* Output is appended to `s3a://lakehouse/silver/ecommerce-events` every 30s.

### `gold_aggregate.py` — Silver → Gold KPIs

Computes business KPIs over **5-minute event-time windows**.

```python
gold_agg = (df.withWatermark("event_timestamp", "10 minutes")
    .groupby(F.window("event_timestamp", "5 minutes"))
    .agg(
        F.count("*").alias("total_events"),
        F.approx_count_distinct("user_id").alias("unique_users"),
        F.sum(F.when(F.col("event_type") == "purchase", F.col("amount")).otherwise(0)).alias("total_revenue"),
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_count"),
        F.avg(F.when(F.col("event_type") == "purchase", F.col("amount"))).alias("avg_order_value"),
    )
    .withColumn("conversion_rate", F.col("purchase_count") / F.col("total_events"))
    .withColumn("window_start", F.col("window.start"))
    .withColumn("window_end",   F.col("window.end"))
    .drop("window"))

gold_agg.writeStream.format("delta")
    .outputMode("complete")                                            # windowed aggregate
    .option("checkpointLocation", "s3a://lakehouse/checkpoints/gold")
    .trigger(processingTime="60 seconds")
    .start("s3a://lakehouse/gold/kpi-metrices")
```

* `F.window(..., "5 minutes")` buckets events into tumbling 5-minute windows by event time.
* `outputMode("complete")` rewrites the full result table each trigger (required for aggregations with this sink pattern).
* The `if __name__ == "__main__"` block prints `query.lastProgress` every 30s so you can watch throughput in the console.

**Example Gold row:**

| window_start | window_end | total_events | unique_users | total_revenue | purchase_count | conversion_rate | avg_order_value |
|---|---|---|---|---|---|---|---|
| 12:00 | 12:05 | 300 | 287 | 12403.0 | 74 | 0.246 | 167.6 |

### `gold_test.py` — verification

A simple **batch** (non-streaming) reader used to confirm data landed in each layer:

```python
print("Gold_layer");   print(spark.read.format("delta").load("s3a://lakehouse/gold/kpi-metrices").count())
print("silver_layer"); print(spark.read.format("delta").load("s3a://lakehouse/silver/ecommerce-events").count())
print("bronze_layer"); print(spark.read.format("delta").load("s3a://lakehouse/bronze/ecommerce-events").count())
```

Run it any time to sanity-check row counts across Bronze/Silver/Gold.

---

## `databricks_version/notebooks/`

These notebooks re-implement the same medallion pipeline on Databricks. Every notebook uses **widgets** so the catalog/schema/volume can be parameterized (and overridden by the Job):

```python
dbutils.widgets.text("catalog", "real-time-streaming-lakehouse")
dbutils.widgets.text("schema", "ecommerce-events")
dbutils.widgets.text("volume_path", "/Volumes/real-time-streaming-lakehouse/ecommerce-events")
```

### `01_generate_raw_data.ipynb` — synthetic event generator

Writes batches of JSON events into the Volume so Auto Loader can pick them up.

```python
categories  = ["electronics", "clothing", "food", "books"]
event_types = ["login", "click_nav", "purchase", "logout"]

def generate_event():
    event_type = random.choice(event_types)
    base_time  = datetime.now(timezone.utc) - timedelta(minutes=10)
    return {
        "user_id":  str(uuid.uuid4()),
        "session_id": str(uuid.uuid4()),
        "event_type": event_type,
        "product_id": random.randrange(1000, 9999),
        "product_category": random.choice(categories),
        "amount": round(random.uniform(3, 50), 2) if event_type == "purchase" else 0.0,
        "page":  random.choice(["home", "product", "cart", "checkout"]),
        "event_timestamp": (base_time + timedelta(seconds=random.randint(0, 600))).isoformat(),
    }

for batch_id in range(10):
    events = [generate_event() for _ in range(30)]     # 10 files × 30 events
    with open(f"{RAW_PATH}/events_{int(time.time())}_{batch_id}.json", "w") as f:
        for event in events:
            f.write(json.dumps(events) + "\n")
    time.sleep(5)
```

* Timestamps are set ~10 minutes in the past so the downstream windowed aggregation has complete windows to work with.
* Only `purchase` events carry a non-zero `amount` — this is what drives revenue/conversion KPIs.

> **Known quirk:** the inner loop writes the whole `events` list on each line (`json.dumps(events)`) instead of the individual `event`. Auto Loader still parses it, but change `events` → `event` if you want one event per line.

### `02_bronze_ingest.ipynb` — Auto Loader → Bronze

Uses **Auto Loader** (`cloudFiles`) to incrementally ingest new JSON files into a Bronze Delta table.

```python
BRONZE_TABLE      = f"`{CATALOG}`.`{SCHEMA}`.`bronze-events`"
BRONZE_CHECKPOINT = f"{VOLUME_PATH}/checkpoints/bronze"
SCHEMA_LOCATION   = f"{VOLUME_PATH}/checkpoints/bronze_schema"

raw_df = (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
    .option("cloudFiles.schemaEvolutionMode", "rescue")     # unexpected fields → _rescued_data
    .load(RAW_FILE)
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn("source", F.col("_metadata.file_path")))    # lineage: which file

(raw_df.writeStream.format("delta")
    .option("checkpointLocation", BRONZE_CHECKPOINT)
    .outputMode("append")
    .trigger(availableNow=True)                              # batch-style: process all, then stop
    .toTable(BRONZE_TABLE))
```

* `schemaEvolutionMode="rescue"` captures unexpected/malformed fields in a `_rescued_data` column instead of failing.
* `_metadata.file_path` adds data lineage (which file each row came from).
* `trigger(availableNow=True)` makes the stream drain everything currently available and then stop — ideal for orchestrated Job runs.

### `03_silver_clean_data.ipynb` — validation, dedup, watermark

```python
BRONZE_TABLE = f"`{CATALOG}`.`{SCHEMA}`.`bronze-events`"
SILVER_TABLE = f"`{CATALOG}`.`{SCHEMA}`.`silver-events`"

silver_df = spark.readStream.table(BRONZE_TABLE)

silver_df = (silver_df.filter(F.col("user_id").isNotNull())
                      .filter(F.col("event_timestamp").isNotNull()))

silver_df = (silver_df
    .withColumn("event_timestamp", F.to_timestamp(F.col("event_timestamp")))
    .withWatermark("event_timestamp", "10 minutes"))

silver_df = (silver_df
    .dropDuplicates(["user_id", "event_type", "event_timestamp", "product_id"])
    .withColumn("processed_at", F.current_timestamp()))

(silver_df.writeStream.format("delta")
    .option("checkpointLocation", SILVER_CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(SILVER_TABLE))
```

Same intent as the local `silver_clean.py`, but reads/writes Unity Catalog tables and dedups on a 4-column key that also includes `product_id`.

### `04_gold_aggregate.ipynb` — KPI windows

```python
SILVER_TABLE = f"`{CATALOG}`.`{SCHEMA}`.`silver-events`"
GOLD_TABLE   = f"`{CATALOG}`.`{SCHEMA}`.`gold-kpi-metrics`"

gold_agg = (spark.readStream.table(SILVER_TABLE)
    .withWatermark("event_timestamp", "10 minutes")
    .groupBy(F.window(F.col("event_timestamp"), "5 minutes"))
    .agg(
        F.count("*").alias("total_events"),
        F.approx_count_distinct("user_id").alias("unique_users"),
        F.sum(F.when(F.col("event_type") == "purchase", F.col("amount")).otherwise(0.0)).alias("total_revenue"),
        F.sum(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_count"),
    )
    .withColumn("conversion_rate", F.col("purchase_count") / F.col("total_events"))
    .withColumn("avg_order_value",
                F.when(F.col("purchase_count") > 0, F.col("total_revenue") / F.col("purchase_count")).otherwise(0))
    .withColumn("window_start", F.col("window.start"))
    .withColumn("window_end",   F.col("window.end"))
    .drop("window"))

(gold_agg.writeStream.format("delta")
    .outputMode("complete")
    .option("checkpointLocation", GOLD_CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(GOLD_TABLE))
```

Note the Databricks version guards `avg_order_value` against divide-by-zero (`when(purchase_count > 0, ...)`), which the local version does not.

### `05_anomalies_detection.ipynb` — the AI alerting agent

This is the "AI agent". It runs after Gold and does five things:

**1. Load secrets & the OpenAI client**

```python
OPENAI_API_KEY = dbutils.secrets.get(scope="anomaly_proj_secrets", key="openai_api_key")
DISCORD_URL    = dbutils.secrets.get(scope="anomaly_proj_secrets", key="discord_webhook_url")
client = OpenAI(api_key=OPENAI_API_KEY)
```

**2. Find the newest un-alerted Gold window** using a left-anti join against `alert-history` (prevents duplicate alerts):

```python
unalerted_df = (gold_df.join(alert_df, on=["window_start", "window_end"], how="left_anti")
                       .orderBy(F.col("window_start").desc()))
current  = unalerted_df.limit(1).collect()[0].asDict()
previous = (gold_df.filter(F.col("window_start") < current["window_start"])
                   .orderBy(F.col("window_start").desc()).limit(1).collect()[0].asDict())
```

**3. Apply deterministic anomaly rules** (current vs. previous window):

```python
def detect_anomalies(current, previous):
    anomalies = []
    if current_conversion < 0.01 and curr_total_events > 50:
        anomalies.append("Conversion rate below 1%")
    if previous_revenue > 0 and current_revenue < previous_revenue * 0.6:
        anomalies.append("Revenue dropped more than 40% compared to previous window")
    if previous_users > 0 and current_users < previous_users * 0.5:
        anomalies.append("Unique users dropped more than 50% compared to previous window")
    if curr_purchase_count == 0 and curr_total_events > 50:
        anomalies.append("No purchases detected despite meaningful traffic")
    return anomalies
```

If no anomaly fires, the notebook writes a "No anomaly detected" row to `alert-history` and exits early via `dbutils.notebook.exit(...)`.

**4. Build business context from Silver** for the anomalous window (funnel by `event_type`, revenue by `product_category`, activity by `page`), then ask OpenAI for a concise incident report:

```python
prompt = f"""
You are an ecommerce incident analyst...
Current 5 minute KPI window: {current}
Previous 5 minute KPI window: {previous}
Detected anomalies: {anomalies}
Funnel context: {funnel_context}
Category context: {category_context}
Page context: {page_context}
Write a Discord incident alert under 140 words...
"""
response = client.responses.create(model="gpt-5-mini", input=prompt)
alert_text = response.output_text
```

**5. Post to Discord and record the alert:**

```python
discord_payload = {'content': f"**Anomaly Incident Alert** ... {alert_text}"}
if anomalies:
    result = requests.post(DISCORD_URL, json=discord_payload)   # see fix note below
    ...
    alert_record.write.mode("append").saveAsTable(ALERT_TABLE)
```

> **Two bugs to fix before running notebook 05:**
> 1. The Discord cell references `DISCORD_WEBHOOK_URL`, but the secret is loaded into `DISCORD_URL`. Rename to match.
> 2. `result` is only assigned inside `if anomalies:`, yet the status-code check runs unconditionally — move the check inside the `if` block.

---

## `databricks_version/workflow/databricks_pipeline.py`

Defines the orchestration DAG as a Databricks **Job** using the Python SDK. Task dependencies:

```text
Raw_Data_Generation → Bronze_Ingestion → Silver_Cleaning → Gold_KPIs → Detect_Anomalies
```

```python
from databricks.sdk.service.jobs import JobSettings as Job
from databricks.sdk import WorkspaceClient

Real_Time_Streaming_Events_Pipeline = Job.from_dict({ "name": "...", "tasks": [ ... ] })

w = WorkspaceClient()
w.jobs.reset(new_settings=Real_Time_Streaming_Events_Pipeline, job_id=1234)   # update existing job 1234
# or: w.jobs.create(**Real_Time_Streaming_Events_Pipeline.as_shallow_dict())  # create new job
```

Notes:

* Each task points at a notebook by workspace path, e.g. `/Workspace/Real Time Streaming Lakehouse with Intelligent Alerting/02_bronze_ingest`.
* `Raw_Data_Generation` is defined with `"disabled": True` — enable it if you want the Job to generate data too.
* `w.jobs.reset(..., job_id=1234)` **updates** job `1234`; change the ID or switch to `w.jobs.create(...)` for a new job.

---

## `databricks_version/sql/create_table.sql`

DDL for the alert-history table used for deduplication and audit:

```sql
CREATE TABLE IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`alert-history` (
    window_start  TIMESTAMP,
    window_end    TIMESTAMP,
    alert_send_at TIMESTAMP,
    alert_reasons array<string>,
    alert_text    string
);
```

Substitute `{CATALOG}` = `real-time-streaming-lakehouse` and `{SCHEMA}` = `ecommerce-events`.

---

# Data Model

## Bronze Layer

Stores raw event data exactly as received (Kafka `value` bytes locally; raw JSON columns on Databricks).

Purpose: raw ingestion, replayability, auditability.

## Silver Layer

Schema enforcement, data-quality validation, deduplication, and standardization.

| Added Column      | Description          |
| ----------------- | -------------------- |
| processed_at      | Processing timestamp |
| (Databricks) source, ingested_at, _rescued_data | Lineage / rescued fields |

## Gold Layer

Business KPI aggregation using 5-minute event-time windows.

| Metric          | Description              |
| --------------- | ------------------------ |
| total_events    | Total events             |
| unique_users    | Approximate unique users |
| total_revenue   | Purchase revenue         |
| purchase_count  | Number of purchases      |
| conversion_rate | Purchases / Events       |
| avg_order_value | Revenue / Purchases      |

---

# Anomaly Detection Framework

Anomalies are detected using deterministic rules comparing the current window to the previous one.

| Rule | Trigger |
|---|---|
| Conversion Drop | `conversion_rate < 0.01` (and `total_events > 50`) |
| Revenue Drop | `current_revenue < previous_revenue * 0.6` |
| Traffic Drop | `current_users < previous_users * 0.5` |
| No Purchases | `purchase_count == 0` (and `total_events > 50`) |

---

# AI Alerting Agent

The Alerting Agent enriches KPI anomalies with supporting business context before generating an incident report.

**Inputs**

* Gold KPIs: revenue, conversion rate, purchase count, unique users.
* Silver context: funnel (`page_view → ... → purchase`), category revenue (Electronics/Books/Clothing/Food), page activity (Home/Product/Cart/Checkout).

**Workflow**

```text
Gold KPI Window → Anomaly Detection → Context Retrieval → OpenAI Analysis → Discord Notification → Alert History Table
```

# Alert History

To prevent duplicate notifications, every alert (and "no anomaly" result) is recorded in `alert-history`. The next run left-anti-joins Gold against this table so each window is evaluated only once.

---

# Setup & Run Instructions

## A) Local Docker Stack

**Prerequisites:** Docker Desktop (Compose v2); Python 3.9+ on the host.

**1. Put the compose file with its build context.** From the repo root:

```powershell
Move-Item ".\docker-compose.yaml" ".\local_version\docker-compose.yaml"
```

Run the remaining commands from `local_version/`.

**2. Create `local_version/.env`:**

```env
# MinIO container credentials
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin

# Spark jobs (inside containers → internal hostnames)
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
KAFKA_BOOTSTRAP_SERVER=kafka:9092
TOPIC=ecommerce-events
```

**3. Start the infrastructure:**

```powershell
docker compose up -d --build
```

* Spark UI: http://localhost:8080  •  MinIO console: http://localhost:9001

**4. Run the producer on the host** (new terminal):

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install confluent-kafka faker python-dotenv
```

Create a `.env` next to `producer.py` with host-side values:

```env
BOOTSTRAP_SERVER=localhost:29092
TOPIC=ecommerce-events
```

```powershell
python .\producer\producer.py
```

**5. Submit the Spark streaming jobs** (each in its own terminal):

```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark_jobs/bronze_ingest.py
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark_jobs/silver_clean.py
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark_jobs/gold_aggregate.py
```

**6. Verify:**

```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark_jobs/gold_test.py
```

**7. Tear down:** `docker compose down` (add `-v` to also delete data volumes).

> The local version does not include the AI alerting/Discord step — that logic lives only in the Databricks notebooks.

## B) Databricks Lakehouse

**Prerequisites:** a Unity Catalog workspace (Free Edition works), an OpenAI API key, and a Discord webhook URL.

**1. Create catalog, schema, and volume:**

```sql
CREATE CATALOG IF NOT EXISTS `real-time-streaming-lakehouse`;
CREATE SCHEMA  IF NOT EXISTS `real-time-streaming-lakehouse`.`ecommerce-events`;
CREATE VOLUME  IF NOT EXISTS `real-time-streaming-lakehouse`.`ecommerce-events`.`ecommerce-events`;
```

**2. Create the alert-history table** — run `databricks_version/sql/create_table.sql` with the placeholders substituted (see above).

**3. Create the secret scope:**

```bash
databricks secrets create-scope anomaly_proj_secrets
databricks secrets put-secret anomaly_proj_secrets openai_api_key
databricks secrets put-secret anomaly_proj_secrets discord_webhook_url
```

**4. Import the notebooks** into the exact folder `/Workspace/Real Time Streaming Lakehouse with Intelligent Alerting/`, keeping names `01_…`–`05_…`.

**5. Fix the two bugs in `05_anomalies_detection`** (see the notebook 05 walkthrough above).

**6. Run notebooks 01 → 05 in order** on serverless compute (each uses `trigger(availableNow=True)`, so it processes available data and stops). Verify with:

```sql
SELECT * FROM `real-time-streaming-lakehouse`.`ecommerce-events`.`gold-kpi-metrics` ORDER BY window_start DESC;
SELECT * FROM `real-time-streaming-lakehouse`.`ecommerce-events`.`alert-history`;
```

**7. (Optional) Orchestrate** via `databricks_version/workflow/databricks_pipeline.py` (SDK) or build the same DAG by hand in the Workflows UI.

> The default synthetic data usually looks "healthy", so no anomaly fires. To force an alert, edit notebook 01 to emit a window with very few purchases or unique users relative to the previous window.

---

# Key Engineering Concepts Demonstrated

* **Streaming:** Spark Structured Streaming, event-time processing, watermarking, tumbling windows, checkpointing.
* **Lakehouse:** Delta Lake, Bronze/Silver/Gold medallion, incremental ingestion (Auto Loader / Kafka source).
* **Data Quality:** schema enforcement, null validation, stateful deduplication, rescued-data handling.
* **Observability:** KPI monitoring, alert-history dedup, incident reporting.
* **AI Integration:** OpenAI-powered, context-aware incident analysis and automated Discord notifications.

# Future Enhancements

* Dynamic / statistical thresholds and ML-based forecasting
* Multi-channel notifications (Slack, Teams, PagerDuty)
* Agentic root-cause investigation and real-time dashboards
* Historical anomaly trend analysis

# Skills Demonstrated

Data Engineering • Streaming Architectures • Delta Lake • Databricks • PySpark • Kafka • Data Modeling • Data Quality Engineering • Workflow Orchestration • AI Agents • OpenAI Integration • Production Monitoring • Incident Management • Cloud Data Platforms
