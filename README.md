
# Local AWS Serverless Data Platform 🚀

A production-grade, end-to-end data engineering platform simulating a complete AWS serverless data analytics lifecycle. Built from first principles using Python, Docker, and LocalStack to eliminate tutorial hell and demonstrate real-world infrastructure orchestration, data optimization, stream processing, and cloud data warehousing.

## 🏗️ System Architecture Flow
[Raw Local Data Stream]
│▼ 
(Continuous Ingestion via Boto3)[Local S3 Bucket Storage Landing Zone]
│
▼ (Snappy-Parquet Columnar Compression)[Optimized Cold Storage Data Lake]
│
▼ (Automated Schema Profiling & Metadata Discovery)[Local Data Catalog Meta Layer]
│
▼ (Parallel High-Velocity Processing Core)[Amazon Kinesis Shard Interception Pipeline]
│
▼ (Mass-Batch Database Loading Pattern)[Local Analytical Data Warehouse (Redshift Simulation)]
│
▼ (SQL Execution Engine)[Aggregated Business Intelligence Query Reports]
## 🛠️ Tech Stack & Core Engines
* **Cloud Infrastructure Emulator**: LocalStack v4.4.0 (Docker Container Orchestrated)
* **Programming Interface**: Python 3.14 (Boto3 SDK Core)
* **Computation Matrix**: Pandas & PyArrow Dataframe Transformations
* **Storage Layer**: Amazon S3 (In-Memory Lakehouse Layout)
* **Stream Infrastructure**: Amazon Kinesis Data Streams Sharding
* **Warehouse Engine**: SQLite3 Relational Relational Store (Redshift Analytical Simulation)

## 📦 Data Lifecycle Components

### 1. Ingestion Layer (`run_data_pipeline.py`)
Programmatically initializes a local S3 data landing zone. Converts and serializes mock telemetry logs, handles memory session pointers, and stream-uploads raw comma-separated value payloads into the object storage environment.

### 2. Optimization Layer (`optimize_storage.py`)
Fetches raw network streams out of S3, maps text files into DataFrame matrices, executes strict type-casting validations, and outputs highly compressed, binary **Apache Parquet (Snappy-Compressed)** file layers to minimize cloud computational costs.

### 3. Cataloging Layer (`discover_schema.py`)
Acts as an autonomous AWS Glue Crawler. Connects to the cold storage zone, reads column layout descriptors, dynamically profiles underlying structures, and registers relational structural maps containing unified `VARCHAR` and `DOUBLE` data profiles.

### 4. Real-Time Streaming Core (`stream_producer.py` & `stream_consumer.py`)
Decoupled, multi-process streaming layout. The producer continuously broadcasts continuous high-velocity events into a Kinesis stream shard. The consumer interceptor captures data packs, decodes transport byte strings, and computes rolling cumulative financial valuation streams in real time.

### 5. Warehouse Engine (`load_warehouse.py`)
The terminal platform state. Executes batch data loads across binary data sets, provisions relational schemas (`fact_transactions`), and runs native SQL aggregation operations to generate final Business Intelligence reports.

## 📊 Verified Output Telemetry
```text
📊 === WAREHOUSE ANALYTICAL QUERY REPORT (SQL RUNTIME EXECUTED) ===
----------------------------------------------------------------------
  📌 Status Tier: ERROR      | Count: 1    | Aggregated Financial Volume: \$0.0
  📌 Status Tier: FAILED     | Count: 1    | Aggregated Financial Volume: \$15.75
  📌 Status Tier: SUCCESS    | Count: 2    | Aggregated Financial Volume: \$700.1
----------------------------------------------------------------------
🚀 Warehouse batch cycle finished. System safely entering standby mode.
```

## ⚡ How to Instantiate Locally
1. Start Docker Desktop and spin up the engine instance container:
   ```bash
   docker run --rm -d --name my_local_aws -p 4566:4566 -p 4510-4559:4510-4559 localstack/localstack:4.4.0
   ```
2. Inject authorization vectors into your environment shell terminal:
   ```bash
   \$env:AWS_ACCESS_KEY_ID="mock_key"
   \$env:AWS_SECRET_ACCESS_KEY="mock_secret"
   \$env:AWS_DEFAULT_REGION="us-east-1"
   ```
3. Run the end-to-end load loop lifecycle sequence:
   ```bash
   python run_data_pipeline.py
   python optimize_storage.py
   python discover_schema.py
   python load_warehouse.py
   ```
