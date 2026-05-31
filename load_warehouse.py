import boto3
import pandas as pd
import sqlite3
import io

print("🏗️ Initializing Amazon Redshift Data Warehouse Simulation Engine...")

endpoint = "http://localhost:4566"
s3_client = boto3.client(
    "s3", 
    endpoint_url=endpoint, 
    aws_access_key_id="mock", 
    aws_secret_access_key="mock", 
    region_name="us-east-1"
)

bucket_name = "stream-ingest-bucket"
parquet_key = "production/parquet_zone/optimized_transactions.parquet"
db_name = "redshift_warehouse.db"

try:
    # 1. Fetch the highly compressed production Parquet asset straight out of S3
    print(f"📥 Pulling optimized Parquet data asset from s3://{bucket_name}/{parquet_key}...")
    s3_object = s3_client.get_object(Bucket=bucket_name, Key=parquet_key)
    parquet_bytes = s3_object['Body'].read()

    # 2. Decompress and parse the asset directly back into an in-memory Dataframe matrix
    df = pd.read_parquet(io.BytesIO(parquet_bytes))
    print(f"  🔹 Extracted {len(df)} historical transactional records from binary storage.")

    # 3. Establish a connection to our high-performance Local analytical data warehouse engine
    print(f"🔌 Connecting to Cloud Data Warehouse: '{db_name}'...")
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # 4. Construct a strict, structured relational data warehouse database schema table definition
    print("🛠️ Generating analytical warehouse schema table structure: 'fact_transactions'...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fact_transactions (
            transaction_id TEXT PRIMARY KEY,
            amount REAL,
            status TEXT
        )
    """)

    # 5. Executing the Data Engineering Core: High-Performance Warehouse Loading Loop
    print("🚀 Initiating high-velocity database record ingestion pipeline...")
    
    # Load rows directly into the relational table
    df.to_sql("fact_transactions", conn, if_exists="replace", index=False)
    conn.commit()
    print("✅ Transaction record bundle safely written and indexed inside the warehouse layer.")

    # 6. VERIFICATION ANALYTICS: Run an explicit SQL Business Intelligence check query
    print("\n📊 === WAREHOUSE ANALYTICAL QUERY REPORT (SQL RUNTIME EXECUTED) ===")
    print("----------------------------------------------------------------------")
    
    # Run a high-performance aggregation SQL statement across the database rows
    cursor.execute("""
        SELECT 
            status, 
            COUNT(*) as total_count, 
            ROUND(SUM(amount), 2) as total_volume 
        FROM fact_transactions 
        GROUP BY status
    """)
    
    rows = cursor.fetchall()
    for row in rows:
        print(f"  📌 Status Tier: {row[0].ljust(10)} | Count: {str(row[1]).ljust(4)} | Aggregated Financial Volume: ${row[2]}")
    print("----------------------------------------------------------------------")
    
    # Clean up connections
    conn.close()
    print("🚀 Warehouse batch cycle finished. System safely entering standby mode.")

except Exception as e:
    print(f"❌ Warehouse loader engine operation failed: {e}")
