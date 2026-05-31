import boto3
import pandas as pd
import io

print("🏗️ Initializing Serverless Storage Optimization Engine...")

endpoint = "http://localhost:4566"
s3_client = boto3.client(
    "s3", 
    endpoint_url=endpoint, 
    aws_access_key_id="mock", 
    aws_secret_access_key="mock", 
    region_name="us-east-1"
)

bucket_name = "stream-ingest-bucket"
raw_file = "live_transactions.csv"
parquet_file = "optimized_transactions.parquet"

try:
    # 1. Fetch the raw CSV file directly from your local S3 cloud layer
    print("📥 Pulling raw CSV telemetry data from S3 landing zone...")
    s3_response = s3_client.get_object(Bucket=bucket_name, Key=raw_file)
    csv_data = s3_response['Body'].read().decode('utf-8')
    
        # 2. Load the text rows into a Pandas Dataframe matrix
    df = pd.read_csv(io.StringIO(csv_data))
    
    # 🚨 THE FIX: Explicitly cast data types to ensure correct schema inference down the line
    df['transaction_id'] = df['transaction_id'].astype(str)
    df['amount'] = df['amount'].astype(float)
    df['status'] = df['status'].astype(str)
    
    print(f"Loaded DataFrame Matrix. Columns found: {list(df.columns)}")

    
  
    
    # 3. Convert the DataFrame into a compressed binary Parquet byte-stream in memory
    print("⚡ Executing Columnar Compression (CSV -> Apache Parquet)...")
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, engine='pyarrow', compression='snappy', index=False)
    parquet_buffer.seek(0)
    
    # 4. Upload the highly optimized asset back to a production data warehouse folder
    production_key = f"production/parquet_zone/{parquet_file}"
    print(f"📤 Uploading optimized asset to s3://{bucket_name}/{production_key}...")
    s3_client.put_object(Bucket=bucket_name, Key=production_key, Body=parquet_buffer.getvalue())
    
    # 5. Verification Check
    print("\n--- Verifying Storage Optimization Tier ---")
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="production/")
    for obj in response.get('Contents', []):
        print(f"🚀 Found Production Asset: {obj['Key']} ({obj['Size']} bytes)")
        
    print("\n✅ Storage Optimization Cycle Completed Successfully!")

except Exception as e:
    print(f"❌ Optimization run failed: {e}")
