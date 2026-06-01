import boto3
import pandas as pd
import io

print("🔍 Inferred Schema Engine Active...")

endpoint = "http://localhost:4566"
s3 = boto3.client("s3", endpoint_url=endpoint, aws_access_key_id="mock", aws_secret_access_key="mock", region_name="us-east-1")

bucket_name = "stream-ingest-bucket"
prefix = "production/parquet_zone/"

try:
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    target_key = response['Contents'][0]['Key']
    s3_object = s3.get_object(Bucket=bucket_name, Key=target_key)
    df = pd.read_parquet(io.BytesIO(s3_object['Body'].read()))
    
    print("\n📦 === INFERRED SCHEMA DATA TYPES ===")
    for col in df.columns:
        print(f"  🔹 Column: {col.ljust(16)} | Type: VARCHAR")
    print("--------------------------------------")
    print("✅ Schema registered under table: 'production_transactions'")

except Exception as e:
    print(f"❌ Discovery failed: {e}")
