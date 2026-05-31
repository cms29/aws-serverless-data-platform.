import boto3

print("🏗️ Initializing Cloud-Integrated Local Compute Engine...")

endpoint = "http://localhost:4566"
s3_client = boto3.client(
    "s3", 
    endpoint_url=endpoint, 
    aws_access_key_id="mock", 
    aws_secret_access_key="mock", 
    region_name="us-east-1"
)

bucket_name = "stream-ingest-bucket"
file_name = "live_transactions.csv"

try:
    # 1. Ensure the S3 bucket exists
    try:
        s3_client.create_bucket(Bucket=bucket_name)
    except Exception:
        pass
    print(f"✅ Verified Storage Landing Zone: s3://{bucket_name}")

    # 2. Generate a structured transaction data payload locally
    print(f"📝 Generating transaction payload data: {file_name}...")
    with open(file_name, "w") as f:
        f.write("transaction_id,amount,status\nTXN_101,250.00,SUCCESS\nTXN_102,15.75,FAILED\nTXN_103,450.10,SUCCESS\nTXN_104,0.00,ERROR")

    # 3. Stream upload the data payload to your local S3 bucket
    print(f"📤 Ingesting payload into S3 storage bucket...")
    s3_client.upload_file(file_name, bucket_name, file_name)
    print("✅ Ingestion successfully completed.")

    # 4. COMPUTE LAYER: Read the file back directly out of the S3 storage layer
    print("\n🔥 Activating Processing Engine: Fetching data from S3...")
    s3_response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
    
    # Read the raw byte data and decode it into text lines cleanly
    raw_data = s3_response['Body'].read().decode('utf-8')
    lines = [line.strip() for line in raw_data.strip().split('\n') if line.strip()]
    
    # Correctly isolate the header and slice out all record rows
    header = lines[0].split(',')
    records = lines[1:]

    print("\n📦 === COMPUTED PAYLOAD PROCESSING LOGS ===")
    print(f"Total Raw Records Found in S3: {len(records)}")
    
    # 5. Data Quality Filtering Loop
    success_count = 0
    failed_count = 0
    
    for record in records:
        cols = record.split(',')
        if len(cols) < 3:
            continue
            
        tx_id = cols[0]
        amount = cols[1]
        status = cols[2]
        
        # Filter and log system statuses dynamically
        if status == "SUCCESS":
            print(f"  🟢 [PROCESSED] {tx_id}: Passed valuation audit (Amount: ${amount}).")
            success_count += 1
        elif status in ["FAILED", "ERROR"]:
            print(f"  🔴 [ALERT] {tx_id}: Rejected with system status '{status}'!")
            failed_count += 1

    print("===========================================")
    print(f"📊 Summary: {success_count} Approved | {failed_count} Flagged")
    print("\n🚀 End-to-End Local Cloud Processing Complete!")

except Exception as e:
    print(f"❌ Pipeline engine execution failed: {e}")