import boto3
import json
import time

print("🔥 Initializing Real-Time Stream Consumer Worker (Amazon Kinesis Interceptor)...")

endpoint = "http://localhost:4566"
kinesis_client = boto3.client(
    "kinesis", 
    endpoint_url=endpoint, 
    aws_access_key_id="mock", 
    aws_secret_access_key="mock", 
    region_name="us-east-1"
)

stream_name = "live-transaction-stream"

try:
    # 1. Locate the active Shard inside the Kinesis pipeline structure
    response = kinesis_client.describe_stream(StreamName=stream_name)
    shard_id = response['StreamDescription']['Shards'][0]['ShardId']
    print(f"📡 Successfully locked onto Stream Shard ID: {shard_id}")

    # 2. Grab a stream iterator pointer to read from the very beginning of the buffer
    iterator_response = kinesis_client.get_shard_iterator(
        StreamName=stream_name,
        ShardId=shard_id,
        ShardIteratorType="TRIM_HORIZON"  # Reads from the oldest unexpired record in the stream
    )
    shard_iterator = iterator_response['ShardIterator']
    
    print("🚀 Listener activated. Monitoring stream memory buffer for data packs...")
    print("-----------------------------------------------------------------")

    total_records_processed = 0
    total_revenue_processed = 0.0

    # 3. Continuous Interception Poll Loop
    while True:
        # Fetch the active record bundle block using our iterator checkpoint pointer
        records_response = kinesis_client.get_records(ShardIterator=shard_iterator, Limit=10)
        
        records = records_response.get('Records', [])
        
        if records:
            print(f"📥 Intercepted a packet array of {len(records)} raw event stream records!")
            
            for rec in records:
                # Decrypt the binary wire data payload back into a readable string
                raw_payload = rec['Data'].decode('utf-8')
                data = json.loads(raw_payload)
                
                total_records_processed += 1
                total_revenue_processed += data['amount']
                
                # Apply conditional filtering rules on the live streaming records
                indicator = "🟢 [SUCCESS]" if data['status'] == "SUCCESS" else "🔴 [ALERT]"
                print(f"  {indicator} Processed stream event -> ID: {data['transaction_id']} | Amt: ${data['amount']} | Status: {data['status']}")
            
            print(f"📊 Live Aggregate Aggregation Metric Summary -> Total Processed: {total_records_processed} | Cumulative Stream Value: ${round(total_revenue_processed, 2)}")
            print("-----------------------------------------------------------------")
            
        # Update the shard iterator tracking pointer to the next consecutive data block
        shard_iterator = records_response.get('NextShardIterator')
        
        # Pacing time block to prevent overloading the local system loop requests
        time.sleep(1)

except Exception as e:
    print(f"❌ Stream consumer processing crashed: {e}")
