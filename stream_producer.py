import boto3
import json
import random
import time
from datetime import datetime

print("🔥 Initializing Real-Time High-Velocity Stream Producer (Amazon Kinesis Mock)...")

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
    # 1. Provision a high-throughput Kinesis stream instance natively
    try:
        kinesis_client.create_stream(StreamName=stream_name, ShardCount=1)
        print(f"📡 Created active Kinesis Stream pipeline: '{stream_name}'")
        time.sleep(2)  # Give the local engine an instant to spin up the shard
    except Exception:
        print(f"📡 Verified active streaming highway: '{stream_name}'")

    print("\n⚡ Stream transmission active. Press Ctrl+C to stop stream production.")
    
    tx_counter = 1000
    statuses = ["SUCCESS", "FAILED", "PENDING", "ERROR"]
    
    # 2. Continuous real-time data injection loop
    while True:
        tx_counter += 1
        payload = {
            "transaction_id": f"TXN_{tx_counter}",
            "amount": round(random.uniform(5.00, 1500.00), 2),
            "status": random.choice(statuses),
            "timestamp": datetime.now().isoformat()
        }
        
        # Serialize your text dictionary map into a raw transport byte string
        data_bytes = json.dumps(payload).encode('utf-8')
        
        # 3. Inject the live record into the streaming core pipeline layer
        kinesis_client.put_record(
            StreamName=stream_name,
            Data=data_bytes,
            PartitionKey=f"shard_partition_{random.randint(1, 5)}"
        )
        
        print(f" 📤 Pushed Event Stream Record $\rightarrow$ {payload['transaction_id']} | Amount: ${payload['amount']} | Status: {payload['status']}")
        
        # Ingest at a continuous pacing interval frequency rate
        time.sleep(0.5)

except KeyboardInterrupt:
    print("\n🛑 Streaming transmission manually terminated by administrator.")
except Exception as e:
    print(f"❌ Streaming client failed: {e}")
