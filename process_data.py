import json

def lambda_handler(event, context):
    print("🔥 Lambda container awakened by S3 trigger event!")
    
    # Extract the bucket name and file name from the S3 event payload
    try:
        for record in event.get('Records', []):
            bucket_name = record['s3']['bucket']['name']
            file_key = record['s3']['object']['key']
            print(f"🎯 Target File Detected: s3://{bucket_name}/{file_key}")
            
        return {
            'statusCode': 200,
            'body': json.dumps('Telemetry data processed successfully!')
        }
    except Exception as e:
        print(f"❌ Processing failed: {str(e)}")
        return {'statusCode': 500, 'body': str(e)}