import boto3
import os
import json
from dotenv import load_dotenv

load_dotenv()

# Configuration
BUCKET_NAME = os.getenv('AWS_BUCKET_NAME')
REGION = os.getenv('AWS_REGION', 'us-east-1')

def get_s3_client():
    return boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=REGION
    )

def upload_to_s3(data, folder, filename):
    """
    Uploads a dict to S3.
    Target: s3://bucket/folder/filename
    """
    s3 = get_s3_client()
    key = f"{folder}/{filename}"
    
    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=json.dumps(data, indent=4),
            ContentType='application/json'
        )
        print(f"☁️  Uploaded to S3: s3://{BUCKET_NAME}/{key}")
        return True
    except Exception as e:
        print(f"❌ S3 Upload Failed: {e}")
        return False

def list_s3_files(folder):
    """List files in an S3 folder (prefix)"""
    s3 = get_s3_client()
    try:
        # Ensure folder ends with /
        if not folder.endswith('/'):
            folder += '/'
            
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=folder)
        if 'Contents' in response:
            return [item['Key'] for item in response['Contents']]
        return []
    except Exception as e:
        print(f"❌ Error listing S3 files: {e}")
        return []

def read_from_s3(key):
    """Read and parse a JSON file from S3"""
    s3 = get_s3_client()
    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        content = response['Body'].read().decode('utf-8')
        return json.loads(content)
    except Exception as e:
        print(f"❌ Error reading {key}: {e}")
        return None

def move_s3_object(source_key, dest_folder):
    """
    'Move' in S3 is actually Copy + Delete.
    Moves file from 'data/raw/file.json' to 'data/archive/file.json'
    """
    s3 = get_s3_client()
    filename = source_key.split('/')[-1]
    dest_key = f"{dest_folder}/{filename}"
    
    try:
        # 1. Copy
        s3.copy_object(
            Bucket=BUCKET_NAME,
            CopySource={'Bucket': BUCKET_NAME, 'Key': source_key},
            Key=dest_key
        )
        # 2. Delete Original
        s3.delete_object(Bucket=BUCKET_NAME, Key=source_key)
        print(f"📦 Archived: {source_key} -> {dest_key}")
        return True
    except Exception as e:
        print(f"❌ Error moving S3 object: {e}")
        return False