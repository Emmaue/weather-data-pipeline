import boto3
import os
from dotenv import load_dotenv

load_dotenv()

BUCKET_NAME = os.getenv('AWS_BUCKET_NAME')
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION', 'us-east-1')
)

def restore_folder(folder_name):
    archive_prefix = f"archive/{folder_name}/"
    raw_prefix = f"raw/{folder_name}/"
    
    print(f"🔍 Checking {archive_prefix}...")
    
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=archive_prefix)
    if 'Contents' not in response:
        print("   No files found to restore.")
        return

    for obj in response['Contents']:
        old_key = obj['Key']
        # Swap 'archive' with 'raw' in the path
        new_key = old_key.replace(archive_prefix, raw_prefix)
        
        print(f"   ♻️ Restoring: {old_key} -> {new_key}")
        
        # Copy
        s3.copy_object(
            Bucket=BUCKET_NAME,
            CopySource={'Bucket': BUCKET_NAME, 'Key': old_key},
            Key=new_key
        )
        # Delete old
        s3.delete_object(Bucket=BUCKET_NAME, Key=old_key)

if __name__ == "__main__":
    print(f"🚀 Restoring data in bucket: {BUCKET_NAME}")
    restore_folder("market_snapshots")
    restore_folder("crypto_data")
    print("\n✅ Restore Complete.")