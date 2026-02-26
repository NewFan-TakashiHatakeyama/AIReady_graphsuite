"""DynamoDB FileMetadata テーブルの内容を表示"""
import sys
import io
import json
import boto3

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

ddb = boto3.resource("dynamodb", region_name="ap-northeast-1")
table = ddb.Table("AIReadyConnect-FileMetadata")

response = table.scan()
items = response.get("Items", [])

print(f"=== AIReadyConnect-FileMetadata: {len(items)} items ===\n")

DISPLAY_KEYS = [
    "item_id", "name", "file_type", "mime_type", "size",
    "web_url", "created_at", "modified_at", "modified_by",
    "is_deleted", "sharing_scope", "drive_id",
]

for i, item in enumerate(items, 1):
    print(f"--- Item {i} ---")
    for key in DISPLAY_KEYS:
        if key in item:
            val = item[key]
            if isinstance(val, str) and len(val) > 100:
                val = val[:100] + "..."
            print(f"  {key}: {val}")
    print()

# DeltaTokens
dt_table = ddb.Table("AIReadyConnect-DeltaTokens")
dt_response = dt_table.scan()
dt_items = dt_response.get("Items", [])
print(f"=== AIReadyConnect-DeltaTokens: {len(dt_items)} items ===")
for item in dt_items:
    print(f"  drive_id: {str(item.get('drive_id', ''))[:30]}...")
    token = str(item.get("delta_token", ""))
    print(f"  delta_token: {token[:80]}...")
    print()

# IdempotencyKeys
ik_table = ddb.Table("AIReadyConnect-IdempotencyKeys")
ik_response = ik_table.scan()
ik_items = ik_response.get("Items", [])
print(f"=== AIReadyConnect-IdempotencyKeys: {len(ik_items)} items ===")
for item in ik_items:
    print(f"  idempotency_key: {item.get('idempotency_key', '')}")
    print(f"  processed_at: {item.get('processed_at', '')}")
    print()

# S3 Raw Payload
s3 = boto3.client("s3", region_name="ap-northeast-1")
bucket = "aireadyconnect-raw-payload-565699611973"
try:
    resp = s3.list_objects_v2(Bucket=bucket, MaxKeys=10)
    contents = resp.get("Contents", [])
    print(f"=== S3 Raw Payloads: {len(contents)} objects ===")
    for obj in contents:
        print(f"  {obj['Key']} ({obj['Size']} bytes)")
except Exception as e:
    print(f"  S3 error: {e}")

print()
