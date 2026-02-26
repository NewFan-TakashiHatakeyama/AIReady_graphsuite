"""全 Lambda のログを確認"""
import sys
import io
import json
import time
import boto3

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

client = boto3.client("logs", region_name="ap-northeast-1")
start_time = int((time.time() - 600) * 1000)  # 過去10分

LAMBDAS = [
    "/aws/lambda/AIReadyConnect-receiveNotification",
    "/aws/lambda/AIReadyConnect-pullFileMetadata",
]


def print_events(log_group: str):
    name = log_group.split("/")[-1]
    print(f"\n{'='*60}")
    print(f"  {name}")
    print(f"{'='*60}")

    try:
        response = client.filter_log_events(
            logGroupName=log_group,
            startTime=start_time,
            limit=100,
        )
    except client.exceptions.ResourceNotFoundException:
        print("  (ロググループが存在しません)")
        return

    events = response.get("events", [])

    # POST/通知関連のログのみフィルタ
    interesting = []
    for e in events:
        msg = e["message"].strip()
        if msg.startswith("REPORT ") or msg.startswith("END ") or msg.startswith("INIT_START"):
            continue
        if "ELB-HealthChecker" in msg:
            continue
        interesting.append(e)

    if not interesting:
        print("  (ヘルスチェック以外のログなし)")
        return

    print(f"  {len(interesting)} 件の関連ログ\n")

    for e in interesting:
        ts = e["timestamp"]
        msg = e["message"].strip()
        t = time.strftime("%H:%M:%S", time.gmtime(ts / 1000))

        if msg.startswith("START "):
            print(f"--- {t} UTC ---")
            continue

        if msg.startswith("{"):
            try:
                d = json.loads(msg)
                level = d.get("level", "?")
                message = d.get("message", "")
                data = d.get("data", {})
                print(f"  [{level}] {message}")
                if data:
                    for k, v in data.items():
                        val_str = str(v)
                        if len(val_str) > 200:
                            val_str = val_str[:200] + "..."
                        print(f"    {k}: {val_str}")
            except json.JSONDecodeError:
                print(f"  {msg[:300]}")
        else:
            print(f"  {msg[:300]}")


for lg in LAMBDAS:
    print_events(lg)

# SQS & DynamoDB status
print(f"\n{'='*60}")
print(f"  インフラ状態")
print(f"{'='*60}")

sqs = boto3.client("sqs", region_name="ap-northeast-1")
for q in ["AIReadyConnect-NotificationQueue", "AIReadyConnect-NotificationDLQ"]:
    url = f"https://sqs.ap-northeast-1.amazonaws.com/565699611973/{q}"
    attrs = sqs.get_queue_attributes(
        QueueUrl=url,
        AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
    )["Attributes"]
    visible = attrs.get("ApproximateNumberOfMessages", "0")
    inflight = attrs.get("ApproximateNumberOfMessagesNotVisible", "0")
    print(f"  {q}: visible={visible}, in-flight={inflight}")

ddb = boto3.client("dynamodb", region_name="ap-northeast-1")
for table in ["AIReadyConnect-FileMetadata", "AIReadyConnect-IdempotencyKeys", "AIReadyConnect-DeltaTokens"]:
    count = ddb.describe_table(TableName=table)["Table"]["ItemCount"]
    print(f"  {table}: items={count}")

print()
