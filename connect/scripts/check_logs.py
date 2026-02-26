"""CloudWatch Logs から受信ログを検索"""
import sys
import io
import json
import time
import boto3

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

client = boto3.client("logs", region_name="ap-northeast-1")
log_group = "/aws/lambda/AIReadyConnect-receiveNotification"

# 過去10分のログを取得
start_time = int((time.time() - 600) * 1000)

response = client.filter_log_events(
    logGroupName=log_group,
    startTime=start_time,
    limit=100,
)

print(f"=== {len(response['events'])} events found ===\n")

for event in response["events"]:
    ts = event["timestamp"]
    msg = event["message"].strip()
    # REPORT/END/START 行はスキップ
    if msg.startswith("REPORT ") or msg.startswith("END ") or msg.startswith("INIT_START"):
        continue
    if msg.startswith("START "):
        print(f"--- {time.strftime('%H:%M:%S', time.gmtime(ts/1000))} UTC ---")
        continue

    # JSON ログを整形
    if msg.startswith("{"):
        try:
            d = json.loads(msg)
            level = d.get("level", "?")
            message = d.get("message", "")
            data = d.get("data", {})
            print(f"  [{level}] {message}")
            if data:
                for k, v in data.items():
                    print(f"    {k}: {v}")
        except json.JSONDecodeError:
            print(f"  {msg[:200]}")
    else:
        print(f"  {msg[:200]}")

print()
