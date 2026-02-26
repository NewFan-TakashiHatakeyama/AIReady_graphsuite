from __future__ import annotations

import argparse
import json
from pathlib import Path

import boto3
import psycopg2


def _get_password(secret_arn: str, region: str) -> str:
    sm = boto3.client("secretsmanager", region_name=region)
    raw = sm.get_secret_value(SecretId=secret_arn)["SecretString"]
    return json.loads(raw)["password"]


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply Aurora SQL migrations in order.")
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--database", default="ai_ready_ontology")
    parser.add_argument("--username", default="ontology_app")
    parser.add_argument("--password")
    parser.add_argument("--secret-arn")
    parser.add_argument("--region", default="ap-northeast-1")
    parser.add_argument(
        "--migrations-dir",
        default=str(Path(__file__).parent / "migrations"),
    )
    args = parser.parse_args()

    if not args.password and not args.secret_arn:
        raise ValueError("Either --password or --secret-arn is required.")

    password = args.password or _get_password(args.secret_arn, args.region)
    migration_dir = Path(args.migrations_dir)
    files = sorted(migration_dir.glob("*.sql"))
    if not files:
        raise FileNotFoundError(f"No SQL files found under {migration_dir}")

    conn = psycopg2.connect(
        host=args.host,
        port=args.port,
        dbname=args.database,
        user=args.username,
        password=password,
        sslmode="require",
        connect_timeout=10,
    )
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            for file in files:
                print(f"Applying migration: {file.name}")
                sql = file.read_text(encoding="utf-8")
                cur.execute(sql)
                conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print("All migrations completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
