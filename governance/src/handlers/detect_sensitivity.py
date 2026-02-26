"""detectSensitivity — SQS トリガーによるファイルコンテンツの PII/Secret 検知

詳細設計 4.1–4.8 節準拠

トリガー: SQS AIReadyGov-SensitivityDetectionQueue
入力: analyzeExposure / batchScoring が投入した機微検知リクエスト
出力:
  - DynamoDB ExposureFinding テーブルの sensitivity 関連フィールド更新
  - CloudWatch Metrics / Logs
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import boto3

from services.document_analysis import save_document_analysis
from services.domain_dictionary import enrich_noun_chunks, get_domain_dictionary
from services.embedding_generator import generate_embedding, save_embedding_to_s3
from services.entity_integration import enqueue_entity_candidates, merge_pii_and_ner
from services.ner_pipeline import NERDetectionResult, extract_ner_and_noun_chunks
from services.pii_detector import PIIDetectionResult, detect_pii
from services.scoring import calculate_sensitivity_score, classify_risk_level
from services.secret_detector import SecretDetectionResult, detect_secrets
from services.summarizer import generate_summary
from services.text_extractor import extract_text, is_supported_format, truncate_text
from shared.config import (
    ENV_DOCUMENT_ANALYSIS_ENABLED,
    SSM_MAX_FILE_SIZE_BYTES,
    SSM_RISK_SCORE_THRESHOLD,
    get_env,
    get_env_bool,
    get_ssm_float,
    get_ssm_int,
)
from shared.dynamodb import float_to_decimal, get_table
from shared.logger import get_logger
from shared.metrics import emit_count, emit_duration

logger = get_logger(__name__)

_s3_client = None
_finding_table = None


def _get_s3_client():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def _get_finding_table():
    global _finding_table
    if _finding_table is None:
        table_name = get_env("FINDING_TABLE_NAME")
        _finding_table = get_table(table_name)
    return _finding_table


def set_finding_table(table):
    """テスト用: テーブルを差し替える。"""
    global _finding_table
    _finding_table = table


def set_s3_client(client):
    """テスト用: S3 クライアントを差し替える。"""
    global _s3_client
    _s3_client = client


def handler(event: dict, context: Any) -> dict:
    """SQS イベントのエントリーポイント。

    バッチサイズ 1 で設定されているため、通常は 1 レコードのみ処理する。
    """
    records = event.get("Records", [])
    processed = 0
    errors = 0

    for record in records:
        try:
            message = json.loads(record.get("body", "{}"))
            process_sensitivity_scan(message)
            processed += 1
        except Exception:
            errors += 1
            logger.error(
                "Sensitivity scan failed",
                exc_info=True,
                extra={"extra_data": {
                    "message_id": record.get("messageId", ""),
                }},
            )
            raise

    logger.info(f"Batch complete: {processed} processed, {errors} errors")
    return {"processed": processed, "errors": errors}


def process_sensitivity_scan(message: dict) -> None:
    """1 ファイルの機微スキャン処理フロー（詳細設計 4.3 準拠）。"""
    start_time = time.time()

    finding_id = message.get("finding_id", "")
    tenant_id = message.get("tenant_id", "")
    raw_s3_key = message.get("raw_s3_key", "")
    raw_s3_bucket = message.get("raw_s3_bucket", "")
    mime_type = message.get("mime_type", "")
    file_size = message.get("size", 0)
    item_name = message.get("item_name", "")

    logger.info(
        f"Processing sensitivity scan: {finding_id}",
        extra={"extra_data": {
            "finding_id": finding_id,
            "item_name": item_name,
            "mime_type": mime_type,
            "size": file_size,
        }},
    )

    dims = {"TenantId": tenant_id, "Lambda": "detectSensitivity"}

    # Step 1: サイズチェック
    max_file_size = _get_max_file_size()
    if file_size > max_file_size:
        logger.warning(f"File too large: {file_size} bytes, skipping content scan")
        _update_finding_scan_status(tenant_id, finding_id, skipped=True, reason="file_too_large")
        emit_count("AIReadyGov.ScanSkipped", dimensions=dims)
        return

    # Step 2: 対応形式チェック
    if not is_supported_format(mime_type):
        logger.info(f"Unsupported format: {mime_type}, skipping content scan")
        _update_finding_scan_status(
            tenant_id, finding_id, skipped=True, reason="unsupported_format"
        )
        emit_count("AIReadyGov.ScanSkipped", dimensions=dims)
        return

    # Step 3: S3 からファイル取得
    file_content = _download_from_s3(raw_s3_bucket, raw_s3_key)
    if file_content is None:
        logger.warning(f"Failed to download file from S3: {raw_s3_bucket}/{raw_s3_key}")
        _update_finding_scan_status(tenant_id, finding_id, skipped=True, reason="s3_error")
        emit_count("AIReadyGov.ScanSkipped", dimensions=dims)
        return

    extracted_text = None
    try:
        # Step 4: テキスト抽出
        extracted_text = extract_text(file_content, mime_type)

        if not extracted_text or not extracted_text.strip():
            _update_finding_scan_status(
                tenant_id, finding_id, skipped=True, reason="no_text_content"
            )
            emit_count("AIReadyGov.ScanSkipped", dimensions=dims)
            return

        # Step 5: テキスト長制限
        truncated_text = truncate_text(extracted_text)

        # Step 6: PII 検出
        pii_results = detect_pii(truncated_text)

        # Step 7: Secret/Credential 検出
        secret_results = detect_secrets(truncated_text)

        # Step 8: SensitivityScore 正式算出
        existing_label_score = _get_existing_label_score(tenant_id, finding_id)
        sensitivity_score = calculate_sensitivity_score(
            pii_results=_pii_result_to_dict(pii_results),
            secret_results=_secret_result_to_dict(secret_results),
            existing_label_score=existing_label_score,
        )

        # Step 9: Finding 更新 + RiskScore 再計算
        _update_finding_with_sensitivity(
            tenant_id=tenant_id,
            finding_id=finding_id,
            sensitivity_score=sensitivity_score,
            pii_results=pii_results,
            secret_results=secret_results,
        )

        # メトリクス
        if pii_results.detected:
            emit_count("AIReadyGov.PIIDetected", dimensions=dims)
        if secret_results.detected:
            emit_count("AIReadyGov.SecretsDetected", dimensions=dims)

        # Step 10+: Phase 6.5 解析一元化（feature flag）
        if get_env_bool(ENV_DOCUMENT_ANALYSIS_ENABLED, default=False):
            _run_document_analysis_extensions(
                tenant_id=tenant_id,
                item_id=message.get("item_id", ""),
                item_name=item_name,
                mime_type=mime_type,
                pii_results=pii_results,
                secret_results=secret_results,
                source_text=truncated_text,
            )

    except Exception as e:
        logger.error(f"Content analysis failed for {finding_id}: {e}", exc_info=True)
        emit_count("AIReadyGov.TextExtractionErrors", dimensions=dims)
        raise
    finally:
        # Step 10: メモリ上のファイルコンテンツを確実に破棄
        del file_content
        if extracted_text is not None:
            del extracted_text

    elapsed_ms = (time.time() - start_time) * 1000
    emit_duration("AIReadyGov.ScanDurationMs", elapsed_ms, dimensions=dims)
    logger.info(
        f"Sensitivity scan complete: {finding_id} ({elapsed_ms:.0f}ms)",
        extra={"extra_data": {
            "finding_id": finding_id,
            "sensitivity_score": sensitivity_score,
            "pii_detected": pii_results.detected,
            "pii_count": pii_results.count,
            "secrets_detected": secret_results.detected,
        }},
    )


def _download_from_s3(bucket: str, key: str) -> bytes | None:
    """S3 からファイルコンテンツをダウンロードする。"""
    try:
        client = _get_s3_client()
        response = client.get_object(Bucket=bucket, Key=key)
        return response["Body"].read()
    except Exception as e:
        logger.warning(f"S3 download failed: {bucket}/{key}: {e}")
        return None


def _run_document_analysis_extensions(
    tenant_id: str,
    item_id: str,
    item_name: str,
    mime_type: str,
    pii_results: PIIDetectionResult,
    secret_results: SecretDetectionResult,
    source_text: str,
) -> None:
    """Phase 6.5 の追加解析を実行する（失敗しても本体フローは継続）。"""
    ner_results = NERDetectionResult()
    summary = ""
    embedding_s3_key = ""

    # NER + 名詞チャンク
    try:
        ner_results = extract_ner_and_noun_chunks(source_text)
        domain_dict = get_domain_dictionary()
        ner_results.noun_chunks = enrich_noun_chunks(ner_results.noun_chunks, domain_dict)
    except Exception as exc:
        logger.warning(f"NER pipeline failed: {exc}")

    # 要約
    try:
        summary = generate_summary(source_text)
    except Exception as exc:
        logger.warning(f"Summary generation failed: {exc}")
        summary = source_text[:200]

    # Embedding 生成 + S3 保存
    try:
        embeddings = generate_embedding(source_text)
        embedding_s3_key = save_embedding_to_s3(tenant_id, item_id, embeddings)
    except Exception as exc:
        logger.warning(f"Embedding generation/save failed: {exc}")

    # DocumentAnalysis 保存
    try:
        save_document_analysis(
            tenant_id=tenant_id,
            item_id=item_id,
            pii_results=pii_results,
            ner_results=ner_results,
            secret_results=secret_results,
            summary=summary,
            embedding_s3_key=embedding_s3_key,
            source_text_length=len(source_text),
        )
    except Exception as exc:
        logger.warning(f"DocumentAnalysis save failed: {exc}")

    # Entity candidates 送信
    try:
        candidates = merge_pii_and_ner(pii_results, ner_results)
        enqueue_entity_candidates(
            tenant_id=tenant_id,
            item_id=item_id,
            candidates=candidates,
            source_document={"item_name": item_name, "mime_type": mime_type},
        )
    except Exception as exc:
        logger.warning(f"Entity candidate enqueue failed: {exc}")


def _get_existing_label_score(tenant_id: str, finding_id: str) -> float:
    """既存 Finding のラベルベーススコアを取得する。"""
    try:
        table = _get_finding_table()
        response = table.get_item(
            Key={"tenant_id": tenant_id, "finding_id": finding_id},
            ProjectionExpression="sensitivity_score, sensitivity_scan_at",
        )
        item = response.get("Item")
        if item and not item.get("sensitivity_scan_at"):
            return float(item.get("sensitivity_score", 1.0))
        return 1.0
    except Exception:
        return 1.0


def _update_finding_with_sensitivity(
    tenant_id: str,
    finding_id: str,
    sensitivity_score: float,
    pii_results: PIIDetectionResult,
    secret_results: SecretDetectionResult,
) -> None:
    """detectSensitivity の結果で Finding を更新し、RiskScore を再計算する（詳細設計 4.7）。"""
    table = _get_finding_table()
    now = datetime.now(timezone.utc).isoformat()

    finding = table.get_item(
        Key={"tenant_id": tenant_id, "finding_id": finding_id}
    ).get("Item")

    if finding is None:
        logger.warning(f"Finding not found: {finding_id}")
        return

    new_risk_score = (
        float(finding.get("exposure_score", 1))
        * sensitivity_score
        * float(finding.get("activity_score", 1))
        * float(finding.get("ai_amplification", 1))
    )
    new_risk_score = round(new_risk_score, 2)

    table.update_item(
        Key={"tenant_id": tenant_id, "finding_id": finding_id},
        UpdateExpression="""
            SET sensitivity_score = :ss,
                risk_score = :rs,
                risk_level = :rl,
                pii_detected = :pd,
                pii_types = :pt,
                pii_count = :pc,
                pii_density = :pden,
                secrets_detected = :sd,
                secret_types = :st,
                sensitivity_scan_at = :scan_at,
                last_evaluated_at = :eval_at
        """,
        ExpressionAttributeValues={
            ":ss": float_to_decimal(sensitivity_score),
            ":rs": float_to_decimal(new_risk_score),
            ":rl": classify_risk_level(new_risk_score),
            ":pd": pii_results.detected,
            ":pt": pii_results.types if pii_results.types else None,
            ":pc": pii_results.count,
            ":pden": pii_results.density,
            ":sd": secret_results.detected,
            ":st": secret_results.types if secret_results.types else None,
            ":scan_at": now,
            ":eval_at": now,
        },
    )

    risk_threshold = _get_risk_threshold()
    if new_risk_score < risk_threshold:
        from services.finding_manager import close_finding

        close_finding(tenant_id, finding_id)

    logger.info(f"Finding updated with sensitivity: {finding_id}, risk_score={new_risk_score}")


def _update_finding_scan_status(
    tenant_id: str,
    finding_id: str,
    skipped: bool,
    reason: str,
) -> None:
    """スキャン状況のみ更新する（スキップ時）。"""
    table = _get_finding_table()
    now = datetime.now(timezone.utc).isoformat()

    try:
        table.update_item(
            Key={"tenant_id": tenant_id, "finding_id": finding_id},
            UpdateExpression="""
                SET sensitivity_scan_at = :scan_at,
                    last_evaluated_at = :eval_at
            """,
            ExpressionAttributeValues={
                ":scan_at": now,
                ":eval_at": now,
            },
            ConditionExpression="attribute_exists(finding_id)",
        )
    except Exception as e:
        logger.warning(f"Failed to update scan status for {finding_id}: {e}")


def _pii_result_to_dict(result: PIIDetectionResult) -> dict:
    """PIIDetectionResult を scoring.calculate_sensitivity_score が受け取る dict に変換する。"""
    return {
        "detected": result.detected,
        "high_risk_detected": result.high_risk_detected,
        "density": result.density,
    }


def _secret_result_to_dict(result: SecretDetectionResult) -> dict:
    """SecretDetectionResult を scoring.calculate_sensitivity_score が受け取る dict に変換する。"""
    return {
        "detected": result.detected,
    }


def _get_max_file_size() -> int:
    try:
        return get_ssm_int(SSM_MAX_FILE_SIZE_BYTES, default=52428800)
    except Exception:
        return 52428800


def _get_risk_threshold() -> float:
    try:
        return get_ssm_float(SSM_RISK_SCORE_THRESHOLD, default=2.0)
    except Exception:
        return 2.0
