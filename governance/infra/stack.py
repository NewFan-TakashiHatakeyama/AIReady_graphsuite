"""AI Ready Governance - CDK スタック

Phase 1: インフラ基盤
  T-001: CDK プロジェクト初期化
  T-002: DynamoDB ExposureFinding テーブル + GSI
  T-003: SQS キュー (SensitivityDetectionQueue + DLQ x2)
  T-004: S3 バケット (レポート用)
  T-005: IAM ロール (Lambda 実行ロール)
  T-006: SSM パラメータ投入

Phase 3: Lambda 1 — analyzeExposure
  T-013: analyzeExposure Lambda + DynamoDB Streams イベントソース

Phase 4: Lambda 2 — detectSensitivity
  T-020: detectSensitivity Docker Lambda + ECR + SQS イベントソース

Phase 5: Lambda 3 — batchScoring
  T-023: batchScoring Lambda + EventBridge rate(1 day) スケジュール

Phase 6: E2E + 監視
  T-026: CloudWatch アラーム設定（DLQ / エラー率 / バッチ実行時間）
"""

from constructs import Construct, Node
import aws_cdk as cdk
from aws_cdk import (
    Stack,
    RemovalPolicy,
    Duration,
    aws_cloudwatch as cloudwatch,
    aws_dynamodb as dynamodb,
    aws_ecr_assets as ecr_assets,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_lambda as _lambda,
    aws_lambda_event_sources as lambda_event_sources,
    aws_sqs as sqs,
    aws_s3 as s3,
    aws_iam as iam,
    aws_ssm as ssm,
)

PROJECT = "AIReadyGov"
CONNECT_PROJECT = "AIReadyConnect"
ONTOLOGY_REPORT_BUCKET_PARAM = "/ai-ready/ontology/report_bucket"
ONTOLOGY_REPORT_PREFIX_PARAM = "/ai-ready/ontology/report_prefix"


def _graphsuite_bucket_suffix(stack: Stack) -> str:
    raw = (stack.node.try_get_context("graphsuiteS3BucketSuffix") or "").strip()
    if not raw:
        return ""
    return raw if raw.startswith("-") else f"-{raw}"


def _context_bool(node: Node, key: str, *, default: bool) -> bool:
    raw = node.try_get_context(key)
    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    return str(raw).strip().lower() in ("1", "true", "yes", "y")


class AIReadyGovernanceStack(Stack):
    """AI Ready Governance 全リソースを含む CDK スタック"""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ==========================================
        # Tags
        # ==========================================
        cdk.Tags.of(self).add("Project", PROJECT)
        cdk.Tags.of(self).add("Environment", "poc")

        # ==========================================
        # Connect リソースの参照（既存スタックから Import）
        # ==========================================
        connect_file_metadata_table_name = f"{CONNECT_PROJECT}-FileMetadata"
        _suffix = _graphsuite_bucket_suffix(self)
        connect_raw_bucket_name = f"{CONNECT_PROJECT.lower()}-raw-payload{_suffix}"

        connect_file_metadata_table = dynamodb.Table.from_table_attributes(
            self,
            "ConnectFileMetadata",
            table_name=connect_file_metadata_table_name,
            table_stream_arn=cdk.Fn.import_value("ConnectFileMetadataStreamArn"),
        )

        connect_raw_bucket = s3.Bucket.from_bucket_name(
            self,
            "ConnectRawBucket",
            connect_raw_bucket_name,
        )

        # ==========================================
        # T-002: DynamoDB ExposureFinding テーブル
        # ==========================================
        finding_table = dynamodb.Table(
            self,
            "ExposureFinding",
            table_name=f"{PROJECT}-ExposureFinding",
            partition_key=dynamodb.Attribute(
                name="tenant_id", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="finding_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=True,
            ),
        )

        # GSI-ItemFinding: item_id から Finding を逆引き（upsert 時の重複チェック）
        finding_table.add_global_secondary_index(
            index_name="GSI-ItemFinding",
            partition_key=dynamodb.Attribute(
                name="item_id", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="tenant_id", type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        # GSI-StatusFinding: status 別の Finding 一覧（acknowledged の期限切れチェック等）
        finding_table.add_global_secondary_index(
            index_name="GSI-StatusFinding",
            partition_key=dynamodb.Attribute(
                name="tenant_id", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="status", type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        # ==========================================
        # T-041: DynamoDB DocumentAnalysis テーブル
        # ==========================================
        document_analysis_table = dynamodb.Table(
            self,
            "DocumentAnalysis",
            table_name=f"{PROJECT}-DocumentAnalysis",
            partition_key=dynamodb.Attribute(
                name="tenant_id", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="item_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            time_to_live_attribute="ttl",
            removal_policy=RemovalPolicy.DESTROY,
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=True,
            ),
        )
        document_analysis_table.add_global_secondary_index(
            index_name="GSI-AnalyzedAt",
            partition_key=dynamodb.Attribute(
                name="tenant_id", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="analyzed_at", type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        policy_scope_table = dynamodb.Table(
            self,
            "PolicyScope",
            table_name=f"{PROJECT}-PolicyScope",
            partition_key=dynamodb.Attribute(
                name="tenant_id", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="policy_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=True,
            ),
        )
        policy_scope_table.add_global_secondary_index(
            index_name="GSI-UpdatedAt",
            partition_key=dynamodb.Attribute(
                name="tenant_id", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="updated_at", type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        scan_job_table = dynamodb.Table(
            self,
            "ScanJob",
            table_name=f"{PROJECT}-ScanJob",
            partition_key=dynamodb.Attribute(
                name="tenant_id", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="job_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=True,
            ),
        )
        scan_job_table.add_global_secondary_index(
            index_name="GSI-AcceptedAt",
            partition_key=dynamodb.Attribute(
                name="tenant_id", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="accepted_at", type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        audit_log_table = dynamodb.Table(
            self,
            "AuditLog",
            table_name=f"{PROJECT}-AuditLog",
            partition_key=dynamodb.Attribute(
                name="tenant_id", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="audit_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=True,
            ),
        )
        audit_log_table.add_global_secondary_index(
            index_name="GSI-Timestamp",
            partition_key=dynamodb.Attribute(
                name="tenant_id", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="timestamp", type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        # ==========================================
        # T-003: SQS キュー（機微検知 + DLQ）
        # ==========================================

        # --- DLQ: analyzeExposure ---
        analyze_exposure_dlq = sqs.Queue(
            self,
            "AnalyzeExposureDLQ",
            queue_name=f"{PROJECT}-analyzeExposure-DLQ",
            retention_period=Duration.days(14),
            removal_policy=RemovalPolicy.DESTROY,
        )

        # --- DLQ: detectSensitivity ---
        detect_sensitivity_dlq = sqs.Queue(
            self,
            "DetectSensitivityDLQ",
            queue_name=f"{PROJECT}-detectSensitivity-DLQ",
            retention_period=Duration.days(14),
            removal_policy=RemovalPolicy.DESTROY,
        )

        # --- メインキュー: SensitivityDetectionQueue ---
        sensitivity_queue = sqs.Queue(
            self,
            "SensitivityDetectionQueue",
            queue_name=f"{PROJECT}-SensitivityDetectionQueue",
            visibility_timeout=Duration.seconds(660),
            retention_period=Duration.days(4),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3,
                queue=detect_sensitivity_dlq,
            ),
            removal_policy=RemovalPolicy.DESTROY,
        )

        # ==========================================
        # T-004: S3 バケット（レポート用）
        # ==========================================
        report_bucket = s3.Bucket(
            self,
            "ReportBucket",
            bucket_name=f"{PROJECT.lower()}-reports{_suffix}",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="glacier-90d",
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.GLACIER,
                            transition_after=Duration.days(90),
                        ),
                    ],
                ),
                s3.LifecycleRule(
                    id="expire-365d",
                    expiration=Duration.days(365),
                ),
            ],
        )

        vectors_bucket = s3.Bucket(
            self,
            "VectorsBucket",
            bucket_name=f"aiready-vectors{_suffix}",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="vectors-deep-archive-365d",
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.DEEP_ARCHIVE,
                            transition_after=Duration.days(365),
                        ),
                    ],
                ),
            ],
        )

        # Ontology Entity Resolution FIFO（Ontology スタックで作成済みキューを参照）
        entity_resolution_queue = sqs.Queue.from_queue_arn(
            self,
            "OntologyEntityResolutionQueue",
            queue_arn=f"arn:aws:sqs:{self.region}:{self.account}:AIReadyOntology-EntityResolutionQueue.fifo",
        )

        if _context_bool(self.node, "governanceOntologyFromContext", default=False):
            ontology_report_bucket_name = (
                str(self.node.try_get_context("governanceOntologyReportBucket") or "").strip()
                or "aiready-ontology-reports"
            )
            ontology_report_prefix = (
                str(self.node.try_get_context("governanceOntologyReportPrefix") or "").strip()
                or "tenant-alpha/reports/"
            )
        else:
            ontology_report_bucket_name = ssm.StringParameter.value_for_string_parameter(
                self,
                ONTOLOGY_REPORT_BUCKET_PARAM,
            )
            ontology_report_prefix = ssm.StringParameter.value_for_string_parameter(
                self,
                ONTOLOGY_REPORT_PREFIX_PARAM,
            )

        # ==========================================
        # T-005: IAM ロール（Lambda 実行ロール）
        # ==========================================
        lambda_role = iam.Role(
            self,
            "LambdaRole",
            role_name=f"{PROJECT}-LambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaVPCAccessExecutionRole"
                ),
            ],
        )

        # DynamoDB: ExposureFinding テーブル (RW)
        finding_table.grant_read_write_data(lambda_role)
        scan_job_table.grant_read_write_data(lambda_role)

        # DynamoDB: Connect の FileMetadata テーブル (R)
        connect_file_metadata_table.grant_read_data(lambda_role)
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "dynamodb:GetItem",
                    "dynamodb:BatchGetItem",
                    "dynamodb:Query",
                    "dynamodb:Scan",
                ],
                resources=[
                    f"arn:aws:dynamodb:{self.region}:{self.account}:table/{connect_file_metadata_table_name}",
                    f"arn:aws:dynamodb:{self.region}:{self.account}:table/{connect_file_metadata_table_name}/index/*",
                ],
            )
        )

        # SQS: SensitivityDetectionQueue (SendMessage / ReceiveMessage / DeleteMessage)
        sensitivity_queue.grant_send_messages(lambda_role)
        sensitivity_queue.grant_consume_messages(lambda_role)

        # DynamoDB / S3: DocumentAnalysis + ベクトル（detectSensitivity 拡張）
        document_analysis_table.grant_read_write_data(lambda_role)
        vectors_bucket.grant_read_write(lambda_role)

        # SQS: Ontology EntityResolution（候補投入）
        entity_resolution_queue.grant_send_messages(lambda_role)

        # S3: Connect の raw-payload バケット (GetObject)
        connect_raw_bucket.grant_read(lambda_role)

        # S3: Governance の reports バケット (PutObject)
        report_bucket.grant_put(lambda_role)

        # S3: Ontology reports バケット (GetObject / ListBucket)
        ontology_report_bucket = s3.Bucket.from_bucket_name(
            self,
            "OntologyReportBucket",
            ontology_report_bucket_name,
        )
        ontology_report_bucket.grant_read(lambda_role)

        # SSM: /aiready/governance/* (Get/Put for runtime config and quota counters)
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "ssm:GetParameter",
                    "ssm:GetParameters",
                    "ssm:PutParameter",
                ],
                resources=[
                    f"arn:aws:ssm:{self.region}:{self.account}:parameter/aiready/governance/*",
                    f"arn:aws:ssm:{self.region}:{self.account}:parameter/aiready/connect/*",
                    f"arn:aws:ssm:{self.region}:{self.account}:parameter/aiready/ontology/*",
                    f"arn:aws:ssm:{self.region}:{self.account}:parameter/ai-ready/ontology/*",
                    f"arn:aws:ssm:{self.region}:{self.account}:parameter/MSGraphTenantId",
                    f"arn:aws:ssm:{self.region}:{self.account}:parameter/MSGraphClientId",
                    f"arn:aws:ssm:{self.region}:{self.account}:parameter/MSGraphClientSecret",
                ],
            )
        )

        # Bedrock: Claude Haiku / Titan Embeddings V2
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["bedrock:InvokeModel"],
                resources=[
                    f"arn:aws:bedrock:{self.region}::foundation-model/anthropic.claude-3-haiku-*",
                    f"arn:aws:bedrock:{self.region}::foundation-model/amazon.titan-embed-text-v2:*",
                ],
            )
        )

        # CloudWatch: Logs + Metrics
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "cloudwatch:PutMetricData",
                ],
                resources=["*"],
                conditions={
                    "StringEquals": {
                        "cloudwatch:namespace": "AIReadyGovernance",
                    }
                },
            )
        )

        # ECR: GetDownloadUrlForLayer (detectSensitivity 用 Docker Lambda)
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "ecr:GetDownloadUrlForLayer",
                    "ecr:BatchGetImage",
                    "ecr:GetAuthorizationToken",
                ],
                resources=["*"],
            )
        )

        # ==========================================
        # T-006: SSM パラメータ投入
        # ==========================================
        ssm_params = {
            "/aiready/governance/risk_score_threshold": ("2.0", "Finding 生成の RiskScore 閾値"),
            "/aiready/governance/max_exposure_score": ("10.0", "ExposureScore の上限キャップ"),
            "/aiready/governance/permissions_count_threshold": (
                "50",
                "excessive_permissions 判定の閾値",
            ),
            "/aiready/governance/rescan_interval_days": ("7", "機微検知の再スキャン間隔（日）"),
            "/aiready/governance/max_file_size_bytes": (
                "52428800",
                "機微検知のファイルサイズ上限（50MB）",
            ),
            "/aiready/governance/max_text_length": ("500000", "テキスト抽出の文字数上限"),
            "/aiready/governance/batch_scoring_hour_utc": ("5", "日次バッチの実行時刻（UTC）"),
            "/aiready/connect/sensitivity_label_map": (
                "{}",
                "Sensitivity label mapping JSON. ex: {\"Confidential\":\"<label-guid>\"}",
            ),
        }

        for i, (param_name, (value, description)) in enumerate(ssm_params.items()):
            ssm.StringParameter(
                self,
                f"SSMParam{i}",
                parameter_name=param_name,
                string_value=value,
                description=description,
                tier=ssm.ParameterTier.STANDARD,
            )

        # ==========================================
        # T-013: analyzeExposure Lambda
        # ==========================================
        analyze_exposure_fn = _lambda.Function(
            self,
            "AnalyzeExposure",
            function_name=f"{PROJECT}-analyzeExposure",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handlers.analyze_exposure.handler",
            code=_lambda.Code.from_asset("../src"),
            memory_size=512,
            timeout=Duration.seconds(60),
            reserved_concurrent_executions=50,
            role=lambda_role,
            environment={
                "FINDING_TABLE_NAME": finding_table.table_name,
                "SENSITIVITY_QUEUE_URL": sensitivity_queue.queue_url,
                "RAW_PAYLOAD_BUCKET": connect_raw_bucket.bucket_name,
                "REPORT_BUCKET": report_bucket.bucket_name,
                "LOG_LEVEL": "INFO",
            },
            dead_letter_queue=analyze_exposure_dlq,
        )

        analyze_exposure_fn.add_event_source(
            lambda_event_sources.DynamoEventSource(
                connect_file_metadata_table,
                starting_position=_lambda.StartingPosition.LATEST,
                batch_size=10,
                max_batching_window=Duration.seconds(5),
                retry_attempts=3,
                bisect_batch_on_error=True,
                on_failure=lambda_event_sources.SqsDlq(analyze_exposure_dlq),
            )
        )

        # DynamoDB Streams 読み取り権限（イベントソースマッピングに必要）
        connect_file_metadata_table.grant_stream_read(lambda_role)

        # ==========================================
        # T-020: detectSensitivity Docker Lambda + ECR
        # ==========================================
        detect_sensitivity_fn = _lambda.DockerImageFunction(
            self,
            "DetectSensitivity",
            function_name=f"{PROJECT}-detectSensitivity",
            code=_lambda.DockerImageCode.from_image_asset(
                directory="..",
                file="docker/Dockerfile.sensitivity",
                platform=ecr_assets.Platform.LINUX_AMD64,
                exclude=[
                    "infra/cdk.out",
                    "infra/cdk.out*",
                    "tests",
                    "Docs",
                    "docs",
                    ".pytest_cache",
                    "__pycache__",
                    "*.md",
                    ".env*",
                ],
            ),
            memory_size=4096,
            timeout=Duration.seconds(600),
            ephemeral_storage_size=cdk.Size.mebibytes(1024),
            reserved_concurrent_executions=20,
            role=lambda_role,
            environment={
                "FINDING_TABLE_NAME": finding_table.table_name,
                "RAW_PAYLOAD_BUCKET": connect_raw_bucket.bucket_name,
                "GINZA_MODEL": "ja_ginza",
                "LOG_LEVEL": "INFO",
                "DOCUMENT_ANALYSIS_TABLE_NAME": document_analysis_table.table_name,
                "VECTORS_BUCKET": vectors_bucket.bucket_name,
                "ENTITY_RESOLUTION_QUEUE_URL": entity_resolution_queue.queue_url,
                "DOCUMENT_ANALYSIS_ENABLED": "true",
            },
            dead_letter_queue=detect_sensitivity_dlq,
        )

        detect_sensitivity_fn.add_event_source(
            lambda_event_sources.SqsEventSource(
                sensitivity_queue,
                batch_size=1,
            )
        )

        # ==========================================
        # T-023: batchScoring Lambda + EventBridge
        # ==========================================
        batch_scoring_kwargs: dict[str, object] = {
            "function_name": f"{PROJECT}-batchScoring",
            "runtime": _lambda.Runtime.PYTHON_3_12,
            "handler": "handlers.batch_scoring.handler",
            "code": _lambda.Code.from_asset(
                "../src",
                bundling=cdk.BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_12.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        "pip install --no-cache-dir -r requirements.batch_scoring.txt -t /asset-output "
                        "&& cp -r . /asset-output",
                    ],
                ),
            ),
            "memory_size": 1024,
            "timeout": Duration.seconds(900),
            "reserved_concurrent_executions": 1,
            "role": lambda_role,
            "environment": {
                "FINDING_TABLE_NAME": finding_table.table_name,
                "CONNECT_TABLE_NAME": connect_file_metadata_table_name,
                "SCAN_JOB_TABLE_NAME": scan_job_table.table_name,
                "SENSITIVITY_QUEUE_URL": sensitivity_queue.queue_url,
                "RAW_PAYLOAD_BUCKET": connect_raw_bucket.bucket_name,
                "REPORT_BUCKET": report_bucket.bucket_name,
                "ONTOLOGY_REPORT_BUCKET": ontology_report_bucket_name,
                "ONTOLOGY_REPORT_PREFIX": ontology_report_prefix,
                "LOG_LEVEL": "INFO",
            },
        }

        batch_scoring_fn = _lambda.Function(
            self,
            "BatchScoring",
            **batch_scoring_kwargs,
        )

        # 単一テナント実行契約: EventBridge からも tenant_id を明示して起動する。
        # 将来 Step Functions へ移行する場合も同じイベント契約を引き継ぐ。
        batch_scoring_tenant_id = cdk.CfnParameter(
            self,
            "BatchScoringTenantId",
            type="String",
            default="tenant-001",
            description="Tenant ID used by the daily batch scoring schedule.",
        )

        # EventBridge rate(1 day) 05:00 UTC
        batch_scoring_rule = events.Rule(
            self,
            "BatchScoringSchedule",
            rule_name=f"{PROJECT}-batchScoring-daily",
            schedule=events.Schedule.cron(
                minute="0",
                hour="5",
                month="*",
                week_day="*",
                year="*",
            ),
            description="Daily batch scoring at 05:00 UTC",
        )
        batch_scoring_rule.add_target(
            events_targets.LambdaFunction(
                batch_scoring_fn,
                retry_attempts=2,
                event=events.RuleTargetInput.from_object(
                    {
                        "trigger": "scheduled",
                        "tenant_id": batch_scoring_tenant_id.value_as_string,
                    }
                ),
            )
        )

        # ==========================================
        # T-070: remediateFinding Lambda (on-demand)
        # ==========================================
        remediate_finding_fn = _lambda.Function(
            self,
            "RemediateFinding",
            function_name=f"{PROJECT}-remediateFinding",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handlers.remediate_finding.handler",
            code=_lambda.Code.from_asset("../src"),
            memory_size=512,
            timeout=Duration.seconds(120),
            reserved_concurrent_executions=10,
            role=lambda_role,
            environment={
                "FINDING_TABLE_NAME": finding_table.table_name,
                "CONNECT_TABLE_NAME": connect_file_metadata_table_name,
                "GOVERNANCE_REMEDIATION_EXECUTION_MODE": "approval_then_auto_execute",
                # Default: allow label automation in normal remediation flow.
                "GOVERNANCE_LABEL_AUTOMATION_MODE": "realtime",
                "GOVERNANCE_LABEL_AUTOMATION_BATCH_HOUR_JST": "3",
                "GOVERNANCE_LABEL_AUTOMATION_DAILY_LIMIT": "20",
                "LOG_LEVEL": "INFO",
            },
        )

        # ==========================================
        # T-064: CloudWatch ダッシュボード（負荷/コスト可視化）
        # ==========================================
        governance_dashboard = cloudwatch.Dashboard(
            self,
            "GovernanceOperationsDashboard",
            dashboard_name=f"{PROJECT}-Operations",
        )
        governance_dashboard.add_widgets(
            cloudwatch.GraphWidget(
                title="Lambda Errors / Throttles",
                left=[
                    analyze_exposure_fn.metric_errors(period=Duration.minutes(5), statistic="Sum"),
                    detect_sensitivity_fn.metric_errors(period=Duration.minutes(5), statistic="Sum"),
                    batch_scoring_fn.metric_errors(period=Duration.minutes(5), statistic="Sum"),
                    remediate_finding_fn.metric_errors(period=Duration.minutes(5), statistic="Sum"),
                ],
                right=[
                    analyze_exposure_fn.metric_throttles(period=Duration.minutes(5), statistic="Sum"),
                    detect_sensitivity_fn.metric_throttles(period=Duration.minutes(5), statistic="Sum"),
                    batch_scoring_fn.metric_throttles(period=Duration.minutes(5), statistic="Sum"),
                    remediate_finding_fn.metric_throttles(period=Duration.minutes(5), statistic="Sum"),
                ],
                width=12,
            ),
            cloudwatch.GraphWidget(
                title="Lambda Duration p95",
                left=[
                    analyze_exposure_fn.metric_duration(period=Duration.minutes(5), statistic="p95"),
                    detect_sensitivity_fn.metric_duration(period=Duration.minutes(5), statistic="p95"),
                    batch_scoring_fn.metric_duration(period=Duration.minutes(5), statistic="p95"),
                    remediate_finding_fn.metric_duration(period=Duration.minutes(5), statistic="p95"),
                ],
                width=12,
            ),
            cloudwatch.GraphWidget(
                title="DynamoDB Capacity (RCU/WCU)",
                left=[
                    finding_table.metric_consumed_read_capacity_units(
                        period=Duration.minutes(5), statistic="Sum"
                    ),
                    finding_table.metric_consumed_write_capacity_units(
                        period=Duration.minutes(5), statistic="Sum"
                    ),
                    scan_job_table.metric_consumed_read_capacity_units(
                        period=Duration.minutes(5), statistic="Sum"
                    ),
                    scan_job_table.metric_consumed_write_capacity_units(
                        period=Duration.minutes(5), statistic="Sum"
                    ),
                ],
                width=12,
            ),
            cloudwatch.GraphWidget(
                title="SQS Queue Health",
                left=[
                    sensitivity_queue.metric_approximate_number_of_messages_visible(
                        period=Duration.minutes(5), statistic="Maximum"
                    ),
                    detect_sensitivity_dlq.metric_approximate_number_of_messages_visible(
                        period=Duration.minutes(5), statistic="Maximum"
                    ),
                    analyze_exposure_dlq.metric_approximate_number_of_messages_visible(
                        period=Duration.minutes(5), statistic="Maximum"
                    ),
                ],
                right=[
                    sensitivity_queue.metric_approximate_age_of_oldest_message(
                        period=Duration.minutes(5), statistic="Maximum"
                    ),
                ],
                width=12,
            ),
            cloudwatch.GraphWidget(
                title="Sensitivity Detection Signals",
                left=[
                    cloudwatch.Metric(
                        namespace="AIReadyGovernance",
                        metric_name="AIReadyGov.PIIDetected",
                        statistic="Sum",
                        period=Duration.minutes(5),
                    ),
                    cloudwatch.Metric(
                        namespace="AIReadyGovernance",
                        metric_name="AIReadyGov.SecretsDetected",
                        statistic="Sum",
                        period=Duration.minutes(5),
                    ),
                    cloudwatch.Metric(
                        namespace="AIReadyGovernance",
                        metric_name="AIReadyGov.ScanSkipped",
                        statistic="Sum",
                        period=Duration.minutes(5),
                    ),
                ],
                right=[
                    cloudwatch.Metric(
                        namespace="AIReadyGovernance",
                        metric_name="AIReadyGov.ScanDurationMs",
                        statistic="p95",
                        period=Duration.minutes(5),
                    ),
                ],
                width=12,
            ),
        )

        # ==========================================
        # Outputs
        # ==========================================
        cdk.CfnOutput(
            self,
            "FindingTableName",
            value=finding_table.table_name,
            export_name="GovFindingTableName",
        )
        cdk.CfnOutput(
            self,
            "FindingTableArn",
            value=finding_table.table_arn,
            export_name="GovFindingTableArn",
        )
        cdk.CfnOutput(
            self,
            "DocumentAnalysisTableName",
            value=document_analysis_table.table_name,
            export_name="GovDocumentAnalysisTableName",
        )
        cdk.CfnOutput(
            self,
            "DocumentAnalysisTableArn",
            value=document_analysis_table.table_arn,
            export_name="GovDocumentAnalysisTableArn",
        )
        cdk.CfnOutput(
            self,
            "PolicyScopeTableName",
            value=policy_scope_table.table_name,
            export_name="GovPolicyScopeTableName",
        )
        cdk.CfnOutput(
            self,
            "ScanJobTableName",
            value=scan_job_table.table_name,
            export_name="GovScanJobTableName",
        )
        cdk.CfnOutput(
            self,
            "AuditLogTableName",
            value=audit_log_table.table_name,
            export_name="GovAuditLogTableName",
        )
        cdk.CfnOutput(
            self,
            "SensitivityQueueUrl",
            value=sensitivity_queue.queue_url,
            export_name="GovSensitivityQueueUrl",
        )
        cdk.CfnOutput(
            self,
            "SensitivityQueueArn",
            value=sensitivity_queue.queue_arn,
            export_name="GovSensitivityQueueArn",
        )
        cdk.CfnOutput(
            self,
            "AnalyzeExposureDlqUrl",
            value=analyze_exposure_dlq.queue_url,
            export_name="GovAnalyzeExposureDlqUrl",
        )
        cdk.CfnOutput(
            self,
            "DetectSensitivityDlqUrl",
            value=detect_sensitivity_dlq.queue_url,
            export_name="GovDetectSensitivityDlqUrl",
        )
        cdk.CfnOutput(
            self,
            "ReportBucketName",
            value=report_bucket.bucket_name,
            export_name="GovReportBucketName",
        )
        cdk.CfnOutput(
            self,
            "VectorsBucketName",
            value=vectors_bucket.bucket_name,
            export_name="GovVectorsBucketName",
        )
        cdk.CfnOutput(
            self,
            "LambdaRoleArn",
            value=lambda_role.role_arn,
            export_name="GovLambdaRoleArn",
        )

        cdk.CfnOutput(
            self,
            "AnalyzeExposureFnArn",
            value=analyze_exposure_fn.function_arn,
            export_name="GovAnalyzeExposureFnArn",
        )

        cdk.CfnOutput(
            self,
            "DetectSensitivityFnArn",
            value=detect_sensitivity_fn.function_arn,
            export_name="GovDetectSensitivityFnArn",
        )

        cdk.CfnOutput(
            self,
            "BatchScoringFnArn",
            value=batch_scoring_fn.function_arn,
            export_name="GovBatchScoringFnArn",
        )
        cdk.CfnOutput(
            self,
            "GovernanceDashboardName",
            value=governance_dashboard.dashboard_name,
            export_name="GovOperationsDashboardName",
        )

        # ==========================================
        # T-026: CloudWatch アラーム設定
        # ==========================================

        # --- analyzeExposure DLQ ≥ 1 → Critical ---
        analyze_exposure_dlq_alarm = cloudwatch.Alarm(
            self,
            "AnalyzeExposureDlqAlarm",
            alarm_name=f"{PROJECT}-analyzeExposure-DLQ-NotEmpty",
            alarm_description="analyzeExposure DLQ にメッセージが滞留 — 処理失敗レコードあり",
            metric=analyze_exposure_dlq.metric_approximate_number_of_messages_visible(
                period=Duration.minutes(1),
                statistic="Maximum",
            ),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )

        # --- detectSensitivity DLQ ≥ 1 → Critical ---
        detect_sensitivity_dlq_alarm = cloudwatch.Alarm(
            self,
            "DetectSensitivityDlqAlarm",
            alarm_name=f"{PROJECT}-detectSensitivity-DLQ-NotEmpty",
            alarm_description="detectSensitivity DLQ にメッセージが滞留 — 処理失敗ファイルあり",
            metric=detect_sensitivity_dlq.metric_approximate_number_of_messages_visible(
                period=Duration.minutes(1),
                statistic="Maximum",
            ),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )

        # --- analyzeExposure エラー率 > 5% → Warning ---
        ae_errors_metric = analyze_exposure_fn.metric_errors(
            period=Duration.minutes(5),
            statistic="Sum",
        )
        ae_invocations_metric = analyze_exposure_fn.metric_invocations(
            period=Duration.minutes(5),
            statistic="Sum",
        )
        ae_error_rate_metric = cloudwatch.MathExpression(
            expression="IF(invocations > 0, errors / invocations * 100, 0)",
            using_metrics={
                "errors": ae_errors_metric,
                "invocations": ae_invocations_metric,
            },
            period=Duration.minutes(5),
            label="analyzeExposure Error Rate (%)",
        )
        ae_error_rate_alarm = cloudwatch.Alarm(
            self,
            "AnalyzeExposureErrorRateAlarm",
            alarm_name=f"{PROJECT}-analyzeExposure-ErrorRate-High",
            alarm_description="analyzeExposure のエラー率が 5% を超過",
            metric=ae_error_rate_metric,
            threshold=5,
            evaluation_periods=3,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )

        # --- detectSensitivity エラー率 > 5% → Warning ---
        ds_errors_metric = detect_sensitivity_fn.metric_errors(
            period=Duration.minutes(5),
            statistic="Sum",
        )
        ds_invocations_metric = detect_sensitivity_fn.metric_invocations(
            period=Duration.minutes(5),
            statistic="Sum",
        )
        ds_error_rate_metric = cloudwatch.MathExpression(
            expression="IF(invocations > 0, errors / invocations * 100, 0)",
            using_metrics={
                "errors": ds_errors_metric,
                "invocations": ds_invocations_metric,
            },
            period=Duration.minutes(5),
            label="detectSensitivity Error Rate (%)",
        )
        ds_error_rate_alarm = cloudwatch.Alarm(
            self,
            "DetectSensitivityErrorRateAlarm",
            alarm_name=f"{PROJECT}-detectSensitivity-ErrorRate-High",
            alarm_description="detectSensitivity のエラー率が 5% を超過",
            metric=ds_error_rate_metric,
            threshold=5,
            evaluation_periods=3,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )

        # --- batchScoring 実行時間 > 14 分 → Warning ---
        bs_duration_alarm = cloudwatch.Alarm(
            self,
            "BatchScoringDurationAlarm",
            alarm_name=f"{PROJECT}-batchScoring-Duration-High",
            alarm_description="batchScoring の実行時間が 14 分を超過 — タイムアウトリスク",
            metric=batch_scoring_fn.metric_duration(
                period=Duration.hours(1),
                statistic="Maximum",
            ),
            threshold=840_000,  # 14 分 = 840,000 ms
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )

        # --- batchScoring エラー回数 >= 1（1h）→ Critical ---
        bs_error_alarm = cloudwatch.Alarm(
            self,
            "BatchScoringErrorAlarm",
            alarm_name=f"{PROJECT}-batchScoring-Errors-Detected",
            alarm_description="batchScoring の実行でエラーを検知（直近1時間で1回以上）",
            metric=batch_scoring_fn.metric_errors(
                period=Duration.hours(1),
                statistic="Sum",
            ),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )

        # 以降のフェーズで参照するためインスタンス変数に保持
        self.finding_table = finding_table
        self.document_analysis_table = document_analysis_table
        self.sensitivity_queue = sensitivity_queue
        self.analyze_exposure_dlq = analyze_exposure_dlq
        self.detect_sensitivity_dlq = detect_sensitivity_dlq
        self.report_bucket = report_bucket
        self.vectors_bucket = vectors_bucket
        self.lambda_role = lambda_role
        self.connect_file_metadata_table = connect_file_metadata_table
        self.connect_raw_bucket = connect_raw_bucket
        self.analyze_exposure_fn = analyze_exposure_fn
        self.detect_sensitivity_fn = detect_sensitivity_fn
        self.batch_scoring_fn = batch_scoring_fn
