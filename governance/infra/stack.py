"""AI Ready Governance - CDK スタック

Phase 1: インフラ基盤
  T-001: CDK プロジェクト初期化
  T-002: DynamoDB ExposureFinding テーブル + GSI
  T-003: SQS（analyzeExposure DLQ）
  T-005: IAM ロール (Lambda 実行ロール)
  T-006: SSM パラメータ投入

Phase 3: Lambda 1 — analyzeExposure
  T-013: analyzeExposure Lambda + DynamoDB Streams イベントソース

Phase 4: Lambda 2 — detectSensitivity
  T-020: detectSensitivity Docker Lambda + ECR + SQS イベントソース

Phase 5: 監視
  T-026: CloudWatch アラーム（DLQ / analyzeExposure エラー率）
"""

from constructs import Construct
import aws_cdk as cdk
from aws_cdk import (
    Stack,
    RemovalPolicy,
    Duration,
    aws_cloudwatch as cloudwatch,
    aws_dynamodb as dynamodb,
    aws_lambda as _lambda,
    aws_lambda_event_sources as lambda_event_sources,
    aws_s3 as s3,
    aws_sqs as sqs,
    aws_iam as iam,
    aws_ssm as ssm,
)

PROJECT = "AIReadyGov"
CONNECT_PROJECT = "AIReadyConnect"


def _graphsuite_bucket_suffix(stack: Stack) -> str:
    raw = (stack.node.try_get_context("graphsuiteS3BucketSuffix") or "").strip()
    if not raw:
        return ""
    return raw if raw.startswith("-") else f"-{raw}"


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
        policy_scope_table.grant_read_data(lambda_role)

        # DynamoDB: Connect の FileMetadata テーブル (R)
        connect_file_metadata_table.grant_read_write_data(lambda_role)
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

        # Hard-cut: detectSensitivity / Ontology 連携権限は廃止

        # S3: Connect の raw-payload バケット (GetObject)
        connect_raw_bucket.grant_read(lambda_role)

        # Hard-cut: Ontology report 参照は廃止

        # SSM: /aiready/governance/* (Get/Put for runtime config and quota counters)
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "ssm:GetParameter",
                    "ssm:GetParameters",
                    "ssm:GetParametersByPath",
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
        # Keep legacy construct IDs to avoid CloudFormation attempting
        # replacement-create for already managed SSM parameter names.
        ssm_params = [
            ("SSMParam1", "/aiready/governance/max_exposure_score", "10.0", "ExposureScore の上限キャップ"),
            (
                "SSMParam2",
                "/aiready/governance/permissions_count_threshold",
                "50",
                "excessive_permissions 判定の閾値",
            ),
            ("SSMParam3", "/aiready/governance/rescan_interval_days", "7", "機微検知の再スキャン間隔（日）"),
            (
                "SSMParam4",
                "/aiready/governance/max_file_size_bytes",
                "52428800",
                "機微検知のファイルサイズ上限（50MB）",
            ),
            ("SSMParam5", "/aiready/governance/max_text_length", "500000", "テキスト抽出の文字数上限"),
            (
                "SSMParam7",
                "/aiready/connect/sensitivity_label_map",
                "{}",
                "Sensitivity label mapping JSON. ex: {\"Confidential\":\"<label-guid>\"}",
            ),
        ]

        for construct_id, param_name, value, description in ssm_params:
            ssm.StringParameter(
                self,
                construct_id,
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
                "POLICY_SCOPE_TABLE_NAME": policy_scope_table.table_name,
                "RAW_PAYLOAD_BUCKET": connect_raw_bucket.bucket_name,
                "GOVERNANCE_POC_DISABLE_SCOPE_POLICIES": "true",
                "GOVERNANCE_POC_EXPECTATION_GAP_ENABLED": "true",
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

        # Hard-cut: detectSensitivity Lambda は廃止
        # Hard-cut: batchScoring（日次 EventBridge）は廃止 — リアルタイムは analyzeExposure のみ

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
                "GOVERNANCE_POC_DISABLE_SCOPE_POLICIES": "true",
                "GOVERNANCE_POC_EXPECTATION_GAP_ENABLED": "true",
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
                    remediate_finding_fn.metric_errors(period=Duration.minutes(5), statistic="Sum"),
                ],
                right=[
                    analyze_exposure_fn.metric_throttles(period=Duration.minutes(5), statistic="Sum"),
                    remediate_finding_fn.metric_throttles(period=Duration.minutes(5), statistic="Sum"),
                ],
                width=12,
            ),
            cloudwatch.GraphWidget(
                title="Lambda Duration p95",
                left=[
                    analyze_exposure_fn.metric_duration(period=Duration.minutes(5), statistic="p95"),
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
                ],
                width=12,
            ),
            cloudwatch.GraphWidget(
                title="SQS Queue Health",
                left=[
                    analyze_exposure_dlq.metric_approximate_number_of_messages_visible(
                        period=Duration.minutes(5), statistic="Maximum"
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
            "PolicyScopeTableName",
            value=policy_scope_table.table_name,
            export_name="GovPolicyScopeTableName",
        )
        cdk.CfnOutput(
            self,
            "AuditLogTableName",
            value=audit_log_table.table_name,
            export_name="GovAuditLogTableName",
        )
        cdk.CfnOutput(
            self,
            "AnalyzeExposureDlqUrl",
            value=analyze_exposure_dlq.queue_url,
            export_name="GovAnalyzeExposureDlqUrl",
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

        # 以降のフェーズで参照するためインスタンス変数に保持
        self.finding_table = finding_table
        self.analyze_exposure_dlq = analyze_exposure_dlq
        self.lambda_role = lambda_role
        self.connect_file_metadata_table = connect_file_metadata_table
        self.connect_raw_bucket = connect_raw_bucket
        self.analyze_exposure_fn = analyze_exposure_fn
