"""AI Ready Connect PoC — 全リソースを1スタックにまとめる

Phase 1: インフラ
  T-008: VPC + Subnets + IGW + NAT + SG
  T-009: DynamoDB (FileMetadata, IdempotencyKeys, DeltaTokens)
  T-010: SNS + SQS + DLQ
  T-011: ALB + HTTPS Listener + Route 53 A レコード
  T-012: Lambda 実行 IAM Role
  T-013: S3 バケット (Raw Payload)

Phase 2: Lambda
  T-024: receive_notification Lambda + ALB Target Group
  T-026: renew_access_token Lambda + EventBridge rate(30min)
  T-030: pull_file_metadata Lambda + SQS Event Source Mapping
  T-032: renew_subscription Lambda + EventBridge rate(1day)
"""

from pathlib import Path

from constructs import Construct
import aws_cdk as cdk
from aws_cdk import (
    Stack,
    RemovalPolicy,
    Duration,
    aws_ec2 as ec2,
    aws_dynamodb as dynamodb,
    aws_sns as sns,
    aws_sqs as sqs,
    aws_sns_subscriptions as sns_subs,
    aws_s3 as s3,
    aws_iam as iam,
    aws_elasticloadbalancingv2 as elbv2,
    aws_elasticloadbalancingv2_targets as elbv2_targets,
    aws_certificatemanager as acm,
    aws_route53 as route53,
    aws_route53_targets as route53_targets,
    aws_lambda as _lambda,
    aws_lambda_event_sources as lambda_event_sources,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_logs as logs,
)

# ==========================================
# 定数
# ==========================================
PROJECT = "AIReadyConnect"


def _graphsuite_bucket_suffix(stack: Stack) -> str:
    raw = (stack.node.try_get_context("graphsuiteS3BucketSuffix") or "").strip()
    if not raw:
        return ""
    return raw if raw.startswith("-") else f"-{raw}"


DOMAIN_NAME = "graphsuite.jp"
WEBHOOK_DOMAIN = f"webhook.{DOMAIN_NAME}"
HOSTED_ZONE_ID = "Z0824732TT6B0CPHB0FE"
ACM_CERT_ARN = (
    "arn:aws:acm:ap-northeast-1:565699611973:"
    "certificate/709e77a3-98dd-4f8b-bcc9-ea4d490b0a53"
)


class AIReadyConnectStack(Stack):
    """AI Ready Connect PoC 全リソースを含む単一スタック"""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ==========================================
        # Tags
        # ==========================================
        cdk.Tags.of(self).add("Project", PROJECT)
        cdk.Tags.of(self).add("Environment", "poc")

        # ==========================================
        # T-008: VPC + Subnets + NAT + SG
        # ==========================================
        vpc = ec2.Vpc(
            self,
            "Vpc",
            vpc_name=f"{PROJECT}-vpc",
            ip_addresses=ec2.IpAddresses.cidr("10.0.0.0/16"),
            max_azs=2,
            nat_gateways=1,  # PoC: コスト削減のため1つ
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="Public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24,
                ),
                ec2.SubnetConfiguration(
                    name="Private",
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                    cidr_mask=24,
                ),
            ],
        )

        # --- Security Groups ---
        sg_alb = ec2.SecurityGroup(
            self,
            "SgAlb",
            vpc=vpc,
            security_group_name=f"{PROJECT}-sg-alb",
            description="ALB - HTTPS inbound from internet",
            allow_all_outbound=True,
        )
        sg_alb.add_ingress_rule(
            ec2.Peer.any_ipv4(),
            ec2.Port.tcp(443),
            "HTTPS from internet",
        )

        sg_lambda = ec2.SecurityGroup(
            self,
            "SgLambda",
            vpc=vpc,
            security_group_name=f"{PROJECT}-sg-lambda",
            description="Lambda - outbound to internet via NAT",
            allow_all_outbound=True,
        )

        # ==========================================
        # T-009: DynamoDB テーブル
        # ==========================================

        # --- FileMetadata ---
        table_file_metadata = dynamodb.Table(
            self,
            "FileMetadata",
            table_name=f"{PROJECT}-FileMetadata",
            partition_key=dynamodb.Attribute(
                name="drive_id", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="item_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            point_in_time_recovery=True,
            stream=dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,
        )
        # GSI: sharing_scope で検索
        table_file_metadata.add_global_secondary_index(
            index_name="GSI-SharingScope",
            partition_key=dynamodb.Attribute(
                name="sharing_scope", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="modified_at", type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )
        # GSI: tenant_id + modified_at で検索
        table_file_metadata.add_global_secondary_index(
            index_name="GSI-ModifiedAt",
            partition_key=dynamodb.Attribute(
                name="tenant_id", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="modified_at", type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        # --- IdempotencyKeys ---
        table_idempotency = dynamodb.Table(
            self,
            "IdempotencyKeys",
            table_name=f"{PROJECT}-IdempotencyKeys",
            partition_key=dynamodb.Attribute(
                name="event_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            time_to_live_attribute="ttl",
        )

        # --- DeltaTokens ---
        table_delta_tokens = dynamodb.Table(
            self,
            "DeltaTokens",
            table_name=f"{PROJECT}-DeltaTokens",
            partition_key=dynamodb.Attribute(
                name="drive_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )
        # --- MessageMetadata ---
        table_message_metadata = dynamodb.Table(
            self,
            "MessageMetadata",
            table_name=f"{PROJECT}-MessageMetadata",
            partition_key=dynamodb.Attribute(
                name="conversation_key", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="message_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            point_in_time_recovery=True,
        )
        table_message_metadata.add_global_secondary_index(
            index_name="GSI-TenantModifiedAt",
            partition_key=dynamodb.Attribute(
                name="tenant_id", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="modified_at", type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )
        # --- Connections ---
        table_connections = dynamodb.Table(
            self,
            "Connections",
            table_name=f"{PROJECT}-Connections",
            partition_key=dynamodb.Attribute(
                name="tenant_id", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="connection_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )
        table_connections.add_global_secondary_index(
            index_name="GSI-UpdatedAt",
            partition_key=dynamodb.Attribute(
                name="tenant_id", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="updated_at", type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )
        # Webhook が Graph subscriptionId から正しい connection_id を引く（複数接続テナント向け）
        table_connections.add_global_secondary_index(
            index_name="GSI-SubscriptionId",
            partition_key=dynamodb.Attribute(
                name="subscription_id", type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        # ==========================================
        # T-010: SNS + SQS + DLQ
        # ==========================================

        # --- DLQ ---
        dlq = sqs.Queue(
            self,
            "NotificationDLQ",
            queue_name=f"{PROJECT}-NotificationDLQ",
            retention_period=Duration.days(14),
            removal_policy=RemovalPolicy.DESTROY,
        )

        # --- SQS Main Queue ---
        queue = sqs.Queue(
            self,
            "NotificationQueue",
            queue_name=f"{PROJECT}-NotificationQueue",
            visibility_timeout=Duration.seconds(300),
            delivery_delay=Duration.seconds(5),
            retention_period=Duration.days(1),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3,
                queue=dlq,
            ),
            removal_policy=RemovalPolicy.DESTROY,
        )
        message_dlq = sqs.Queue(
            self,
            "MessageNotificationDLQ",
            queue_name=f"{PROJECT}-MessageNotificationDLQ",
            retention_period=Duration.days(14),
            removal_policy=RemovalPolicy.DESTROY,
        )
        message_queue = sqs.Queue(
            self,
            "MessageNotificationQueue",
            queue_name=f"{PROJECT}-MessageNotificationQueue",
            visibility_timeout=Duration.seconds(300),
            delivery_delay=Duration.seconds(5),
            retention_period=Duration.days(1),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3,
                queue=message_dlq,
            ),
            removal_policy=RemovalPolicy.DESTROY,
        )

        # --- SNS Topic ---
        topic = sns.Topic(
            self,
            "NotificationTopic",
            topic_name=f"{PROJECT}-NotificationTopic",
        )
        topic.add_subscription(
            sns_subs.SqsSubscription(
                queue,
                raw_message_delivery=True,
                filter_policy={
                    "resourceType": sns.SubscriptionFilter.string_filter(allowlist=["drive"])
                },
            )
        )
        topic.add_subscription(
            sns_subs.SqsSubscription(
                message_queue,
                raw_message_delivery=True,
                filter_policy={
                    "resourceType": sns.SubscriptionFilter.string_filter(allowlist=["message"])
                },
            )
        )

        # ==========================================
        # T-011: ALB + HTTPS Listener + Route 53
        # ==========================================

        # ACM 証明書の参照
        certificate = acm.Certificate.from_certificate_arn(
            self, "Certificate", ACM_CERT_ARN
        )

        # Route 53 ホストゾーンの参照
        hosted_zone = route53.HostedZone.from_hosted_zone_attributes(
            self,
            "HostedZone",
            hosted_zone_id=HOSTED_ZONE_ID,
            zone_name=DOMAIN_NAME,
        )

        # ALB
        alb = elbv2.ApplicationLoadBalancer(
            self,
            "Alb",
            load_balancer_name=f"{PROJECT}-alb",
            vpc=vpc,
            internet_facing=True,
            security_group=sg_alb,
        )

        # HTTPS Listener (Lambda ターゲットは Phase 2 で追加)
        listener = alb.add_listener(
            "HttpsListener",
            port=443,
            certificates=[certificate],
            default_action=elbv2.ListenerAction.fixed_response(
                status_code=200,
                content_type="text/plain",
                message_body="AI Ready Connect - Webhook Endpoint",
            ),
        )

        # Route 53 A レコード → ALB
        route53.ARecord(
            self,
            "WebhookARecord",
            zone=hosted_zone,
            record_name="webhook",
            target=route53.RecordTarget.from_alias(
                route53_targets.LoadBalancerTarget(alb)
            ),
        )

        # ==========================================
        # T-012: Lambda 実行 IAM Role
        # ==========================================
        lambda_role = iam.Role(
            self,
            "LambdaRole",
            role_name=f"{PROJECT}-LambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaVPCAccessExecutionRole"
                ),
            ],
        )

        # CloudWatch Logs
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                resources=["arn:aws:logs:*:*:*"],
            )
        )

        # Connect API ロールへ CloudWatch Logs Insights 実行権限を付与（T-065）
        # cdk context 例:
        # {
        #   "connect_api_role_arn": "arn:aws:iam::<account-id>:role/<api-role-name>"
        # }
        connect_api_role_arn = self.node.try_get_context("connect_api_role_arn")
        if connect_api_role_arn:
            connect_api_role = iam.Role.from_role_arn(
                self,
                "ConnectApiRole",
                role_arn=str(connect_api_role_arn),
                mutable=True,
            )
            connect_log_group_arns = [
                f"arn:aws:logs:{self.region}:{self.account}:log-group:/aws/lambda/{PROJECT}-pullFileMetadata:*",
                f"arn:aws:logs:{self.region}:{self.account}:log-group:/aws/lambda/{PROJECT}-receiveNotification:*",
                f"arn:aws:logs:{self.region}:{self.account}:log-group:/aws/lambda/{PROJECT}-renewSubscription:*",
                f"arn:aws:logs:{self.region}:{self.account}:log-group:/aws/lambda/{PROJECT}-renewAccessToken:*",
                f"arn:aws:logs:{self.region}:{self.account}:log-group:/aws/lambda/{PROJECT}-cleanupConnectionArtifacts:*",
            ]
            connect_api_role.add_to_principal_policy(
                iam.PolicyStatement(
                    sid="AllowConnectLogsInsightsStartQuery",
                    effect=iam.Effect.ALLOW,
                    actions=["logs:StartQuery"],
                    resources=connect_log_group_arns,
                )
            )
            # queryId ベース API はリソースを限定できないため "*" が必要。
            connect_api_role.add_to_principal_policy(
                iam.PolicyStatement(
                    sid="AllowConnectLogsInsightsQueryLifecycle",
                    effect=iam.Effect.ALLOW,
                    actions=["logs:GetQueryResults", "logs:StopQuery"],
                    resources=["*"],
                )
            )

        # SSM Parameter Store (read/write)
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "ssm:GetParameter",
                    "ssm:GetParameters",
                    "ssm:PutParameter",
                ],
                resources=[
                    f"arn:aws:ssm:{self.region}:{self.account}:parameter/MSGraph*",
                    f"arn:aws:ssm:{self.region}:{self.account}:parameter/{PROJECT}/*",
                    f"arn:aws:ssm:{self.region}:{self.account}:parameter/aiready/connect/*",
                ],
            )
        )

        # DynamoDB
        for table in [table_file_metadata, table_idempotency, table_delta_tokens, table_message_metadata, table_connections]:
            table.grant_read_write_data(lambda_role)

        # SNS Publish
        topic.grant_publish(lambda_role)

        # SQS Consume
        queue.grant_consume_messages(lambda_role)
        message_queue.grant_consume_messages(lambda_role)

        # ==========================================
        # T-013: S3 バケット (Raw Payload)
        # ==========================================
        _suffix = _graphsuite_bucket_suffix(self)
        raw_bucket = s3.Bucket(
            self,
            "RawPayloadBucket",
            bucket_name=f"{PROJECT.lower()}-raw-payload{_suffix}",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            lifecycle_rules=[
                s3.LifecycleRule(
                    expiration=Duration.days(90),
                    id="expire-raw-90d",
                ),
            ],
        )
        raw_bucket.grant_put(lambda_role)

        # ==========================================
        # Phase 2: Lambda 共通設定
        # ==========================================

        # Lambda コードのルートディレクトリ（infra/ の1つ上 = connect/）
        code_root = str(Path(__file__).resolve().parent.parent)

        # 共通環境変数
        common_env = {
            "TENANT_ID": "default",
            "FILE_METADATA_TABLE": table_file_metadata.table_name,
            "IDEMPOTENCY_TABLE": table_idempotency.table_name,
            "DELTA_TOKENS_TABLE": table_delta_tokens.table_name,
            "MESSAGE_METADATA_TABLE": table_message_metadata.table_name,
            "NOTIFICATION_TOPIC_ARN": topic.topic_arn,
            "RAW_BUCKET": raw_bucket.bucket_name,
            "WEBHOOK_URL": f"https://{WEBHOOK_DOMAIN}",
            "LOG_LEVEL": "INFO",
            "CONNECT_CONNECTIONS_TABLE": table_connections.table_name,
        }

        # 共通 Lambda Layer (requests + python-dotenv)
        deps_layer = _lambda.LayerVersion(
            self,
            "DepsLayer",
            layer_version_name=f"{PROJECT}-deps",
            code=_lambda.Code.from_asset(
                str(Path(__file__).resolve().parent / "layers" / "deps"),
            ),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_12],
            description="requests, python-dotenv",
        )

        # ==========================================
        # T-024: receive_notification Lambda + ALB TG
        # ==========================================
        fn_receive = _lambda.Function(
            self,
            "ReceiveNotification",
            function_name=f"{PROJECT}-receiveNotification",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="src.handlers.receive_notification.lambda_handler",
            code=_lambda.Code.from_asset(
                code_root,
                exclude=[
                    "infra/*", "tests/*", "scripts/*", "Docs/*",
                    ".env*", "*.md", "__pycache__", ".git*",
                    "cdk.out/*", ".venv/*",
                ],
            ),
            layers=[deps_layer],
            role=lambda_role,
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
            ),
            security_groups=[sg_lambda],
            timeout=Duration.seconds(30),
            memory_size=256,
            environment=common_env,
            log_retention=logs.RetentionDays.TWO_WEEKS,
        )

        # ALB Target Group に Lambda を登録
        tg = elbv2.ApplicationTargetGroup(
            self,
            "WebhookTargetGroup",
            target_group_name=f"{PROJECT}-webhook-tg",
            targets=[elbv2_targets.LambdaTarget(fn_receive)],
            health_check=elbv2.HealthCheck(
                enabled=True,
                path="/",
                healthy_http_codes="200",
            ),
        )

        # リスナーのデフォルトアクションを Lambda TG に変更
        listener.add_action(
            "WebhookAction",
            priority=1,
            conditions=[elbv2.ListenerCondition.path_patterns(["/*"])],
            action=elbv2.ListenerAction.forward([tg]),
        )

        # ==========================================
        # T-026: renew_access_token Lambda + EventBridge
        # ==========================================
        fn_renew_token = _lambda.Function(
            self,
            "RenewAccessToken",
            function_name=f"{PROJECT}-renewAccessToken",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="src.handlers.renew_access_token.lambda_handler",
            code=_lambda.Code.from_asset(
                code_root,
                exclude=[
                    "infra/*", "tests/*", "scripts/*", "Docs/*",
                    ".env*", "*.md", "__pycache__", ".git*",
                    "cdk.out/*", ".venv/*",
                ],
            ),
            layers=[deps_layer],
            role=lambda_role,
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
            ),
            security_groups=[sg_lambda],
            timeout=Duration.seconds(60),
            memory_size=256,
            environment=common_env,
            log_retention=logs.RetentionDays.TWO_WEEKS,
        )

        # EventBridge: 30分ごとに実行
        events.Rule(
            self,
            "RenewTokenSchedule",
            rule_name=f"{PROJECT}-renewTokenSchedule",
            schedule=events.Schedule.rate(Duration.minutes(30)),
            targets=[events_targets.LambdaFunction(fn_renew_token)],
        )

        # ==========================================
        # T-030: pull_file_metadata Lambda + SQS
        # ==========================================
        fn_pull_metadata = _lambda.Function(
            self,
            "PullFileMetadata",
            function_name=f"{PROJECT}-pullFileMetadata",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="src.handlers.pull_file_metadata.lambda_handler",
            code=_lambda.Code.from_asset(
                code_root,
                exclude=[
                    "infra/*", "tests/*", "scripts/*", "Docs/*",
                    ".env*", "*.md", "__pycache__", ".git*",
                    "cdk.out/*", ".venv/*",
                ],
            ),
            layers=[deps_layer],
            role=lambda_role,
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
            ),
            security_groups=[sg_lambda],
            timeout=Duration.seconds(300),
            memory_size=512,
            environment=common_env,
            log_retention=logs.RetentionDays.TWO_WEEKS,
        )

        # SQS Event Source Mapping
        fn_pull_metadata.add_event_source(
            lambda_event_sources.SqsEventSource(
                queue,
                batch_size=10,
                max_batching_window=Duration.seconds(30),
                report_batch_item_failures=True,
            )
        )
        fn_pull_message_metadata = _lambda.Function(
            self,
            "PullMessageMetadata",
            function_name=f"{PROJECT}-pullMessageMetadata",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="src.handlers.pull_message_metadata.lambda_handler",
            code=_lambda.Code.from_asset(
                code_root,
                exclude=[
                    "infra/*", "tests/*", "scripts/*", "Docs/*",
                    ".env*", "*.md", "__pycache__", ".git*",
                    "cdk.out/*", ".venv/*",
                ],
            ),
            layers=[deps_layer],
            role=lambda_role,
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
            ),
            security_groups=[sg_lambda],
            timeout=Duration.seconds(300),
            memory_size=512,
            environment=common_env,
            log_retention=logs.RetentionDays.TWO_WEEKS,
        )
        fn_pull_message_metadata.add_event_source(
            lambda_event_sources.SqsEventSource(
                message_queue,
                batch_size=10,
                max_batching_window=Duration.seconds(30),
                report_batch_item_failures=True,
            )
        )

        # ==========================================
        # T-032: renew_subscription Lambda + EventBridge
        # ==========================================
        fn_renew_sub = _lambda.Function(
            self,
            "RenewSubscription",
            function_name=f"{PROJECT}-renewSubscription",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="src.handlers.renew_subscription.lambda_handler",
            code=_lambda.Code.from_asset(
                code_root,
                exclude=[
                    "infra/*", "tests/*", "scripts/*", "Docs/*",
                    ".env*", "*.md", "__pycache__", ".git*",
                    "cdk.out/*", ".venv/*",
                ],
            ),
            layers=[deps_layer],
            role=lambda_role,
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
            ),
            security_groups=[sg_lambda],
            timeout=Duration.seconds(60),
            memory_size=256,
            environment=common_env,
            log_retention=logs.RetentionDays.TWO_WEEKS,
        )

        # EventBridge: 1日ごとに実行
        events.Rule(
            self,
            "RenewSubscriptionSchedule",
            rule_name=f"{PROJECT}-renewSubSchedule",
            schedule=events.Schedule.rate(Duration.days(1)),
            targets=[events_targets.LambdaFunction(fn_renew_sub)],
        )

        # ==========================================
        # T-083: init_subscription Lambda
        # ==========================================
        fn_init_subscription = _lambda.Function(
            self,
            "InitSubscription",
            function_name=f"{PROJECT}-initSubscription",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="src.handlers.init_subscription.lambda_handler",
            code=_lambda.Code.from_asset(
                code_root,
                exclude=[
                    "infra/*", "tests/*", "scripts/*", "Docs/*",
                    ".env*", "*.md", "__pycache__", ".git*",
                    "cdk.out/*", ".venv/*",
                ],
            ),
            layers=[deps_layer],
            role=lambda_role,
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
            ),
            security_groups=[sg_lambda],
            timeout=Duration.seconds(120),
            memory_size=256,
            environment=common_env,
            log_retention=logs.RetentionDays.TWO_WEEKS,
        )

        backfill_chat_env = {
            **common_env,
            "CONNECT_CHAT_BACKFILL_MAX_MESSAGES": "500",
        }
        fn_backfill_chat_messages = _lambda.Function(
            self,
            "BackfillChatMessages",
            function_name=f"{PROJECT}-backfillChatMessages",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="src.handlers.backfill_chat_messages.lambda_handler",
            code=_lambda.Code.from_asset(
                code_root,
                exclude=[
                    "infra/*", "tests/*", "scripts/*", "Docs/*",
                    ".env*", "*.md", "__pycache__", ".git*",
                    "cdk.out/*", ".venv/*",
                ],
            ),
            layers=[deps_layer],
            role=lambda_role,
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
            ),
            security_groups=[sg_lambda],
            timeout=Duration.seconds(900),
            memory_size=512,
            environment=backfill_chat_env,
            log_retention=logs.RetentionDays.TWO_WEEKS,
        )

        # ==========================================
        # T-090: cleanup_connection_artifacts Lambda
        # ==========================================
        fn_cleanup_connection_artifacts = _lambda.Function(
            self,
            "CleanupConnectionArtifacts",
            function_name=f"{PROJECT}-cleanupConnectionArtifacts",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="src.handlers.cleanup_connection_artifacts.lambda_handler",
            code=_lambda.Code.from_asset(
                code_root,
                exclude=[
                    "infra/*", "tests/*", "scripts/*", "Docs/*",
                    ".env*", "*.md", "__pycache__", ".git*",
                    "cdk.out/*", ".venv/*",
                ],
            ),
            layers=[deps_layer],
            role=lambda_role,
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
            ),
            security_groups=[sg_lambda],
            timeout=Duration.seconds(180),
            memory_size=512,
            environment=common_env,
            log_retention=logs.RetentionDays.TWO_WEEKS,
        )

        # ==========================================
        # Outputs
        # ==========================================
        cdk.CfnOutput(self, "VpcId", value=vpc.vpc_id)
        cdk.CfnOutput(self, "AlbDnsName", value=alb.load_balancer_dns_name)
        cdk.CfnOutput(self, "WebhookUrl", value=f"https://{WEBHOOK_DOMAIN}")
        cdk.CfnOutput(self, "TopicArn", value=topic.topic_arn)
        cdk.CfnOutput(self, "QueueUrl", value=queue.queue_url)
        cdk.CfnOutput(self, "DlqUrl", value=dlq.queue_url)
        cdk.CfnOutput(self, "MessageQueueUrl", value=message_queue.queue_url)
        cdk.CfnOutput(self, "MessageDlqUrl", value=message_dlq.queue_url)
        cdk.CfnOutput(
            self, "FileMetadataTable", value=table_file_metadata.table_name
        )
        cdk.CfnOutput(
            self,
            "FileMetadataStreamArn",
            value=table_file_metadata.table_stream_arn,
            export_name="ConnectFileMetadataStreamArn",
            description="FileMetadata DynamoDB Streams ARN — Governance スタックが参照",
        )
        cdk.CfnOutput(
            self, "IdempotencyTable", value=table_idempotency.table_name
        )
        cdk.CfnOutput(
            self, "DeltaTokensTable", value=table_delta_tokens.table_name
        )
        cdk.CfnOutput(
            self, "MessageMetadataTable", value=table_message_metadata.table_name
        )
        cdk.CfnOutput(self, "RawBucketName", value=raw_bucket.bucket_name)
        cdk.CfnOutput(self, "LambdaRoleArn", value=lambda_role.role_arn)
        cdk.CfnOutput(self, "ListenerArn", value=listener.listener_arn)
        cdk.CfnOutput(
            self, "ReceiveNotificationFn",
            value=fn_receive.function_name,
        )
        cdk.CfnOutput(
            self, "RenewAccessTokenFn",
            value=fn_renew_token.function_name,
        )
        cdk.CfnOutput(
            self, "PullFileMetadataFn",
            value=fn_pull_metadata.function_name,
        )
        cdk.CfnOutput(
            self, "PullMessageMetadataFn",
            value=fn_pull_message_metadata.function_name,
        )
        cdk.CfnOutput(
            self, "RenewSubscriptionFn",
            value=fn_renew_sub.function_name,
        )
        cdk.CfnOutput(
            self, "ConnectConnectionsTable", value=table_connections.table_name
        )
        cdk.CfnOutput(
            self, "InitSubscriptionFn",
            value=fn_init_subscription.function_name,
            export_name="ConnectInitSubscriptionLambdaName",
        )
        cdk.CfnOutput(
            self, "BackfillChatMessagesFn",
            value=fn_backfill_chat_messages.function_name,
            export_name="ConnectBackfillChatMessagesLambdaName",
        )
        cdk.CfnOutput(
            self, "CleanupConnectionArtifactsFn",
            value=fn_cleanup_connection_artifacts.function_name,
            export_name="ConnectCleanupConnectionArtifactsLambdaName",
        )

        # Phase 2 で参照するためインスタンス変数に保持
        self.vpc = vpc
        self.sg_alb = sg_alb
        self.sg_lambda = sg_lambda
        self.table_file_metadata = table_file_metadata
        self.table_idempotency = table_idempotency
        self.table_delta_tokens = table_delta_tokens
        self.table_message_metadata = table_message_metadata
        self.table_connections = table_connections
        self.topic = topic
        self.queue = queue
        self.message_queue = message_queue
        self.dlq = dlq
        self.message_dlq = message_dlq
        self.alb = alb
        self.listener = listener
        self.lambda_role = lambda_role
        self.raw_bucket = raw_bucket
        self.certificate = certificate
        self.hosted_zone = hosted_zone
