from __future__ import annotations

from pathlib import Path
from typing import Optional

from aws_cdk import Duration, Stack
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as sns_subscriptions
from aws_cdk import aws_sqs as sqs
from constructs import Construct


class OntologyStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        tenant_id: str,
        aurora_secret_arn: str,
        aurora_proxy_endpoint: str,
        aurora_db_name: str = "ai_ready_ontology",
        aurora_username: str = "ontology_app",
        alert_email: str | None = None,
        vpc: Optional[ec2.IVpc] = None,
        lambda_security_group: Optional[ec2.ISecurityGroup] = None,
        connect_file_metadata_table_name: str | None = None,
        connect_file_metadata_stream_arn: str | None = None,
        governance_finding_table_name: str = "AIReadyGov-ExposureFinding",
        document_analysis_table_name: str = "AIReady-DocumentAnalysis",
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.tenant_id = tenant_id
        self.vpc = vpc
        self.lambda_security_group = lambda_security_group
        self.connect_file_metadata_table_name = (
            connect_file_metadata_table_name or f"FileMetadata-{tenant_id}"
        )
        self.connect_file_metadata_stream_arn = connect_file_metadata_stream_arn
        self.governance_finding_table_name = governance_finding_table_name
        self.document_analysis_table_name = document_analysis_table_name
        self.aurora_secret_arn = aurora_secret_arn
        self.aurora_proxy_endpoint = aurora_proxy_endpoint
        self.aurora_db_name = aurora_db_name
        self.aurora_username = aurora_username

        unified_metadata_table = dynamodb.Table(
            self,
            "UnifiedMetadataTable",
            table_name="AIReadyOntology-UnifiedMetadata",
            partition_key=dynamodb.Attribute(
                name="tenant_id", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="item_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=True
            ),
            time_to_live_attribute="ttl",
        )
        unified_metadata_table.add_global_secondary_index(
            index_name="GSI-RiskLevel",
            partition_key=dynamodb.Attribute(
                name="risk_level", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="last_modified", type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )
        unified_metadata_table.add_global_secondary_index(
            index_name="GSI-Source",
            partition_key=dynamodb.Attribute(
                name="source", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="transformed_at", type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )
        unified_metadata_table.add_global_secondary_index(
            index_name="GSI-FreshnessStatus",
            partition_key=dynamodb.Attribute(
                name="freshness_status", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="last_modified", type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        lineage_event_table = dynamodb.Table(
            self,
            "LineageEventTable",
            table_name="AIReadyOntology-LineageEvent",
            partition_key=dynamodb.Attribute(
                name="tenant_id", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="lineage_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=True
            ),
            time_to_live_attribute="ttl",
        )
        lineage_event_table.add_global_secondary_index(
            index_name="GSI-JobName",
            partition_key=dynamodb.Attribute(
                name="job_name", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="event_time", type=dynamodb.AttributeType.STRING
            ),
        )
        lineage_event_table.add_global_secondary_index(
            index_name="GSI-Status",
            partition_key=dynamodb.Attribute(
                name="status", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="event_time", type=dynamodb.AttributeType.STRING
            ),
        )

        dynamodb.Table(
            self,
            "EntityCandidateTable",
            table_name="AIReadyOntology-EntityCandidate",
            partition_key=dynamodb.Attribute(
                name="tenant_id", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="candidate_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            time_to_live_attribute="ttl",
        )

        entity_resolution_dlq = sqs.Queue(
            self,
            "EntityResolutionDLQ",
            queue_name="AIReadyOntology-EntityResolution-DLQ.fifo",
            fifo=True,
            retention_period=Duration.days(14),
            content_based_deduplication=False,
        )

        entity_resolution_queue = sqs.Queue(
            self,
            "EntityResolutionQueue",
            queue_name="AIReadyOntology-EntityResolutionQueue.fifo",
            fifo=True,
            visibility_timeout=Duration.seconds(180),
            content_based_deduplication=False,
            dead_letter_queue=sqs.DeadLetterQueue(
                queue=entity_resolution_dlq,
                max_receive_count=3,
            ),
        )

        report_bucket = s3.Bucket(
            self,
            "OntologyReportBucket",
            bucket_name=f"aiready-ontology-reports-{self.tenant_id}".lower(),
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            versioned=False,
            lifecycle_rules=[
                s3.LifecycleRule(
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.GLACIER,
                            transition_after=Duration.days(90),
                        )
                    ],
                    expiration=Duration.days(365),
                )
            ],
        )

        alert_topic = sns.Topic(
            self,
            "OntologyAlertTopic",
            topic_name="AIReadyOntology-Alerts",
            display_name="AI Ready Ontology Alerts",
        )
        if alert_email:
            alert_topic.add_subscription(
                sns_subscriptions.EmailSubscription(alert_email)
            )

        lambda_roles = self._create_lambda_roles(
            unified_metadata_table=unified_metadata_table,
            lineage_event_table=lineage_event_table,
            report_bucket=report_bucket,
            entity_resolution_queue=entity_resolution_queue,
            alert_topic=alert_topic,
            aurora_secret_arn=aurora_secret_arn,
        )
        self._create_phase3_lambdas(
            unified_metadata_table=unified_metadata_table,
            lineage_event_table=lineage_event_table,
            schema_transform_role=lambda_roles["schema_transform"],
            lineage_recorder_role=lambda_roles["lineage_recorder"],
        )
        self._create_phase5_entity_resolver_lambda(
            entity_resolution_queue=entity_resolution_queue,
            entity_resolver_role=lambda_roles["entity_resolver"],
            alert_topic=alert_topic,
        )

    def _create_lambda_roles(
        self,
        *,
        unified_metadata_table: dynamodb.Table,
        lineage_event_table: dynamodb.Table,
        report_bucket: s3.Bucket,
        entity_resolution_queue: sqs.Queue,
        alert_topic: sns.Topic,
        aurora_secret_arn: str,
    ) -> dict[str, iam.Role]:
        schema_transform_role = self._base_lambda_role("SchemaTransformRole")
        lineage_recorder_role = self._base_lambda_role("LineageRecorderRole")
        entity_resolver_role = self._base_lambda_role("EntityResolverRole")
        batch_reconciler_role = self._base_lambda_role("BatchReconcilerRole")

        unified_metadata_table.grant_read_write_data(schema_transform_role)
        schema_transform_role.add_to_policy(
            iam.PolicyStatement(
                actions=["dynamodb:GetItem", "dynamodb:Query", "dynamodb:Scan"],
                resources=[
                    f"arn:aws:dynamodb:{self.region}:{self.account}:table/{self.governance_finding_table_name}",
                    f"arn:aws:dynamodb:{self.region}:{self.account}:table/{self.document_analysis_table_name}",
                    f"arn:aws:dynamodb:{self.region}:{self.account}:table/FileMetadata-*",
                    f"arn:aws:dynamodb:{self.region}:{self.account}:table/{self.connect_file_metadata_table_name}",
                ],
            )
        )
        schema_transform_role.add_to_policy(
            iam.PolicyStatement(
                actions=["lambda:InvokeFunction"],
                resources=[
                    f"arn:aws:lambda:{self.region}:{self.account}:function:AIReadyOntology-lineageRecorder"
                ],
            )
        )
        if self.connect_file_metadata_stream_arn:
            schema_transform_role.add_to_policy(
                iam.PolicyStatement(
                    actions=[
                        "dynamodb:DescribeStream",
                        "dynamodb:GetRecords",
                        "dynamodb:GetShardIterator",
                        "dynamodb:ListStreams",
                    ],
                    resources=[self.connect_file_metadata_stream_arn],
                )
            )

        lineage_event_table.grant_read_write_data(lineage_recorder_role)

        entity_resolution_queue.grant_consume_messages(entity_resolver_role)
        entity_resolver_role.add_to_policy(
            iam.PolicyStatement(
                actions=["dynamodb:GetItem", "dynamodb:Query", "dynamodb:Scan"],
                resources=[
                    f"arn:aws:dynamodb:{self.region}:{self.account}:table/{self.document_analysis_table_name}"
                ],
            )
        )
        entity_resolver_role.add_to_policy(
            iam.PolicyStatement(
                actions=["secretsmanager:GetSecretValue"],
                resources=[aurora_secret_arn],
            )
        )
        alert_topic.grant_publish(entity_resolver_role)
        entity_resolver_role.add_to_policy(
            iam.PolicyStatement(
                actions=["lambda:InvokeFunction"],
                resources=[
                    f"arn:aws:lambda:{self.region}:{self.account}:function:AIReadyOntology-lineageRecorder"
                ],
            )
        )

        unified_metadata_table.grant_read_write_data(batch_reconciler_role)
        report_bucket.grant_put(batch_reconciler_role)
        batch_reconciler_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "dynamodb:GetItem",
                    "dynamodb:Query",
                    "dynamodb:Scan",
                ],
                resources=[
                    f"arn:aws:dynamodb:{self.region}:{self.account}:table/{self.document_analysis_table_name}",
                    f"arn:aws:dynamodb:{self.region}:{self.account}:table/FileMetadata-*",
                    f"arn:aws:dynamodb:{self.region}:{self.account}:table/{self.connect_file_metadata_table_name}",
                ],
            )
        )
        batch_reconciler_role.add_to_policy(
            iam.PolicyStatement(
                actions=["secretsmanager:GetSecretValue"],
                resources=[aurora_secret_arn],
            )
        )
        batch_reconciler_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "states:StartExecution",
                    "states:DescribeExecution",
                    "states:StopExecution",
                ],
                resources=[
                    f"arn:aws:states:{self.region}:{self.account}:stateMachine:*"
                ],
            )
        )

        return {
            "schema_transform": schema_transform_role,
            "lineage_recorder": lineage_recorder_role,
            "entity_resolver": entity_resolver_role,
            "batch_reconciler": batch_reconciler_role,
        }

    def _create_phase3_lambdas(
        self,
        *,
        unified_metadata_table: dynamodb.Table,
        lineage_event_table: dynamodb.Table,
        schema_transform_role: iam.Role,
        lineage_recorder_role: iam.Role,
    ) -> None:
        if not self.vpc:
            raise ValueError("VPC is required to create Ontology Lambda functions.")
        if not self.lambda_security_group:
            raise ValueError(
                "lambda_security_group is required to create Ontology Lambda functions."
            )

        code_asset_path = str(Path(__file__).resolve().parents[2])
        lambda_code = lambda_.Code.from_asset(
            code_asset_path,
            exclude=[
                "cdk.out",
                "cdk.out/**",
                "cdk.out*",
                "cdk.out*/**",
                "cdk.out.t022",
                "cdk.out.t022/**",
                ".git",
                ".git/**",
                ".pytest_cache",
                ".pytest_cache/**",
                "**/__pycache__",
                "**/__pycache__/**",
            ],
        )

        lineage_recorder = lambda_.Function(
            self,
            "LineageRecorderLambda",
            function_name="AIReadyOntology-lineageRecorder",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="src.handlers.lineage_recorder.handler",
            code=lambda_code,
            memory_size=256,
            timeout=Duration.seconds(30),
            role=lineage_recorder_role,
            vpc=self.vpc,
            security_groups=[self.lambda_security_group],
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            ),
            environment={
                "LINEAGE_EVENT_TABLE": lineage_event_table.table_name,
                "TENANT_ID": self.tenant_id,
            },
        )

        schema_transform = lambda_.Function(
            self,
            "SchemaTransformLambda",
            function_name="AIReadyOntology-schemaTransform",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="src.handlers.schema_transform.handler",
            code=lambda_code,
            memory_size=512,
            timeout=Duration.seconds(120),
            role=schema_transform_role,
            reserved_concurrent_executions=10,
            vpc=self.vpc,
            security_groups=[self.lambda_security_group],
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            ),
            environment={
                "UNIFIED_METADATA_TABLE": unified_metadata_table.table_name,
                "GOVERNANCE_FINDING_TABLE": self.governance_finding_table_name,
                "DOCUMENT_ANALYSIS_TABLE": self.document_analysis_table_name,
                "LINEAGE_FUNCTION_NAME": lineage_recorder.function_name,
                "TENANT_ID": self.tenant_id,
            },
        )

        if self.connect_file_metadata_stream_arn:
            lambda_.CfnEventSourceMapping(
                self,
                "SchemaTransformFileMetadataStreamMapping",
                function_name=schema_transform.function_name,
                event_source_arn=self.connect_file_metadata_stream_arn,
                starting_position="LATEST",
                batch_size=10,
                maximum_batching_window_in_seconds=5,
                enabled=True,
            )

    def _create_phase5_entity_resolver_lambda(
        self,
        *,
        entity_resolution_queue: sqs.Queue,
        entity_resolver_role: iam.Role,
        alert_topic: sns.Topic,
    ) -> None:
        if not self.vpc:
            raise ValueError("VPC is required to create Ontology Lambda functions.")
        if not self.lambda_security_group:
            raise ValueError(
                "lambda_security_group is required to create Ontology Lambda functions."
            )

        code_asset_path = str(Path(__file__).resolve().parents[2])
        lambda_code = lambda_.Code.from_asset(
            code_asset_path,
            exclude=[
                "cdk.out",
                "cdk.out/**",
                "cdk.out*",
                "cdk.out*/**",
                "cdk.out.t022",
                "cdk.out.t022/**",
                ".git",
                ".git/**",
                ".pytest_cache",
                ".pytest_cache/**",
                "**/__pycache__",
                "**/__pycache__/**",
            ],
        )

        common_layer = lambda_.LayerVersion(
            self,
            "CommonLayer",
            layer_version_name="AIReadyOntology-common-layer",
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
            code=lambda_.Code.from_asset(
                str(Path(__file__).resolve().parents[2] / "layers" / "common-layer" / "build")
            ),
            description="Common dependencies layer (openlineage, utils)",
        )
        aurora_layer = lambda_.LayerVersion(
            self,
            "AuroraLayer",
            layer_version_name="AIReadyOntology-aurora-layer",
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
            code=lambda_.Code.from_asset(
                str(Path(__file__).resolve().parents[2] / "layers" / "aurora-layer" / "build")
            ),
            description="Aurora dependencies layer (psycopg2)",
        )

        entity_resolver = lambda_.Function(
            self,
            "EntityResolverLambda",
            function_name="AIReadyOntology-entityResolver",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="src.handlers.entity_resolver.handler",
            code=lambda_code,
            memory_size=512,
            timeout=Duration.seconds(120),
            role=entity_resolver_role,
            reserved_concurrent_executions=5,
            vpc=self.vpc,
            security_groups=[self.lambda_security_group],
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            ),
            layers=[common_layer, aurora_layer],
            environment={
                "AURORA_PROXY_ENDPOINT": self.aurora_proxy_endpoint,
                "AURORA_PORT": "5432",
                "AURORA_DB_NAME": self.aurora_db_name,
                "AURORA_USERNAME": self.aurora_username,
                "AURORA_SECRET_ARN": self.aurora_secret_arn,
                "PII_ENCRYPTION_KEY_PARAM": "/ai-ready/ontology/{tenant_id}/pii-encryption-key",
                "ALERT_TOPIC_ARN": alert_topic.topic_arn,
                "LINEAGE_FUNCTION_NAME": "AIReadyOntology-lineageRecorder",
                "DOCUMENT_ANALYSIS_TABLE": self.document_analysis_table_name,
                "TENANT_ID": self.tenant_id,
            },
        )

        lambda_.CfnEventSourceMapping(
            self,
            "EntityResolverSqsMapping",
            function_name=entity_resolver.function_name,
            event_source_arn=entity_resolution_queue.queue_arn,
            batch_size=1,
            enabled=True,
        )

    def _base_lambda_role(self, role_id: str) -> iam.Role:
        role = iam.Role(
            self,
            role_id,
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
        role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "ssm:GetParameter",
                    "ssm:GetParameters",
                    "ssm:GetParametersByPath",
                ],
                resources=[
                    f"arn:aws:ssm:{self.region}:{self.account}:parameter/ai-ready/ontology/*"
                ],
            )
        )
        role.add_to_policy(
            iam.PolicyStatement(
                actions=["cloudwatch:PutMetricData"],
                resources=["*"],
                conditions={
                    "StringEquals": {"cloudwatch:namespace": "AIReadyOntology"}
                },
            )
        )
        return role
