from __future__ import annotations

from typing import Optional

from aws_cdk import CfnOutput, Duration, RemovalPolicy, Stack
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_rds as rds
from aws_cdk import aws_secretsmanager as secretsmanager
from constructs import Construct


class AuroraStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        tenant_id: str,
        shared_vpc_id: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.tenant_id = tenant_id
        self.shared_vpc_id = shared_vpc_id

        self.vpc = self._build_vpc()

        self.lambda_sg = ec2.SecurityGroup(
            self,
            "OntologyLambdaSecurityGroup",
            vpc=self.vpc,
            allow_all_outbound=True,
            description="Security group for Ontology Lambda functions",
        )

        self.db_sg = ec2.SecurityGroup(
            self,
            "OntologyAuroraSecurityGroup",
            vpc=self.vpc,
            allow_all_outbound=True,
            description="Security group for Aurora PostgreSQL",
        )
        self.db_sg.add_ingress_rule(
            peer=self.lambda_sg,
            connection=ec2.Port.tcp(5432),
            description="Allow PostgreSQL access from Ontology Lambda SG only",
        )

        self.aurora_secret = secretsmanager.Secret(
            self,
            "AuroraCredentialsSecret",
            secret_name="ai-ready-ontology/aurora-credentials",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template='{"username":"ontology_app"}',
                generate_string_key="password",
                exclude_punctuation=True,
            ),
        )

        self.cluster = rds.DatabaseCluster(
            self,
            "OntologyAuroraCluster",
            engine=rds.DatabaseClusterEngine.aurora_postgres(
                version=rds.AuroraPostgresEngineVersion.VER_15_14
            ),
            writer=rds.ClusterInstance.serverless_v2("writer"),
            readers=[
                rds.ClusterInstance.serverless_v2("reader", scale_with_writer=True)
            ],
            credentials=rds.Credentials.from_secret(self.aurora_secret),
            default_database_name="ai_ready_ontology",
            serverless_v2_min_capacity=0.5,
            serverless_v2_max_capacity=8,
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            ),
            security_groups=[self.db_sg],
            storage_encrypted=True,
            backup=rds.BackupProps(retention=Duration.days(7)),
            deletion_protection=False,
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.rds_proxy = self.cluster.add_proxy(
            "OntologyRdsProxy",
            db_proxy_name=f"ai-ready-ontology-proxy-{self.tenant_id}",
            secrets=[self.aurora_secret],
            vpc=self.vpc,
            iam_auth=False,
            security_groups=[self.db_sg],
            require_tls=True,
            max_connections_percent=100,
            max_idle_connections_percent=50,
        )

        CfnOutput(self, "AuroraClusterArn", value=self.cluster.cluster_arn)
        CfnOutput(self, "AuroraProxyEndpoint", value=self.rds_proxy.endpoint)
        CfnOutput(self, "AuroraSecretArn", value=self.aurora_secret.secret_arn)
        CfnOutput(self, "LambdaSecurityGroupId", value=self.lambda_sg.security_group_id)

    def _build_vpc(self) -> ec2.IVpc:
        existing_vpc_id: Optional[str] = self.shared_vpc_id
        if existing_vpc_id:
            return ec2.Vpc.from_lookup(self, "SharedVpc", vpc_id=existing_vpc_id)

        return ec2.Vpc(
            self,
            "OntologyVpc",
            max_azs=2,
            nat_gateways=1,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24,
                ),
                ec2.SubnetConfiguration(
                    name="private",
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                    cidr_mask=24,
                ),
            ],
        )
