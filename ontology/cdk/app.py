#!/usr/bin/env python3
import json
from pathlib import Path

from aws_cdk import App
from aws_cdk import Environment

from stacks.aurora_stack import AuroraStack
from stacks.monitoring_stack import MonitoringStack
from stacks.ontology_stack import OntologyStack


app = App()

env_name = app.node.try_get_context("env") or "dev"
config_path = Path(__file__).parent / "environments.json"
raw_config = json.loads(config_path.read_text(encoding="utf-8"))
env_config = raw_config.get(env_name)
if not env_config:
    raise ValueError(
        f"Environment '{env_name}' is not defined in cdk/environments.json"
    )

tenant_id = env_config["tenantId"]
deployment_env = Environment(
    account=env_config["account"],
    region=env_config["region"],
)

stack_prefix = env_config.get("stackPrefix", "AIReadyOntology")
shared_vpc_id = env_config.get("sharedVpcId")
alert_email = env_config.get("alertEmail")
connect_file_metadata_stream_arn = env_config.get("connectFileMetadataStreamArn")
connect_file_metadata_table_name = env_config.get(
    "connectFileMetadataTableName", f"FileMetadata-{tenant_id}"
)
governance_finding_table_name = env_config.get(
    "governanceFindingTableName", "AIReadyGov-ExposureFinding"
)
document_analysis_table_name = env_config.get(
    "documentAnalysisTableName", "AIReady-DocumentAnalysis"
)

aurora_stack = AuroraStack(
    app,
    f"{stack_prefix}-AuroraStack",
    tenant_id=tenant_id,
    shared_vpc_id=shared_vpc_id,
    env=deployment_env,
)

ontology_stack = OntologyStack(
    app,
    f"{stack_prefix}-CoreStack",
    tenant_id=tenant_id,
    aurora_secret_arn=aurora_stack.aurora_secret.secret_arn,
    aurora_proxy_endpoint=aurora_stack.rds_proxy.endpoint,
    aurora_db_name="ai_ready_ontology",
    aurora_username="ontology_app",
    alert_email=alert_email,
    vpc=aurora_stack.vpc,
    lambda_security_group=aurora_stack.lambda_sg,
    connect_file_metadata_table_name=connect_file_metadata_table_name,
    connect_file_metadata_stream_arn=connect_file_metadata_stream_arn,
    governance_finding_table_name=governance_finding_table_name,
    document_analysis_table_name=document_analysis_table_name,
    env=deployment_env,
)
ontology_stack.add_dependency(aurora_stack)

MonitoringStack(
    app,
    f"{stack_prefix}-MonitoringStack",
    tenant_id=tenant_id,
    env=deployment_env,
)

app.synth()
