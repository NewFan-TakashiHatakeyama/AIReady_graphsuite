#!/usr/bin/env python3
import json
from pathlib import Path

from aws_cdk import App
from aws_cdk import Environment

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
    "documentAnalysisTableName", "AIReadyGov-DocumentAnalysis"
)

ontology_stack = OntologyStack(
    app,
    f"{stack_prefix}-CoreStack",
    tenant_id=tenant_id,
    alert_email=alert_email,
    shared_vpc_id=shared_vpc_id,
    connect_file_metadata_table_name=connect_file_metadata_table_name,
    connect_file_metadata_stream_arn=connect_file_metadata_stream_arn,
    governance_finding_table_name=governance_finding_table_name,
    document_analysis_table_name=document_analysis_table_name,
    env=deployment_env,
)

MonitoringStack(
    app,
    f"{stack_prefix}-MonitoringStack",
    tenant_id=tenant_id,
    state_machine_arn=None,
    alert_topic_arn=ontology_stack.alert_topic_arn,
    env=deployment_env,
)

app.synth()
