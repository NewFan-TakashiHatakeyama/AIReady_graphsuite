from aws_cdk import Stack
from constructs import Construct


class MonitoringStack(Stack):
    """Phase 1 template stack. CloudWatch alarms are added in later phases."""

    def __init__(
        self, scope: Construct, construct_id: str, tenant_id: str, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.tenant_id = tenant_id
