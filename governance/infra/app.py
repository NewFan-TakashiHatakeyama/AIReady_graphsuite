#!/usr/bin/env python3
"""CDK エントリーポイント — AI Ready Governance"""

import aws_cdk as cdk

from stack import AIReadyGovernanceStack

app = cdk.App()

AIReadyGovernanceStack(
    app,
    "AIReadyGovernanceStack",
    env=cdk.Environment(
        account="565699611973",
        region="ap-northeast-1",
    ),
    description="AI Ready Governance - Oversharing detection pipeline",
)

app.synth()
