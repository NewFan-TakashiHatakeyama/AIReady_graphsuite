#!/usr/bin/env python3
"""CDK エントリーポイント"""

import aws_cdk as cdk

from stack import AIReadyConnectStack

app = cdk.App()

AIReadyConnectStack(
    app,
    "AIReadyConnectStack",
    env=cdk.Environment(
        account="565699611973",
        region="ap-northeast-1",
    ),
    description="AI Ready Connect - M365 file event detection PoC",
)

app.synth()
