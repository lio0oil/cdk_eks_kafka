#!/usr/bin/env python3
import os

import aws_cdk as cdk

from ekscdk.ekscdk_stack import EksCdkStack

app = cdk.App()

EksCdkStack(
    app,
    "EksCdkStack",
    env=cdk.Environment(
        account=os.getenv("CDK_DEFAULT_ACCOUNT"),
        region=os.getenv("CDK_DEFAULT_REGION"),
    ),
)

app.synth()
