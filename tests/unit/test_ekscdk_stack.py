import aws_cdk as core
import aws_cdk.assertions as assertions

from ekscdk.iam_stack import IamStack
from ekscdk.ekscdk_stack import EksCdkStack


def test_stack_synthesizes():
    app = core.App(context={"repo-url": "https://github.com/example/ekscdk"})
    env = core.Environment(account="123456789012", region="ap-northeast-1")
    iam_stack = IamStack(app, "IamStack", env=env)
    stack = EksCdkStack(app, "ekscdk", admin_role=iam_stack.eks_admin_role, env=env)
    assertions.Template.from_stack(stack)
