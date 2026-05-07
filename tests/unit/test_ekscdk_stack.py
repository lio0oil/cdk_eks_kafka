import aws_cdk as core
import aws_cdk.assertions as assertions

from ekscdk.ekscdk_stack import EksCdkStack


def test_stack_synthesizes():
    app = core.App(context={"repo-url": "https://github.com/example/ekscdk"})
    stack = EksCdkStack(app, "ekscdk", env=core.Environment(account="123456789012", region="ap-northeast-1"))
    assertions.Template.from_stack(stack)
