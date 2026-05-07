import aws_cdk as core
import aws_cdk.assertions as assertions

from ekscdk.ekscdk_stack import EkscdkStack

# example tests. To run these tests, uncomment this file along with the example
# resource in ekscdk/ekscdk_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = EkscdkStack(app, "ekscdk")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
