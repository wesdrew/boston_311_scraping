from aws_cdk import aws_sns
from constructs import Construct


class Notifications(Construct):
    """
    SNS topic for event level logging across components.
    """

    def __init__(self, scope: Construct, construct_id: str) -> None:
        super().__init__(scope, construct_id)

        self.topic: aws_sns.Topic = aws_sns.Topic(
            self,
            "Topic",
            display_name="Boston311Notifications",
        )
