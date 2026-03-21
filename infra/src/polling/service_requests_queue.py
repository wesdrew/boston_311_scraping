from aws_cdk import Duration, RemovalPolicy, aws_sqs
from constructs import Construct


class ServiceRequestsQueue(Construct):
    """
    SQS queue for receiving polled service requests.
    """

    def __init__(self, scope: Construct, construct_id: str) -> None:
        super().__init__(scope, construct_id)

        self.dlq: aws_sqs.Queue = aws_sqs.Queue(
            self,
            "DLQ",
            retention_period=Duration.days(14),
            removal_policy=RemovalPolicy.RETAIN,
        )
        self.queue: aws_sqs.Queue = aws_sqs.Queue(
            self,
            "Queue",
            visibility_timeout=Duration.minutes(15),
            dead_letter_queue=aws_sqs.DeadLetterQueue(max_receive_count=3, queue=self.dlq),
        )
