from aws_cdk import RemovalPolicy, Stack, aws_logs
from aws_cdk.aws_lambda_python_alpha import PythonLayerVersion
from constructs import Construct
from consumer_lambda import ConsumerLambda
from notifications import Notifications
from polling_lambda import PollingLambda
from service_requests_queue import ServiceRequestsQueue
from shared_layer import SharedLayer


class PollingStack(Stack):
    """
    Polling lambda on cron trigger pulling service requests from the Boston311 API.
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs: dict) -> None:
        super().__init__(scope, construct_id, **kwargs)

        srq: ServiceRequestsQueue = ServiceRequestsQueue(self, "ServiceRequestsQueue")
        notifications: Notifications = Notifications(self, "Notifications")
        shared_layer: PythonLayerVersion = SharedLayer(self, "SharedResources").layer

        polling_log_group: aws_logs.LogGroup = aws_logs.LogGroup(
            self,
            "PollingLogGroup",
            log_group_name=construct_id,
            retention=aws_logs.RetentionDays.ONE_MONTH,
            removal_policy=RemovalPolicy.RETAIN,
        )
        consumer_log_group: aws_logs.LogGroup = aws_logs.LogGroup(
            self,
            "ConsumerLogGroup",
            log_group_name=f"{construct_id}-consumer",
            retention=aws_logs.RetentionDays.ONE_MONTH,
            removal_policy=RemovalPolicy.RETAIN,
        )

        PollingLambda(
            self,
            "PollingLambda",
            queue=srq.queue,
            topic=notifications.topic,
            stack_name=construct_id,
            shared_layer=shared_layer,
            log_group=polling_log_group,
        )
        ConsumerLambda(
            self,
            "ConsumerLambda",
            queue=srq.queue,
            topic=notifications.topic,
            stack_name=construct_id,
            shared_layer=shared_layer,
            log_group=consumer_log_group,
        )
