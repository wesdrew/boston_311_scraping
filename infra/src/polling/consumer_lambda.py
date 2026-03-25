from aws_cdk import (
    Duration,
    aws_ec2,
    aws_lambda,
    aws_lambda_event_sources,
    aws_logs,
    aws_secretsmanager,
    aws_sns,
    aws_sqs,
)
from aws_cdk.aws_lambda_python_alpha import BundlingOptions, PythonFunction, PythonLayerVersion
from constructs import Construct


class ConsumerLambda(Construct):
    """
    Consumer lambda triggered by SQS, processing service requests from the Boston311 polling queue.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        queue: aws_sqs.Queue,
        topic: aws_sns.Topic,
        shared_layer: PythonLayerVersion,
        log_group: aws_logs.LogGroup,
        vpc: aws_ec2.Vpc,
        security_group: aws_ec2.SecurityGroup,
        db_secret: aws_secretsmanager.Secret,
    ) -> None:
        super().__init__(scope, construct_id)

        self.fn = PythonFunction(
            self,
            "Boston311ConsumerLambda",
            entry="polling",
            index="src/consumer/consumer_lambda.py",
            handler="handler",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            timeout=Duration.minutes(5),
            memory_size=256,
            log_group=log_group,
            layers=[shared_layer],
            bundling=BundlingOptions(asset_excludes=["tests", "__pycache__", "*.pyc"]),
            tracing=aws_lambda.Tracing.ACTIVE,
            insights_version=aws_lambda.LambdaInsightsVersion.VERSION_1_0_119_0,
            vpc=vpc,
            vpc_subnets=aws_ec2.SubnetSelection(subnet_type=aws_ec2.SubnetType.PRIVATE_ISOLATED),
            security_groups=[security_group],
            environment={
                "PYTHONPATH": "/var/task/src",
                "APP_EVENTS_TOPIC_ARN": topic.topic_arn,
                "DB_SECRET_ARN": db_secret.secret_arn,
            },
        )

        self.fn.add_event_source(
            aws_lambda_event_sources.SqsEventSource(
                queue,
                batch_size=10,
                report_batch_item_failures=True,
            )
        )

        topic.grant_publish(self.fn)
        db_secret.grant_read(self.fn)
