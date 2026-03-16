import opentelemetry.environment_variables as otel_core_env
import opentelemetry.sdk.environment_variables as otel_env
from aws_cdk import Duration, RemovalPolicy, Stack, aws_events, aws_events_targets, aws_lambda, aws_logs
from aws_cdk.aws_lambda_python_alpha import BundlingOptions, PythonFunction
from constructs import Construct
from notifications import Notifications
from service_requests_queue import ServiceRequestsQueue
from shared_layer import SharedLayer


class PollingStack(Stack):
    """
    Polling lambda on cron trigger pulling service requests from the Boston311 API.
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs: dict) -> None:
        super().__init__(scope, construct_id, **kwargs)

        polling_log_group: aws_logs.LogGroup = aws_logs.LogGroup(
            self,
            "PollingLogGroup",
            log_group_name=construct_id,
            retention=aws_logs.RetentionDays.ONE_MONTH,
            removal_policy=RemovalPolicy.RETAIN,
        )

        srq: ServiceRequestsQueue = ServiceRequestsQueue(self, "ServiceRequestsQueue")
        notifications: Notifications = Notifications(self, "Notifications")

        self.polling_fn = PythonFunction(
            self,
            id="Boston311PollingLambda",
            entry="polling",
            index="src/polling_lambda.py",
            handler="handler",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            timeout=Duration.seconds(300),
            memory_size=256,
            log_group=polling_log_group,
            layers=[SharedLayer(self, "SharedResources").layer],
            bundling=BundlingOptions(asset_excludes=["tests", "__pycache__", "*.pyc"]),
            tracing=aws_lambda.Tracing.ACTIVE,
            insights_version=aws_lambda.LambdaInsightsVersion.VERSION_1_0_119_0,
            adot_instrumentation=aws_lambda.AdotInstrumentationConfig(
                layer_version=aws_lambda.AdotLayerVersion.from_python_sdk_layer_version(
                    aws_lambda.AdotLambdaLayerPythonSdkVersion.LATEST
                ),
                exec_wrapper=aws_lambda.AdotLambdaExecWrapper.INSTRUMENT_HANDLER,
            ),
            environment={
                "PYTHONPATH": "/var/task/src",
                "SERVICE_REQUESTS_QUEUE_URL": srq.queue.queue_url,
                "APP_EVENTS_TOPIC_ARN": notifications.topic.topic_arn,
                otel_env.OTEL_SERVICE_NAME: construct_id.lower(),
                otel_env.OTEL_EXPORTER_OTLP_PROTOCOL: "http/protobuf",
                otel_env.OTEL_EXPORTER_OTLP_ENDPOINT: "http://localhost:4318",
                otel_env.OTEL_LOG_LEVEL: "info",
                otel_env.OTEL_TRACES_SAMPLER: "xray",
                otel_core_env.OTEL_METRICS_EXPORTER: "otlp",
                otel_core_env.OTEL_TRACES_EXPORTER: "otlp",
                otel_env.OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE: "delta",
            },
        )

        srq.queue.grant_send_messages(self.polling_fn)
        notifications.topic.grant_publish(self.polling_fn)

        rule: aws_events.Rule = aws_events.Rule(
            self,
            "PollingScheduleRule",
            schedule=aws_events.Schedule.rate(Duration.minutes(10)),
            description="Trigger Boston 311 polling every 10 minutes",
        )

        rule.add_target(aws_events_targets.LambdaFunction(self.polling_fn))
