import opentelemetry.sdk.environment_variables as otel_env
from aws_cdk import Duration, RemovalPolicy, Stack, aws_events, aws_events_targets, aws_lambda, aws_logs
from aws_cdk.aws_lambda_python_alpha import BundlingOptions, PythonFunction
from constructs import Construct
from shared_layer import create_shared_layer


class PollingStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs: dict) -> None:
        super().__init__(scope, construct_id, **kwargs)

        polling_log_group = aws_logs.LogGroup(
            self,
            "PollingLogGroup",
            log_group_name="Boston311.polling",
            retention=aws_logs.RetentionDays.ONE_MONTH,
            removal_policy=RemovalPolicy.RETAIN,
        )

        self.polling_fn = PythonFunction(
            self,
            id="Boston311PollingLambda",
            entry="polling",
            index="src/app.py",
            handler="handler",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            timeout=Duration.seconds(300),
            memory_size=256,
            log_group=polling_log_group,
            layers=[create_shared_layer(self).get_layer()],
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
                otel_env.OTEL_SERVICE_NAME: "boston311.polling",
                otel_env.OTEL_EXPORTER_OTLP_PROTOCOL: "http/protobuf",
                otel_env.OTEL_EXPORTER_OTLP_ENDPOINT: "http://localhost:4318",
                otel_env.OTEL_LOG_LEVEL: "info",
                otel_env.OTEL_TRACES_SAMPLER: "xray",
                otel_env.OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE: "delta",
            },
        )

        rule = aws_events.Rule(
            self,
            "PollingScheduleRule",
            schedule=aws_events.Schedule.rate(Duration.minutes(10)),
            description="Trigger Boston 311 polling every 5 minutes",
        )

        rule.add_target(aws_events_targets.LambdaFunction(self.polling_fn))
