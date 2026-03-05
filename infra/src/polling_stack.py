from aws_cdk import Duration, Stack, aws_events, aws_events_targets, aws_lambda
from aws_cdk.aws_lambda_python_alpha import PythonFunction
from constructs import Construct


class PollingStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs: dict) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.polling_fn = PythonFunction(
            self,
            id="Boston311PollingLambda",
            entry="polling",
            index="src/app.py",
            handler="handler",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            timeout=Duration.seconds(300),
            memory_size=256,
        )

        rule = aws_events.Rule(
            self,
            "PollingScheduleRule",
            schedule=aws_events.Schedule.rate(Duration.minutes(5)),
            description="Trigger Boston 311 polling every 5 minutes",
        )

        rule.add_target(aws_events_targets.LambdaFunction(self.polling_fn))
