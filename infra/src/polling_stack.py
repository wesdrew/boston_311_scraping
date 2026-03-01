import os
from pathlib import Path

from aws_cdk import DockerImage, Duration, Stack, aws_lambda
from aws_cdk.aws_lambda_python_alpha import BundlingOptions, PythonFunction
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
