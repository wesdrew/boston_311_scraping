import json

from aws_cdk import RemovalPolicy, Stack, aws_secretsmanager
from constructs import Construct

_USERNAME = "admin"


class SecretStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs: dict) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.secret = aws_secretsmanager.Secret(
            self,
            "DbCredentials",
            generate_secret_string=aws_secretsmanager.SecretStringGenerator(
                secret_string_template=json.dumps({"username": _USERNAME}),
                generate_string_key="password",
                exclude_punctuation=True,
            ),
            removal_policy=RemovalPolicy.RETAIN,
        )
