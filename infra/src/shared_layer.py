from aws_cdk import aws_lambda
from aws_cdk.aws_lambda_python_alpha import PythonLayerVersion
from constructs import Construct


class SharedLayer(Construct):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self._layer = PythonLayerVersion(
            self,
            "Layer",
            entry="shared/src",
            compatible_runtimes=[aws_lambda.Runtime.PYTHON_3_12],
            description="Shared utilities and dependencies for Boston 311 project",
        )

    def get_layer(self) -> PythonLayerVersion:
        return self._layer


def create_shared_layer(scope: Construct) -> SharedLayer:
    return SharedLayer(scope, "SharedResources")
