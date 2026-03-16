from unittest.mock import MagicMock

from shared.boston_311_api.service_request import ServiceRequest
from shared.boston_311_api.service_request_response import ServiceRequestResponse


def make_response(count: int) -> ServiceRequestResponse:
    return ServiceRequestResponse([ServiceRequest(service_request_id=str(i), status="open") for i in range(count)])


def make_context() -> MagicMock:
    context = MagicMock()
    context.aws_request_id = "test-request-id"
    context.function_name = "test-function"
    context.function_version = "$LATEST"
    context.invoked_function_arn = "arn:aws:lambda:us-east-1:123456789:function:test"
    context.log_group_name = "/aws/lambda/test"
    context.log_stream_name = "2026/03/15/[$LATEST]abc123"
    return context
