import json
from pathlib import Path

from shared.boston_311_api.service_request_response import ServiceRequestResponse


def test_service_request_response_empty():
    response = ServiceRequestResponse.model_validate([])
    assert response.root == []


def test_service_request_response_list():
    data = json.loads(Path(__file__).parent.joinpath("service_requests.json").read_text())
    response = ServiceRequestResponse.model_validate(data)
    assert len(response.root) == len(data)
