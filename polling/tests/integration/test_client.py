from datetime import datetime, timedelta, timezone

import pytest
from client import ThreeOneOneClient
from request import ThreeOneOneRequest

from shared.boston_311_api.service_request_response import ServiceRequestResponse

BASE_URL = "https://311.boston.gov/open311/v2/requests.json"


@pytest.mark.integration
def test_get_service_requests():
    client = ThreeOneOneClient(BASE_URL)
    start_date = datetime.now(timezone.utc) - timedelta(minutes=20)
    request = ThreeOneOneRequest(start_date=start_date)
    response = client.get_service_requests(request)
    assert isinstance(response, ServiceRequestResponse)
