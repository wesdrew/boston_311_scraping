from requests import Response, get
from shared.boston_311_api.service_request_response import ServiceRequestResponse

from .request import ThreeOneOneRequest


class ThreeOneOneClientConfigException(Exception):
    pass


class ThreeOneOneClient:
    def __init__(self, base_url: str):
        if not base_url:
            raise ThreeOneOneClientConfigException("base_url is required")
        self.base_url = base_url

    def get_service_requests(self, request: ThreeOneOneRequest) -> ServiceRequestResponse:
        response: Response = get(self.base_url, params=request.model_dump(exclude_none=True), timeout=5.0)
        response.raise_for_status()
        return ServiceRequestResponse.model_validate(response.json())
