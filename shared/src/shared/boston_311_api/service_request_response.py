from pydantic import RootModel

from shared.boston_311_api.service_request import ServiceRequest


class ServiceRequestResponse(RootModel):
    root: list[ServiceRequest]
