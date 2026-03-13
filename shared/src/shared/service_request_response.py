from pydantic import RootModel

from shared.service_request import ServiceRequest


class ServiceRequestResponse(RootModel):
    root: list[ServiceRequest]
