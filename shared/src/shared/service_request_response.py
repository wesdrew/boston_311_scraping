from typing import List

from pydantic import RootModel

from shared.service_request import ServiceRequest


class ServiceRequestResponse(RootModel):
    root: List[ServiceRequest]
