from pydantic import BaseModel, ConfigDict

from shared.boston_311_api.service_extended_attributes import ServiceExtendedAttributes


class Service(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    # Standard Open311 fields
    service_code: str
    service_name: str
    description: str | None = None
    metadata: bool
    type: str
    keywords: str | None = None
    group: str | None = None

    # Boston Extensions
    extended_attributes: ServiceExtendedAttributes | None = None
