from typing import Optional

from pydantic import BaseModel, ConfigDict

from shared.service_extended_attributes import ServiceExtendedAttributes


class Service(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    # Standard Open311 fields
    service_code: str
    service_name: str
    description: Optional[str] = None
    metadata: bool
    type: str
    keywords: Optional[str] = None
    group: Optional[str] = None

    # Boston Extensions
    extended_attributes: Optional[ServiceExtendedAttributes] = None
