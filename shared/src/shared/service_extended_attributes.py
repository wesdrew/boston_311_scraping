from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict


class ServiceExtendedAttributes(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    active: Optional[bool] = None
    notice: Optional[str] = None
    updated_at: Optional[datetime] = None
