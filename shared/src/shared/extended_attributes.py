from typing import List, Optional

from pydantic import BaseModel, ConfigDict

from shared.photo import Photo


class ExtendedAttributes(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    x: Optional[float] = None
    y: Optional[float] = None
    photos: Optional[List[Photo]] = None
