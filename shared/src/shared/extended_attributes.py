from pydantic import BaseModel, ConfigDict

from shared.photo import Photo


class ExtendedAttributes(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    x: float | None = None
    y: float | None = None
    photos: list[Photo] = []
