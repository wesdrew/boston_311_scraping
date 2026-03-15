from pydantic import BaseModel, ConfigDict

from shared.boston_311_api.photo import Photo


class ExtendedAttributes(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    x: float | str | None = None
    y: float | str | None = None
    name: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    email: str | None = None
    phone: str | None = None
    photos: list[Photo] = []
