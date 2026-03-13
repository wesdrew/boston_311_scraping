from pydantic import RootModel

from shared.service import Service


class ServiceResponse(RootModel):
    root: list[Service]
