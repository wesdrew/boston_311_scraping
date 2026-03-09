from typing import List

from pydantic import RootModel

from shared.service import Service


class ServiceResponse(RootModel):
    root: List[Service]
