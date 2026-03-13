from datetime import datetime, timedelta, timezone

from aws_lambda_powertools.logging import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext
from client import ThreeOneOneClient, ThreeOneOneClientConfigException, ThreeOneOneRequest

from shared.service_request_response import ServiceRequestResponse

logger: Logger = Logger(service="polling")

BASE_URL = "https://311.boston.gov/open311/v2/requests.json"


client: ThreeOneOneClient = ThreeOneOneClient(BASE_URL)


@logger.inject_lambda_context
def handler(_event: dict, _context: LambdaContext) -> dict:
    logger.info("Fetching service requests from Boston 311")
    requests_starting_at_datetime: datetime = datetime.now(timezone.utc) - timedelta(minutes=20)
    request: ThreeOneOneRequest = ThreeOneOneRequest(start_date=requests_starting_at_datetime)
    response: ServiceRequestResponse = client.get_service_requests(request)
    logger.info("Repsonse from 311", response=response)
    return {"data": response}
