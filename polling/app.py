from aws_lambda_powertools.logging import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext
from requests import Response, get

logger: Logger = Logger()

BASE_URL = "https://311.boston.gov/open311/v2/requests.json"


def get_service_requests() -> tuple[int, dict]:
    response: Response = get(BASE_URL, params=params, timeout=1.0)
    response.raise_for_status()
    return (response.status_code, response.json())


@logger.inject_lambda_context
def handler(_event: dict, _context: LambdaContext) -> dict:
    logger.info("Fetching service requests from Boston 311")
    data: tuple[int, dict] = get_service_requests()
    logger.info("Got service requests", number_of_requests=len(data[1]))
    return {"statusCode": data[0], "body": data[1]}
