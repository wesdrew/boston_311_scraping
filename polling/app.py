from aws_lambda_powertools.logging import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

logger: Logger = Logger()


@logger.inject_lambda_context
def handler(_event: dict, _context: LambdaContext) -> dict:
    logger.info("We back up")
    return {}
