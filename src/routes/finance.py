from aws_lambda_powertools.event_handler.api_gateway import Router

from services import finance_service

router = Router()


@router.get("/interest-rate")
def get_interest_rate():
  return finance_service.get_interest_rate()
