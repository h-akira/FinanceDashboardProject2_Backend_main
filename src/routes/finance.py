from aws_lambda_powertools.event_handler.api_gateway import Router

from common.exceptions import ValidationError
from services import finance_service, custom_chart_service

router = Router()


@router.get("/interest-rate")
def get_interest_rate():
  return finance_service.get_interest_rate()


@router.get("/custom-chart/sources")
def get_custom_chart_sources():
  return custom_chart_service.get_sources()


@router.get("/custom-chart/data")
def get_custom_chart_data():
  sources_param: str = router.current_event.get_query_string_value("sources", "")
  if not sources_param:
    raise ValidationError("sources parameter is required")
  source_ids = [s.strip() for s in sources_param.split(",") if s.strip()]
  if not source_ids:
    raise ValidationError("sources parameter is required")
  return custom_chart_service.get_data(source_ids)
