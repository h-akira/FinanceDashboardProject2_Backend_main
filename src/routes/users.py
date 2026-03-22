from aws_lambda_powertools.event_handler.api_gateway import Router, Response

from common.auth import get_username
from services import user_service

router = Router()


@router.get("/me")
def get_me():
  claims = router.current_event.request_context.authorizer.claims
  user = user_service.get_me(claims)
  return user


@router.delete("/me")
def delete_me():
  username = get_username(router)
  body = router.current_event.json_body or {}
  password = body.get("password", "")
  user_service.delete_account(username, password)
  return Response(status_code=204, body="")
