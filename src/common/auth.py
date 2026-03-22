from aws_lambda_powertools.event_handler import APIGatewayRestResolver


def get_username(app: APIGatewayRestResolver) -> str:
  return app.current_event.request_context.authorizer.claims["cognito:username"]
