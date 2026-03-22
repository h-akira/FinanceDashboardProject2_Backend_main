from __future__ import annotations

import boto3
from aws_lambda_powertools import Tracer
from botocore.exceptions import ClientError

from common import config
from common.exceptions import AuthenticationError, ValidationError

tracer = Tracer()


@tracer.capture_method(capture_response=False)
def get_me(claims: dict) -> dict:
  username = claims["cognito:username"]
  email = claims.get("email", "")
  email_verified = claims.get("email_verified", "false")

  client = boto3.client("cognito-idp")
  try:
    user_info = client.admin_get_user(
      UserPoolId=config.USER_POOL_ID,
      Username=username,
    )
    created_at = user_info["UserCreateDate"].strftime("%Y-%m-%dT%H:%M:%SZ")
  except ClientError:
    created_at = ""

  return {
    "username": username,
    "email": email,
    "email_verified": email_verified in ("true", True),
    "created_at": created_at,
  }


@tracer.capture_method(capture_response=False)
def delete_account(username: str, password: str) -> None:
  if not password:
    raise ValidationError("password is required")

  client = boto3.client("cognito-idp")

  # Verify password via AdminInitiateAuth
  try:
    client.admin_initiate_auth(
      UserPoolId=config.USER_POOL_ID,
      ClientId=config.CLIENT_ID,
      AuthFlow="ADMIN_NO_SRP_AUTH",
      AuthParameters={
        "USERNAME": username,
        "PASSWORD": password,
      },
    )
  except ClientError as e:
    if e.response["Error"]["Code"] in (
      "NotAuthorizedException",
      "UserNotFoundException",
    ):
      raise AuthenticationError("Invalid password")
    raise

  # Delete Cognito user
  client.admin_delete_user(
    UserPoolId=config.USER_POOL_ID,
    Username=username,
  )
