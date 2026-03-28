import json
import os
from decimal import Decimal

# Set environment variables before any application imports
os.environ["USER_POOL_ID"] = "ap-northeast-1_TestPool"
os.environ["CLIENT_ID"] = "test-client-id"
os.environ["TABLE_NAME"] = "test-table"
os.environ["AWS_DEFAULT_REGION"] = "ap-northeast-1"
os.environ["AWS_ACCESS_KEY_ID"] = "testing"
os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
os.environ["AWS_SECURITY_TOKEN"] = "testing"
os.environ["AWS_SESSION_TOKEN"] = "testing"

import pytest
import boto3
from moto import mock_aws


@pytest.fixture
def aws_mock():
  with mock_aws():
    yield


@pytest.fixture
def cognito_resources(aws_mock):
  client = boto3.client("cognito-idp", region_name="ap-northeast-1")
  pool = client.create_user_pool(PoolName="test-pool")
  pool_id = pool["UserPool"]["Id"]
  app_client = client.create_user_pool_client(
    UserPoolId=pool_id,
    ClientName="test-client",
    ExplicitAuthFlows=["ADMIN_NO_SRP_AUTH"],
  )
  client_id = app_client["UserPoolClient"]["ClientId"]
  os.environ["USER_POOL_ID"] = pool_id
  os.environ["CLIENT_ID"] = client_id

  # Reload config to pick up new values
  import common.config as config_mod
  config_mod.USER_POOL_ID = pool_id
  config_mod.CLIENT_ID = client_id

  yield {"pool_id": pool_id, "client_id": client_id}


@pytest.fixture
def dynamodb_table(aws_mock):
  dynamodb = boto3.resource("dynamodb", region_name="ap-northeast-1")
  table = dynamodb.create_table(
    TableName="test-table",
    KeySchema=[
      {"AttributeName": "PK", "KeyType": "HASH"},
      {"AttributeName": "SK", "KeyType": "RANGE"},
    ],
    AttributeDefinitions=[
      {"AttributeName": "PK", "AttributeType": "S"},
      {"AttributeName": "SK", "AttributeType": "S"},
    ],
    BillingMode="PAY_PER_REQUEST",
  )

  # Reset cached table reference in repository
  import repositories.finance_repository as repo
  repo._table = None

  yield table


def seed_interest_rate_data(table) -> None:
  """Insert sample interest rate data into DynamoDB."""
  items = [
    {"PK": "KIND#target_rate", "SK": "TIME#2024-11-30", "value": Decimal("4.5800")},
    {"PK": "KIND#target_rate", "SK": "TIME#2024-12-31", "value": Decimal("4.3300")},
    {"PK": "KIND#target_rate", "SK": "TIME#2025-01-31", "value": Decimal("4.3300")},
    {"PK": "KIND#dgs10", "SK": "TIME#2024-11-30", "value": Decimal("4.1930")},
    {"PK": "KIND#dgs10", "SK": "TIME#2024-12-31", "value": Decimal("4.5770")},
    {"PK": "KIND#dgs10", "SK": "TIME#2025-01-31", "value": Decimal("4.5410")},
  ]
  with table.batch_writer() as batch:
    for item in items:
      batch.put_item(Item=item)


def seed_custom_chart_data(table) -> None:
  """Insert sample custom chart data into DynamoDB."""
  items = [
    {"PK": "KIND#target_rate", "SK": "TIME#2024-01-15", "value": Decimal("5.3300")},
    {"PK": "KIND#target_rate", "SK": "TIME#2024-02-01", "value": Decimal("5.3300")},
    {"PK": "KIND#sp500", "SK": "TIME#2024-01-15", "value": Decimal("4783.4500")},
    {"PK": "KIND#sp500", "SK": "TIME#2024-02-01", "value": Decimal("4845.6500")},
    {"PK": "KIND#dgs10", "SK": "TIME#2024-01-15", "value": Decimal("4.1200")},
    {"PK": "KIND#score", "SK": "TIME#2024-01-15", "value": Decimal("3.5000")},
  ]
  with table.batch_writer() as batch:
    for item in items:
      batch.put_item(Item=item)


def make_apigw_event(
  method: str,
  path: str,
  body: dict | None = None,
  username: str | None = None,
  query_params: dict | None = None,
  path_params: dict | None = None,
) -> dict:
  event = {
    "httpMethod": method,
    "path": path,
    "body": json.dumps(body) if body else None,
    "queryStringParameters": query_params,
    "pathParameters": path_params,
    "headers": {"Content-Type": "application/json"},
    "requestContext": {},
    "multiValueHeaders": {},
    "multiValueQueryStringParameters": query_params if query_params else None,
    "isBase64Encoded": False,
    "resource": path,
    "stageVariables": None,
  }
  if username:
    event["requestContext"] = {
      "authorizer": {
        "claims": {
          "cognito:username": username,
          "email": f"{username}@example.com",
          "email_verified": "true",
        },
      },
    }
  return event
