import json
from unittest.mock import patch

import boto3

from conftest import make_apigw_event, seed_interest_rate_data, seed_custom_chart_data


class TestUsersRoutes:
  def test_get_me(self, cognito_resources):
    pool_id = cognito_resources["pool_id"]
    client = boto3.client("cognito-idp", region_name="ap-northeast-1")
    client.admin_create_user(
      UserPoolId=pool_id,
      Username="testuser",
      UserAttributes=[
        {"Name": "email", "Value": "testuser@example.com"},
      ],
    )

    from app import lambda_handler
    event = make_apigw_event("GET", "/api/v1/main/users/me", username="testuser")
    result = lambda_handler(event, None)

    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert body["username"] == "testuser"
    assert body["email"] == "testuser@example.com"
    assert body["email_verified"] is True
    assert "created_at" in body

  def test_delete_me_success(self, cognito_resources):
    pool_id = cognito_resources["pool_id"]
    client = boto3.client("cognito-idp", region_name="ap-northeast-1")
    client.admin_create_user(
      UserPoolId=pool_id,
      Username="testuser",
      TemporaryPassword="TempPass1!",
    )
    client.admin_set_user_password(
      UserPoolId=pool_id,
      Username="testuser",
      Password="MyPass123!",
      Permanent=True,
    )

    from app import lambda_handler
    event = make_apigw_event(
      "DELETE", "/api/v1/main/users/me",
      body={"password": "MyPass123!"},
      username="testuser",
    )
    result = lambda_handler(event, None)

    assert result["statusCode"] == 204

  def test_delete_me_wrong_password(self, cognito_resources):
    pool_id = cognito_resources["pool_id"]
    client = boto3.client("cognito-idp", region_name="ap-northeast-1")
    client.admin_create_user(
      UserPoolId=pool_id,
      Username="testuser",
      TemporaryPassword="TempPass1!",
    )
    client.admin_set_user_password(
      UserPoolId=pool_id,
      Username="testuser",
      Password="MyPass123!",
      Permanent=True,
    )

    from app import lambda_handler
    event = make_apigw_event(
      "DELETE", "/api/v1/main/users/me",
      body={"password": "WrongPass1!"},
      username="testuser",
    )
    result = lambda_handler(event, None)

    assert result["statusCode"] == 403
    body = json.loads(result["body"])
    assert "Invalid password" in body["message"]

  def test_delete_me_no_password(self, cognito_resources):
    from app import lambda_handler
    event = make_apigw_event(
      "DELETE", "/api/v1/main/users/me",
      body={},
      username="testuser",
    )
    result = lambda_handler(event, None)

    assert result["statusCode"] == 400
    body = json.loads(result["body"])
    assert "password" in body["message"].lower()


class TestFinanceRoutes:
  def test_get_interest_rate_from_dynamodb(self, dynamodb_table):
    seed_interest_rate_data(dynamodb_table)

    from app import lambda_handler
    with patch("services.finance_service._fetch_fred_recent", return_value=[]):
      event = make_apigw_event("GET", "/api/v1/main/finance/interest-rate", username="testuser")
      result = lambda_handler(event, None)

    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert "data" in body
    assert len(body["data"]) == 3

    first = body["data"][0]
    assert first["time"] == "2024-11-30"
    assert "target_rate" in first
    assert "dgs10" in first

  def test_get_interest_rate_empty_table(self, dynamodb_table):
    from app import lambda_handler
    with patch("services.finance_service._fetch_fred_recent", return_value=[]):
      event = make_apigw_event("GET", "/api/v1/main/finance/interest-rate", username="testuser")
      result = lambda_handler(event, None)

    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert body["data"] == []

  def test_get_interest_rate_with_fred_data(self, dynamodb_table):
    seed_interest_rate_data(dynamodb_table)

    fred_data = [
      {"time": "2026-01-31", "target_rate": 4.0, "dgs10": 4.2},
      {"time": "2024-12-31", "target_rate": 9.99, "dgs10": 9.99},  # duplicate, should be ignored
    ]

    from app import lambda_handler
    with patch("services.finance_service._fetch_fred_recent", return_value=fred_data):
      event = make_apigw_event("GET", "/api/v1/main/finance/interest-rate", username="testuser")
      result = lambda_handler(event, None)

    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert len(body["data"]) == 4  # 3 stored + 1 new FRED

    last = body["data"][-1]
    assert last["time"] == "2026-01-31"
    assert last["target_rate"] == 4.0

    # Verify stored data takes precedence over FRED duplicate
    dec_entry = next(d for d in body["data"] if d["time"] == "2024-12-31")
    assert dec_entry["target_rate"] != 9.99


class TestCustomChartRoutes:
  def test_get_sources(self, dynamodb_table):
    from app import lambda_handler
    event = make_apigw_event("GET", "/api/v1/main/finance/custom-chart/sources", username="testuser")
    result = lambda_handler(event, None)

    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert "sources" in body
    assert "max_axes" in body
    assert "axis_groups" in body
    assert "other_display_name" in body
    assert body["max_axes"] == 2
    assert body["other_display_name"] == "その他"
    assert len(body["sources"]) == 7

    # Verify default field exists on all sources
    for s in body["sources"]:
      assert "default" in s

    # Verify axis_groups have display_name
    for group in body["axis_groups"].values():
      assert "label" in group
      assert "display_name" in group

  def test_get_data_success(self, dynamodb_table):
    seed_custom_chart_data(dynamodb_table)

    from app import lambda_handler
    with patch("services.custom_chart_service._fetch_recent_for_source", return_value=[]):
      event = make_apigw_event(
        "GET", "/api/v1/main/finance/custom-chart/data",
        username="testuser",
        query_params={"sources": "target_rate,dgs10"},
      )
      result = lambda_handler(event, None)

    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert len(body["series"]) == 2

  def test_get_data_no_sources_param(self, dynamodb_table):
    from app import lambda_handler
    event = make_apigw_event(
      "GET", "/api/v1/main/finance/custom-chart/data",
      username="testuser",
    )
    result = lambda_handler(event, None)

    assert result["statusCode"] == 400
    body = json.loads(result["body"])
    assert "sources" in body["message"].lower()

  def test_get_data_invalid_source(self, dynamodb_table):
    from app import lambda_handler
    with patch("services.custom_chart_service._fetch_recent_for_source", return_value=[]):
      event = make_apigw_event(
        "GET", "/api/v1/main/finance/custom-chart/data",
        username="testuser",
        query_params={"sources": "invalid_id"},
      )
      result = lambda_handler(event, None)

    assert result["statusCode"] == 400
    body = json.loads(result["body"])
    assert "Invalid source IDs" in body["message"]

  def test_get_data_empty_sources(self, dynamodb_table):
    from app import lambda_handler
    event = make_apigw_event(
      "GET", "/api/v1/main/finance/custom-chart/data",
      username="testuser",
      query_params={"sources": ""},
    )
    result = lambda_handler(event, None)

    assert result["statusCode"] == 400
