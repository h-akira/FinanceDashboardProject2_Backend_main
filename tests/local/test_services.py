from unittest.mock import patch

import boto3

from conftest import seed_interest_rate_data


class TestUserService:
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

    from services.user_service import get_me
    claims = {
      "cognito:username": "testuser",
      "email": "testuser@example.com",
      "email_verified": "true",
    }
    result = get_me(claims)

    assert result["username"] == "testuser"
    assert result["email"] == "testuser@example.com"
    assert result["email_verified"] is True
    assert result["created_at"] != ""

  def test_get_me_cognito_error(self, cognito_resources):
    from services.user_service import get_me
    claims = {
      "cognito:username": "nonexistent",
      "email": "test@example.com",
      "email_verified": "false",
    }
    result = get_me(claims)

    assert result["username"] == "nonexistent"
    assert result["created_at"] == ""

  def test_delete_account_no_password(self, cognito_resources):
    import pytest
    from services.user_service import delete_account
    from common.exceptions import ValidationError

    with pytest.raises(ValidationError, match="password is required"):
      delete_account("testuser", "")

  def test_delete_account_wrong_password(self, cognito_resources):
    import pytest
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

    from services.user_service import delete_account
    from common.exceptions import AuthenticationError

    with pytest.raises(AuthenticationError, match="Invalid password"):
      delete_account("testuser", "WrongPass1!")


class TestFinanceService:
  def test_get_interest_rate(self, dynamodb_table):
    seed_interest_rate_data(dynamodb_table)

    from services.finance_service import get_interest_rate
    with patch("services.finance_service._fetch_fred_recent", return_value=[]):
      result = get_interest_rate()

    assert "data" in result
    assert len(result["data"]) == 3
    assert all("time" in d and "target_rate" in d and "dgs10" in d for d in result["data"])

    # Verify sorted order
    times = [d["time"] for d in result["data"]]
    assert times == sorted(times)

  def test_get_interest_rate_merges_fred(self, dynamodb_table):
    seed_interest_rate_data(dynamodb_table)

    fred_data = [
      {"time": "2026-01-31", "target_rate": 4.0, "dgs10": 4.2},
    ]

    from services.finance_service import get_interest_rate
    with patch("services.finance_service._fetch_fred_recent", return_value=fred_data):
      result = get_interest_rate()

    assert len(result["data"]) == 4
    assert result["data"][-1]["time"] == "2026-01-31"
