from unittest.mock import patch

import boto3

from conftest import seed_interest_rate_data, seed_custom_chart_data


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


class TestCustomChartService:
  def test_get_sources(self):
    from services.custom_chart_service import get_sources
    result = get_sources()

    assert result["max_axes"] == 2
    assert len(result["sources"]) == 9
    for s in result["sources"]:
      assert all(k in s for k in ("id", "name", "axis_group", "axis_label", "default"))

    # Verify axis_groups includes both normal and independent groups
    assert "axis_groups" in result
    assert "rate_pct1" in result["axis_groups"]
    assert "independent1" in result["axis_groups"]

    # Verify independent_groups and other_display_name are NOT in response
    assert "independent_groups" not in result
    assert "other_display_name" not in result

    # Verify axis_groups have independent flag and display_name
    for ag_key, group in result["axis_groups"].items():
      assert "display_name" in group
      assert "independent" in group
      if not group["independent"]:
        assert "label" in group

    # Verify independent axis group
    ind = result["axis_groups"]["independent1"]
    assert ind["independent"] is True
    assert ind["display_name"] == "独立軸"
    assert "stock_index" in ind["local_groups"]
    assert "investment_env" in ind["local_groups"]

    # Verify normal axis group with local_groups
    rate = result["axis_groups"]["rate_pct1"]
    assert rate["independent"] is False
    assert "us" in rate["local_groups"]

  def test_get_sources_defaults(self):
    from services.custom_chart_service import get_sources
    result = get_sources()

    sources_by_id = {s["id"]: s for s in result["sources"]}
    assert sources_by_id["target_rate"]["default"] is True
    assert sources_by_id["dgs10"]["default"] is True
    assert sources_by_id["baa10y"]["default"] is False
    assert sources_by_id["sp500"]["default"] is False
    assert sources_by_id["score"]["default"] is False

  def test_get_sources_independent_axis(self):
    from services.custom_chart_service import get_sources
    result = get_sources()

    score = next(s for s in result["sources"] if s["id"] == "score")
    assert score["axis_group"] == "independent1"
    assert score["axis_label"] == "スコア"
    assert score["name"] == "投資環境スコア（堀井）"
    assert score["local_group"] == "investment_env"

    sp500 = next(s for s in result["sources"] if s["id"] == "sp500")
    assert sp500["axis_group"] == "independent1"
    assert sp500["axis_label"] == "USD"
    assert sp500["local_group"] == "stock_index"

  def test_get_sources_local_group(self):
    from services.custom_chart_service import get_sources
    result = get_sources()

    tr = next(s for s in result["sources"] if s["id"] == "target_rate")
    assert tr["local_group"] == "us"

    # Source without local_group should not have the key
    dtw = next(s for s in result["sources"] if s["id"] == "dtwexbgs")
    assert "local_group" not in dtw

  def test_get_sources_normal_axis_label(self):
    from services.custom_chart_service import get_sources
    result = get_sources()

    tr = next(s for s in result["sources"] if s["id"] == "target_rate")
    assert tr["axis_label"] == "%"
    assert tr["axis_group"] == "rate_pct1"

  def test_get_data_from_dynamodb(self, dynamodb_table):
    seed_custom_chart_data(dynamodb_table)

    from services.custom_chart_service import get_data
    with patch("services.custom_chart_service._fetch_recent_for_source", return_value=[]):
      result = get_data(["target_rate", "sp500"])

    assert len(result["series"]) == 2
    tr = next(s for s in result["series"] if s["id"] == "target_rate")
    assert len(tr["data"]) == 2
    assert tr["axis_group"] == "rate_pct1"
    assert tr["axis_label"] == "%"

    sp = next(s for s in result["series"] if s["id"] == "sp500")
    assert len(sp["data"]) == 2
    assert sp["axis_group"] == "independent1"
    assert sp["axis_label"] == "USD"

  def test_get_data_independent_axis_source(self, dynamodb_table):
    seed_custom_chart_data(dynamodb_table)

    from services.custom_chart_service import get_data
    with patch("services.custom_chart_service._fetch_recent_for_source", return_value=[]):
      result = get_data(["score"])

    score = result["series"][0]
    assert score["axis_group"] == "independent1"
    assert score["axis_label"] == "スコア"

  def test_get_data_deduplication(self, dynamodb_table):
    seed_custom_chart_data(dynamodb_table)

    recent = [
      {"time": "2024-01-15", "value": 9999.0},  # duplicate, should be ignored
      {"time": "2024-03-01", "value": 5.5},      # new, should be added
    ]

    from services.custom_chart_service import get_data
    with patch("services.custom_chart_service._fetch_recent_for_source", return_value=recent):
      result = get_data(["target_rate"])

    tr = result["series"][0]
    assert len(tr["data"]) == 3  # 2 stored + 1 new
    jan = next(d for d in tr["data"] if d["time"] == "2024-01-15")
    assert jan["value"] == 5.33  # stored value, not 9999

  def test_get_data_invalid_source(self, dynamodb_table):
    import pytest
    from services.custom_chart_service import get_data
    from common.exceptions import ValidationError

    with pytest.raises(ValidationError, match="Invalid source IDs"):
      get_data(["nonexistent"])
