from conftest import seed_interest_rate_data


class TestFinanceRepository:
  def test_query_by_kind_target_rate(self, dynamodb_table):
    seed_interest_rate_data(dynamodb_table)

    from repositories.finance_repository import query_by_kind
    items = query_by_kind("target_rate")

    assert len(items) == 3
    assert items[0]["time"] == "2024-11-30"
    assert items[0]["value"] == 4.58
    assert items[2]["time"] == "2025-01-31"

  def test_query_by_kind_dgs10(self, dynamodb_table):
    seed_interest_rate_data(dynamodb_table)

    from repositories.finance_repository import query_by_kind
    items = query_by_kind("dgs10")

    assert len(items) == 3
    assert items[0]["time"] == "2024-11-30"
    assert items[0]["value"] == 4.193

  def test_query_by_kind_empty(self, dynamodb_table):
    from repositories.finance_repository import query_by_kind
    items = query_by_kind("nonexistent")

    assert items == []

  def test_query_by_kind_sorted_ascending(self, dynamodb_table):
    seed_interest_rate_data(dynamodb_table)

    from repositories.finance_repository import query_by_kind
    items = query_by_kind("target_rate")

    times = [item["time"] for item in items]
    assert times == sorted(times)
