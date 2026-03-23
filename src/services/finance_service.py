from __future__ import annotations

import datetime

import pandas as pd
import pandas_datareader.data as web
from aws_lambda_powertools import Logger, Tracer

from common.exceptions import ExternalServiceError
from repositories import finance_repository

tracer = Tracer()
logger = Logger()

FRED_BOUNDARY = datetime.datetime(2026, 1, 1)


def _fetch_fred_series(series_id: str, start: datetime.datetime, end: datetime.datetime) -> pd.Series:
  """Fetch a single FRED series and sample on the 1st and 15th of each month
  (or next available business day), plus the latest available data point."""
  df: pd.DataFrame = web.DataReader([series_id], "fred", start, end)
  daily: pd.Series = df[series_id].ffill().dropna()
  if daily.empty:
    return daily

  # Semi-monthly sampling (1st and 15th or next available business day)
  targets = []
  for year in range(daily.index[0].year, daily.index[-1].year + 1):
    for month in range(1, 13):
      for day in (1, 15):
        try:
          d = pd.Timestamp(year, month, day)
        except ValueError:
          continue
        loc = daily.index.searchsorted(d)
        if loc < len(daily.index):
          targets.append(daily.index[loc])
  targets = sorted(set(targets))
  sampled = daily.loc[daily.index.isin(targets)]

  # Append the latest data point
  latest_idx = daily.index[-1]
  if latest_idx not in sampled.index:
    sampled = pd.concat([sampled, daily.iloc[[-1]]])

  return sampled


@tracer.capture_method(capture_response=False)
def _fetch_fred_recent() -> list[dict]:
  """Fetch recent data from FRED (2026-01-01 onwards)."""
  end = datetime.datetime.now()
  if end < FRED_BOUNDARY:
    return []

  try:
    # DFEDTARU: upper limit of target range (from 2008-12-16)
    target_rate = _fetch_fred_series("DFEDTARU", FRED_BOUNDARY, end)
    dgs10 = _fetch_fred_series("DGS10", FRED_BOUNDARY, end)
  except Exception as e:
    logger.error("Failed to fetch FRED data", error=str(e))
    raise ExternalServiceError("Failed to fetch data from FRED")

  combined = pd.concat([target_rate, dgs10], axis=1)
  combined.columns = ["target_rate", "dgs10"]
  combined = combined.ffill().dropna()

  return [
    {
      "time": index.strftime("%Y-%m-%d"),
      "target_rate": round(float(row["target_rate"]), 4),
      "dgs10": round(float(row["dgs10"]), 4),
    }
    for index, row in combined.iterrows()
  ]


@tracer.capture_method(capture_response=False)
def get_interest_rate() -> dict:
  """Get interest rate data by combining DynamoDB stored data and FRED recent data."""
  # Query DynamoDB for stored data
  target_rate_items = finance_repository.query_by_kind("target_rate")
  dgs10_items = finance_repository.query_by_kind("dgs10")

  # Merge by time
  target_rate_map = {item["time"]: item["value"] for item in target_rate_items}
  dgs10_map = {item["time"]: item["value"] for item in dgs10_items}

  all_times = sorted(set(target_rate_map.keys()) | set(dgs10_map.keys()))
  stored_data = [
    {
      "time": t,
      "target_rate": target_rate_map.get(t, 0.0),
      "dgs10": dgs10_map.get(t, 0.0),
    }
    for t in all_times
    if t in target_rate_map and t in dgs10_map
  ]

  # Fetch recent data from FRED
  recent_data = _fetch_fred_recent()

  # Deduplicate: stored data takes precedence
  stored_times = {d["time"] for d in stored_data}
  new_data = [d for d in recent_data if d["time"] not in stored_times]

  data = stored_data + new_data

  return {"data": data}
