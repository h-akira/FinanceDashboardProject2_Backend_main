from __future__ import annotations

import datetime
import json
from pathlib import Path

import pandas as pd
import pandas_datareader.data as web
import yfinance as yf
from aws_lambda_powertools import Logger, Tracer

from common.exceptions import ExternalServiceError, ValidationError
from repositories import finance_repository

tracer = Tracer()
logger = Logger()

FRED_BOUNDARY = datetime.datetime(2026, 1, 1)

_config: dict | None = None


def _load_config() -> dict:
  global _config
  if _config is None:
    config_path = Path(__file__).parent.parent / "custom_chart_sources.json"
    with open(config_path, encoding="utf-8") as f:
      _config = json.load(f)
  return _config


def get_sources() -> dict:
  """Return available sources and max_axes for the frontend checklist."""
  config = _load_config()
  sources = []
  for source_id, source_def in config["sources"].items():
    axis_group = source_def["axis_group"]
    axis_label = config["axis_groups"][axis_group]["label"]
    sources.append({
      "id": source_id,
      "name": source_def["name"],
      "axis_group": axis_group,
      "axis_label": axis_label,
    })
  return {
    "sources": sources,
    "max_axes": config["max_axes"],
  }


def _resample_semi_monthly(series: pd.Series) -> pd.Series:
  """Sample on the 1st and 15th of each month (or next available business day),
  plus the latest available data point."""
  daily = series.ffill().dropna()
  if daily.empty:
    return daily

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

  latest_idx = daily.index[-1]
  if latest_idx not in sampled.index:
    sampled = pd.concat([sampled, daily.iloc[[-1]]])

  return sampled


def _fetch_fred_series_recent(series_id: str) -> pd.Series:
  """Fetch a FRED series from FRED_BOUNDARY to now with semi-monthly resampling."""
  end = datetime.datetime.now()
  if end < FRED_BOUNDARY:
    return pd.Series(dtype=float)
  df = web.DataReader([series_id], "fred", FRED_BOUNDARY, end)
  return _resample_semi_monthly(df[series_id])


def _fetch_target_rate_recent() -> pd.Series:
  """Fetch recent target rate (DFEDTARU only, post-2008)."""
  return _fetch_fred_series_recent("DFEDTARU")


def _fetch_yfinance_recent(ticker: str) -> pd.Series:
  """Fetch recent price data from yfinance with semi-monthly resampling."""
  end = datetime.datetime.now()
  if end < FRED_BOUNDARY:
    return pd.Series(dtype=float)
  raw = yf.download(ticker, start=FRED_BOUNDARY, end=end, progress=False)
  if raw.empty:
    return pd.Series(dtype=float)
  close = raw["Close"].squeeze()
  return _resample_semi_monthly(close)


def _fetch_yfinance_yoy_recent(ticker: str) -> pd.Series:
  """Fetch recent YoY ratio from yfinance with semi-monthly resampling."""
  end = datetime.datetime.now()
  if end < FRED_BOUNDARY:
    return pd.Series(dtype=float)
  # Need 1 year lookback for YoY calculation
  lookback_start = FRED_BOUNDARY - datetime.timedelta(days=400)
  raw = yf.download(ticker, start=lookback_start, end=end, progress=False)
  if raw.empty:
    return pd.Series(dtype=float)
  close = raw["Close"].squeeze()
  yoy = close / close.shift(365, freq="D")
  yoy = yoy.dropna()
  # Filter to FRED_BOUNDARY onwards
  yoy = yoy[yoy.index >= pd.Timestamp(FRED_BOUNDARY)]
  return _resample_semi_monthly(yoy)


def _fetch_score_recent() -> pd.Series:
  """Fetch recent composite score."""
  end = datetime.datetime.now()
  if end < FRED_BOUNDARY:
    return pd.Series(dtype=float)
  lookback_start = FRED_BOUNDARY - datetime.timedelta(days=400)

  try:
    fred_data = web.DataReader(
      ["EFFR", "DGS10", "BAA10Y", "DTWEXBGS"], "fred", lookback_start, end
    )
  except Exception as e:
    logger.error("Failed to fetch FRED data for score", error=str(e))
    raise ExternalServiceError("Failed to fetch data from FRED")

  data = fred_data.ffill().dropna()
  if data.empty:
    return pd.Series(dtype=float)

  data["EFFR_diff"] = data["EFFR"] - data["EFFR"].shift(365, freq="D")
  data["DSG10_EFFR_diff"] = data["DGS10"] - data["EFFR"]
  data["DGS10_diff"] = data["DGS10"] - data["DGS10"].shift(365, freq="D")
  data["BAA10Y_diff"] = data["BAA10Y"] - data["BAA10Y"].shift(365, freq="D")
  data["DTWEXBGS_rate"] = data["DTWEXBGS"] / data["DTWEXBGS"].shift(365, freq="D")
  data = data.dropna()

  def _cal_score(row: pd.Series) -> int:
    score = 0
    if row["EFFR_diff"] <= 0.25:
      score += 2
    else:
      score -= 2
    if row["DSG10_EFFR_diff"] >= 1:
      score += 2
    elif row["DSG10_EFFR_diff"] >= 0:
      score += 0
    else:
      score -= 2
    if row["DGS10_diff"] >= 0:
      score += 2
    else:
      score -= 2
    if row["BAA10Y_diff"] <= 0:
      score += 2
    else:
      score -= 2
    if row["DTWEXBGS_rate"] <= 1:
      score += 2
    else:
      score -= 2
    return score

  data["score"] = data.apply(_cal_score, axis=1)

  # Filter to FRED_BOUNDARY onwards
  score_series = data["score"]
  score_series = score_series[score_series.index >= pd.Timestamp(FRED_BOUNDARY)]
  return _resample_semi_monthly(score_series)


def _fetch_recent_for_source(source_id: str, source_def: dict) -> list[dict]:
  """Fetch recent data (2026-01-01 onwards) for a single source."""
  end = datetime.datetime.now()
  if end < FRED_BOUNDARY:
    return []

  try:
    source_type = source_def.get("source_type", "fred")

    if source_id == "target_rate":
      series = _fetch_target_rate_recent()
    elif source_type == "fred":
      fred_series = source_def["fred_series"]
      series = _fetch_fred_series_recent(fred_series[0])
    elif source_type == "yfinance":
      ticker = source_def["ticker"]
      transform = source_def.get("transform")
      if transform == "yoy_ratio":
        series = _fetch_yfinance_yoy_recent(ticker)
      else:
        series = _fetch_yfinance_recent(ticker)
    elif source_type == "calculated" and source_id == "score":
      series = _fetch_score_recent()
    else:
      return []
  except ExternalServiceError:
    raise
  except Exception as e:
    logger.error("Failed to fetch recent data", source_id=source_id, error=str(e))
    raise ExternalServiceError(f"Failed to fetch data for {source_id}")

  return [
    {"time": idx.strftime("%Y-%m-%d"), "value": round(float(val), 4)}
    for idx, val in series.items()
  ]


@tracer.capture_method(capture_response=False)
def get_data(source_ids: list[str]) -> dict:
  """Get time series data for the requested sources."""
  config = _load_config()
  valid_sources = config["sources"]

  # Validate source IDs
  invalid = [s for s in source_ids if s not in valid_sources]
  if invalid:
    raise ValidationError(f"Invalid source IDs: {', '.join(invalid)}")

  series_list = []
  for source_id in source_ids:
    source_def = valid_sources[source_id]
    axis_group = source_def["axis_group"]
    axis_label = config["axis_groups"][axis_group]["label"]

    # Get stored data from DynamoDB
    stored_items = finance_repository.query_by_kind(source_id)

    # Get recent data from external APIs
    recent_items = _fetch_recent_for_source(source_id, source_def)

    # Deduplicate: stored data takes precedence
    stored_times = {item["time"] for item in stored_items}
    new_items = [item for item in recent_items if item["time"] not in stored_times]
    all_items = stored_items + new_items

    series_list.append({
      "id": source_id,
      "name": source_def["name"],
      "axis_group": axis_group,
      "axis_label": axis_label,
      "data": all_items,
    })

  return {"series": series_list}
