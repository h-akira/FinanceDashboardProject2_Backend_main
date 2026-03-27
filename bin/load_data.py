#!/usr/bin/env python3
"""Load financial data from FRED/yfinance into DynamoDB.

Reads source definitions from src/custom_chart_sources.json and loads
semi-monthly data for each source into DynamoDB.

Usage:
  python bin/load_data.py --env dev
  python bin/load_data.py --env dev --sources target_rate,dgs10
  python bin/load_data.py --env dev --dry-run
  python bin/load_data.py --env dev --remove-all
  python bin/load_data.py --env dev --remove-all --sources sp500
"""

from __future__ import annotations

import argparse
import datetime
import json
from decimal import Decimal
from pathlib import Path

import boto3
import boto3.dynamodb.conditions
import pandas as pd
import pandas_datareader.data as web
import yfinance as yf

PROJECT = "fdp"
END = datetime.datetime(2025, 12, 31)
CONFIG_PATH = Path(__file__).parent / ".." / "src" / "custom_chart_sources.json"


def _load_config() -> dict:
  with open(CONFIG_PATH, encoding="utf-8") as f:
    return json.load(f)


def _resample_semi_monthly(series: pd.Series) -> pd.Series:
  """Sample on the 1st and 15th of each month (or next available business day)."""
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
  return daily.loc[daily.index.isin(targets)]


def _fetch_target_rate() -> pd.Series:
  """Fetch federal funds target rate (upper limit).

  DFEDTAR: single target (until 2008-12-15)
  DFEDTARU: upper limit of target range (from 2008-12-16)
  """
  dfedtar: pd.DataFrame = web.DataReader(
    ["DFEDTAR"], "fred", datetime.datetime(1982, 9, 27), datetime.datetime(2008, 12, 16)
  )
  dfedtaru: pd.DataFrame = web.DataReader(
    ["DFEDTARU"], "fred", datetime.datetime(2008, 12, 16), END
  )
  combined: pd.Series = pd.concat([
    dfedtar["DFEDTAR"],
    dfedtaru["DFEDTARU"],
  ])
  combined.name = "TARGET_RATE"
  return combined


def _fetch_fred_single(series_id: str, start: str) -> pd.Series:
  """Fetch a single FRED series."""
  start_dt = datetime.datetime.fromisoformat(start)
  df: pd.DataFrame = web.DataReader([series_id], "fred", start_dt, END)
  return df[series_id]


def _fetch_yfinance(ticker: str, start: str) -> pd.Series:
  """Fetch closing price from yfinance."""
  start_dt = datetime.datetime.fromisoformat(start)
  raw: pd.DataFrame = yf.download(ticker, start=start_dt, end=END, progress=False)
  return raw["Close"].squeeze()


def _fetch_yfinance_yoy(ticker: str, start: str) -> pd.Series:
  """Fetch YoY ratio from yfinance."""
  start_dt = datetime.datetime.fromisoformat(start)
  # Need 1 year lookback for YoY calculation
  lookback_start = start_dt - datetime.timedelta(days=400)
  raw: pd.DataFrame = yf.download(ticker, start=lookback_start, end=END, progress=False)
  if raw.empty:
    return pd.Series(dtype=float)
  close = raw["Close"].squeeze()
  yoy = close / close.shift(365, freq="D")
  yoy = yoy.dropna()
  yoy = yoy[yoy.index >= pd.Timestamp(start_dt)]
  return yoy


def _fetch_score(start: str) -> pd.Series:
  """Fetch composite score."""
  start_dt = datetime.datetime.fromisoformat(start)
  lookback_start = start_dt - datetime.timedelta(days=400)
  fred_data: pd.DataFrame = web.DataReader(
    ["EFFR", "DGS10", "BAA10Y", "DTWEXBGS"], "fred", lookback_start, END
  )
  data = fred_data.ffill().dropna()

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
  score_series = data["score"]
  score_series = score_series[score_series.index >= pd.Timestamp(start_dt)]
  return score_series


def _fetch_source(source_id: str, source_def: dict) -> pd.Series:
  """Fetch data for a single source based on its definition."""
  start = source_def["start"]
  source_type = source_def.get("source_type", "fred")

  if source_id == "target_rate":
    return _fetch_target_rate()
  elif source_type == "fred":
    fred_series = source_def["fred_series"]
    return _fetch_fred_single(fred_series[0], start)
  elif source_type == "yfinance":
    ticker = source_def["ticker"]
    transform = source_def.get("transform")
    if transform == "yoy_ratio":
      return _fetch_yfinance_yoy(ticker, start)
    else:
      return _fetch_yfinance(ticker, start)
  elif source_type == "calculated" and source_id == "score":
    return _fetch_score(start)
  else:
    raise ValueError(f"Unknown source type: {source_type} for {source_id}")


def _print_items(kind: str, series: pd.Series) -> None:
  """Print items to stdout (dry-run mode)."""
  for index, value in series.items():
    print(f"  PK=KIND#{kind}  SK=TIME#{index.strftime('%Y-%m-%d')}  value={round(float(value), 4)}")


def _put_items(table, kind: str, series: pd.Series) -> int:
  """Write data to DynamoDB using batch_writer."""
  count = 0
  with table.batch_writer() as batch:
    for index, value in series.items():
      batch.put_item(Item={
        "PK": f"KIND#{kind}",
        "SK": f"TIME#{index.strftime('%Y-%m-%d')}",
        "value": Decimal(str(round(float(value), 4))),
      })
      count += 1
  return count


def _remove_kind(table, kind: str) -> int:
  """Remove all items for a given KIND."""
  pk = f"KIND#{kind}"
  count = 0
  query_kwargs = {
    "KeyConditionExpression": boto3.dynamodb.conditions.Key("PK").eq(pk),
    "ProjectionExpression": "PK, SK",
  }
  while True:
    resp = table.query(**query_kwargs)
    with table.batch_writer() as batch:
      for item in resp["Items"]:
        batch.delete_item(Key={"PK": item["PK"], "SK": item["SK"]})
        count += 1
    if "LastEvaluatedKey" not in resp:
      break
    query_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
  return count


def main() -> None:
  config = _load_config()
  all_source_ids = list(config["sources"].keys())

  parser = argparse.ArgumentParser(description="Load financial data into DynamoDB")
  parser.add_argument("--env", required=True, choices=["dev", "pro"], help="Environment (dev, pro)")
  parser.add_argument("--region", default="ap-northeast-1", help="AWS region (default: ap-northeast-1)")
  parser.add_argument(
    "--sources",
    default=None,
    help=f"Comma-separated source IDs to load (default: all). Available: {', '.join(all_source_ids)}",
  )
  parser.add_argument("--remove-all", action="store_true", help="Remove data for specified sources and exit")
  parser.add_argument("--dry-run", action="store_true", help="Print data to stdout instead of writing to DynamoDB")
  args = parser.parse_args()

  if args.remove_all and args.dry_run:
    parser.error("--remove-all and --dry-run cannot be used together")

  # Determine which sources to process
  if args.sources:
    source_ids = [s.strip() for s in args.sources.split(",")]
    invalid = [s for s in source_ids if s not in config["sources"]]
    if invalid:
      parser.error(f"Invalid source IDs: {', '.join(invalid)}. Available: {', '.join(all_source_ids)}")
  else:
    source_ids = all_source_ids

  table_name = f"table-{PROJECT}-{args.env}-backend-main"
  print(f"Target table: {table_name}")
  print(f"Sources: {', '.join(source_ids)}")

  dynamodb = boto3.resource("dynamodb", region_name=args.region)
  table = dynamodb.Table(table_name)

  if args.remove_all:
    print("Removing data...")
    for source_id in source_ids:
      count = _remove_kind(table, source_id)
      print(f"  {source_id}: {count} items deleted")
    print("Done.")
    return

  for source_id in source_ids:
    source_def = config["sources"][source_id]
    print(f"Fetching {source_id} ({source_def['name']})...")
    raw = _fetch_source(source_id, source_def)
    resampled = _resample_semi_monthly(raw)
    print(f"  {len(resampled)} records")

    if args.dry_run:
      _print_items(source_id, resampled)
    else:
      count = _put_items(table, source_id, resampled)
      print(f"  {count} items written")

  print("Done." + (" (dry-run)" if args.dry_run else ""))


if __name__ == "__main__":
  main()
