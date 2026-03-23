#!/usr/bin/env python3
"""Load interest rate data from FRED into DynamoDB.

Usage:
  python bin/load_interest_rate.py --env dev
  python bin/load_interest_rate.py --env pro
"""

from __future__ import annotations

import argparse
import datetime
from decimal import Decimal

import boto3
import pandas as pd
import pandas_datareader.data as web

PROJECT = "fdp"
END = datetime.datetime(2025, 12, 31)


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


def _fetch_dgs10() -> pd.Series:
  """Fetch 10-Year Treasury Constant Maturity Rate."""
  df: pd.DataFrame = web.DataReader(
    ["DGS10"], "fred", datetime.datetime(1982, 9, 27), END
  )
  return df["DGS10"]


def _resample_semi_monthly(series: pd.Series) -> pd.Series:
  """Sample on the 1st and 15th of each month (or next available business day)."""
  daily = series.ffill().dropna()
  targets = []
  for year in range(daily.index[0].year, daily.index[-1].year + 1):
    for month in range(1, 13):
      for day in (1, 15):
        try:
          d = pd.Timestamp(year, month, day)
        except ValueError:
          continue
        # Find the nearest business day on or after the target date
        loc = daily.index.searchsorted(d)
        if loc < len(daily.index):
          targets.append(daily.index[loc])
  targets = sorted(set(targets))
  return daily.loc[daily.index.isin(targets)]


def _print_items(kind: str, series: pd.Series) -> None:
  """Print items to stdout (dry-run mode)."""
  for index, value in series.items():
    print(f"  PK=KIND#{kind}  SK=TIME#{index.strftime('%Y-%m-%d')}  value={round(float(value), 4)}")


def _put_items(table, kind: str, monthly: pd.Series) -> int:
  """Write monthly data to DynamoDB using batch_writer."""
  count = 0
  with table.batch_writer() as batch:
    for index, value in monthly.items():
      batch.put_item(Item={
        "PK": f"KIND#{kind}",
        "SK": f"TIME#{index.strftime('%Y-%m-%d')}",
        "value": Decimal(str(round(float(value), 4))),
      })
      count += 1
  return count


def main() -> None:
  parser = argparse.ArgumentParser(description="Load interest rate data into DynamoDB")
  parser.add_argument("--env", required=True, choices=["dev", "pro"], help="Environment (dev, pro)")
  parser.add_argument("--region", default="ap-northeast-1", help="AWS region (default: ap-northeast-1)")
  parser.add_argument("--remove-all", action="store_true", help="Remove all interest rate data and exit")
  parser.add_argument("--dry-run", action="store_true", help="Print data to stdout instead of writing to DynamoDB")
  args = parser.parse_args()

  if args.remove_all and args.dry_run:
    parser.error("--remove-all and --dry-run cannot be used together")

  table_name = f"table-{PROJECT}-{args.env}-backend-main"
  print(f"Target table: {table_name}")

  dynamodb = boto3.resource("dynamodb", region_name=args.region)
  table = dynamodb.Table(table_name)

  if args.remove_all:
    print("Removing all interest rate data...")
    count = 0
    for kind in ("target_rate", "dgs10"):
      pk = f"KIND#{kind}"
      query_kwargs = {"KeyConditionExpression": boto3.dynamodb.conditions.Key("PK").eq(pk), "ProjectionExpression": "PK, SK"}
      while True:
        resp = table.query(**query_kwargs)
        with table.batch_writer() as batch:
          for item in resp["Items"]:
            batch.delete_item(Key={"PK": item["PK"], "SK": item["SK"]})
            count += 1
        if "LastEvaluatedKey" not in resp:
          break
        query_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    print(f"  {count} items deleted")
    print("Done.")
    return

  # Fetch data from FRED
  print("Fetching target rate from FRED...")
  target_rate = _resample_semi_monthly(_fetch_target_rate())
  print(f"  {len(target_rate)} records")

  print("Fetching DGS10 from FRED...")
  dgs10 = _resample_semi_monthly(_fetch_dgs10())
  print(f"  {len(dgs10)} records")

  if args.dry_run:
    print("target_rate:")
    _print_items("target_rate", target_rate)
    print("dgs10:")
    _print_items("dgs10", dgs10)
    print("Done. (dry-run)")
    return

  # Write to DynamoDB
  table = dynamodb.Table(table_name)

  print("Writing target_rate to DynamoDB...")
  count = _put_items(table, "target_rate", target_rate)
  print(f"  {count} items written")

  print("Writing dgs10 to DynamoDB...")
  count = _put_items(table, "dgs10", dgs10)
  print(f"  {count} items written")

  print("Done.")


if __name__ == "__main__":
  main()
