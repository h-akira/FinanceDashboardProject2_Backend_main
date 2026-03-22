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


def _resample_monthly(series: pd.Series) -> pd.Series:
  """Forward-fill, resample to monthly (last value), drop NaN."""
  return series.ffill().dropna().resample("ME").last().dropna()


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
  args = parser.parse_args()

  table_name = f"table-{PROJECT}-{args.env}-backend-main"
  print(f"Target table: {table_name}")

  # Fetch data from FRED
  print("Fetching target rate from FRED...")
  target_rate = _resample_monthly(_fetch_target_rate())
  print(f"  {len(target_rate)} records")

  print("Fetching DGS10 from FRED...")
  dgs10 = _resample_monthly(_fetch_dgs10())
  print(f"  {len(dgs10)} records")

  # Write to DynamoDB
  dynamodb = boto3.resource("dynamodb")
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
