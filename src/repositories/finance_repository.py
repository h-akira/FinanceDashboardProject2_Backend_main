from __future__ import annotations

from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key
from aws_lambda_powertools import Tracer

from common import config

tracer = Tracer()

_table = None


def _get_table():
  global _table
  if _table is None:
    dynamodb = boto3.resource("dynamodb")
    _table = dynamodb.Table(config.TABLE_NAME)
  return _table


@tracer.capture_method(capture_response=False)
def query_by_kind(kind: str) -> list[dict]:
  """Query all items for a given KIND.

  Returns items sorted by SK (ascending).
  """
  table = _get_table()
  response = table.query(
    KeyConditionExpression=Key("PK").eq(f"KIND#{kind}"),
    ScanIndexForward=True,
  )
  items = response["Items"]

  # Handle pagination
  while "LastEvaluatedKey" in response:
    response = table.query(
      KeyConditionExpression=Key("PK").eq(f"KIND#{kind}"),
      ScanIndexForward=True,
      ExclusiveStartKey=response["LastEvaluatedKey"],
    )
    items.extend(response["Items"])

  return [
    {
      "time": item["SK"].replace("TIME#", ""),
      "value": float(item["value"]) if isinstance(item["value"], Decimal) else item["value"],
    }
    for item in items
  ]
