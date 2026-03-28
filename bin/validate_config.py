#!/usr/bin/env python3
"""Validate custom_chart_sources.json configuration.

Usage:
  python bin/validate_config.py
  python bin/validate_config.py --path path/to/custom_chart_sources.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def validate(config: dict) -> list[str]:
  """Validate custom_chart_sources.json and return a list of error messages."""
  errors: list[str] = []
  axis_groups = config.get("axis_groups", {})
  sources = config.get("sources", {})

  # Validate axis_groups
  for ag_key, ag_def in axis_groups.items():
    # V5: independent is required
    if "independent" not in ag_def:
      errors.append(f"axis_groups.{ag_key}: 'independent' field is required")
      continue

    independent = ag_def["independent"]

    # V3: independent=true must NOT have label
    if independent and "label" in ag_def:
      errors.append(
        f"axis_groups.{ag_key}: independent=true must not have 'label'"
      )

    # V4: independent=false must have label
    if not independent and "label" not in ag_def:
      errors.append(
        f"axis_groups.{ag_key}: independent=false must have 'label'"
      )

  # Validate sources
  for src_key, src_def in sources.items():
    ag_key = src_def.get("axis_group")

    # V1: axis_group reference must exist
    if ag_key not in axis_groups:
      errors.append(
        f"sources.{src_key}: axis_group '{ag_key}' not found in axis_groups"
      )
      continue

    ag_def = axis_groups[ag_key]
    independent = ag_def.get("independent")

    # V6: independent source must have label
    if independent and "label" not in src_def:
      errors.append(
        f"sources.{src_key}: source in independent axis_group '{ag_key}' "
        f"must have 'label'"
      )

    # V7: non-independent source must NOT have label
    if not independent and "label" in src_def:
      errors.append(
        f"sources.{src_key}: source in non-independent axis_group '{ag_key}' "
        f"must not have 'label'"
      )

    # V2: local_group reference must exist
    local_group = src_def.get("local_group")
    if local_group is not None:
      local_groups = ag_def.get("local_groups", {})
      if local_group not in local_groups:
        errors.append(
          f"sources.{src_key}: local_group '{local_group}' not found in "
          f"axis_groups.{ag_key}.local_groups"
        )

  return errors


def main() -> None:
  parser = argparse.ArgumentParser(
    description="Validate custom_chart_sources.json"
  )
  parser.add_argument(
    "--path",
    default=None,
    help="Path to custom_chart_sources.json (default: src/custom_chart_sources.json)",
  )
  args = parser.parse_args()

  if args.path:
    config_path = Path(args.path)
  else:
    config_path = Path(__file__).parent.parent / "src" / "custom_chart_sources.json"

  if not config_path.exists():
    print(f"Error: {config_path} not found", file=sys.stderr)
    sys.exit(1)

  with open(config_path, encoding="utf-8") as f:
    config = json.load(f)

  errors = validate(config)

  if errors:
    print(f"Validation failed with {len(errors)} error(s):", file=sys.stderr)
    for err in errors:
      print(f"  - {err}", file=sys.stderr)
    sys.exit(1)

  print("Validation passed.")


if __name__ == "__main__":
  main()
