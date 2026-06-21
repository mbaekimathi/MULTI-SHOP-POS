#!/usr/bin/env python3
"""One-time FIFO COGS backfill for historical shop stock outs."""

from __future__ import annotations

import argparse
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from database import (  # noqa: E402
    backfill_shop_stock_fifo_historical_cogs,
    ensure_shop_stock_fifo_schema,
    init_schema,
    set_site_settings,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Replay shop stock history and assign FIFO cost_total on outs."
    )
    parser.add_argument(
        "--shop-id",
        type=int,
        default=None,
        help="Limit replay to one shop (default: all shops)",
    )
    parser.add_argument(
        "--skip-schema-init",
        action="store_true",
        help="Do not run init_schema() before backfill",
    )
    parser.add_argument(
        "--mark-done",
        action="store_true",
        help="Set site setting so startup will not rerun automatically",
    )
    args = parser.parse_args()

    if not args.skip_schema_init:
        if not init_schema():
            print("Schema init failed.", file=sys.stderr)
            return 1
    elif not ensure_shop_stock_fifo_schema():
        print("FIFO schema not ready.", file=sys.stderr)
        return 1

    result = backfill_shop_stock_fifo_historical_cogs(
        shop_id=args.shop_id,
        clear_existing=True,
    )
    print(json.dumps(result, indent=2))
    if not result.get("ok"):
        return 1
    if args.mark_done:
        set_site_settings({"shop_fifo_historical_cogs_v1": "done"})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
