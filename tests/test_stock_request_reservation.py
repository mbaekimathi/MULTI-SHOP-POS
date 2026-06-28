"""Tests for stock request soft-reservation helpers."""

import unittest

from database import (
    STOCK_QTY_DECIMAL_PLACES,
    _admin_stock_approval_queue_sql,
    _available_qty_after_pending,
)


class StockRequestReservationTests(unittest.TestCase):
    def test_available_qty_after_pending_never_negative(self):
        self.assertEqual(_available_qty_after_pending(10.0, 3.5), 6.5)
        self.assertEqual(_available_qty_after_pending(5.0, 12.0), 0.0)
        self.assertEqual(_available_qty_after_pending(0.0, 0.0), 0.0)

    def test_available_qty_respects_decimal_places(self):
        got = _available_qty_after_pending(1.33339, 0.0004)
        self.assertEqual(got, round(1.33339 - 0.0004, STOCK_QTY_DECIMAL_PLACES))

    def test_admin_approval_queue_sql_targets_company_and_returns(self):
        sql = _admin_stock_approval_queue_sql(alias="r")
        self.assertIn("return_to_company", sql)
        self.assertIn("source_type", sql)
        self.assertIn("r.", sql)


if __name__ == "__main__":
    unittest.main()
