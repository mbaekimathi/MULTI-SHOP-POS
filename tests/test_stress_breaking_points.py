"""Stress / concurrency checks for hot paths and hardened breaking points."""

from __future__ import annotations

import time
import unittest
from concurrent.futures import ThreadPoolExecutor, as_completed
from unittest.mock import MagicMock, patch

from flask import session

from app import (
    _RATE_LIMIT_BUCKETS,
    _build_storefront_quotation_lines,
    _enforce_employee_session_idle_timeout,
    _invalidate_employee_session_idle_settings_cache,
    _rate_limit_allow,
    app,
)


class IdleHotPathStressTests(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()
        _invalidate_employee_session_idle_settings_cache()

    def tearDown(self):
        _invalidate_employee_session_idle_settings_cache()
        _RATE_LIMIT_BUCKETS.clear()

    def test_anonymous_public_request_skips_idle_settings_db(self):
        with app.test_request_context("/", method="GET"):
            with patch("app._load_employee_session_idle_settings") as load_mock:
                result = _enforce_employee_session_idle_timeout()
            self.assertIsNone(result)
            load_mock.assert_not_called()

    def test_idle_settings_are_ttl_cached(self):
        from app import _load_employee_session_idle_settings

        _invalidate_employee_session_idle_settings_cache()
        with patch(
            "database.get_site_settings",
            return_value={"employee_session_idle_json": '{"enabled":true,"idle_minutes":5}'},
        ) as gs:
            a = _load_employee_session_idle_settings()
            b = _load_employee_session_idle_settings()
        self.assertEqual(a["idle_minutes"], 5)
        self.assertEqual(b["idle_minutes"], 5)
        self.assertEqual(gs.call_count, 1)

    def test_concurrent_anonymous_hits_do_not_load_idle_settings(self):
        calls = {"n": 0}

        def counting_load():
            calls["n"] += 1
            return {"enabled": True, "idle_minutes": 5, "idle_seconds": 300}

        def one_hit(_i):
            with app.test_request_context("/", method="GET"):
                with patch("app._load_employee_session_idle_settings", side_effect=counting_load):
                    return _enforce_employee_session_idle_timeout()

        with ThreadPoolExecutor(max_workers=16) as pool:
            futs = [pool.submit(one_hit, i) for i in range(64)]
            results = [f.result() for f in as_completed(futs)]
        self.assertTrue(all(r is None for r in results))
        self.assertEqual(calls["n"], 0)


class RateLimitStressTests(unittest.TestCase):
    def setUp(self):
        _RATE_LIMIT_BUCKETS.clear()

    def tearDown(self):
        _RATE_LIMIT_BUCKETS.clear()

    def test_rate_limit_blocks_after_threshold(self):
        key = "stress:test-ip"
        for _ in range(5):
            self.assertTrue(_rate_limit_allow(key, limit=5, window_sec=60.0))
        self.assertFalse(_rate_limit_allow(key, limit=5, window_sec=60.0))

    def test_customer_lookup_returns_429_under_storm(self):
        client = app.test_client()
        with patch("database.lookup_storefront_customer_by_phone", return_value=None):
            statuses = []
            for _ in range(45):
                resp = client.post(
                    "/api/storefront/customer-lookup",
                    json={"phone": "0712345678"},
                    headers={"Content-Type": "application/json"},
                )
                statuses.append(resp.status_code)
        self.assertIn(429, statuses)
        self.assertTrue(any(s == 200 for s in statuses))


class QuotationCatalogStressTests(unittest.TestCase):
    def test_quotation_accepts_non_featured_catalog_ids(self):
        rows = [
            {"id": 101, "name": "Catalog Lamp", "price": 1500.0},
            {"id": 202, "name": "Catalog Fan", "price": 2500.0},
        ]
        with patch("database.list_website_products_by_ids", return_value=rows) as by_ids:
            lines, total, count, err = _build_storefront_quotation_lines(
                [{"id": 101, "qty": 2}, {"id": 202, "qty": 1}]
            )
        self.assertIsNone(err)
        self.assertEqual(count, 3)
        self.assertEqual(total, 5500.0)
        self.assertEqual(len(lines), 2)
        by_ids.assert_called_once()
        self.assertEqual(by_ids.call_args.args[0], [101, 202])


class SchemaCacheStressTests(unittest.TestCase):
    def test_table_exists_hits_db_once_per_name(self):
        import database as db

        db.clear_schema_existence_cache()
        fake_cur = MagicMock()
        fake_cur.fetchone.return_value = {"1": 1}
        fake_cm = MagicMock()
        fake_cm.__enter__.return_value = fake_cur
        fake_cm.__exit__.return_value = False
        with patch.object(db, "get_cursor", return_value=fake_cm) as gc:
            self.assertTrue(db.table_exists("items"))
            self.assertTrue(db.table_exists("items"))
            self.assertTrue(db.table_exists("items"))
        self.assertEqual(gc.call_count, 1)
        db.clear_schema_existence_cache()

    def test_list_shops_ensures_columns_once(self):
        import database as db

        db._SHOP_COLUMNS_ENSURED = False
        calls = {"n": 0}

        def counting_ensure_loc():
            calls["n"] += 1
            return True

        fake_cur = MagicMock()
        fake_cur.fetchall.return_value = []
        fake_cm = MagicMock()
        fake_cm.__enter__.return_value = fake_cur
        fake_cm.__exit__.return_value = False
        with patch.object(db, "ensure_shop_location_description_column", side_effect=counting_ensure_loc):
            with patch.object(db, "ensure_shop_phone_column", return_value=True):
                with patch.object(db, "get_cursor", return_value=fake_cm):
                    db.list_shops(limit=10)
                    db.list_shops(limit=10)
                    db.list_shops(limit=10)
        self.assertEqual(calls["n"], 1)
        db._SHOP_COLUMNS_ENSURED = False


class ConcurrentPublicEndpointSmokeTests(unittest.TestCase):
    """Hammer public JSON with mocked DB — assert no 5xx under concurrency."""

    def test_products_json_survives_parallel_reads(self):
        client = app.test_client()

        def one(_i):
            with patch("database.list_website_catalog_items", return_value=[]):
                return client.get("/api/storefront/products.json")

        statuses = []
        with ThreadPoolExecutor(max_workers=12) as pool:
            futs = [pool.submit(one, i) for i in range(36)]
            for f in as_completed(futs):
                resp = f.result()
                statuses.append(resp.status_code)
        self.assertTrue(all(s < 500 for s in statuses), statuses)
        self.assertTrue(all(s == 200 for s in statuses), statuses)


if __name__ == "__main__":
    unittest.main()
