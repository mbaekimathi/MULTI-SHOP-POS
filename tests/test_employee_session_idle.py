"""Tests for employee portal inactivity sign-out."""

import json
import time
import unittest
from unittest.mock import patch

from flask import session

from app import (
    _enforce_employee_session_idle_timeout,
    _load_employee_session_idle_settings,
    _normalize_employee_session_idle_settings,
    _request_is_shop_branch_session_view,
    app,
)


def _idle_seconds():
    return _load_employee_session_idle_settings()["idle_seconds"]


class EmployeeSessionIdleTests(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()

    def test_active_session_is_extended_on_request(self):
        base = 1_700_000_000.0
        elapsed = 60.0
        with app.test_request_context("/notifications", method="GET"):
            session["employee_id"] = 7
            session["employee_last_activity"] = base
            with patch("app.time.time", return_value=base + elapsed):
                with patch("app._log_hr_activity_safe"):
                    result = _enforce_employee_session_idle_timeout()
            self.assertIsNone(result)
            self.assertEqual(session["employee_id"], 7)
            self.assertEqual(session["employee_last_activity"], base + elapsed)

    def test_idle_session_is_cleared(self):
        base = 1_700_000_000.0
        expired_after = float(_idle_seconds() + 30)
        with app.test_request_context("/notifications", method="GET"):
            session["employee_id"] = 7
            session["employee_name"] = "Jane Doe"
            session["employee_role"] = "employee"
            session["employee_last_activity"] = base
            with patch("app.time.time", return_value=base + expired_after):
                with patch("app._log_hr_activity_safe") as log_mock:
                    result = _enforce_employee_session_idle_timeout()
            self.assertIsNotNone(result)
            self.assertNotIn("employee_id", session)
            log_mock.assert_called_once()
            self.assertIn("inactivity", log_mock.call_args.kwargs.get("description", ""))

    def test_disabled_idle_setting_skips_enforcement(self):
        base = 1_700_000_000.0
        disabled = _normalize_employee_session_idle_settings({"enabled": False, "idle_minutes": 5})
        with app.test_request_context("/notifications", method="GET"):
            session["employee_id"] = 7
            session["employee_last_activity"] = base
            with patch("app._load_employee_session_idle_settings", return_value=disabled):
                with patch("app.time.time", return_value=base + 9999):
                    with patch("app._log_hr_activity_safe") as log_mock:
                        result = _enforce_employee_session_idle_timeout()
            self.assertIsNone(result)
            self.assertEqual(session["employee_id"], 7)
            log_mock.assert_not_called()

    def test_idle_session_json_response_for_ajax(self):
        stale = time.time() - float(_idle_seconds() + 30)
        headers = {
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/json",
        }
        with self.client.session_transaction() as sess:
            sess["employee_id"] = 3
            sess["employee_name"] = "Pat"
            sess["employee_last_activity"] = stale
        with patch("app._log_hr_activity_safe"):
            resp = self.client.get("/notifications", headers=headers)
        self.assertEqual(resp.status_code, 401)
        data = json.loads(resp.data)
        self.assertFalse(data["ok"])
        self.assertIn("login_url", data)
        self.assertIn("error", data)

    def test_shop_branch_view_skips_idle_enforcement(self):
        base = 1_700_000_000.0
        with app.test_request_context("/shops/12/dashboard", method="GET"):
            session["employee_id"] = 9
            session["shop_id"] = 12
            session["employee_last_activity"] = base
            with patch("app.time.time", return_value=base + 9999):
                with patch("app._log_hr_activity_safe") as log_mock:
                    result = _enforce_employee_session_idle_timeout()
            self.assertIsNone(result)
            self.assertEqual(session["employee_id"], 9)
            log_mock.assert_not_called()

    def test_request_is_shop_branch_session_view_by_path(self):
        with app.test_request_context("/shops/4/analytics"):
            session["shop_id"] = 4
            self.assertTrue(_request_is_shop_branch_session_view())

    def test_activity_ping_extends_session(self):
        recent = time.time() - 120.0
        headers = {
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/json",
        }
        with self.client.session_transaction() as sess:
            sess["employee_id"] = 5
            sess["employee_last_activity"] = recent
        before = time.time()
        resp = self.client.post("/session/employee-activity", headers=headers)
        after = time.time()
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        self.assertTrue(data["ok"])
        self.assertEqual(data["idle_seconds"], _idle_seconds())
        with self.client.session_transaction() as sess:
            self.assertEqual(sess["employee_id"], 5)
            self.assertGreaterEqual(sess["employee_last_activity"], before)
            self.assertLessEqual(sess["employee_last_activity"], after)

    def test_activity_ping_without_employee_returns_401(self):
        headers = {
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/json",
        }
        resp = self.client.post("/session/employee-activity", headers=headers)
        self.assertEqual(resp.status_code, 401)
        data = json.loads(resp.data)
        self.assertFalse(data["ok"])
        self.assertTrue(data.get("expired"))

    def test_normalize_clamps_minutes(self):
        cfg = _normalize_employee_session_idle_settings({"enabled": True, "idle_minutes": 999})
        self.assertEqual(cfg["idle_minutes"], 480)
        cfg = _normalize_employee_session_idle_settings({"enabled": True, "idle_minutes": 0})
        self.assertGreaterEqual(cfg["idle_minutes"], 1)


if __name__ == "__main__":
    unittest.main()
