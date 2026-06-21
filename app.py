import calendar
import csv
import hmac
import io
import json
import logging
import os
import re
import secrets
import socket
import threading
import time
import uuid
from datetime import date, datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from urllib.parse import quote, urlparse

from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

import project_env  # noqa: F401 — loads .env.example then .env before os.getenv below
from flask import (
    Flask,
    jsonify,
    Response,
    abort,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    send_from_directory,
    session,
    stream_with_context,
    url_for,
)
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename

logger = logging.getLogger(__name__)

# Portal footer "Powered by JOS" — override with env JOS_VERSION
JOS_VERSION = (os.getenv("JOS_VERSION") or "1.0.0").strip()

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-secret-change-me")
app.config["MAX_CONTENT_LENGTH"] = 5 * 1024 * 1024  # 5 MB uploads

# ngrok and other reverse proxies forward HTTP to Flask; trust X-Forwarded-* so
# url_for(..., _external=True) and request.url_root use the public HTTPS URL.
if (os.getenv("TRUST_PROXY_HEADERS") or "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}:
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

# Long-lived "remember this device" sessions so the till stays signed in across
# restarts and brief offline periods. Cached pages render in an authenticated
# state because the session cookie still validates once the network returns.
_SESSION_DAYS = int(os.getenv("SESSION_LIFETIME_DAYS", "90") or "90")
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=max(1, _SESSION_DAYS))
_EMPLOYEE_IDLE_MINUTES = max(1, int(os.getenv("EMPLOYEE_SESSION_IDLE_MINUTES", "5") or "5"))
EMPLOYEE_SESSION_IDLE_SECONDS = _EMPLOYEE_IDLE_MINUTES * 60
_EMPLOYEE_IDLE_WARN_SECONDS_RAW = int(os.getenv("EMPLOYEE_SESSION_IDLE_WARN_SECONDS", "60") or "60")
EMPLOYEE_SESSION_IDLE_WARN_SECONDS = max(
    15,
    min(_EMPLOYEE_IDLE_WARN_SECONDS_RAW, max(EMPLOYEE_SESSION_IDLE_SECONDS - 15, 15)),
)
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
# Only set Secure when explicitly told the deployment is on HTTPS, so localhost
# dev (http) still works without a custom env file.
app.config["SESSION_COOKIE_SECURE"] = (
    (os.getenv("SESSION_COOKIE_SECURE") or "").strip().lower() in {"1", "true", "yes", "on"}
)

_STATIC_CACHE_MAX_AGE = int(os.getenv("STATIC_CACHE_MAX_AGE", "604800") or "604800")


@app.after_request
def _add_static_cache_headers(response):
    """Long-cache versioned static assets; skip in debug to ease local iteration."""
    if request.path.startswith("/static/") and not app.debug:
        response.cache_control.public = True
        response.cache_control.max_age = max(3600, _STATIC_CACHE_MAX_AGE)
        response.cache_control.immutable = True
    return response

UPLOAD_FOLDER_REL = "uploads/profiles"
ALLOWED_PROFILE_EXT = {"png", "jpg", "jpeg", "gif", "webp"}
ALLOWED_APP_ICON_EXT = {"png", "jpg", "jpeg", "gif", "webp", "ico", "svg"}

CODE_RE = re.compile(r"^\d{6}$")
PASSWORD_DIGITS_RE = re.compile(r"^\d{6,32}$")
PASSWORD_RESET_RE = re.compile(r"^.{6,128}$")
RESET_CODE_RE = re.compile(r"^\d{6}$")

SIGNUP_PHONE_COUNTRIES: tuple[dict, ...] = (
    {
        "code": "254",
        "iso2": "ke",
        "label": "Kenya",
        "flag": "🇰🇪",
        "placeholder": "712345678",
        "hint": "Kenya: enter 07xxxxxxxx or 712345678 — saved as 2547xxxxxxxx.",
    },
    {
        "code": "256",
        "iso2": "ug",
        "label": "Uganda",
        "flag": "🇺🇬",
        "placeholder": "712345678",
        "hint": "Uganda: enter local number without +256.",
    },
    {
        "code": "255",
        "iso2": "tz",
        "label": "Tanzania",
        "flag": "🇹🇿",
        "placeholder": "712345678",
        "hint": "Tanzania: enter local number without +255.",
    },
    {
        "code": "250",
        "iso2": "rw",
        "label": "Rwanda",
        "flag": "🇷🇼",
        "placeholder": "781234567",
        "hint": "Rwanda: enter local number without +250.",
    },
    {
        "code": "251",
        "iso2": "et",
        "label": "Ethiopia",
        "flag": "🇪🇹",
        "placeholder": "911234567",
        "hint": "Ethiopia: enter local number without +251.",
    },
    {
        "code": "211",
        "iso2": "ss",
        "label": "South Sudan",
        "flag": "🇸🇸",
        "placeholder": "912345678",
        "hint": "South Sudan: enter local number without +211.",
    },
    {
        "code": "243",
        "iso2": "cd",
        "label": "DR Congo",
        "flag": "🇨🇩",
        "placeholder": "812345678",
        "hint": "DR Congo: enter local number without +243.",
    },
    {
        "code": "27",
        "iso2": "za",
        "label": "South Africa",
        "flag": "🇿🇦",
        "placeholder": "821234567",
        "hint": "South Africa: enter local number without +27.",
    },
    {
        "code": "234",
        "iso2": "ng",
        "label": "Nigeria",
        "flag": "🇳🇬",
        "placeholder": "8012345678",
        "hint": "Nigeria: enter local number without +234.",
    },
    {
        "code": "44",
        "iso2": "gb",
        "label": "United Kingdom",
        "flag": "🇬🇧",
        "placeholder": "7123456789",
        "hint": "UK: enter local number without +44.",
    },
    {
        "code": "1",
        "iso2": "us",
        "label": "US / Canada",
        "flag": "🇺🇸",
        "placeholder": "2025550123",
        "hint": "US/Canada: enter local number without +1.",
    },
)

SIGNUP_EMAIL_DOMAINS: tuple[dict, ...] = (
    {"value": "gmail.com", "brand": "gmail", "label": "Gmail", "hint": "Recommended — most staff use Gmail."},
    {"value": "yahoo.com", "brand": "yahoo", "label": "Yahoo", "hint": "Yahoo Mail."},
    {"value": "outlook.com", "brand": "outlook", "label": "Outlook", "hint": "Microsoft Outlook."},
    {"value": "hotmail.com", "brand": "hotmail", "label": "Hotmail", "hint": "Hotmail / Outlook."},
    {"value": "icloud.com", "brand": "icloud", "label": "iCloud", "hint": "Apple iCloud Mail."},
    {"value": "live.com", "brand": "microsoft", "label": "Live", "hint": "Microsoft Live Mail."},
    {"value": "__custom__", "brand": "custom", "label": "Other", "hint": "Enter your company or custom domain below."},
)

ROLE_LABELS = {
    "super_admin": "Super admin",
    "it_support": "IT support",
    "company_manager": "Company manager",
    "admin": "Admin",
    "manager": "Manager",
    "sales": "Sales",
    "finance": "Finance",
    "employee": "Employee",
    "rider": "Rider",
}

STATUS_LABELS = {
    "pending_approval": "Pending approval",
    "active": "Active",
    "suspended": "Suspended",
}

VALID_ROLES = frozenset(ROLE_LABELS)

# Company portal login (no branch allocation): IT, super admin, company HR manager.
# Full company portal + IT routes: super admin, IT support, and company manager (same access tier).
COMPANY_PORTAL_ROLES = frozenset({"super_admin", "it_support", "company_manager"})

# Company exec roles (super admin / IT / company manager) can preview branch roles in the shop shell (UI only).
SHOP_UI_PREVIEW_ROLES: Tuple[str, ...] = ("manager", "admin", "sales", "finance", "employee", "rider")
SHOP_UI_PREVIEW_ROLE_SET = frozenset(SHOP_UI_PREVIEW_ROLES)


def _shop_role_preview_template_ctx(role_key: str) -> dict[str, Any]:
    rk = (role_key or "employee").strip().lower()
    if rk not in VALID_ROLES:
        rk = "employee"
    preview_raw = (session.get("shop_role_preview") or "").strip().lower()
    preview_allowed = rk in COMPANY_PORTAL_ROLES
    active = bool(preview_allowed and preview_raw in SHOP_UI_PREVIEW_ROLE_SET)
    ui_role = preview_raw if active else rk
    if ui_role not in VALID_ROLES:
        ui_role = rk
    return {
        "shop_ui_role": ui_role,
        "shop_role_preview_active": active,
        "shop_role_preview_label": ROLE_LABELS.get(ui_role, ui_role) if active else None,
        "shop_show_it_branch_controls": preview_allowed and not active,
        "shop_role_preview_options": SHOP_UI_PREVIEW_ROLES,
    }


# Shop shell sidebar / pages (respects super-admin & IT "view as" preview via session.shop_role_preview).
SHOP_SHELL_CAN_ROLES: dict[str, frozenset[str]] = {
    "item_toggles": frozenset(SHOP_UI_PREVIEW_ROLES),
    "manage_items": frozenset({"manager", "admin"}),
    "stock": frozenset({"manager", "admin"}),
    "kitchen_portions": frozenset({"manager", "admin"}),
    "hr": frozenset({"manager", "admin"}),
    "audits": frozenset({"manager", "admin"}),
    "analytics": frozenset({"manager", "admin", "sales", "finance", "employee", "rider"}),
    "receipts_nav": frozenset({"manager", "admin", "finance"}),
    "receipt_mark": frozenset({"manager", "admin", "finance"}),
    "credit_payments": frozenset({"manager", "admin", "finance", "sales"}),
    "settings": frozenset({"manager", "admin"}),
    "expenses": frozenset({"manager", "admin"}),
}

SHOP_DAY_OPENING_SUBMIT_ROLES = frozenset({"admin", "manager"})
SHOP_DAY_CLOSING_SUBMIT_ROLES = frozenset(
    {"admin", "manager", "super_admin", "it_support", "company_manager"}
)


def _session_shop_shell_role_key() -> str:
    """Effective branch UI role for shop-shell RBAC (preview-aware)."""
    uid = session.get("employee_id")
    if not uid:
        return "manager" if session.get("shop_id") else ""
    try:
        from database import get_employee_by_id

        emp = get_employee_by_id(int(uid))
        rk = (emp.get("role") if emp else None) or session.get("employee_role") or "employee"
        rk = str(rk or "employee").strip().lower()
        if rk not in VALID_ROLES:
            rk = "employee"
    except Exception:
        rk = str(session.get("employee_role") or "employee").strip().lower()
        if rk not in VALID_ROLES:
            rk = "employee"
    ctx = _shop_role_preview_template_ctx(rk)
    return str(ctx.get("shop_ui_role") or rk)


def _session_shop_shell_it_without_preview() -> bool:
    er = str(session.get("employee_role") or "").strip().lower()
    if er not in COMPANY_PORTAL_ROLES:
        return False
    preview_raw = str(session.get("shop_role_preview") or "").strip().lower()
    return preview_raw not in SHOP_UI_PREVIEW_ROLE_SET


@app.template_global()
def shop_shell_can(op: str) -> bool:
    """Return True if the current session may see/use shop-shell capability ``op`` (honours role preview)."""
    if _session_shop_shell_it_without_preview():
        return True
    rk = _session_shop_shell_role_key()
    if not rk:
        return False
    allowed = SHOP_SHELL_CAN_ROLES.get(str(op or "").strip().lower())
    if allowed is None:
        return False
    return rk in allowed


def _redirect_to_employee_dashboard():
    """Redirect company-portal roles to /role/dashboard; others to allocated shop if set."""
    role_key = session.get("employee_role") or "employee"
    if role_key in COMPANY_PORTAL_ROLES:
        return redirect(url_for("employee_dashboard", role=role_key))
    uid = session.get("employee_id")
    if uid:
        try:
            from database import get_employee_by_id

            emp = get_employee_by_id(int(uid))
            sid = emp.get("shop_id") if emp else None
            if sid:
                try:
                    sid_int = int(sid)
                    if sid_int > 0:
                        return redirect(url_for("shop_dashboard", shop_id=sid_int))
                except (TypeError, ValueError):
                    pass
        except Exception:
            pass
    return redirect(url_for("employee_dashboard", role=role_key))


def _clear_employee_portal_session() -> None:
    """Drop employee portal keys (e.g. before starting a shop-password session)."""
    session.pop("employee_id", None)
    session.pop("employee_name", None)
    session.pop("employee_role", None)
    session.pop("shop_role_preview", None)
    session.pop("employee_last_activity", None)


def _touch_employee_session_activity() -> None:
    session["employee_last_activity"] = time.time()


def _request_is_shop_branch_session_view() -> bool:
    """True on shop till / branch pages where idle sign-out would interrupt selling."""
    if not session.get("shop_id"):
        return False
    ep = request.endpoint or ""
    if ep == "shop_pos" or ep.startswith("shop_"):
        return True
    path = (request.path or "").strip("/")
    parts = path.split("/")
    return len(parts) >= 2 and parts[0] == "shops" and parts[1].isdigit()


_EMPLOYEE_IDLE_SKIP_ENDPOINTS = frozenset(
    {
        "static",
        "employee_login",
        "employee_logout",
        "employee_signup",
        "employee_reset_password",
        "api_reset_password_email_for_code",
        "api_reset_password_verify_email",
        "api_reset_password_send_code",
        "api_reset_password_complete",
        "public_shop_login",
        "shop_login",
        "shop_logout",
    }
)


def _enforce_employee_session_idle_timeout():
    """Sign out employee portal sessions after inactivity (shop-password session may remain)."""
    if request.endpoint in _EMPLOYEE_IDLE_SKIP_ENDPOINTS:
        return None
    if not session.get("employee_id"):
        return None
    if _request_is_shop_branch_session_view():
        return None

    now = time.time()
    try:
        last = float(session.get("employee_last_activity"))
    except (TypeError, ValueError):
        _touch_employee_session_activity()
        return None

    if now - last <= EMPLOYEE_SESSION_IDLE_SECONDS:
        _touch_employee_session_activity()
        return None

    emp_id = session.get("employee_id")
    emp_name = session.get("employee_name")
    emp_role = session.get("employee_role")
    _log_hr_activity_safe(
        "logout",
        employee_id=emp_id,
        target_type="employee",
        target_id=int(emp_id) if emp_id else None,
        description=f"Automatic sign-out after {_EMPLOYEE_IDLE_MINUTES} minutes of inactivity",
        employee_full_name=emp_name,
        employee_role=emp_role,
    )
    _clear_employee_portal_session()

    wants_json = (
        request.headers.get("X-Requested-With") == "XMLHttpRequest"
        or "application/json" in (request.headers.get("Accept") or "").lower()
    )
    if wants_json:
        next_url = _request_continuation_url()
        return (
            jsonify(
                {
                    "ok": False,
                    "error": f"Session expired after {_EMPLOYEE_IDLE_MINUTES} minutes of inactivity.",
                    "login_url": _employee_login_url(next_url=next_url, reason="idle"),
                }
            ),
            401,
        )

    flash(str(_EMPLOYEE_IDLE_MINUTES), "idle_signout")
    next_url = _request_continuation_url()
    return redirect(_employee_login_url(next_url=next_url, reason="idle"))


@app.before_request
def _employee_session_idle_timeout_on_request():
    return _enforce_employee_session_idle_timeout()


def _safe_login_next(next_url: str) -> bool:
    """Reject open redirects (``//evil``) while allowing same-origin paths."""
    u = (next_url or "").strip()
    return u.startswith("/") and not u.startswith("//")


def _request_continuation_url() -> str:
    """Same-origin path (+ query) for resuming after idle sign-out."""
    if request.method != "GET":
        return ""
    path = (request.path or "").strip()
    if not path.startswith("/"):
        return ""
    qs = (request.query_string or b"").decode("utf-8", errors="ignore").strip()
    return f"{path}?{qs}" if qs else path


def _employee_login_url(*, next_url: str = "", reason: str = "") -> str:
    """Build employee login URL, optionally preserving continuation and idle reason."""
    nxt = (next_url or "").strip()
    idle = (reason or "").strip().lower() == "idle"
    if _safe_login_next(nxt) and idle:
        return url_for("employee_login", next=nxt, reason="idle")
    if _safe_login_next(nxt):
        return url_for("employee_login", next=nxt)
    if idle:
        return url_for("employee_login", reason="idle")
    return url_for("employee_login")


def _parse_next_shop_id(next_url: str) -> Optional[int]:
    """If ``next_url`` targets ``/shops/<id>/...``, return that shop id."""
    if not _safe_login_next(next_url):
        return None
    path = urlparse(next_url).path.strip("/")
    parts = path.split("/")
    if len(parts) >= 2 and parts[0] == "shops" and parts[1].isdigit():
        try:
            sid = int(parts[1])
            return sid if sid > 0 else None
        except ValueError:
            return None
    return None


def _redirect_login_preserving_next():
    """After failed POST /login, preserve optional continuation ``next`` query."""
    nxt = (request.form.get("next") or "").strip()
    idle = (request.form.get("reason") or "").strip().lower() == "idle"
    if _safe_login_next(nxt):
        return redirect(_employee_login_url(next_url=nxt, reason="idle" if idle else ""))
    if idle:
        return redirect(_employee_login_url(reason="idle"))
    return redirect(url_for("employee_login"))


def _session_may_follow_login_next(next_url: str):
    """
    Logged-in portal user visits GET /login?next=...
    Honor same-origin continuation URLs (shop branches, role dashboards, IT pages).
    """
    if not _safe_login_next(next_url) or not session.get("employee_id"):
        return None
    role_key = str(session.get("employee_role") or "employee").strip().lower()
    path = urlparse(next_url).path.rstrip("/")

    target_sid = _parse_next_shop_id(next_url)
    if target_sid is not None:
        if role_key in COMPANY_PORTAL_ROLES:
            return redirect(next_url)
        try:
            from database import get_employee_by_id, employee_may_use_shop_branch

            emp = get_employee_by_id(int(session.get("employee_id") or 0))
            if emp and employee_may_use_shop_branch(emp, target_sid):
                return redirect(next_url)
        except Exception:
            pass
        return None

    if _is_role_dashboard_path(path) or _is_legacy_employee_role_dashboard_path(path):
        return redirect(url_for("employee_dashboard", role=role_key))

    if role_key in COMPANY_PORTAL_ROLES:
        return redirect(next_url)

    if path.startswith("/it_support"):
        flash("That page is for IT support only.", "warning")
        return redirect(url_for("employee_dashboard", role=role_key))

    if path.startswith("/shops/"):
        return redirect(next_url)

    return redirect(next_url)


def _is_role_dashboard_path(path: str) -> bool:
    """True if path is /<role>/dashboard with a known role key."""
    p = path.rstrip("/")
    parts = p.strip("/").split("/")
    return len(parts) == 2 and parts[1] == "dashboard" and parts[0] in VALID_ROLES


def _is_legacy_employee_role_dashboard_path(path: str) -> bool:
    """Old shape /employee/<role>/dashboard (bookmark or ?next=); normalize to /<role>/dashboard."""
    p = path.rstrip("/")
    parts = p.strip("/").split("/")
    return (
        len(parts) == 3
        and parts[0] == "employee"
        and parts[2] == "dashboard"
        and parts[1] in VALID_ROLES
    )


def _bootstrap_database_schema() -> bool:
    """Ensure MySQL database exists, create missing tables, apply migrations (idempotent)."""
    try:
        from database import init_schema

        ok = init_schema()
        if not ok:
            logger.warning(
                "Database schema initialization did not complete successfully; check MySQL credentials and server logs."
            )
        return ok
    except Exception:
        logger.exception("Database schema initialization failed.")
        return False


_db_schema_ready = _bootstrap_database_schema()
_db_schema_lock = threading.Lock()
_db_schema_next_retry_mono = 0.0
_DB_SCHEMA_RETRY_AFTER_FAILURE_SEC = 3.0


@app.before_request
def _ensure_database_schema_on_request():
    """On each request, ensure schema is ready if import-time init failed (e.g. MySQL still starting)."""
    global _db_schema_ready, _db_schema_next_retry_mono
    if _db_schema_ready:
        return
    if request.endpoint == "static":
        return
    now = time.monotonic()
    if now < _db_schema_next_retry_mono:
        return
    with _db_schema_lock:
        if _db_schema_ready:
            return
        now = time.monotonic()
        if now < _db_schema_next_retry_mono:
            return
        ok = _bootstrap_database_schema()
        _db_schema_ready = ok
        if not ok:
            _db_schema_next_retry_mono = time.monotonic() + _DB_SCHEMA_RETRY_AFTER_FAILURE_SEC


@app.context_processor
def inject_jos_version():
    return {"jos_version": JOS_VERSION}


@app.context_processor
def inject_font_catalog():
    from theme_presets import fonts_for_template

    return {"appearance_fonts": fonts_for_template()}


def _effective_pos_theme_colors(shop: dict) -> tuple[str, str]:
    """Shop primary/accent when appearance is customized; otherwise company defaults."""
    eff = _effective_appearance_settings_for_shop(shop)
    return (
        _hex_to_rgb_triplet(eff.get("primary_color") or "#f97316"),
        _hex_to_rgb_triplet(eff.get("accent_color") or "#fb923c"),
    )


def _hex_to_rgb_triplet(hex_color: str) -> str:
    s = (hex_color or "").strip().lstrip("#")
    if len(s) == 3:
        s = "".join([c * 2 for c in s])
    if len(s) != 6:
        return "249 115 22"  # Tailwind orange-500
    try:
        r = int(s[0:2], 16)
        g = int(s[2:4], 16)
        b = int(s[4:6], 16)
        return f"{r} {g} {b}"
    except Exception:
        return "249 115 22"


@app.template_filter("font_stack")
def font_stack_filter(display_name: str) -> str:
    from theme_presets import font_css_stack

    return font_css_stack(display_name)


@app.template_filter("font_google_url")
def font_google_url_filter(display_name: str) -> str:
    from theme_presets import google_fonts_url, normalize_font_family

    return google_fonts_url(normalize_font_family(display_name))


def _effective_portal_font_family() -> str:
    """Site-wide font, or the active shop's font when a shop session is open."""
    from theme_presets import DEFAULT_FONT_FAMILY, normalize_font_family

    try:
        from database import get_shop_by_id, get_site_settings

        stored = get_site_settings(["font_family"]) or {}
        site_font = normalize_font_family(stored.get("font_family"))
    except Exception:
        site_font = DEFAULT_FONT_FAMILY

    shop_id = session.get("shop_id")
    if not shop_id:
        return site_font
    try:
        shop = get_shop_by_id(int(shop_id))
    except (TypeError, ValueError):
        return site_font
    if not shop:
        return site_font
    if not _shop_has_appearance_override(shop):
        return site_font
    shop_font = (shop.get("font_family") or "").strip()
    return normalize_font_family(shop_font) if shop_font else site_font


def _build_analytics_filter():
    today = date.today()
    modes = {"single_day", "period", "month", "year"}
    mode = (request.args.get("mode") or "single_day").strip().lower()
    if mode not in modes:
        mode = "single_day"

    single_day = (request.args.get("single_day") or today.isoformat()).strip()
    start_date = (request.args.get("start_date") or today.isoformat()).strip()
    end_date = (request.args.get("end_date") or today.isoformat()).strip()
    month = (request.args.get("month") or today.strftime("%Y-%m")).strip()
    year = (request.args.get("year") or str(today.year)).strip()

    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", single_day):
        single_day = today.isoformat()
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", start_date):
        start_date = today.isoformat()
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", end_date):
        end_date = today.isoformat()
    if not re.fullmatch(r"\d{4}-\d{2}", month):
        month = today.strftime("%Y-%m")
    if not re.fullmatch(r"\d{4}", year):
        year = str(today.year)

    # Build a canonical datetime range used by analytics SQL:
    # range_start <= created_at < range_end_exclusive
    if mode == "period":
        try:
            d1 = datetime.strptime(start_date, "%Y-%m-%d").date()
        except Exception:
            d1 = today
        try:
            d2 = datetime.strptime(end_date, "%Y-%m-%d").date()
        except Exception:
            d2 = d1
        if d1 > d2:
            d1, d2 = d2, d1
        range_start = d1.isoformat()
        range_end_exclusive = (d2 + timedelta(days=1)).isoformat()
        range_label = f"{d1.isoformat()} to {d2.isoformat()}"
        start_date = d1.isoformat()
        end_date = d2.isoformat()
    elif mode == "month":
        try:
            d1 = datetime.strptime(month + "-01", "%Y-%m-%d").date()
        except Exception:
            d1 = date(today.year, today.month, 1)
            month = d1.strftime("%Y-%m")
        if d1.month == 12:
            d2 = date(d1.year + 1, 1, 1)
        else:
            d2 = date(d1.year, d1.month + 1, 1)
        range_start = d1.isoformat()
        range_end_exclusive = d2.isoformat()
        range_label = d1.strftime("%B %Y")
    elif mode == "year":
        try:
            y = int(year)
        except Exception:
            y = today.year
            year = str(y)
        if y < 2000:
            y = 2000
            year = "2000"
        if y > 2100:
            y = 2100
            year = "2100"
        d1 = date(y, 1, 1)
        d2 = date(y + 1, 1, 1)
        range_start = d1.isoformat()
        range_end_exclusive = d2.isoformat()
        range_label = str(y)
    else:
        try:
            d1 = datetime.strptime(single_day, "%Y-%m-%d").date()
        except Exception:
            d1 = today
            single_day = d1.isoformat()
        range_start = d1.isoformat()
        range_end_exclusive = (d1 + timedelta(days=1)).isoformat()
        range_label = d1.isoformat()

    return {
        "mode": mode,
        "single_day": single_day,
        "start_date": start_date,
        "end_date": end_date,
        "month": month,
        "year": year,
        "range_start": range_start,
        "range_end_exclusive": range_end_exclusive,
        "range_label": range_label,
    }


def _shop_report_live_enabled(analytics_filter: dict) -> bool:
    """Live JSON refresh only when viewing today's single-day report."""
    return _analytics_filter_is_today(analytics_filter)


def _company_report_shop_filter_from_request() -> Optional[int]:
    """Optional shop_id query param for company report / expenditure pages."""
    shop_filter_arg = request.args.get("shop_id", type=int)
    return shop_filter_arg if shop_filter_arg and shop_filter_arg > 0 else None


def _analytics_filter_is_today(analytics_filter: dict) -> bool:
    """True when the filter is single-day and that day is today (live report refresh)."""
    af = analytics_filter or {}
    if (af.get("mode") or "single_day") != "single_day":
        return False
    return (af.get("single_day") or "").strip() == date.today().isoformat()


def _shop_report_has_filter_params() -> bool:
    keys = ("mode", "single_day", "start_date", "end_date", "month", "year", "analytics_scope")
    return any((request.args.get(k) or "").strip() for k in keys)


def _shop_report_period_label(analytics_filter: dict) -> str:
    af = analytics_filter or {}
    today = date.today()
    mode = (af.get("mode") or "single_day").strip().lower()
    if mode == "single_day":
        sd = (af.get("single_day") or "").strip()
        if sd == today.isoformat():
            return "Today"
        try:
            return datetime.strptime(sd, "%Y-%m-%d").date().strftime("%d %b %Y")
        except Exception:
            return sd or "Today"
    return (af.get("range_label") or "Selected period").strip()


def _analytics_scope_from_request() -> str:
    scope = (request.args.get("analytics_scope") or "general").strip().lower()
    return scope if scope in ("general", "actual") else "general"


def _analytics_nav_kwargs() -> dict:
    """Query args for IT Support analytics sidebar links (shared period + scope)."""
    f = _build_analytics_filter()
    out = {
        "mode": f["mode"],
        "single_day": f["single_day"],
        "start_date": f["start_date"],
        "end_date": f["end_date"],
        "month": f["month"],
        "year": f["year"],
        "analytics_scope": _analytics_scope_from_request(),
    }
    # shop_id / shop_view are set per-link in shop sidebar templates (not here) to avoid
    # duplicate url_for kwargs when merging with **_anav.
    return out


def _shop_view_from_request() -> str:
    shop_view = (request.args.get("shop_view") or "revenue").strip().lower()
    if shop_view not in ("revenue", "item", "sales", "credit", "period", "stock", "customer"):
        return "revenue"
    return shop_view


def _fetch_shop_analytics_payload(analytics_filter: dict, analytics_scope: str) -> dict:
    """Load shop-scoped analytics for the active shop_view tab."""
    shop_id = request.args.get("shop_id", type=int)
    if not shop_id:
        return {}
    shop_view = _shop_view_from_request()
    try:
        from database import (
            get_shop_credit_analytics,
            get_shop_customer_analytics,
            get_shop_item_analytics,
            get_shop_period_analytics,
            get_shop_revenue_analytics,
            get_shop_sales_analytics,
            get_shop_stock_analytics,
        )

        if shop_view == "item":
            return get_shop_item_analytics(
                shop_id=shop_id,
                analytics_filter=analytics_filter,
                analytics_scope=analytics_scope,
            )
        if shop_view == "sales":
            return get_shop_sales_analytics(
                shop_id=shop_id,
                analytics_filter=analytics_filter,
                analytics_scope=analytics_scope,
            )
        if shop_view == "credit":
            return get_shop_credit_analytics(
                shop_id=shop_id,
                analytics_filter=analytics_filter,
                analytics_scope=analytics_scope,
            )
        if shop_view == "period":
            return get_shop_period_analytics(
                shop_id=shop_id,
                analytics_filter=analytics_filter,
                analytics_scope=analytics_scope,
            )
        if shop_view == "stock":
            return get_shop_stock_analytics(
                shop_id=shop_id, analytics_filter=analytics_filter
            )
        if shop_view == "customer":
            return get_shop_customer_analytics(
                shop_id=shop_id,
                analytics_filter=analytics_filter,
                analytics_scope=analytics_scope,
            )
        return get_shop_revenue_analytics(
            shop_id=shop_id,
            analytics_filter=analytics_filter,
            analytics_scope=analytics_scope,
        )
    except Exception:
        return {}


_IT_SUPPORT_ANALYTICS_NAV_ENDPOINTS = frozenset(
    {
        "it_support_analytics",
        "it_support_revenue_analytics",
        "it_support_item_analytics_page",
        "it_support_sales_analytics",
        "it_support_credit_analytics",
        "it_support_period_analytics",
        "it_support_employee_analytics",
        "it_support_customer_analytics",
        "it_support_shop_analytics",
        "it_support_customer_transactions",
    }
)

_SHOP_ANALYTICS_NAV_ENDPOINTS = frozenset(
    {
        "shop_analytics",
        "shop_revenue_analytics",
        "shop_item_analytics",
        "shop_period_analytics",
        "shop_sales_analytics",
        "shop_credit_analytics",
        "shop_customer_analytics",
        "shop_customer_analytics_detail",
        "shop_stock_analytics",
        "shop_receipts",
    }
)


@app.context_processor
def inject_it_support_analytics_nav():
    ep = request.endpoint or ""
    if ep not in _IT_SUPPORT_ANALYTICS_NAV_ENDPOINTS:
        return {}
    return {"analytics_nav": _analytics_nav_kwargs()}


@app.context_processor
def inject_shop_analytics_nav():
    ep = request.endpoint or ""
    if ep not in _SHOP_ANALYTICS_NAV_ENDPOINTS:
        return {}
    return {"analytics_nav": _analytics_nav_kwargs()}


def _normalize_static_relative_path(path) -> str:
    """Normalize DB paths for Flask url_for('static', filename=...)."""
    if path is None:
        return ""
    if isinstance(path, bytes):
        path = path.decode("utf-8", errors="replace")
    s = str(path).strip()
    if not s:
        return ""
    if s.lower().startswith(("http://", "https://")):
        return s
    s = s.replace("\\", "/")
    while s.startswith("/"):
        s = s[1:]
    low = s.lower()
    if low.startswith("static/"):
        s = s[7:]
    return s


@app.template_global()
def static_upload_url(path) -> str:
    """Public URL for files under static/ (e.g. uploads/items/...) or an absolute image URL."""
    s = _normalize_static_relative_path(path)
    if not s:
        return ""
    if s.startswith("http://") or s.startswith("https://"):
        return s
    return url_for("static", filename=s)


@app.template_global()
def role_label(key: str) -> str:
    """Human-readable role title for templates (e.g. shop role preview dropdown)."""
    k = (key or "").strip().lower()
    return str(ROLE_LABELS.get(k, key or ""))


@app.context_processor
def inject_website_settings():
    try:
        ws = _website_settings_for_template()
    except Exception:
        ws = _default_website_settings()
    try:
        is_live = _is_public_website_host_request()
    except Exception:
        is_live = False
    domain = (ws.get("domain") or "").strip().rstrip("/") if isinstance(ws, dict) else ""
    try:
        share = _public_storefront_share_info()
    except Exception:
        share = {"url": "/site", "kind": "preview", "label": "Preview link", "hint": "", "is_branded": False}
    return {
        "website_settings": ws,
        "is_public_website_host": is_live,
        "public_storefront_url": domain or None,
        "public_storefront_share_url": share.get("url") or "/site",
        "public_storefront_share": share,
    }


@app.context_processor
def inject_storefront_homepage_copy():
    """Homepage copy and contact resolved from website builder + company settings."""
    try:
        ws = _load_website_settings()
        co = _load_company_identity_settings()
        return {"storefront_homepage_copy": _storefront_homepage_copy(ws.get("design"), co)}
    except Exception:
        return {"storefront_homepage_copy": _storefront_homepage_copy({}, {})}


@app.context_processor
def inject_storefront_seo():
    """Canonical, Open Graph, and JSON-LD context for public storefront pages."""
    try:
        seo = _storefront_seo_context_for_request()
    except Exception:
        seo = {}
    return {"storefront_seo": seo or None}


@app.context_processor
def inject_site_settings():
    from theme_presets import (
        DEFAULT_THEME_PRESET,
        THEME_PRESETS,
        font_css_stack,
        google_fonts_url,
        normalize_default_theme,
        normalize_font_family,
        normalize_theme_preset,
    )

    defaults = {
        "company_name": "Point of Sale",
        "company_email": "",
        "company_phone": "",
        "company_facebook": "",
        "company_instagram": "",
        "company_twitter": "",
        "company_tiktok": "",
        "public_app_url": "",
        "company_location_name": "",
        "company_latitude": "",
        "company_longitude": "",
        "app_icon": "",
        "primary_color": "#f97316",
        "accent_color": "#fb923c",
        "font_family": "Plus Jakarta Sans",
        "default_theme": "dark",
        "theme_preset": "eye-comfort",
    }
    try:
        from database import get_site_settings

        stored = get_site_settings(list(defaults.keys()))
    except Exception:
        stored = {}
    merged = {**defaults, **{k: v for k, v in (stored or {}).items() if v is not None and v != ""}}
    merged["primary_color_rgb"] = _hex_to_rgb_triplet(merged.get("primary_color"))
    merged["accent_color_rgb"] = _hex_to_rgb_triplet(merged.get("accent_color"))
    merged["font_family"] = normalize_font_family(merged.get("font_family"))
    merged["default_theme"] = normalize_default_theme(merged.get("default_theme"))
    merged["theme_preset"] = normalize_theme_preset(merged.get("theme_preset"))
    preset_key = merged["theme_preset"]
    portal_font = _effective_portal_font_family()
    site_font = merged["font_family"]
    primary = (merged.get("primary_color") or defaults["primary_color"]).strip()
    accent = (merged.get("accent_color") or defaults["accent_color"]).strip()
    merged["font_google_url"] = google_fonts_url(site_font)
    merged["theme_config_key"] = "|".join(
        [
            str(merged.get("default_theme") or defaults["default_theme"]),
            primary.lower(),
            accent.lower(),
            str(site_font),
            str(preset_key or defaults["theme_preset"]),
        ]
    )
    return {
        "site_settings": merged,
        "theme_default": merged["default_theme"],
        "font_family": site_font,
        "portal_font_family": portal_font,
        "font_family_stack": font_css_stack(site_font),
        "theme_preset": preset_key,
        "theme_presets_css": THEME_PRESETS,
        "theme_font_google_url": google_fonts_url(portal_font),
        "google_maps_api_key": _google_maps_api_key(),
    }


@app.context_processor
def inject_employee_session_idle_config():
    base = {"employee_session_idle_minutes": _EMPLOYEE_IDLE_MINUTES}
    if not session.get("employee_id") or _request_is_shop_branch_session_view():
        return {**base, "employee_session_idle_guard": False}
    return {
        **base,
        "employee_session_idle_guard": True,
        "employee_session_idle_warn_seconds": EMPLOYEE_SESSION_IDLE_WARN_SECONDS,
    }


@app.context_processor
def inject_portal_context():
    """Nav + portal shell: profile, role, links (DB row when logged in)."""
    if request.endpoint == "shop_pos":
        uid = session.get("employee_id")
        if not uid:
            out = {
                "nav_employee": None,
                "portal_employee": None,
                "shop_ui_role": "manager" if session.get("shop_id") else None,
                "shop_role_preview_active": False,
                "shop_role_preview_label": None,
                "shop_show_it_branch_controls": False,
                "shop_role_preview_options": SHOP_UI_PREVIEW_ROLES,
            }
            return out
        role_key = str(session.get("employee_role") or "employee").strip().lower()
        if role_key not in VALID_ROLES:
            role_key = "employee"
        pe = {
            "name": session.get("employee_name") or "",
            "role_key": role_key,
            "role_label": ROLE_LABELS.get(role_key, role_key),
            "profile_image": None,
            "dashboard_url": url_for("employee_dashboard", role=role_key),
            "profile_settings_url": url_for("employee_profile_settings"),
        }
        return {
            "nav_employee": {
                "name": pe["name"],
                "role_label": pe["role_label"],
                "dashboard_url": pe["dashboard_url"],
            },
            "portal_employee": pe,
            **_shop_role_preview_template_ctx(role_key),
        }

    uid = session.get("employee_id")
    if not uid:
        out = {
            "nav_employee": None,
            "portal_employee": None,
            "shop_ui_role": None,
            "shop_role_preview_active": False,
            "shop_role_preview_label": None,
            "shop_show_it_branch_controls": False,
            "shop_role_preview_options": SHOP_UI_PREVIEW_ROLES,
        }
        # Shop-password session (no employee portal): treat UI like branch lead for legacy POS/tablet flows.
        if session.get("shop_id"):
            out["shop_ui_role"] = "manager"
        return out
    try:
        from database import get_employee_by_id

        emp = get_employee_by_id(uid)
    except Exception:
        emp = None
    if not emp:
        role_key = session.get("employee_role") or "employee"
        return {
            "nav_employee": {
                "name": session.get("employee_name") or "",
                "role_label": ROLE_LABELS.get(role_key, role_key),
                "dashboard_url": url_for("employee_dashboard", role=role_key),
            },
            "portal_employee": None,
            **_shop_role_preview_template_ctx(role_key),
        }
    role_key = emp.get("role") or "employee"
    if role_key not in VALID_ROLES:
        role_key = "employee"
    pe = {
        "name": emp.get("full_name") or "",
        "role_key": role_key,
        "role_label": ROLE_LABELS.get(role_key, role_key),
        "profile_image": emp.get("profile_image"),
        "dashboard_url": url_for("employee_dashboard", role=role_key),
        "profile_settings_url": url_for("employee_profile_settings"),
    }
    extra: dict[str, object] = {}
    if role_key in COMPANY_PORTAL_ROLES:
        try:
            ps = _load_printing_settings()
            extra["company_pos_inventory_exclusive"] = _printing_pos_inventory_exclusive_choice(ps)
            extra["company_pos_allow_credit_sale"] = bool(ps.get("pos_allow_credit_sale"))
            extra["company_pos_cart_mode_withhold"] = _coerce_pos_cart_mode(ps.get("pos_cart_mode")) == "withhold"
        except Exception:
            extra["company_pos_inventory_exclusive"] = "shop"
            extra["company_pos_allow_credit_sale"] = True
            extra["company_pos_cart_mode_withhold"] = False
    return {
        "nav_employee": {
            "name": pe["name"],
            "role_label": pe["role_label"],
            "dashboard_url": pe["dashboard_url"],
        },
        "portal_employee": pe,
        **_shop_role_preview_template_ctx(role_key),
        **extra,
    }


def _notify_new_shop_stock_request(
    *,
    req_id: int,
    requesting_shop_id: int,
    source_type: str,
    source_shop_id: int | None,
    request_type: str,
    item_id: int,
    qty: int,
) -> None:
    """Create scoped notifications for company IT/super_admin and the shops involved."""
    from database import create_notification, get_item_by_id, get_shop_by_id

    rq_shop = get_shop_by_id(int(requesting_shop_id)) or {}
    rq_name = (rq_shop.get("shop_name") or "").strip() or f"Shop #{requesting_shop_id}"
    item = get_item_by_id(int(item_id)) or {}
    item_label = (item.get("name") or "").strip() or f"Item #{item_id}"
    rt = (request_type or "stock_in").strip().lower()
    st = (source_type or "").strip().lower()

    create_notification(
        title="Request received",
        message=(
            f"Request #{req_id}: {item_label} × {qty} submitted and is awaiting approval."
        )[:500],
        shop_id=int(requesting_shop_id),
        audience_role="all",
        link_url=url_for("shop_notifications", shop_id=int(requesting_shop_id)),
        dedupe_key=f"sr:new:{req_id}:rq",
    )

    if rt == "return_to_company":
        create_notification(
            title="New return-to-company request",
            message=(f"Request #{req_id}: {rq_name} requests to return {item_label} × {qty} to company stock.")[:500],
            shop_id=None,
            audience_role="admin_only",
            link_url=url_for("notifications"),
            dedupe_key=f"sr:new:{req_id}:admin",
        )
        return

    if st == "company":
        create_notification(
            title="New stock request",
            message=(f"Request #{req_id}: {rq_name} requests {item_label} × {qty} from company stock.")[:500],
            shop_id=None,
            audience_role="admin_only",
            link_url=url_for("notifications"),
            dedupe_key=f"sr:new:{req_id}:admin",
        )
        return

    if st == "shop" and source_shop_id:
        s = get_shop_by_id(int(source_shop_id)) or {}
        src_nm = (s.get("shop_name") or "").strip() or f"Shop #{source_shop_id}"
        create_notification(
            title="Incoming stock request",
            message=(
                f"Request #{req_id}: {rq_name} requests {item_label} × {qty} from your shop ({src_nm})."
            )[:500],
            shop_id=int(source_shop_id),
            audience_role="all",
            link_url=url_for("shop_notifications", shop_id=int(source_shop_id)),
            dedupe_key=f"sr:new:{req_id}:src:{int(source_shop_id)}",
        )


@app.context_processor
def inject_notification_context():
    uid = session.get("employee_id")
    if not uid:
        return {"notification_count": 0, "notifications_url": None, "notification_scope": None, "notification_bell_popup": False}
    role_key = (session.get("employee_role") or "employee").strip().lower()
    shop_id = _effective_viewer_shop_id(role_key)
    if request.endpoint == "shop_pos":
        try:
            pos_sid = request.view_args.get("shop_id") if request.view_args else None
            if pos_sid is not None:
                shop_id = int(pos_sid)
        except Exception:
            pass
    try:
        from database import count_pending_stock_requests_for_session

        notification_count = count_pending_stock_requests_for_session(
            role_key=role_key,
            viewer_shop_id=int(shop_id) if shop_id else None,
        )
    except Exception:
        notification_count = 0
    notifications_url = (
        url_for("shop_notifications", shop_id=int(shop_id)) if shop_id else url_for("notifications")
    )
    notification_scope = f"shop-{int(shop_id)}" if shop_id else "portal"
    notification_bell_popup = request.endpoint == "shop_pos"
    return {
        "notification_count": int(notification_count or 0),
        "notifications_url": notifications_url,
        "notification_scope": notification_scope,
        "notification_bell_popup": notification_bell_popup,
    }


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("employee_id"):
            return redirect(url_for("employee_login", next=request.path))
        return f(*args, **kwargs)

    return decorated


def _log_hr_activity_safe(
    action_kind: str,
    *,
    employee_id=None,
    target_type: str | None = None,
    target_id: int | None = None,
    description: str | None = None,
    employee_full_name: str | None = None,
    employee_role: str | None = None,
) -> None:
    """Best-effort wrapper around database.log_hr_activity (silent on failure)."""
    try:
        from database import log_hr_activity as _log

        eid = employee_id
        if eid is None:
            eid = session.get("employee_id")
        ip = None
        ua = None
        try:
            ip = (request.headers.get("X-Forwarded-For") or request.remote_addr or "").split(",")[0].strip() or None
            ua = request.headers.get("User-Agent")
        except Exception:
            ip = None
            ua = None
        if employee_full_name is None and eid is not None and eid == session.get("employee_id"):
            employee_full_name = session.get("employee_name")
            employee_role = employee_role or session.get("employee_role")
        _log(
            eid,
            action_kind,
            target_type=target_type,
            target_id=target_id,
            description=description,
            ip_address=ip,
            user_agent=ua,
            employee_full_name=employee_full_name,
            employee_role=employee_role,
        )
    except Exception:
        pass


def _save_profile_upload(file_storage):
    if not file_storage or not getattr(file_storage, "filename", None):
        return None
    raw = secure_filename(file_storage.filename)
    if not raw or "." not in raw:
        return None
    ext = raw.rsplit(".", 1)[-1].lower()
    if ext not in ALLOWED_PROFILE_EXT:
        return None
    fn = f"{uuid.uuid4().hex}.{ext}"
    folder = os.path.join(app.root_path, "static", UPLOAD_FOLDER_REL)
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, fn)
    file_storage.save(path)
    return f"{UPLOAD_FOLDER_REL}/{fn}"


ITEM_UPLOAD_FOLDER_REL = "uploads/items"


def _save_item_upload(file_storage):
    if not file_storage or not getattr(file_storage, "filename", None):
        return None
    raw = secure_filename(file_storage.filename)
    if not raw or "." not in raw:
        return None
    ext = raw.rsplit(".", 1)[-1].lower()
    if ext not in ALLOWED_PROFILE_EXT:
        return None
    fn = f"{uuid.uuid4().hex}.{ext}"
    folder = os.path.join(app.root_path, "static", ITEM_UPLOAD_FOLDER_REL)
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, fn)
    file_storage.save(path)
    return f"{ITEM_UPLOAD_FOLDER_REL}/{fn}"


BRANDING_UPLOAD_FOLDER_REL = "uploads/branding"


def _save_branding_upload(file_storage):
    if not file_storage or not getattr(file_storage, "filename", None):
        return None
    raw = secure_filename(file_storage.filename)
    if not raw or "." not in raw:
        return None
    ext = raw.rsplit(".", 1)[-1].lower()
    if ext not in ALLOWED_APP_ICON_EXT:
        return None
    fn = f"{uuid.uuid4().hex}.{ext}"
    folder = os.path.join(app.root_path, "static", BRANDING_UPLOAD_FOLDER_REL)
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, fn)
    file_storage.save(path)
    return f"{BRANDING_UPLOAD_FOLDER_REL}/{fn}"


def _resolve_shop_logo_upload(*, existing: str | None = None) -> tuple[str | None, bool, str | None]:
    """Return (logo_path, should_update, error_message)."""
    if (request.form.get("remove_shop_logo") or "").strip() == "1":
        return ("", True, None)
    logo_file = request.files.get("shop_logo")
    if logo_file and getattr(logo_file, "filename", ""):
        saved = _save_branding_upload(logo_file)
        if saved is None:
            return (existing, False, "Shop image must be PNG, JPG, GIF, WebP, ICO, or SVG.")
        return (saved, True, None)
    return (existing, False, None)


@app.route("/")
def index():
    """Public customer storefront at root, or shop login when the website is turned off."""
    if not _public_website_enabled():
        return redirect(url_for("public_shop_login"))
    return _render_public_storefront()


@app.route("/site")
def marketing_home():
    """Public storefront preview (/site) or redirect to / on the live website domain."""
    if _is_public_website_host_request():
        return redirect(url_for("index"), code=301)
    return _render_public_storefront()


@app.route("/site/catalog")
def marketing_catalog():
    """Full product catalog for the public shop."""
    if _is_public_website_host_request():
        return redirect(url_for("marketing_catalog_live"), code=301)
    return _render_public_storefront(catalog_mode=True)


@app.route("/catalog")
def marketing_catalog_live():
    """Full catalog on the live website domain."""
    if not _is_public_website_host_request():
        return redirect(url_for("marketing_catalog"), code=302)
    if not _public_website_enabled():
        return redirect(url_for("public_shop_login"))
    return _render_public_storefront(catalog_mode=True)


@app.route("/robots.txt")
def storefront_robots_txt():
    """Crawler rules for the public shop."""
    return Response(_storefront_robots_txt_body(), mimetype="text/plain; charset=utf-8")


@app.route("/sitemap.xml")
def storefront_sitemap_xml():
    """XML sitemap for homepage, catalogue, and category pages."""
    entries = _storefront_sitemap_entries()
    url_blocks = []
    for row in entries:
        loc = (row.get("loc") or "").replace("&", "&amp;").replace("<", "&lt;")
        if not loc:
            continue
        url_blocks.append(
            "  <url>\n"
            f"    <loc>{loc}</loc>\n"
            f"    <changefreq>{row.get('changefreq', 'weekly')}</changefreq>\n"
            f"    <priority>{row.get('priority', '0.5')}</priority>\n"
            "  </url>"
        )
    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        + "\n".join(url_blocks)
        + "\n</urlset>\n"
    )
    return Response(xml, mimetype="application/xml; charset=utf-8")


@app.route("/api/storefront/products.json")
def storefront_products_json():
    """Lightweight product list for cart — loaded after catalog HTML paints."""
    try:
        from database import list_website_catalog_items

        catalog_rows = list_website_catalog_items(limit=500)
    except Exception:
        catalog_rows = []
    products = [
        _serialize_website_product_cart_row(_serialize_website_product_row(r))
        for r in catalog_rows
        if int(r.get("id") or 0) > 0
    ]
    response = jsonify({"ok": True, "products": products})
    if not app.debug:
        response.cache_control.public = True
        response.cache_control.max_age = 300
    return response


@app.route("/features")
def marketing_features():
    return render_template("marketing/features.html")


@app.route("/pricing")
def marketing_pricing():
    return render_template("marketing/pricing.html")


@app.route("/about")
def marketing_about():
    return render_template("marketing/about.html")


@app.route("/contact", methods=["GET", "POST"])
def marketing_contact():
    if request.method == "POST":
        return _handle_marketing_contact_post()
    return render_template("marketing/contact.html")


@app.route("/dashboard-preview")
def marketing_dashboard_preview():
    return render_template("marketing/dashboard_preview.html")


def _handle_marketing_contact_post():
    name = (request.form.get("name") or "").strip()
    email = (request.form.get("email") or "").strip()
    company = (request.form.get("company") or "").strip()
    message = (request.form.get("message") or "").strip()

    if not name or not email or not message:
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "error": "Please fill name, email, and message."}), 400
        flash("Please fill name, email, and message.", "error")
        return redirect(url_for("marketing_contact"))

    try:
        from database import save_contact_message

        save_contact_message(name, email, company, message)
    except Exception:
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": "Could not save your message. Please try again later.",
                    }
                ),
                503,
            )
        flash("Could not save your message. Please try again later.", "error")
        return redirect(url_for("marketing_contact"))

    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return jsonify({"ok": True, "message": "Thanks — we will get back to you shortly."})

    flash("Thanks — we will get back to you shortly.", "success")
    return redirect(url_for("marketing_contact"))


@app.route("/shop-login", methods=["GET", "POST"])
def public_shop_login():
    if request.method == "POST":
        shop_code = (request.form.get("shop_code") or "").strip()
        password = (request.form.get("shop_password") or "").strip()

        if not re.fullmatch(r"\d{6}", shop_code):
            flash("Shop code must be exactly 6 digits (numbers only).", "error")
            return redirect(url_for("public_shop_login"))

        if not password:
            flash("Enter shop password.", "error")
            return redirect(url_for("public_shop_login"))

        try:
            from database import get_shop_by_code

            shop = get_shop_by_code(shop_code)
        except Exception:
            shop = None

        if not shop:
            flash("Invalid shop code.", "error")
            return redirect(url_for("public_shop_login"))

        if shop.get("status") != "active":
            flash("This shop is suspended. Contact IT support.", "error")
            return redirect(url_for("public_shop_login"))

        if not check_password_hash(shop.get("shop_password_hash") or "", password):
            flash("Invalid shop password.", "error")
            return redirect(url_for("public_shop_login"))

        _clear_employee_portal_session()
        session["shop_id"] = int(shop["id"])
        session["shop_name"] = shop.get("shop_name")
        # Till devices stay signed in by default ("Remember this device" is
        # checked on the form); user can uncheck for a single-shift session.
        remember = (request.form.get("remember_device") or "").strip().lower() in {"1", "on", "true", "yes"}
        session.permanent = remember
        flash(f"Welcome to {shop.get('shop_name')}.", "success")
        return redirect(url_for("shop_pos", shop_id=int(shop["id"])))

    return render_template("public_shop_login.html")


@app.route("/login", methods=["GET", "POST"])
def employee_login():
    if session.get("employee_id"):
        next_q = (request.args.get("next") or "").strip()
        follow = _session_may_follow_login_next(next_q)
        if follow is not None:
            return follow
        return _redirect_to_employee_dashboard()

    if request.method == "POST":
        next_url = (request.form.get("next") or "").strip()
        code = (request.form.get("employee_code") or "").strip()
        password = request.form.get("password") or ""

        if not CODE_RE.match(code) or not password:
            flash("Enter your 6-digit code and password.", "error")
            return _redirect_login_preserving_next()

        try:
            from database import get_employee_by_code

            row = get_employee_by_code(code)
        except Exception:
            flash("Unable to sign in right now. Try again later.", "error")
            return _redirect_login_preserving_next()

        if not row or not check_password_hash(row["password_hash"], password):
            flash("Invalid employee code or password.", "error")
            return _redirect_login_preserving_next()

        status = row["status"]
        if status == "pending_approval":
            flash("Your account is pending approval. You will be notified when it is active.", "warning")
            return _redirect_login_preserving_next()
        if status == "suspended":
            flash("Your account is suspended. Contact your administrator.", "warning")
            return _redirect_login_preserving_next()
        if status != "active":
            flash("You cannot sign in with this account.", "warning")
            return _redirect_login_preserving_next()

        # Replace any shop-password session; employee login owns the session from here.
        session.pop("shop_id", None)
        session.pop("shop_name", None)

        session["employee_id"] = row["id"]
        session["employee_name"] = row["full_name"]
        session["employee_role"] = row.get("role") or "employee"
        role_key = session.get("employee_role") or "employee"
        _touch_employee_session_activity()

        _log_hr_activity_safe(
            "login",
            employee_id=row["id"],
            target_type="employee",
            target_id=int(row["id"]),
            description=f"Login by {row.get('full_name') or 'employee'}",
            employee_full_name=row.get("full_name"),
            employee_role=role_key,
        )

        # Honor the "Remember this device" checkbox so the session persists
        # across browser restarts and short offline periods.
        remember = (request.form.get("remember_device") or "").strip().lower() in {"1", "on", "true", "yes"}
        session.permanent = remember
        _flash_login_welcome(row.get("full_name") or "")

        if role_key in COMPANY_PORTAL_ROLES:
            if next_url.startswith("/") and not next_url.startswith("//"):
                path = urlparse(next_url).path.rstrip("/")
                if (
                    path == "/employee/dashboard"
                    or _is_role_dashboard_path(path)
                    or _is_legacy_employee_role_dashboard_path(path)
                ):
                    return redirect(url_for("employee_dashboard", role=role_key))
                return redirect(next_url)
            return redirect(url_for("employee_dashboard", role=role_key))

        # Allocated shop staff: company portal login opens their shop session.
        shop_id_raw = row.get("shop_id")
        try:
            alloc_shop_id = int(shop_id_raw) if shop_id_raw is not None else None
        except (TypeError, ValueError):
            alloc_shop_id = None
        if not alloc_shop_id or alloc_shop_id <= 0:
            flash("No shop is assigned to your account. Contact IT support.", "error")
            session.pop("employee_id", None)
            session.pop("employee_name", None)
            session.pop("employee_role", None)
            return _redirect_login_preserving_next()

        try:
            from database import get_shop_by_id

            alloc_shop = get_shop_by_id(alloc_shop_id)
        except Exception:
            alloc_shop = None
        if not alloc_shop or alloc_shop.get("status") != "active":
            flash("Your assigned shop is unavailable or suspended. Contact IT support.", "error")
            session.pop("employee_id", None)
            session.pop("employee_name", None)
            session.pop("employee_role", None)
            return _redirect_login_preserving_next()

        session["shop_id"] = int(alloc_shop["id"])
        session["shop_name"] = alloc_shop.get("shop_name")

        if next_url.startswith("/") and not next_url.startswith("//"):
            next_path = urlparse(next_url).path.rstrip("/")
            if next_path.startswith("/it_support"):
                flash("Signed in. That IT page is not available for your role.", "warning")
                return redirect(url_for("shop_dashboard", shop_id=alloc_shop_id))
            # If login started from POS, shop staff should land on their shop role page,
            # not be sent back to the POS screen immediately.
            if next_path == f"/shops/{alloc_shop_id}/shop-pos":
                return redirect(url_for("shop_dashboard", shop_id=alloc_shop_id))
            target_next_sid = _parse_next_shop_id(next_url)
            if target_next_sid is not None:
                try:
                    from database import employee_may_use_shop_branch

                    if not employee_may_use_shop_branch(row, target_next_sid):
                        flash("You don't have access to that branch.", "error")
                        return redirect(url_for("shop_dashboard", shop_id=alloc_shop_id))
                except Exception:
                    flash("You don't have access to that branch.", "error")
                    return redirect(url_for("shop_dashboard", shop_id=alloc_shop_id))
            return redirect(next_url)
        return redirect(url_for("shop_dashboard", shop_id=alloc_shop_id))

    return render_template("login.html")


@app.route("/reset-password", methods=["GET"])
def employee_reset_password():
    if session.get("employee_id"):
        return _redirect_to_employee_dashboard()
    prefill_code = (request.args.get("code") or "").strip()
    if prefill_code and not CODE_RE.match(prefill_code):
        prefill_code = ""
    return render_template("reset_password.html", prefill_code=prefill_code)


@app.route("/api/reset-password/email-for-code")
def api_reset_password_email_for_code():
    code = (request.args.get("code") or "").strip()
    if not CODE_RE.match(code):
        return jsonify({"ok": False, "email": None})
    try:
        from database import get_active_employee_email_for_code

        email = get_active_employee_email_for_code(code)
    except Exception:
        email = None
    if not email:
        return jsonify({"ok": False, "email": None})
    return jsonify({"ok": True, "email": email})


@app.route("/api/reset-password/verify-email", methods=["POST"])
def api_reset_password_verify_email():
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or request.form.get("email") or "").strip().lower()
    employee_code = (data.get("employee_code") or request.form.get("employee_code") or "").strip()
    if not email or "@" not in email:
        return jsonify({"ok": False, "registered": False, "error": "Enter a valid email address."}), 400
    if employee_code and not CODE_RE.match(employee_code):
        return jsonify({"ok": False, "registered": False, "error": "Employee code must be 6 digits."}), 400
    try:
        from database import get_active_employee_by_email, resolve_active_employee_for_password_reset

        row = get_active_employee_by_email(email)
        if not row:
            return jsonify({"ok": True, "registered": False, "message": "This email is not registered on an active account."})
        if employee_code:
            matched = resolve_active_employee_for_password_reset(email, employee_code)
            if not matched:
                return jsonify(
                    {
                        "ok": True,
                        "registered": True,
                        "matched": False,
                        "message": "This email is registered, but it does not match the employee code entered.",
                    }
                )
        return jsonify(
            {
                "ok": True,
                "registered": True,
                "matched": True,
                "full_name": (row.get("full_name") or "").strip(),
                "message": "Email verified. You can send a reset verification code.",
            }
        )
    except Exception:
        return jsonify({"ok": False, "error": "Could not verify email right now."}), 500


@app.route("/api/reset-password/send-code", methods=["POST"])
def api_reset_password_send_code():
    import secrets

    data = request.get_json(silent=True) or {}
    email = (data.get("email") or request.form.get("email") or "").strip().lower()
    employee_code = (data.get("employee_code") or request.form.get("employee_code") or "").strip()
    if not email or "@" not in email:
        return jsonify({"ok": False, "error": "Enter a valid email address."}), 400
    if employee_code and not CODE_RE.match(employee_code):
        return jsonify({"ok": False, "error": "Employee code must be 6 digits."}), 400
    try:
        from database import (
            create_employee_password_reset,
            recent_employee_password_reset_exists,
            resolve_active_employee_for_password_reset,
        )
        from mail_service import is_mail_configured, queue_password_reset_email

        row = resolve_active_employee_for_password_reset(email, employee_code or None)
        if not row:
            return jsonify({"ok": False, "error": "Email not found or does not match the employee code."}), 404
        if not is_mail_configured():
            return jsonify({"ok": False, "error": "Email is not configured on this system. Contact IT support."}), 503
        if recent_employee_password_reset_exists(int(row["id"]), within_seconds=60):
            return jsonify({"ok": False, "error": "Please wait a minute before requesting another code."}), 429

        code = f"{secrets.randbelow(1_000_000):06d}"
        code_hash = generate_password_hash(code)
        expires_at = datetime.now() + timedelta(minutes=15)
        if not create_employee_password_reset(int(row["id"]), email, code_hash, expires_at):
            return jsonify({"ok": False, "error": "Could not create reset code."}), 500

        company = (_load_company_identity_settings().get("company_name") or "Point of Sale").strip()
        queue_password_reset_email(
            to_email=email,
            full_name=(row.get("full_name") or "").strip(),
            verification_code=code,
            company_name=company,
            login_url=_employee_portal_login_url_external(),
        )
        return jsonify({"ok": True, "message": f"Verification code sent to {email}."})
    except Exception:
        logger.exception("Failed to send password reset code for %s", email)
        return jsonify({"ok": False, "error": "Could not send verification code."}), 500


@app.route("/api/reset-password/complete", methods=["POST"])
def api_reset_password_complete():
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or request.form.get("email") or "").strip().lower()
    employee_code = (data.get("employee_code") or request.form.get("employee_code") or "").strip()
    verification_code = (data.get("verification_code") or request.form.get("verification_code") or "").strip()
    password = data.get("password") or request.form.get("password") or ""
    confirm = data.get("confirm_password") or request.form.get("confirm_password") or ""

    if not email or "@" not in email:
        return jsonify({"ok": False, "error": "Enter a valid email address."}), 400
    if employee_code and not CODE_RE.match(employee_code):
        return jsonify({"ok": False, "error": "Employee code must be 6 digits."}), 400
    if not RESET_CODE_RE.match(verification_code):
        return jsonify({"ok": False, "error": "Enter the 6-digit verification code from your email."}), 400
    if not PASSWORD_RESET_RE.match(password):
        return jsonify({"ok": False, "error": "Password must be at least 6 characters."}), 400
    if password != confirm:
        return jsonify({"ok": False, "error": "Passwords do not match."}), 400

    try:
        from database import (
            mark_employee_password_reset_used,
            resolve_active_employee_for_password_reset,
            update_employee_password_hash,
            verify_employee_password_reset_code,
        )

        row = resolve_active_employee_for_password_reset(email, employee_code or None)
        if not row:
            return jsonify({"ok": False, "error": "Email not found or does not match the employee code."}), 404

        reset_id = verify_employee_password_reset_code(int(row["id"]), verification_code)
        if not reset_id:
            return jsonify({"ok": False, "error": "Invalid or expired verification code."}), 400

        pwd_hash = generate_password_hash(password)
        if not update_employee_password_hash(int(row["id"]), pwd_hash):
            return jsonify({"ok": False, "error": "Could not update password."}), 500
        mark_employee_password_reset_used(reset_id)
        return jsonify({"ok": True, "message": "Password updated. You can sign in now.", "login_url": url_for("employee_login")})
    except Exception:
        logger.exception("Password reset complete failed for %s", email)
        return jsonify({"ok": False, "error": "Could not reset password."}), 500


@app.route("/logout", methods=["POST", "GET"])
def employee_logout():
    had_employee = bool(session.get("employee_id"))
    idle_sign_out = (request.args.get("reason") or "").strip().lower() == "idle"
    next_url = (request.args.get("next") or "").strip()
    if idle_sign_out and not _safe_login_next(next_url):
        ref = (request.referrer or "").strip()
        if ref:
            parsed = urlparse(ref)
            candidate = parsed.path
            if parsed.query:
                candidate = f"{candidate}?{parsed.query}"
            login_path = urlparse(url_for("employee_login")).path
            if _safe_login_next(candidate) and candidate != login_path and not candidate.startswith("/logout"):
                next_url = candidate
    if had_employee:
        _log_hr_activity_safe(
            "logout",
            target_type="employee",
            target_id=session.get("employee_id"),
            description=(
                f"Automatic sign-out after {_EMPLOYEE_IDLE_MINUTES} minutes of inactivity"
                if idle_sign_out
                else f"Logout by {session.get('employee_name') or 'employee'}"
            ),
        )
    session.pop("employee_id", None)
    session.pop("employee_name", None)
    session.pop("employee_role", None)
    session.pop("shop_id", None)
    session.pop("shop_name", None)
    session.pop("shop_role_preview", None)
    session.pop("employee_last_activity", None)
    if idle_sign_out:
        _flash_idle_signout()
    else:
        flash("You have been signed out.", "success")
    if had_employee:
        if idle_sign_out:
            return redirect(_employee_login_url(next_url=next_url, reason="idle"))
        return redirect(url_for("employee_login"))
    return redirect(url_for("public_shop_login"))


@app.route("/session/employee-activity", methods=["POST"])
def employee_session_activity_ping():
    """Keep employee session alive after user confirms they are still present."""
    if not session.get("employee_id"):
        next_url = _request_continuation_url()
        return (
            jsonify(
                {
                    "ok": False,
                    "expired": True,
                    "login_url": _employee_login_url(next_url=next_url, reason="idle"),
                }
            ),
            401,
        )
    _touch_employee_session_activity()
    return jsonify(
        {
            "ok": True,
            "idle_seconds": EMPLOYEE_SESSION_IDLE_SECONDS,
            "warn_seconds": EMPLOYEE_SESSION_IDLE_WARN_SECONDS,
        }
    )


def _compose_signup_phone(country_code: str, local_number: str) -> tuple[str, str | None]:
    """Build E.164-style digits for employee signup (Kenya 254… preferred)."""
    from daraja_api import normalize_msisdn

    cc = re.sub(r"\D", "", (country_code or "").strip())
    local_digits = re.sub(r"\D", "", (local_number or "").strip())
    if not cc:
        return "", "Select your country code."
    if not local_digits:
        return "", "Enter your phone number."

    if cc == "254":
        candidate = local_digits
        if not candidate.startswith("254"):
            if candidate.startswith("0") and len(candidate) >= 10:
                candidate = "254" + candidate[1:11]
            elif len(candidate) == 9:
                candidate = "254" + candidate
            else:
                candidate = "254" + candidate.lstrip("0")
        normalized = normalize_msisdn(candidate)
        if not normalized or len(normalized) != 12 or not normalized.startswith("254"):
            return "", "Kenya number: use 07xxxxxxxx or 712345678 (saved as 254…)."
        return normalized, None

    local_clean = local_digits[1:] if local_digits.startswith("0") else local_digits
    if len(local_clean) < 6:
        return "", "Enter a valid phone number for the selected country."
    if len(local_clean) > 15:
        return "", "Phone number is too long."
    return cc + local_clean, None


def _compose_signup_email(local_part: str, domain: str, custom_domain: str = "") -> tuple[str, str | None]:
    """Build a full email from username + domain dropdown (Gmail default)."""
    local = (local_part or "").strip().lower()
    if not local:
        return "", "Enter your email username (the part before @)."
    if "@" in local:
        if re.fullmatch(r"^[^\s@]+@[^\s@]+\.[^\s@]+$", local):
            return local, None
        return "", "Enter a valid email address."

    raw_domain = (custom_domain if domain == "__custom__" else domain).strip().lower().lstrip("@")
    if not raw_domain:
        return "", "Select an email provider or enter a custom domain."
    if not re.fullmatch(r"^[a-z0-9.-]+\.[a-z]{2,}$", raw_domain):
        return "", "Enter a valid domain (e.g. gmail.com or company.co.ke)."
    return f"{local}@{raw_domain}", None


def _employee_portal_login_url_external() -> str:
    try:
        return url_for("employee_login", _external=True)
    except RuntimeError:
        return "/login"


def _flash_login_welcome(full_name: str) -> None:
    name = (full_name or "").strip()
    if name:
        flash(name, "login_welcome")


def _flash_idle_signout() -> None:
    flash(str(_EMPLOYEE_IDLE_MINUTES), "idle_signout")


def _notify_employee_signup_pending_email(full_name: str, email: str, employee_code: str) -> None:
    try:
        from mail_service import is_mail_configured, queue_signup_pending_email

        if not is_mail_configured():
            return
        company = (_load_company_identity_settings().get("company_name") or "Point of Sale").strip()
        queue_signup_pending_email(
            to_email=email,
            full_name=full_name,
            employee_code=employee_code,
            company_name=company,
            login_url=_employee_portal_login_url_external(),
        )
    except Exception:
        logger.exception("Could not queue signup pending email for %s", email)


def _notify_employee_approved_email(emp: dict, role_key: str) -> None:
    try:
        from mail_service import is_mail_configured, queue_approval_email

        if not is_mail_configured():
            return
        to_email = (emp.get("email") or "").strip()
        if not to_email or "@" not in to_email:
            return
        company = (_load_company_identity_settings().get("company_name") or "Point of Sale").strip()
        role_label = ROLE_LABELS.get((role_key or "").strip().lower(), role_key or "Employee")
        queue_approval_email(
            to_email=to_email,
            full_name=(emp.get("full_name") or "").strip(),
            employee_code=(emp.get("employee_code") or "").strip(),
            role_label=role_label,
            company_name=company,
            login_url=_employee_portal_login_url_external(),
        )
    except Exception:
        logger.exception("Could not queue approval email for employee id %s", emp.get("id"))


@app.route("/signup", methods=["GET", "POST"])
def employee_signup():
    if session.get("employee_id"):
        return _redirect_to_employee_dashboard()

    if request.method == "POST":
        full_name = (request.form.get("full_name") or "").strip().upper()
        email_local = (request.form.get("email_local") or "").strip()
        email_domain = (request.form.get("email_domain") or "gmail.com").strip()
        email_domain_custom = (request.form.get("email_domain_custom") or "").strip()
        email_fallback = (request.form.get("email") or "").strip()
        phone_country = (request.form.get("phone_country") or "254").strip()
        phone_local = (request.form.get("phone_local") or "").strip()
        phone_fallback = (request.form.get("phone") or "").strip()
        code = (request.form.get("employee_code") or "").strip()
        password = request.form.get("password") or ""
        confirm = request.form.get("confirm_password") or ""

        errors = []
        if len(full_name) < 2:
            errors.append("Enter your full name.")
        email = ""
        if email_local or email_domain_custom:
            email, email_err = _compose_signup_email(email_local, email_domain, email_domain_custom)
            if email_err:
                errors.append(email_err)
        else:
            email = email_fallback.lower()
            if not email or "@" not in email:
                errors.append("Enter a valid email.")
        phone = ""
        if phone_local:
            phone, phone_err = _compose_signup_phone(phone_country, phone_local)
            if phone_err:
                errors.append(phone_err)
        else:
            from daraja_api import normalize_msisdn

            phone = normalize_msisdn(phone_fallback) or _normalize_storefront_phone(phone_fallback)
            if len(phone) < 10:
                errors.append("Enter a valid phone number.")
        if not CODE_RE.match(code):
            errors.append("Employee code must be exactly 6 digits.")
        if not PASSWORD_DIGITS_RE.match(password):
            errors.append("Password must be at least 6 digits (numbers only).")
        if password != confirm:
            errors.append("Passwords do not match.")

        profile_path = None
        f = request.files.get("profile")
        if f and f.filename:
            profile_path = _save_profile_upload(f)
            if profile_path is None:
                errors.append("Profile image must be PNG, JPG, GIF, or WebP.")

        if errors:
            for e in errors:
                flash(e, "error")
            return redirect(url_for("employee_signup"))

        try:
            from database import create_employee_pending, email_available, employee_code_available

            if not employee_code_available(code):
                flash("This 6-digit code is already taken. Choose another.", "error")
                return redirect(url_for("employee_signup"))
            if not email_available(email):
                flash("This email is already registered.", "error")
                return redirect(url_for("employee_signup"))

            pwd_hash = generate_password_hash(password)
            create_employee_pending(full_name, email, phone, code, pwd_hash, profile_path)
            _notify_employee_signup_pending_email(full_name, email, code)
        except Exception:
            flash("Could not complete registration. Check database settings or try again later.", "error")
            return redirect(url_for("employee_signup"))

        flash_msg = (
            "Registration submitted. Your status is pending approval — an administrator will activate your account."
        )
        try:
            from mail_service import is_mail_configured

            if is_mail_configured():
                flash_msg += f" A confirmation email was sent to {email}."
        except Exception:
            pass
        flash(flash_msg, "success")
        return redirect(url_for("employee_login"))

    return render_template(
        "signup.html",
        phone_countries=SIGNUP_PHONE_COUNTRIES,
        email_domains=SIGNUP_EMAIL_DOMAINS,
    )


@app.route("/profile/settings", methods=["GET", "POST"])
@login_required
def employee_profile_settings():
    emp_id = session.get("employee_id")
    try:
        from database import email_taken_by_other, get_employee_by_id, update_employee_profile

        row = get_employee_by_id(emp_id)
    except Exception:
        row = None

    if not row:
        session.clear()
        flash("Session expired. Please sign in again.", "error")
        return redirect(url_for("employee_login"))

    role_key = row.get("role") or "employee"
    if role_key not in VALID_ROLES:
        role_key = "employee"

    if request.method == "POST":
        full_name = (request.form.get("full_name") or "").strip().upper()
        email = (request.form.get("email") or "").strip()
        phone = (request.form.get("phone") or "").strip()
        remove_photo = request.form.get("remove_photo") == "1"

        errors = []
        if len(full_name) < 2:
            errors.append("Enter your full name.")
        if not email or "@" not in email:
            errors.append("Enter a valid email.")
        if len(phone) < 8:
            errors.append("Enter a valid phone number.")

        if not errors and email_taken_by_other(email, row["id"]):
            errors.append("That email is already in use by another account.")

        if errors:
            for e in errors:
                flash(e, "error")
            return redirect(url_for("employee_profile_settings"))

        f = request.files.get("profile")
        new_upload = _save_profile_upload(f) if f and f.filename else None
        if f and f.filename and new_upload is None:
            flash("Profile image must be PNG, JPG, GIF, or WebP.", "error")
            return redirect(url_for("employee_profile_settings"))

        if new_upload is not None:
            ok = update_employee_profile(
                row["id"], full_name, email, phone, profile_image=new_upload
            )
        elif remove_photo:
            ok = update_employee_profile(
                row["id"], full_name, email, phone, profile_image=None
            )
        else:
            ok = update_employee_profile(row["id"], full_name, email, phone)

        if not ok:
            flash("Could not update profile. Try again later.", "error")
            return redirect(url_for("employee_profile_settings"))

        session["employee_name"] = full_name
        session["employee_role"] = role_key
        flash("Profile updated.", "success")
        return redirect(url_for("employee_profile_settings"))

    return render_template("profile_settings.html", employee=row)


@app.route("/employee/<role>/dashboard")
def employee_dashboard_legacy_prefix(role):
    """Rewrite old /employee/<role>/dashboard URLs to /<role>/dashboard."""
    if role not in VALID_ROLES:
        abort(404)
    return redirect(url_for("employee_dashboard", role=role), code=301)


@app.route("/signup/check-code")
def check_employee_code():
    code = (request.args.get("code") or "").strip()
    if not CODE_RE.match(code):
        return jsonify({"ok": False, "available": False, "message": "Enter a valid 6-digit code."}), 400
    try:
        from database import employee_code_available

        available = employee_code_available(code)
        return jsonify({"ok": True, "available": available})
    except Exception:
        return jsonify({"ok": False, "message": "Could not verify code availability."}), 503


@app.route("/<role>/dashboard")
@login_required
def employee_dashboard(role):
    if role not in VALID_ROLES:
        abort(404)

    emp_id = session.get("employee_id")
    try:
        from database import get_employee_by_id

        row = get_employee_by_id(emp_id)
    except Exception:
        row = None

    if not row:
        session.clear()
        flash("Session expired. Please sign in again.", "error")
        return redirect(url_for("employee_login"))

    role_key = row.get("role") or "employee"
    if role_key not in VALID_ROLES:
        role_key = "employee"
    if role != role_key:
        return redirect(url_for("employee_dashboard", role=role_key))

    session["employee_role"] = role_key
    session["employee_name"] = row["full_name"]
    return render_template("employee_dashboard.html")


def _normalize_receipt_width_value(raw: object) -> str:
    """Canonical thermal width for layout: ``58mm`` (narrow / ~50–58mm rolls) or ``80mm`` (wide)."""
    s = str(raw or "").strip().lower()
    if s in ("58mm", "58", "50mm", "50", "narrow", "48mm", "2in", "2"):
        return "58mm"
    return "80mm"


def _default_receipt_settings() -> dict:
    return {
        "receipt_width": "80mm",
        "font_size": "12pt",
        "bold_headers": True,
        "receipt_number_format": "sequential",
        "receipt_number_prefix": "T",
        "starting_number": "1001",
        "show_payment_details": False,
        "payment_detail_type": "buy_goods",
        "payment_detail_text": "",
        "include_tax": False,
        "tax_percent": "",
        "show_logo": True,
        "show_address": True,
        "show_contact": True,
        "show_server": True,
        "show_datetime": True,
        "receipt_header": "",
        "receipt_footer": "",
        "receipt_qr_enabled": False,
        "receipt_qr_mode": "receipt_details",
        "receipt_qr_website_url": "",
    }


def _load_receipt_settings() -> dict:
    from database import get_site_settings

    defaults = _default_receipt_settings()
    raw = get_site_settings(["receipt_settings_json"]).get("receipt_settings_json") or "{}"
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        data = {}
    if not isinstance(data, dict):
        data = {}
    merged = {**defaults, **data}
    for k in (
        "bold_headers",
        "show_payment_details",
        "include_tax",
        "show_logo",
        "show_address",
        "show_contact",
        "show_server",
        "show_datetime",
        "receipt_qr_enabled",
    ):
        merged[k] = merged.get(k) in (True, "true", "1", 1, "True")
    qm = str(merged.get("receipt_qr_mode") or "receipt_details").strip()
    if qm not in ("website", "receipt_details"):
        qm = "receipt_details"
    merged["receipt_qr_mode"] = qm
    merged["receipt_width"] = _normalize_receipt_width_value(merged.get("receipt_width"))
    return merged


def _parse_shop_settings_json(raw) -> Optional[dict]:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s or s == "{}":
        return None
    try:
        data = json.loads(s)
    except (json.JSONDecodeError, TypeError):
        return None
    return data if isinstance(data, dict) else None


SHOP_STOCK_WORKSPACE_SETTING_KEYS = (
    "require_request_stock_notes",
    "require_manual_in_supplier",
    "require_manual_in_notes",
    "require_manual_out_refund",
    "require_manual_out_notes",
)


def _default_shop_stock_workspace_settings() -> dict:
    return {k: False for k in SHOP_STOCK_WORKSPACE_SETTING_KEYS}


def _load_shop_stock_workspace_settings(shop: dict) -> dict:
    out = _default_shop_stock_workspace_settings()
    data = _parse_shop_settings_json((shop or {}).get("stock_workspace_settings_json"))
    if not data:
        return out
    for key in SHOP_STOCK_WORKSPACE_SETTING_KEYS:
        if key in data:
            out[key] = bool(data[key])
    return out


def _shop_stock_workspace_settings_from_form() -> dict:
    out = _default_shop_stock_workspace_settings()
    for key in SHOP_STOCK_WORKSPACE_SETTING_KEYS:
        out[key] = (request.form.get(key) or "").strip() == "1"
    return out


def _save_shop_stock_workspace_settings(shop_id: int, settings: dict) -> bool:
    from database import update_shop_stock_workspace_settings_json

    payload = {k: bool((settings or {}).get(k)) for k in SHOP_STOCK_WORKSPACE_SETTING_KEYS}
    return update_shop_stock_workspace_settings_json(
        shop_id,
        json.dumps(payload, separators=(",", ":")),
    )


def _shop_has_printing_override(shop: dict) -> bool:
    return _parse_shop_settings_json(shop.get("printing_settings_json")) is not None


def _shop_has_receipt_override(shop: dict) -> bool:
    return _parse_shop_settings_json(shop.get("receipt_settings_json")) is not None


def _shop_has_appearance_override(shop: dict) -> bool:
    return _parse_shop_settings_json(shop.get("appearance_settings_json")) is not None


def _load_company_appearance_settings() -> dict:
    from theme_presets import DEFAULT_FONT_FAMILY, normalize_default_theme, normalize_font_family

    defaults = {
        "default_theme": "dark",
        "font_family": DEFAULT_FONT_FAMILY,
        "primary_color": "#f97316",
        "accent_color": "#fb923c",
    }
    try:
        from database import get_site_settings

        stored = get_site_settings(["default_theme", "font_family", "primary_color", "accent_color"]) or {}
    except Exception:
        stored = {}
    return {
        "default_theme": normalize_default_theme(stored.get("default_theme") or defaults["default_theme"]),
        "font_family": normalize_font_family(stored.get("font_family") or defaults["font_family"]),
        "primary_color": (stored.get("primary_color") or defaults["primary_color"]).strip() or defaults["primary_color"],
        "accent_color": (stored.get("accent_color") or defaults["accent_color"]).strip() or defaults["accent_color"],
    }


def _effective_appearance_settings_for_shop(shop: dict) -> dict:
    base = _load_company_appearance_settings()
    data = _parse_shop_settings_json(shop.get("appearance_settings_json"))
    if not data:
        return base
    merged = {**base, **data}
    from theme_presets import normalize_default_theme, normalize_font_family

    merged["default_theme"] = normalize_default_theme(merged.get("default_theme"))
    merged["font_family"] = normalize_font_family(merged.get("font_family"))
    for k in ("primary_color", "accent_color"):
        v = (merged.get(k) or "").strip()
        if not re.match(r"^#[0-9a-fA-F]{6}$", v):
            merged[k] = base[k]
    return merged


def _shop_theme_template_vars(shop: dict) -> dict:
    eff = _effective_appearance_settings_for_shop(shop)
    pri = eff.get("primary_color") or "#f97316"
    acc = eff.get("accent_color") or "#fb923c"
    sid = shop["id"]
    return {
        "theme_key": f"richcom-theme-shop-{sid}",
        "theme_default": eff.get("default_theme") or "dark",
        "font_family": eff.get("font_family") or "Plus Jakarta Sans",
        "primary_color_rgb": _hex_to_rgb_triplet(pri),
        "accent_color_rgb": _hex_to_rgb_triplet(acc),
    }


_COMPANY_IDENTITY_KEYS = (
    "company_name",
    "company_email",
    "company_phone",
    "company_facebook",
    "company_instagram",
    "company_twitter",
    "company_tiktok",
    "public_app_url",
    "company_location_name",
    "company_latitude",
    "company_longitude",
)

COMPANY_OPENING_HOURS_JSON_KEY = "company_opening_hours_json"
COMPANY_OPENING_HOURS_SHOP_KEY = "opening_hours"

COMPANY_WEEKDAYS = (
    ("monday", "Monday"),
    ("tuesday", "Tuesday"),
    ("wednesday", "Wednesday"),
    ("thursday", "Thursday"),
    ("friday", "Friday"),
    ("saturday", "Saturday"),
    ("sunday", "Sunday"),
)


def _google_maps_api_key() -> str:
    return (os.getenv("GOOGLE_MAPS_API_KEY") or "").strip()


def _parse_latitude_from_form(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    try:
        v = float(s)
    except (TypeError, ValueError):
        return ""
    if v < -90 or v > 90:
        return ""
    return f"{v:.7f}".rstrip("0").rstrip(".") or "0"


def _parse_longitude_from_form(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    try:
        v = float(s)
    except (TypeError, ValueError):
        return ""
    if v < -180 or v > 180:
        return ""
    return f"{v:.7f}".rstrip("0").rstrip(".") or "0"


def _location_settings_from_form() -> dict:
    return {
        "company_location_name": (request.form.get("company_location_name") or "").strip(),
        "company_latitude": _parse_latitude_from_form(request.form.get("company_latitude") or ""),
        "company_longitude": _parse_longitude_from_form(request.form.get("company_longitude") or ""),
    }


def _default_company_opening_hours() -> dict:
    return {
        "open_24_hours": True,
        "days": {day: {"open": "", "close": "", "closed": False} for day, _ in COMPANY_WEEKDAYS},
    }


def _parse_company_time_hhmm(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    parts = s.split(":", 1)
    if len(parts) != 2:
        return ""
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except (TypeError, ValueError):
        return ""
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        return ""
    return f"{hour:02d}:{minute:02d}"


def _parse_company_opening_hours_day_row(row) -> dict:
    if not isinstance(row, dict):
        return {"open": "", "close": "", "closed": False}
    closed = row.get("closed") in (True, "true", "1", 1, "True")
    open_t = _parse_company_time_hhmm(str(row.get("open") or ""))
    close_t = _parse_company_time_hhmm(str(row.get("close") or ""))
    if closed:
        open_t, close_t = "", ""
    return {"open": open_t, "close": close_t, "closed": closed}


def _normalize_opening_hours_data(data) -> dict:
    defaults = _default_company_opening_hours()
    if not isinstance(data, dict):
        return defaults

    open_24 = defaults["open_24_hours"]
    if "open_24_hours" in data:
        open_24 = data.get("open_24_hours") in (True, "true", "1", 1, "True")

    days_out = dict(defaults["days"])
    day_source = data.get("days") if isinstance(data.get("days"), dict) else {}
    if not day_source:
        for day, _ in COMPANY_WEEKDAYS:
            if day in data:
                day_source[day] = data.get(day)
    for day, _ in COMPANY_WEEKDAYS:
        if day in day_source:
            days_out[day] = _parse_company_opening_hours_day_row(day_source.get(day))

    result = {"open_24_hours": open_24, "days": days_out}
    if _opening_hours_has_scheduled_days(result):
        result["open_24_hours"] = False
    else:
        result["open_24_hours"] = True
    return result


def _load_company_opening_hours() -> dict:
    try:
        from database import get_site_settings

        raw = get_site_settings([COMPANY_OPENING_HOURS_JSON_KEY]).get(COMPANY_OPENING_HOURS_JSON_KEY) or "{}"
    except Exception:
        raw = "{}"
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        data = {}
    return _normalize_opening_hours_data(data)


def _opening_hours_has_scheduled_days(hours: dict) -> bool:
    days = hours.get("days") if isinstance(hours.get("days"), dict) else {}
    for day, _ in COMPANY_WEEKDAYS:
        row = _parse_company_opening_hours_day_row(days.get(day))
        if row.get("open") and row.get("close"):
            return True
    return False


def _effective_open_24_hours_flag(hours: dict) -> bool:
    """True when no per-day open/close windows are configured (defaults to 24/7)."""
    return not _opening_hours_has_scheduled_days(hours)


def _opening_hours_from_form(existing: dict | None = None) -> dict:
    existing = existing if isinstance(existing, dict) else {}
    fallback_days = existing.get("days") if isinstance(existing.get("days"), dict) else None
    days = _company_opening_hours_days_from_form(fallback_days)
    has_schedule = _opening_hours_has_scheduled_days({"days": days})

    open_24_raw = request.form.get("hours_open_24_hours")
    if has_schedule:
        open_24 = False
    elif open_24_raw is None:
        open_24 = bool(existing.get("open_24_hours", True))
    else:
        open_24 = open_24_raw.strip() == "1"

    if open_24:
        seed_days = existing.get("days") if isinstance(existing.get("days"), dict) else None
        if not isinstance(seed_days, dict) or not seed_days:
            seed_days = _default_company_opening_hours()["days"]
        return {"open_24_hours": True, "days": seed_days}
    return {"open_24_hours": False, "days": days}


def _company_opening_hours_from_form() -> dict:
    return _opening_hours_from_form(_load_company_opening_hours())


def _load_shop_opening_hours_override(shop: dict) -> dict | None:
    data = _parse_shop_settings_json(shop.get("company_settings_json"))
    if not isinstance(data, dict):
        return None
    blob = data.get(COMPANY_OPENING_HOURS_SHOP_KEY)
    if blob is None:
        return None
    if isinstance(blob, str):
        try:
            blob = json.loads(blob)
        except (json.JSONDecodeError, TypeError):
            return None
    return _normalize_opening_hours_data(blob)


def _shop_has_opening_hours_override(shop: dict) -> bool:
    return _load_shop_opening_hours_override(shop) is not None


def _effective_opening_hours_for_shop(shop: dict) -> dict:
    override = _load_shop_opening_hours_override(shop)
    if override is not None:
        return override
    return _load_company_opening_hours()


def _shop_opening_hours_for_form(shop: dict) -> dict:
    override = _load_shop_opening_hours_override(shop)
    if override is not None:
        return override
    return _load_company_opening_hours()


def _shop_company_settings_payload_from_form(shop: dict) -> dict | None:
    use_custom = (request.form.get("shop_company_custom") or "").strip() == "1"
    use_hours_custom = (request.form.get("shop_opening_hours_custom") or "").strip() == "1"
    if not use_custom and not use_hours_custom:
        return None

    payload: dict = {}
    if use_custom:
        payload.update(
            {
                "company_name": (request.form.get("company_name") or "").strip() or "Point of Sale",
                "company_email": (request.form.get("company_email") or "").strip(),
                "company_phone": (request.form.get("company_phone") or "").strip(),
                "company_facebook": (request.form.get("company_facebook") or "").strip(),
                "company_instagram": (request.form.get("company_instagram") or "").strip(),
                "company_twitter": (request.form.get("company_twitter") or "").strip(),
                "company_tiktok": (request.form.get("company_tiktok") or "").strip(),
                **_location_settings_from_form(),
            }
        )
    if use_hours_custom:
        seed = _load_shop_opening_hours_override(shop) or _load_company_opening_hours()
        payload[COMPANY_OPENING_HOURS_SHOP_KEY] = _opening_hours_from_form(seed)
    return payload


def _company_opening_hours_days_from_form(fallback: dict | None = None) -> dict:
    days = {}
    fallback = fallback if isinstance(fallback, dict) else {}
    for day, _ in COMPANY_WEEKDAYS:
        open_key = f"hours_{day}_open"
        close_key = f"hours_{day}_close"
        closed_key = f"hours_{day}_closed"
        if (
            open_key not in request.form
            and close_key not in request.form
            and closed_key not in request.form
        ):
            days[day] = _parse_company_opening_hours_day_row(fallback.get(day))
            continue
        closed = (request.form.get(closed_key) or "").strip() == "1"
        open_t = _parse_company_time_hhmm(request.form.get(open_key) or "")
        close_t = _parse_company_time_hhmm(request.form.get(close_key) or "")
        if closed:
            open_t, close_t = "", ""
        days[day] = {"open": open_t, "close": close_t, "closed": closed}
    return days


def _time_hhmm_to_minutes(raw: str) -> int | None:
    parsed = _parse_company_time_hhmm(raw)
    if not parsed:
        return None
    hour, minute = parsed.split(":", 1)
    return int(hour) * 60 + int(minute)


def _shop_opening_hours_status(shop: dict, at: datetime | None = None) -> dict:
    now = at or datetime.now()
    hours = _effective_opening_hours_for_shop(shop)
    source = "shop" if _shop_has_opening_hours_override(shop) else "company"
    day_key, day_label = COMPANY_WEEKDAYS[now.weekday()]
    base = {
        "source": source,
        "open_24_hours": _effective_open_24_hours_flag(hours),
        "today_label": day_label,
        "today_key": day_key,
        "checked_at": now.strftime("%Y-%m-%dT%H:%M:%S"),
    }

    if _effective_open_24_hours_flag(hours):
        return {
            **base,
            "is_open_now": True,
            "status": "open_24",
            "message": "Open 24 hours",
            "today_hours": None,
        }

    day_row = (hours.get("days") or {}).get(day_key) or {}
    today_hours = _parse_company_opening_hours_day_row(day_row)
    if today_hours.get("closed"):
        return {
            **base,
            "open_24_hours": False,
            "is_open_now": False,
            "status": "closed_today",
            "message": f"Closed today ({day_label})",
            "today_hours": today_hours,
        }

    open_t = today_hours.get("open") or ""
    close_t = today_hours.get("close") or ""
    if not open_t or not close_t:
        return {
            **base,
            "open_24_hours": True,
            "is_open_now": True,
            "status": "open_24_day",
            "message": f"Open 24 hours ({day_label})",
            "today_hours": today_hours,
        }

    open_m = _time_hhmm_to_minutes(open_t)
    close_m = _time_hhmm_to_minutes(close_t)
    now_m = now.hour * 60 + now.minute
    if open_m is None or close_m is None:
        return {
            **base,
            "open_24_hours": False,
            "is_open_now": False,
            "status": "invalid_hours",
            "message": "Closed — opening hours are not configured correctly",
            "today_hours": today_hours,
        }

    if open_m == close_m:
        return {
            **base,
            "open_24_hours": False,
            "is_open_now": False,
            "status": "closed_today",
            "message": f"Closed today ({day_label})",
            "today_hours": today_hours,
        }

    if open_m < close_m:
        is_open = open_m <= now_m < close_m
        if is_open:
            message = f"Open until {close_t}"
            status = "open"
        elif now_m < open_m:
            message = f"Closed — opens at {open_t}"
            status = "before_open"
        else:
            message = f"Closed — today's hours were {open_t}–{close_t}"
            status = "after_close"
    else:
        is_open = now_m >= open_m or now_m < close_m
        if is_open:
            message = f"Open until {close_t}"
            status = "open_overnight"
        else:
            message = f"Closed — opens at {open_t}"
            status = "before_open_overnight"

    return {
        **base,
        "open_24_hours": False,
        "is_open_now": is_open,
        "status": status,
        "message": message,
        "today_hours": today_hours,
    }


def _shop_pos_till_allows_sales(shop: dict) -> tuple[bool, str]:
    """Till open: today's opening submitted, not end-of-day closed, no pending prior closing."""
    from database import get_pending_shop_day_closing, get_shop_day_opening

    shop_id = int(shop.get("id") or 0)
    if shop_id <= 0:
        return True, ""
    pending = get_pending_shop_day_closing(shop_id)
    if pending:
        label = pending.get("label") or pending.get("business_date") or "a previous day"
        return (
            False,
            f"Closing balances for {label} must be submitted before sales can start.",
        )
    today_opening = get_shop_day_opening(shop_id, date.today())
    if not today_opening:
        return (
            False,
            "Opening cash and M-Pesa balances must be set by a shop admin or manager before sales can start.",
        )
    if today_opening.get("closing_submitted_at"):
        return (
            False,
            "Shop closed for today. Closing balances were submitted — sales resume after the next opening.",
        )
    return True, ""


def _shop_pos_schedule_allows_sales(shop: dict) -> tuple[bool, str]:
    """Scheduled hours: no times configured = 24/7; set times apply for that day."""
    hours = _effective_opening_hours_for_shop(shop)
    if not _opening_hours_has_scheduled_days(hours):
        return True, ""
    status = _shop_opening_hours_status(shop)
    if status.get("is_open_now"):
        return True, ""
    return False, str(status.get("message") or "Shop is closed outside opening hours.")


def _shop_pos_sales_allowed(shop: dict) -> tuple[bool, str]:
    """Sales allowed when till is open and (if hours are set) within today's schedule."""
    ok, msg = _shop_pos_till_allows_sales(shop)
    if not ok:
        return False, msg
    ok, msg = _shop_pos_schedule_allows_sales(shop)
    if not ok:
        return False, msg
    return True, ""


def _shop_session_can_submit_day_opening() -> bool:
    er = str(session.get("employee_role") or "").strip().lower()
    if er in COMPANY_PORTAL_ROLES:
        return True
    return _session_shop_shell_role_key() in SHOP_DAY_OPENING_SUBMIT_ROLES


def _shop_session_can_submit_day_closing() -> bool:
    er = str(session.get("employee_role") or "").strip().lower()
    if er in COMPANY_PORTAL_ROLES:
        return True
    return _session_shop_shell_role_key() in SHOP_DAY_CLOSING_SUBMIT_ROLES


def _shop_day_opening_requires_stock_confirmation(shop: dict) -> bool:
    """Stock confirmation in the opening popup applies when POS uses shop shelf stock."""
    return _pos_inventory_mode(shop) in ("shop", "both")


def _shop_closing_reminder_window(shop: dict, at: datetime | None = None) -> dict:
    """True when current time is within one hour before today's configured closing time."""
    now = at or datetime.now()
    hours = _effective_opening_hours_for_shop(shop)
    if _effective_open_24_hours_flag(hours):
        return {"active": False, "close_time": "", "minutes_until_close": None}

    status = _shop_opening_hours_status(shop, at=now)
    today_hours = status.get("today_hours") or {}
    if today_hours.get("closed"):
        return {"active": False, "close_time": "", "minutes_until_close": None}

    close_t = (today_hours.get("close") or "").strip()
    open_t = (today_hours.get("open") or "").strip()
    close_m = _time_hhmm_to_minutes(close_t)
    open_m = _time_hhmm_to_minutes(open_t)
    if close_m is None or open_m is None or not close_t:
        return {"active": False, "close_time": close_t, "minutes_until_close": None}

    now_m = now.hour * 60 + now.minute
    minutes_until: int | None

    if open_m < close_m:
        minutes_until = close_m - now_m
        active = now_m >= (close_m - 60)
    elif status.get("is_open_now"):
        if now_m >= open_m:
            minutes_until = (24 * 60 - now_m) + close_m
        else:
            minutes_until = close_m - now_m
        active = minutes_until is not None and minutes_until <= 60
    else:
        minutes_until = None
        active = False

    return {
        "active": active,
        "close_time": close_t,
        "open_time": open_t,
        "minutes_until_close": minutes_until if minutes_until is not None else None,
    }


def _shop_day_closing_payload(shop: dict, business_date) -> dict | None:
    from database import get_shop_day_closing_context

    shop_id = int(shop.get("id") or 0)
    if shop_id <= 0:
        return None
    ctx = get_shop_day_closing_context(shop_id, business_date)
    if not ctx:
        return None
    return ctx


def _shop_day_closing_reminder_payload(shop: dict) -> dict:
    """End-of-day closing for today (admin/manager). Manual open only — no auto popup."""
    shop_id = int(shop.get("id") or 0)
    biz = date.today()
    can_submit = _shop_session_can_submit_day_closing()
    ctx = _shop_day_closing_payload(shop, biz)
    window = _shop_closing_reminder_window(shop)
    pending = ctx if ctx and not ctx.get("submitted") else None
    can_close_today = bool(can_submit and pending)
    return {
        "business_date": biz.isoformat(),
        "can_submit": can_submit,
        "can_close_today": can_close_today,
        "requires_employee_code": True,
        "pending": pending,
        "auto_show": False,
        "close_time": window.get("close_time") or "",
        "minutes_until_close": window.get("minutes_until_close"),
    }


def _shop_day_opening_boot_payload(shop: dict) -> dict:
    from database import get_pending_shop_day_closing, get_shop_day_opening

    shop_id = int(shop.get("id") or 0)
    biz = date.today()
    record = get_shop_day_opening(shop_id, biz) if shop_id > 0 else None
    pending_closing = get_pending_shop_day_closing(shop_id) if shop_id > 0 else None
    rec_out = None
    if record:
        rec_out = {
            "opening_cash": float(record.get("opening_cash") or 0),
            "opening_mpesa": float(record.get("opening_mpesa") or 0),
            "stock_confirmed": bool(record.get("stock_confirmed")),
            "submitted_by_name": record.get("submitted_by_name") or "",
            "submitted_at": record.get("created_at") or "",
        }
    opening_completed = record is not None
    today_closing_submitted = bool(record and record.get("closing_submitted_at"))
    ready_for_sales = opening_completed and not pending_closing and not today_closing_submitted
    sales_allowed, sales_blocked_message = _shop_pos_sales_allowed(shop)
    closing_rec_out = None
    if today_closing_submitted and record:
        closing_rec_out = {
            "closing_cash": float(record.get("closing_cash") or 0),
            "closing_mpesa": float(record.get("closing_mpesa") or 0),
            "submitted_by_name": record.get("closing_submitted_by_name") or "",
            "submitted_at": record.get("closing_submitted_at") or "",
            "business_date": record.get("business_date") or biz.isoformat(),
        }
    return {
        "business_date": biz.isoformat(),
        "completed": opening_completed,
        "today_closed": today_closing_submitted,
        "ready_for_sales": ready_for_sales,
        "sales_allowed": sales_allowed,
        "sales_blocked_message": sales_blocked_message,
        "can_submit": _shop_session_can_submit_day_opening(),
        "requires_employee_code": True,
        "requires_stock_confirmation": _shop_day_opening_requires_stock_confirmation(shop),
        "inventory_mode": _pos_inventory_mode(shop),
        "record": rec_out,
        "closing_record": closing_rec_out,
        "pending_closing": pending_closing,
        "closing_reminder": _shop_day_closing_reminder_payload(shop),
    }


def _shop_till_day_summary(shop: dict) -> dict:
    """Compact till open/closed state for shop list UIs."""
    boot = _shop_day_opening_boot_payload(shop)
    pending = boot.get("pending_closing")
    today_closed = boot.get("today_closed")
    completed = boot.get("completed")
    if pending:
        state, label = "pending_closing", "Previous closing due"
        can_open_till = True
        can_close_till = False
    elif today_closed:
        state, label = "closed", "Closed for today"
        can_open_till = True
        can_close_till = False
    elif completed:
        state, label = "open", "Open"
        can_open_till = False
        can_close_till = True
    else:
        state, label = "not_opened", "Not opened"
        can_open_till = True
        can_close_till = False
    cr = boot.get("closing_reminder") or {}
    closing_ctx = cr.get("pending") if isinstance(cr, dict) else None
    if boot.get("today_closed") and boot.get("closing_record"):
        closing_ctx = boot.get("closing_record")
    return {
        "till_state": state,
        "till_label": label,
        "can_open_till": can_open_till,
        "can_close_till": can_close_till,
        "requires_stock_confirmation": boot.get("requires_stock_confirmation") is True,
        "inventory_mode": boot.get("inventory_mode") or "shop",
        "pending_closing": pending,
        "closing_context": closing_ctx,
        "opening_record": boot.get("record"),
        "closing_record": boot.get("closing_record"),
        "business_date": boot.get("business_date") or date.today().isoformat(),
    }


def _resolve_it_portal_day_submitter() -> Tuple[Optional[dict], Optional[str], int]:
    """IT / company portal session user for till day actions (no 6-digit code)."""
    uid = session.get("employee_id")
    if not uid:
        return None, "Sign in required.", 403
    try:
        from database import get_employee_by_id

        row = get_employee_by_id(int(uid))
    except Exception:
        row = None
    if not row:
        return None, "Session employee not found.", 403
    role = str(row.get("role") or "").strip().lower()
    if role not in COMPANY_PORTAL_ROLES:
        return None, "Only company portal staff can manage till day status.", 403
    return row, None, 200


def _apply_shop_day_opening_or_reopen(
    shop: dict,
    shop_id: int,
    *,
    opening_cash: float,
    opening_mpesa: float,
    stock_confirmed: bool,
    emp_row: Optional[dict],
    employee_code: str = "",
) -> Tuple[bool, Optional[str], Optional[dict], bool]:
    """Save today's opening or reopen till after closing. Returns (ok, err, record, reopened)."""
    from database import get_pending_shop_day_closing, get_shop_day_opening, reopen_shop_day_till, save_shop_day_opening

    pending = get_pending_shop_day_closing(shop_id)
    if pending:
        label = pending.get("label") or pending.get("business_date") or "a previous day"
        return False, f"Submit closing balances for {label} first.", None, False

    require_stock = _shop_day_opening_requires_stock_confirmation(shop)
    eid = emp_row.get("id") if emp_row else None
    ecode = (emp_row.get("employee_code") if emp_row else employee_code) or ""
    ename = emp_row.get("full_name") if emp_row else None
    erole = emp_row.get("role") if emp_row else None

    today_rec = get_shop_day_opening(shop_id, date.today())
    if today_rec and today_rec.get("closing_submitted_at"):
        ok, err, record = reopen_shop_day_till(
            shop_id,
            date.today(),
            opening_cash=opening_cash,
            opening_mpesa=opening_mpesa,
            stock_confirmed=stock_confirmed,
            require_stock_confirmation=require_stock,
            employee_id=eid,
            employee_code=ecode,
            employee_name=ename,
            employee_role=erole,
        )
        return ok, err, record, True

    ok, err, record = save_shop_day_opening(
        shop_id,
        date.today(),
        opening_cash=opening_cash,
        opening_mpesa=opening_mpesa,
        stock_confirmed=stock_confirmed,
        require_stock_confirmation=require_stock,
        employee_id=eid,
        employee_code=ecode,
        employee_name=ename,
        employee_role=erole,
    )
    if ok and record and record.get("closing_submitted_at"):
        ok, err, record = reopen_shop_day_till(
            shop_id,
            date.today(),
            opening_cash=opening_cash,
            opening_mpesa=opening_mpesa,
            stock_confirmed=stock_confirmed,
            require_stock_confirmation=require_stock,
            employee_id=eid,
            employee_code=ecode,
            employee_name=ename,
            employee_role=erole,
        )
        return ok, err, record, True
    return ok, err, record, False


def _resolve_day_opening_submitter(
    shop_id: int, employee_code: str
) -> Tuple[Optional[dict], Optional[str], int]:
    """Opening always requires a 6-digit code from an authorized role."""
    code = (employee_code or "").strip()
    if not re.fullmatch(r"\d{6}", code):
        return (
            None,
            "Enter a manager or admin 6-digit code to open the shop.",
            400,
        )

    row, err, status = _shop_pos_validate_employee_code(shop_id, code)
    if err:
        return None, err, status
    role = str(row.get("role") or "").strip().lower()
    if role in COMPANY_PORTAL_ROLES:
        return row, None, 200
    if role not in SHOP_DAY_OPENING_SUBMIT_ROLES:
        return None, "Only shop admin or manager can submit opening balances.", 403
    return row, None, 200


def _resolve_day_closing_submitter(
    shop_id: int, employee_code: str
) -> Tuple[Optional[dict], Optional[str], int]:
    """Closing always requires a 6-digit code from an authorized role."""
    code = (employee_code or "").strip()
    if not re.fullmatch(r"\d{6}", code):
        return (
            None,
            "Enter a manager, admin, company manager, IT support, or super admin 6-digit code to close the shop.",
            400,
        )

    row, err, status = _shop_pos_validate_employee_code(shop_id, code)
    if err:
        return None, err, status
    role = str(row.get("role") or "").strip().lower()
    if role not in SHOP_DAY_CLOSING_SUBMIT_ROLES:
        return (
            None,
            "Only a manager, admin, company manager, IT support, or super admin can close the shop.",
            403,
        )
    return row, None, 200


def _shop_pos_sales_hours_block(shop: dict):
    allowed, msg = _shop_pos_sales_allowed(shop)
    if allowed:
        return None
    return jsonify({"ok": False, "error": msg, "shop_closed": True}), 403


def _opening_hours_boot_payload(shop: dict) -> dict:
    hours = _effective_opening_hours_for_shop(shop)
    status = _shop_opening_hours_status(shop)
    return {
        **status,
        "open_24_hours": _effective_open_24_hours_flag(hours),
        "schedule_enforced": _opening_hours_has_scheduled_days(hours),
        "days": hours.get("days") or _default_company_opening_hours()["days"],
    }


def _company_identity_values_from_form() -> dict:
    return {
        "company_name": (request.form.get("company_name") or "").strip() or "Point of Sale",
        "company_email": (request.form.get("company_email") or "").strip(),
        "company_phone": (request.form.get("company_phone") or "").strip(),
        "company_facebook": (request.form.get("company_facebook") or "").strip(),
        "company_instagram": (request.form.get("company_instagram") or "").strip(),
        "company_twitter": (request.form.get("company_twitter") or "").strip(),
        "company_tiktok": (request.form.get("company_tiktok") or "").strip(),
        **_location_settings_from_form(),
    }


def _save_company_identity_from_request() -> tuple[bool, str | None]:
    """Persist company identity fields from the current request (incl. optional logo upload)."""
    values = _company_identity_values_from_form()
    app_icon_file = request.files.get("app_icon")
    remove_app_icon = (request.form.get("remove_app_icon") or "").strip() == "1"
    app_icon_path = None
    if remove_app_icon:
        app_icon_path = ""
    elif app_icon_file and getattr(app_icon_file, "filename", ""):
        app_icon_path = _save_branding_upload(app_icon_file)
        if app_icon_path is None:
            return False, "Logo must be PNG, JPG, GIF, WebP, ICO, or SVG."
    if app_icon_path is not None:
        values["app_icon"] = app_icon_path
    try:
        from database import set_site_settings

        ok = set_site_settings(values)
    except Exception:
        ok = False
    if not ok:
        return False, "Could not save business details. Try again."
    return True, None


def _normalize_public_app_url(raw: str) -> str:
    """Normalize hosted public base URL (https://domain, no trailing slash)."""
    u = (raw or "").strip().rstrip("/")
    if not u:
        return ""
    if not u.lower().startswith(("http://", "https://")):
        u = "https://" + u.lstrip("/")
    if u.lower().startswith("http://") and "localhost" not in u.lower():
        u = "https://" + u[7:]
    return u.rstrip("/")


def _public_app_url_from_env() -> str:
    for key in ("PUBLIC_APP_URL", "APP_BASE_URL"):
        env = _normalize_public_app_url(os.getenv(key) or "")
        if env:
            return env
    return ""


def _load_public_app_url_setting() -> str:
    try:
        from database import get_site_settings

        stored = get_site_settings(["public_app_url"]).get("public_app_url") or ""
    except Exception:
        stored = ""
    return _normalize_public_app_url(stored)


def _effective_public_app_url() -> str:
    """Public HTTPS base for share links and hosted API callbacks (env overrides DB)."""
    env = _public_app_url_from_env()
    if env:
        return env
    db = _load_public_app_url_setting()
    if db:
        return db
    try:
        return _normalize_website_domain(_load_website_settings().get("domain") or "")
    except Exception:
        return ""


def _daraja_external_callback_hint() -> str:
    """Request URL hint passed to Daraja callback resolution (never localhost)."""
    from daraja_api import _is_local_callback_host

    base = _effective_public_app_url()
    path = url_for("daraja_mpesa_stk_callback", _external=False)
    if base:
        candidate = base.rstrip("/") + path
        if not _is_local_callback_host(candidate):
            return candidate
    try:
        ext = url_for("daraja_mpesa_stk_callback", _external=True)
    except RuntimeError:
        ext = ""
    if ext and not _is_local_callback_host(ext):
        return ext
    return ""


WEBSITE_SETTINGS_JSON_KEY = "website_settings_json"

WEBSITE_HOMEPAGE_FEATURED_MAX = 6

WEBSITE_THEME_STYLES: dict[str, dict[str, str]] = {
    "violet": {"label": "Violet", "tagline": "Modern retail default", "primary": "#9333ea", "accent": "#a855f7"},
    "emerald": {"label": "Emerald", "tagline": "Fresh & trustworthy", "primary": "#059669", "accent": "#10b981"},
    "sky": {"label": "Sky", "tagline": "Clear & professional", "primary": "#0284c7", "accent": "#0ea5e9"},
    "rose": {"label": "Rose", "tagline": "Bold & energetic", "primary": "#e11d48", "accent": "#f43f5e"},
    "amber": {"label": "Amber", "tagline": "Warm & inviting", "primary": "#d97706", "accent": "#f59e0b"},
    "slate": {"label": "Slate", "tagline": "Minimal & corporate", "primary": "#475569", "accent": "#64748b"},
}


_LEGACY_POS_WEBSITE_COPY_EXACT = frozenset(
    {
        "for owners & operations teams",
        "for owners and operations teams",
        "run every store with clarity",
        "lead with numbers you trust",
        "shop sign-in",
        "shop sign in",
        "shop login",
        "employee sign-in",
        "employee sign in",
        "employee login",
    }
)

_LEGACY_POS_WEBSITE_COPY_SUBSTRINGS = (
    "run every store with clear sales",
    "one view of your retail business",
    "from the shop to head office",
)


def _normalize_website_copy(s: str) -> str:
    return " ".join((s or "").strip().lower().rstrip(".").split())


def _is_legacy_pos_website_copy(s: str) -> bool:
    n = _normalize_website_copy(s)
    if not n:
        return False
    if n in _LEGACY_POS_WEBSITE_COPY_EXACT:
        return True
    return any(sub in n for sub in _LEGACY_POS_WEBSITE_COPY_SUBSTRINGS)


def _website_design_field(design: dict | None, key: str) -> str:
    val = (design.get(key) if isinstance(design, dict) else None) or ""
    val = str(val).strip()
    if _is_legacy_pos_website_copy(val):
        return ""
    return val


def _strip_legacy_website_design(design: dict | None) -> tuple[dict, bool]:
    """Remove old POS marketing hero copy from saved website design."""
    if not isinstance(design, dict):
        return {}, False
    out = dict(design)
    dirty = False
    for key in (
        "hero_eyebrow",
        "hero_headline",
        "hero_headline_accent",
        "hero_body",
        "hero_body_secondary",
        "meta_title",
        "meta_description",
        "store_tagline",
        "featured_section_subtitle",
        "cta_primary_label",
        "cta_secondary_label",
    ):
        val = str(out.get(key) or "").strip()
        if val and _is_legacy_pos_website_copy(val):
            out[key] = ""
            dirty = True
    return out, dirty


def _default_website_design() -> dict:
    return {
        "hero_eyebrow": "",
        "hero_headline": "",
        "hero_headline_accent": "",
        "hero_body": "",
        "hero_body_secondary": "",
        "meta_title": "",
        "meta_description": "",
        "return_refund_policy": "",
        "shipping_delivery_guidelines": "",
        "cta_primary_label": "",
        "cta_secondary_label": "",
        "store_tagline": "",
        "featured_section_title": "",
        "featured_section_subtitle": "",
        "promo_banner_text": "",
        "search_placeholder": "",
    }


def _website_design_from_form(current: dict | None = None) -> dict:
    """Merge homepage hero, SEO, and trust-policy fields from POST into website design."""
    base = {**_default_website_design(), **(current or {})}
    base["hero_eyebrow"] = (request.form.get("hero_eyebrow") or "").strip()[:120]
    base["hero_headline"] = (request.form.get("hero_headline") or "").strip()[:160]
    base["hero_headline_accent"] = (request.form.get("hero_headline_accent") or "").strip()[:160]
    base["hero_body"] = (request.form.get("hero_body") or "").strip()[:600]
    base["cta_primary_label"] = (request.form.get("cta_primary_label") or "").strip()[:48]
    base["cta_secondary_label"] = (request.form.get("cta_secondary_label") or "").strip()[:48]
    base["promo_banner_text"] = (request.form.get("promo_banner_text") or "").strip()[:160]
    base["featured_section_title"] = (request.form.get("featured_section_title") or "").strip()[:80]
    base["featured_section_subtitle"] = (request.form.get("featured_section_subtitle") or "").strip()[:320]
    base["search_placeholder"] = (request.form.get("search_placeholder") or "").strip()[:80]
    base["meta_title"] = (request.form.get("meta_title") or "").strip()[:120]
    base["meta_description"] = (request.form.get("meta_description") or "").strip()[:320]
    base["return_refund_policy"] = (request.form.get("return_refund_policy") or "").strip()[:8000]
    base["shipping_delivery_guidelines"] = (request.form.get("shipping_delivery_guidelines") or "").strip()[:8000]
    return base


def _storefront_homepage_copy(design: dict | None, company: dict | None) -> dict:
    """Resolved homepage text + contact from website design and company settings."""
    d = design if isinstance(design, dict) else {}
    co = company if isinstance(company, dict) else {}
    name = (co.get("company_name") or "").strip() or "Our Store"
    meta_title = _website_design_field(d, "meta_title")
    meta_desc = _website_design_field(d, "meta_description")
    store_tagline = _website_design_field(d, "store_tagline")
    hero_body = _website_design_field(d, "hero_body")
    hero_eyebrow = _website_design_field(d, "hero_eyebrow")
    hero_headline = _website_design_field(d, "hero_headline")
    hero_accent = _website_design_field(d, "hero_headline_accent")
    location = (co.get("company_location_name") or "").strip()
    lead = hero_body or meta_desc or store_tagline
    header_tagline = meta_desc or store_tagline or location
    if len(header_tagline) > 80:
        header_tagline = header_tagline[:77].rstrip() + "…"
    return {
        "company_name": name,
        "page_title": meta_title or name,
        "meta_description": meta_desc or store_tagline or f"Shop {name} — browse products and request a quote online.",
        "hero_eyebrow": hero_eyebrow,
        "hero_show_eyebrow": bool(hero_eyebrow),
        "hero_headline": hero_headline or meta_title or f"Welcome to {name}",
        "hero_headline_accent": hero_accent,
        "hero_show_accent": bool(hero_accent),
        "hero_lead": lead or f"Browse our catalogue and request a quote when you are ready.",
        "header_tagline": header_tagline,
        "featured_subtitle": _website_design_field(d, "featured_section_subtitle") or meta_desc or f"Featured products from {name}.",
        "featured_section_title": _website_design_field(d, "featured_section_title") or "Featured products",
        "shops_section_title": "Registered shops",
        "shops_section_subtitle": "Find our active branches, locations, and shop details.",
        "promo_banner_text": _website_design_field(d, "promo_banner_text"),
        "search_placeholder": _website_design_field(d, "search_placeholder") or "Search products…",
        "cta_primary_label": _website_design_field(d, "cta_primary_label") or "Browse products",
        "cta_secondary_label": _website_design_field(d, "cta_secondary_label") or "Request quote",
        "phone": (co.get("company_phone") or "").strip(),
        "whatsapp_url": _whatsapp_send_url(
            (co.get("company_phone") or "").strip(),
            f"Hello, I'd like to get in touch with {name}.",
        ),
        "email": (co.get("company_email") or "").strip(),
        "location": location,
        "latitude": (co.get("company_latitude") or "").strip(),
        "longitude": (co.get("company_longitude") or "").strip(),
        "facebook": (co.get("company_facebook") or "").strip(),
        "instagram": (co.get("company_instagram") or "").strip(),
        "twitter": (co.get("company_twitter") or "").strip(),
        "tiktok": (co.get("company_tiktok") or "").strip(),
        "app_icon": (co.get("app_icon") or "").strip(),
        "return_refund_policy": _website_design_field(d, "return_refund_policy"),
        "shipping_delivery_guidelines": _website_design_field(d, "shipping_delivery_guidelines"),
    }


def _default_website_settings() -> dict:
    style = WEBSITE_THEME_STYLES["violet"]
    return {
        "domain": "",
        "theme_style": "violet",
        "primary_color": style["primary"],
        "accent_color": style["accent"],
        "font_family": "Plus Jakarta Sans",
        "default_theme": "system",
        "design": _default_website_design(),
        "featured_item_ids": [],
        "public_website_enabled": True,
    }


def _public_website_enabled() -> bool:
    """Whether the public storefront is shown at / (vs shop login)."""
    try:
        return bool(_load_website_settings().get("public_website_enabled", True))
    except Exception:
        return True


def _normalize_website_domain(raw: str) -> str:
    return _normalize_public_app_url(raw)


def _host_from_public_url(raw: str) -> str:
    """Hostname from a public URL (no port, lowercase)."""
    u = _normalize_public_app_url(raw)
    if not u:
        return ""
    try:
        return (urlparse(u).hostname or "").strip().lower()
    except Exception:
        return ""


def _normalize_host(raw: str) -> str:
    """Compare hosts with optional www. stripped."""
    h = (raw or "").split(":")[0].strip().lower().rstrip(".")
    if h.startswith("www."):
        return h[4:]
    return h


def _website_public_domain_host() -> str:
    try:
        return _host_from_public_url(_load_website_settings().get("domain") or "")
    except Exception:
        return ""


def _request_host() -> str:
    try:
        return _normalize_host(request.host or "")
    except RuntimeError:
        return ""


def _hosts_equivalent(a: str, b: str) -> bool:
    na = _normalize_host(a)
    nb = _normalize_host(b)
    return bool(na and nb and na == nb)


def _is_public_website_host_request() -> bool:
    """True when the current request host matches Website settings → domain."""
    configured = _website_public_domain_host()
    if not configured:
        return False
    current = _request_host()
    if not current:
        return False
    return _hosts_equivalent(configured, current)


def _public_storefront_url() -> str:
    """Canonical shareable public shop URL."""
    return _public_storefront_share_info()["url"]


def _public_storefront_share_info() -> dict:
    """Share link metadata for templates (uses website domain when set)."""
    try:
        domain = _normalize_website_domain(_load_website_settings().get("domain") or "")
    except Exception:
        domain = ""
    if domain:
        url = domain.rstrip("/") + "/"
        return {
            "url": url,
            "kind": "domain",
            "label": "Share your shop",
            "hint": "Send this link to customers on WhatsApp, SMS, or social media.",
            "is_branded": True,
        }
    hosted = _effective_public_app_url()
    if hosted:
        url = hosted.rstrip("/") + "/"
        return {
            "url": url,
            "kind": "hosted",
            "label": "Shop link",
            "hint": "Using your hosted server URL. Set Website domain for a branded link (e.g. www.yourcompany.com).",
            "is_branded": False,
        }
    try:
        url = url_for("marketing_home", _external=True)
    except RuntimeError:
        url = "/site"
    return {
        "url": url,
        "kind": "preview",
        "label": "Preview link",
        "hint": "Set a website domain in settings to generate your shareable shop URL.",
        "is_branded": False,
    }


def _storefront_seo_canonical_base() -> str:
    """Absolute base URL for canonical links and sitemap (branded domain preferred)."""
    share = _public_storefront_share_info()
    kind = share.get("kind") or ""
    url = (share.get("url") or "").strip()
    if kind in ("domain", "hosted") and url.startswith("http"):
        return url.rstrip("/")
    try:
        root = (request.url_root or "").strip().rstrip("/")
        if root:
            return root
    except RuntimeError:
        pass
    if url.startswith("http"):
        return url.rstrip("/")
    return ""


def _storefront_seo_absolute_url(path: str = "/") -> str:
    """Build an absolute storefront URL for SEO tags and sitemap entries."""
    base = _storefront_seo_canonical_base()
    if not base:
        return ""
    if not path.startswith("/"):
        path = "/" + path
    return base.rstrip("/") + path


def _storefront_seo_image_url() -> str:
    """Absolute URL for og:image / business schema image."""
    try:
        co = _load_company_identity_settings()
        icon = (co.get("app_icon") or "").strip() or "app-icon.svg"
        base = _storefront_seo_canonical_base()
        static_path = url_for("static", filename=icon)
        if base:
            return base.rstrip("/") + static_path
        return url_for("static", filename=icon, _external=True)
    except Exception:
        return ""


def _storefront_local_business_json_ld() -> dict | None:
    """Schema.org Store payload for the public homepage."""
    try:
        ws = _load_website_settings()
        co = _load_company_identity_settings()
        copy = _storefront_homepage_copy(ws.get("design"), co)
    except Exception:
        return None
    name = (copy.get("company_name") or "").strip()
    if not name:
        return None
    home_url = _storefront_seo_absolute_url("/")
    payload: dict = {
        "@context": "https://schema.org",
        "@type": "Store",
        "name": name,
        "description": (copy.get("meta_description") or copy.get("hero_lead") or "").strip(),
    }
    if home_url:
        payload["url"] = home_url
    phone = (copy.get("phone") or "").strip()
    if phone:
        payload["telephone"] = phone
    email = (copy.get("email") or "").strip()
    if email:
        payload["email"] = email
    image = _storefront_seo_image_url()
    if image:
        payload["image"] = image
    location = (copy.get("location") or "").strip()
    if location:
        payload["address"] = {"@type": "PostalAddress", "streetAddress": location}
    lat = (copy.get("latitude") or "").strip()
    lng = (copy.get("longitude") or "").strip()
    if lat and lng:
        try:
            payload["geo"] = {
                "@type": "GeoCoordinates",
                "latitude": float(lat),
                "longitude": float(lng),
            }
        except (TypeError, ValueError):
            pass
    if not payload.get("description"):
        payload.pop("description", None)
    return payload


def _storefront_sitemap_entries() -> list[dict]:
    """Public storefront URLs for sitemap.xml."""
    base = _storefront_seo_canonical_base()
    if not base:
        return []
    entries = [
        {"loc": _storefront_seo_absolute_url("/"), "changefreq": "daily", "priority": "1.0"},
        {"loc": _storefront_seo_absolute_url("/catalog"), "changefreq": "daily", "priority": "0.9"},
    ]
    try:
        for cat in _website_catalog_categories():
            name = (cat.get("name") if isinstance(cat, dict) else str(cat or "")).strip()
            if not name:
                continue
            entries.append(
                {
                    "loc": _storefront_seo_absolute_url("/catalog") + "?cat=" + quote(name),
                    "changefreq": "weekly",
                    "priority": "0.7",
                }
            )
    except Exception:
        pass
    return entries


def _storefront_robots_txt_body() -> str:
    """robots.txt for public shop discovery."""
    lines = [
        "User-agent: *",
        "Allow: /",
        "Allow: /catalog",
        "Disallow: /api/",
        "Disallow: /it_support/",
        "Disallow: /shop/",
        "Disallow: /employee/",
        "Disallow: /login",
        "Disallow: /shop-login",
        "Disallow: /signup",
        "Disallow: /logout",
        "Disallow: /profile/",
        "Disallow: /site",
        "Disallow: /features",
        "Disallow: /pricing",
        "Disallow: /about",
        "Disallow: /contact",
        "Disallow: /dashboard-preview",
        "",
    ]
    sitemap_url = _storefront_seo_absolute_url("/sitemap.xml")
    if sitemap_url:
        lines.append(f"Sitemap: {sitemap_url}")
    return "\n".join(lines) + "\n"


def _storefront_seo_context_for_request() -> dict:
    """SEO meta for public storefront pages (homepage + catalogue)."""
    ep = request.endpoint or ""
    page_map = {
        "index": ("home", "/"),
        "marketing_home": ("home", "/"),
        "marketing_catalog": ("catalog", "/catalog"),
        "marketing_catalog_live": ("catalog", "/catalog"),
    }
    if ep not in page_map:
        return {}
    page, canonical_path = page_map[ep]
    try:
        ws = _load_website_settings()
        co = _load_company_identity_settings()
        copy = _storefront_homepage_copy(ws.get("design"), co)
    except Exception:
        copy = _storefront_homepage_copy({}, {})
    company = copy.get("company_name") or "Our Store"
    if page == "catalog":
        og_title = f"All products — {company}"
        meta_desc = (copy.get("meta_description") or "").strip()
        og_description = meta_desc or f"Browse the full {company} product catalogue and request a quote online."
    else:
        og_title = (copy.get("page_title") or company).strip()
        og_description = (copy.get("meta_description") or copy.get("hero_lead") or "").strip()
        if not og_description:
            og_description = f"Shop {company} — browse products and request a quote online."
    canonical_url = _storefront_seo_absolute_url(canonical_path)
    image_url = _storefront_seo_image_url()
    ctx = {
        "page": page,
        "canonical_url": canonical_url,
        "og_title": og_title,
        "og_description": og_description,
        "og_url": canonical_url,
        "og_type": "website",
        "og_image": image_url,
        "og_site_name": company,
        "twitter_card": "summary_large_image" if image_url else "summary",
    }
    if page == "home":
        ld = _storefront_local_business_json_ld()
        if ld:
            ctx["local_business_json"] = ld
    return ctx


def _render_public_storefront(*, catalog_mode: bool = False):
    ws = _load_website_settings()
    design = ws.get("design") or _default_website_design()
    if catalog_mode:
        try:
            from database import list_website_catalog_items

            catalog_rows = list_website_catalog_items(limit=500)
        except Exception:
            catalog_rows = []
        products = [_serialize_website_product_row(r) for r in catalog_rows if int(r.get("id") or 0) > 0]
        return render_template(
            "marketing/catalog.html",
            website_design=design,
            website_featured_products=products,
            website_cart_products=[_serialize_website_product_cart_row(p) for p in products],
            website_product_categories=_website_product_categories(products),
            is_live_public_website=_is_public_website_host_request(),
            storefront_catalog_mode=True,
        )
    products = _website_featured_products(limit=WEBSITE_HOMEPAGE_FEATURED_MAX)
    return render_template(
        "marketing/home.html",
        website_design=design,
        website_featured_products=products,
        website_cart_products=[_serialize_website_product_cart_row(p) for p in products],
        website_public_shops=_public_storefront_shops(),
        website_product_categories=_website_catalog_categories(),
        is_live_public_website=_is_public_website_host_request(),
        storefront_catalog_mode=False,
    )


def _sync_website_domain_to_hosted(domain: str) -> None:
    """Keep Company hosted domain in sync when a public website domain is set."""
    if not domain or _public_app_url_from_env():
        return
    try:
        from database import set_site_settings

        set_site_settings({"public_app_url": domain})
    except Exception:
        pass


def _ok_hex_color(s: str) -> bool:
    s = (s or "").strip().lstrip("#")
    return len(s) in (3, 6) and all(c in "0123456789abcdefABCDEF" for c in s)


def _normalize_featured_item_ids(raw) -> list[int]:
    """Unique positive item ids for homepage, max WEBSITE_HOMEPAGE_FEATURED_MAX, preserving order."""
    if raw is None:
        return []
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return []
        try:
            parsed = json.loads(s)
        except (TypeError, ValueError):
            parsed = [p.strip() for p in s.split(",") if p.strip()]
        raw = parsed
    if not isinstance(raw, (list, tuple)):
        return []
    out: list[int] = []
    seen: set[int] = set()
    for item in raw:
        try:
            iid = int(item)
        except (TypeError, ValueError):
            continue
        if iid <= 0 or iid in seen:
            continue
        seen.add(iid)
        out.append(iid)
        if len(out) >= WEBSITE_HOMEPAGE_FEATURED_MAX:
            break
    return out


def _website_featured_product_rows(limit: int | None = None) -> list[dict]:
    """Raw catalog rows for the public storefront (curated order or best-sellers)."""
    lim = max(1, min(int(limit or WEBSITE_HOMEPAGE_FEATURED_MAX), 500))
    try:
        ws = _load_website_settings()
        ids = _normalize_featured_item_ids(ws.get("featured_item_ids"))
    except Exception:
        ids = []
    try:
        if ids:
            from database import list_website_products_by_ids

            return list_website_products_by_ids(ids)[:lim]
        from database import list_website_featured_products

        return list_website_featured_products(limit=lim)
    except Exception:
        return []


def _serialize_website_product_row(r: dict) -> dict:
    iid = int(r.get("id") or 0)
    img_rel = _normalize_static_relative_path(r.get("image_path"))
    if img_rel.startswith("http://") or img_rel.startswith("https://"):
        image_url = img_rel
    elif img_rel:
        image_url = url_for("static", filename=img_rel)
    else:
        image_url = ""
    sell = float(r.get("price") or 0)
    orig = float(r.get("original_price") or sell)
    if orig <= 0:
        orig = sell
    lowest = min(sell, orig)
    was = max(sell, orig)
    on_sale = was > lowest + 0.009
    discount_pct = int(round((1 - lowest / was) * 100)) if on_sale and was > 0 else 0
    return {
        "id": iid,
        "category": (r.get("category") or "").strip() or "General",
        "name": (r.get("name") or "").strip() or "Product",
        "description": (r.get("description") or "").strip(),
        "price": round(lowest, 2),
        "original_price": round(was, 2),
        "image_url": image_url,
        "qty_sold": float(r.get("qty_sold") or 0),
        "on_sale": on_sale,
        "discount_percent": discount_pct,
        "discount_amount": round(was - lowest, 2) if on_sale else 0,
    }


def _serialize_website_product_cart_row(p: dict) -> dict:
    """Minimal product payload for cart JSON (smaller inline / API responses)."""
    return {
        "id": int(p.get("id") or 0),
        "name": (p.get("name") or "Product").strip() or "Product",
        "price": float(p.get("price") or 0),
        "image_url": (p.get("image_url") or "").strip(),
    }


def _website_featured_products(limit: int = 12) -> list[dict]:
    """Serialize featured storefront products with public image URLs."""
    products: list[dict] = []
    for r in _website_featured_product_rows(limit=limit):
        iid = int(r.get("id") or 0)
        if iid <= 0:
            continue
        products.append(_serialize_website_product_row(r))
    return products


def _public_shop_display_phone(shop_row: dict, company_fallback: str) -> str:
    """Contact number for a branch on the public homepage."""
    phone = (shop_row.get("shop_phone") or "").strip()
    if not phone:
        override = _parse_shop_settings_json(shop_row.get("company_settings_json"))
        if override:
            phone = (override.get("company_phone") or "").strip()
    if not phone:
        phone = (company_fallback or "").strip()
    return phone


def _public_storefront_shops() -> list[dict]:
    """Active registered shops for the public homepage."""
    try:
        from database import list_shops

        rows = list_shops(limit=100) or []
    except Exception:
        return []
    company_phone = (_load_company_identity_settings().get("company_phone") or "").strip()
    shops: list[dict] = []
    for s in rows:
        if (s.get("status") or "").strip().lower() != "active":
            continue
        logo_rel = (s.get("shop_logo") or "").strip()
        logo_url = url_for("static", filename=logo_rel) if logo_rel else ""
        display_phone = _public_shop_display_phone(s, company_phone)
        phone_tel = _normalize_storefront_phone(display_phone) if display_phone else ""
        shops.append(
            {
                "id": int(s.get("id") or 0),
                "shop_name": (s.get("shop_name") or "").strip(),
                "shop_location": (s.get("shop_location") or "").strip(),
                "shop_location_description": (s.get("shop_location_description") or "").strip(),
                "shop_logo_url": logo_url,
                "shop_phone": display_phone,
                "shop_phone_tel": phone_tel,
            }
        )
    return shops


def _website_product_categories(products: list[dict]) -> list[dict]:
    """Unique categories with the highest-selling product image per category."""
    best: dict[str, dict] = {}
    order: list[str] = []
    for p in products or []:
        c = (p.get("category") or "General").strip() or "General"
        key = c.upper()
        qty = float(p.get("qty_sold") or 0)
        image_url = (p.get("image_url") or "").strip()
        name = (p.get("name") or "").strip()
        if key not in best:
            order.append(key)
            best[key] = {
                "name": c,
                "key": key,
                "image_url": image_url,
                "top_product_name": name,
                "qty_sold": qty,
            }
            continue
        if qty > best[key]["qty_sold"]:
            best[key]["qty_sold"] = qty
            best[key]["image_url"] = image_url or best[key]["image_url"]
            best[key]["top_product_name"] = name or best[key]["top_product_name"]
        elif not best[key]["image_url"] and image_url:
            best[key]["image_url"] = image_url
            best[key]["top_product_name"] = name or best[key]["top_product_name"]
    return [best[k] for k in order]


def _website_catalog_categories() -> list[dict]:
    """All product categories from the full storefront catalogue."""
    try:
        from database import list_website_catalog_items

        rows = list_website_catalog_items(limit=500)
    except Exception:
        rows = []
    products = [_serialize_website_product_row(r) for r in rows if int(r.get("id") or 0) > 0]
    return _website_product_categories(products)


def _normalize_storefront_phone(raw: str) -> str:
    return re.sub(r"\D+", "", (raw or "").strip())


def _normalize_whatsapp_phone(raw: str) -> str:
    """E.164-style digits for api.whatsapp.com (Kenya 254…)."""
    d = _normalize_storefront_phone(raw)
    if not d or d == "-":
        return ""
    if d.startswith("254") and len(d) >= 12:
        return d[:12]
    if d.startswith("0") and len(d) >= 10:
        return "254" + d[1:11]
    if len(d) == 9:
        return "254" + d
    return d


def _whatsapp_send_url(phone_raw: str, text: str) -> str:
    phone = _normalize_whatsapp_phone(phone_raw)
    encoded = quote((text or "").strip())
    if phone and len(phone) >= 12:
        return f"https://api.whatsapp.com/send?phone={phone}&text={encoded}"
    return f"https://api.whatsapp.com/send?text={encoded}"


def _company_whatsapp_phone() -> str:
    try:
        return _normalize_whatsapp_phone(_load_company_identity_settings().get("company_phone") or "")
    except Exception:
        return ""


def _format_quotation_whatsapp_message(
    *,
    quote_id: int,
    customer_name: str,
    customer_phone: str,
    customer_notes: str,
    lines: list,
    total: float,
    channel: str = "online",
    customer_location: str = "",
    customer_location_distance_km: float | None = None,
) -> str:
    company = (_load_company_identity_settings().get("company_name") or "Our shop").strip()
    channel_label = "Website" if (channel or "").strip().lower() == "online" else "Walk-in"
    parts = [
        f"*New {channel_label} quotation — {company}*",
        f"Quote #{quote_id}",
        "",
        f"Customer: {customer_name or '—'}",
        f"Phone: {customer_phone or '—'}",
    ]
    loc = (customer_location or "").strip()
    if loc:
        loc_line = f"Location: {loc}"
        if customer_location_distance_km is not None and customer_location_distance_km >= 0:
            loc_line += f" (approx. {customer_location_distance_km:.1f} km from shop)"
        parts.append(loc_line)
    if customer_notes:
        parts.extend(["", f"Message: {customer_notes}"])
    parts.extend(["", "*Items:*"])
    for ln in lines or []:
        if not isinstance(ln, dict):
            continue
        nm = (ln.get("name") or "Item").strip()
        try:
            qty = int(ln.get("qty") or 0)
        except (TypeError, ValueError):
            qty = 0
        try:
            pr = float(ln.get("price") or 0)
        except (TypeError, ValueError):
            pr = 0.0
        try:
            tot = float(ln.get("total") if ln.get("total") is not None else pr * qty)
        except (TypeError, ValueError):
            tot = pr * qty
        parts.append(f"• {nm} ×{qty} @ KES {pr:,.2f} = KES {tot:,.2f}")
    parts.extend(["", f"*Estimated total: KES {float(total):,.2f}*"])
    return "\n".join(parts)


def _format_pos_walkin_quotation_whatsapp_message(
    *,
    quote_id: int,
    customer_name: str,
    lines: list,
    total: float,
    share_url: str = "",
) -> str:
    """Customer-facing WhatsApp body for a saved POS walk-in quotation."""
    company = (_load_company_identity_settings().get("company_name") or "Our shop").strip()
    name = (customer_name or "").strip()
    parts = [
        f"Hello {name or 'there'},",
        "",
        f"Thank you for visiting *{company}*. Here is your quotation #{quote_id}:",
        "",
        "*Items:*",
    ]
    for ln in lines or []:
        if not isinstance(ln, dict):
            continue
        nm = (ln.get("name") or "Item").strip()
        try:
            qty = int(ln.get("qty") or 0)
        except (TypeError, ValueError):
            qty = 0
        try:
            pr = float(ln.get("price") or 0)
        except (TypeError, ValueError):
            pr = 0.0
        try:
            tot = float(ln.get("total") if ln.get("total") is not None else pr * qty)
        except (TypeError, ValueError):
            tot = pr * qty
        parts.append(f"• {nm} ×{qty} @ KES {pr:,.2f} = KES {tot:,.2f}")
    parts.extend(
        [
            "",
            f"*Estimated total: KES {float(total):,.2f}*",
        ]
    )
    share = _ensure_clickable_public_url((share_url or "").strip())
    if share:
        parts.extend(
            [
                "",
                "View your quotation on our website:",
                "",
                share,
            ]
        )
    parts.extend(["", "Reply to confirm or ask any questions."])
    return "\n".join(parts)


def _quotation_whatsapp_links_for_row(q: dict, company_wa: str = "") -> dict:
    company_wa = company_wa or _company_whatsapp_phone()
    qid = int(q.get("id") or 0)
    name = (q.get("customer_name") or "").strip()
    channel = (q.get("quote_channel") or "walkin").strip().lower()
    company = (_load_company_identity_settings().get("company_name") or "us").strip()
    msg = _format_quotation_whatsapp_message(
        quote_id=qid,
        customer_name=name,
        customer_phone=(q.get("customer_phone") or "").strip(),
        customer_notes=(q.get("customer_notes") or "").strip(),
        lines=q.get("lines") or [],
        total=float(q.get("total_amount") or 0),
        channel=channel,
    )
    customer_reply = f"Hello {name or 'there'}, regarding your quotation #{qid} from {company}…"
    return {
        "company_url": _whatsapp_send_url(company_wa, msg) if company_wa else "",
        "customer_url": _whatsapp_send_url(q.get("customer_phone") or "", customer_reply)
        if (q.get("customer_phone") or "").strip()
        else "",
    }


def _quotation_whatsapp_by_id(quotes: list) -> dict:
    company_wa = _company_whatsapp_phone()
    out: dict = {}
    for q in quotes or []:
        qid = str(q.get("id") or "")
        if not qid:
            continue
        out[qid] = _quotation_whatsapp_links_for_row(q, company_wa)
    return out


_QUOTATION_SHARE_TOKEN_SALT = "quotation-share-v1"
_QUOTATION_SHARE_TOKEN_MAX_AGE_SECONDS = 30 * 24 * 3600
_QUOTATION_SHARE_MAX_ITEMS = 50
_POS_QUOTATION_SHARE_TOKEN_SALT = "pos-quotation-share-v1"
_POS_QUOTATION_SHARE_TOKEN_MAX_AGE_SECONDS = 90 * 24 * 3600


def _quotation_share_token_serializer() -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(app.secret_key, salt=_QUOTATION_SHARE_TOKEN_SALT)


def _normalize_quotation_share_item_ids(raw_ids) -> list[int]:
    out: list[int] = []
    seen: set[int] = set()
    for raw in raw_ids or []:
        try:
            iid = int(raw)
        except (TypeError, ValueError):
            continue
        if iid <= 0 or iid in seen:
            continue
        seen.add(iid)
        out.append(iid)
        if len(out) >= _QUOTATION_SHARE_MAX_ITEMS:
            break
    return out


def _make_quotation_share_token(item_ids: list[int]) -> str:
    ids = _normalize_quotation_share_item_ids(item_ids)
    if not ids:
        raise ValueError("Select at least one item to share.")
    return _quotation_share_token_serializer().dumps({"i": ids})


def _parse_quotation_share_token(token: str) -> list[int]:
    raw_token = str(token or "").strip()
    if not raw_token:
        raise ValueError("This quotation link is missing.")
    try:
        raw = _quotation_share_token_serializer().loads(
            raw_token, max_age=_QUOTATION_SHARE_TOKEN_MAX_AGE_SECONDS
        )
    except SignatureExpired as exc:
        raise ValueError("This quotation link has expired. Ask for a new one.") from exc
    except BadSignature as exc:
        raise ValueError("This quotation link is invalid.") from exc
    if not isinstance(raw, dict):
        raise ValueError("This quotation link is invalid.")
    ids = _normalize_quotation_share_item_ids(raw.get("i"))
    if not ids:
        raise ValueError("This quotation link has no items.")
    return ids


def _quotation_share_public_url(token: str) -> str:
    path = url_for("quotation_share_public", token=token, _external=False)
    return _ensure_clickable_public_url(_public_absolute_url(path))


def _pos_quotation_share_token_serializer() -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(app.secret_key, salt=_POS_QUOTATION_SHARE_TOKEN_SALT)


def _make_pos_quotation_share_token(*, quote_id: int, shop_id: int) -> str:
    qid = int(quote_id)
    sid = int(shop_id)
    if qid <= 0 or sid <= 0:
        raise ValueError("Invalid quotation reference.")
    return _pos_quotation_share_token_serializer().dumps({"q": qid, "s": sid})


def _parse_pos_quotation_share_token(token: str) -> tuple[int, int]:
    raw_token = str(token or "").strip()
    if not raw_token:
        raise ValueError("This quotation link is missing.")
    try:
        raw = _pos_quotation_share_token_serializer().loads(
            raw_token, max_age=_POS_QUOTATION_SHARE_TOKEN_MAX_AGE_SECONDS
        )
    except SignatureExpired as exc:
        raise ValueError("This quotation link has expired. Ask the shop for a new one.") from exc
    except BadSignature as exc:
        raise ValueError("This quotation link is invalid.") from exc
    if not isinstance(raw, dict):
        raise ValueError("This quotation link is invalid.")
    try:
        qid = int(raw.get("q") or 0)
        sid = int(raw.get("s") or 0)
    except (TypeError, ValueError) as exc:
        raise ValueError("This quotation link is invalid.") from exc
    if qid <= 0 or sid <= 0:
        raise ValueError("This quotation link is invalid.")
    return qid, sid


def _pos_quotation_share_public_url(token: str) -> str:
    path = url_for("pos_quotation_share_public", token=token, _external=False)
    return _ensure_clickable_public_url(_public_absolute_url(path))


def _ensure_clickable_public_url(url: str) -> str:
    """Absolute HTTPS URL suitable for WhatsApp link detection (when a public host is configured)."""
    link = (url or "").strip()
    if not link:
        return ""
    if not link.lower().startswith(("http://", "https://")):
        link = _public_absolute_url(link if link.startswith("/") else f"/{link}")
    if not link:
        return ""
    low = link.lower()
    if low.startswith("http://") and "ngrok" not in low and "localhost" not in low and "127.0.0.1" not in low:
        link = "https://" + link[7:]
    return link


def _quotation_share_items_for_public(item_ids: list[int]) -> list[dict]:
    try:
        from database import list_website_products_by_ids

        rows = list_website_products_by_ids(item_ids) or []
    except Exception:
        rows = []
    out: list[dict] = []
    for r in rows:
        ip = (r.get("image_path") or "").strip()
        img_url = ""
        if ip:
            img_url = _public_static_upload_url(ip)
        out.append(
            {
                "id": int(r.get("id") or 0),
                "category": (r.get("category") or "").strip() or "General",
                "name": (r.get("name") or "").strip() or "Product",
                "description": (r.get("description") or "").strip(),
                "price": float(r.get("price") or 0),
                "image_url": img_url,
            }
        )
    return out


def _format_quotation_share_whatsapp_message(items: list[dict], share_url: str) -> str:
    company = (_load_company_identity_settings().get("company_name") or "Our shop").strip()
    item_list = items or []
    n = len(item_list)
    parts = [
        f"📋 *Quotation from {company}*",
        f"_{n} item{'s' if n != 1 else ''} · indicative pricing_",
        "",
    ]
    total = 0.0
    for idx, item in enumerate(item_list, start=1):
        if not isinstance(item, dict):
            continue
        nm = (item.get("name") or "Item").strip()
        cat = (item.get("category") or "").strip()
        desc = (item.get("description") or "").strip()
        try:
            pr = float(item.get("price") or 0)
        except (TypeError, ValueError):
            pr = 0.0
        total += pr
        parts.append(f"*{idx}. {nm}*")
        meta = []
        if cat:
            meta.append(cat)
        meta.append(f"KES {pr:,.2f}")
        parts.append(" · ".join(meta))
        if desc:
            parts.append(desc)
        parts.append("")
    if n > 1:
        parts.append(f"*Estimated total: KES {total:,.2f}*")
        parts.append("")
    if share_url:
        parts.append(share_url)
        parts.append("")
    parts.append("_Reply to confirm availability or place your order._")
    return "\n".join(parts).strip()


def _quotation_lines_catalog_lookup(item_ids: list[int]) -> dict[int, dict]:
    ids = _normalize_quotation_share_item_ids(item_ids)
    if not ids:
        return {}
    try:
        from database import list_website_products_by_ids

        rows = list_website_products_by_ids(ids) or []
    except Exception:
        return {}
    return {int(r.get("id") or 0): r for r in rows if int(r.get("id") or 0) > 0}


def _enrich_quotation_lines_for_display(
    lines: list, catalog: Optional[dict[int, dict]] = None
) -> list[dict]:
    """Attach catalog image, description, and category to stored quote line rows."""
    raw_lines = [ln for ln in (lines or []) if isinstance(ln, dict)]
    if catalog is None:
        item_ids: list[int] = []
        seen: set[int] = set()
        for ln in raw_lines:
            try:
                iid = int(ln.get("id") or 0)
            except (TypeError, ValueError):
                iid = 0
            if iid > 0 and iid not in seen:
                seen.add(iid)
                item_ids.append(iid)
        catalog = _quotation_lines_catalog_lookup(item_ids)
    out: list[dict] = []
    for ln in raw_lines:
        row = dict(ln)
        try:
            iid = int(row.get("id") or 0)
        except (TypeError, ValueError):
            iid = 0
        cat = catalog.get(iid) if iid > 0 else None
        if cat:
            row["category"] = (cat.get("category") or "").strip() or "General"
            row["description"] = (cat.get("description") or "").strip()
            ip = (cat.get("image_path") or "").strip()
            row["image_url"] = _public_static_upload_url(ip) if ip else ""
        else:
            row.setdefault("category", "")
            row.setdefault("description", "")
            row.setdefault("image_url", "")
        out.append(row)
    return out


def _quotation_share_url_for_line_ids(item_ids: list[int]) -> str:
    ids = _normalize_quotation_share_item_ids(item_ids)
    if not ids:
        return ""
    try:
        token = _make_quotation_share_token(ids)
    except ValueError:
        return ""
    return _quotation_share_public_url(token)


def _format_customer_quotation_whatsapp_message(
    q: dict, enriched_lines: list[dict], share_url: str = ""
) -> str:
    company = (_load_company_identity_settings().get("company_name") or "Our shop").strip()
    qid = int(q.get("id") or 0)
    name = (q.get("customer_name") or "").strip()
    parts = [f"*Your quotation from {company}*"]
    if qid:
        parts.append(f"Quote #{qid}")
    if name:
        parts.extend(["", f"Hello {name},"])
    parts.extend(["", "*Items:*"])
    total = 0.0
    for ln in enriched_lines or []:
        if not isinstance(ln, dict):
            continue
        nm = (ln.get("name") or "Item").strip()
        desc = (ln.get("description") or "").strip()
        try:
            qty = int(ln.get("qty") or 0)
        except (TypeError, ValueError):
            qty = 0
        if qty <= 0:
            qty = 1
        try:
            pr = float(ln.get("price") or 0)
        except (TypeError, ValueError):
            pr = 0.0
        try:
            tot = float(ln.get("total") if ln.get("total") is not None else pr * qty)
        except (TypeError, ValueError):
            tot = pr * qty
        total += tot
        parts.append(f"• *{nm}* ×{qty} @ KES {pr:,.2f} = KES {tot:,.2f}")
        if desc:
            parts.append(f"  {desc}")
    parts.extend(["", f"*Estimated total: KES {total:,.2f}*"])
    if (q.get("customer_notes") or "").strip():
        parts.extend(["", f"Note: {(q.get('customer_notes') or '').strip()}"])
    if share_url:
        parts.extend(["", f"View full quotation: {share_url}"])
    return "\n".join(parts).strip()


def _pos_quote_lines_to_public_items(enriched_lines: list) -> list[dict]:
    """Map stored POS quote lines to the public quotation page item shape."""
    out: list[dict] = []
    for ln in enriched_lines or []:
        if not isinstance(ln, dict):
            continue
        nm = (ln.get("name") or "Item").strip()
        try:
            qty = int(ln.get("qty") or 0)
        except (TypeError, ValueError):
            qty = 0
        if qty <= 0:
            qty = 1
        try:
            unit = float(ln.get("price") or 0)
        except (TypeError, ValueError):
            unit = 0.0
        try:
            line_total = float(ln.get("total") if ln.get("total") is not None else unit * qty)
        except (TypeError, ValueError):
            line_total = unit * qty
        display_name = f"{nm} × {qty}" if qty != 1 else nm
        out.append(
            {
                "name": display_name,
                "category": (ln.get("category") or "").strip() or "General",
                "description": (ln.get("description") or "").strip(),
                "price": line_total,
                "image_url": (ln.get("image_url") or "").strip(),
            }
        )
    return out


def _render_quotation_share_public_response(
    *,
    items: Optional[list] = None,
    company_name: str = "",
    company_logo_url: str = "",
    total_amount: float = 0.0,
    share_url: str = "",
    generated_date: str = "",
    whatsapp_contact_url: str = "",
    error: Optional[str] = None,
    http_status: int = 200,
):
    return (
        render_template(
            "quotation_share_public.html",
            items=items or [],
            company_name=company_name,
            company_logo_url=company_logo_url,
            total_amount=total_amount,
            share_url=share_url,
            generated_date=generated_date,
            whatsapp_contact_url=whatsapp_contact_url,
            error=error,
        ),
        http_status,
    )


def _quotation_leads_detail_context(quotes: list) -> tuple[dict, dict]:
    """Enriched line rows and customer WhatsApp share links keyed by quote id."""
    lines_by_id: dict = {}
    share_by_id: dict = {}
    all_item_ids: list[int] = []
    seen_ids: set[int] = set()
    for q in quotes or []:
        for ln in q.get("lines") or []:
            if not isinstance(ln, dict):
                continue
            try:
                iid = int(ln.get("id") or 0)
            except (TypeError, ValueError):
                iid = 0
            if iid > 0 and iid not in seen_ids:
                seen_ids.add(iid)
                all_item_ids.append(iid)
    catalog = _quotation_lines_catalog_lookup(all_item_ids)
    for q in quotes or []:
        qid = str(q.get("id") or "")
        if not qid:
            continue
        enriched = _enrich_quotation_lines_for_display(q.get("lines") or [], catalog)
        lines_by_id[qid] = enriched
        phone = (q.get("customer_phone") or "").strip()
        item_ids = [int(ln.get("id") or 0) for ln in enriched if int(ln.get("id") or 0) > 0]
        share_url = _quotation_share_url_for_line_ids(item_ids)
        wa_text = _format_customer_quotation_whatsapp_message(q, enriched, share_url)
        share_by_id[qid] = {
            "customer_phone": phone,
            "share_url": share_url,
            "whatsapp_text": wa_text,
            "whatsapp_url": _whatsapp_send_url(phone, wa_text) if phone else "",
            "has_phone": bool(phone),
        }
    return lines_by_id, share_by_id


def _build_storefront_quotation_lines(cart_lines: list) -> tuple[list[dict], float, int, Optional[str]]:
    """Validate cart lines against active catalog; return lines, total, item_count, error."""
    if not cart_lines:
        return [], 0.0, 0, "Your cart is empty."
    try:
        from database import list_website_featured_products

        catalog = {int(p["id"]): p for p in _website_featured_product_rows(limit=48) if int(p.get("id") or 0) > 0}
    except Exception:
        catalog = {}
    if not catalog:
        return [], 0.0, 0, "No products are available right now."

    validated: list[dict] = []
    total = 0.0
    count = 0
    for raw in cart_lines:
        if not isinstance(raw, dict):
            continue
        try:
            iid = int(raw.get("id") or 0)
        except (TypeError, ValueError):
            continue
        if iid <= 0:
            continue
        try:
            qty = int(raw.get("qty") or 0)
        except (TypeError, ValueError):
            qty = 0
        if qty <= 0 or qty > 999:
            continue
        item = catalog.get(iid)
        if not item:
            return [], 0.0, 0, "One or more products are no longer available."
        price = float(item.get("price") or 0)
        line_total = round(price * qty, 2)
        validated.append(
            {
                "id": iid,
                "name": (item.get("name") or "Product")[:200],
                "qty": qty,
                "price": price,
                "total": line_total,
            }
        )
        total += line_total
        count += qty
    if not validated:
        return [], 0.0, 0, "Your cart is empty."
    return validated, round(total, 2), count, None


@app.route("/api/storefront/customer-lookup", methods=["POST"])
def storefront_customer_lookup():
    """Public read-only phone lookup for cart name autofill (no registration)."""
    data = request.get_json(force=True, silent=True) or {}
    phone = (data.get("phone") or "").strip()
    if len(re.sub(r"\D", "", phone)) < 7:
        return jsonify({"ok": False, "error": "Enter a valid phone number."}), 400
    try:
        from database import lookup_storefront_customer_by_phone

        row = lookup_storefront_customer_by_phone(phone)
    except Exception:
        row = None
    if not row:
        return jsonify({"ok": True, "customer": None})
    return jsonify(
        {
            "ok": True,
            "customer": {
                "customer_name": row["customer_name"],
                "phone": row.get("phone") or phone,
            },
        }
    )


def _notify_online_storefront_quotation(
    *,
    quote_id: int,
    customer_name: str,
    customer_phone: str,
    total: float,
    item_count: int,
) -> None:
    """Alert IT/super admins in-app when a website quotation is saved."""
    try:
        from database import create_notification

        company = (_load_company_identity_settings().get("company_name") or "Our shop").strip()
        create_notification(
            title="New website quotation",
            message=(
                f"Quote #{quote_id} — {customer_name or 'Customer'} "
                f"({customer_phone or '—'}): {int(item_count)} item(s), "
                f"KES {float(total):,.2f} ({company})"
            )[:500],
            shop_id=None,
            audience_role="admin_only",
            link_url=url_for("it_support_leads"),
            dedupe_key=f"online-quote:{int(quote_id)}",
        )
    except Exception:
        logger.exception("Failed to create notification for online quote %s", quote_id)


@app.route("/api/storefront/request-quotation", methods=["POST"])
def storefront_request_quotation():
    """Public website cart → online quotation (lead) for IT leads & quotations."""
    data = request.get_json(force=True, silent=True) or {}
    phone = _normalize_storefront_phone(data.get("customer_phone") or "")
    name = (data.get("customer_name") or "").strip()
    location = (data.get("customer_location") or "").strip()[:200]
    notes = (data.get("customer_notes") or "").strip()[:500]
    distance_km = None
    raw_dist = data.get("customer_location_distance_km")
    if raw_dist is not None and str(raw_dist).strip() != "":
        try:
            distance_km = float(raw_dist)
            if distance_km < 0:
                distance_km = None
        except (TypeError, ValueError):
            distance_km = None

    if len(phone) < 9:
        return jsonify({"ok": False, "error": "Please enter a valid phone number."}), 400
    if len(name) < 2:
        return jsonify({"ok": False, "error": "Please enter your name."}), 400
    if location and len(location) < 2:
        return jsonify({"ok": False, "error": "Please select a valid location or leave it blank."}), 400

    stored_notes = notes or None
    if location:
        loc_line = f"Location: {location}"
        if distance_km is not None:
            loc_line += f" (approx. {distance_km:.1f} km from shop)"
        stored_notes = f"{loc_line}\n\n{notes}".strip() if notes else loc_line

    cart_lines = data.get("lines") if isinstance(data.get("lines"), list) else []
    lines, total, count, err = _build_storefront_quotation_lines(cart_lines)
    if err:
        return jsonify({"ok": False, "error": err}), 400

    try:
        from database import create_shop_pos_quotation

        qid, qerr = create_shop_pos_quotation(
            shop_id=None,
            quote_basis="sale",
            quote_channel="online",
            total_amount=total,
            item_count=count,
            customer_name=name,
            customer_phone=phone,
            customer_notes=stored_notes,
            employee_name="Website storefront",
            lines=lines,
        )
    except Exception:
        qid, qerr = None, None
    if not qid:
        return jsonify({"ok": False, "error": qerr or "Could not submit your request. Try again."}), 500

    _notify_online_storefront_quotation(
        quote_id=int(qid),
        customer_name=name,
        customer_phone=phone,
        total=total,
        item_count=count,
    )

    company_wa = _company_whatsapp_phone()
    wa_message = _format_quotation_whatsapp_message(
        quote_id=int(qid),
        customer_name=name,
        customer_phone=phone,
        customer_notes=notes,
        customer_location=location,
        customer_location_distance_km=distance_km,
        lines=lines,
        total=total,
        channel="online",
    )
    company_wa_url = _whatsapp_send_url(company_wa, wa_message) if company_wa else ""

    success_message = "Thank you! Your quotation was saved to our system."
    if company_wa_url:
        success_message += " WhatsApp will open — tap Send to deliver it to the company phone."
    else:
        success_message += " Set the company phone in System settings → Company to enable WhatsApp delivery."

    return jsonify(
        {
            "ok": True,
            "quote_id": int(qid),
            "message": success_message,
            "whatsapp_url": company_wa_url,
            "company_whatsapp_url": company_wa_url,
            "whatsapp_configured": bool(company_wa),
            "system_saved": True,
        }
    )


def _load_website_settings() -> dict:
    defaults = _default_website_settings()
    try:
        from database import get_site_settings

        raw = (get_site_settings([WEBSITE_SETTINGS_JSON_KEY]).get(WEBSITE_SETTINGS_JSON_KEY) or "").strip()
    except Exception:
        raw = ""
    if not raw:
        return dict(defaults)
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        return dict(defaults)
    if not isinstance(parsed, dict):
        return dict(defaults)
    merged = {**defaults, **{k: v for k, v in parsed.items() if k in defaults and v is not None}}
    design_defaults = _default_website_design()
    design_in = parsed.get("design") if isinstance(parsed.get("design"), dict) else {}
    merged["design"] = {**design_defaults, **{k: str(v) for k, v in design_in.items() if k in design_defaults}}
    merged["domain"] = _normalize_website_domain(str(merged.get("domain") or ""))
    theme_style = str(merged.get("theme_style") or "violet").strip().lower()
    if theme_style not in WEBSITE_THEME_STYLES:
        theme_style = "violet"
    merged["theme_style"] = theme_style
    from theme_presets import normalize_default_theme, normalize_font_family

    merged["font_family"] = normalize_font_family(str(merged.get("font_family") or defaults["font_family"]))
    merged["default_theme"] = normalize_default_theme(str(merged.get("default_theme") or defaults["default_theme"]))
    for color_key in ("primary_color", "accent_color"):
        if not _ok_hex_color(str(merged.get(color_key) or "")):
            merged[color_key] = defaults[color_key]
    merged["featured_item_ids"] = _normalize_featured_item_ids(
        parsed.get("featured_item_ids") if "featured_item_ids" in parsed else merged.get("featured_item_ids")
    )
    if "public_website_enabled" in parsed:
        merged["public_website_enabled"] = bool(parsed.get("public_website_enabled"))
    design, design_dirty = _strip_legacy_website_design(merged.get("design"))
    merged["design"] = design
    if design_dirty:
        try:
            _save_website_settings(merged)
        except Exception:
            pass
    return merged


def _save_website_settings(values: dict) -> bool:
    try:
        from database import set_site_settings

        payload = json.dumps(values, separators=(",", ":"))
        return bool(set_site_settings({WEBSITE_SETTINGS_JSON_KEY: payload}))
    except Exception:
        return False


def _website_settings_for_template() -> dict:
    from theme_presets import font_css_stack, google_fonts_url

    ws = _load_website_settings()
    style = WEBSITE_THEME_STYLES.get(ws.get("theme_style") or "violet") or WEBSITE_THEME_STYLES["violet"]
    primary = (ws.get("primary_color") or style["primary"]).strip()
    accent = (ws.get("accent_color") or style["accent"]).strip()
    if not _ok_hex_color(primary):
        primary = style["primary"]
    if not _ok_hex_color(accent):
        accent = style["accent"]
    ws["effective_primary"] = primary
    ws["effective_accent"] = accent
    ws["primary_color"] = primary
    ws["accent_color"] = accent
    ws["primary_color_rgb"] = _hex_to_rgb_triplet(primary)
    ws["accent_color_rgb"] = _hex_to_rgb_triplet(accent)
    ws["font_family_stack"] = font_css_stack(ws.get("font_family"))
    ws["font_google_url"] = google_fonts_url(ws.get("font_family"))
    ws["theme_style_label"] = style["label"]
    theme_default = ws.get("default_theme") or "system"
    if theme_default == "light":
        theme_default = "system"
    ws["default_theme"] = theme_default
    ws["theme_config_key"] = "|".join(
        [
            str(theme_default),
            primary.lower(),
            accent.lower(),
            str(ws.get("font_family") or "Plus Jakarta Sans"),
            str(ws.get("theme_style") or "violet"),
        ]
    )
    return ws


def _load_company_identity_settings() -> dict:
    defaults = {
        "company_name": "Point of Sale",
        "company_email": "",
        "company_phone": "",
        "company_facebook": "",
        "company_instagram": "",
        "company_twitter": "",
        "company_tiktok": "",
        "public_app_url": "",
        "company_location_name": "",
        "company_latitude": "",
        "company_longitude": "",
        "app_icon": "",
    }
    try:
        from database import get_site_settings

        stored = get_site_settings(list(_COMPANY_IDENTITY_KEYS) + ["app_icon"]) or {}
    except Exception:
        stored = {}
    out = {k: (stored.get(k) or defaults[k]).strip() if k != "company_name" else (stored.get(k) or defaults[k]).strip() or defaults[k] for k in defaults}
    if not out["company_name"]:
        out["company_name"] = "Point of Sale"
    return out


def _shop_has_company_override(shop: dict) -> bool:
    data = _parse_shop_settings_json(shop.get("company_settings_json"))
    if not data:
        return False
    return any(k in data for k in _COMPANY_IDENTITY_KEYS)


def _effective_company_settings_for_shop(shop: dict) -> dict:
    base = _load_company_identity_settings()
    data = _parse_shop_settings_json(shop.get("company_settings_json"))
    if not data:
        return {**base, "shop_logo": (shop.get("shop_logo") or "").strip()}
    merged = dict(base)
    for k in _COMPANY_IDENTITY_KEYS:
        if k in data and str(data.get(k) or "").strip():
            merged[k] = str(data[k]).strip()
        elif k in data:
            merged[k] = str(data.get(k) or "").strip()
    merged["shop_logo"] = (shop.get("shop_logo") or "").strip()
    return merged


def _enforce_exclusive_pos_inventory(merged: dict) -> None:
    """
    Exactly one POS inventory posture is active among:
    dual (kitchen + shelf), kitchen-only (portions at checkout), or shop-only (shelf qty at checkout).
    Priority after loading legacy JSON: ``pos_inventory_use_both`` ⇒ dual;
    elif kitchen on ⇒ kitchen-only; elif shop sale on ⇒ shop-only; else default shop-only.
    """
    if not isinstance(merged, dict):
        return
    use_both = bool(merged.get("pos_inventory_use_both"))
    k = bool(merged.get("pos_kitchen_portions"))
    s = bool(merged.get("pos_shop_stock_sale"))
    if use_both:
        merged["pos_inventory_use_both"] = True
        merged["pos_kitchen_portions"] = True
        merged["pos_shop_stock_sale"] = True
    elif k:
        merged["pos_inventory_use_both"] = False
        merged["pos_kitchen_portions"] = True
        merged["pos_shop_stock_sale"] = False
    elif s:
        merged["pos_inventory_use_both"] = False
        merged["pos_kitchen_portions"] = False
        merged["pos_shop_stock_sale"] = True
    else:
        merged["pos_inventory_use_both"] = False
        merged["pos_kitchen_portions"] = False
        merged["pos_shop_stock_sale"] = True


def _printing_pos_inventory_exclusive_choice(merged: dict) -> str:
    """Single UI value derived from stored printing flags (after enforce rules)."""
    if not isinstance(merged, dict):
        return "shop"
    if bool(merged.get("pos_inventory_use_both")):
        return "dual"
    if bool(merged.get("pos_kitchen_portions")):
        return "kitchen"
    return "shop"


def _apply_pos_inventory_exclusive_form_choice(merged: dict, choice: str) -> None:
    """Set the three booleans from the POS inventory dropdown (shop | kitchen | dual)."""
    if not isinstance(merged, dict):
        return
    c = (choice or "").strip().lower()
    if c == "kitchen":
        merged["pos_inventory_use_both"] = False
        merged["pos_kitchen_portions"] = True
        merged["pos_shop_stock_sale"] = False
    elif c == "dual":
        merged["pos_inventory_use_both"] = True
        merged["pos_kitchen_portions"] = True
        merged["pos_shop_stock_sale"] = True
    else:
        merged["pos_inventory_use_both"] = False
        merged["pos_kitchen_portions"] = False
        merged["pos_shop_stock_sale"] = True


def _effective_printing_settings_for_shop(shop: dict) -> dict:
    base = _load_printing_settings()
    data = _parse_shop_settings_json(shop.get("printing_settings_json"))
    if not data:
        return base
    merged = {**base, **data}
    _normalize_print_compulsory_key(merged)
    _coalesce_printer_allow_nulls(merged)
    for k in (
        "print_compulsory_sale",
        "allow_line_price_edit",
        "pos_allow_cash_sale",
        "pos_allow_credit_sale",
        "pos_allow_quotations",
        "pos_payment_cash",
        "pos_payment_mpesa",
        "pos_payment_both",
        "pos_show_buy_items_link",
        "pos_show_customer_details_sale",
        "pos_cart_amount_sets_qty",
        "pos_include_tax",
        "pos_inventory_use_both",
        "pos_kitchen_portions",
        "pos_shop_stock_sale",
        "printer_allow_bluetooth",
        "printer_allow_network",
        "printer_allow_usb",
    ):
        merged[k] = merged.get(k) in (True, "true", "1", 1, "True")
    rc = str(merged.get("receipt_copies") or "1").strip()
    if rc not in ("1", "2", "3"):
        rc = "1"
    merged["receipt_copies"] = rc
    merged["pos_cart_mode"] = _coerce_pos_cart_mode(merged.get("pos_cart_mode"))
    _ensure_at_least_one_pos_transactional_type(merged)
    _ensure_at_least_one_pos_payment_method(merged)
    _sync_print_compulsory_with_printer_allow_list(merged)
    _enforce_exclusive_pos_inventory(merged)
    # Company IT policy (site settings) — shop JSON may carry stale overrides from older saves.
    for k in (
        "printer_allow_bluetooth",
        "printer_allow_network",
        "printer_allow_usb",
        "print_compulsory_sale",
    ):
        merged[k] = base[k]
    _sync_print_compulsory_with_printer_allow_list(merged)
    return merged


def _shop_pos_allow_credit_sale(shop: dict) -> bool:
    """POS credit checkout and shop credit navigation (when False, hide links and block credit routes)."""
    return bool(_effective_printing_settings_for_shop(shop).get("pos_allow_credit_sale"))


def _shop_pos_allow_cash_sale(shop: dict) -> bool:
    """Standard cash/M-Pesa sale checkout from POS (when False, only credit / quotation flows may exist)."""
    return bool(_effective_printing_settings_for_shop(shop).get("pos_allow_cash_sale"))


def _shop_pos_allow_quotations(shop: dict) -> bool:
    return bool(_effective_printing_settings_for_shop(shop).get("pos_allow_quotations"))


def _pos_inventory_mode_from_ps(ps: dict | None) -> str:
    """How POS deducts inventory given merged printing settings dict (global or per-shop)."""
    if not ps:
        return "none"
    k = bool(ps.get("pos_kitchen_portions"))
    s = bool(ps.get("pos_shop_stock_sale"))
    if k and s:
        return "both"
    if k:
        return "kitchen"
    if s:
        return "shop"
    return "none"


def _pos_inventory_mode(shop: dict) -> str:
    """How POS deducts inventory: shop stock, kitchen portions, both, or none."""
    try:
        from database import resolve_shop_pos_inventory_mode

        sid = int((shop or {}).get("id") or 0)
        if sid > 0:
            return resolve_shop_pos_inventory_mode(sid)
    except Exception:
        pass
    return _pos_inventory_mode_from_ps(_effective_printing_settings_for_shop(shop))


def _global_pos_inventory_mode() -> str:
    """Company-level POS inventory mode from system printing settings."""
    return _pos_inventory_mode_from_ps(_load_printing_settings())


def _effective_receipt_settings_for_shop(shop: dict) -> dict:
    base = _load_receipt_settings()
    data = _parse_shop_settings_json(shop.get("receipt_settings_json"))
    if not data:
        return base
    merged = {**base, **data}
    for k in (
        "bold_headers",
        "show_payment_details",
        "include_tax",
        "show_logo",
        "show_address",
        "show_contact",
        "show_server",
        "show_datetime",
        "receipt_qr_enabled",
    ):
        merged[k] = merged.get(k) in (True, "true", "1", 1, "True")
    qm = str(merged.get("receipt_qr_mode") or "receipt_details").strip()
    if qm not in ("website", "receipt_details"):
        qm = "receipt_details"
    merged["receipt_qr_mode"] = qm
    merged["receipt_width"] = _normalize_receipt_width_value(merged.get("receipt_width"))
    return merged


DARAJA_CREDENTIAL_KEYS = (
    "daraja_consumer_key",
    "daraja_consumer_secret",
    "daraja_passkey",
    "daraja_security_credential",
)
DARAJA_SECRET_KEYS = ("daraja_consumer_secret", "daraja_passkey", "daraja_security_credential")
DARAJA_KEEP_IF_BLANK = (
    "daraja_consumer_key",
    "daraja_consumer_secret",
    "daraja_passkey",
    "daraja_security_credential",
    "daraja_shortcode",
    "daraja_callback_url",
    "daraja_callback_url_local",
    "daraja_account_reference",
    "daraja_initiator_name",
)


def _default_daraja_settings() -> dict:
    return {
        "daraja_enabled": False,
        "daraja_environment": "sandbox",
        "daraja_consumer_key": "",
        "daraja_consumer_secret": "",
        "daraja_passkey": "",
        "daraja_shortcode": "",
        "daraja_transaction_type": "CustomerBuyGoodsOnline",
        "daraja_account_reference": "",
        "daraja_callback_url": "",
        "daraja_callback_url_local": "",
        "daraja_initiator_name": "",
        "daraja_security_credential": "",
        "daraja_stk_auto_customer_from_mpesa": True,
    }


def _load_daraja_settings() -> dict:
    from database import get_site_settings

    defaults = _default_daraja_settings()
    store_keys = ["daraja_settings_json", *DARAJA_CREDENTIAL_KEYS]
    stored = get_site_settings(store_keys)
    raw = stored.get("daraja_settings_json") or "{}"
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        data = {}
    if not isinstance(data, dict):
        data = {}
    merged = {**defaults, **data}
    for cred_key in DARAJA_CREDENTIAL_KEYS:
        dedicated = str(stored.get(cred_key) or "").strip()
        legacy = str(merged.get(cred_key) or "").strip()
        merged[cred_key] = dedicated or legacy
    merged["daraja_enabled"] = merged.get("daraja_enabled") in (True, "true", "1", 1, "True")
    merged["daraja_stk_auto_customer_from_mpesa"] = merged.get(
        "daraja_stk_auto_customer_from_mpesa"
    ) not in (False, "false", "0", 0, "False")
    env = str(merged.get("daraja_environment") or "sandbox").strip().lower()
    if env not in ("sandbox", "production"):
        env = "sandbox"
    merged["daraja_environment"] = env
    tx = str(merged.get("daraja_transaction_type") or "CustomerBuyGoodsOnline").strip()
    if tx not in ("CustomerBuyGoodsOnline", "CustomerPayBillOnline"):
        tx = "CustomerBuyGoodsOnline"
    merged["daraja_transaction_type"] = tx
    merged["daraja_shortcode"] = str(merged.get("daraja_shortcode") or "").strip()
    merged["public_app_url"] = _effective_public_app_url()
    return merged


def _daraja_settings_persist_payload(settings: dict) -> dict:
    """Persist non-secret Daraja config in JSON; credentials in dedicated site_settings keys."""
    creds = {k: str(settings.get(k) or "").strip() for k in DARAJA_CREDENTIAL_KEYS}
    public = {k: v for k, v in settings.items() if k not in DARAJA_CREDENTIAL_KEYS}
    for cred_key in DARAJA_CREDENTIAL_KEYS:
        public.pop(cred_key, None)
    payload = {"daraja_settings_json": json.dumps(public, separators=(",", ":"))}
    payload.update(creds)
    return payload


def _test_daraja_oauth_settings(settings: dict) -> Optional[str]:
    """Return an error message when Daraja OAuth fails, else None."""
    if not settings.get("daraja_enabled"):
        return None
    from daraja_api import DarajaApiError, daraja_settings_ready, get_access_token

    if not daraja_settings_ready(settings):
        return "Daraja is enabled but consumer key and secret are required."
    try:
        get_access_token(settings)
        return None
    except DarajaApiError as exc:
        return str(exc)


def _daraja_settings_for_template(merged: Optional[dict] = None) -> dict:
    """Strip secret values before rendering settings UI."""
    data = dict(merged if merged is not None else _load_daraja_settings())
    data["daraja_consumer_key_configured"] = bool(str(data.get("daraja_consumer_key") or "").strip())
    oauth_ok = data.get("daraja_oauth_ok")
    if oauth_ok in (True, "true", "1", 1, "True"):
        data["daraja_oauth_ok"] = True
    elif oauth_ok in (False, "false", "0", 0, "False"):
        data["daraja_oauth_ok"] = False
    else:
        data["daraja_oauth_ok"] = None
    for k in DARAJA_SECRET_KEYS:
        data[k + "_configured"] = bool(str(data.get(k) or "").strip())
        data.pop(k, None)
    return data


def _daraja_settings_from_form() -> dict:
    existing = _load_daraja_settings()

    def _b(key: str) -> bool:
        return (request.form.get(key) or "").strip() == "1"

    def _keep(key: str, *, max_len: Optional[int] = None) -> str:
        v = (request.form.get(key) or "").strip()
        if not v and key in DARAJA_KEEP_IF_BLANK:
            v = str(existing.get(key) or "").strip()
        if max_len is not None and len(v) > max_len:
            v = v[:max_len]
        return v

    env = (request.form.get("daraja_environment") or existing.get("daraja_environment") or "sandbox")
    env = str(env).strip().lower()
    if env not in ("sandbox", "production"):
        env = "sandbox"
    tx = (request.form.get("daraja_transaction_type") or existing.get("daraja_transaction_type") or "CustomerBuyGoodsOnline")
    tx = str(tx).strip()
    if tx not in ("CustomerBuyGoodsOnline", "CustomerPayBillOnline"):
        tx = "CustomerBuyGoodsOnline"
    return {
        "daraja_enabled": _b("daraja_enabled"),
        "daraja_environment": env,
        "daraja_consumer_key": _keep("daraja_consumer_key", max_len=256),
        "daraja_consumer_secret": _keep("daraja_consumer_secret", max_len=512),
        "daraja_passkey": _keep("daraja_passkey", max_len=512),
        "daraja_shortcode": _keep("daraja_shortcode", max_len=32),
        "daraja_transaction_type": tx,
        "daraja_account_reference": _keep("daraja_account_reference", max_len=64),
        "daraja_callback_url": _keep("daraja_callback_url", max_len=500),
        "daraja_callback_url_local": _keep("daraja_callback_url_local", max_len=500),
        "daraja_initiator_name": _keep("daraja_initiator_name", max_len=128),
        "daraja_security_credential": _keep("daraja_security_credential", max_len=4000),
        "daraja_stk_auto_customer_from_mpesa": (
            _b("daraja_stk_auto_customer_from_mpesa")
            if "daraja_stk_auto_customer_from_mpesa" in request.form
            else bool(existing.get("daraja_stk_auto_customer_from_mpesa", True))
        ),
        "daraja_oauth_ok": existing.get("daraja_oauth_ok"),
        "daraja_oauth_checked_at": existing.get("daraja_oauth_checked_at"),
    }


def _shop_customer_api_payload(row) -> Optional[dict]:
    if not row:
        return None
    return {
        "id": row.get("id"),
        "customer_name": (row.get("customer_name") or "").strip(),
        "phone": (row.get("phone") or "").strip(),
    }


def _stk_row_mpesa_payer(row: dict) -> tuple[str, str]:
    """Payer name and phone from merged STK status (callback metadata + stored fields)."""
    from daraja_api import extract_mpesa_payer_from_stk_metadata, normalize_msisdn

    meta = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    fallback_phone = str(row.get("phone") or row.get("mpesa_payer_phone") or "")
    payer_name, payer_phone = extract_mpesa_payer_from_stk_metadata(meta, fallback_phone)
    if not payer_name:
        payer_name = str(row.get("mpesa_payer_name") or "").strip()
    if not payer_name and row.get("credit_pay"):
        payer_name = str(row.get("customer_name") or "").strip()
    phone = normalize_msisdn(payer_phone or fallback_phone)
    return payer_name.strip(), phone


def _maybe_auto_register_shop_customer_from_mpesa_stk(
    shop_id: int, row: dict
) -> tuple[Optional[dict], bool]:
    """After STK paid: match or register shop customer. Returns (customer, was_new)."""
    if not shop_id or not row:
        return None, False

    from database import (
        get_public_customer_by_phone,
        get_shop_customer_by_phone,
        upsert_shop_customer,
    )

    payer_name, phone = _stk_row_mpesa_payer(row)
    if len(re.sub(r"\D", "", phone)) < 9:
        return None, False

    existing = get_shop_customer_by_phone(shop_id, phone)
    if existing:
        return _shop_customer_api_payload(existing), False

    settings = _load_daraja_settings()
    if not settings.get("daraja_stk_auto_customer_from_mpesa"):
        if payer_name:
            return {"id": None, "customer_name": payer_name, "phone": phone}, False
        return None, False

    name = payer_name
    if len(name) < 2:
        pub = get_public_customer_by_phone(phone)
        if pub:
            name = str(pub.get("customer_name") or "").strip()
    if len(name) < 2:
        return None, False

    if not upsert_shop_customer(shop_id, name, phone):
        return None, False
    created = get_shop_customer_by_phone(shop_id, phone)
    if not created:
        return None, False
    logger.info(
        "Auto-registered shop customer from M-Pesa STK shop=%s phone=%s name=%s",
        shop_id,
        phone,
        name,
    )
    return _shop_customer_api_payload(created), True


def _daraja_pos_boot_payload() -> dict:
    try:
        from daraja_api import daraja_settings_ready

        settings = _load_daraja_settings()
        return {
            "enabled": daraja_settings_ready(settings),
            "autoCustomerFromMpesa": bool(settings.get("daraja_stk_auto_customer_from_mpesa")),
        }
    except Exception:
        return {"enabled": False, "autoCustomerFromMpesa": False}


_CREDIT_PAY_TOKEN_SALT = "credit-mpesa-pay-v1"
_CREDIT_PAY_TOKEN_MAX_AGE_SECONDS = 14 * 24 * 3600


def _credit_pay_token_serializer() -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(app.secret_key, salt=_CREDIT_PAY_TOKEN_SALT)


def _make_credit_pay_token(
    shop_id: int,
    customer_name: str,
    customer_phone: str,
    max_amount: float,
) -> str:
    payload = {
        "s": int(shop_id),
        "n": (customer_name or "WALK IN").strip()[:120],
        "p": (customer_phone or "-").strip()[:32],
        "m": round(max(float(max_amount or 0), 0), 2),
    }
    return _credit_pay_token_serializer().dumps(payload)


def _parse_credit_pay_token(token: str) -> dict:
    raw_token = str(token or "").strip()
    if not raw_token:
        raise ValueError("Payment link is missing.")
    try:
        raw = _credit_pay_token_serializer().loads(
            raw_token, max_age=_CREDIT_PAY_TOKEN_MAX_AGE_SECONDS
        )
    except SignatureExpired as exc:
        raise ValueError(
            "This payment link has expired. Ask the shop for a new credit note."
        ) from exc
    except BadSignature as exc:
        raise ValueError("This payment link is invalid.") from exc
    if not isinstance(raw, dict):
        raise ValueError("This payment link is invalid.")
    shop_id = int(raw.get("s") or 0)
    if shop_id < 0:
        raise ValueError("This payment link is invalid.")
    return {
        "shop_id": shop_id,
        "customer_name": str(raw.get("n") or "WALK IN").strip() or "WALK IN",
        "customer_phone": str(raw.get("p") or "-").strip() or "-",
        "max_amount": float(raw.get("m") or 0),
        "token": raw_token,
    }


def _public_share_base_url() -> str:
    """
    Public base URL for WhatsApp-shareable links.

    WhatsApp only linkifies URLs customers can open (HTTPS public host).
    Prefer branded website domain, then PUBLIC_APP_URL / Company hosted domain,
    ngrok, or a real Daraja callback host (never placeholder URLs).
    """
    try:
        domain = _normalize_website_domain(_load_website_settings().get("domain") or "")
        if domain:
            return domain
    except Exception:
        pass

    hosted = _effective_public_app_url()
    if hosted:
        return hosted

    try:
        from daraja_api import (
            _is_local_callback_host,
            _is_placeholder_callback_url,
            _try_local_ngrok_callback_url,
        )

        ngrok_cb = _try_local_ngrok_callback_url()
        if ngrok_cb:
            parsed = urlparse(ngrok_cb)
            if parsed.scheme and parsed.netloc:
                return f"{parsed.scheme}://{parsed.netloc}".rstrip("/")

        hosted = str(_load_daraja_settings().get("daraja_callback_url") or "").strip().rstrip("/")
        if (
            hosted
            and not _is_local_callback_host(hosted)
            and not _is_placeholder_callback_url(hosted)
        ):
            parsed = urlparse(hosted)
            if parsed.scheme and parsed.netloc:
                return f"{parsed.scheme}://{parsed.netloc}".rstrip("/")
    except Exception:
        pass

    try:
        root = (request.url_root or "").strip().rstrip("/")
        if root:
            from daraja_api import _is_local_callback_host

            if not _is_local_callback_host(root):
                return root
    except Exception:
        pass
    return ""


def _public_absolute_url(path: str) -> str:
    """Absolute URL for a site path using the configured public app domain when set."""
    raw = (path or "").strip()
    if not raw:
        return ""
    if raw.lower().startswith(("http://", "https://")):
        return raw
    rel = raw if raw.startswith("/") else "/" + raw
    base = _public_share_base_url()
    if base:
        link = base.rstrip("/") + rel
        if link.lower().startswith("http://") and "ngrok" not in link.lower():
            link = "https://" + link[7:]
        return link
    try:
        return url_for("static", filename=rel.lstrip("/"), _external=True) if rel.startswith("/static/") else (
            (request.url_root or "").rstrip("/") + rel
        )
    except RuntimeError:
        return rel


def _public_static_upload_url(path) -> str:
    """Public absolute URL for uploaded static assets (quotation share pages, etc.)."""
    rel = _normalize_static_relative_path(path)
    if not rel:
        return ""
    if rel.startswith("http://") or rel.startswith("https://"):
        return rel
    return _public_absolute_url(url_for("static", filename=rel, _external=False))


def _quotation_share_domain_info() -> dict:
    """Share-link metadata for IT Support quotation tools."""
    base = _public_share_base_url()
    if not base:
        return {
            "base_url": "",
            "kind": "local",
            "is_branded": False,
            "hint": (
                "Set a website domain or hosted URL under Company / Website settings "
                "so customers receive a proper shareable link (not localhost)."
            ),
        }
    try:
        domain = _normalize_website_domain(_load_website_settings().get("domain") or "")
    except Exception:
        domain = ""
    is_branded = bool(domain and base.rstrip("/") == domain.rstrip("/"))
    if is_branded:
        hint = "Quotation links use your branded website domain."
    elif _public_app_url_from_env() or _load_public_app_url_setting():
        hint = "Quotation links use your hosted app URL from settings."
    else:
        hint = "Quotation links use your current public server URL."
    return {
        "base_url": base,
        "kind": "domain" if is_branded else "hosted",
        "is_branded": is_branded,
        "hint": hint,
    }


def _credit_pay_public_link(
    shop_id: int,
    customer_name: str,
    customer_phone: str,
    max_amount: float,
) -> str:
    try:
        from daraja_api import daraja_settings_ready

        if not daraja_settings_ready(_load_daraja_settings()):
            return ""
    except Exception:
        return ""
    if float(max_amount or 0) < 0.01:
        return ""
    tok = _make_credit_pay_token(int(shop_id or 0), customer_name, customer_phone, max_amount)
    path = url_for("credit_pay_public", token=tok, _external=False)
    public_base = _public_share_base_url()
    if public_base:
        link = public_base.rstrip("/") + path
        if link.lower().startswith("http://") and "ngrok" not in link.lower():
            link = "https://" + link[7:]
        return link
    return url_for("credit_pay_public", token=tok, _external=True)


def _credit_pay_public_link_full(
    shop_id: int,
    customer_name: str,
    customer_phone: str,
    max_amount: float,
) -> str:
    """Public STK pay page URL with amount and phone pre-filled for WhatsApp share."""
    base = _credit_pay_public_link(shop_id, customer_name, customer_phone, max_amount)
    if not base:
        return ""
    from urllib.parse import urlencode

    params: dict[str, str] = {}
    amt = float(max_amount or 0)
    if amt >= 0.01:
        params["amount"] = f"{amt:.2f}"
    phone = (customer_phone or "").strip()
    if phone and phone != "-":
        params["phone"] = phone
    if not params:
        return base
    sep = "&" if "?" in base else "?"
    return base + sep + urlencode(params)


_CREDIT_NOTE_SHARE_TOKEN_SALT = "credit-note-share-v1"
_CREDIT_NOTE_SHARE_TOKEN_MAX_AGE_SECONDS = 14 * 24 * 3600


def _credit_note_share_token_serializer() -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(app.secret_key, salt=_CREDIT_NOTE_SHARE_TOKEN_SALT)


def _make_credit_note_share_token(
    shop_id: int,
    customer_name: str,
    customer_phone: str,
    *,
    company_scope: bool = False,
) -> str:
    payload = {
        "s": int(shop_id or 0),
        "n": (customer_name or "WALK IN").strip()[:120],
        "p": (customer_phone or "-").strip()[:32],
        "c": 1 if company_scope else 0,
    }
    return _credit_note_share_token_serializer().dumps(payload)


def _parse_credit_note_share_token(token: str) -> dict:
    raw_token = str(token or "").strip()
    if not raw_token:
        raise ValueError("Credit note link is missing.")
    try:
        raw = _credit_note_share_token_serializer().loads(
            raw_token, max_age=_CREDIT_NOTE_SHARE_TOKEN_MAX_AGE_SECONDS
        )
    except SignatureExpired as exc:
        raise ValueError("This credit note link has expired. Ask for a new one.") from exc
    except BadSignature as exc:
        raise ValueError("This credit note link is invalid.") from exc
    if not isinstance(raw, dict):
        raise ValueError("This credit note link is invalid.")
    shop_id = int(raw.get("s") or 0)
    if shop_id < 0:
        raise ValueError("This credit note link is invalid.")
    return {
        "shop_id": shop_id,
        "customer_name": str(raw.get("n") or "WALK IN").strip() or "WALK IN",
        "customer_phone": str(raw.get("p") or "-").strip() or "-",
        "company_scope": bool(int(raw.get("c") or 0)),
        "token": raw_token,
    }


def _credit_note_public_share_url(
    shop_id: int,
    customer_name: str,
    customer_phone: str,
    *,
    company_scope: bool = False,
) -> str:
    """Public HTTPS link to the customer credit note PDF (for WhatsApp share text)."""
    tok = _make_credit_note_share_token(
        shop_id, customer_name, customer_phone, company_scope=company_scope
    )
    path = url_for("credit_note_public_share_pdf", token=tok, _external=False)
    public_base = _public_share_base_url()
    if public_base:
        link = public_base.rstrip("/") + path
        if link.lower().startswith("http://") and "ngrok" not in link.lower():
            link = "https://" + link[7:]
        return link
    return url_for("credit_note_public_share_pdf", token=tok, _external=True)


_MPESA_STK_STATUS: Dict[str, dict] = {}
_MPESA_STK_STATUS_LOCK = threading.Lock()


def _mpesa_stk_merge_status(prev: dict, incoming: dict) -> dict:
    prev = prev if isinstance(prev, dict) else {}
    incoming = incoming if isinstance(incoming, dict) else {}
    merged = {**prev, **incoming}
    prev_meta = prev.get("metadata") if isinstance(prev.get("metadata"), dict) else {}
    new_meta = incoming.get("metadata")
    if isinstance(new_meta, dict):
        merged["metadata"] = {**prev_meta, **new_meta}
    receipt = _mpesa_stk_receipt_number(merged)
    if receipt:
        merged["mpesa_receipt_number"] = receipt
    return merged


def _mpesa_stk_status_store(checkout_request_id: str, payload: dict) -> None:
    cid = str(checkout_request_id or "").strip()
    if not cid:
        return
    incoming = payload if isinstance(payload, dict) else {}
    prev: dict = {}
    try:
        from database import get_mpesa_stk_request

        prev = get_mpesa_stk_request(cid) or {}
    except Exception:
        logger.debug("STK DB load before merge failed for %s", cid, exc_info=True)
    if not prev:
        with _MPESA_STK_STATUS_LOCK:
            prev = dict(_MPESA_STK_STATUS.get(cid) or {})
    merged = _mpesa_stk_merge_status(prev, incoming)
    with _MPESA_STK_STATUS_LOCK:
        _MPESA_STK_STATUS[cid] = merged
    try:
        from database import upsert_mpesa_stk_request

        upsert_mpesa_stk_request(cid, merged)
    except Exception:
        logger.exception("Failed to persist M-Pesa STK status for %s", cid)


def _mpesa_stk_receipt_number(row: Optional[dict]) -> str:
    """Safaricom M-Pesa receipt reference (MpesaReceiptNumber) when STK payment succeeded."""
    if not row:
        return ""
    direct = str(row.get("mpesa_receipt_number") or "").strip()
    if direct:
        return direct
    meta = row.get("metadata")
    if not isinstance(meta, dict):
        return ""
    for key in ("MpesaReceiptNumber", "mpesa_receipt_number", "ReceiptNumber"):
        val = str(meta.get(key) or "").strip()
        if val:
            return val
    return ""


def _mpesa_stk_status_get(checkout_request_id: str) -> Optional[dict]:
    cid = str(checkout_request_id or "").strip()
    if not cid:
        return None
    row: Optional[dict] = None
    try:
        from database import get_mpesa_stk_request

        row = get_mpesa_stk_request(cid)
    except Exception:
        logger.debug("M-Pesa STK DB load failed for %s", cid, exc_info=True)
    if not row:
        with _MPESA_STK_STATUS_LOCK:
            mem = _MPESA_STK_STATUS.get(cid)
            row = dict(mem) if mem else None
    elif row:
        with _MPESA_STK_STATUS_LOCK:
            _MPESA_STK_STATUS[cid] = row
    return dict(row) if row else None


_DARAJA_BALANCE_STATUS: Dict[str, dict] = {}
_DARAJA_BALANCE_STATUS_LOCK = threading.Lock()
_DARAJA_BALANCE_SNAPSHOT_KEY = "daraja_account_balance_json"
_DARAJA_BALANCE_STATUS_KEY = "daraja_balance_status_json"
_DARAJA_BALANCE_STATUS_MAX = 40


def _daraja_balance_snapshot_load() -> dict:
    try:
        from database import get_site_settings

        raw = (
            get_site_settings([_DARAJA_BALANCE_SNAPSHOT_KEY]).get(
                _DARAJA_BALANCE_SNAPSHOT_KEY
            )
            or ""
        )
        data = json.loads(raw) if raw else {}
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _daraja_balance_status_load_all() -> dict:
    try:
        from database import get_site_settings

        raw = (
            get_site_settings([_DARAJA_BALANCE_STATUS_KEY]).get(
                _DARAJA_BALANCE_STATUS_KEY
            )
            or ""
        )
        data = json.loads(raw) if raw else {}
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _daraja_balance_status_save_all(data: dict) -> None:
    try:
        from database import set_site_settings

        if not isinstance(data, dict):
            return
        items = list(data.items())
        if len(items) > _DARAJA_BALANCE_STATUS_MAX:
            items = items[-_DARAJA_BALANCE_STATUS_MAX :]
        set_site_settings(
            {
                _DARAJA_BALANCE_STATUS_KEY: json.dumps(
                    dict(items), separators=(",", ":")
                )
            }
        )
    except Exception:
        logger.exception("Failed to persist Daraja balance status map")


def _daraja_balance_snapshot_save(payload: dict) -> None:
    try:
        from database import set_site_settings

        set_site_settings(
            {
                _DARAJA_BALANCE_SNAPSHOT_KEY: json.dumps(
                    payload, separators=(",", ":")
                )
            }
        )
    except Exception:
        logger.exception("Failed to persist Daraja balance snapshot")


def _daraja_balance_status_store(conversation_id: str, payload: dict) -> None:
    cid = str(conversation_id or "").strip()
    if not cid:
        return
    incoming = payload if isinstance(payload, dict) else {}
    with _DARAJA_BALANCE_STATUS_LOCK:
        prev = dict(_DARAJA_BALANCE_STATUS.get(cid) or {})
        persisted = _daraja_balance_status_load_all()
        if not prev:
            prev = dict(persisted.get(cid) or {})
        merged = {**prev, **incoming, "conversation_id": cid}
        _DARAJA_BALANCE_STATUS[cid] = merged
        persisted[cid] = merged
        _daraja_balance_status_save_all(persisted)
    if merged.get("completed") and merged.get("result_code") == 0:
        snap = _daraja_balance_snapshot_load()
        snap.update(
            {
                "balance": merged.get("balance"),
                "currency": merged.get("currency") or snap.get("currency") or "KES",
                "account_label": merged.get("account_label")
                or snap.get("account_label")
                or "",
                "result_desc": merged.get("result_desc") or "",
                "result_code": merged.get("result_code"),
                "conversation_id": cid,
                "updated_at": datetime.now().isoformat(timespec="seconds"),
            }
        )
        _daraja_balance_snapshot_save(snap)


def _daraja_balance_status_get(conversation_id: str) -> Optional[dict]:
    cid = str(conversation_id or "").strip()
    if not cid:
        return None
    with _DARAJA_BALANCE_STATUS_LOCK:
        row = _DARAJA_BALANCE_STATUS.get(cid)
        if not row:
            row = _daraja_balance_status_load_all().get(cid)
            if row:
                _DARAJA_BALANCE_STATUS[cid] = dict(row)
        return dict(row) if row else None


def _daraja_balance_status_get_any(conversation_ids: list) -> Optional[dict]:
    for raw in conversation_ids or []:
        cid = str(raw or "").strip()
        if not cid:
            continue
        row = _daraja_balance_status_get(cid)
        if row:
            return row
    return None


def _daraja_balance_register_pending(*conversation_ids: str) -> None:
    pending = {
        "pending": True,
        "completed": False,
        "timed_out": False,
        "requested_at": datetime.now().isoformat(timespec="seconds"),
    }
    ids = []
    for raw in conversation_ids:
        cid = str(raw or "").strip()
        if cid and cid not in ids:
            ids.append(cid)
    for cid in ids:
        _daraja_balance_status_store(cid, {**pending, "conversation_id": cid, "poll_ids": ids})


_DARAJA_B2C_STATUS: Dict[str, dict] = {}
_DARAJA_B2C_STATUS_LOCK = threading.Lock()
_DARAJA_B2C_STATUS_KEY = "daraja_b2c_status_json"
_DARAJA_B2C_STATUS_MAX = 60


def _daraja_b2c_status_load_all() -> dict:
    try:
        from database import get_site_settings

        raw = get_site_settings([_DARAJA_B2C_STATUS_KEY]).get(_DARAJA_B2C_STATUS_KEY) or ""
        data = json.loads(raw) if raw else {}
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _daraja_b2c_status_save_all(data: dict) -> None:
    try:
        from database import set_site_settings

        if not isinstance(data, dict):
            return
        items = list(data.items())
        if len(items) > _DARAJA_B2C_STATUS_MAX:
            items = items[-_DARAJA_B2C_STATUS_MAX :]
        set_site_settings(
            {
                _DARAJA_B2C_STATUS_KEY: json.dumps(
                    dict(items), separators=(",", ":")
                )
            }
        )
    except Exception:
        logger.exception("Failed to persist Daraja B2C status map")


def _daraja_b2c_status_store(conversation_id: str, payload: dict) -> None:
    cid = str(conversation_id or "").strip()
    if not cid:
        return
    incoming = payload if isinstance(payload, dict) else {}
    with _DARAJA_B2C_STATUS_LOCK:
        prev = dict(_DARAJA_B2C_STATUS.get(cid) or {})
        persisted = _daraja_b2c_status_load_all()
        if not prev:
            prev = dict(persisted.get(cid) or {})
        merged = {**prev, **incoming, "conversation_id": cid}
        _DARAJA_B2C_STATUS[cid] = merged
        persisted[cid] = merged
        _daraja_b2c_status_save_all(persisted)


def _daraja_b2c_status_get(conversation_id: str) -> Optional[dict]:
    cid = str(conversation_id or "").strip()
    if not cid:
        return None
    with _DARAJA_B2C_STATUS_LOCK:
        row = _DARAJA_B2C_STATUS.get(cid)
        if not row:
            row = _daraja_b2c_status_load_all().get(cid)
            if row:
                _DARAJA_B2C_STATUS[cid] = dict(row)
        return dict(row) if row else None


def _daraja_b2c_status_get_any(conversation_ids: list) -> Optional[dict]:
    for raw in conversation_ids or []:
        cid = str(raw or "").strip()
        if not cid:
            continue
        row = _daraja_b2c_status_get(cid)
        if row:
            return row
    return None


def _daraja_b2c_register_pending(
    *conversation_ids: str, meta: Optional[dict] = None
) -> None:
    pending = {
        "pending": True,
        "completed": False,
        "timed_out": False,
        "payment_applied": False,
        "requested_at": datetime.now().isoformat(timespec="seconds"),
        **(meta or {}),
    }
    ids = []
    for raw in conversation_ids:
        cid = str(raw or "").strip()
        if cid and cid not in ids:
            ids.append(cid)
    for cid in ids:
        _daraja_b2c_status_store(
            cid, {**pending, "conversation_id": cid, "poll_ids": ids}
        )


def _daraja_b2c_apply_expense_payment(payload: dict) -> bool:
    if payload.get("payment_applied"):
        return False
    tx_id = payload.get("expense_tx_id")
    tx_scope = str(payload.get("expense_tx_scope") or "").strip().lower()
    amount = payload.get("payout_amount")
    if amount is None:
        amount = payload.get("amount")
    try:
        tx_id_int = int(tx_id or 0)
        pay = float(amount or 0)
    except (TypeError, ValueError):
        return False
    if tx_id_int < 1 or pay <= 0:
        return False
    try:
        if tx_scope == "company":
            from database import update_company_stock_in_payment

            updated = update_company_stock_in_payment(
                tx_id_int, additional_payment=pay
            )
        else:
            from database import update_shop_manual_stock_in_payment

            updated = update_shop_manual_stock_in_payment(
                tx_id_int, additional_payment=pay
            )
    except Exception:
        logger.exception("Failed to apply B2C expense payment tx=%s", tx_id_int)
        return False
    return updated is not None


def _daraja_expenses_mpesa_context(request_url: str = "") -> dict:
    from daraja_api import (
        daraja_b2c_settings_ready,
        daraja_settings_ready,
        resolve_b2c_callbacks_detailed,
    )

    settings = _load_daraja_settings()
    detailed = resolve_b2c_callbacks_detailed(settings, request_url, probe=False)
    return {
        "enabled": bool(settings.get("daraja_enabled")),
        "configured": daraja_settings_ready(settings),
        "payout_ready": daraja_b2c_settings_ready(settings)
        and bool(detailed.get("result_url")),
        "callback_mode": detailed.get("callback_mode") or "hosted",
        "is_local_session": bool(detailed.get("is_local_session")),
        "hosted_fallback": bool(detailed.get("hosted_fallback")),
        "result_url": detailed.get("result_url") or "",
    }


def _daraja_company_account_context(request_url: str = "") -> dict:
    from daraja_api import (
        balance_callback_url_options,
        daraja_account_type_label,
        daraja_api_endpoints,
        daraja_balance_settings_ready,
        daraja_settings_ready,
    )

    settings = _load_daraja_settings()
    shortcode = str(settings.get("daraja_shortcode") or "").strip()
    env = str(settings.get("daraja_environment") or "sandbox").strip().lower()
    if not shortcode and env == "sandbox":
        shortcode = "174379"
    snapshot = _daraja_balance_snapshot_load()
    if "balance" not in snapshot:
        snapshot["balance"] = None
    endpoints = daraja_api_endpoints(settings)
    balance_callbacks = balance_callback_url_options(settings, request_url)
    return {
        "daraja_endpoints": endpoints,
        "mpesa_account": {
            "configured": daraja_settings_ready(settings),
            "balance_ready": daraja_balance_settings_ready(settings)
            and balance_callbacks.get("balance_ready"),
            "enabled": bool(settings.get("daraja_enabled")),
            "environment": env,
            "shortcode": shortcode,
            "account_type": daraja_account_type_label(settings),
            "transaction_type": settings.get("daraja_transaction_type")
            or "CustomerBuyGoodsOnline",
            "initiator_configured": bool(
                str(settings.get("daraja_initiator_name") or "").strip()
            ),
            "security_credential_configured": bool(
                str(settings.get("daraja_security_credential") or "").strip()
            ),
            "balance_api_url": endpoints.get("account_balance") or "",
            "balance_callbacks": balance_callbacks,
        },
        "mpesa_balance": snapshot,
    }


def _receipt_settings_from_form() -> dict:
    def _b(key: str) -> bool:
        return (request.form.get(key) or "").strip() == "1"

    def _s(key: str, default: str = "", max_len=None) -> str:
        v = (request.form.get(key) or "").strip()
        if max_len is not None and len(v) > max_len:
            v = v[:max_len]
        return v or default

    ptype = _s("receipt_payment_type", "buy_goods")
    if ptype not in ("buy_goods", "paybill", "send_money"):
        ptype = "buy_goods"
    fmt = _s("receipt_number_format", "sequential")
    if fmt not in ("sequential", "daily", "per_month"):
        fmt = "sequential"
    width = _normalize_receipt_width_value(_s("receipt_width", "80mm"))
    fsize = _s("receipt_font_size", "12pt")
    if fsize not in ("8pt", "10pt", "12pt"):
        fsize = "12pt"
    qr_mode = _s("receipt_qr_mode", "receipt_details")
    if qr_mode not in ("website", "receipt_details"):
        qr_mode = "receipt_details"
    return {
        "receipt_width": width,
        "font_size": fsize,
        "bold_headers": _b("receipt_bold_headers"),
        "receipt_number_format": fmt,
        "receipt_number_prefix": _s("receipt_number_prefix", "T", 32),
        "starting_number": _s("receipt_starting_number", "1001", 32),
        "show_payment_details": _b("receipt_show_payment_details"),
        "payment_detail_type": ptype,
        "payment_detail_text": _s("receipt_payment_detail_text", "", 2000),
        "include_tax": _b("receipt_include_tax"),
        "tax_percent": _s("receipt_tax_percent", "", 16),
        "show_logo": _b("receipt_show_logo"),
        "show_address": _b("receipt_show_address"),
        "show_contact": _b("receipt_show_contact"),
        "show_server": _b("receipt_show_server"),
        "show_datetime": _b("receipt_show_datetime"),
        "receipt_header": _s("receipt_header", "", 4000),
        "receipt_footer": _s("receipt_footer", "", 4000),
        "receipt_qr_enabled": _b("receipt_qr_enabled"),
        "receipt_qr_mode": qr_mode,
        "receipt_qr_website_url": _s("receipt_qr_website_url", "", 2000),
    }


def _ensure_at_least_one_pos_transactional_type(merged: dict) -> None:
    """Checkout requires at least one of sale / credit / quotation to be enabled."""
    if merged.get("pos_allow_cash_sale") or merged.get("pos_allow_credit_sale") or merged.get("pos_allow_quotations"):
        return
    merged["pos_allow_cash_sale"] = True


def _ensure_at_least_one_pos_payment_method(merged: dict) -> None:
    """Standard sale checkout needs at least one of cash / mpesa / both split."""
    if merged.get("pos_payment_cash") or merged.get("pos_payment_mpesa") or merged.get("pos_payment_both"):
        return
    merged["pos_payment_cash"] = True


def _sync_print_compulsory_with_printer_allow_list(merged: dict) -> None:
    """No allowed printer types → cannot enforce compulsory printing (must be off). Otherwise keep saved choice."""
    bt = bool(merged.get("printer_allow_bluetooth"))
    net = bool(merged.get("printer_allow_network"))
    usb = bool(merged.get("printer_allow_usb"))
    if not (bt or net or usb):
        merged["print_compulsory_sale"] = False


def _normalize_print_compulsory_key(merged: dict) -> None:
    """Fold form-style alias ``printing_compulsory_sale`` into canonical ``print_compulsory_sale`` (saved JSON)."""
    if merged.get("printing_compulsory_sale") is not None:
        merged["print_compulsory_sale"] = merged.get("printing_compulsory_sale")
    merged.pop("printing_compulsory_sale", None)


def _coalesce_printer_allow_nulls(merged: dict) -> None:
    """
    JSON null / Python None for printer_allow_* must inherit defaults (allowed).

    Otherwise ``merged.get(k) in (True, ...)`` treats None as falsy and disables every
    connection type on hosted DBs — hiding USB in POS setup and forcing compulsory off in sync.
    """
    defaults = _default_printing_settings()
    for k in ("printer_allow_bluetooth", "printer_allow_network", "printer_allow_usb"):
        if merged.get(k) is None:
            merged[k] = defaults[k]


def _shop_pos_payment_method_allowed(shop: dict, method: str) -> bool:
    m = (method or "").strip().lower()
    ps = _effective_printing_settings_for_shop(shop)
    if m == "cash":
        return bool(ps.get("pos_payment_cash"))
    if m == "mpesa":
        return bool(ps.get("pos_payment_mpesa"))
    if m == "both":
        return bool(ps.get("pos_payment_both"))
    return False


def _shop_print_compulsory_on_sale_enabled(shop: dict) -> bool:
    """IT setting: finalized cash sales must go through receipt printing workflow."""
    return bool(_effective_printing_settings_for_shop(shop).get("print_compulsory_sale"))


def _printer_type_allowed_by_printing_settings(ps: dict, printer_type: str) -> bool:
    """Whether IT allows saving/using this POS printer connection type."""
    pt = (printer_type or "").strip().lower()
    if pt == "bluetooth":
        return bool(ps.get("printer_allow_bluetooth"))
    if pt == "network":
        return bool(ps.get("printer_allow_network"))
    if pt == "usb":
        return bool(ps.get("printer_allow_usb"))
    return False


def _shop_has_saved_pos_printer(shop_id: int) -> bool:
    """True if shop has a saved printer whose type is allowed by current IT settings."""
    try:
        from database import get_shop_by_id, get_shop_printer_settings

        row = get_shop_printer_settings(shop_id)
        shop = get_shop_by_id(shop_id)
    except Exception:
        row = None
        shop = None
    if not row or not shop:
        return False
    if not (row.get("printer_type") or "").strip():
        return False
    ps = _effective_printing_settings_for_shop(shop)
    return _printer_type_allowed_by_printing_settings(ps, row.get("printer_type") or "")


def _shop_compulsory_printer_record_sale_gate(shop_id: int) -> Tuple[bool, str]:
    """When compulsory printing is on: require a saved printer; network printers must be TCP-reachable (or LAN print agent)."""
    if not _shop_has_saved_pos_printer(shop_id):
        return False, "Printing is compulsory on sale: configure a receipt printer for this shop before checkout."
    row, cfg = _printer_config_dict(shop_id)
    if not row:
        return False, "Printing is compulsory on sale: configure a receipt printer for this shop before checkout."
    pt = (row.get("printer_type") or "").strip().lower()
    ps = _effective_printing_settings_for_shop(_get_shop_or_404(shop_id))
    if not _printer_type_allowed_by_printing_settings(ps, pt):
        allowed = []
        if ps.get("printer_allow_bluetooth"):
            allowed.append("Bluetooth")
        if ps.get("printer_allow_network"):
            allowed.append("network")
        if ps.get("printer_allow_usb"):
            allowed.append("USB")
        hint = ", ".join(allowed) if allowed else "an allowed type"
        return (
            False,
            f"Printing is compulsory on sale: saved printer is {pt or 'unknown'}, but IT only allows {hint}. "
            "Open printer setup, tap Forget saved printer, then connect an allowed printer.",
        )
    if pt != "network":
        return True, ""
    if cfg.get("print_agent_enabled") and (cfg.get("print_agent_token") or "").strip():
        return True, ""
    host = (cfg.get("host") or "").strip()
    try:
        port = int(cfg.get("port") or 9100)
    except (TypeError, ValueError):
        port = 9100
    if not host or port < 1 or port > 65535:
        return False, "Printing is compulsory on sale: invalid network printer address."
    if _tcp_probe_host_port(host, port, 3.0):
        return True, ""
    return (
        False,
        "Printing is compulsory on sale: network printer is not reachable from the server. Check it is on and the IP/port are correct.",
    )


_POS_CART_MODES: Tuple[str, ...] = ("direct", "withhold")


def _coerce_pos_cart_mode(raw: object) -> str:
    """Normalize stored / submitted cart-mode value to a known choice; unknown → 'direct'."""
    if isinstance(raw, str):
        c = raw.strip().lower()
        if c in _POS_CART_MODES:
            return c
    return "direct"


def _default_printing_settings() -> dict:
    return {
        "print_compulsory_sale": False,
        "allow_line_price_edit": False,
        "pos_allow_cash_sale": True,
        "pos_allow_credit_sale": True,
        "pos_allow_quotations": True,
        "pos_payment_cash": True,
        "pos_payment_mpesa": False,
        "pos_payment_both": False,
        "pos_show_buy_items_link": True,
        "pos_show_customer_details_sale": True,
        "pos_cart_amount_sets_qty": False,
        "pos_include_tax": True,
        "pos_inventory_use_both": False,
        "pos_kitchen_portions": False,
        "pos_shop_stock_sale": True,
        "pos_cart_mode": "direct",
        "receipt_copies": "1",
        "printer_allow_bluetooth": True,
        "printer_allow_network": True,
        "printer_allow_usb": True,
    }


def _load_printing_settings() -> dict:
    from database import get_site_settings

    defaults = _default_printing_settings()
    raw = get_site_settings(["printing_settings_json"]).get("printing_settings_json") or "{}"
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        data = {}
    if not isinstance(data, dict):
        data = {}
    merged = {**defaults, **data}
    _normalize_print_compulsory_key(merged)
    _coalesce_printer_allow_nulls(merged)
    for k in (
        "print_compulsory_sale",
        "allow_line_price_edit",
        "pos_allow_cash_sale",
        "pos_allow_credit_sale",
        "pos_allow_quotations",
        "pos_payment_cash",
        "pos_payment_mpesa",
        "pos_payment_both",
        "pos_show_buy_items_link",
        "pos_show_customer_details_sale",
        "pos_cart_amount_sets_qty",
        "pos_include_tax",
        "pos_inventory_use_both",
        "pos_kitchen_portions",
        "pos_shop_stock_sale",
        "printer_allow_bluetooth",
        "printer_allow_network",
        "printer_allow_usb",
    ):
        merged[k] = merged.get(k) in (True, "true", "1", 1, "True")
    rc = str(merged.get("receipt_copies") or "1").strip()
    if rc not in ("1", "2", "3"):
        rc = "1"
    merged["receipt_copies"] = rc
    merged["pos_cart_mode"] = _coerce_pos_cart_mode(merged.get("pos_cart_mode"))
    _ensure_at_least_one_pos_transactional_type(merged)
    _ensure_at_least_one_pos_payment_method(merged)
    _sync_print_compulsory_with_printer_allow_list(merged)
    _enforce_exclusive_pos_inventory(merged)
    return merged


def _printing_settings_from_form() -> dict:
    def _b(key: str) -> bool:
        return (request.form.get(key) or "").strip() == "1"

    rc = (request.form.get("printing_receipt_copies") or "1").strip()
    if rc not in ("1", "2", "3"):
        rc = "1"
    allow_price = _b("printing_allow_line_price_edit") or _b("pos_allow_line_price_edit")
    inv_choice = (request.form.get("pos_inventory_exclusive") or "shop").strip().lower()
    if inv_choice not in ("shop", "kitchen", "dual"):
        inv_choice = "shop"
    cart_mode = _coerce_pos_cart_mode(request.form.get("pos_cart_mode"))
    merged = {
        "print_compulsory_sale": _b("printing_compulsory_sale"),
        "allow_line_price_edit": allow_price,
        "pos_allow_cash_sale": _b("pos_allow_cash_sale"),
        "pos_allow_credit_sale": _b("pos_allow_credit_sale"),
        "pos_allow_quotations": _b("pos_allow_quotations"),
        "pos_payment_cash": _b("pos_payment_cash"),
        "pos_payment_mpesa": _b("pos_payment_mpesa"),
        "pos_payment_both": _b("pos_payment_both"),
        "pos_show_buy_items_link": _b("pos_show_buy_items_link"),
        "pos_show_customer_details_sale": _b("pos_show_customer_details_sale"),
        "pos_cart_amount_sets_qty": _b("pos_cart_amount_sets_qty"),
        "pos_include_tax": _b("pos_include_tax"),
        "pos_inventory_use_both": False,
        "pos_kitchen_portions": False,
        "pos_shop_stock_sale": True,
        "pos_cart_mode": cart_mode,
        "receipt_copies": rc,
        "printer_allow_bluetooth": _b("printing_allow_bluetooth"),
        "printer_allow_network": _b("printing_allow_network"),
        "printer_allow_usb": _b("printing_allow_usb"),
    }
    _apply_pos_inventory_exclusive_form_choice(merged, inv_choice)
    _ensure_at_least_one_pos_transactional_type(merged)
    _ensure_at_least_one_pos_payment_method(merged)
    _sync_print_compulsory_with_printer_allow_list(merged)
    _enforce_exclusive_pos_inventory(merged)
    return merged


# Form field name (POS settings tab checkboxes) -> key in merged printing_settings dict
_POS_PANEL_PRINTING_PATCH_FIELDS: Tuple[Tuple[str, str], ...] = (
    ("pos_allow_cash_sale", "pos_allow_cash_sale"),
    ("pos_allow_credit_sale", "pos_allow_credit_sale"),
    ("pos_allow_quotations", "pos_allow_quotations"),
    ("pos_payment_cash", "pos_payment_cash"),
    ("pos_payment_mpesa", "pos_payment_mpesa"),
    ("pos_payment_both", "pos_payment_both"),
    ("pos_show_buy_items_link", "pos_show_buy_items_link"),
    ("pos_show_customer_details_sale", "pos_show_customer_details_sale"),
    ("pos_cart_amount_sets_qty", "pos_cart_amount_sets_qty"),
    ("pos_include_tax", "pos_include_tax"),
    ("pos_inventory_use_both", "pos_inventory_use_both"),
    ("pos_kitchen_portions", "pos_kitchen_portions"),
    ("pos_shop_stock_sale", "pos_shop_stock_sale"),
    ("pos_allow_line_price_edit", "allow_line_price_edit"),
    ("printing_compulsory_sale", "print_compulsory_sale"),
    ("printing_allow_bluetooth", "printer_allow_bluetooth"),
    ("printing_allow_network", "printer_allow_network"),
    ("printing_allow_usb", "printer_allow_usb"),
)


def _pos_patch_bool(raw: object) -> Optional[bool]:
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, (int, float)):
        return bool(int(raw))
    if isinstance(raw, str):
        t = raw.strip().lower()
        if t in ("true", "1", "yes", "on"):
            return True
        if t in ("false", "0", "no", "off", ""):
            return False
    return None


def _printing_settings_apply_pos_panel_patch_dict(merged: dict, patch: dict) -> None:
    """Apply JSON patch (form field names → bool); mutates merged. Mirrors _printing_settings_from_form inventory rules."""
    if not isinstance(patch, dict):
        return
    inv_ex = patch.get("pos_inventory_exclusive")
    inv_exclusive: Optional[str] = None
    if isinstance(inv_ex, str):
        c = inv_ex.strip().lower()
        if c in ("shop", "kitchen", "dual"):
            inv_exclusive = c
    if inv_exclusive is not None:
        _apply_pos_inventory_exclusive_form_choice(merged, inv_exclusive)
    if "pos_cart_mode" in patch:
        merged["pos_cart_mode"] = _coerce_pos_cart_mode(patch.get("pos_cart_mode"))
    for form_name, mkey in _POS_PANEL_PRINTING_PATCH_FIELDS:
        if inv_exclusive is not None and form_name in (
            "pos_inventory_use_both",
            "pos_kitchen_portions",
            "pos_shop_stock_sale",
        ):
            continue
        if form_name not in patch:
            continue
        b = _pos_patch_bool(patch.get(form_name))
        if b is None:
            continue
        merged[mkey] = b
    _enforce_exclusive_pos_inventory(merged)
    _ensure_at_least_one_pos_transactional_type(merged)
    _ensure_at_least_one_pos_payment_method(merged)
    _sync_print_compulsory_with_printer_allow_list(merged)


def _printing_settings_pos_panel_client_payload(merged: dict) -> Dict[str, Any]:
    """Field names matching POS tab form controls; inventory is one exclusive choice plus legacy bools."""
    out: Dict[str, Any] = {}
    for form_name, mkey in _POS_PANEL_PRINTING_PATCH_FIELDS:
        out[form_name] = bool(merged.get(mkey))
    out["pos_inventory_exclusive"] = _printing_pos_inventory_exclusive_choice(merged)
    out["pos_cart_mode"] = _coerce_pos_cart_mode(merged.get("pos_cart_mode"))
    return out


@app.route("/it_support/system-settings/printing-pos-patch", methods=["POST"])
@login_required
def it_support_printing_pos_patch():
    """Merge POS-tab printing toggles without posting the whole system-settings form."""
    role_key = str(session.get("employee_role") or "employee").strip().lower()
    if role_key not in COMPANY_PORTAL_ROLES:
        return jsonify({"ok": False, "error": "Forbidden."}), 403
    payload = request.get_json(silent=True)
    patch = payload.get("patch") if isinstance(payload, dict) else None
    if not isinstance(patch, dict):
        return jsonify({"ok": False, "error": "Expected JSON body { \"patch\": { ... } }."}), 400
    merged = dict(_load_printing_settings())
    _printing_settings_apply_pos_panel_patch_dict(merged, patch)
    try:
        from database import set_site_settings

        ok = set_site_settings(
            {"printing_settings_json": json.dumps(merged, separators=(",", ":"))}
        )
    except Exception:
        ok = False
    if not ok:
        return jsonify({"ok": False, "error": "Could not save settings."}), 500
    return jsonify(
        {"ok": True, "patch": _printing_settings_pos_panel_client_payload(merged)}
    )


@app.route("/it_support/system-settings", methods=["GET", "POST"])
@login_required
def it_support_system_settings():
    role_key = str(session.get("employee_role") or "employee").strip().lower()
    if role_key not in COMPANY_PORTAL_ROLES:
        abort(403)
    if request.method == "POST":
        wants_json = (
            request.headers.get("X-Requested-With") == "XMLHttpRequest"
            or "application/json" in (request.headers.get("Accept") or "").lower()
        )

        company_name = (request.form.get("company_name") or "").strip() or "Point of Sale"
        company_email = (request.form.get("company_email") or "").strip()
        company_phone = (request.form.get("company_phone") or "").strip()
        company_facebook = (request.form.get("company_facebook") or "").strip()
        company_instagram = (request.form.get("company_instagram") or "").strip()
        company_twitter = (request.form.get("company_twitter") or "").strip()
        company_tiktok = (request.form.get("company_tiktok") or "").strip()
        location_settings = _location_settings_from_form()
        public_app_url = _normalize_public_app_url((request.form.get("public_app_url") or "").strip())
        primary_color = (request.form.get("primary_color") or "#f97316").strip()
        accent_color = (request.form.get("accent_color") or "#fb923c").strip()
        from theme_presets import normalize_default_theme, normalize_font_family, normalize_theme_preset

        font_family = normalize_font_family((request.form.get("font_family") or "").strip())
        default_theme = normalize_default_theme((request.form.get("default_theme") or "").strip())
        theme_preset = normalize_theme_preset((request.form.get("theme_preset") or "").strip())
        app_icon_file = request.files.get("app_icon")
        remove_app_icon = (request.form.get("remove_app_icon") or "").strip() == "1"

        def _ok_hex(s: str) -> bool:
            s = (s or "").strip().lstrip("#")
            return len(s) in (3, 6) and all(c in "0123456789abcdefABCDEF" for c in s)

        if not _ok_hex(primary_color):
            primary_color = "#f97316"
        if not _ok_hex(accent_color):
            accent_color = "#fb923c"
        app_icon_path = None
        if remove_app_icon:
            app_icon_path = ""
        elif app_icon_file and getattr(app_icon_file, "filename", ""):
            app_icon_path = _save_branding_upload(app_icon_file)
            if app_icon_path is None:
                _icon_err = "App icon must be PNG, JPG, GIF, WebP, ICO, or SVG."
                if wants_json:
                    return jsonify({"ok": False, "error": _icon_err}), 400
                flash(_icon_err, "error")
                return redirect(
                    url_for("it_support_system_settings") + (request.form.get("return_hash") or "")
                )

        try:
            from database import set_site_settings

            daraja_form = _daraja_settings_from_form()
            daraja_oauth_err = _test_daraja_oauth_settings(daraja_form)
            if not daraja_form.get("daraja_enabled"):
                daraja_form["daraja_oauth_ok"] = None
            elif daraja_oauth_err:
                daraja_form["daraja_oauth_ok"] = False
            else:
                daraja_form["daraja_oauth_ok"] = True
            daraja_form["daraja_oauth_checked_at"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
            values = {
                "company_name": company_name,
                "company_email": company_email,
                "company_phone": company_phone,
                "company_facebook": company_facebook,
                "company_instagram": company_instagram,
                "company_twitter": company_twitter,
                "company_tiktok": company_tiktok,
                **location_settings,
                "public_app_url": public_app_url,
                "primary_color": primary_color,
                "accent_color": accent_color,
                "font_family": font_family,
                "default_theme": default_theme,
                "theme_preset": theme_preset,
                "receipt_settings_json": json.dumps(_receipt_settings_from_form(), separators=(",", ":")),
                "printing_settings_json": json.dumps(_printing_settings_from_form(), separators=(",", ":")),
                COMPANY_OPENING_HOURS_JSON_KEY: json.dumps(
                    _company_opening_hours_from_form(), separators=(",", ":")
                ),
            }
            values.update(_daraja_settings_persist_payload(daraja_form))
            if app_icon_path is not None:
                values["app_icon"] = app_icon_path
            ok = set_site_settings(values)
        except Exception:
            ok = False

        if ok:
            if wants_json:
                ps_after = _load_printing_settings()
                payload = {
                    "ok": True,
                    "printing_pos_patch": _printing_settings_pos_panel_client_payload(ps_after),
                    "printing_compulsory_sale": bool(ps_after.get("print_compulsory_sale")),
                    "daraja_oauth_ok": bool(daraja_form.get("daraja_oauth_ok")),
                }
                if daraja_oauth_err:
                    payload["daraja_oauth_warning"] = daraja_oauth_err
                return jsonify(payload)
            if daraja_oauth_err:
                flash(f"Settings saved, but Daraja OAuth failed: {daraja_oauth_err}", "error")
            else:
                flash("Settings updated.", "success")
        else:
            if wants_json:
                return jsonify(
                    {"ok": False, "error": "Could not update settings. Check database connection."}
                ), 500
            flash("Could not update settings. Check database connection.", "error")
        return redirect(url_for("it_support_system_settings") + (request.form.get("return_hash") or ""))
    _tab_raw = (request.args.get("tab") or "").strip().lower()
    _tab_q = {"printing": "receipt", "website": "theme"}.get(_tab_raw, _tab_raw)
    _valid_tabs = ("system", "theme", "company", "website", "pos", "receipt")
    initial_settings_tab = _tab_q if _tab_q in _valid_tabs else None
    from theme_presets import fonts_for_template, google_fonts_url, theme_presets_for_template
    from daraja_api import preview_stk_callback_url

    font_ids = [f["id"] for f in fonts_for_template()]
    ws = _load_website_settings()
    design_defaults = _default_website_design()
    products = _website_featured_products(limit=WEBSITE_HOMEPAGE_FEATURED_MAX)
    selected_ids = ws.get("featured_item_ids") or []
    return render_template(
        "it_support_system_settings.html",
        receipt_settings=_load_receipt_settings(),
        printing_settings=_load_printing_settings(),
        daraja_settings=_daraja_settings_for_template(),
        hosted_api_base=_effective_public_app_url(),
        stk_callback_preview=preview_stk_callback_url(
            _load_daraja_settings(),
            url_for("daraja_mpesa_stk_callback", _external=True),
        ),
        public_app_url_env_override=bool(_public_app_url_from_env()),
        initial_settings_tab=initial_settings_tab,
        appearance_presets=theme_presets_for_template(),
        appearance_fonts=fonts_for_template(),
        appearance_fonts_google_url=google_fonts_url(*font_ids),
        website_design=ws.get("design") or design_defaults,
        website_featured_products=products,
        website_public_shops=_public_storefront_shops(),
        website_product_categories=_website_product_categories(products),
        website_using_auto_products=not selected_ids,
        website_homepage_featured_max=WEBSITE_HOMEPAGE_FEATURED_MAX,
        website_preview_url=_public_storefront_url(),
        catalog_preview_url=url_for("marketing_catalog", _external=False),
        preview_url=_public_storefront_url(),
        company_opening_hours=_load_company_opening_hours(),
        company_weekdays=COMPANY_WEEKDAYS,
    )


def _it_support_kitchen_portion_matrix(shops: list) -> dict:
    """Items as rows, shops as columns; cell = portions or None if item not on that shop POS."""
    from database import list_shop_kitchen_portion_editor_rows

    shop_list = []
    shop_ids: list = []
    for s in shops or []:
        try:
            sid = int(s.get("id") or 0)
        except (TypeError, ValueError):
            continue
        if sid <= 0:
            continue
        shop_list.append(s)
        shop_ids.append(sid)

    item_map: Dict[int, dict] = {}
    for s in shop_list:
        sid = int(s["id"])
        try:
            rows = list_shop_kitchen_portion_editor_rows(shop_id=sid, limit=5000)
        except Exception:
            rows = []
        for r in rows:
            try:
                iid = int(r.get("id") or 0)
            except (TypeError, ValueError):
                continue
            if iid <= 0:
                continue
            name = (r.get("name") or "").strip()
            cat = (r.get("category") or "").strip().upper()
            if iid not in item_map:
                item_map[iid] = {
                    "item_id": iid,
                    "name": name,
                    "sort_key": (cat, name.upper()),
                    "portions": {},
                    "production_prices": {},
                }
            item_map[iid]["portions"][sid] = int(r.get("portions_remaining") or 0)
            item_map[iid]["production_prices"][sid] = round(
                float(r.get("estimated_production_price") or 0), 2
            )

    for data in item_map.values():
        for sid in shop_ids:
            if sid not in data["portions"]:
                data["portions"][sid] = None
            if sid not in data["production_prices"]:
                data["production_prices"][sid] = None

    matrix_rows = sorted(item_map.values(), key=lambda x: x["sort_key"])

    shop_modes: Dict[int, str] = {}
    shop_editable: Dict[int, bool] = {}
    for s in shop_list:
        sid = int(s["id"])
        m = _pos_inventory_mode(s)
        shop_modes[sid] = m
        shop_editable[sid] = m in ("kitchen", "both")

    return {
        "shops": shop_list,
        "shop_ids": shop_ids,
        "matrix_rows": matrix_rows,
        "shop_modes": shop_modes,
        "shop_editable": shop_editable,
        "any_editable": any(shop_editable.values()) if shop_editable else False,
    }


@app.route("/it_support/kitchen-portions", methods=["GET"])
@login_required
def it_support_dashboard():
    """IT hub: matrix of items × shops with kitchen portion counts."""
    _it_support_or_super_admin_only()
    from database import list_shops

    shops = list_shops(limit=5000) or []
    km = _it_support_kitchen_portion_matrix(shops)
    return render_template(
        "it_support_dashboard.html",
        kitchen_matrix=km,
    )


@app.route("/it_support/kitchen-portions", methods=["POST"])
@login_required
def it_support_kitchen_portions_save():
    _it_support_or_super_admin_only()
    from database import upsert_shop_kitchen_portion_qty

    saved = 0
    try:
        portion_vals: Dict[tuple, int] = {}
        price_vals: Dict[tuple, float] = {}
        for key, raw in request.form.items():
            m = re.match(r"^p_(\d+)_(\d+)$", key)
            if m:
                portion_vals[(int(m.group(1)), int(m.group(2)))] = int(str(raw or "").strip() or "0")
                continue
            m2 = re.match(r"^pp_(\d+)_(\d+)$", key)
            if m2:
                try:
                    price_vals[(int(m2.group(1)), int(m2.group(2)))] = round(
                        float(str(raw or "").strip() or "0"), 2
                    )
                except (TypeError, ValueError):
                    price_vals[(int(m2.group(1)), int(m2.group(2)))] = 0.0
        keys = set(portion_vals.keys()) | set(price_vals.keys())
        for shop_id, item_id in keys:
            shop = _get_shop_or_404(shop_id)
            if _pos_inventory_mode(shop) not in ("kitchen", "both"):
                continue
            try:
                q = int(portion_vals.get((shop_id, item_id), 0))
            except (TypeError, ValueError):
                q = 0
            price = price_vals.get((shop_id, item_id), 0.0)
            if upsert_shop_kitchen_portion_qty(
                shop_id, item_id, q, estimated_production_price=price
            ):
                saved += 1
        if saved:
            flash("Kitchen portions updated.", "success")
        else:
            flash(
                "Nothing was saved. Turn on kitchen mode for a shop and edit its cells, then save.",
                "error",
            )
    except Exception:
        flash("Could not save kitchen portions.", "error")
    return redirect(url_for("it_support_dashboard"))


def _it_support_kitchen_analytics_params() -> dict:
    """Parse filter, date range, shop, and tab for kitchen portion analytics."""
    filter_type = (request.args.get("filter") or "period").strip().lower()
    if filter_type not in ("day", "period", "month"):
        filter_type = "period"
    today = date.today()
    shop_id = request.args.get("shop_id", type=int) or 0
    tab = (request.args.get("tab") or "raw").strip().lower()
    if tab not in ("raw", "visuals"):
        tab = "raw"

    date_from = today - timedelta(days=6)
    date_to = today

    if filter_type == "day":
        ds = (request.args.get("on") or "").strip()[:10]
        if ds and re.match(r"^\d{4}-\d{2}-\d{2}$", ds):
            try:
                y, m, d = (int(x) for x in ds.split("-", 2))
                date_from = date(y, m, d)
                date_to = date_from
            except Exception:
                date_from = today
                date_to = today
        else:
            date_from = today
            date_to = today
    elif filter_type == "month":
        ms = (request.args.get("month") or "").strip()[:7]
        if ms and re.match(r"^\d{4}-\d{2}$", ms):
            try:
                y, mo = (int(x) for x in ms.split("-", 1))
                date_from = date(y, mo, 1)
                ld = calendar.monthrange(y, mo)[1]
                date_to = date(y, mo, ld)
            except Exception:
                date_from = date(today.year, today.month, 1)
                date_to = date(
                    today.year,
                    today.month,
                    calendar.monthrange(today.year, today.month)[1],
                )
        else:
            date_from = date(today.year, today.month, 1)
            date_to = date(
                today.year,
                today.month,
                calendar.monthrange(today.year, today.month)[1],
            )
    else:
        df = (request.args.get("date_from") or "").strip()[:10]
        dt = (request.args.get("date_to") or "").strip()[:10]
        if df and re.match(r"^\d{4}-\d{2}-\d{2}$", df):
            try:
                y, m, d = (int(x) for x in df.split("-", 2))
                date_from = date(y, m, d)
            except Exception:
                pass
        if dt and re.match(r"^\d{4}-\d{2}-\d{2}$", dt):
            try:
                y, m, d = (int(x) for x in dt.split("-", 2))
                date_to = date(y, m, d)
            except Exception:
                pass
        if date_to < date_from:
            date_to = date_from

    return {
        "filter_type": filter_type,
        "date_from": date_from,
        "date_to": date_to,
        "shop_id": shop_id,
        "tab": tab,
    }


def _kitchen_analytics_url_kwargs(p: dict, tab: str) -> dict:
    """Build query kwargs for kitchen analytics URLs (preserve filter + dates + shop)."""
    kw: dict = {"filter": p["filter_type"], "tab": tab}
    if p["filter_type"] == "day":
        kw["on"] = p["date_from"].isoformat()
    elif p["filter_type"] == "month":
        kw["month"] = f"{p['date_from'].year:04d}-{p['date_from'].month:02d}"
    else:
        kw["date_from"] = p["date_from"].isoformat()
        kw["date_to"] = p["date_to"].isoformat()
    if p.get("shop_id"):
        kw["shop_id"] = int(p["shop_id"])
    return kw


def _shop_kitchen_analytics_tab_url(shop_id: int, p: dict, tab: str) -> str:
    """Tab URL for shop-scoped kitchen analytics (shop_id is in the path, not the query string)."""
    kw = _kitchen_analytics_url_kwargs({**p, "shop_id": shop_id}, tab)
    kw.pop("shop_id", None)
    return url_for("shop_kitchen_portion_analytics", shop_id=shop_id, **kw)


@app.route("/it_support/kitchen-portions/analytics")
@login_required
def it_support_kitchen_portion_analytics():
    """Kitchen portions sold (POS checkouts in kitchen mode) — raw lines and charts."""
    _it_support_or_super_admin_only()
    from database import (
        kitchen_portion_analytics_by_day,
        kitchen_portion_analytics_by_item,
        list_kitchen_portion_analytics_lines,
        list_shops,
    )

    params = _it_support_kitchen_analytics_params()
    df = params["date_from"]
    dt = params["date_to"]
    shop_filter = params["shop_id"] if params["shop_id"] > 0 else None

    shops = list_shops(limit=5000) or []
    raw_lines = list_kitchen_portion_analytics_lines(
        date_from=df,
        date_to=dt,
        shop_id=shop_filter,
        limit=50000,
    )
    by_item = kitchen_portion_analytics_by_item(
        date_from=df,
        date_to=dt,
        shop_id=shop_filter,
        limit=40,
    )
    by_day = kitchen_portion_analytics_by_day(
        date_from=df,
        date_to=dt,
        shop_id=shop_filter,
    )

    tab_raw_url = url_for(
        "it_support_kitchen_portion_analytics",
        **_kitchen_analytics_url_kwargs(params, "raw"),
    )
    tab_visuals_url = url_for(
        "it_support_kitchen_portion_analytics",
        **_kitchen_analytics_url_kwargs(params, "visuals"),
    )

    return render_template(
        "it_support_kitchen_portion_analytics.html",
        analytics=params,
        shops=shops,
        raw_lines=raw_lines,
        chart_by_item=by_item,
        chart_by_day=by_day,
        tab_raw_url=tab_raw_url,
        tab_visuals_url=tab_visuals_url,
    )


def _it_support_only():
    """Company portal admin routes: IT support, super admin, and company manager (same access tier)."""
    role_key = str(session.get("employee_role") or "employee").strip().lower()
    if role_key not in COMPANY_PORTAL_ROLES:
        abort(403)


def _request_wants_json() -> bool:
    return (
        request.headers.get("X-Requested-With") == "XMLHttpRequest"
        or "application/json" in (request.headers.get("Accept") or "").lower()
    )


def _request_data_str(data, key: str) -> str:
    """Coerce a form or JSON request field to a stripped string."""
    val = data.get(key)
    if val is None:
        return ""
    if isinstance(val, str):
        return val.strip()
    return str(val).strip()


def _it_support_or_super_admin_only():
    _it_support_only()


def _get_shop_or_404(shop_id: int):
    try:
        from database import get_shop_by_id

        shop = get_shop_by_id(shop_id)
    except Exception:
        app.logger.exception("Database error while loading shop id=%s", shop_id)
        abort(503)
    if not shop:
        abort(404)
    return shop


def _require_shop_access(shop: dict):
    # IT support / super admin / company manager can open any shop session view directly.
    if str(session.get("employee_role") or "").strip().lower() in COMPANY_PORTAL_ROLES:
        session["shop_id"] = int(shop["id"])
        session["shop_name"] = shop.get("shop_name")
        return None

    shop_id_int = int(shop["id"])

    # Employee portal (manager, admin, staff): may use primary shop_id or linked branches
    # (employee_shop_access). Session shop_id alone is not enough — it stays on the primary branch.
    emp_id = session.get("employee_id")
    emp = None
    if emp_id:
        try:
            from database import get_employee_by_id, employee_may_use_shop_branch

            emp = get_employee_by_id(int(emp_id))
        except Exception:
            emp = None
        if emp and employee_may_use_shop_branch(emp, shop_id_int):
            session["shop_id"] = shop_id_int
            session["shop_name"] = shop.get("shop_name")
            if shop.get("status") != "active":
                flash("This shop is suspended. Contact IT support.", "error")
                return _redirect_to_employee_dashboard()
            return None

    # Shop password sessions: must match branch exactly (also when employee branch check failed).
    if int(session.get("shop_id") or 0) == shop_id_int:
        if shop.get("status") != "active":
            flash("This shop is suspended. Contact IT support.", "error")
            return redirect(url_for("shop_login", shop_id=shop_id_int))
        return None

    if emp_id and emp:
        flash("You don't have access to this shop.", "error")
        return _redirect_to_employee_dashboard()

    return redirect(url_for("shop_login", shop_id=shop_id_int))


@app.route("/it_support/item-management")
@login_required
def it_support_item_management():
    _it_support_only()
    try:
        from database import list_items

        items = list_items(limit=200)
    except Exception:
        items = []
    return render_template(
        "it_support_item_management.html",
        items=items,
        pos_inventory_mode=_pos_inventory_mode_from_ps(_load_printing_settings()),
    )


@app.route("/it_support/item-management/register-item", methods=["GET", "POST"])
@login_required
def it_support_register_item():
    _it_support_only()
    if request.method == "POST":
        category = (request.form.get("category") or "").strip().upper()
        name = (request.form.get("name") or "").strip().upper()
        description = (request.form.get("description") or "").strip().upper()
        price_raw = (request.form.get("price") or "").strip()
        selling_raw = (request.form.get("selling_price") or "").strip()

        if not category or not name or not price_raw:
            flash("Please fill item category, item name, and original selling price.", "error")
            return redirect(url_for("it_support_register_item"))

        try:
            price = float(price_raw)
            if price < 0:
                raise ValueError()
        except Exception:
            flash("Original selling price must be a valid number.", "error")
            return redirect(url_for("it_support_register_item"))

        selling_price = None
        if selling_raw:
            try:
                selling_price = float(selling_raw)
                if selling_price < 0:
                    raise ValueError()
            except Exception:
                flash("Selling price must be a valid number.", "error")
                return redirect(url_for("it_support_register_item"))

        img = request.files.get("image")
        image_path = _save_item_upload(img) if img and img.filename else None
        if img and img.filename and image_path is None:
            flash("Item image must be PNG, JPG, GIF, or WebP.", "error")
            return redirect(url_for("it_support_register_item"))

        try:
            from database import (
                create_item,
                init_shop_items_table,
                seed_shop_items_for_company_item,
            )

            init_shop_items_table()
            new_item_id = create_item(
                category=category,
                name=name,
                description=description,
                price=price,
                selling_price=selling_price,
                image_path=image_path,
                status="active",
                created_by_employee_id=session.get("employee_id"),
            )
            if new_item_id:
                seed_shop_items_for_company_item(int(new_item_id))
        except Exception:
            flash("Could not register item. Check database connection.", "error")
            return redirect(url_for("it_support_register_item"))

        _log_hr_activity_safe(
            "register",
            target_type="item",
            target_id=int(new_item_id) if isinstance(new_item_id, int) else None,
            description=f"Registered item '{name}' ({category})",
        )
        flash("Item registered.", "success")
        return redirect(url_for("it_support_item_management"))

    return render_template("it_support_register_item.html")


def _it_support_stock_page(direction: str):
    """Shared logic for IT stock in/out pages."""
    direction = (direction or "in").strip().lower()
    if direction not in ("in", "out"):
        direction = "in"

    if request.method == "POST":
        item_id_raw = (request.form.get("item_id") or "").strip()
        qty_raw = (request.form.get("qty") or "").strip()
        buying_price_raw = (request.form.get("buying_price") or "").strip()
        seller_phone = (request.form.get("seller_phone") or "").strip()
        seller_name = (request.form.get("seller_name") or "").strip().upper()
        stock_out_reason = (request.form.get("stock_out_reason") or "").strip().lower()
        refunded_raw = (request.form.get("refunded") or "").strip().lower()
        refund_amount_raw = (request.form.get("refund_amount") or "").strip()
        note = (request.form.get("note") or "").strip().upper()

        try:
            item_id = int(item_id_raw)
        except Exception:
            flash("Invalid item.", "error")
            return redirect(url_for("it_support_stock_in" if direction == "in" else "it_support_stock_out"))

        from database import normalize_stock_move_qty

        qty = normalize_stock_move_qty(qty_raw)
        if qty is None:
            flash("Quantity must be a positive number (decimals allowed, e.g. 0.15).", "error")
            return redirect(
                url_for("it_support_stock_in" if direction == "in" else "it_support_stock_out", item_id=item_id)
            )

        buying_price = None
        refund_amount = None
        refunded = refunded_raw == "yes"
        if direction == "in":
            if not buying_price_raw:
                flash("Buying price is required for stock in.", "error")
                return redirect(url_for("it_support_stock_in", item_id=item_id))
            if not seller_phone:
                flash("Seller phone is required for stock in.", "error")
                return redirect(url_for("it_support_stock_in", item_id=item_id))
            try:
                buying_price = float(buying_price_raw)
                if buying_price < 0:
                    raise ValueError()
            except Exception:
                flash("Buying price must be a valid number.", "error")
                return redirect(url_for("it_support_stock_in", item_id=item_id))
        else:
            allowed_reasons = {"return", "waste", "display"}
            if stock_out_reason not in allowed_reasons:
                flash("Please choose a valid stock out reason.", "error")
                return redirect(url_for("it_support_stock_out", item_id=item_id))
            if refunded_raw not in ("yes", "no"):
                flash("Please choose if the stock out is refunded.", "error")
                return redirect(url_for("it_support_stock_out", item_id=item_id))
            if refunded:
                if not refund_amount_raw:
                    flash("Refund amount is required when refunded is YES.", "error")
                    return redirect(url_for("it_support_stock_out", item_id=item_id))
                try:
                    refund_amount = float(refund_amount_raw)
                    if refund_amount < 0:
                        raise ValueError()
                except Exception:
                    flash("Refund amount must be a valid number.", "error")
                    return redirect(url_for("it_support_stock_out", item_id=item_id))

        try:
            from database import create_stock_transaction, resolve_seller_name_and_phone

            resolved_name, resolved_phone = (None, None)
            if direction == "in":
                resolved_name, resolved_phone = resolve_seller_name_and_phone(
                    seller_phone=seller_phone,
                    seller_name=seller_name,
                )
                if not resolved_name or not resolved_phone:
                    flash("Seller phone must be valid. If new, enter seller name to register.", "error")
                    return redirect(url_for("it_support_stock_in", item_id=item_id))

            ok = create_stock_transaction(
                item_id=item_id,
                direction=direction,
                qty=qty,
                buying_price=buying_price,
                place_brought_from=(resolved_name if direction == "in" else None),
                seller_phone=(resolved_phone if direction == "in" else None),
                stock_out_reason=stock_out_reason.upper() if direction == "out" else None,
                refunded=refunded if direction == "out" else False,
                refund_amount=refund_amount if direction == "out" else None,
                note=note or None,
                created_by_employee_id=session.get("employee_id"),
            )
        except Exception:
            ok = False

        if not ok:
            flash("Could not update stock. Ensure item is ACTIVE and stock update is enabled.", "error")
            return redirect(
                url_for("it_support_stock_in" if direction == "in" else "it_support_stock_out", item_id=item_id)
            )

        flash("Stock updated.", "success")
        return redirect(url_for("it_support_stock_in" if direction == "in" else "it_support_stock_out", item_id=item_id))

    item_id = request.args.get("item_id", type=int)
    try:
        from database import list_stock_manage_items, list_stock_transactions

        items = list_stock_manage_items(limit=500)
        txs = list_stock_transactions(item_id, direction=direction, limit=200) if item_id else []
    except Exception:
        items, txs = [], []

    selected_item = None
    if item_id and items:
        for it in items:
            try:
                if int(it.get("id")) == int(item_id):
                    selected_item = it
                    break
            except Exception:
                continue
    return items, item_id, selected_item, txs


def _it_support_store_stock_catalog_post(action: str):
    """Edit / suspend / delete store SKUs on the IT stock-management page (Both mode)."""
    _it_support_only()
    if _global_pos_inventory_mode() != "both":
        flash("Store stock catalog actions apply only when POS inventory is Both (kitchen + shelf).", "error")
        return redirect(url_for("it_support_stock_management"))

    try:
        store_item_id = int(request.form.get("store_item_id") or 0)
    except (TypeError, ValueError):
        store_item_id = 0
    if store_item_id <= 0:
        flash("Invalid store stock item.", "error")
        return redirect(url_for("it_support_stock_management"))

    if action == "update_it_store_stock_item":
        parsed = _parse_shop_store_stock_item_form()
        if not parsed:
            flash("Name, category, and a valid measure unit are required.", "error")
            return redirect(
                url_for("it_support_stock_management", manage_item=store_item_id)
                + f"#it-store-item-{store_item_id}"
            )
        cat, nm, desc, measure = parsed
        try:
            from database import update_store_stock_item_by_id

            ok = update_store_stock_item_by_id(
                store_item_id=store_item_id,
                category=cat,
                name=nm,
                description=desc,
                measure_unit=measure,
            )
            flash(
                "Store stock item updated." if ok else "Could not update store stock item.",
                "success" if ok else "error",
            )
        except Exception as e:
            app.logger.exception("update_it_store_stock_item: %s", e)
            flash("Could not update store stock item.", "error")
        return redirect(url_for("it_support_stock_management"))

    if action == "toggle_it_store_stock_item_status":
        try:
            from database import toggle_store_stock_item_status_by_id

            new_status = toggle_store_stock_item_status_by_id(store_item_id)
        except Exception as e:
            app.logger.exception("toggle_it_store_stock_item_status: %s", e)
            new_status = None
        if new_status == "active":
            flash("Store stock item is active again (unsuspended).", "success")
        elif new_status == "inactive":
            flash("Store stock item suspended — hidden from stock in/out until unsuspended.", "success")
        else:
            flash("Could not change store stock item status.", "error")
        return redirect(url_for("it_support_stock_management"))

    if action == "delete_it_store_stock_item":
        try:
            from database import delete_store_stock_item_by_id

            ok, msg = delete_store_stock_item_by_id(store_item_id)
        except Exception as e:
            app.logger.exception("delete_it_store_stock_item: %s", e)
            ok, msg = False, "Could not delete store stock item."
        flash(msg, "success" if ok else "error")
        return redirect(url_for("it_support_stock_management"))

    flash("Invalid action.", "error")
    return redirect(url_for("it_support_stock_management"))


def _it_support_stock_management_post():
    """Bulk stock in/out from the company stock management grid."""
    _it_support_only()
    direction = (request.form.get("bulk_direction") or "").strip().lower()
    if direction not in ("in", "out"):
        flash("Invalid action.", "error")
        return redirect(url_for("it_support_stock_management"))

    is_store_stock = _global_pos_inventory_mode() == "both"
    if is_store_stock:
        return _it_support_store_stock_management_post(direction)

    try:
        from database import (
            create_stock_transactions_batch,
            list_stock_manage_items,
            normalize_stock_move_qty,
            resolve_seller_name_and_phone,
        )

        items = list_stock_manage_items(limit=500)
    except Exception:
        items = []
    if not items:
        flash("No eligible items found.", "error")
        return redirect(url_for("it_support_stock_management"))

    allowed_reasons = {"return", "waste", "display"}
    allowed_pay = frozenset({"pending_payment", "partially_paid", "paid"})
    errors: list[str] = []
    operations: list[dict] = []

    for it in items:
        try:
            iid = int(it.get("id"))
        except Exception:
            continue
        if direction == "in":
            qty_raw = (request.form.get(f"in_qty_{iid}") or "").strip()
            bp_raw = (request.form.get(f"in_buying_price_{iid}") or "").strip()
            place = (request.form.get(f"in_place_{iid}") or "").strip()
            phone_raw = (request.form.get(f"in_seller_phone_{iid}") or "").strip()
            pay_raw = (request.form.get(f"in_payment_status_{iid}") or "").strip().lower()
            label = it.get("name") or f"Item #{iid}"
            pay_selected = pay_raw in allowed_pay
            partial_without_qty = bool(bp_raw or place or phone_raw or pay_selected)

            if not qty_raw:
                if partial_without_qty:
                    errors.append(
                        f"{label}: enter a stock-in quantity or clear buying price, seller, phone, and payment."
                    )
                continue
            qty = normalize_stock_move_qty(qty_raw)
            if qty is None:
                errors.append(f"{it.get('name') or ('Item #' + str(iid))}: invalid quantity.")
                continue
            if not bp_raw:
                errors.append(f"{label}: buying price is required when quantity is set.")
                continue
            if not place:
                errors.append(f"{label}: place bought is required when quantity is set.")
                continue
            if pay_raw not in allowed_pay:
                errors.append(f"{label}: select payment (Not paid, Partially paid, or Paid).")
                continue
            payment_status = pay_raw
            place_final = place.upper()
            resolved_phone = None
            if phone_raw:
                rn, rp = resolve_seller_name_and_phone(phone_raw, place)
                if not rn or not rp:
                    errors.append(
                        f"{label}: seller phone must be valid (07… or 254…). "
                        "If new, enter seller name in the seller field."
                    )
                    continue
                place_final = (rn or place).strip().upper()
                resolved_phone = rp
            try:
                buying_price = float(bp_raw)
                if buying_price < 0:
                    raise ValueError()
            except Exception:
                errors.append(f"{label}: buying price must be a valid number.")
                continue
            operations.append(
                {
                    "item_id": iid,
                    "direction": "in",
                    "qty": qty,
                    "buying_price": buying_price,
                    "place_brought_from": place_final,
                    "seller_phone": resolved_phone,
                    "stock_out_reason": None,
                    "refunded": False,
                    "refund_amount": None,
                    "note": None,
                    "payment_status": payment_status,
                    "amount_paid": None,
                }
            )
        else:
            qty_raw = (request.form.get(f"out_qty_{iid}") or "").strip()
            reason = (request.form.get(f"out_reason_{iid}") or "").strip().lower()
            ram = (request.form.get(f"out_refund_amount_{iid}") or "").strip()
            label = it.get("name") or f"Item #{iid}"
            partial_out = reason in allowed_reasons or bool(ram)

            if not qty_raw:
                if partial_out:
                    errors.append(
                        f"{label}: enter a quantity out or clear reason and refund amount for this row."
                    )
                continue
            qty = normalize_stock_move_qty(qty_raw)
            if qty is None:
                errors.append(f"{it.get('name') or ('Item #' + str(iid))}: invalid quantity.")
                continue
            if reason not in allowed_reasons:
                errors.append(f"{label}: choose a stock out reason.")
                continue
            refunded_raw = (request.form.get(f"out_refunded_{iid}") or "").strip().lower()
            if refunded_raw not in ("yes", "no"):
                errors.append(f"{label}: choose whether this line is refunded.")
                continue
            refunded = refunded_raw == "yes"
            refund_amount = None
            if refunded:
                if not ram:
                    errors.append(f"{label}: refund amount is required when refunded is yes.")
                    continue
                try:
                    refund_amount = float(ram)
                    if refund_amount < 0:
                        raise ValueError()
                except Exception:
                    errors.append(f"{label}: refund amount must be a valid number.")
                    continue
            operations.append(
                {
                    "item_id": iid,
                    "direction": "out",
                    "qty": qty,
                    "buying_price": None,
                    "place_brought_from": None,
                    "stock_out_reason": reason.upper(),
                    "refunded": refunded,
                    "refund_amount": refund_amount,
                    "note": None,
                }
            )

    if errors:
        flash(" ".join(errors[:6]) + (" …" if len(errors) > 6 else ""), "error")
        return redirect(url_for("it_support_stock_management"))
    if not operations:
        flash("Enter a quantity on at least one row to apply.", "error")
        return redirect(url_for("it_support_stock_management"))

    ok, msg = create_stock_transactions_batch(
        operations=operations,
        created_by_employee_id=session.get("employee_id"),
    )
    flash(msg, "success" if ok else "error")
    return redirect(url_for("it_support_stock_management"))


def _it_support_store_stock_management_post(direction: str):
    """Bulk stock in/out for the ``store_stock_items`` catalog (Both mode)."""
    try:
        from database import (
            create_store_stock_transactions_batch,
            list_store_stock_items_for_management,
            normalize_stock_move_qty,
            resolve_seller_name_and_phone,
        )

        items = list_store_stock_items_for_management(limit=2000)
    except Exception:
        items = []
    if not items:
        flash("No registered store stock items found.", "error")
        return redirect(url_for("it_support_stock_management"))

    allowed_reasons = {"return", "waste", "display"}
    allowed_pay = frozenset({"pending_payment", "partially_paid", "paid"})
    errors: list[str] = []
    operations: list[dict] = []

    for it in items:
        try:
            iid = int(it.get("id"))
        except Exception:
            continue
        label_name = it.get("name") or f"Store item #{iid}"
        shop_label = it.get("shop_name") or ""
        label = f"{label_name} ({shop_label})" if shop_label else label_name

        if direction == "in":
            qty_raw = (request.form.get(f"in_qty_{iid}") or "").strip()
            bp_raw = (request.form.get(f"in_buying_price_{iid}") or "").strip()
            place = (request.form.get(f"in_place_{iid}") or "").strip()
            phone_raw = (request.form.get(f"in_seller_phone_{iid}") or "").strip()
            pay_raw = (request.form.get(f"in_payment_status_{iid}") or "").strip().lower()
            pay_selected = pay_raw in allowed_pay
            partial_without_qty = bool(bp_raw or place or phone_raw or pay_selected)

            if not qty_raw:
                if partial_without_qty:
                    errors.append(
                        f"{label}: enter a stock-in quantity or clear buying price, seller, phone, and payment."
                    )
                continue
            qty = normalize_stock_move_qty(qty_raw)
            if qty is None:
                errors.append(f"{label}: invalid quantity.")
                continue
            if not bp_raw:
                errors.append(f"{label}: buying price is required when quantity is set.")
                continue
            if not place:
                errors.append(f"{label}: place bought is required when quantity is set.")
                continue
            if pay_raw not in allowed_pay:
                errors.append(f"{label}: select payment (Not paid, Partially paid, or Paid).")
                continue
            payment_status = pay_raw
            place_final = place.upper()
            resolved_phone = None
            if phone_raw:
                rn, rp = resolve_seller_name_and_phone(phone_raw, place)
                if not rn or not rp:
                    errors.append(
                        f"{label}: seller phone must be valid (07… or 254…). "
                        "If new, enter seller name in the seller field."
                    )
                    continue
                place_final = (rn or place).strip().upper()
                resolved_phone = rp
            try:
                buying_price = float(bp_raw)
                if buying_price < 0:
                    raise ValueError()
            except Exception:
                errors.append(f"{label}: buying price must be a valid number.")
                continue
            operations.append(
                {
                    "store_stock_item_id": iid,
                    "direction": "in",
                    "qty": qty,
                    "buying_price": buying_price,
                    "place_brought_from": place_final,
                    "seller_phone": resolved_phone,
                    "stock_out_reason": None,
                    "refunded": False,
                    "refund_amount": None,
                    "note": None,
                    "payment_status": payment_status,
                    "amount_paid": None,
                }
            )
        else:
            qty_raw = (request.form.get(f"out_qty_{iid}") or "").strip()
            reason = (request.form.get(f"out_reason_{iid}") or "").strip().lower()
            ram = (request.form.get(f"out_refund_amount_{iid}") or "").strip()
            partial_out = reason in allowed_reasons or bool(ram)

            if not qty_raw:
                if partial_out:
                    errors.append(
                        f"{label}: enter a quantity out or clear reason and refund amount for this row."
                    )
                continue
            qty = normalize_stock_move_qty(qty_raw)
            if qty is None:
                errors.append(f"{label}: invalid quantity.")
                continue
            if reason not in allowed_reasons:
                errors.append(f"{label}: choose a stock out reason.")
                continue
            refunded_raw = (request.form.get(f"out_refunded_{iid}") or "").strip().lower()
            if refunded_raw not in ("yes", "no"):
                errors.append(f"{label}: choose whether this line is refunded.")
                continue
            refunded = refunded_raw == "yes"
            refund_amount = None
            if refunded:
                if not ram:
                    errors.append(f"{label}: refund amount is required when refunded is yes.")
                    continue
                try:
                    refund_amount = float(ram)
                    if refund_amount < 0:
                        raise ValueError()
                except Exception:
                    errors.append(f"{label}: refund amount must be a valid number.")
                    continue
            operations.append(
                {
                    "store_stock_item_id": iid,
                    "direction": "out",
                    "qty": qty,
                    "buying_price": None,
                    "place_brought_from": None,
                    "stock_out_reason": reason.upper(),
                    "refunded": refunded,
                    "refund_amount": refund_amount,
                    "note": None,
                }
            )

    if errors:
        flash(" ".join(errors[:6]) + (" …" if len(errors) > 6 else ""), "error")
        return redirect(url_for("it_support_stock_management"))
    if not operations:
        flash("Enter a quantity on at least one row to apply.", "error")
        return redirect(url_for("it_support_stock_management"))

    ok, msg = create_store_stock_transactions_batch(
        operations=operations,
        created_by_employee_id=session.get("employee_id"),
    )
    flash(msg, "success" if ok else "error")
    return redirect(url_for("it_support_stock_management"))


@app.route("/it_support/item-management/stock-management", methods=["GET", "POST"])
@login_required
def it_support_stock_management():
    """Company stock grid: choose stock in or out, fill rows, submit once."""
    _it_support_only()
    if request.method == "POST":
        action = (request.form.get("action") or "").strip()
        if action in (
            "update_it_store_stock_item",
            "toggle_it_store_stock_item_status",
            "delete_it_store_stock_item",
        ):
            return _it_support_store_stock_catalog_post(action)
        return _it_support_stock_management_post()
    global_mode = _global_pos_inventory_mode()
    is_store_stock = global_mode == "both"
    show_register_stock_item_link = is_store_stock
    items: list = []
    store_catalog_items: list = []
    both_shops_count = 0
    inline_register_measure_options: tuple = ()
    manage_item_id: int | None = None
    try:
        manage_item_id = int(request.args.get("manage_item") or 0) or None
    except (TypeError, ValueError):
        manage_item_id = None
    try:
        if is_store_stock:
            from database import (
                init_store_stock_items_table,
                init_store_stock_transactions_table,
                list_shops,
                list_store_stock_items_for_management,
                list_store_stock_items_for_management_catalog,
                resolve_shop_pos_inventory_mode,
            )

            init_store_stock_items_table()
            init_store_stock_transactions_table()
            items = list_store_stock_items_for_management(limit=2000)
            store_catalog_items = list_store_stock_items_for_management_catalog(limit=5000)
            try:
                for s in (list_shops(limit=500) or []):
                    try:
                        sid = int(s.get("id") or 0)
                    except Exception:
                        sid = 0
                    if sid <= 0:
                        continue
                    try:
                        if resolve_shop_pos_inventory_mode(sid) == "both":
                            both_shops_count += 1
                    except Exception:
                        continue
            except Exception:
                both_shops_count = 0
            inline_register_measure_options = (
                "pcs",
                "kg",
                "g",
                "l",
                "ml",
                "pack",
                "crate",
                "box",
                "dozen",
                "portion",
            )
        else:
            from database import list_stock_manage_items

            items = list_stock_manage_items(limit=500)
    except Exception:
        items = []
    return render_template(
        "it_support_stock_management.html",
        items=items,
        store_catalog_items=store_catalog_items,
        manage_item_id=manage_item_id,
        show_register_stock_item_link=show_register_stock_item_link,
        is_store_stock=is_store_stock,
        both_shops_count=both_shops_count,
        inline_register_measure_options=inline_register_measure_options,
    )


@app.route("/it_support/item-management/register-stock-item", methods=["GET", "POST"])
@login_required
def it_support_register_stock_item():
    """Register dedicated store stock items for all shops in POS Both mode."""
    _it_support_only()
    try:
        from database import (
            create_store_stock_item,
            init_store_stock_items_table,
            list_store_stock_items,
            list_shops,
            resolve_shop_pos_inventory_mode,
        )
    except Exception:
        flash("Could not load stock item registration tools.", "error")
        return redirect(url_for("it_support_stock_management"))

    shops = list_shops(limit=500) or []
    both_shops = []
    for s in (shops or []):
        try:
            sid = int(s.get("id") or 0)
        except Exception:
            sid = 0
        if sid <= 0:
            continue
        try:
            if resolve_shop_pos_inventory_mode(sid) == "both":
                both_shops.append(s)
        except Exception:
            continue
    measure_options = (
        "pcs",
        "kg",
        "g",
        "l",
        "ml",
        "pack",
        "crate",
        "box",
        "dozen",
        "portion",
    )

    def _safe_next_redirect(default_endpoint: str):
        nxt = (request.form.get("next") or request.args.get("next") or "").strip()
        allowed_endpoints = {
            "it_support_stock_management",
            "it_support_register_stock_item",
        }
        if nxt in allowed_endpoints:
            return redirect(url_for(nxt))
        return redirect(url_for(default_endpoint))

    if request.method == "POST":
        if not both_shops:
            flash("No shops are currently set to Both mode.", "error")
            return _safe_next_redirect("it_support_register_stock_item")
        action = (request.form.get("action") or "").strip().lower()
        if action == "create_stock_item":
            cat = (request.form.get("category") or "").strip()
            nm = (request.form.get("name") or "").strip()
            desc = (request.form.get("description") or "").strip()
            measure = (request.form.get("measure_unit") or "").strip().lower()
            if measure not in measure_options:
                flash("Choose a valid measure unit.", "error")
                return _safe_next_redirect("it_support_register_stock_item")
            if not cat or not nm:
                flash("Category and stock item name are required.", "error")
                return _safe_next_redirect("it_support_register_stock_item")
            init_store_stock_items_table()
            created = 0
            for s in both_shops:
                try:
                    sid = int(s.get("id") or 0)
                except Exception:
                    sid = 0
                if sid <= 0:
                    continue
                new_id = create_store_stock_item(
                    shop_id=sid,
                    category=cat,
                    name=nm,
                    description=desc,
                    measure_unit=measure,
                    created_by_employee_id=session.get("employee_id"),
                )
                if new_id:
                    created += 1
            flash(
                f"Stock item created in {created} shop(s) using Both mode." if created else "Could not create stock item.",
                "success" if created else "error",
            )
        else:
            flash("Invalid action.", "error")
        return _safe_next_redirect("it_support_register_stock_item")

    init_store_stock_items_table()
    store_stock_items = []
    for s in both_shops:
        try:
            sid = int(s.get("id") or 0)
        except Exception:
            sid = 0
        if sid <= 0:
            continue
        sname = (s.get("shop_name") or "").strip() or f"Shop #{sid}"
        for row in (list_store_stock_items(shop_id=sid, limit=5000, active_only=True) or []):
            rr = dict(row)
            rr["shop_name"] = sname
            store_stock_items.append(rr)

    return render_template(
        "it_support_register_stock_item.html",
        both_shops=both_shops,
        measure_options=measure_options,
        store_stock_items=store_stock_items,
    )


@app.route("/it_support/item-management/stock-in", methods=["GET", "POST"])
@login_required
def it_support_stock_in():
    _it_support_only()
    page = _it_support_stock_page("in")
    if isinstance(page, Response):
        return page
    items, item_id, selected_item, txs = page
    if not item_id:
        flash("Select an item from Company stock management first.", "error")
        return redirect(url_for("it_support_stock_management"))
    return render_template(
        "it_support_stock_in.html",
        selected_item_id=item_id,
        selected_item=selected_item,
        transactions=txs,
    )


@app.route("/it_support/item-management/stock-out", methods=["GET", "POST"])
@login_required
def it_support_stock_out():
    _it_support_only()
    page = _it_support_stock_page("out")
    if isinstance(page, Response):
        return page
    items, item_id, selected_item, txs = page
    if not item_id:
        flash("Select an item from Company stock management first.", "error")
        return redirect(url_for("it_support_stock_management"))
    return render_template(
        "it_support_stock_out.html",
        selected_item_id=item_id,
        selected_item=selected_item,
        transactions=txs,
    )


@app.route("/it_support/stock-status")
@login_required
def it_support_company_stock_analytics():
    _it_support_or_super_admin_only()
    analytics_filter = _build_analytics_filter()
    try:
        from database import get_company_stock_analytics

        stock_data = get_company_stock_analytics(analytics_filter=analytics_filter)
    except Exception:
        stock_data = {}
    inv_mode = _global_pos_inventory_mode()
    try:
        from database import get_company_stock_status, list_company_stock_movements

        shops, stock_rows = get_company_stock_status(limit_items=2000, inventory_mode=inv_mode)
        transaction_rows = list_company_stock_movements(analytics_filter=analytics_filter, limit=2000)
    except Exception:
        shops, stock_rows = [], []
        transaction_rows = []
    allowed_ids = {int(r.get("id") or 0) for r in (stock_rows or []) if int(r.get("id") or 0) > 0}
    if inv_mode in ("both", "kitchen"):
        transaction_rows = [r for r in (transaction_rows or []) if int(r.get("item_id") or 0) in allowed_ids]
    return render_template(
        "it_support_stock_analytics.html",
        analytics_filter=analytics_filter,
        stock_data=stock_data,
        stock_shops=shops,
        stock_rows=stock_rows,
        transaction_rows=transaction_rows,
    )


@app.route("/it_support/stock-status/section")
@login_required
def it_support_stock_status():
    _it_support_or_super_admin_only()
    return redirect(url_for("it_support_company_stock_analytics") + "#stock-status")


@app.route("/it_support/stock-status/manual-stock-ins/<int:tx_id>/payment", methods=["POST"])
@login_required
def it_support_manual_stock_in_payment_update(tx_id: int):
    _it_support_or_super_admin_only()
    add_raw = (request.form.get("additional_payment") or "").strip()
    use_fifo = (request.form.get("fifo") or "").strip().lower() in ("1", "true", "yes", "on")
    tx_scope = (request.form.get("tx_scope") or "shop").strip().lower()
    if tx_scope not in ("shop", "company"):
        tx_scope = "shop"
    if add_raw != "":
        try:
            additional_payment = float(add_raw)
        except Exception:
            return jsonify({"ok": False, "error": "Invalid additional payment amount."}), 400
        if additional_payment < 0:
            return jsonify({"ok": False, "error": "Additional payment cannot be negative."}), 400
        try:
            if tx_scope == "company":
                if use_fifo:
                    from database import apply_supplier_payment_fifo_company_from_tx

                    fifo_res = apply_supplier_payment_fifo_company_from_tx(tx_id, additional_payment)
                    if not fifo_res:
                        row = None
                    else:
                        row = {
                            "id": int(tx_id),
                            "payment_status": "updated",
                            "amount_paid": None,
                            "fifo_allocated_count": len(fifo_res.get("allocated") or []),
                            "fifo_unused": float(fifo_res.get("unused") or 0.0),
                        }
                else:
                    from database import update_company_stock_in_payment

                    row = update_company_stock_in_payment(tx_id, additional_payment=additional_payment)
            elif use_fifo:
                from database import apply_supplier_payment_fifo_from_tx

                fifo_res = apply_supplier_payment_fifo_from_tx(tx_id, additional_payment)
                if not fifo_res:
                    row = None
                else:
                    row = {
                        "id": int(tx_id),
                        "payment_status": "updated",
                        "amount_paid": None,
                        "fifo_allocated_count": len(fifo_res.get("allocated") or []),
                        "fifo_unused": float(fifo_res.get("unused") or 0.0),
                    }
            else:
                from database import update_shop_manual_stock_in_payment

                row = update_shop_manual_stock_in_payment(
                    tx_id, additional_payment=additional_payment
                )
        except Exception:
            row = None
    else:
        amount_raw = (request.form.get("amount_paid") or "").strip()
        try:
            amount_paid = float(amount_raw)
        except Exception:
            return jsonify({"ok": False, "error": "Invalid amount paid."}), 400
        if amount_paid < 0:
            return jsonify({"ok": False, "error": "Amount paid cannot be negative."}), 400
        try:
            if tx_scope == "company":
                from database import update_company_stock_in_payment

                row = update_company_stock_in_payment(tx_id, amount_paid=amount_paid)
            else:
                from database import update_shop_manual_stock_in_payment

                row = update_shop_manual_stock_in_payment(tx_id, amount_paid=amount_paid)
        except Exception:
            row = None
    if not row:
        return jsonify({"ok": False, "error": "Transaction not found."}), 404
    return jsonify({"ok": True, "row": row})


@app.route("/it_support/stock-status/print/daily-data")
@login_required
def it_support_stock_status_print_daily_data():
    _it_support_or_super_admin_only()
    shop_id = request.args.get("shop_id", type=int)
    daily_date = (request.args.get("date") or "").strip()[:10]
    if not shop_id or not daily_date:
        return jsonify({"ok": False, "error": "shop_id and date are required."}), 400
    try:
        from database import list_shop_stock_count_sheet_items

        rows = list_shop_stock_count_sheet_items(shop_id)
    except Exception:
        rows = []
    shop_name = ""
    try:
        from database import get_cursor

        with get_cursor() as cur:
            cur.execute("SELECT shop_name FROM shops WHERE id=%s LIMIT 1", (int(shop_id),))
            rr = cur.fetchone() or {}
            shop_name = (rr.get("shop_name") or "").strip()
    except Exception:
        shop_name = ""
    return jsonify(
        {
            "ok": True,
            "rows": rows,
            "shop_id": int(shop_id),
            "shop_name": shop_name or f"Shop #{shop_id}",
            "date": daily_date,
        }
    )


@app.route("/it_support/stock-status/print")
@login_required
def it_support_stock_status_print():
    _it_support_or_super_admin_only()
    analytics_filter = _build_analytics_filter()
    inv_mode = _global_pos_inventory_mode()
    try:
        from database import get_company_stock_status

        shops, stock_rows = get_company_stock_status(limit_items=2000, inventory_mode=inv_mode)
    except Exception:
        shops, stock_rows = [], []
    return render_template(
        "it_support_stock_status_print.html",
        analytics_filter=analytics_filter,
        stock_shops=shops,
        stock_rows=stock_rows,
        today_iso=date.today().strftime("%Y-%m-%d"),
    )


@app.route("/it_support/stock-analytics")
@login_required
def it_support_stock_analytics_legacy():
    _it_support_or_super_admin_only()
    return redirect(url_for("it_support_company_stock_analytics"))


@app.route("/it_support/stock-movement-analysis")
@login_required
def it_support_stock_movement_analysis():
    _it_support_or_super_admin_only()
    analytics_filter = _build_analytics_filter()
    inv_mode = _global_pos_inventory_mode()
    selected_shop_id = request.args.get("shop_id", type=int)
    selected_employee_id = request.args.get("employee_id", type=int)
    try:
        from database import (
            get_company_stock_status,
            get_company_stock_movement_analytics,
            list_company_stock_movements,
            list_employees,
            list_shops,
        )

        shops = list_shops(limit=500)
        employees = list_employees(limit=2000)
        movement = get_company_stock_movement_analytics(
            analytics_filter=analytics_filter,
            shop_id=selected_shop_id,
        )
        movement_rows = list_company_stock_movements(
            analytics_filter=analytics_filter,
            shop_id=selected_shop_id,
            employee_id=selected_employee_id,
            limit=1500,
        )
        _, allowed_rows = get_company_stock_status(limit_items=5000, inventory_mode=inv_mode)
        allowed_ids = {int(r.get("id") or 0) for r in (allowed_rows or []) if int(r.get("id") or 0) > 0}
        if inv_mode in ("both", "kitchen"):
            movement_rows = [r for r in (movement_rows or []) if int(r.get("item_id") or 0) in allowed_ids]
            tx_count = len(movement_rows)
            qty_in = sum(int(r.get("qty") or 0) for r in movement_rows if str(r.get("direction") or "").lower() == "in")
            qty_out = sum(int(r.get("qty") or 0) for r in movement_rows if str(r.get("direction") or "").lower() == "out")
            distinct_shops = len(
                {
                    int(r.get("shop_id") or 0)
                    for r in movement_rows
                    if int(r.get("shop_id") or 0) > 0
                }
            )
            movement = {
                **(movement or {}),
                "tx_count": tx_count,
                "qty_in": qty_in,
                "qty_out": qty_out,
                "net_qty": qty_in - qty_out,
                "distinct_shops": distinct_shops,
            }
    except Exception:
        shops = []
        employees = []
        movement = {}
        movement_rows = []
    return render_template(
        "it_support_stock_movement_analysis.html",
        analytics_filter=analytics_filter,
        movement_data=movement,
        movement_rows=movement_rows,
        shops=shops,
        employees=employees or [],
        selected_shop_id=selected_shop_id,
        selected_employee_id=selected_employee_id,
    )


_PROFIT_SORT_LABELS = {
    "name": "Item name",
    "margin_pct": "Margin %",
    "margin_amount": "Margin / unit",
    "qty_sold": "Qty sold",
    "revenue": "Revenue",
    "selling_price": "Selling price",
    "avg_buying_price": "Avg buying price",
    "stock_qty": "Stock qty",
    "stock_value": "Stock value",
    "velocity": "Turnover rate",
    "company_stock_qty": "Company stock",
}

_PROFIT_SORT_COLUMNS_BY_VIEW = {
    "margin": (
        "margin_pct",
        "qty_sold",
        "revenue",
        "margin_amount",
        "selling_price",
        "avg_buying_price",
        "stock_qty",
        "name",
    ),
    "stock_value": (
        "stock_value",
        "stock_qty",
        "qty_sold",
        "revenue",
        "avg_buying_price",
        "name",
    ),
    "velocity": ("velocity", "qty_sold", "stock_qty", "revenue", "name"),
    "leakage": ("revenue", "qty_sold", "stock_qty", "company_stock_qty", "name"),
    "low_margin_high_volume": (
        "margin_pct",
        "qty_sold",
        "revenue",
        "margin_amount",
        "name",
    ),
    "low_stock": ("stock_qty", "qty_sold", "velocity", "revenue", "name"),
}

# Primary column + ascending flag for each tab (matches the purpose of the view).
_PROFIT_DEFAULT_SORT = {
    "margin": ("margin_pct", False),  # highest margin % first
    "stock_value": ("stock_value", False),  # highest stock value first
    "velocity": ("velocity", False),  # highest turnover first
    "leakage": ("revenue", False),  # largest revenue-at-risk first
    "low_margin_high_volume": ("margin_pct", True),  # worst margin % first
    "low_stock": ("stock_qty", True),  # lowest on-hand first
}


def _sort_profitability_rows(rows, col, ascending):
    """Stable sort for profitability sub-tables (numeric + name). Missing numbers sort last."""
    if col == "name":
        return sorted(
            rows,
            key=lambda r: (r.get("name") or "").strip().lower(),
            reverse=not ascending,
        )

    def num_tuple(r):
        v = r.get(col)
        if v is None or v == "":
            return (1, 0.0)
        try:
            return (0, float(v))
        except (TypeError, ValueError):
            return (1, 0.0)

    if ascending:
        return sorted(rows, key=num_tuple)

    def desc_tuple(r):
        g, x = num_tuple(r)
        if g == 1:
            return (1, 0.0)
        return (0, -x)

    return sorted(rows, key=desc_tuple)


_REPORT_SORT_LABELS = {
    "name": "Item name",
    "stock_qty": "Stock qty",
    "tx_count": "Moves",
    "stock_value": "Stock value",
    "avg_buying_price": "Avg buy",
    "updated_at": "Updated",
    "low_stock_threshold": "Alert level",
}

_REPORT_SORT_COLUMNS_BY_VIEW = {
    "low_stock": ("stock_qty", "low_stock_threshold", "tx_count", "stock_value", "name", "avg_buying_price"),
    "fast_moving": ("tx_count", "stock_qty", "stock_value", "updated_at", "name"),
    "valuation": ("stock_value", "stock_qty", "tx_count", "avg_buying_price", "name"),
    "highest_value": ("stock_value", "stock_qty", "tx_count", "name", "avg_buying_price"),
    "stagnant": ("stock_value", "stock_qty", "tx_count", "updated_at", "name"),
}

_REPORT_DEFAULT_SORT = {
    "low_stock": ("stock_qty", True),  # lowest on-hand first
    "fast_moving": ("tx_count", False),  # most moves first
    "valuation": ("stock_value", False),  # highest value first
    "highest_value": ("stock_value", False),
    "stagnant": ("stock_value", False),  # largest idle value first
}


def _sort_stock_report_rows(rows, col, ascending):
    """Sort IT stock report rows (name / updated_at / numeric columns)."""
    if col == "name":
        return sorted(
            rows,
            key=lambda r: (r.get("name") or "").strip().lower(),
            reverse=not ascending,
        )
    if col == "updated_at":
        return sorted(
            rows,
            key=lambda r: str(r.get("updated_at") or ""),
            reverse=not ascending,
        )
    return _sort_profitability_rows(rows, col, ascending)


@app.route("/it_support/stock-profitability-analysis")
@login_required
def it_support_stock_profitability_analysis():
    _it_support_or_super_admin_only()
    analytics_filter = _build_analytics_filter()
    inv_mode = _global_pos_inventory_mode()
    selected_view = (request.args.get("view") or "margin").strip().lower()
    allowed_views = {
        "margin",
        "stock_value",
        "velocity",
        "leakage",
        "low_margin_high_volume",
        "low_stock",
    }
    if selected_view not in allowed_views:
        selected_view = "margin"
    profit_sort_allowed = set(_PROFIT_SORT_COLUMNS_BY_VIEW.get(selected_view, ()))
    dacol, daasc = _PROFIT_DEFAULT_SORT.get(selected_view, ("name", True))
    raw_sort = (request.args.get("profit_sort") or "").strip().lower()
    if raw_sort in profit_sort_allowed:
        eff_col = raw_sort
        por = (request.args.get("profit_order") or "desc").strip().lower()
        eff_asc = por == "asc"
    else:
        eff_col = dacol
        eff_asc = daasc
    profit_order = "asc" if eff_asc else "desc"
    profit_sort_options = [
        (c, _PROFIT_SORT_LABELS.get(c, c.replace("_", " ").title()))
        for c in _PROFIT_SORT_COLUMNS_BY_VIEW.get(selected_view, ())
    ]
    item_rows = []
    avg_margin_pct = 0.0
    total_stock_value = 0.0
    dead_stock_value = 0.0
    dead_stock_count = 0
    high_value_zero_stock = []
    low_margin_high_volume = []
    low_stock_items = []
    top_velocity_items = []
    margin_rows = []

    from database import (
        IT_SUPPORT_ANALYTICS_ITEMS_MAX,
        get_company_stock_status,
        get_it_support_item_analytics,
        get_cursor,
        list_active_catalog_items_for_it_analytics,
    )

    try:
        items = list_active_catalog_items_for_it_analytics(
            limit=IT_SUPPORT_ANALYTICS_ITEMS_MAX,
            inventory_mode=inv_mode,
        ) or []
    except Exception:
        items = []
    try:
        _, stock_rows = get_company_stock_status(
            limit_items=IT_SUPPORT_ANALYTICS_ITEMS_MAX,
            inventory_mode=inv_mode,
            only_active=True,
        )
    except Exception:
        stock_rows = []
    try:
        item_sales = get_it_support_item_analytics(
            analytics_filter=analytics_filter,
            analytics_scope=_analytics_scope_from_request(),
            top_items_limit=0,
        ) or {}
    except Exception:
        item_sales = {}

    stock_map = {}
    company_stock_map = {}
    for r in stock_rows or []:
        try:
            iid = int(r.get("id") or 0)
        except Exception:
            continue
        if iid > 0:
            stock_map[iid] = int(r.get("total_stock_qty") or 0)
            company_stock_map[iid] = int(r.get("company_stock_qty") or 0)

    sales_map = {}
    for r in (item_sales or {}).get("top_items") or []:
        try:
            iid = int(r.get("item_id") or 0)
        except Exception:
            continue
        if iid <= 0:
            continue
        sales_map[iid] = {
            "qty_sold": int(r.get("qty_sold") or 0),
            "revenue": float(r.get("revenue") or 0),
        }

    avg_buy_map = {}
    try:
        with get_cursor() as cur:
            cur.execute(
                """
                SELECT
                    item_id,
                    SUM(COALESCE(buying_price,0) * qty) AS buy_value,
                    SUM(qty) AS buy_qty
                FROM stock_transactions
                WHERE direction='in' AND buying_price IS NOT NULL
                GROUP BY item_id
                """
            )
            for rr in (cur.fetchall() or []):
                try:
                    iid = int(rr.get("item_id") or 0)
                    bq = int(rr.get("buy_qty") or 0)
                    bv = float(rr.get("buy_value") or 0)
                except Exception:
                    continue
                if iid > 0 and bq > 0:
                    avg_buy_map[iid] = bv / bq
    except Exception:
        avg_buy_map = {}

    margin_sum = 0.0
    margin_n = 0
    for it in items:
        try:
            iid = int(it.get("id") or 0)
        except Exception:
            continue
        if iid <= 0:
            continue

        try:
            selling_price = float(
                it.get("selling_price")
                if it.get("selling_price") is not None
                else (it.get("price") or 0)
            )
        except Exception:
            selling_price = 0.0
        avg_buying_price = avg_buy_map.get(iid)
        if avg_buying_price is None:
            lp = it.get("last_buying_price")
            if lp is not None:
                try:
                    avg_buying_price = float(lp)
                except Exception:
                    avg_buying_price = None
        qty_sold = int((sales_map.get(iid) or {}).get("qty_sold") or 0)
        revenue = float((sales_map.get(iid) or {}).get("revenue") or 0)
        company_qty = int(company_stock_map.get(iid, int(it.get("stock_qty") or 0)))
        stock_qty = int(stock_map.get(iid, company_qty))
        row_low_stock_threshold = max(0, int(it.get("low_stock_threshold") or 0))
        row_reorder_level = max(0, int(it.get("reorder_level") or 0))

        margin_amount = None
        margin_pct = None
        if avg_buying_price is not None and selling_price > 0:
            margin_amount = selling_price - avg_buying_price
            margin_pct = (margin_amount / selling_price) * 100.0
            margin_sum += margin_pct
            margin_n += 1

        stock_value = max(stock_qty, 0) * (avg_buying_price or 0.0)
        velocity = qty_sold / max(stock_qty, 1) if qty_sold > 0 else 0.0

        row = {
            "item_id": iid,
            "category": (it.get("category") or "").strip(),
            "name": (it.get("name") or "").strip() or f"Item #{iid}",
            "selling_price": selling_price,
            "avg_buying_price": avg_buying_price,
            "margin_amount": margin_amount,
            "margin_pct": margin_pct,
            "stock_qty": stock_qty,
            "company_stock_qty": company_qty,
            "stock_value": stock_value,
            "qty_sold": qty_sold,
            "revenue": revenue,
            "velocity": velocity,
            "low_stock_threshold": row_low_stock_threshold,
            "reorder_level": row_reorder_level,
        }
        item_rows.append(row)
        total_stock_value += stock_value

        if stock_qty > 0 and qty_sold == 0:
            dead_stock_count += 1
            dead_stock_value += stock_value
        if revenue > 0 and (stock_qty <= 0 or company_qty <= 0):
            high_value_zero_stock.append(row)
        if row_low_stock_threshold > 0 and stock_qty <= row_low_stock_threshold:
            low_stock_items.append(row)
        if margin_pct is not None and margin_pct <= 18.0 and qty_sold >= 5:
            low_margin_high_volume.append(row)
        if margin_pct is not None:
            margin_rows.append(row)

    avg_margin_pct = (margin_sum / margin_n) if margin_n else 0.0
    # Intrinsic ordering for off-tab lists; current tab uses eff_col / eff_asc (defaults match tab purpose).
    margin_rows = _sort_profitability_rows(margin_rows, "margin_pct", False)
    stock_value_rows = _sort_profitability_rows(list(item_rows), "stock_value", False)
    high_value_zero_stock = _sort_profitability_rows(high_value_zero_stock, "revenue", False)
    low_margin_high_volume = _sort_profitability_rows(low_margin_high_volume, "margin_pct", True)
    low_stock_items = _sort_profitability_rows(low_stock_items, "stock_qty", True)
    top_velocity_items = _sort_profitability_rows(list(item_rows), "velocity", False)

    if selected_view == "margin":
        margin_rows = _sort_profitability_rows(margin_rows, eff_col, eff_asc)
    elif selected_view == "stock_value":
        stock_value_rows = _sort_profitability_rows(list(item_rows), eff_col, eff_asc)
    elif selected_view == "velocity":
        top_velocity_items = _sort_profitability_rows(list(item_rows), eff_col, eff_asc)
    elif selected_view == "leakage":
        high_value_zero_stock = _sort_profitability_rows(high_value_zero_stock, eff_col, eff_asc)
    elif selected_view == "low_margin_high_volume":
        low_margin_high_volume = _sort_profitability_rows(low_margin_high_volume, eff_col, eff_asc)
    elif selected_view == "low_stock":
        low_stock_items = _sort_profitability_rows(low_stock_items, eff_col, eff_asc)

    return render_template(
        "it_support_stock_profitability_analysis.html",
        analytics_filter=analytics_filter,
        analytics_scope=_analytics_scope_from_request(),
        selected_view=selected_view,
        profit_sort=eff_col,
        profit_order=profit_order,
        profit_sort_options=profit_sort_options,
        item_rows=item_rows,
        avg_margin_pct=avg_margin_pct,
        total_stock_value=total_stock_value,
        dead_stock_value=dead_stock_value,
        dead_stock_count=dead_stock_count,
        margin_rows=(margin_rows[:100]),
        stock_value_rows=(stock_value_rows[:100]),
        high_value_zero_stock=(high_value_zero_stock[:100]),
        low_margin_high_volume=(low_margin_high_volume[:100]),
        low_stock_items=(low_stock_items[:150]),
        top_velocity_items=(top_velocity_items[:100]),
    )


@app.route("/it_support/stock-reports")
@login_required
def it_support_stock_reports():
    _it_support_or_super_admin_only()
    analytics_filter = _build_analytics_filter()
    inv_mode = _global_pos_inventory_mode()
    selected_view = (request.args.get("view") or "low_stock").strip().lower()
    allowed_views = {
        "low_stock",
        "fast_moving",
        "valuation",
        "highest_value",
        "stagnant",
    }
    if selected_view not in allowed_views:
        selected_view = "low_stock"

    report_sort_allowed = set(_REPORT_SORT_COLUMNS_BY_VIEW.get(selected_view, ()))
    rdacol, rdaasc = _REPORT_DEFAULT_SORT.get(selected_view, ("name", True))
    raw_rs = (request.args.get("report_sort") or "").strip().lower()
    if raw_rs in report_sort_allowed:
        rep_col = raw_rs
        rep_asc = (request.args.get("report_order") or "desc").strip().lower() == "asc"
    else:
        rep_col = rdacol
        rep_asc = rdaasc
    report_order = "asc" if rep_asc else "desc"
    report_sort_options = [
        (c, _REPORT_SORT_LABELS.get(c, c.replace("_", " ").title()))
        for c in _REPORT_SORT_COLUMNS_BY_VIEW.get(selected_view, ())
    ]

    low_stock_rows = []
    fast_moving_rows = []
    valuation_rows = []
    highest_value_rows = []
    stagnant_rows = []
    total_valuation = 0.0

    try:
        from database import (
            IT_SUPPORT_ANALYTICS_ITEMS_MAX,
            _analytics_where_clause,
            get_company_stock_status,
            get_cursor,
            list_active_catalog_items_for_it_analytics,
        )

        items = list_active_catalog_items_for_it_analytics(
            limit=IT_SUPPORT_ANALYTICS_ITEMS_MAX,
            inventory_mode=inv_mode,
        ) or []

        _, stock_rows = get_company_stock_status(
            limit_items=IT_SUPPORT_ANALYTICS_ITEMS_MAX,
            inventory_mode=inv_mode,
            only_active=True,
        )
        stock_map = {}
        for r in stock_rows or []:
            try:
                iid = int(r.get("id") or 0)
            except Exception:
                continue
            if iid > 0:
                stock_map[iid] = int(r.get("total_stock_qty") or 0)

        mv_where_st, mv_params_st = _analytics_where_clause(analytics_filter, "st")
        mv_where_sst, mv_params_sst = _analytics_where_clause(analytics_filter, "sst")
        tx_count_map = {}
        with get_cursor() as cur:
            cur.execute(
                f"""
                SELECT item_id, SUM(tx_count) AS tx_count
                FROM (
                  SELECT st.item_id AS item_id, COUNT(*) AS tx_count
                  FROM stock_transactions st
                  WHERE {mv_where_st}
                  GROUP BY st.item_id
                  UNION ALL
                  SELECT sst.item_id AS item_id, COUNT(*) AS tx_count
                  FROM shop_stock_transactions sst
                  WHERE {mv_where_sst}
                  GROUP BY sst.item_id
                ) u
                GROUP BY item_id
                """,
                tuple(list(mv_params_st) + list(mv_params_sst)),
            )
            for rr in (cur.fetchall() or []):
                try:
                    tx_count_map[int(rr.get("item_id") or 0)] = int(rr.get("tx_count") or 0)
                except Exception:
                    continue

        avg_buy_map = {}
        with get_cursor() as cur:
            cur.execute(
                """
                SELECT
                    item_id,
                    SUM(COALESCE(buying_price,0) * qty) AS buy_value,
                    SUM(qty) AS buy_qty
                FROM stock_transactions
                WHERE direction='in' AND buying_price IS NOT NULL
                GROUP BY item_id
                """
            )
            for rr in (cur.fetchall() or []):
                try:
                    iid = int(rr.get("item_id") or 0)
                    bq = int(rr.get("buy_qty") or 0)
                    bv = float(rr.get("buy_value") or 0)
                except Exception:
                    continue
                if iid > 0 and bq > 0:
                    avg_buy_map[iid] = bv / bq

        for it in items:
            try:
                iid = int(it.get("id") or 0)
            except Exception:
                continue
            if iid <= 0:
                continue
            stock_qty = int(stock_map.get(iid, int(it.get("stock_qty") or 0)))
            tx_count = int(tx_count_map.get(iid, 0))
            avg_buy = float(avg_buy_map.get(iid) or 0.0)
            stock_value = max(stock_qty, 0) * avg_buy
            total_valuation += stock_value

            row_low = max(0, int(it.get("low_stock_threshold") or 0))
            row_reorder = max(0, int(it.get("reorder_level") or 0))
            row = {
                "item_id": iid,
                "name": (it.get("name") or "").strip() or f"Item #{iid}",
                "category": (it.get("category") or "").strip(),
                "stock_qty": stock_qty,
                "tx_count": tx_count,
                "updated_at": it.get("updated_at"),
                "avg_buying_price": avg_buy,
                "stock_value": stock_value,
                "low_stock_threshold": row_low,
                "reorder_level": row_reorder,
            }
            valuation_rows.append(row)
            if row_low > 0 and stock_qty <= row_low:
                low_stock_rows.append(row)
            if stock_qty > 0 and tx_count == 0:
                stagnant_rows.append(row)

        val_snap = list(valuation_rows)
        fast_moving_rows = [r for r in val_snap if (r.get("tx_count") or 0) > 0]

        low_stock_rows = _sort_stock_report_rows(low_stock_rows, "stock_qty", True)
        fast_moving_rows = _sort_stock_report_rows(fast_moving_rows, "tx_count", False)
        valuation_rows = _sort_stock_report_rows(val_snap, "stock_value", False)
        highest_value_rows = _sort_stock_report_rows(val_snap, "stock_value", False)
        stagnant_rows = _sort_stock_report_rows(stagnant_rows, "stock_value", False)

        if selected_view == "low_stock":
            low_stock_rows = _sort_stock_report_rows(low_stock_rows, rep_col, rep_asc)
        elif selected_view == "fast_moving":
            fast_moving_rows = _sort_stock_report_rows(fast_moving_rows, rep_col, rep_asc)
        elif selected_view == "valuation":
            valuation_rows = _sort_stock_report_rows(valuation_rows, rep_col, rep_asc)
        elif selected_view == "highest_value":
            highest_value_rows = _sort_stock_report_rows(highest_value_rows, rep_col, rep_asc)
        elif selected_view == "stagnant":
            stagnant_rows = _sort_stock_report_rows(stagnant_rows, rep_col, rep_asc)
    except Exception:
        pass

    return render_template(
        "it_support_stock_reports.html",
        analytics_filter=analytics_filter,
        selected_view=selected_view,
        report_sort=rep_col,
        report_order=report_order,
        report_sort_options=report_sort_options,
        total_valuation=total_valuation,
        low_stock_rows=low_stock_rows[:120],
        fast_moving_rows=fast_moving_rows[:120],
        valuation_rows=valuation_rows[:300],
        highest_value_rows=highest_value_rows[:120],
        stagnant_rows=stagnant_rows[:120],
    )


@app.route("/it_support/stock-requests-audit")
@login_required
def it_support_stock_requests_audit():
    _it_support_or_super_admin_only()
    from database import STOCK_REQUEST_PENDING_EXPIRY_DAYS

    try:
        from database import expire_old_pending_stock_requests, list_stock_requests_audit_rows, list_shops

        expired_n = expire_old_pending_stock_requests()
        status = (request.args.get("status") or "").strip().lower() or None
        if status not in (None, "pending", "approved", "rejected", "expired"):
            status = None
        shop_id = request.args.get("shop_id", type=int)
        rows = list_stock_requests_audit_rows(
            status=status,
            shop_id=shop_id if shop_id and shop_id > 0 else None,
            limit=500,
            offset=0,
        )
        shops = list_shops(limit=500) or []
    except Exception:
        expired_n = 0
        rows = []
        shops = []
        status = None
        shop_id = None
    return render_template(
        "it_support_stock_requests_audit.html",
        stock_request_rows=rows,
        filter_status=status or "",
        filter_shop_id=shop_id,
        shops=shops,
        expired_n=expired_n,
        expiry_days=STOCK_REQUEST_PENDING_EXPIRY_DAYS,
    )


@app.route("/it_support/stock-requests-audit/export-requests")
@login_required
def it_support_stock_requests_audit_export_requests():
    _it_support_or_super_admin_only()
    try:
        from database import list_stock_requests_audit_rows

        status = (request.args.get("status") or "").strip().lower() or None
        if status not in (None, "pending", "approved", "rejected", "expired"):
            status = None
        shop_id = request.args.get("shop_id", type=int)
        rows = list_stock_requests_audit_rows(
            status=status,
            shop_id=shop_id if shop_id and shop_id > 0 else None,
            limit=5000,
            offset=0,
        )
    except Exception:
        rows = []
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(
        [
            "id",
            "request_type",
            "status",
            "requesting_shop_id",
            "requesting_shop_name",
            "source_type",
            "source_shop_id",
            "source_shop_name",
            "item_id",
            "item_name",
            "qty",
            "note",
            "requested_by_employee_id",
            "reviewed_by_employee_id",
            "review_note",
            "created_at",
            "reviewed_at",
            "event_count",
        ]
    )
    for r in rows or []:
        w.writerow(
            [
                r.get("id"),
                r.get("request_type"),
                r.get("status"),
                r.get("requesting_shop_id"),
                r.get("requesting_shop_name"),
                r.get("source_type"),
                r.get("source_shop_id"),
                r.get("source_shop_name"),
                r.get("item_id"),
                r.get("item_name"),
                r.get("qty"),
                r.get("note"),
                r.get("requested_by_employee_id"),
                r.get("reviewed_by_employee_id"),
                r.get("review_note"),
                r.get("created_at"),
                r.get("reviewed_at"),
                r.get("event_count"),
            ]
        )
    out = buf.getvalue()
    return Response(
        out,
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=stock_requests_audit.csv"},
    )


@app.route("/it_support/stock-requests-audit/export-events")
@login_required
def it_support_stock_requests_audit_export_events():
    _it_support_or_super_admin_only()
    try:
        from database import list_stock_request_events_export

        rid = request.args.get("request_id", type=int)
        rows = list_stock_request_events_export(request_id=rid if rid and rid > 0 else None, limit=10000)
    except Exception:
        rows = []
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["id", "request_id", "event_type", "actor_employee_id", "actor_shop_id", "payload_json", "created_at"])
    for e in rows or []:
        pj = e.get("payload_json")
        if pj is not None and not isinstance(pj, str):
            try:
                pj = json.dumps(pj, ensure_ascii=False, default=str)
            except Exception:
                pj = str(pj)
        w.writerow(
            [
                e.get("id"),
                e.get("request_id"),
                e.get("event_type"),
                e.get("actor_employee_id"),
                e.get("actor_shop_id"),
                pj,
                e.get("created_at"),
            ]
        )
    out = buf.getvalue()
    return Response(
        out,
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=stock_request_events.csv"},
    )


def _serialize_stock_in_row(tx: dict) -> dict:
    from decimal import Decimal

    def _f(x) -> float:
        if x is None:
            return 0.0
        if isinstance(x, Decimal):
            return float(x)
        try:
            return float(x)
        except Exception:
            return 0.0

    q = int(tx.get("qty") or 0)
    bp = _f(tx.get("buying_price"))
    created = tx.get("created_at")
    if hasattr(created, "isoformat"):
        created = created.isoformat(sep=" ", timespec="seconds")
    return {
        "id": tx.get("id"),
        "created_at": str(created or ""),
        "qty": q,
        "buying_price": round(bp, 4),
        "line_value": round(bp * q, 2),
        "supplier": (tx.get("place_brought_from") or "").strip() or "—",
        "note": (tx.get("note") or "").strip() or "—",
        "stock_before": int(tx.get("stock_before") or 0),
        "stock_after": int(tx.get("stock_after") or 0),
        "stocked_by": (tx.get("stocked_by") or "").strip() or "—",
    }


def _compute_shop_reorder_suggestion(stock_qty: int, movement_row: dict) -> int:
    """Compute balanced reorder suggestion from movement + current stock (advisory only)."""
    stock_qty = max(0, int(stock_qty or 0))
    mv = movement_row or {}
    out_qty = max(0, int(mv.get("out_qty") or 0))
    in_qty = max(0, int(mv.get("in_qty") or 0))
    lookback_days = max(7, int(mv.get("lookback_days") or 30))
    if out_qty <= 0:
        return 0
    daily_out = out_qty / float(lookback_days)
    baseline = daily_out * 7.0
    safety = max(1.0, daily_out * 2.0)
    two_week_cover = daily_out * 14.0
    stock_gap = max(0.0, two_week_cover - float(stock_qty))
    suggested = baseline + safety + (stock_gap * 0.35)
    if in_qty > 0:
        suggested -= min(suggested * 0.4, in_qty / 4.0)
    max_cap = max(5.0, daily_out * 30.0)
    suggested = max(0.0, min(suggested, max_cap))
    return int(round(suggested))


def _filter_supplier_stock_ins_rows_by_payment(rows: list, payment_filter: str) -> list:
    """Filter company/shop supplier stock-in rows by payment settlement state (current row snapshot)."""
    filt = (payment_filter or "pending").strip().lower()
    if filt not in ("pending", "partial", "paid", "all"):
        filt = "pending"
    out: list = []
    for r in rows:
        try:
            tc = float(r.get("total_cost") or 0)
            paid = float(r.get("amount_paid") or 0)
            bal = float(r.get("balance") or max(0.0, tc - paid))
        except (TypeError, ValueError):
            continue
        if tc <= 1e-6:
            continue
        if filt == "all":
            out.append(r)
        elif filt == "pending":
            if paid <= 1e-6 and bal > 1e-6:
                out.append(r)
        elif filt == "partial":
            if paid > 1e-6 and bal > 1e-6:
                out.append(r)
        elif filt == "paid":
            if bal <= 1e-6:
                out.append(r)
    return out


@app.route("/it_support/stock-reports/suppliers")
@login_required
def it_support_stock_suppliers():
    _it_support_or_super_admin_only()
    has_date_filter = any(
        (request.args.get(k) or "").strip()
        for k in ("mode", "single_day", "start_date", "end_date", "month", "year")
    )
    analytics_filter = _build_analytics_filter() if has_date_filter else {"range_label": "All dates"}
    supplier_q = (request.args.get("supplier_q") or "").strip()
    moved_by_q = (request.args.get("moved_by") or "").strip()
    filter_shop_id = request.args.get("shop_id", type=int)
    if filter_shop_id is not None and filter_shop_id <= 0:
        filter_shop_id = None
    try:
        from database import list_company_supplier_stock_ins, list_registered_sellers_for_report, list_shops

        rows = list_company_supplier_stock_ins(
            analytics_filter=analytics_filter,
            shop_id=filter_shop_id,
            supplier_search=supplier_q or None,
            moved_by_contains=moved_by_q or None,
            limit=5000,
        ) or []
        stock_shops = list_shops(limit=500) or []
    except Exception:
        rows, stock_shops = [], []

    by_supplier = {}
    for r in rows:
        nm = (r.get("seller_name") or "-").strip() or "-"
        ph = (r.get("seller_phone") or "-").strip() or "-"
        key = f"{nm}||{ph}"
        g = by_supplier.get(key)
        if g is None:
            g = {
                "seller_name": nm,
                "seller_phone": ph,
                "tx_count": 0,
                "total_qty": 0,
                "total_cost": 0.0,
                "amount_paid": 0.0,
                "balance": 0.0,
                "last_activity": r.get("created_at"),
            }
            by_supplier[key] = g
        g["tx_count"] += 1
        g["total_qty"] += int(r.get("qty") or 0)
        g["total_cost"] += float(r.get("total_cost") or 0)
        g["amount_paid"] += float(r.get("amount_paid") or 0)
        g["balance"] = max(g["total_cost"] - g["amount_paid"], 0.0)
        if (r.get("created_at") or "") > (g.get("last_activity") or ""):
            g["last_activity"] = r.get("created_at")

    # Registered sellers with no matching stock-ins still appear (unless shop / moved-by filters apply).
    if filter_shop_id is None and not moved_by_q:
        try:
            from database import list_registered_sellers_for_report

            for s in list_registered_sellers_for_report(
                name_or_phone_contains=supplier_q or None,
                limit=8000,
            ):
                nm = (s.get("seller_name") or "-").strip() or "-"
                ph = (s.get("seller_phone") or "-").strip() or "-"
                key = f"{nm}||{ph}"
                if key in by_supplier:
                    continue
                by_supplier[key] = {
                    "seller_name": nm,
                    "seller_phone": ph,
                    "tx_count": 0,
                    "total_qty": 0,
                    "total_cost": 0.0,
                    "amount_paid": 0.0,
                    "balance": 0.0,
                    "last_activity": None,
                }
        except Exception:
            pass

    supplier_groups = sorted(
        by_supplier.values(),
        key=lambda x: (
            -float(x.get("balance") or 0.0),
            -float(x.get("total_cost") or 0.0),
            str(x.get("seller_name") or ""),
        ),
    )
    return render_template(
        "it_support_stock_suppliers_grouped.html",
        analytics_filter=analytics_filter,
        has_date_filter=has_date_filter,
        transaction_rows=rows,
        supplier_groups=supplier_groups,
        supplier_q=supplier_q,
        moved_by_q=moved_by_q,
        filter_shop_id=filter_shop_id,
        stock_shops=stock_shops,
    )


@app.route("/it_support/stock-reports/supplier-payment-transactions")
@login_required
def it_support_supplier_payment_transactions():
    """Per-line supplier stock-in payment state: pending, partially paid, or settled."""
    _it_support_or_super_admin_only()
    payment_filter = (request.args.get("filter") or "pending").strip().lower()
    if payment_filter not in ("pending", "partial", "paid", "all"):
        payment_filter = "pending"
    has_date_filter = any(
        (request.args.get(k) or "").strip()
        for k in ("mode", "single_day", "start_date", "end_date", "month", "year")
    )
    analytics_filter = _build_analytics_filter() if has_date_filter else {"range_label": "All dates"}
    supplier_q = (request.args.get("supplier_q") or "").strip()
    moved_by_q = (request.args.get("moved_by") or "").strip()
    filter_shop_id = request.args.get("shop_id", type=int)
    if filter_shop_id is not None and filter_shop_id <= 0:
        filter_shop_id = None
    rows: list = []
    stock_shops: list = []
    try:
        from database import list_company_supplier_stock_ins, list_shops

        raw = (
            list_company_supplier_stock_ins(
                analytics_filter=analytics_filter,
                shop_id=filter_shop_id,
                supplier_search=supplier_q or None,
                moved_by_contains=moved_by_q or None,
                limit=10000,
            )
            or []
        )
        raw = [r for r in raw if float(r.get("total_cost") or 0) > 1e-6]
        rows = _filter_supplier_stock_ins_rows_by_payment(raw, payment_filter)
        stock_shops = list_shops(limit=500) or []
    except Exception:
        rows, stock_shops = [], []

    sum_total = sum(float(r.get("total_cost") or 0) for r in rows)
    sum_paid = sum(float(r.get("amount_paid") or 0) for r in rows)
    sum_balance = sum(float(r.get("balance") or 0) for r in rows)
    filter_titles = {
        "pending": "Pending payment (nothing paid yet)",
        "partial": "Partially paid (payment started, balance remaining)",
        "paid": "Paid in full (no balance)",
        "all": "All lines with supplier cost",
    }
    return render_template(
        "it_support_supplier_payment_transactions.html",
        analytics_filter=analytics_filter,
        has_date_filter=has_date_filter,
        payment_filter=payment_filter,
        payment_filter_title=filter_titles.get(payment_filter, filter_titles["pending"]),
        payment_rows=rows,
        supplier_q=supplier_q,
        moved_by_q=moved_by_q,
        filter_shop_id=filter_shop_id,
        stock_shops=stock_shops,
        sum_total=sum_total,
        sum_paid=sum_paid,
        sum_balance=sum_balance,
    )


@app.route("/it_support/stock-reports/suppliers/seller")
@login_required
def it_support_stock_supplier_detail():
    _it_support_or_super_admin_only()
    has_date_filter = any(
        (request.args.get(k) or "").strip()
        for k in ("mode", "single_day", "start_date", "end_date", "month", "year")
    )
    analytics_filter = _build_analytics_filter() if has_date_filter else {"range_label": "All dates"}
    seller_name = (request.args.get("seller_name") or "").strip() or "-"
    seller_phone = (request.args.get("seller_phone") or "").strip() or "-"
    filter_shop_id = request.args.get("shop_id", type=int)
    if filter_shop_id is not None and filter_shop_id <= 0:
        filter_shop_id = None
    should_print = (request.args.get("print") or "").strip().lower() in ("1", "true", "yes", "on")
    try:
        from database import list_company_supplier_stock_ins

        rows = list_company_supplier_stock_ins(
            analytics_filter=analytics_filter,
            shop_id=filter_shop_id,
            supplier_search=None,
            moved_by_contains=None,
            limit=10000,
        ) or []
    except Exception:
        rows = []
    rows = [
        r
        for r in rows
        if ((r.get("seller_name") or "-").strip() or "-") == seller_name
        and ((r.get("seller_phone") or "-").strip() or "-") == seller_phone
    ]
    payment_rows = [r for r in rows if float(r.get("amount_paid") or 0) > 0]
    total_cost = sum(float(r.get("total_cost") or 0) for r in rows)
    total_paid = sum(float(r.get("amount_paid") or 0) for r in rows)
    total_balance = max(total_cost - total_paid, 0.0)
    return render_template(
        "it_support_stock_supplier_detail.html",
        seller_name=seller_name,
        seller_phone=seller_phone,
        analytics_filter=analytics_filter,
        has_date_filter=has_date_filter,
        supplier_rows=rows,
        supplier_payment_rows=payment_rows,
        total_cost=total_cost,
        total_paid=total_paid,
        total_balance=total_balance,
        filter_shop_id=filter_shop_id,
        supplier_q=(request.args.get("supplier_q") or "").strip(),
        moved_by_q=(request.args.get("moved_by") or "").strip(),
        supplier_should_print=should_print,
    )


@app.route("/it_support/stock-reports/suppliers/transaction")
@login_required
def it_support_stock_supplier_transaction_detail():
    _it_support_or_super_admin_only()
    tx_id = request.args.get("tx_id", type=int)
    if not tx_id:
        abort(404)
    seller_name = (request.args.get("seller_name") or "").strip() or "-"
    seller_phone = (request.args.get("seller_phone") or "").strip() or "-"
    tx_scope = (request.args.get("tx_scope") or "shop").strip().lower()
    if tx_scope not in ("shop", "company"):
        tx_scope = "shop"
    row = None
    try:
        if tx_scope == "company":
            from database import get_company_stock_in_transaction

            row = get_company_stock_in_transaction(tx_id)
        else:
            from database import get_shop_manual_stock_in_transaction

            row = get_shop_manual_stock_in_transaction(tx_id)
    except Exception:
        row = None
    if not row:
        abort(404)
    return render_template(
        "it_support_stock_supplier_transaction_detail.html",
        tx=row,
        seller_name=seller_name,
        seller_phone=seller_phone,
        mode=(request.args.get("mode") or "").strip(),
        single_day=(request.args.get("single_day") or "").strip(),
        start_date=(request.args.get("start_date") or "").strip(),
        end_date=(request.args.get("end_date") or "").strip(),
        month=(request.args.get("month") or "").strip(),
        year=(request.args.get("year") or "").strip(),
        filter_shop_id=request.args.get("shop_id", type=int),
        supplier_q=(request.args.get("supplier_q") or "").strip(),
        moved_by_q=(request.args.get("moved_by") or "").strip(),
    )


@app.route("/it_support/stock-reports/suppliers/<int:item_id>/stock-ins")
@login_required
def it_support_stock_supplier_stock_ins(item_id: int):
    _it_support_or_super_admin_only()
    try:
        from database import list_stock_transactions

        txs = list_stock_transactions(int(item_id), direction="in", limit=500)
    except Exception:
        txs = []
    payload = [_serialize_stock_in_row(dict(t)) for t in (txs or [])]
    return jsonify({"ok": True, "item_id": int(item_id), "transactions": payload})


@app.route("/it_support/company-stock-settings", methods=["GET", "POST"])
@login_required
def it_support_company_stock_settings():
    _it_support_or_super_admin_only()
    inv_mode = _global_pos_inventory_mode()
    try:
        from database import (
            get_shop_item_stock_movement_summary,
            init_items_table,
            init_shop_items_table,
            list_items_for_company_stock_settings,
            list_shop_items,
            list_shops,
            set_item_stock_alert_levels,
        )

        init_items_table()
        init_shop_items_table()
        items = list_items_for_company_stock_settings(limit=8000, inventory_mode=inv_mode) or []
        shops = list_shops(limit=1000) or []
        shop_reorder_totals = {}
        for sh in shops:
            sid = int(sh.get("id") or 0)
            if sid <= 0:
                continue
            s_items = list_shop_items(shop_id=sid, limit=8000) or []
            movement_map = get_shop_item_stock_movement_summary(shop_id=sid, lookback_days=30) or {}
            for s_it in s_items:
                iid = int(s_it.get("id") or 0)
                if iid <= 0:
                    continue
                shop_override = int(s_it.get("shop_reorder_level") or 0)
                suggested = _compute_shop_reorder_suggestion(
                    stock_qty=int(s_it.get("shop_stock_qty") or 0),
                    movement_row=(movement_map.get(iid) or {}),
                )
                effective = shop_override if shop_override > 0 else suggested
                if effective <= 0:
                    continue
                shop_reorder_totals[iid] = int(shop_reorder_totals.get(iid, 0)) + int(effective)
        for it in items:
            try:
                iid = int(it.get("id") or 0)
            except Exception:
                iid = 0
            it["reorder_level"] = int(shop_reorder_totals.get(iid, 0))
    except Exception:
        items = []

    if request.method == "POST":
        if inv_mode == "both":
            flash("Company stock thresholds are not editable from this page in Both mode store stock items.", "warning")
            return redirect(url_for("it_support_company_stock_settings"))
        allowed_ids = sorted({int(i.get("id") or 0) for i in items if i.get("id")} - {0})
        reorder_by_id = {int(i.get("id") or 0): int(i.get("reorder_level") or 0) for i in items if i.get("id")}
        ok = True
        for iid in allowed_ids:
            try:
                low_v = int(float((request.form.get(f"low_stock_{iid}") or "0").strip() or 0))
            except Exception:
                low_v = 0
            rl_v = int(reorder_by_id.get(iid, 0))
            if not set_item_stock_alert_levels(iid, low_v, rl_v):
                ok = False
                break
        if not allowed_ids:
            flash("No items were loaded to update.", "error")
        elif ok:
            flash(f"Saved low stock and reorder levels for {len(allowed_ids)} item(s).", "success")
        else:
            flash("Could not save. Ensure the database schema is current (restart app to run migrations).", "error")
        return redirect(url_for("it_support_company_stock_settings"))

    return render_template("it_support_company_stock_settings.html", items=items)


@app.route("/it_support/stock-settings")
@login_required
def it_support_stock_settings():
    _it_support_or_super_admin_only()
    return redirect(url_for("it_support_company_stock_settings"))


@app.route("/it_support/company-stock-update", methods=["GET", "POST"])
@login_required
def it_support_company_stock_update():
    _it_support_or_super_admin_only()
    inv_mode = _global_pos_inventory_mode()
    try:
        from database import get_company_stock_status, list_shops, list_stock_manage_items

        shops = list_shops(limit=500)
        items = list_stock_manage_items(limit=2000, inventory_mode=inv_mode)
        _, stock_rows = get_company_stock_status(limit_items=2000, inventory_mode=inv_mode)
    except Exception:
        shops, items, stock_rows = [], [], []

    if request.method == "POST":
        if inv_mode == "kitchen":
            flash(
                "Company stock transfers are not available while POS is set to kitchen portions only. "
                "Turn on shop stock sales in company printing settings, or use branch stock management where shelf inventory applies.",
                "warning",
            )
            return redirect(url_for("it_support_company_stock_update"))
        if inv_mode == "both":
            flash("Company stock update actions here use sales catalog items. In Both mode, manage shelf stock from shop stock management.", "warning")
            return redirect(url_for("it_support_company_stock_update"))
        action = (request.form.get("action") or "").strip().lower()
        note = (request.form.get("note") or "").strip().upper() or None

        item_ids = request.form.getlist("item_id[]")
        qtys = request.form.getlist("qty[]")

        allowed_ids = {int(i.get("id") or 0) for i in (items or []) if int(i.get("id") or 0) > 0}
        lines = []
        from database import normalize_stock_move_qty

        for i in range(min(len(item_ids), len(qtys), 200)):
            iid_raw = (item_ids[i] or "").strip()
            qty_raw = (qtys[i] or "").strip()
            if not iid_raw or not qty_raw:
                continue
            try:
                iid = int(iid_raw)
                q = normalize_stock_move_qty(qty_raw)
            except Exception:
                continue
            if q is None:
                continue
            if iid not in allowed_ids:
                continue
            lines.append((iid, q))

        if not lines:
            flash("Add at least one item and quantity.", "error")
            return redirect(url_for("it_support_company_stock_update"))

        emp_id = session.get("employee_id")
        ok_count = 0
        fail_count = 0

        try:
            from database import (
                ensure_shop_items_for_shop,
                shop_manual_stock_out,
                shop_request_stock_from_company,
                shop_return_stock_to_company,
                shop_transfer_stock_between_shops,
            )

            if action == "stock_in":
                shop_id = request.form.get("shop_id", type=int)
                if not shop_id:
                    flash("Select a shop for stock in.", "error")
                    return redirect(url_for("it_support_company_stock_update"))
                ensure_shop_items_for_shop(shop_id)
                for iid, q in lines:
                    ok = shop_request_stock_from_company(
                        shop_id=shop_id,
                        item_id=iid,
                        qty=q,
                        note=note,
                        created_by_employee_id=emp_id,
                    )
                    ok_count += 1 if ok else 0
                    fail_count += 0 if ok else 1

            elif action == "stock_out":
                shop_id = request.form.get("shop_id", type=int)
                reason = (request.form.get("stock_out_reason") or "").strip().lower()
                if not shop_id:
                    flash("Select a shop for stock out.", "error")
                    return redirect(url_for("it_support_company_stock_update"))
                if reason not in ("return", "waste"):
                    flash("Choose RETURN or WASTE.", "error")
                    return redirect(url_for("it_support_company_stock_update"))
                ensure_shop_items_for_shop(shop_id)
                for iid, q in lines:
                    if reason == "return":
                        ok = shop_return_stock_to_company(
                            shop_id=shop_id,
                            item_id=iid,
                            qty=q,
                            reason="return",
                            refunded=False,
                            refund_amount=None,
                            note=note,
                            created_by_employee_id=emp_id,
                        )
                    else:
                        # Waste reduces shop stock only.
                        ok = shop_manual_stock_out(
                            shop_id=shop_id,
                            item_id=iid,
                            qty=q,
                            reason="waste",
                            refunded=False,
                            refund_amount=None,
                            note=note,
                            created_by_employee_id=emp_id,
                        )
                    ok_count += 1 if ok else 0
                    fail_count += 0 if ok else 1

            elif action == "transfer":
                from_shop_id = request.form.get("from_shop_id", type=int)
                to_shop_id = request.form.get("to_shop_id", type=int)
                if not from_shop_id or not to_shop_id or int(from_shop_id) == int(to_shop_id):
                    flash("Select different FROM and TO shops for transfer.", "error")
                    return redirect(url_for("it_support_company_stock_update"))
                ensure_shop_items_for_shop(from_shop_id)
                ensure_shop_items_for_shop(to_shop_id)
                for iid, q in lines:
                    ok = shop_transfer_stock_between_shops(
                        from_shop_id=from_shop_id,
                        to_shop_id=to_shop_id,
                        item_id=iid,
                        qty=q,
                        note=note,
                        created_by_employee_id=emp_id,
                    )
                    ok_count += 1 if ok else 0
                    fail_count += 0 if ok else 1
            else:
                flash("Invalid action.", "error")
                return redirect(url_for("it_support_company_stock_update"))

        except Exception as e:
            app.logger.exception("Company stock update failed: %s", e)
            flash(f"Could not update stock. {type(e).__name__}: {e}", "error")
            return redirect(url_for("it_support_company_stock_update"))

        if ok_count and not fail_count:
            flash(f"Stock updated for {ok_count} item(s).", "success")
        elif ok_count and fail_count:
            flash(f"Stock updated for {ok_count} item(s); {fail_count} failed.", "warning")
        else:
            flash("No items were updated. Check quantities and stock availability.", "error")
        return redirect(url_for("it_support_company_stock_update"))

    return render_template(
        "it_support_company_stock_update.html",
        shops=shops,
        items=items,
        stock_rows=stock_rows,
        pos_inventory_mode=inv_mode,
    )


@app.route("/it_support/item-management/item-analytics")
@login_required
def it_support_item_analytics():
    _it_support_only()
    analytics_filter = _build_analytics_filter()
    item_id = request.args.get("item_id", type=int)
    catalog_items = []
    detail = None
    try:
        from database import get_it_support_item_detail_analytics, list_items

        catalog_items = list_items(limit=500) or []
        if item_id:
            detail = get_it_support_item_detail_analytics(item_id, analytics_filter)
    except Exception:
        catalog_items, detail = [], None
    if item_id and not detail:
        flash("Item not found or analytics could not be loaded.", "error")
    return render_template(
        "it_support_item_analytics.html",
        analytics_filter=analytics_filter,
        catalog_items=catalog_items,
        detail=detail,
        selected_item_id=item_id,
    )


@app.route("/it_support/item-management/item-audit")
@login_required
def it_support_item_audit():
    _it_support_only()
    try:
        from database import list_items

        items = list_items(limit=5000) or []
    except Exception:
        items = []
    return render_template("it_support_item_audit.html", items=items)


@app.route("/it_support/items/<int:item_id>/toggle-status", methods=["POST"])
@login_required
def it_support_item_toggle_status(item_id: int):
    _it_support_only()
    wants_json = _request_wants_json()
    try:
        from database import get_item_by_id, toggle_item_status

        ok = toggle_item_status(item_id)
        item = get_item_by_id(item_id) if ok else None
    except Exception:
        ok, item = False, None
    if wants_json:
        if ok and item:
            return jsonify(
                {
                    "ok": True,
                    "message": "Item status updated.",
                    "item_id": item_id,
                    "status": item.get("status"),
                }
            )
        return jsonify({"ok": False, "error": "Could not update item status."}), 400
    flash("Item status updated." if ok else "Could not update item status.", "success" if ok else "error")
    return redirect(url_for("it_support_item_management"))


@app.route("/it_support/items/<int:item_id>/toggle-stock-update", methods=["POST"])
@login_required
def it_support_item_toggle_stock_update(item_id: int):
    _it_support_only()
    wants_json = _request_wants_json()
    try:
        from database import get_item_by_id, toggle_stock_update

        ok = toggle_stock_update(item_id)
        item = get_item_by_id(item_id) if ok else None
    except Exception:
        ok, item = False, None
    m = _pos_inventory_mode_from_ps(_load_printing_settings())
    if ok:
        if m == "kitchen":
            msg = "Kitchen portion update setting updated."
        elif m == "shop":
            msg = "Shop stock update setting updated."
        elif m == "both":
            msg = "Kitchen portion (POS) toggle updated. Shelf stock is managed separately in branch Stock management."
        else:
            msg = "Company POS inventory toggle updated."
    else:
        msg = "Could not update company setting."
    if wants_json:
        if ok and item:
            return jsonify(
                {
                    "ok": True,
                    "message": msg,
                    "item_id": item_id,
                    "stock_update_enabled": bool(item.get("stock_update_enabled")),
                }
            )
        return jsonify({"ok": False, "error": msg}), 400
    if ok:
        flash(msg, "success")
    else:
        flash(msg, "error")
    return redirect(url_for("it_support_item_management"))


@app.route("/it_support/item-management/bulk-status", methods=["POST"])
@login_required
def it_support_item_bulk_status():
    _it_support_only()
    wants_json = _request_wants_json()
    state = (request.form.get("state") or "").strip().lower()
    active = state == "on"
    if state not in ("on", "off"):
        if wants_json:
            return jsonify({"ok": False, "error": "Invalid bulk status action."}), 400
        flash("Invalid bulk status action.", "error")
        return redirect(url_for("it_support_item_management"))
    try:
        from database import set_all_items_status

        count = set_all_items_status(active)
    except Exception:
        count = 0
    if count > 0:
        msg = f"All {count} item(s) {'activated' if active else 'suspended'}."
        if wants_json:
            return jsonify(
                {
                    "ok": True,
                    "message": msg,
                    "count": count,
                    "status": "active" if active else "suspended",
                }
            )
        flash(msg, "success")
    else:
        if wants_json:
            return jsonify({"ok": False, "error": "No items to update."}), 400
        flash("No items to update.", "error")
    return redirect(url_for("it_support_item_management"))


@app.route("/it_support/item-management/bulk-stock-update", methods=["POST"])
@login_required
def it_support_item_bulk_stock_update():
    _it_support_only()
    wants_json = _request_wants_json()
    state = (request.form.get("state") or "").strip().lower()
    enabled = state == "on"
    if state not in ("on", "off"):
        if wants_json:
            return jsonify({"ok": False, "error": "Invalid bulk stock action."}), 400
        flash("Invalid bulk stock action.", "error")
        return redirect(url_for("it_support_item_management"))
    try:
        from database import set_all_items_stock_update

        count = set_all_items_stock_update(enabled)
    except Exception:
        count = 0
    m = _pos_inventory_mode_from_ps(_load_printing_settings())
    if count > 0:
        if enabled:
            if m == "kitchen":
                msg = f"Kitchen portion updates enabled for all {count} item(s)."
            elif m == "shop":
                msg = f"Stock updates enabled for all {count} item(s)."
            elif m == "both":
                msg = (
                    f"Kitchen portion (POS) updates enabled for all {count} item(s). "
                    "Shelf stock remains in branch Stock management."
                )
            else:
                msg = f"POS inventory master enabled for all {count} item(s)."
        else:
            if m == "kitchen":
                msg = f"Kitchen portion updates disabled for all {count} item(s)."
            elif m == "shop":
                msg = f"Stock updates disabled for all {count} item(s)."
            elif m == "both":
                msg = f"Kitchen portion (POS) updates disabled for all {count} item(s)."
            else:
                msg = f"POS inventory master disabled for all {count} item(s)."
        if wants_json:
            return jsonify(
                {
                    "ok": True,
                    "message": msg,
                    "count": count,
                    "stock_update_enabled": enabled,
                }
            )
        flash(msg, "success")
    else:
        if wants_json:
            return jsonify({"ok": False, "error": "No items to update."}), 400
        flash("No items to update.", "error")
    return redirect(url_for("it_support_item_management"))


@app.route("/it_support/items/<int:item_id>/delete", methods=["POST"])
@login_required
def it_support_item_delete(item_id: int):
    _it_support_only()
    try:
        from database import delete_item

        ok = delete_item(item_id)
    except Exception:
        ok = False
    if ok:
        _log_hr_activity_safe(
            "delete",
            target_type="item",
            target_id=int(item_id),
            description=f"Deleted item #{item_id}",
        )
    flash("Item deleted." if ok else "Could not delete item.", "success" if ok else "error")
    return redirect(url_for("it_support_item_management"))


@app.route("/it_support/items/<int:item_id>/edit", methods=["GET", "POST"])
@login_required
def it_support_item_edit(item_id: int):
    _it_support_only()
    try:
        from database import get_item_by_id

        item = get_item_by_id(item_id)
    except Exception:
        item = None
    if not item:
        flash("Item not found.", "error")
        return redirect(url_for("it_support_item_management"))

    if request.method == "POST":
        category = (request.form.get("category") or "").strip().upper()
        name = (request.form.get("name") or "").strip().upper()
        description = (request.form.get("description") or "").strip().upper()
        price_raw = (request.form.get("price") or "").strip()
        selling_raw = (request.form.get("selling_price") or "").strip()

        if not category or not name or not price_raw:
            flash("Please fill item category, item name, and original selling price.", "error")
            return redirect(url_for("it_support_item_edit", item_id=item_id))

        try:
            price = float(price_raw)
            if price < 0:
                raise ValueError()
        except Exception:
            flash("Original selling price must be a valid number.", "error")
            return redirect(url_for("it_support_item_edit", item_id=item_id))

        if not selling_raw:
            flash("Selling price is required.", "error")
            return redirect(url_for("it_support_item_edit", item_id=item_id))
        try:
            selling_price = float(selling_raw)
            if selling_price < 0:
                raise ValueError()
        except Exception:
            flash("Selling price must be a valid number.", "error")
            return redirect(url_for("it_support_item_edit", item_id=item_id))

        # Stock quantity is managed only via stock management transactions.
        stock_qty = int(item.get("stock_qty") or 0)

        img = request.files.get("image")
        image_path = item.get("image_path")
        if request.form.get("remove_image") == "1":
            image_path = None
        if img and img.filename:
            new_path = _save_item_upload(img)
            if new_path is None:
                flash("Item image must be PNG, JPG, GIF, or WebP.", "error")
                return redirect(url_for("it_support_item_edit", item_id=item_id))
            image_path = new_path

        try:
            from database import update_item

            ok = update_item(
                item_id,
                category=category,
                name=name,
                description=description,
                price=price,
                selling_price=selling_price,
                image_path=image_path,
                stock_qty=stock_qty,
            )
        except Exception:
            ok = False

        if ok:
            _log_hr_activity_safe(
                "update",
                target_type="item",
                target_id=int(item_id),
                description=f"Updated item #{item_id} ({name})",
            )
        flash("Item updated." if ok else "Could not update item.", "success" if ok else "error")
        return redirect(url_for("it_support_item_management"))

    return render_template("it_support_edit_item.html", item=item)


@app.route("/it_support/analytics")
@login_required
def it_support_analytics():
    _it_support_or_super_admin_only()
    return render_template("it_support_analytics.html")


def _render_it_support_analytics_page(view_key: str):
    _it_support_or_super_admin_only()
    analytics_filter = _build_analytics_filter()
    analytics_scope = _analytics_scope_from_request()
    labels = {
        "revenue": "Revenue analytics",
        "item": "Item analytics",
        "sales": "Sales analytics",
        "credit": "Credit analytics",
        "period": "Period analytics",
        "employee": "Employee analytics",
        "customer": "Customer analytics",
        "shop": "Shop analytics",
    }
    if view_key not in labels:
        abort(404)
    revenue_data = None
    item_data = None
    period_data = None
    employee_data = None
    sales_data = None
    credit_data = None
    customer_data = None
    shops = []
    selected_shop_id = request.args.get("shop_id", type=int)
    shop_view = (request.args.get("shop_view") or "revenue").strip().lower()
    if shop_view not in ("revenue", "item", "sales", "credit", "period", "stock", "customer"):
        shop_view = "revenue"
    shop_view_data = None
    if view_key == "revenue":
        try:
            from database import get_it_support_revenue_analytics

            revenue_data = get_it_support_revenue_analytics(
                analytics_filter=analytics_filter,
                analytics_scope=analytics_scope,
                transactions_limit=120,
                transactions_offset=0,
                include_transactions=True,
            )
        except Exception:
            revenue_data = {
                "tx_count": 0,
                "sale_amount": 0.0,
                "credit_amount": 0.0,
                "total_amount": 0.0,
                "cash_paid_total": 0.0,
                "mpesa_paid_total": 0.0,
                "shops": [],
                "daily": [],
                "transactions": [],
                "transactions_meta": {
                    "limit": 120,
                    "offset": 0,
                    "loaded_count": 0,
                    "total_count": 0,
                    "has_more": False,
                },
            }
    if view_key == "item":
        try:
            from database import get_it_support_item_analytics

            item_data = get_it_support_item_analytics(
                analytics_filter=analytics_filter,
                analytics_scope=analytics_scope,
                lines_limit=120,
                lines_offset=0,
                include_lines=True,
            )
        except Exception:
            item_data = {
                "total_qty": 0,
                "total_revenue": 0.0,
                "line_count": 0,
                "distinct_items": 0,
                "top_items": [],
                "shops": [],
                "lines": [],
                "lines_meta": {
                    "limit": 120,
                    "offset": 0,
                    "loaded_count": 0,
                    "total_count": 0,
                    "has_more": False,
                },
            }
    if view_key == "period":
        try:
            from database import get_it_support_period_analytics

            period_data = get_it_support_period_analytics(
                analytics_filter=analytics_filter, analytics_scope=analytics_scope
            )
        except Exception:
            period_data = {
                "total_tx_count": 0,
                "total_revenue": 0.0,
                "daily": [],
                "hourly": [],
                "employees": [],
                "shops": [],
                "peak_day": None,
                "peak_hour": None,
            }
    if view_key == "employee":
        try:
            from database import get_it_support_employee_analytics

            employee_data = get_it_support_employee_analytics(
                analytics_filter=analytics_filter, analytics_scope=analytics_scope
            )
        except Exception:
            employee_data = {
                "total_tx_count": 0,
                "total_revenue": 0.0,
                "distinct_employees": 0,
                "employees": [],
                "employee_shop_rows": [],
                "transactions": [],
                "top_employee": None,
                "least_employee": None,
            }
    if view_key == "sales":
        try:
            from database import get_it_support_sales_analytics

            sales_data = get_it_support_sales_analytics(
                analytics_filter=analytics_filter, analytics_scope=analytics_scope
            )
        except Exception:
            sales_data = {
                "total_tx_count": 0,
                "total_revenue": 0.0,
                "daily": [],
                "hourly": [],
                "shops": [],
                "employees": [],
                "transactions": [],
                "peak_day": None,
                "peak_hour": None,
            }
    if view_key == "credit":
        try:
            from database import get_it_support_credit_analytics

            credit_data = get_it_support_credit_analytics(
                analytics_filter=analytics_filter, analytics_scope=analytics_scope
            )
        except Exception:
            credit_data = {
                "total_tx_count": 0,
                "total_revenue": 0.0,
                "daily": [],
                "hourly": [],
                "shops": [],
                "employees": [],
                "customers": [],
                "transactions": [],
                "peak_day": None,
                "peak_hour": None,
            }
    if view_key == "customer":
        try:
            from database import get_it_support_customer_analytics

            customer_data = get_it_support_customer_analytics(
                analytics_filter=analytics_filter,
                analytics_scope=analytics_scope,
                customers_limit=120,
                customers_offset=0,
                include_customers=True,
            )
        except Exception:
            customer_data = {
                "total_tx_count": 0,
                "total_amount": 0.0,
                "distinct_customers": 0,
                "customers": [],
                "customers_meta": {
                    "limit": 120,
                    "offset": 0,
                    "loaded_count": 0,
                    "total_count": 0,
                    "has_more": False,
                },
            }
    if view_key == "shop":
        try:
            from database import list_shops

            shops = list_shops(limit=1000)
        except Exception:
            shops = []
        valid_ids = {int(s.get("id")) for s in shops if s.get("id") is not None}
        if selected_shop_id not in valid_ids:
            selected_shop_id = int((shops or [{}])[0].get("id") or 0) or None
        if selected_shop_id:
            try:
                from database import (
                    get_shop_credit_analytics,
                    get_shop_customer_analytics,
                    get_shop_item_analytics,
                    get_shop_period_analytics,
                    get_shop_revenue_analytics,
                    get_shop_sales_analytics,
                    get_shop_stock_analytics,
                )

                if shop_view == "item":
                    shop_view_data = get_shop_item_analytics(
                        shop_id=selected_shop_id,
                        analytics_filter=analytics_filter,
                        analytics_scope=analytics_scope,
                    )
                elif shop_view == "sales":
                    shop_view_data = get_shop_sales_analytics(
                        shop_id=selected_shop_id,
                        analytics_filter=analytics_filter,
                        analytics_scope=analytics_scope,
                    )
                elif shop_view == "credit":
                    shop_view_data = get_shop_credit_analytics(
                        shop_id=selected_shop_id,
                        analytics_filter=analytics_filter,
                        analytics_scope=analytics_scope,
                    )
                elif shop_view == "period":
                    shop_view_data = get_shop_period_analytics(
                        shop_id=selected_shop_id,
                        analytics_filter=analytics_filter,
                        analytics_scope=analytics_scope,
                    )
                elif shop_view == "stock":
                    shop_view_data = get_shop_stock_analytics(
                        shop_id=selected_shop_id, analytics_filter=analytics_filter
                    )
                elif shop_view == "customer":
                    shop_view_data = get_shop_customer_analytics(
                        shop_id=selected_shop_id,
                        analytics_filter=analytics_filter,
                        analytics_scope=analytics_scope,
                    )
                else:
                    shop_view_data = get_shop_revenue_analytics(
                        shop_id=selected_shop_id,
                        analytics_filter=analytics_filter,
                        analytics_scope=analytics_scope,
                    )
            except Exception:
                shop_view_data = None
    shop_sku_count = 0
    if view_key == "shop" and selected_shop_id:
        shop_sku_count = _shop_active_sku_count(int(selected_shop_id))
    return render_template(
        "it_support_analytics_page.html",
        analytics_key=view_key,
        analytics_title=labels[view_key],
        analytics_filter=analytics_filter,
        analytics_scope=analytics_scope,
        revenue_data=revenue_data,
        item_data=item_data,
        period_data=period_data,
        employee_data=employee_data,
        sales_data=sales_data,
        credit_data=credit_data,
        customer_data=customer_data,
        shops=shops,
        selected_shop_id=selected_shop_id,
        shop_view=shop_view,
        shop_view_data=shop_view_data,
        shop_sku_count=shop_sku_count,
        shops_list=shops,
    )


def _revenue_tx_params_from_request() -> dict:
    """Pagination / staged-load flags for revenue analytics API."""
    include_transactions = request.args.get("include_transactions", "1") != "0"
    try:
        tx_limit = int(request.args.get("tx_limit", 150))
    except (TypeError, ValueError):
        tx_limit = 150
    try:
        tx_offset = int(request.args.get("tx_offset", 0))
    except (TypeError, ValueError):
        tx_offset = 0
    return {
        "transactions_limit": tx_limit,
        "transactions_offset": tx_offset,
        "include_transactions": include_transactions,
    }


def _item_lines_params_from_request() -> dict:
    include_lines = request.args.get("include_lines", "1") != "0"
    try:
        lines_limit = int(request.args.get("bulk_limit", request.args.get("lines_limit", 150)))
    except (TypeError, ValueError):
        lines_limit = 150
    try:
        lines_offset = int(request.args.get("bulk_offset", request.args.get("lines_offset", 0)))
    except (TypeError, ValueError):
        lines_offset = 0
    return {
        "lines_limit": lines_limit,
        "lines_offset": lines_offset,
        "include_lines": include_lines,
    }


def _customer_bulk_params_from_request() -> dict:
    include_customers = request.args.get("include_customers", "1") != "0"
    try:
        customers_limit = int(request.args.get("bulk_limit", request.args.get("customers_limit", 150)))
    except (TypeError, ValueError):
        customers_limit = 150
    try:
        customers_offset = int(request.args.get("bulk_offset", request.args.get("customers_offset", 0)))
    except (TypeError, ValueError):
        customers_offset = 0
    return {
        "customers_limit": customers_limit,
        "customers_offset": customers_offset,
        "include_customers": include_customers,
    }


def _json_safe_value(val):
    if val is None:
        return val
    if hasattr(val, "strftime"):
        return val.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(val, dict):
        return {k: _json_safe_value(v) for k, v in val.items()}
    if isinstance(val, list):
        return [_json_safe_value(v) for v in val]
    return val


def _serialize_analytics_json(data: dict) -> dict:
    """Make analytics payload JSON-safe (datetimes → strings)."""
    if not data:
        return {}
    return _json_safe_value(dict(data))


def _serialize_revenue_analytics_json(data: dict) -> dict:
    """Make revenue analytics payload JSON-safe (datetimes → strings)."""
    return _serialize_analytics_json(data)


def _fetch_it_support_analytics_payload(view_key: str, analytics_filter: dict, analytics_scope: str) -> dict:
    """Load analytics data for API / live updates."""
    if view_key == "revenue":
        from database import get_it_support_revenue_analytics

        return get_it_support_revenue_analytics(
            analytics_filter=analytics_filter,
            analytics_scope=analytics_scope,
            **_revenue_tx_params_from_request(),
        )
    if view_key == "item":
        from database import get_it_support_item_analytics

        return get_it_support_item_analytics(
            analytics_filter=analytics_filter,
            analytics_scope=analytics_scope,
            **_item_lines_params_from_request(),
        )
    if view_key == "customer":
        from database import get_it_support_customer_analytics

        return get_it_support_customer_analytics(
            analytics_filter=analytics_filter,
            analytics_scope=analytics_scope,
            **_customer_bulk_params_from_request(),
        )
    if view_key == "period":
        from database import get_it_support_period_analytics

        return get_it_support_period_analytics(
            analytics_filter=analytics_filter, analytics_scope=analytics_scope
        )
    if view_key == "employee":
        from database import get_it_support_employee_analytics

        return get_it_support_employee_analytics(
            analytics_filter=analytics_filter, analytics_scope=analytics_scope
        )
    if view_key == "sales":
        from database import get_it_support_sales_analytics

        return get_it_support_sales_analytics(
            analytics_filter=analytics_filter, analytics_scope=analytics_scope
        )
    if view_key == "credit":
        from database import get_it_support_credit_analytics

        return get_it_support_credit_analytics(
            analytics_filter=analytics_filter, analytics_scope=analytics_scope
        )
    if view_key == "shop":
        return _fetch_shop_analytics_payload(analytics_filter, analytics_scope)
    abort(404)


def _it_support_analytics_json_response(view_key: str, payload: dict, analytics_filter: dict, analytics_scope: str):
    safe = _serialize_analytics_json(payload)
    body = {
        "ok": True,
        "key": view_key,
        "data": safe,
        "filter": analytics_filter,
        "analytics_scope": analytics_scope,
        "period_label": analytics_filter.get("range_label") or "",
    }
    if view_key == "revenue":
        body["revenue"] = safe
    if view_key == "shop":
        body["shop_data"] = safe
        body["shop_view"] = _shop_view_from_request()
    return jsonify(body)


@app.route("/it_support/analytics/revenue")
@login_required
def it_support_revenue_analytics():
    return _render_it_support_analytics_page("revenue")


@app.route("/it_support/analytics/revenue/data")
@login_required
def it_support_revenue_analytics_data():
    """JSON payload for live revenue analytics filter updates."""
    return it_support_analytics_data("revenue")


@app.route("/it_support/analytics/<view_key>/data")
@login_required
def it_support_analytics_data(view_key: str):
    """JSON payload for live IT support analytics filter updates."""
    _it_support_or_super_admin_only()
    labels = (
        "revenue",
        "item",
        "sales",
        "credit",
        "period",
        "employee",
        "customer",
        "shop",
    )
    if view_key not in labels:
        abort(404)
    analytics_filter = _build_analytics_filter()
    analytics_scope = _analytics_scope_from_request()
    try:
        payload = _fetch_it_support_analytics_payload(view_key, analytics_filter, analytics_scope)
    except Exception:
        payload = {}
    return _it_support_analytics_json_response(view_key, payload, analytics_filter, analytics_scope)


@app.route("/it_support/analytics/item")
@login_required
def it_support_item_analytics_page():
    return _render_it_support_analytics_page("item")


@app.route("/it_support/analytics/sales")
@login_required
def it_support_sales_analytics():
    return _render_it_support_analytics_page("sales")


@app.route("/it_support/analytics/credit")
@login_required
def it_support_credit_analytics():
    return _render_it_support_analytics_page("credit")


@app.route("/it_support/analytics/period")
@login_required
def it_support_period_analytics():
    return _render_it_support_analytics_page("period")


@app.route("/it_support/analytics/employee")
@login_required
def it_support_employee_analytics():
    return _render_it_support_analytics_page("employee")


@app.route("/it_support/analytics/shop")
@login_required
def it_support_shop_analytics():
    return _render_it_support_analytics_page("shop")


@app.route("/it_support/analytics/customer")
@login_required
def it_support_customer_analytics():
    return _render_it_support_analytics_page("customer")


@app.route("/it_support/analytics/customer/transactions")
@login_required
def it_support_customer_transactions():
    _it_support_or_super_admin_only()
    customer_name = (request.args.get("customer_name") or "").strip() or "WALK IN"
    customer_phone = (request.args.get("customer_phone") or "").strip() or "-"
    scoped_shop_id = request.args.get("shop_id", type=int)
    analytics_filter = _build_analytics_filter()
    analytics_scope = _analytics_scope_from_request()
    try:
        from database import (
            get_it_support_customer_detail_analytics,
            get_it_support_customer_transaction_items,
            get_it_support_customer_transactions,
        )

        txs = get_it_support_customer_transactions(
            customer_name=customer_name,
            customer_phone=customer_phone,
            limit=3000,
            analytics_filter=analytics_filter,
            shop_id=scoped_shop_id,
            analytics_scope=analytics_scope,
        )
        analytics = get_it_support_customer_detail_analytics(
            customer_name=customer_name,
            customer_phone=customer_phone,
            analytics_filter=analytics_filter,
            shop_id=scoped_shop_id,
            analytics_scope=analytics_scope,
        )
        tx_item_rows = get_it_support_customer_transaction_items(
            customer_name=customer_name,
            customer_phone=customer_phone,
            limit=5000,
            analytics_filter=analytics_filter,
            shop_id=scoped_shop_id,
            analytics_scope=analytics_scope,
        )
        items_by_sale_id = {}
        for row in (tx_item_rows or []):
            try:
                sale_id = int(row.get("sale_id") or 0)
            except Exception:
                sale_id = 0
            if sale_id <= 0:
                continue
            items_by_sale_id.setdefault(sale_id, []).append(
                {
                    "item_name": row.get("item_name") or "Item",
                    "qty": int(row.get("qty") or 0),
                    "amount": float(row.get("amount") or 0),
                    "picked_at": _credit_item_pick_date(row.get("created_at")),
                }
            )
    except Exception:
        txs = []
        tx_item_rows = []
        items_by_sale_id = {}
        analytics = {
            "total_amount": 0.0,
            "total_tx_count": 0,
            "sale_amount": 0.0,
            "sale_tx_count": 0,
            "credit_amount": 0.0,
            "credit_tx_count": 0,
            "total_item_qty": 0,
            "distinct_items": 0,
            "avg_ticket": 0.0,
            "daily": [],
            "hourly": [],
            "shops": [],
            "employees": [],
            "top_items": [],
        }
    return render_template(
        "it_support_customer_transactions.html",
        customer_name=customer_name,
        customer_phone=customer_phone,
        scoped_shop_id=scoped_shop_id,
        analytics_filter=analytics_filter,
        analytics_scope=analytics_scope,
        customer_analytics=analytics,
        transactions=txs,
        transaction_item_rows=tx_item_rows,
        transaction_items_by_sale_id=items_by_sale_id,
    )


def _build_credit_payments_query_dict(
    *,
    all_time: bool,
    analytics_filter: dict,
    filter_shop_id: int | None,
    customer_q: str,
) -> dict:
    """Canonical query args for credit payments (avoids duplicate period params in the URL)."""
    q: dict = {}
    if filter_shop_id:
        q["shop_id"] = filter_shop_id
    cq = (customer_q or "").strip()
    if cq:
        q["customer_q"] = cq
    if all_time:
        q["all_time"] = "1"
        return q
    f = analytics_filter or {}
    mode = f.get("mode") or "single_day"
    q["mode"] = mode
    if mode == "single_day":
        q["single_day"] = f.get("single_day")
    elif mode == "period":
        q["start_date"] = f.get("start_date")
        q["end_date"] = f.get("end_date")
    elif mode == "month":
        q["month"] = f.get("month")
    elif mode == "year":
        q["year"] = f.get("year")
    return q


def _credit_payments_args_match(canonical: dict) -> bool:
    skip = frozenset({"print"})
    cur = {k: request.args.get(k) for k in request.args if k not in skip}
    for key, val in canonical.items():
        if str(cur.get(key, "") or "") != str(val or ""):
            return False
    for key in cur:
        if key not in canonical:
            return False
    return True


@app.route("/it_support/credit-payments")
@login_required
def it_support_credit_payments():
    _it_support_or_super_admin_only()
    analytics_filter = _build_analytics_filter()
    all_time_arg = (request.args.get("all_time") or "").strip().lower()
    all_time = all_time_arg in ("1", "true", "yes", "on")
    mode = (request.args.get("mode") or "").strip()
    single_day = (request.args.get("single_day") or "").strip()
    start_date = (request.args.get("start_date") or "").strip()
    end_date = (request.args.get("end_date") or "").strip()
    month = (request.args.get("month") or "").strip()
    year = (request.args.get("year") or "").strip()
    filter_shop_id = request.args.get("shop_id", type=int)
    if filter_shop_id is not None and filter_shop_id <= 0:
        filter_shop_id = None
    customer_q = (request.args.get("customer_q") or "").strip()
    should_print = (request.args.get("print") or "").strip().lower() in ("1", "true", "yes", "on")

    # Default behavior: show all credit sales until the user intentionally applies filters.
    has_explicit_filter = bool(
        all_time
        or mode
        or single_day
        or start_date
        or end_date
        or month
        or year
        or filter_shop_id
        or customer_q
    )
    if not has_explicit_filter:
        all_time = True

    canonical_q = _build_credit_payments_query_dict(
        all_time=all_time,
        analytics_filter=analytics_filter,
        filter_shop_id=filter_shop_id,
        customer_q=customer_q,
    )
    if request.args and not should_print and not _credit_payments_args_match(canonical_q):
        return redirect(url_for("it_support_credit_payments", **canonical_q))

    credit_customers = []
    shops = []
    try:
        from database import list_company_credit_customers, list_shops

        shops = list_shops(limit=500) or []
        rows = list_company_credit_customers(
            limit=5000,
            analytics_filter=None if all_time else analytics_filter,
            shop_id=filter_shop_id,
            customer_q=customer_q or None,
        )
        credit_customers = [
            {
                "customer_name": r.get("customer_name") or "WALK IN",
                "customer_phone": r.get("customer_phone") or "-",
                "tx_count": int(r.get("tx_count") or 0),
                "credit_total": float(r.get("total_amount") or 0),
                "paid_total": float(r.get("paid_amount") or 0),
                "balance": float(r.get("remaining_amount") or 0),
                "last_credit_at": r.get("created_at"),
            }
            for r in (rows or [])
        ]
    except Exception:
        credit_customers, shops = [], []
    analytics_nav = _build_credit_payments_query_dict(
        all_time=all_time,
        analytics_filter=analytics_filter,
        filter_shop_id=filter_shop_id,
        customer_q=customer_q,
    )
    print_q = dict(analytics_nav)
    print_q["all_time"] = "1"
    print_q["print"] = "1"
    return render_template(
        "it_support_credit_payments.html",
        credit_customers=credit_customers,
        shops=shops,
        analytics_filter=analytics_filter,
        analytics_nav=analytics_nav,
        filter_shop_id=filter_shop_id,
        customer_q=customer_q,
        credit_all_time=all_time,
        credit_should_print=should_print,
        filter_print_url=url_for("it_support_credit_payments", **print_q),
    )


@app.route("/it_support/credit-payments/audit")
@login_required
def it_support_credit_payments_audit():
    _it_support_or_super_admin_only()
    analytics_filter = _build_analytics_filter()
    all_time_arg = (request.args.get("all_time") or "").strip().lower()
    all_time = all_time_arg in ("1", "true", "yes", "on")
    mode = (request.args.get("mode") or "").strip()
    single_day = (request.args.get("single_day") or "").strip()
    start_date = (request.args.get("start_date") or "").strip()
    end_date = (request.args.get("end_date") or "").strip()
    month = (request.args.get("month") or "").strip()
    year = (request.args.get("year") or "").strip()
    filter_shop_id = request.args.get("shop_id", type=int)
    if filter_shop_id is not None and filter_shop_id <= 0:
        filter_shop_id = None
    customer_q = (request.args.get("customer_q") or "").strip()
    payment_scope = (request.args.get("payment_scope") or "all").strip().lower()
    if payment_scope not in ("all", "partial", "paid"):
        payment_scope = "all"

    has_explicit_filter = bool(
        all_time
        or mode
        or single_day
        or start_date
        or end_date
        or month
        or year
        or filter_shop_id
        or customer_q
    )
    if not has_explicit_filter:
        all_time = True

    audit_sales = []
    payment_receipts = []
    shops = []
    try:
        from database import (
            list_all_shops_company_credit_payment_receipts,
            list_all_shops_credit_sales_with_payments_audit,
            list_shops,
        )

        shops = list_shops(limit=500) or []
        af = None if all_time else analytics_filter
        audit_sales = list_all_shops_credit_sales_with_payments_audit(
            limit=5000,
            analytics_filter=af,
            shop_id=filter_shop_id,
            customer_q=customer_q or None,
            payment_scope=payment_scope,
        )
        payment_receipts = list_all_shops_company_credit_payment_receipts(
            limit=5000,
            analytics_filter=af,
            shop_id=filter_shop_id,
            customer_q=customer_q or None,
        )
    except Exception:
        audit_sales, payment_receipts, shops = [], [], []
    analytics_nav = _build_credit_payments_query_dict(
        all_time=all_time,
        analytics_filter=analytics_filter,
        filter_shop_id=filter_shop_id,
        customer_q=customer_q,
    )
    return render_template(
        "it_support_credit_payments_audit.html",
        audit_sales=audit_sales,
        payment_receipts=payment_receipts,
        shops=shops,
        analytics_filter=analytics_filter,
        analytics_nav=analytics_nav,
        filter_shop_id=filter_shop_id,
        customer_q=customer_q,
        payment_scope=payment_scope,
        credit_all_time=all_time,
    )


@app.route("/it_support/credit-payments/upcoming-due")
@login_required
def it_support_credit_payments_upcoming_due():
    _it_support_or_super_admin_only()
    days_ahead = request.args.get("days", type=int) or 90
    if days_ahead < 1:
        days_ahead = 1
    if days_ahead > 730:
        days_ahead = 730
    filter_shop_id = request.args.get("shop_id", type=int)
    if filter_shop_id is not None and filter_shop_id <= 0:
        filter_shop_id = None
    customer_q = (request.args.get("customer_q") or "").strip()

    due_rows = []
    shops = []
    try:
        from database import list_all_shops_credit_due_reminders, list_shops

        shops = list_shops(limit=500) or []
        due_rows = list_all_shops_credit_due_reminders(
            limit=5000,
            days_ahead=days_ahead,
            shop_id=filter_shop_id,
            customer_q=customer_q or None,
        )
    except Exception:
        due_rows, shops = [], []
    return render_template(
        "it_support_credit_upcoming_due.html",
        due_rows=due_rows,
        shops=shops,
        analytics_nav=_analytics_nav_kwargs(),
        filter_shop_id=filter_shop_id,
        customer_q=customer_q,
        due_days_ahead=days_ahead,
    )


@app.route("/it_support/credit-payments/customer")
@login_required
def it_support_credit_payments_customer():
    _it_support_or_super_admin_only()
    customer_name = (request.args.get("customer_name") or "").strip() or "WALK IN"
    customer_phone = (request.args.get("customer_phone") or "").strip() or "-"
    shop_id = request.args.get("shop_id", type=int)
    if shop_id:
        shop = _get_shop_or_404(shop_id)
        note_ctx = _shop_customer_credit_note_context(
            shop_id, customer_name, customer_phone, analytics_filter=None
        )
        return render_template(
            "it_support_credit_payments_customer.html",
            shop=shop,
            shop_id=shop_id,
            customer_name=customer_name,
            customer_phone=customer_phone,
            company_credit_scope=False,
            embed_record_payment=True,
            credit_payment_return_to="customer",
            credit_note_pdf_url=url_for(
                "it_support_credit_payments_customer_pdf",
                shop_id=shop_id,
                customer_name=customer_name,
                customer_phone=customer_phone,
            ),
            **note_ctx,
        )
    note_ctx = _company_customer_credit_note_context(customer_name, customer_phone)
    shop_stub = {"shop_name": "All shops", "shop_code": "", "id": 0}
    return render_template(
        "it_support_credit_payments_customer.html",
        shop=shop_stub,
        shop_id=None,
        customer_name=customer_name,
        customer_phone=customer_phone,
        embed_record_payment=True,
        credit_payment_return_to="customer",
        credit_note_pdf_url=url_for(
            "it_support_credit_payments_customer_pdf",
            customer_name=customer_name,
            customer_phone=customer_phone,
        ),
        **note_ctx,
    )


@app.route("/it_support/credit-payments/customer.pdf")
@login_required
def it_support_credit_payments_customer_pdf():
    _it_support_or_super_admin_only()
    customer_name = (request.args.get("customer_name") or "").strip() or "WALK IN"
    customer_phone = (request.args.get("customer_phone") or "").strip() or "-"
    shop_id = request.args.get("shop_id", type=int)
    company_name = ""
    primary_color = ""
    try:
        from database import get_site_settings

        stored = get_site_settings(["company_name", "primary_color"]) or {}
        company_name = (stored.get("company_name") or "").strip()
        primary_color = (stored.get("primary_color") or "").strip()
    except Exception:
        pass
    slug = secure_filename(
        f"credit-note-{(customer_name or 'customer').replace(' ', '-').lower()}"
    )
    if shop_id:
        shop = _get_shop_or_404(shop_id)
        note_ctx = _shop_customer_credit_note_context(
            shop_id, customer_name, customer_phone, analytics_filter=None
        )
        return _credit_note_pdf_response(
            shop_name=shop.get("shop_name") or f"Shop {shop_id}",
            company_name=company_name or "Company",
            customer_name=customer_name,
            customer_phone=customer_phone,
            note_ctx=note_ctx,
            primary_color=primary_color or None,
            filename_slug=slug or "credit-note",
        )
    note_ctx = _company_customer_credit_note_context(customer_name, customer_phone)
    return _credit_note_pdf_response(
        shop_name="All shops",
        company_name=company_name or "Company",
        customer_name=customer_name,
        customer_phone=customer_phone,
        note_ctx=note_ctx,
        company_scope=True,
        primary_color=primary_color or None,
        filename_slug=slug or "credit-note",
    )


@app.route("/company/credit-payments/customer")
@login_required
def company_credit_payments_customer():
    role_key = (session.get("employee_role") or "").strip().lower()
    if role_key not in ("admin", "it_support", "super_admin", "company_manager"):
        abort(403)
    customer_name = (request.args.get("customer_name") or "").strip() or "WALK IN"
    customer_phone = (request.args.get("customer_phone") or "").strip() or "-"
    back_shop_id = request.args.get("back_shop_id", type=int)
    if back_shop_id:
        bshop = _get_shop_or_404(back_shop_id)
        if not _shop_pos_allow_credit_sale(bshop):
            return redirect(url_for("shop_dashboard", shop_id=back_shop_id))
    should_print = (request.args.get("print") or "").strip().lower() in ("1", "true", "yes", "on")
    note_ctx = _company_customer_credit_note_context(customer_name, customer_phone)
    shop_stub = {"shop_name": "All shops", "shop_code": ""}
    return render_template(
        "company_credit_payments_customer.html",
        shop=shop_stub,
        customer_name=customer_name,
        customer_phone=customer_phone,
        back_shop_id=back_shop_id,
        embed_record_payment=True,
        company_should_print=should_print,
        **note_ctx,
    )


@app.route("/company/credit-payments/customer.pdf")
@login_required
def company_credit_payments_customer_pdf():
    role_key = (session.get("employee_role") or "").strip().lower()
    if role_key not in ("admin", "it_support", "super_admin", "company_manager"):
        abort(403)
    customer_name = (request.args.get("customer_name") or "").strip() or "WALK IN"
    customer_phone = (request.args.get("customer_phone") or "").strip() or "-"
    note_ctx = _company_customer_credit_note_context(customer_name, customer_phone)
    company_name = ""
    primary_color = ""
    try:
        from database import get_site_settings

        stored = get_site_settings(["company_name", "primary_color"]) or {}
        company_name = (stored.get("company_name") or "").strip()
        primary_color = (stored.get("primary_color") or "").strip()
    except Exception:
        pass
    slug = secure_filename(
        f"credit-note-{(customer_name or 'customer').replace(' ', '-').lower()}"
    )
    return _credit_note_pdf_response(
        shop_name="All shops",
        company_name=company_name or "Company",
        customer_name=customer_name,
        customer_phone=customer_phone,
        note_ctx=note_ctx,
        company_scope=True,
        primary_color=primary_color or None,
        filename_slug=slug or "credit-note",
    )


@app.route("/company/credit-payments/pay", methods=["POST"])
@login_required
def company_credit_payments_pay():
    role_key = (session.get("employee_role") or "").strip().lower()
    if role_key not in ("admin", "it_support", "super_admin", "company_manager"):
        abort(403)
    customer_name = (request.form.get("customer_name") or "").strip() or "WALK IN"
    customer_phone = (request.form.get("customer_phone") or "").strip() or "-"
    back_shop_id = request.form.get("back_shop_id", type=int)
    if back_shop_id:
        bshop = _get_shop_or_404(back_shop_id)
        if not _shop_pos_allow_credit_sale(bshop):
            flash("Credit sales are disabled for this shop.", "error")
            return redirect(url_for("shop_dashboard", shop_id=back_shop_id))
    amount_raw = (request.form.get("amount") or "").strip()
    payment_method = (request.form.get("payment_method") or "").strip().lower()
    if payment_method not in ("cash", "mpesa", "bank", "other"):
        flash("Select a valid payment method.", "error")
        return redirect(
            url_for(
                "company_credit_payments_customer",
                customer_name=customer_name,
                customer_phone=customer_phone,
                back_shop_id=back_shop_id,
            )
        )
    try:
        amount = float(amount_raw)
    except Exception:
        flash("Enter a valid amount.", "error")
        return redirect(
            url_for(
                "company_credit_payments_customer",
                customer_name=customer_name,
                customer_phone=customer_phone,
                back_shop_id=back_shop_id,
            )
        )
    try:
        from database import apply_company_credit_payment_fifo

        res = apply_company_credit_payment_fifo(
            customer_name=customer_name,
            customer_phone=customer_phone,
            amount=amount,
            payment_method=payment_method,
        )
    except Exception:
        res = {"ok": False, "error": "Could not apply payment."}
    if not res.get("ok"):
        flash(res.get("error") or "Could not apply payment.", "error")
    else:
        unused = float(res.get("unused") or 0)
        if unused > 0.0001:
            flash(f"Payment applied. Unused amount: {unused:.2f}", "success")
        else:
            flash("Payment applied successfully.", "success")
    target = url_for(
        "company_credit_payments_customer",
        customer_name=customer_name,
        customer_phone=customer_phone,
        back_shop_id=back_shop_id,
    )
    return redirect(f"{target}#shop-credit-payments")


@app.route("/it_support/credit-payments/pay", methods=["POST"])
@login_required
def it_support_credit_payments_pay():
    _it_support_or_super_admin_only()
    shop_id = request.form.get("shop_id", type=int) if hasattr(request.form, "get") else None
    try:
        shop_id = int(shop_id or 0)
    except Exception:
        shop_id = 0
    customer_name = (request.form.get("customer_name") or "").strip() or "WALK IN"
    customer_phone = (request.form.get("customer_phone") or "").strip() or "-"
    return_to = (request.form.get("return_to") or "customer").strip().lower()
    sale_id = request.form.get("sale_id", type=int)
    amount_raw = (request.form.get("amount") or "").strip()
    note = (request.form.get("note") or "").strip() or None
    payment_method = (request.form.get("payment_method") or "").strip().lower()
    company_scope = shop_id <= 0

    def _it_support_credit_pay_redirect():
        kwargs = {
            "customer_name": customer_name,
            "customer_phone": customer_phone,
        }
        if not company_scope:
            kwargs["shop_id"] = shop_id
        return url_for("it_support_credit_payments_customer", **kwargs)

    try:
        amount = float(amount_raw)
    except Exception:
        flash("Enter a valid amount.", "error")
        return redirect(_it_support_credit_pay_redirect())
    if company_scope:
        if payment_method not in ("cash", "mpesa", "bank", "other"):
            flash("Select a valid payment method.", "error")
            return redirect(_it_support_credit_pay_redirect())
        try:
            from database import apply_company_credit_payment_fifo

            res = apply_company_credit_payment_fifo(
                customer_name=customer_name,
                customer_phone=customer_phone,
                amount=amount,
                payment_method=payment_method,
            )
        except Exception:
            res = {"ok": False, "error": "Could not apply payment."}
    else:
        try:
            from database import apply_shop_credit_payment_fifo

            res = apply_shop_credit_payment_fifo(
                shop_id=shop_id,
                customer_name=customer_name,
                customer_phone=customer_phone,
                amount=amount,
                note=note,
            )
        except Exception:
            res = {"ok": False, "error": "Could not apply payment."}
    if not res.get("ok"):
        flash(res.get("error") or "Could not apply payment.", "error")
    else:
        unused = float(res.get("unused") or 0)
        if unused > 0.0001:
            flash(f"Payment applied. Unused amount: {unused:.2f}", "success")
        else:
            flash("Payment applied successfully.", "success")
    return redirect(f"{_it_support_credit_pay_redirect()}#shop-credit-payments")


@app.route("/it_support/credit-payments/sale")
@login_required
def it_support_credit_sale_detail():
    """Legacy sale URL — redirect to combined customer account (phone reference)."""
    _it_support_or_super_admin_only()
    shop_id = request.args.get("shop_id", type=int)
    sale_id = request.args.get("sale_id", type=int)
    if not shop_id or not sale_id:
        abort(404)
    try:
        from database import get_shop_credit_sale_detail

        d = get_shop_credit_sale_detail(shop_id=shop_id, sale_id=sale_id) or {}
    except Exception:
        d = {}
    if not d.get("sale"):
        abort(404)
    sale = d["sale"]
    customer_name = (sale.get("customer_name") or "").strip() or "WALK IN"
    customer_phone = (sale.get("customer_phone") or "").strip() or "-"
    return redirect(
        url_for(
            "it_support_credit_payments_customer",
            customer_name=customer_name,
            customer_phone=customer_phone,
        )
    )


@app.route("/it_support/credit-payments/sale.pdf")
@login_required
def it_support_credit_sale_detail_pdf():
    _it_support_or_super_admin_only()
    shop_id = request.args.get("shop_id", type=int)
    sale_id = request.args.get("sale_id", type=int)
    if not shop_id or not sale_id:
        abort(404)
    shop = _get_shop_or_404(shop_id)
    try:
        from database import get_shop_credit_sale_detail

        d = get_shop_credit_sale_detail(shop_id=shop_id, sale_id=sale_id) or {}
    except Exception:
        d = {}
    if not d.get("sale"):
        abort(404)
    sale = d["sale"]
    customer_name = (sale.get("customer_name") or "").strip() or "WALK IN"
    customer_phone = (sale.get("customer_phone") or "").strip() or "-"
    note_ctx = _shop_customer_credit_note_context(
        shop_id, customer_name, customer_phone, analytics_filter=None
    )
    company_name = ""
    primary_color = ""
    try:
        from database import get_site_settings

        stored = get_site_settings(["company_name", "primary_color"]) or {}
        company_name = (stored.get("company_name") or "").strip()
        primary_color = (stored.get("primary_color") or "").strip()
    except Exception:
        pass
    return _credit_note_pdf_response(
        shop_name=shop.get("shop_name") or f"Shop {shop_id}",
        company_name=company_name or "Company",
        customer_name=customer_name,
        customer_phone=customer_phone,
        note_ctx=note_ctx,
        primary_color=primary_color or None,
        focus_sale=d,
        filename_slug=secure_filename(f"credit-sale-{sale_id}") or "credit-sale",
    )


@app.route("/company/credit-payments/sale")
@login_required
def company_credit_sale_detail():
    role_key = (session.get("employee_role") or "").strip().lower()
    if role_key not in ("admin", "it_support", "super_admin", "company_manager"):
        abort(403)
    shop_id = request.args.get("shop_id", type=int)
    sale_id = request.args.get("sale_id", type=int)
    customer_name = (request.args.get("customer_name") or "").strip() or "WALK IN"
    customer_phone = (request.args.get("customer_phone") or "").strip() or "-"
    back_shop_id = request.args.get("back_shop_id", type=int)
    if not shop_id or not sale_id:
        abort(404)
    if back_shop_id:
        bshop = _get_shop_or_404(back_shop_id)
        if not _shop_pos_allow_credit_sale(bshop):
            return redirect(url_for("shop_dashboard", shop_id=back_shop_id))
    try:
        from database import get_shop_credit_sale_detail

        d = get_shop_credit_sale_detail(shop_id=shop_id, sale_id=sale_id) or {}
    except Exception:
        d = {}
    if not d.get("sale"):
        abort(404)
    return render_template(
        "company_credit_sale_detail.html",
        shop_id=shop_id,
        sale=d["sale"],
        items=d.get("items") or [],
        customer_name=customer_name,
        customer_phone=customer_phone,
        back_shop_id=back_shop_id,
    )


@app.route("/it_support/store-management")
@login_required
def it_support_store_management():
    _it_support_only()
    return redirect(url_for("it_support_stock_management"))


@app.route("/it_support/register-shop", methods=["GET", "POST"])
@login_required
def it_support_register_shop():
    _it_support_only()

    if request.method == "POST":
        shop_name = (request.form.get("shop_name") or "").strip().upper()
        shop_code = (request.form.get("shop_code") or "").strip()
        shop_password = (request.form.get("shop_password") or "").strip()
        shop_location = (request.form.get("shop_location") or "").strip().upper()
        shop_location_description = (request.form.get("shop_location_description") or "").strip().upper()
        shop_phone = (request.form.get("shop_phone") or "").strip()

        if not shop_name or not shop_code or not shop_password or not shop_location:
            flash("Please fill shop name, shop code, shop password, and shop location.", "error")
            return redirect(url_for("it_support_register_shop"))
        if not CODE_RE.match(shop_code):
            flash("Shop code must be exactly 6 digits.", "error")
            return redirect(url_for("it_support_register_shop"))

        logo_path, _, logo_err = _resolve_shop_logo_upload()
        if logo_err:
            flash(logo_err, "error")
            return redirect(url_for("it_support_register_shop"))

        try:
            from database import create_shop, shop_code_available

            if not shop_code_available(shop_code):
                flash("Shop code already exists. Use a different code.", "error")
                return redirect(url_for("it_support_register_shop"))

            create_shop(
                shop_name=shop_name,
                shop_code=shop_code,
                shop_password_hash=generate_password_hash(shop_password),
                shop_location=shop_location,
                shop_location_description=shop_location_description or None,
                shop_phone=shop_phone or None,
                created_by_employee_id=session.get("employee_id"),
                shop_logo=logo_path,
            )
        except RuntimeError as e:
            app.logger.error("Register shop schema error: %s", e)
            flash("Could not save location description — database update required. Restart the app or contact support.", "error")
            return redirect(url_for("it_support_register_shop"))
        except Exception:
            app.logger.exception("Could not register shop")
            flash("Could not register shop. Check database connection.", "error")
            return redirect(url_for("it_support_register_shop"))

        flash("Shop registered.", "success")
        return redirect(url_for("it_support_register_shop"))

    try:
        from database import list_shops

        shops_raw = list_shops(limit=500)
    except Exception:
        shops_raw = []
    shops = []
    for s in shops_raw or []:
        row = dict(s)
        try:
            row.update(_shop_till_day_summary(row))
        except Exception:
            row.update(
                {
                    "till_state": "not_opened",
                    "till_label": "Not opened",
                    "can_open_till": True,
                    "can_close_till": False,
                }
            )
        shops.append(row)
    return render_template("it_support_register_shop.html", shops=shops)


@app.route("/it_support/shops/<int:shop_id>/day-status.json")
@login_required
def it_support_shop_day_status(shop_id: int):
    """Till day open/closed status for IT register-shop page."""
    _it_support_only()
    shop = _get_shop_or_404(shop_id)
    summary = _shop_till_day_summary(shop)
    boot = _shop_day_opening_boot_payload(shop)
    return jsonify({"ok": True, **summary, "boot": boot})


@app.route("/it_support/shops/<int:shop_id>/day-opening", methods=["POST"])
@login_required
def it_support_shop_day_opening_submit(shop_id: int):
    """Submit today's opening balances from IT register-shop (no 6-digit code)."""
    _it_support_only()
    shop = _get_shop_or_404(shop_id)
    data = request.get_json(force=True, silent=True) or {}
    emp_row, emp_err, emp_status = _resolve_it_portal_day_submitter()
    if emp_err:
        return jsonify({"ok": False, "error": emp_err}), emp_status

    try:
        opening_cash = float(
            data.get("opening_cash") if data.get("opening_cash") is not None else request.form.get("opening_cash") or 0
        )
        opening_mpesa = float(
            data.get("opening_mpesa") if data.get("opening_mpesa") is not None else request.form.get("opening_mpesa") or 0
        )
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Enter valid opening cash and M-Pesa amounts."}), 400

    stock_confirmed_raw = data.get("stock_confirmed")
    if stock_confirmed_raw is None:
        stock_confirmed_raw = request.form.get("stock_confirmed")
    stock_confirmed = stock_confirmed_raw in (True, 1, "1", "true", "yes", "on")

    ok, err, record, reopened = _apply_shop_day_opening_or_reopen(
        shop,
        shop_id,
        opening_cash=opening_cash,
        opening_mpesa=opening_mpesa,
        stock_confirmed=stock_confirmed,
        emp_row=emp_row,
    )
    if not ok:
        return jsonify({"ok": False, "error": err or "Could not save opening balances."}), 400

    summary = _shop_till_day_summary(shop)
    rec_out = None
    if record:
        rec_out = {
            "opening_cash": float(record.get("opening_cash") or 0),
            "opening_mpesa": float(record.get("opening_mpesa") or 0),
            "stock_confirmed": bool(record.get("stock_confirmed")),
            "submitted_by_name": record.get("submitted_by_name") or "",
            "submitted_at": record.get("created_at") or "",
        }
    return jsonify({"ok": True, "record": rec_out, "reopened": reopened, **summary})


@app.route("/it_support/shops/<int:shop_id>/day-closing", methods=["POST"])
@login_required
def it_support_shop_day_closing_submit(shop_id: int):
    """Submit closing balances from IT register-shop (no 6-digit code)."""
    _it_support_only()
    shop = _get_shop_or_404(shop_id)
    data = request.get_json(force=True, silent=True) or {}
    business_date_raw = (data.get("business_date") or request.form.get("business_date") or "").strip()
    if not business_date_raw:
        from database import get_pending_shop_day_closing

        today_ctx = _shop_day_closing_payload(shop, date.today())
        if today_ctx and not today_ctx.get("submitted"):
            business_date_raw = date.today().isoformat()
        else:
            pending = get_pending_shop_day_closing(shop_id)
            if not pending:
                return jsonify({"ok": False, "error": "No closing balance to submit for today or a previous day."}), 400
            business_date_raw = pending.get("business_date") or ""

    emp_row, emp_err, emp_status = _resolve_it_portal_day_submitter()
    if emp_err:
        return jsonify({"ok": False, "error": emp_err}), emp_status

    try:
        closing_cash = float(
            data.get("closing_cash") if data.get("closing_cash") is not None else request.form.get("closing_cash") or 0
        )
        closing_mpesa = float(
            data.get("closing_mpesa") if data.get("closing_mpesa") is not None else request.form.get("closing_mpesa") or 0
        )
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Enter valid closing cash and M-Pesa amounts."}), 400

    from database import save_shop_day_closing

    ok, err, record = save_shop_day_closing(
        shop_id,
        business_date_raw,
        closing_cash=closing_cash,
        closing_mpesa=closing_mpesa,
        employee_id=emp_row.get("id") if emp_row else None,
        employee_code=emp_row.get("employee_code") if emp_row else None,
        employee_name=emp_row.get("full_name") if emp_row else None,
        employee_role=emp_row.get("role") if emp_row else None,
    )
    if not ok:
        return jsonify({"ok": False, "error": err or "Could not save closing balances."}), 400

    summary = _shop_till_day_summary(shop)
    rec_out = None
    if record:
        rec_out = {
            "closing_cash": float(record.get("closing_cash") or 0),
            "closing_mpesa": float(record.get("closing_mpesa") or 0),
            "submitted_by_name": record.get("closing_submitted_by_name") or "",
            "submitted_at": record.get("closing_submitted_at") or "",
            "business_date": record.get("business_date") or "",
        }
    return jsonify({"ok": True, "record": rec_out, **summary})


@app.route("/it_support/check-shop-code")
@login_required
def it_support_check_shop_code():
    _it_support_only()
    code = (request.args.get("code") or "").strip()
    if not CODE_RE.match(code):
        return jsonify({"ok": False, "available": False, "message": "Enter a valid 6-digit code."}), 400
    try:
        from database import shop_code_available

        available = shop_code_available(code)
        return jsonify({"ok": True, "available": available})
    except Exception:
        return jsonify({"ok": False, "message": "Could not verify code availability."}), 503


@app.route("/it_support/shops/<int:shop_id>/toggle-status", methods=["POST"])
@login_required
def it_support_shop_toggle_status(shop_id: int):
    _it_support_only()
    try:
        from database import toggle_shop_status

        ok = toggle_shop_status(shop_id)
    except Exception:
        ok = False
    flash("Shop status updated." if ok else "Could not update shop status.", "success" if ok else "error")
    return redirect(url_for("it_support_register_shop"))


@app.route("/it_support/shops/<int:shop_id>/edit", methods=["POST"])
@login_required
def it_support_shop_edit(shop_id: int):
    _it_support_only()
    shop_name = (request.form.get("shop_name") or "").strip().upper()
    shop_code = (request.form.get("shop_code") or "").strip()
    shop_password = (request.form.get("shop_password") or "").strip()
    shop_location = (request.form.get("shop_location") or "").strip().upper()
    shop_location_description = (request.form.get("shop_location_description") or "").strip().upper()
    shop_phone = (request.form.get("shop_phone") or "").strip()
    status = (request.form.get("status") or "").strip().lower()

    if not shop_name or not shop_code or not shop_location:
        flash("Please fill shop name, shop code, and shop location.", "error")
        return redirect(url_for("it_support_register_shop"))
    if not CODE_RE.match(shop_code):
        flash("Shop code must be exactly 6 digits.", "error")
        return redirect(url_for("it_support_register_shop"))
    if status not in {"active", "suspended"}:
        flash("Invalid shop status.", "error")
        return redirect(url_for("it_support_register_shop"))

    try:
        from database import get_shop_by_id, update_shop_details

        shop = get_shop_by_id(shop_id)
        if not shop:
            flash("Shop not found.", "error")
            return redirect(url_for("it_support_register_shop"))

        logo_path, update_logo, logo_err = _resolve_shop_logo_upload(existing=(shop.get("shop_logo") or "").strip() or None)
        if logo_err:
            flash(logo_err, "error")
            return redirect(url_for("it_support_register_shop"))

        ok = update_shop_details(
            shop_id=shop_id,
            shop_name=shop_name,
            shop_code=shop_code,
            shop_location=shop_location,
            shop_location_description=shop_location_description or None,
            shop_phone=shop_phone or None,
            status=status,
            shop_password_hash=generate_password_hash(shop_password) if shop_password else None,
            shop_logo=logo_path,
            update_shop_logo=update_logo,
        )
    except Exception:
        app.logger.exception("Could not update shop %s", shop_id)
        ok = False

    flash("Shop details updated." if ok else "Could not update shop details.", "success" if ok else "error")
    return redirect(url_for("it_support_register_shop"))


@app.route("/it_support/shops/<int:shop_id>/delete", methods=["POST"])
@login_required
def it_support_shop_delete(shop_id: int):
    _it_support_only()
    try:
        from database import delete_shop

        ok = delete_shop(shop_id)
    except Exception:
        ok = False
    flash("Shop deleted." if ok else "Could not delete shop.", "success" if ok else "error")
    return redirect(url_for("it_support_register_shop"))


@app.route("/shops/<int:shop_id>/login", methods=["GET", "POST"])
def shop_login(shop_id: int):
    shop = _get_shop_or_404(shop_id)

    if request.method == "POST":
        password = (request.form.get("shop_password") or "").strip()
        if not password:
            flash("Enter shop password.", "error")
            return redirect(url_for("shop_login", shop_id=shop_id))

        if shop.get("status") != "active":
            flash("This shop is suspended. Contact IT support.", "error")
            return redirect(url_for("shop_login", shop_id=shop_id))

        if not check_password_hash(shop.get("shop_password_hash") or "", password):
            flash("Invalid shop password.", "error")
            return redirect(url_for("shop_login", shop_id=shop_id))

        _clear_employee_portal_session()
        session["shop_id"] = int(shop["id"])
        session["shop_name"] = shop.get("shop_name")
        flash(f"Welcome to {shop.get('shop_name')}.", "success")
        next_url = (request.form.get("next") or request.args.get("next") or "").strip()
        if _safe_login_next(next_url):
            target_sid = _parse_next_shop_id(next_url)
            if target_sid is None or target_sid == int(shop["id"]):
                return redirect(next_url)
        return redirect(url_for("shop_pos", shop_id=shop_id))

    next_url = (request.args.get("next") or "").strip()
    return render_template("shop_login.html", shop=shop, login_next=next_url if _safe_login_next(next_url) else "")


@app.route("/shops/<int:shop_id>/logout", methods=["GET", "POST"])
def shop_logout(shop_id: int):
    """End shop password session for this branch (does not clear employee portal session).

    After sign-out, users are redirected to the public shop login page (``/shop-login``).
    """
    shop = _get_shop_or_404(shop_id)
    try:
        sid = int(session.get("shop_id") or 0)
    except (TypeError, ValueError):
        sid = 0
    if sid == int(shop_id):
        session.pop("shop_id", None)
        session.pop("shop_name", None)
        flash("You have been signed out from this shop.", "success")
    return redirect(url_for("public_shop_login"), code=303)


@app.route("/shops/<int:shop_id>/profile")
def shop_profile(shop_id: int):
    """Shop profile hub: same entry as the shop dashboard for this branch."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    return redirect(url_for("shop_dashboard", shop_id=shop_id))


@app.route("/shops/<int:shop_id>")
def shop_portal(shop_id: int):
    return redirect(url_for("shop_pos", shop_id=shop_id))


def _pos_catalog_items_payload(rows: list) -> list[dict]:
    """Serialize POS catalog rows for catalog.json (stock refresh + client bootstrap)."""
    items: list[dict] = []
    for it in rows or []:
        try:
            price = float(it.get("price") or 0)
        except (TypeError, ValueError):
            price = 0.0
        try:
            orig = float(it.get("original_selling_price") or 0)
        except (TypeError, ValueError):
            orig = 0.0
        img_rel = _normalize_static_relative_path(it.get("image_path"))
        if img_rel.startswith("http://") or img_rel.startswith("https://"):
            image_url = img_rel
        elif img_rel:
            image_url = url_for("static", filename=img_rel)
        else:
            image_url = ""
        items.append(
            {
                "id": int(it.get("id") or 0),
                "category": (it.get("category") or "").strip() or "Uncategorized",
                "name": (it.get("name") or "").strip(),
                "shop_stock_qty": round(float(it.get("shop_stock_qty") or 0), 4),
                "kitchen_portions": int(it.get("kitchen_portions") or 0),
                "stock_update_enabled": int(it.get("stock_update_enabled") or 0),
                "price": round(price, 2),
                "original_selling_price": round(orig, 2),
                "image_url": image_url,
            }
        )
    return items


def _load_shop_pos_catalog_rows(shop_id: int) -> list:
    from database import ensure_shop_items_for_shop, list_shop_pos_items

    ensure_shop_items_for_shop(shop_id)
    return list_shop_pos_items(shop_id=shop_id, limit=2000)


@app.route("/shops/<int:shop_id>/shop-pos")
def shop_pos(shop_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    try:
        items = _load_shop_pos_catalog_rows(shop_id)
    except Exception:
        app.logger.exception("shop_pos catalog load failed for shop %s", shop_id)
        items = []

    inventory_mode = _pos_inventory_mode(shop)
    is_store_stock_mode = inventory_mode == "both"
    buy_items_catalog: list = []
    if is_store_stock_mode:
        try:
            from database import (
                init_store_stock_items_table,
                init_store_stock_transactions_table,
                list_store_stock_items_for_shop_buy,
            )

            init_store_stock_items_table()
            init_store_stock_transactions_table()
            buy_items_catalog = list_store_stock_items_for_shop_buy(
                shop_id=shop_id, limit=2000
            )
        except Exception:
            buy_items_catalog = []
    else:
        buy_items_catalog = items

    shop_co = _effective_company_settings_for_shop(shop)
    opening_hours = _opening_hours_boot_payload(shop)
    day_opening = _shop_day_opening_boot_payload(shop)
    return render_template(
        "shop_pos.html",
        shop=shop,
        items=items,
        buy_items_catalog=buy_items_catalog,
        is_store_stock_mode=is_store_stock_mode,
        pos_printing_settings=_effective_printing_settings_for_shop(shop),
        pos_inventory_mode=inventory_mode,
        pos_receipt_settings=_effective_receipt_settings_for_shop(shop),
        daraja_pos_boot=_daraja_pos_boot_payload(),
        shop_company_settings=shop_co,
        shop_opening_hours_status=opening_hours,
        shop_day_opening_boot=day_opening,
        **_shop_theme_template_vars(shop),
    )


@app.route("/shops/<int:shop_id>/shop-pos/day-opening/status", methods=["GET"])
def shop_pos_day_opening_status(shop_id: int):
    """Return whether today's opening balances have been submitted for this shop."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    payload = _shop_day_opening_boot_payload(shop)
    return jsonify({"ok": True, **payload})


@app.route("/shops/<int:shop_id>/shop-pos/day-opening", methods=["POST"])
def shop_pos_day_opening_submit(shop_id: int):
    """Submit daily opening cash/M-Pesa balances and stock confirmation (admin/manager once per day)."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    if not _shop_session_can_submit_day_opening():
        return jsonify({"ok": False, "error": "Only shop admin or manager can submit opening balances."}), 403

    data = request.get_json(force=True, silent=True) or {}
    employee_code = (data.get("employee_code") or request.form.get("employee_code") or "").strip()
    emp_row, emp_err, emp_status = _resolve_day_opening_submitter(shop_id, employee_code)
    if emp_err:
        return jsonify({"ok": False, "error": emp_err}), emp_status

    try:
        opening_cash = float(data.get("opening_cash") if data.get("opening_cash") is not None else request.form.get("opening_cash") or 0)
        opening_mpesa = float(data.get("opening_mpesa") if data.get("opening_mpesa") is not None else request.form.get("opening_mpesa") or 0)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Enter valid opening cash and M-Pesa amounts."}), 400

    stock_confirmed_raw = data.get("stock_confirmed")
    if stock_confirmed_raw is None:
        stock_confirmed_raw = request.form.get("stock_confirmed")
    stock_confirmed = stock_confirmed_raw in (True, 1, "1", "true", "yes", "on")

    ok, err, record, reopened = _apply_shop_day_opening_or_reopen(
        shop,
        shop_id,
        opening_cash=opening_cash,
        opening_mpesa=opening_mpesa,
        stock_confirmed=stock_confirmed,
        emp_row=emp_row,
        employee_code=employee_code,
    )
    if not ok:
        return jsonify({"ok": False, "error": err or "Could not save opening balances."}), 400

    rec_out = None
    if record:
        rec_out = {
            "opening_cash": float(record.get("opening_cash") or 0),
            "opening_mpesa": float(record.get("opening_mpesa") or 0),
            "stock_confirmed": bool(record.get("stock_confirmed")),
            "submitted_by_name": record.get("submitted_by_name") or "",
            "submitted_at": record.get("created_at") or "",
        }
    payload = _shop_day_opening_boot_payload(shop)
    return jsonify(
        {
            "ok": True,
            "completed": payload.get("completed") is True,
            "reopened": reopened,
            "ready_for_sales": payload.get("ready_for_sales"),
            "sales_allowed": payload.get("sales_allowed"),
            "sales_blocked_message": payload.get("sales_blocked_message"),
            "today_closed": payload.get("today_closed"),
            "pending_closing": payload.get("pending_closing"),
            "closing_reminder": payload.get("closing_reminder"),
            "business_date": date.today().isoformat(),
            "record": rec_out,
        }
    )


@app.route("/shops/<int:shop_id>/shop-pos/day-closing", methods=["POST"])
def shop_pos_day_closing_submit(shop_id: int):
    """Submit end-of-day closing cash/M-Pesa for a past business day (admin/manager)."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    if not _shop_session_can_submit_day_closing():
        return jsonify(
            {
                "ok": False,
                "error": "Only a manager, admin, company manager, IT support, or super admin can close the shop.",
            }
        ), 403

    data = request.get_json(force=True, silent=True) or {}
    business_date_raw = (
        data.get("business_date")
        or request.form.get("business_date")
        or ""
    ).strip()
    if not business_date_raw:
        from database import get_pending_shop_day_closing

        today_ctx = _shop_day_closing_payload(shop, date.today())
        if today_ctx and not today_ctx.get("submitted"):
            business_date_raw = date.today().isoformat()
        else:
            pending = get_pending_shop_day_closing(shop_id)
            if not pending:
                return jsonify({"ok": False, "error": "No closing balance to submit for today or a previous day."}), 400
            business_date_raw = pending.get("business_date") or ""

    employee_code = (data.get("employee_code") or request.form.get("employee_code") or "").strip()
    emp_row, emp_err, emp_status = _resolve_day_closing_submitter(shop_id, employee_code)
    if emp_err:
        return jsonify({"ok": False, "error": emp_err}), emp_status

    try:
        closing_cash = float(
            data.get("closing_cash")
            if data.get("closing_cash") is not None
            else request.form.get("closing_cash") or 0
        )
        closing_mpesa = float(
            data.get("closing_mpesa")
            if data.get("closing_mpesa") is not None
            else request.form.get("closing_mpesa") or 0
        )
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Enter valid closing cash and M-Pesa amounts."}), 400

    from database import save_shop_day_closing

    ok, err, record = save_shop_day_closing(
        shop_id,
        business_date_raw,
        closing_cash=closing_cash,
        closing_mpesa=closing_mpesa,
        employee_id=emp_row.get("id") if emp_row else None,
        employee_code=emp_row.get("employee_code") if emp_row else employee_code,
        employee_name=emp_row.get("full_name") if emp_row else None,
        employee_role=emp_row.get("role") if emp_row else None,
    )
    if not ok:
        return jsonify({"ok": False, "error": err or "Could not save closing balances."}), 400

    payload = _shop_day_opening_boot_payload(shop)
    rec_out = None
    if record:
        rec_out = {
            "closing_cash": float(record.get("closing_cash") or 0),
            "closing_mpesa": float(record.get("closing_mpesa") or 0),
            "submitted_by_name": record.get("closing_submitted_by_name") or "",
            "submitted_at": record.get("closing_submitted_at") or "",
            "business_date": record.get("business_date") or "",
        }
    return jsonify(
        {
            "ok": True,
            "record": rec_out,
            "pending_closing": payload.get("pending_closing"),
            "ready_for_sales": payload.get("ready_for_sales"),
            "sales_allowed": payload.get("sales_allowed"),
            "sales_blocked_message": payload.get("sales_blocked_message"),
            "today_closed": payload.get("today_closed"),
            "completed": payload.get("completed"),
        }
    )


@app.route("/shops/<int:shop_id>/shop-pos/refill-kitchen-portions", methods=["POST"])
def shop_pos_refill_kitchen_portions(shop_id: int):
    """Quick +qty refill of kitchen portions from the POS (kitchen/both mode only).

    Form fields: item_id, qty (positive int), employee_code (6 digits), note (optional).
    Returns JSON for AJAX callers (X-Requested-With / Accept: application/json).
    """
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    wants_json = (
        request.headers.get("X-Requested-With") == "XMLHttpRequest"
        or "application/json" in (request.headers.get("Accept") or "")
    )
    if _pos_inventory_mode(shop) not in ("kitchen", "both"):
        msg = "Kitchen portion refill is only available in kitchen/both inventory modes."
        if wants_json:
            return jsonify({"ok": False, "error": msg}), 403
        flash(msg, "error")
        return redirect(url_for("shop_pos", shop_id=shop_id))

    employee_code = (request.form.get("employee_code") or "").strip()
    emp_row, emp_err, emp_status = _shop_pos_validate_employee_code(shop_id, employee_code)
    if emp_err:
        if wants_json:
            return jsonify({"ok": False, "error": emp_err}), emp_status
        flash(emp_err, "error")
        return redirect(url_for("shop_pos", shop_id=shop_id))

    try:
        item_id = int((request.form.get("item_id") or "").strip())
    except (TypeError, ValueError):
        item_id = 0
    try:
        qty = int(float((request.form.get("qty") or "0").strip()))
    except (TypeError, ValueError):
        qty = 0

    if item_id <= 0:
        if wants_json:
            return jsonify({"ok": False, "error": "Pick a valid item to refill."}), 400
        flash("Pick a valid item to refill.", "error")
        return redirect(url_for("shop_pos", shop_id=shop_id))
    if qty <= 0:
        if wants_json:
            return jsonify({"ok": False, "error": "Portions to add must be greater than zero."}), 400
        flash("Portions to add must be greater than zero.", "error")
        return redirect(url_for("shop_pos", shop_id=shop_id))

    try:
        from database import add_shop_kitchen_portions

        ok, new_total = add_shop_kitchen_portions(shop_id=shop_id, item_id=item_id, delta=qty)
    except Exception:
        ok, new_total = False, None

    if not ok:
        if wants_json:
            return jsonify({"ok": False, "error": "Could not refill kitchen portions."}), 400
        flash("Could not refill kitchen portions.", "error")
        return redirect(url_for("shop_pos", shop_id=shop_id))

    if wants_json:
        return jsonify(
            {
                "ok": True,
                "message": f"Added {qty} portion(s).",
                "item_id": item_id,
                "added": qty,
                "portions_remaining": new_total,
            }
        )
    flash(f"Added {qty} portion(s).", "success")
    return redirect(url_for("shop_pos", shop_id=shop_id))


@app.route("/shops/<int:shop_id>/shop-pos/stock-in", methods=["POST"])
def shop_pos_stock_in(shop_id: int):
    """Quick manual stock-in from the POS page."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    wants_json = (
        request.headers.get("X-Requested-With") == "XMLHttpRequest"
        or "application/json" in (request.headers.get("Accept") or "")
    )
    if not _effective_printing_settings_for_shop(shop).get("pos_show_buy_items_link"):
        msg = "Buy items / stock-in from POS is disabled for this shop."
        if wants_json:
            return jsonify({"ok": False, "error": msg}), 403
        flash(msg, "error")
        return redirect(url_for("shop_pos", shop_id=shop_id))

    entry_type = (request.form.get("entry_type") or "stock").strip().lower()
    employee_code = (request.form.get("employee_code") or "").strip()

    emp_row, emp_err, emp_status = _shop_pos_validate_employee_code(shop_id, employee_code)
    if emp_err:
        if wants_json:
            return jsonify({"ok": False, "error": emp_err}), emp_status
        flash(emp_err, "error")
        return redirect(url_for("shop_pos", shop_id=shop_id))
    created_by_employee_id = int(emp_row["id"])

    if entry_type == "expense":
        expense_category = (request.form.get("expense_category") or "").strip().upper()
        expense_name = (request.form.get("expense_name") or "").strip().upper()
        qty_raw = (request.form.get("qty") or "").strip()
        unit_price_raw = (request.form.get("unit_price") or request.form.get("buying_price") or "").strip()
        note = (request.form.get("note") or "").strip().upper()
        seller_phone = (request.form.get("seller_phone") or "").strip()
        seller_name = (request.form.get("seller_name") or "").strip().upper()
        try:
            from database import init_operational_expense_tables, register_shop_operational_expense

            init_operational_expense_tables()
            ok, expense_id, err_msg = register_shop_operational_expense(
                shop_id=shop_id,
                category_name=expense_category,
                expense_name=expense_name,
                qty=qty_raw,
                unit_price=unit_price_raw,
                seller_phone=seller_phone,
                supplier_name=seller_name,
                note=note or None,
                created_by_employee_id=created_by_employee_id,
                payment_status="pending_payment",
            )
        except Exception:
            ok = False
            expense_id = None
            err_msg = "Could not register expense."
        if not ok:
            if wants_json:
                return jsonify({"ok": False, "error": err_msg or "Could not register expense."}), 400
            flash(err_msg or "Could not register expense.", "error")
        else:
            if wants_json:
                receipt_url = None
                if expense_id:
                    receipt_url = url_for(
                        "shop_stock_in_receipt",
                        shop_id=shop_id,
                        tx_id=expense_id,
                        kind="expense",
                        auto_print=1,
                    )
                return jsonify(
                    {
                        "ok": True,
                        "message": "Expense registered successfully.",
                        "expense_id": expense_id,
                        "entry_type": "expense",
                        "receipt_url": receipt_url,
                    }
                )
            flash("Expense registered successfully.", "success")
        return redirect(url_for("shop_pos", shop_id=shop_id))

    is_store_stock_mode = _pos_inventory_mode(shop) == "both"

    item_id_raw = (request.form.get("item_id") or "").strip()
    qty_raw = (request.form.get("qty") or "").strip()
    buying_price_raw = (request.form.get("buying_price") or "").strip()
    seller_phone = (request.form.get("seller_phone") or "").strip()
    seller_name = (request.form.get("seller_name") or "").strip().upper()
    payment_status = "pending_payment"
    note = (request.form.get("note") or "").strip().upper()

    try:
        from database import normalize_stock_move_qty

        item_id = int(item_id_raw)
        qty = normalize_stock_move_qty(qty_raw)
    except Exception:
        if wants_json:
            return jsonify({"ok": False, "error": "Invalid stock-in values."}), 400
        flash("Invalid stock-in values.", "error")
        return redirect(url_for("shop_pos", shop_id=shop_id))

    if qty is None:
        if wants_json:
            return jsonify({"ok": False, "error": "Quantity must be greater than zero."}), 400
        flash("Quantity must be greater than zero.", "error")
        return redirect(url_for("shop_pos", shop_id=shop_id))

    tx_id = None
    try:
        from database import resolve_seller_name_and_phone

        resolved_name, resolved_phone = resolve_seller_name_and_phone(
            seller_phone=seller_phone,
            seller_name=seller_name,
        )
        if not resolved_name or not resolved_phone:
            if wants_json:
                return jsonify({"ok": False, "error": "Seller phone must be valid. If new, provide seller name to register."}), 400
            flash("Seller phone must be valid. If new, provide seller name to register.", "error")
            return redirect(url_for("shop_pos", shop_id=shop_id))

        if is_store_stock_mode:
            from database import (
                get_latest_shop_manual_store_stock_in_tx_id,
                shop_manual_store_stock_in,
            )

            ok = shop_manual_store_stock_in(
                shop_id=shop_id,
                store_stock_item_id=item_id,
                qty=qty,
                buying_price=buying_price_raw,
                place_brought_from=resolved_name,
                seller_phone=resolved_phone,
                payment_status=payment_status,
                note=note or None,
                created_by_employee_id=created_by_employee_id,
            )
            if ok:
                tx_id = get_latest_shop_manual_store_stock_in_tx_id(
                    shop_id=shop_id,
                    store_stock_item_id=item_id,
                    created_by_employee_id=created_by_employee_id,
                )
        else:
            from database import (
                ensure_shop_items_for_shop,
                get_latest_shop_manual_stock_in_tx_id,
                shop_manual_stock_in,
            )

            ensure_shop_items_for_shop(shop_id)
            ok = shop_manual_stock_in(
                shop_id=shop_id,
                item_id=item_id,
                qty=qty,
                buying_price=buying_price_raw,
                place_brought_from=resolved_name,
                seller_phone=resolved_phone,
                payment_status=payment_status,
                note=note or None,
                created_by_employee_id=created_by_employee_id,
            )
            if ok:
                tx_id = get_latest_shop_manual_stock_in_tx_id(
                    shop_id=shop_id,
                    item_id=item_id,
                    created_by_employee_id=created_by_employee_id,
                )
    except Exception:
        ok = False

    if not ok:
        if wants_json:
            return jsonify({"ok": False, "error": "Could not stock in item. Check item and input details."}), 400
        flash("Could not stock in item. Check item and input details.", "error")
    else:
        if wants_json:
            receipt_url = None
            if tx_id:
                if is_store_stock_mode:
                    receipt_url = url_for(
                        "shop_stock_in_receipt",
                        shop_id=shop_id,
                        tx_id=tx_id,
                        kind="store",
                        embed=1,
                        auto_print=1,
                    )
                else:
                    receipt_url = url_for(
                        "shop_stock_in_receipt",
                        shop_id=shop_id,
                        tx_id=tx_id,
                        embed=1,
                        auto_print=1,
                    )
            return jsonify(
                {
                    "ok": True,
                    "message": "Item stocked in successfully.",
                    "tx_id": tx_id,
                    "receipt_url": receipt_url,
                }
            )
        flash("Item stocked in successfully.", "success")
    return redirect(url_for("shop_pos", shop_id=shop_id))


@app.route("/shops/<int:shop_id>/stock-in-receipt/<int:tx_id>")
def shop_stock_in_receipt(shop_id: int, tx_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    kind = (request.args.get("kind") or "").strip().lower()
    receipt_kind_label = (request.args.get("receipt_kind") or "").strip()
    try:
        if kind == "store":
            from database import get_shop_store_stock_in_receipt_row

            receipt = get_shop_store_stock_in_receipt_row(shop_id=shop_id, tx_id=tx_id)
            if not receipt_kind_label:
                receipt_kind_label = "Purchase invoice"
        elif kind == "expense":
            from database import get_shop_operational_expense_receipt_row

            receipt = get_shop_operational_expense_receipt_row(
                shop_id=shop_id,
                expense_id=tx_id,
            )
            if not receipt_kind_label:
                receipt_kind_label = "Operational expense"
        else:
            from database import get_shop_stock_in_receipt_row

            receipt = get_shop_stock_in_receipt_row(shop_id=shop_id, tx_id=tx_id)
            if not receipt_kind_label:
                receipt_kind_label = "Purchase invoice"
    except Exception:
        receipt = None
    if not receipt:
        if request.args.get("embed") in ("1", "true", "yes"):
            return ("Receipt not found.", 404)
        flash("Receipt not found.", "error")
        if kind == "expense":
            return redirect(url_for("shop_expenses", shop_id=shop_id))
        return redirect(url_for("shop_stock_management", shop_id=shop_id, view="manual"))
    embed_kind_kw = {"kind": kind} if kind in ("store", "expense") else {}
    receipt_ctx = {
        "shop": shop,
        "receipt": receipt,
        "receipt_kind": receipt_kind_label,
        "pos_receipt_settings": _effective_receipt_settings_for_shop(shop),
        "embed_print_url": url_for(
            "shop_stock_in_receipt",
            shop_id=shop_id,
            tx_id=tx_id,
            embed=1,
            auto_print=1,
            **embed_kind_kw,
        ),
    }
    use_embed = request.args.get("embed") in ("1", "true", "yes") or request.args.get("auto_print") in (
        "1",
        "true",
        "yes",
    )
    if use_embed:
        return render_template("shop_stock_in_receipt_embed.html", **receipt_ctx)
    return render_template(
        "shop_stock_in_receipt.html",
        **receipt_ctx,
        theme_key=f"richcom-theme-shop-{shop['id']}",
        theme_default=shop.get("default_theme") or "dark",
        font_family=shop.get("font_family") or "Plus Jakarta Sans",
        primary_color_rgb=_hex_to_rgb_triplet(shop.get("primary_color") or "#10b981"),
        accent_color_rgb=_hex_to_rgb_triplet(shop.get("accent_color") or "#14b8a6"),
    )


@app.route("/shops/<int:shop_id>/shop-pos/catalog.json")
def shop_pos_catalog_json(shop_id: int):
    """Catalog snapshot for POS bootstrap and live stock/price refresh."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    try:
        rows = _load_shop_pos_catalog_rows(shop_id)
    except Exception:
        rows = []
    items = _pos_catalog_items_payload(rows)
    response = jsonify(
        {
            "ok": True,
            "inventory_mode": _pos_inventory_mode(shop),
            "items": items,
        }
    )
    # Live stock + inventory_mode must not be cached (browser SW previously served stale catalog).
    response.headers["Cache-Control"] = "no-store, max-age=0"
    response.headers["Pragma"] = "no-cache"
    return response


@app.route("/shops/<int:shop_id>/shop-pos/incoming-stock-requests.json")
def shop_pos_incoming_stock_requests_json(shop_id: int):
    """Pending shop-to-shop stock requests where this shop is the source (for POS popup)."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    try:
        from database import list_incoming_pending_stock_requests_for_shop

        rows = list_incoming_pending_stock_requests_for_shop(source_shop_id=shop_id, limit=30)
    except Exception:
        rows = []
    out = []
    for r in rows or []:
        ca = r.get("created_at")
        if hasattr(ca, "isoformat"):
            ca = ca.isoformat(sep=" ", timespec="seconds")
        else:
            ca = str(ca) if ca is not None else None
        rq = round(float(r.get("qty") or 0), 4)
        src_avail = round(float(r.get("source_shop_stock_qty") or 0), 4)
        max_ap = max(0.0, min(rq, src_avail))
        out.append(
            {
                "id": int(r.get("id") or 0),
                "requesting_shop_name": (r.get("requesting_shop_name") or "").strip(),
                "item_name": (r.get("item_name") or "").strip(),
                "qty": rq,
                "requested_qty": rq,
                "source_shop_stock_qty": src_avail,
                "max_approve_qty": max_ap,
                "note": (r.get("note") or "").strip() or None,
                "created_at": ca,
            }
        )
    return jsonify({"ok": True, "requests": out})


@app.route(
    "/shops/<int:shop_id>/shop-pos/incoming-stock-requests/<int:request_id>/review",
    methods=["POST"],
)
def shop_pos_incoming_stock_request_review(shop_id: int, request_id: int):
    """Approve or decline an incoming shop-to-shop stock request from POS (6-digit employee code)."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    data = request.get_json(force=True, silent=True) or {}
    action = (data.get("action") or "").strip().lower()
    code = (data.get("employee_code") or "").strip()
    emp, err, st = _shop_pos_validate_employee_code(shop_id, code)
    if err:
        return jsonify({"ok": False, "error": err}), st
    if action not in ("approve", "reject"):
        return jsonify({"ok": False, "error": "Use action approve or reject."}), 400

    fulfill_qty = data.get("qty")
    if fulfill_qty is not None and str(fulfill_qty).strip() != "":
        from database import normalize_stock_move_qty

        fulfill_qty_norm = normalize_stock_move_qty(fulfill_qty)
        if fulfill_qty_norm is None:
            return jsonify({"ok": False, "error": "Quantity must be a positive number (decimals allowed)."}), 400
        fulfill_qty = fulfill_qty_norm
    else:
        fulfill_qty = None

    review_note = (data.get("review_note") or "").strip() or None
    delivered_by = None
    if action == "approve" and "delivered_by" in data:
        delivered_by = (data.get("delivered_by") or "").strip()[:120]
    role_key = (emp.get("role") or "employee").strip().lower()

    try:
        from database import review_stock_request

        ok, err_msg = review_stock_request(
            request_id=request_id,
            approve=(action == "approve"),
            approver_employee_id=int(emp["id"]),
            approver_role=role_key,
            approver_shop_id=int(shop_id),
            review_note=review_note,
            fulfill_qty=fulfill_qty if action == "approve" else None,
            delivered_by=delivered_by,
        )
    except Exception as e:
        logger.exception("shop_pos_incoming_stock_request_review: %s", e)
        ok, err_msg = False, "Something went wrong. Please try again."

    if not ok:
        return jsonify({"ok": False, "error": err_msg or "Could not update this request."}), 400

    receipt_url = None
    receipt_summary = None
    if action == "approve":
        receipt_url = url_for(
            "shop_stock_transfer_receipt",
            shop_id=shop_id,
            request_id=request_id,
            auto_print=1,
            embed=1,
        )
        try:
            from database import get_shop_stock_transfer_out_receipt_row

            receipt_row = get_shop_stock_transfer_out_receipt_row(
                shop_id=int(shop_id),
                stock_request_id=int(request_id),
            )
            if receipt_row:
                receipt_summary = {
                    "tx_id": int(receipt_row.get("id") or 0),
                    "request_id": int(receipt_row.get("transfer_request_id") or request_id),
                    "item_name": (receipt_row.get("item_name") or "").strip(),
                    "qty": receipt_row.get("qty"),
                    "transfer_from": (receipt_row.get("shop_name") or shop.get("shop_name") or "").strip(),
                    "transfer_to": (receipt_row.get("place_brought_from") or "").strip(),
                    "served_by": (receipt_row.get("served_by") or "").strip(),
                    "delivered_by": (receipt_row.get("delivered_by") or delivered_by or "").strip(),
                }
        except Exception:
            receipt_summary = None
        if not receipt_summary:
            try:
                from database import get_cursor

                with get_cursor() as cur:
                    cur.execute(
                        """
                        SELECT r.qty, i.name AS item_name, rq.shop_name AS requesting_shop_name
                        FROM shop_stock_requests r
                        JOIN items i ON i.id = r.item_id
                        JOIN shops rq ON rq.id = r.requesting_shop_id
                        WHERE r.id=%s
                        LIMIT 1
                        """,
                        (int(request_id),),
                    )
                    req_row = cur.fetchone()
                if req_row:
                    eff_qty = fulfill_qty if fulfill_qty is not None else req_row.get("qty")
                    receipt_summary = {
                        "tx_id": 0,
                        "request_id": int(request_id),
                        "item_name": (req_row.get("item_name") or "").strip(),
                        "qty": eff_qty,
                        "transfer_from": (shop.get("shop_name") or "").strip(),
                        "transfer_to": (req_row.get("requesting_shop_name") or "").strip(),
                        "served_by": (emp.get("full_name") or emp.get("name") or "").strip() or "—",
                        "delivered_by": (delivered_by or "").strip(),
                    }
            except Exception:
                receipt_summary = None

    return jsonify({"ok": True, "action": action, "receipt_url": receipt_url, "receipt": receipt_summary})


@app.route("/shops/<int:shop_id>/stock-transfer-receipt/<int:request_id>")
def shop_stock_transfer_receipt(shop_id: int, request_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    try:
        from database import get_shop_stock_transfer_out_receipt_row

        receipt = get_shop_stock_transfer_out_receipt_row(
            shop_id=int(shop_id),
            stock_request_id=int(request_id),
        )
    except Exception:
        receipt = None
    if not receipt:
        if request.args.get("embed") in ("1", "true", "yes"):
            return ("Transfer receipt not found.", 404)
        flash("Transfer receipt not found.", "error")
        return redirect(url_for("shop_notifications", shop_id=shop_id))
    if request.args.get("embed") in ("1", "true", "yes") or request.args.get("auto_print") in ("1", "true", "yes"):
        return render_template(
            "shop_stock_in_receipt_embed.html",
            shop=shop,
            receipt=receipt,
            receipt_kind="Stock transfer",
            pos_receipt_settings=_effective_receipt_settings_for_shop(shop),
        )
    return render_template(
        "shop_stock_in_receipt.html",
        shop=shop,
        receipt=receipt,
        receipt_kind="Stock transfer",
        pos_receipt_settings=_effective_receipt_settings_for_shop(shop),
        embed_print_url=url_for(
            "shop_stock_transfer_receipt",
            shop_id=shop_id,
            request_id=request_id,
            embed=1,
            auto_print=1,
        ),
        theme_key=f"richcom-theme-shop-{shop['id']}",
        theme_default=shop.get("default_theme") or "dark",
        font_family=shop.get("font_family") or "Plus Jakarta Sans",
        primary_color_rgb=_hex_to_rgb_triplet(shop.get("primary_color") or "#10b981"),
        accent_color_rgb=_hex_to_rgb_triplet(shop.get("accent_color") or "#14b8a6"),
    )


def _shop_pos_validate_employee_code(shop_id: int, code: str) -> Tuple[Optional[dict], Optional[str], int]:
    """
    Shared POS rule: 6-digit code, active employee, shop assignment (unless IT/super_admin).
    Returns (employee_row, None, 200) on success, or (None, error_message, http_status) on failure.
    """
    code = (code or "").strip()
    if not re.fullmatch(r"\d{6}", code):
        return None, "Enter a valid 6-digit employee code.", 400

    from database import employee_may_use_shop_branch, get_employee_by_code_for_pos_auth

    row = get_employee_by_code_for_pos_auth(code)
    if not row:
        return None, "Employee code not registered. Try another code.", 404

    if (row.get("status") or "").lower() != "active":
        return None, "Employee is not active. Enter another active employee code.", 403

    role_key = (row.get("role") or "employee").lower()
    if role_key not in COMPANY_PORTAL_ROLES and not employee_may_use_shop_branch(
        dict(row),
        int(shop_id),
    ):
        return None, "Employee is not assigned to this shop. Enter another code.", 403

    return row, None, 200


@app.route("/shops/<int:shop_id>/shop-pos/authorize-employee", methods=["POST"])
def shop_pos_authorize_employee(shop_id: int):
    """Validate 6-digit employee code for POS authorization."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate

    data = request.get_json(force=True, silent=True) or {}
    code = (data.get("employee_code") or "").strip()
    row, err, status = _shop_pos_validate_employee_code(shop_id, code)
    if err:
        return jsonify({"ok": False, "error": err}), status

    return jsonify(
        {
            "ok": True,
            "employee": {
                "id": row["id"],
                "full_name": row.get("full_name"),
                "employee_code": row.get("employee_code"),
                "role": row.get("role") or "employee",
                "status": row.get("status") or "active",
            },
        }
    )


@app.route("/shops/<int:shop_id>/shop-pos/employee-auth-cache.json", methods=["GET"])
def shop_pos_employee_auth_cache(shop_id: int):
    """Lightweight employee auth directory for offline POS code validation."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate

    from database import employee_may_use_shop_branch, get_hr_employee_shop_link_mode, list_employees_for_pos_auth

    link_mode = get_hr_employee_shop_link_mode()
    out = []
    try:
        rows = list_employees_for_pos_auth(limit=5000) or []
    except Exception:
        rows = []
    for row in rows:
        code = str((row or {}).get("employee_code") or "").strip()
        if not re.fullmatch(r"\d{6}", code):
            continue
        role_key = str((row or {}).get("role") or "employee").strip().lower()
        if role_key not in COMPANY_PORTAL_ROLES:
            try:
                if not employee_may_use_shop_branch(
                    dict(row), int(shop_id), link_mode=link_mode
                ):
                    continue
            except Exception:
                continue
        out.append(
            {
                "id": row.get("id"),
                "full_name": row.get("full_name"),
                "employee_code": code,
                "role": row.get("role") or "employee",
                "status": "active",
            }
        )
    return jsonify({"ok": True, "employees": out, "shop_id": int(shop_id)})


@app.route("/shops/<int:shop_id>/shop-pos/customer-lookup", methods=["POST"])
def shop_pos_customer_lookup(shop_id: int):
    """Lookup POS customer by phone for this shop."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate

    data = request.get_json(force=True, silent=True) or {}
    phone = (data.get("phone") or "").strip()
    if len(re.sub(r"\D", "", phone)) < 7:
        return jsonify({"ok": False, "error": "Enter a valid phone number."}), 400

    from database import get_shop_customer_by_phone

    row = get_shop_customer_by_phone(shop_id, phone)
    if not row:
        return jsonify({"ok": True, "customer": None})
    return jsonify(
        {
            "ok": True,
            "customer": {
                "id": row["id"],
                "customer_name": row["customer_name"],
                "phone": row["phone"],
            },
        }
    )


@app.route("/sellers/lookup", methods=["POST"])
def seller_lookup():
    """Lookup registered seller by phone for stock-in forms."""
    if not session.get("employee_id") and not session.get("shop_id"):
        return jsonify({"ok": False, "error": "Authentication required."}), 401
    phone = (request.form.get("seller_phone") or "").strip()
    try:
        from database import get_seller_by_phone

        row = get_seller_by_phone(phone)
    except Exception:
        row = None
    if not row:
        return jsonify({"ok": True, "registered": False, "seller": None})
    return jsonify(
        {
            "ok": True,
            "registered": True,
            "seller": {
                "id": int(row.get("id") or 0),
                "seller_name": (row.get("seller_name") or "").strip(),
                "phone": (row.get("phone") or "").strip(),
            },
        }
    )


@app.route("/sellers/suggest", methods=["GET"])
def seller_suggest():
    """Live seller-name suggestions by prefix."""
    if not session.get("employee_id") and not session.get("shop_id"):
        return jsonify({"ok": False, "error": "Authentication required.", "suggestions": []}), 401
    q = (request.args.get("q") or "").strip()
    if len(q) < 1:
        return jsonify({"ok": True, "suggestions": []})
    try:
        from database import search_seller_names

        suggestions = search_seller_names(q, limit=10)
    except Exception:
        suggestions = []
    return jsonify({"ok": True, "suggestions": suggestions})


@app.route("/shops/<int:shop_id>/shop-pos/customer-upsert", methods=["POST"])
def shop_pos_customer_upsert(shop_id: int):
    """Create or update POS customer by phone for this shop."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate

    data = request.get_json(force=True, silent=True) or {}
    phone = (data.get("phone") or "").strip()
    name = (data.get("customer_name") or "").strip()
    if len(re.sub(r"\D", "", phone)) < 7:
        return jsonify({"ok": False, "error": "Enter a valid phone number."}), 400
    if len(name) < 2:
        return jsonify({"ok": False, "error": "Enter customer name."}), 400

    from database import get_shop_customer_by_phone, upsert_shop_customer

    if not upsert_shop_customer(shop_id, name, phone):
        return jsonify({"ok": False, "error": "Could not save customer."}), 500
    row = get_shop_customer_by_phone(shop_id, phone)
    return jsonify(
        {
            "ok": True,
            "customer": {
                "id": row["id"] if row else None,
                "customer_name": (row or {}).get("customer_name", name),
                "phone": (row or {}).get("phone", phone),
            },
        }
    )


@app.route("/api/daraja/mpesa-callback", methods=["POST"])
def daraja_mpesa_stk_callback():
    """Safaricom STK Push result callback (Body STKCallback JSON)."""
    data = request.get_json(force=True, silent=True) or {}
    body = data.get("Body") if isinstance(data.get("Body"), dict) else {}
    stk = body.get("stkCallback") if isinstance(body.get("stkCallback"), dict) else {}
    checkout_id = str(stk.get("CheckoutRequestID") or "").strip()
    result_code = stk.get("ResultCode")
    result_desc = str(stk.get("ResultDesc") or "").strip()
    metadata = {}
    for item in stk.get("CallbackMetadata", {}).get("Item", []) or []:
        if not isinstance(item, dict):
            continue
        name = str(item.get("Name") or "").strip()
        if name:
            metadata[name] = item.get("Value")
    receipt_no = ""
    for key in ("MpesaReceiptNumber", "mpesa_receipt_number", "ReceiptNumber"):
        val = metadata.get(key)
        if val is not None and str(val).strip():
            receipt_no = str(val).strip()
            break
    from daraja_api import extract_mpesa_payer_from_stk_metadata

    mpesa_payer_name, mpesa_payer_phone = extract_mpesa_payer_from_stk_metadata(metadata, "")
    if checkout_id:
        _mpesa_stk_status_store(
            checkout_id,
            {
                "checkout_request_id": checkout_id,
                "merchant_request_id": str(stk.get("MerchantRequestID") or "").strip(),
                "result_code": result_code,
                "result_desc": result_desc,
                "metadata": metadata,
                "mpesa_receipt_number": receipt_no or None,
                "mpesa_payer_name": mpesa_payer_name or None,
                "mpesa_payer_phone": mpesa_payer_phone or None,
                "completed": True,
                "pending": False,
            },
        )
        if receipt_no:
            logger.info("M-Pesa STK callback stored receipt %s for %s", receipt_no, checkout_id)
    return jsonify({"ResultCode": 0, "ResultDesc": "Accepted"})


def _daraja_balance_callback_payload() -> dict:
    data = request.get_json(force=True, silent=True)
    if isinstance(data, dict) and data:
        return data
    raw = (request.get_data(as_text=True) or "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except (json.JSONDecodeError, TypeError):
        logger.warning("Daraja balance callback non-JSON body: %s", raw[:500])
        return {}


@app.route("/api/daraja/account-balance-result", methods=["GET", "POST"])
def daraja_account_balance_result_callback():
    """Safaricom AccountBalance ResultURL callback."""
    if request.method == "GET":
        return jsonify(
            {
                "ok": True,
                "message": "Balance ResultURL endpoint is reachable. Safaricom uses POST.",
            }
        )
    data = _daraja_balance_callback_payload()
    logger.info("Daraja balance result callback received (%s bytes)", len(request.get_data() or b""))
    parsed = {}
    try:
        from daraja_api import parse_account_balance_callback

        parsed = parse_account_balance_callback(data)
    except Exception:
        logger.exception("Daraja balance callback parse failed")
    cid = str(parsed.get("conversation_id") or "").strip()
    orig = str(parsed.get("originator_conversation_id") or "").strip()
    for key in (cid, orig):
        if key:
            _daraja_balance_status_store(key, parsed)
    if cid or orig:
        logger.info(
            "M-Pesa balance callback conv=%s orig=%s code=%s balance=%s",
            cid or "—",
            orig or "—",
            parsed.get("result_code"),
            parsed.get("balance"),
        )
    return jsonify({"ResultCode": 0, "ResultDesc": "Accepted"})


@app.route("/api/daraja/account-balance-timeout", methods=["GET", "POST"])
def daraja_account_balance_timeout_callback():
    """Safaricom AccountBalance QueueTimeOutURL callback."""
    if request.method == "GET":
        return jsonify(
            {
                "ok": True,
                "message": "Balance timeout URL endpoint is reachable. Safaricom uses POST.",
            }
        )
    data = _daraja_balance_callback_payload()
    result = data.get("Result") if isinstance(data.get("Result"), dict) else data
    ids = []
    for raw in (
        result.get("ConversationID"),
        result.get("OriginatorConversationID"),
    ):
        cid = str(raw or "").strip()
        if cid and cid not in ids:
            ids.append(cid)
    payload = {
        "completed": True,
        "pending": False,
        "timed_out": True,
        "result_code": -1,
        "result_desc": str(result.get("ResultDesc") or "Balance request timed out."),
    }
    for cid in ids:
        _daraja_balance_status_store(cid, {**payload, "conversation_id": cid})
    if ids:
        logger.info("M-Pesa balance timeout callback for %s", ", ".join(ids))
    return jsonify({"ResultCode": 0, "ResultDesc": "Accepted"})


@app.route("/api/daraja/b2c-result", methods=["GET", "POST"])
def daraja_b2c_result_callback():
    """Safaricom B2C ResultURL callback."""
    if request.method == "GET":
        return jsonify(
            {
                "ok": True,
                "message": "B2C ResultURL endpoint is reachable. Safaricom uses POST.",
            }
        )
    data = _daraja_balance_callback_payload()
    logger.info("Daraja B2C result callback received (%s bytes)", len(request.get_data() or b""))
    parsed = {}
    try:
        from daraja_api import parse_b2c_callback

        parsed = parse_b2c_callback(data)
    except Exception:
        logger.exception("Daraja B2C callback parse failed")
    cid = str(parsed.get("conversation_id") or "").strip()
    orig = str(parsed.get("originator_conversation_id") or "").strip()
    for key in (cid, orig):
        if not key:
            continue
        prev = _daraja_b2c_status_get(key) or {}
        merged = {**prev, **parsed, "conversation_id": key}
        if int(parsed.get("result_code") or -1) == 0 and not prev.get("payment_applied"):
            if _daraja_b2c_apply_expense_payment({**prev, **merged}):
                merged["payment_applied"] = True
        _daraja_b2c_status_store(key, merged)
    if cid or orig:
        logger.info(
            "M-Pesa B2C callback conv=%s orig=%s code=%s receipt=%s",
            cid or "—",
            orig or "—",
            parsed.get("result_code"),
            parsed.get("transaction_receipt"),
        )
    return jsonify({"ResultCode": 0, "ResultDesc": "Accepted"})


@app.route("/api/daraja/b2c-timeout", methods=["GET", "POST"])
def daraja_b2c_timeout_callback():
    """Safaricom B2C QueueTimeOutURL callback."""
    if request.method == "GET":
        return jsonify(
            {
                "ok": True,
                "message": "B2C timeout URL endpoint is reachable. Safaricom uses POST.",
            }
        )
    data = _daraja_balance_callback_payload()
    result = data.get("Result") if isinstance(data.get("Result"), dict) else data
    ids = []
    for raw in (
        result.get("ConversationID"),
        result.get("OriginatorConversationID"),
    ):
        cid = str(raw or "").strip()
        if cid and cid not in ids:
            ids.append(cid)
    payload = {
        "completed": True,
        "pending": False,
        "timed_out": True,
        "result_code": -1,
        "result_desc": str(result.get("ResultDesc") or "B2C payment timed out."),
    }
    for cid in ids:
        _daraja_b2c_status_store(cid, {**payload, "conversation_id": cid})
    if ids:
        logger.info("M-Pesa B2C timeout callback for %s", ", ".join(ids))
    return jsonify({"ResultCode": 0, "ResultDesc": "Accepted"})


@app.route("/shops/<int:shop_id>/shop-pos/mpesa-stk-push", methods=["POST"])
def shop_pos_mpesa_stk_push(shop_id: int):
    """Send Lipa Na M-Pesa STK Push to the customer phone."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    hours_block = _shop_pos_sales_hours_block(shop)
    if hours_block is not None:
        return hours_block

    data = request.get_json(force=True, silent=True) or {}
    phone = (data.get("phone") or "").strip()
    try:
        amount = float(data.get("amount") or 0)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Invalid M-Pesa amount."}), 400

    settings = _load_daraja_settings()
    from daraja_api import DarajaApiError, initiate_stk_push, normalize_msisdn

    if not settings.get("daraja_enabled"):
        return jsonify({"ok": False, "error": "Daraja M-Pesa is not enabled. Configure it in Company settings."}), 403

    account_ref = settings.get("daraja_account_reference") or shop.get("shop_code") or shop.get("shop_name") or "POS"
    account_ref = str(account_ref or "POS").strip()[:12]
    callback_url = _daraja_external_callback_hint()
    try:
        result = initiate_stk_push(
            settings,
            phone=phone,
            amount=amount,
            callback_url=callback_url,
            account_reference=account_ref,
            transaction_desc=f"{shop.get('shop_name') or 'POS'} sale"[:13],
        )
    except DarajaApiError as exc:
        logger.warning("STK Push rejected for shop %s: %s", shop_id, exc)
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception:
        logger.exception("STK Push failed for shop %s", shop_id)
        return jsonify({"ok": False, "error": "Could not send STK Push. Try again."}), 500

    checkout_id = str(result.get("CheckoutRequestID") or "").strip()
    merchant_id = str(result.get("MerchantRequestID") or "").strip()
    if checkout_id:
        _mpesa_stk_status_store(
            checkout_id,
            {
                "checkout_request_id": checkout_id,
                "merchant_request_id": merchant_id,
                "shop_id": shop_id,
                "phone": normalize_msisdn(phone),
                "amount": round(amount, 2),
                "result_code": None,
                "result_desc": str(result.get("CustomerMessage") or result.get("ResponseDescription") or "").strip(),
                "completed": False,
            },
        )
    return jsonify(
        {
            "ok": True,
            "checkout_request_id": checkout_id,
            "merchant_request_id": merchant_id,
            "customer_message": str(
                result.get("CustomerMessage") or result.get("ResponseDescription") or "STK Push sent."
            ).strip(),
            "phone": normalize_msisdn(phone),
            "amount": round(amount, 2),
        }
    )


def _mpesa_stk_refresh_from_daraja_query(row: dict) -> dict:
    """Poll Safaricom STK Query API when callback has not arrived (e.g. localhost)."""
    if not row:
        return row
    from daraja_api import (
        DarajaApiError,
        _stk_query_payload_pending,
        _stk_result_desc_is_pending,
        query_stk_push,
    )

    if row.get("completed"):
        desc = str(row.get("result_desc") or "")
        if _stk_result_desc_is_pending(desc):
            row = {**row, "completed": False, "pending": True}
        else:
            return row

    settings = _load_daraja_settings()
    if not settings.get("daraja_enabled"):
        return row
    checkout_id = str(row.get("checkout_request_id") or "").strip()
    if not checkout_id:
        return row
    try:
        q = query_stk_push(settings, checkout_id)
    except DarajaApiError:
        logger.debug("STK query pending/failed for %s", checkout_id, exc_info=True)
        return row
    if _stk_query_payload_pending(q):
        merged = {
            **row,
            "pending": True,
            "completed": False,
            "result_desc": q.get("ResultDesc")
            or q.get("result_desc")
            or "Waiting for M-Pesa payment on phone…",
            "queried_via_daraja": True,
        }
        _mpesa_stk_status_store(checkout_id, merged)
        return merged
    patch = {
        "result_code": q.get("result_code", q.get("ResultCode")),
        "result_desc": q.get("result_desc") or q.get("ResultDesc") or "",
        "completed": True,
        "pending": False,
        "queried_via_daraja": True,
    }
    merged = {**row, **{k: v for k, v in patch.items() if v is not None}}
    _mpesa_stk_status_store(checkout_id, merged)
    return merged


def _mpesa_stk_status_pending(row: dict, paid: bool) -> bool:
    if paid or not row:
        return False
    if row.get("pending"):
        return True
    if not row.get("completed"):
        return True
    return _mpesa_stk_result_desc_is_pending(str(row.get("result_desc") or ""))


def _mpesa_stk_result_desc_is_pending(desc: str) -> bool:
    from daraja_api import _stk_result_desc_is_pending

    return _stk_result_desc_is_pending(desc)


@app.route("/shops/<int:shop_id>/shop-pos/mpesa-stk-status/<checkout_request_id>", methods=["GET"])
def shop_pos_mpesa_stk_status(shop_id: int, checkout_request_id: str):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    row = _mpesa_stk_status_get(checkout_request_id)
    if not row:
        return jsonify({"ok": False, "error": "STK request not found."}), 404
    if int(row.get("shop_id") or 0) not in (0, shop_id):
        return jsonify({"ok": False, "error": "STK request not found."}), 404
    row = _mpesa_stk_refresh_from_daraja_query(row)
    paid = str(row.get("result_code")) == "0"
    pending = _mpesa_stk_status_pending(row, paid)
    failed = bool(row.get("completed")) and not paid and not pending
    receipt_no = _mpesa_stk_receipt_number(row) if paid else ""
    from daraja_api import humanize_stk_result

    settings = _load_daraja_settings()
    status_message = humanize_stk_result(
        row.get("result_code"),
        str(row.get("result_desc") or ""),
        environment=settings.get("daraja_environment") or "sandbox",
    )
    customer = None
    auto_registered_customer = False
    mpesa_phone = ""
    mpesa_payer_name = ""
    if paid:
        mpesa_payer_name, mpesa_phone = _stk_row_mpesa_payer(row)
        customer, auto_registered_customer = _maybe_auto_register_shop_customer_from_mpesa_stk(
            shop_id, row
        )
    return jsonify(
        {
            "ok": True,
            "status": row,
            "status_message": status_message,
            "paid": paid,
            "pending": pending,
            "failed": failed,
            "mpesa_receipt_number": receipt_no,
            "mpesa_phone": mpesa_phone or None,
            "mpesa_payer_name": mpesa_payer_name or None,
            "customer": customer,
            "auto_registered_customer": auto_registered_customer,
        }
    )


@app.route("/pay/credit/<token>")
def credit_pay_public(token: str):
    """Public M-Pesa credit payment page (from WhatsApp link)."""
    try:
        ctx = _parse_credit_pay_token(token)
    except ValueError as exc:
        return render_template("credit_pay_public.html", error=str(exc)), 400
    company_pay = int(ctx["shop_id"] or 0) == 0
    if company_pay:
        shop = {"id": 0, "shop_name": "All shops", "shop_code": ""}
    else:
        shop = _get_shop_or_404(ctx["shop_id"])
        if not _shop_pos_allow_credit_sale(shop):
            return render_template(
                "credit_pay_public.html",
                error="Credit payments are not enabled for this shop.",
            ), 403
    settings = _load_daraja_settings()
    from daraja_api import daraja_settings_ready

    if not daraja_settings_ready(settings):
        return render_template(
            "credit_pay_public.html",
            error="M-Pesa payments are not configured. Contact the shop.",
        ), 503
    suggested = float(ctx["max_amount"] or 0)
    amount_raw = (request.args.get("amount") or "").strip()
    phone_raw = (request.args.get("phone") or "").strip()
    try:
        amount = float(amount_raw) if amount_raw else suggested
    except (TypeError, ValueError):
        amount = suggested
    amount = max(0.0, min(amount, suggested))
    phone = phone_raw or ctx["customer_phone"]
    if phone == "-":
        phone = ""
    company_name = "Point of Sale"
    try:
        from database import get_site_settings

        company_name = (
            get_site_settings(["company_name"]).get("company_name") or company_name
        )
    except Exception:
        pass
    credit_ctx = (
        _company_customer_credit_note_context(ctx["customer_name"], ctx["customer_phone"])
        if company_pay
        else _shop_customer_credit_note_context(
            ctx["shop_id"],
            ctx["customer_name"],
            ctx["customer_phone"],
        )
    )
    balance_due = float(credit_ctx.get("account_balance_due") or 0)
    payment_max = round(min(suggested, balance_due) if balance_due > 0 else suggested, 2)
    if payment_max < 0.01:
        payment_max = suggested
    default_amt = round(amount, 2) if amount >= 0.01 else payment_max
    default_amt = max(0.0, min(default_amt, payment_max))
    return render_template(
        "credit_pay_public.html",
        shop=shop,
        pay_ctx=ctx,
        pay_token=ctx["token"],
        suggested_amount=payment_max,
        default_amount=default_amt,
        default_phone=phone,
        company_name=company_name,
        customer_name=ctx["customer_name"],
        customer_phone=ctx["customer_phone"],
        public_credit_pay=True,
        **credit_ctx,
    )


@app.route("/share/credit-note/<token>.pdf")
def credit_note_public_share_pdf(token: str):
    """Public credit note PDF (signed link for WhatsApp share — no login)."""
    try:
        ctx = _parse_credit_note_share_token(token)
    except ValueError as exc:
        abort(400, description=str(exc))
    company_scope = bool(ctx.get("company_scope"))
    shop_id = int(ctx.get("shop_id") or 0)
    customer_name = ctx["customer_name"]
    customer_phone = ctx["customer_phone"]
    company_name = "Company"
    primary_color = ""
    try:
        from database import get_site_settings

        stored = get_site_settings(["company_name", "primary_color"]) or {}
        company_name = (stored.get("company_name") or "").strip() or company_name
        primary_color = (stored.get("primary_color") or "").strip()
    except Exception:
        pass
    slug = secure_filename(
        f"credit-note-{(customer_name or 'customer').replace(' ', '-').lower()}"
    )
    if company_scope:
        note_ctx = _company_customer_credit_note_context(customer_name, customer_phone)
        return _credit_note_pdf_response(
            shop_name="All shops",
            company_name=company_name,
            customer_name=customer_name,
            customer_phone=customer_phone,
            note_ctx=note_ctx,
            company_scope=True,
            primary_color=primary_color or None,
            filename_slug=slug or "credit-note",
        )
    shop = _get_shop_or_404(shop_id)
    note_ctx = _shop_customer_credit_note_context(
        shop_id, customer_name, customer_phone, analytics_filter=None
    )
    return _credit_note_pdf_response(
        shop_name=shop.get("shop_name") or f"Shop {shop_id}",
        company_name=company_name,
        customer_name=customer_name,
        customer_phone=customer_phone,
        note_ctx=note_ctx,
        primary_color=primary_color or (shop.get("primary_color") or "").strip() or None,
        filename_slug=slug or "credit-note",
    )


@app.route("/api/pay/credit/<token>/stk-push", methods=["POST"])
def credit_pay_public_stk_push(token: str):
    """Initiate STK Push from a signed credit payment link."""
    try:
        ctx = _parse_credit_pay_token(token)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    company_pay = int(ctx["shop_id"] or 0) == 0
    if company_pay:
        shop = {"id": 0, "shop_name": "All shops", "shop_code": ""}
    else:
        shop = _get_shop_or_404(ctx["shop_id"])
        if not _shop_pos_allow_credit_sale(shop):
            return jsonify({"ok": False, "error": "Credit payments are disabled."}), 403

    data = request.get_json(force=True, silent=True) or {}
    phone = (data.get("phone") or "").strip()
    try:
        amount = float(data.get("amount") or 0)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Invalid amount."}), 400

    max_amt = float(ctx["max_amount"] or 0)
    if amount < 1:
        return jsonify({"ok": False, "error": "Amount must be at least 1."}), 400
    if amount > max_amt + 0.02:
        return jsonify(
            {
                "ok": False,
                "error": f"Amount cannot exceed {max_amt:.2f} on this link.",
            }
        ), 400

    settings = _load_daraja_settings()
    from daraja_api import DarajaApiError, initiate_stk_push, normalize_msisdn

    if not settings.get("daraja_enabled"):
        return jsonify({"ok": False, "error": "M-Pesa is not enabled."}), 403

    account_ref = (
        settings.get("daraja_account_reference") or shop.get("shop_code") or "CREDIT"
    )
    account_ref = str(account_ref or "CREDIT").strip()[:12]
    callback_url = _daraja_external_callback_hint()
    shop_id = int(ctx["shop_id"])
    txn_desc = (
        "Credit"
        if company_pay
        else f"{shop.get('shop_name') or 'Credit'}"[:13]
    )
    try:
        result = initiate_stk_push(
            settings,
            phone=phone,
            amount=amount,
            callback_url=callback_url,
            account_reference=account_ref,
            transaction_desc=txn_desc,
        )
    except DarajaApiError as exc:
        logger.warning("Credit pay STK rejected shop %s: %s", shop_id, exc)
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception:
        logger.exception("Credit pay STK failed shop %s", shop_id)
        return jsonify({"ok": False, "error": "Could not send STK Push."}), 500

    checkout_id = str(result.get("CheckoutRequestID") or "").strip()
    merchant_id = str(result.get("MerchantRequestID") or "").strip()
    if checkout_id:
        _mpesa_stk_status_store(
            checkout_id,
            {
                "checkout_request_id": checkout_id,
                "merchant_request_id": merchant_id,
                "shop_id": shop_id,
                "phone": normalize_msisdn(phone),
                "amount": round(amount, 2),
                "result_code": None,
                "result_desc": str(
                    result.get("CustomerMessage") or result.get("ResponseDescription") or ""
                ).strip(),
                "completed": False,
                "credit_pay": True,
                "credit_pay_token": ctx["token"],
                "customer_name": ctx["customer_name"],
                "customer_phone": ctx["customer_phone"],
            },
        )
    return jsonify(
        {
            "ok": True,
            "checkout_request_id": checkout_id,
            "customer_message": str(
                result.get("CustomerMessage")
                or result.get("ResponseDescription")
                or "STK Push sent."
            ).strip(),
        }
    )


@app.route("/api/pay/credit/<token>/stk-status/<checkout_request_id>", methods=["GET"])
def credit_pay_public_stk_status(token: str, checkout_request_id: str):
    """Poll STK status for a credit payment link (no login)."""
    try:
        ctx = _parse_credit_pay_token(token)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    row = _mpesa_stk_status_get(checkout_request_id)
    if not row:
        return jsonify({"ok": False, "error": "Payment request not found."}), 404
    if str(row.get("credit_pay_token") or "") != ctx["token"]:
        return jsonify({"ok": False, "error": "Payment request not found."}), 404
    if int(row.get("shop_id") or 0) != int(ctx["shop_id"]):
        return jsonify({"ok": False, "error": "Payment request not found."}), 404
    row = _mpesa_stk_refresh_from_daraja_query(row)
    paid = str(row.get("result_code")) == "0"
    pending = _mpesa_stk_status_pending(row, paid)
    failed = bool(row.get("completed")) and not paid and not pending
    receipt_no = _mpesa_stk_receipt_number(row) if paid else ""
    recorded = bool(row.get("credit_payment_recorded"))
    from daraja_api import humanize_stk_result

    settings = _load_daraja_settings()
    status_message = humanize_stk_result(
        row.get("result_code"),
        str(row.get("result_desc") or ""),
        environment=settings.get("daraja_environment") or "sandbox",
    )
    customer = None
    auto_registered_customer = False
    mpesa_phone = ""
    mpesa_payer_name = ""
    if paid:
        mpesa_payer_name, mpesa_phone = _stk_row_mpesa_payer(row)
        customer, auto_registered_customer = _maybe_auto_register_shop_customer_from_mpesa_stk(
            int(ctx["shop_id"]), row
        )
    return jsonify(
        {
            "ok": True,
            "paid": paid,
            "pending": pending,
            "failed": failed,
            "recorded": recorded,
            "mpesa_receipt_number": receipt_no,
            "status": row,
            "status_message": status_message,
            "mpesa_phone": mpesa_phone or None,
            "mpesa_payer_name": mpesa_payer_name or None,
            "customer": customer,
            "auto_registered_customer": auto_registered_customer,
        }
    )


@app.route("/api/pay/credit/<token>/record", methods=["POST"])
def credit_pay_public_record(token: str):
    """Apply FIFO credit payment after successful STK on a public pay link."""
    try:
        ctx = _parse_credit_pay_token(token)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400

    data = request.get_json(force=True, silent=True) or {}
    checkout_id = str(data.get("checkout_request_id") or "").strip()
    if not checkout_id:
        return jsonify({"ok": False, "error": "Checkout request ID is required."}), 400

    row = _mpesa_stk_status_get(checkout_id)
    if not row or str(row.get("credit_pay_token") or "") != ctx["token"]:
        return jsonify({"ok": False, "error": "Payment request not found."}), 404

    if row.get("credit_payment_recorded"):
        return jsonify({"ok": True, "already_recorded": True, "message": "Payment already recorded."})

    row = _mpesa_stk_refresh_from_daraja_query(row)
    paid = str(row.get("result_code")) == "0"
    if not paid:
        return jsonify({"ok": False, "error": "M-Pesa payment is not confirmed yet."}), 400

    amount = float(row.get("amount") or 0)
    receipt_no = _mpesa_stk_receipt_number(row)
    note = f"M-Pesa Ref: {receipt_no}" if receipt_no else "M-Pesa STK (pay link)"

    try:
        from database import apply_company_credit_payment_fifo, apply_shop_credit_payment_fifo

        if int(ctx["shop_id"] or 0) == 0:
            res = apply_company_credit_payment_fifo(
                customer_name=ctx["customer_name"],
                customer_phone=ctx["customer_phone"],
                amount=amount,
                payment_method="mpesa",
            )
        else:
            res = apply_shop_credit_payment_fifo(
                shop_id=int(ctx["shop_id"]),
                customer_name=ctx["customer_name"],
                customer_phone=ctx["customer_phone"],
                amount=amount,
                note=note,
            )
    except Exception:
        logger.exception("Credit pay record failed for %s", checkout_id)
        return jsonify({"ok": False, "error": "Could not record payment."}), 500

    if not res.get("ok"):
        return jsonify({"ok": False, "error": res.get("error") or "Could not record payment."}), 400

    _mpesa_stk_status_store(
        checkout_id,
        {**row, "credit_payment_recorded": True, "credit_payment_note": note},
    )
    return jsonify(
        {
            "ok": True,
            "message": "Payment recorded. Thank you!",
            "mpesa_receipt_number": receipt_no,
            "allocated": res.get("allocated") or [],
        }
    )


@app.route("/shops/<int:shop_id>/shop-pos/record-sale", methods=["POST"])
def shop_pos_record_sale(shop_id: int):
    """Record POS sale/credit checkout for analytics."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate

    data = request.get_json(force=True, silent=True) or {}
    client_txn_id = (data.get("client_txn_id") or "").strip()[:64] or None
    # Offline-queued sales replay with this flag / timestamp; do not block on compulsory printer (sale already completed offline).
    offline_queue_replay = bool(data.get("offline_queue_sync")) or bool(
        str(data.get("queued_at") or "").strip()
    )
    if not offline_queue_replay:
        hours_block = _shop_pos_sales_hours_block(shop)
        if hours_block is not None:
            return hours_block

    sale_type = (data.get("sale_type") or "sale").strip().lower()
    if sale_type not in ("sale", "credit"):
        return jsonify({"ok": False, "error": "Invalid sale type."}), 400

    if sale_type == "sale" and not _shop_pos_allow_cash_sale(shop):
        return jsonify({"ok": False, "error": "Standard sales are disabled for this POS."}), 403
    if sale_type == "credit" and not _shop_pos_allow_credit_sale(shop):
        return jsonify({"ok": False, "error": "Credit sales are disabled for this POS."}), 403

    try:
        total_amount = float(data.get("total_amount") or 0)
    except Exception:
        return jsonify({"ok": False, "error": "Invalid total amount."}), 400
    if total_amount < 0:
        return jsonify({"ok": False, "error": "Invalid total amount."}), 400

    try:
        item_count = float(data.get("item_count") or 0)
    except Exception:
        return jsonify({"ok": False, "error": "Invalid item count."}), 400
    if item_count < 0:
        item_count = 0.0

    emp = data.get("employee") or {}
    credit_due_raw = (data.get("credit_due_date") or "").strip() or None
    if sale_type != "credit":
        credit_due_raw = None
    payment_method = (data.get("payment_method") or "").strip().lower()
    if sale_type == "credit":
        payment_method = "credit"
    elif payment_method not in ("cash", "mpesa", "both"):
        return jsonify({"ok": False, "error": "Select a valid payment method."}), 400
    if sale_type == "sale" and not _shop_pos_payment_method_allowed(shop, payment_method):
        return jsonify({"ok": False, "error": "This payment option is not enabled for this POS."}), 403
    if sale_type == "sale" and _shop_print_compulsory_on_sale_enabled(shop) and not offline_queue_replay:
        gate_ok, gate_err = _shop_compulsory_printer_record_sale_gate(shop_id)
        if not gate_ok:
            return jsonify({"ok": False, "error": gate_err}), 403
    try:
        cash_amount = float(data.get("cash_amount") or 0)
    except Exception:
        return jsonify({"ok": False, "error": "Invalid cash amount."}), 400
    try:
        mpesa_amount = float(data.get("mpesa_amount") or 0)
    except Exception:
        return jsonify({"ok": False, "error": "Invalid M-Pesa amount."}), 400
    if cash_amount < 0 or mpesa_amount < 0:
        return jsonify({"ok": False, "error": "Payment amounts cannot be negative."}), 400
    if sale_type != "credit":
        if payment_method == "cash":
            cash_amount = total_amount
            mpesa_amount = 0.0
        elif payment_method == "mpesa":
            cash_amount = 0.0
            mpesa_amount = total_amount
        elif abs(round(cash_amount + mpesa_amount, 2) - round(total_amount, 2)) > 0.01:
            return jsonify({"ok": False, "error": "Cash + M-Pesa must equal the total amount."}), 400
    else:
        cash_amount = 0.0
        mpesa_amount = 0.0
    # Optional: finalizing a withhold-POS held order. When set, the sale receipt is recorded but
    # stock movement is skipped (already deducted incrementally by /shop-pos/hold/save).
    held_order_id_raw = data.get("held_order_id")
    held_order_id: Optional[int] = None
    if held_order_id_raw not in (None, "", 0, "0"):
        try:
            held_order_id = int(held_order_id_raw)
            if held_order_id <= 0:
                held_order_id = None
        except (TypeError, ValueError):
            held_order_id = None
    if held_order_id is not None and sale_type != "sale":
        return jsonify({"ok": False, "error": "Held orders are finalized through standard sale only."}), 400
    mpesa_receipt_number = (data.get("mpesa_receipt_number") or "").strip()
    if not mpesa_receipt_number:
        stk_checkout_id = (data.get("mpesa_checkout_request_id") or "").strip()
        if stk_checkout_id:
            stk_row = _mpesa_stk_status_get(stk_checkout_id)
            if stk_row and int(stk_row.get("shop_id") or 0) in (0, shop_id):
                mpesa_receipt_number = _mpesa_stk_receipt_number(stk_row)
    try:
        from database import create_shop_pos_sale

        ok, sale_err, sale_id, receipt_number = create_shop_pos_sale(
            shop_id=shop_id,
            sale_type=sale_type,
            payment_method=payment_method,
            cash_amount=cash_amount,
            mpesa_amount=mpesa_amount,
            total_amount=total_amount,
            item_count=item_count,
            customer_name=(data.get("customer_name") or "").strip() or None,
            customer_phone=(data.get("customer_phone") or "").strip() or None,
            employee_id=emp.get("id"),
            employee_code=(emp.get("employee_code") or "").strip() or None,
            employee_name=(emp.get("full_name") or "").strip() or None,
            credit_due_date=credit_due_raw,
            lines=data.get("lines") if isinstance(data.get("lines"), list) else [],
            inventory_mode=_pos_inventory_mode(shop),
            client_txn_id=client_txn_id,
            skip_stock_deduction=held_order_id is not None,
            mpesa_receipt_number=mpesa_receipt_number or None,
        )
    except Exception:
        ok, sale_err = False, None
    if not ok:
        msg = sale_err or "Could not record sale."
        status = 400 if sale_err else 500
        return jsonify({"ok": False, "error": msg}), status
    if held_order_id is not None and sale_id:
        try:
            from database import pos_held_order_mark_finalized

            pos_held_order_mark_finalized(shop_id, held_order_id, int(sale_id))
        except Exception:
            pass
    return jsonify(
        {
            "ok": True,
            "sale_id": int(sale_id or 0),
            "receipt_number": (receipt_number or "").strip(),
            "held_order_finalized_id": int(held_order_id) if held_order_id is not None else None,
        }
    )


@app.route("/shops/<int:shop_id>/shop-pos/hold/save", methods=["POST"])
def shop_pos_hold_save(shop_id: int):
    """Withhold-POS: create or update a held order; deducts stock for positive deltas.

    Quantity decreases require ``reduction_approver_code`` (manager / admin / company manager /
    super admin). On success the POS prints **company copy only** for the saved cart.
    """
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    hours_block = _shop_pos_sales_hours_block(shop)
    if hours_block is not None:
        return hours_block

    data = request.get_json(force=True, silent=True) or {}
    emp = data.get("employee") or {}
    employee_code = str(emp.get("employee_code") or "").strip()
    emp_row, emp_err, emp_status = _shop_pos_validate_employee_code(shop_id, employee_code)
    if emp_err:
        return jsonify({"ok": False, "error": emp_err}), emp_status

    lines = data.get("lines")
    if not isinstance(lines, list) or not lines:
        return jsonify({"ok": False, "error": "Cart is empty — nothing to save."}), 400

    hold_id_raw = data.get("hold_id")
    hold_id: Optional[int] = None
    if hold_id_raw not in (None, "", 0, "0"):
        try:
            hold_id = int(hold_id_raw)
            if hold_id <= 0:
                hold_id = None
        except (TypeError, ValueError):
            hold_id = None

    try:
        total_amount = float(data.get("total_amount") or 0)
    except (TypeError, ValueError):
        total_amount = 0.0
    try:
        item_count = float(data.get("item_count") or 0)
    except (TypeError, ValueError):
        item_count = 0.0

    try:
        from database import pos_held_order_save

        ok, err, hold_id_out, delta_lines, committed_after, reduction_lines = pos_held_order_save(
            shop_id=shop_id,
            hold_id=hold_id,
            lines=lines,
            inventory_mode=_pos_inventory_mode(shop),
            label=(data.get("label") or "").strip() or None,
            customer_name=(data.get("customer_name") or "").strip() or None,
            customer_phone=(data.get("customer_phone") or "").strip() or None,
            total_amount=total_amount,
            item_count=item_count,
            employee_id=int(emp_row.get("id") or 0) if emp_row else None,
            employee_code=employee_code or None,
            employee_name=(emp_row.get("full_name") or "").strip() if emp_row else None,
            reduction_approver_code=(data.get("reduction_approver_code") or "").strip() or None,
        )
    except Exception:
        ok, err, hold_id_out, delta_lines, committed_after, reduction_lines = (
            False,
            None,
            None,
            [],
            {},
            [],
        )
    if not ok:
        return jsonify({"ok": False, "error": err or "Could not save the held order."}), 400 if err else 500
    ps = _effective_printing_settings_for_shop(shop)
    return jsonify(
        {
            "ok": True,
            "hold_id": int(hold_id_out or 0),
            "delta_lines": delta_lines or [],
            "reduction_lines": reduction_lines or [],
            "committed": committed_after or {},
            "inventory_mode": _pos_inventory_mode(shop),
            "should_print_company_copy": bool(ps.get("print_compulsory_sale")),
        }
    )


@app.route("/shops/<int:shop_id>/shop-pos/hold/list", methods=["GET"])
def shop_pos_hold_list(shop_id: int):
    """Withhold-POS: open held orders for this shop."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    # Optional per-cashier filter — supplied by the held-orders modal once the cashier
    # has authenticated with their 6-digit code. When supplied, the code is validated
    # server-side and only that employee's open held orders are returned.
    employee_code = (request.args.get("employee_code") or "").strip()
    employee_payload: Optional[dict] = None
    if employee_code:
        if not employee_code.isdigit() or len(employee_code) != 6:
            return jsonify({"ok": False, "error": "Enter a valid 6-digit code."}), 400
        emp_row, emp_err, emp_status = _shop_pos_validate_employee_code(shop_id, employee_code)
        if emp_err:
            return jsonify({"ok": False, "error": emp_err}), emp_status
        employee_payload = {
            "id": int(emp_row.get("id") or 0) if emp_row else 0,
            "employee_code": employee_code,
            "full_name": (emp_row.get("full_name") or "").strip() if emp_row else "",
        }
    try:
        from database import pos_held_order_list_open

        if employee_payload and employee_payload.get("id"):
            rows = pos_held_order_list_open(
                shop_id,
                limit=200,
                employee_id=int(employee_payload["id"]),
                employee_code=employee_code,
            )
        elif employee_code:
            rows = pos_held_order_list_open(
                shop_id, limit=200, employee_code=employee_code
            )
        else:
            rows = pos_held_order_list_open(shop_id, limit=200)
    except Exception:
        rows = []
    out = []
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        last = r.get("last_save_at") or r.get("created_at")
        out.append(
            {
                "id": int(r.get("id") or 0),
                "customer_name": r.get("customer_name") or "",
                "customer_phone": r.get("customer_phone") or "",
                "label": r.get("label") or "",
                "total_amount": float(r.get("total_amount") or 0),
                "item_count": float(r.get("item_count") or 0),
                "saves_count": int(r.get("saves_count") or 0),
                "last_save_at": last.isoformat() if hasattr(last, "isoformat") else (str(last) if last else None),
                "employee_code": r.get("employee_code") or "",
                "employee_name": r.get("employee_name") or "",
                "inventory_mode": r.get("inventory_mode") or "shop",
            }
        )
    response: dict = {"ok": True, "held_orders": out}
    if employee_payload:
        response["employee"] = employee_payload
        response["filtered"] = True
    return jsonify(response)


@app.route("/shops/<int:shop_id>/shop-pos/hold/<int:hold_id>", methods=["GET"])
def shop_pos_hold_get(shop_id: int, hold_id: int):
    """Withhold-POS: fetch one held order to reopen it on the POS."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    try:
        from database import pos_held_order_get

        row = pos_held_order_get(shop_id, hold_id)
    except Exception:
        row = None
    if not row:
        return jsonify({"ok": False, "error": "Held order not found."}), 404
    if (row.get("status") or "open") != "open":
        return jsonify({"ok": False, "error": "Held order is no longer open."}), 409
    last = row.get("last_save_at") or row.get("created_at")
    return jsonify(
        {
            "ok": True,
            "hold": {
                "id": int(row.get("id") or 0),
                "customer_name": row.get("customer_name") or "",
                "customer_phone": row.get("customer_phone") or "",
                "label": row.get("label") or "",
                "total_amount": float(row.get("total_amount") or 0),
                "item_count": float(row.get("item_count") or 0),
                "saves_count": int(row.get("saves_count") or 0),
                "last_save_at": last.isoformat() if hasattr(last, "isoformat") else (str(last) if last else None),
                "employee_code": row.get("employee_code") or "",
                "employee_name": row.get("employee_name") or "",
                "inventory_mode": row.get("inventory_mode") or "shop",
                "lines": row.get("cart_lines") or [],
                "committed": row.get("committed_map") or {},
            },
        }
    )


@app.route("/shops/<int:shop_id>/shop-pos/hold/<int:hold_id>/void", methods=["POST"])
def shop_pos_hold_void(shop_id: int, hold_id: int):
    """Withhold-POS: void a held order. Disallowed once any qty has been stock-committed."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate

    data = request.get_json(force=True, silent=True) or {}
    emp = data.get("employee") or {}
    employee_code = str(emp.get("employee_code") or "").strip()
    emp_row, emp_err, emp_status = _shop_pos_validate_employee_code(shop_id, employee_code)
    if emp_err:
        return jsonify({"ok": False, "error": emp_err}), emp_status

    try:
        from database import pos_held_order_void

        ok, err = pos_held_order_void(
            shop_id,
            hold_id,
            employee_id=int(emp_row.get("id") or 0) if emp_row else None,
        )
    except Exception:
        ok, err = False, None
    if not ok:
        return jsonify({"ok": False, "error": err or "Could not void the held order."}), 400 if err else 500
    return jsonify({"ok": True, "hold_id": int(hold_id)})


@app.route("/shops/<int:shop_id>/shop-pos/record-quote", methods=["POST"])
def shop_pos_record_quote(shop_id: int):
    """Save a POS quotation (lead): no sale row, no stock movement. Used before printing a quote receipt."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    hours_block = _shop_pos_sales_hours_block(shop)
    if hours_block is not None:
        return hours_block

    data = request.get_json(force=True, silent=True) or {}
    if not _shop_pos_allow_quotations(shop):
        return jsonify({"ok": False, "error": "Quotations are disabled for this POS."}), 403

    client_txn_id = (data.get("client_txn_id") or "").strip()[:64] or None

    quote_basis = (data.get("quote_basis") or "sale").strip().lower()
    if quote_basis not in ("sale", "credit"):
        return jsonify({"ok": False, "error": "Invalid quotation type."}), 400
    if quote_basis == "credit" and not _shop_pos_allow_credit_sale(shop):
        return jsonify({"ok": False, "error": "Credit-based quotations require credit to be enabled for this POS."}), 403

    try:
        total_amount = float(data.get("total_amount") or 0)
    except Exception:
        return jsonify({"ok": False, "error": "Invalid total amount."}), 400
    if total_amount < 0:
        return jsonify({"ok": False, "error": "Invalid total amount."}), 400

    try:
        item_count = int(data.get("item_count") or 0)
    except Exception:
        return jsonify({"ok": False, "error": "Invalid item count."}), 400
    if item_count < 0:
        item_count = 0

    emp = data.get("employee") or {}
    try:
        from database import create_shop_pos_quotation

        qid, err = create_shop_pos_quotation(
            shop_id=shop_id,
            quote_basis=quote_basis,
            quote_channel="walkin",
            total_amount=total_amount,
            item_count=item_count,
            customer_name=(data.get("customer_name") or "").strip() or None,
            customer_phone=(data.get("customer_phone") or "").strip() or None,
            employee_id=emp.get("id"),
            employee_code=(emp.get("employee_code") or "").strip() or None,
            employee_name=(emp.get("full_name") or "").strip() or None,
            lines=data.get("lines") if isinstance(data.get("lines"), list) else [],
            client_txn_id=client_txn_id,
        )
    except Exception:
        qid, err = None, None
    if not qid:
        msg = err or "Could not save quotation."
        status = 400 if err else 500
        return jsonify({"ok": False, "error": msg}), status

    customer_name = (data.get("customer_name") or "").strip()
    customer_phone_raw = (data.get("customer_phone") or "").strip()
    customer_phone = _normalize_whatsapp_phone(customer_phone_raw) if customer_phone_raw else ""
    quote_lines = data.get("lines") if isinstance(data.get("lines"), list) else []
    share_url = ""
    try:
        share_token = _make_pos_quotation_share_token(quote_id=int(qid), shop_id=int(shop_id))
        share_url = _pos_quotation_share_public_url(share_token)
    except ValueError:
        share_url = ""
    wa_url = ""
    wa_text = ""
    if customer_phone and len(customer_phone) >= 12:
        wa_text = _format_pos_walkin_quotation_whatsapp_message(
            quote_id=int(qid),
            customer_name=customer_name,
            lines=quote_lines,
            total=total_amount,
            share_url=share_url,
        )
        wa_url = _whatsapp_send_url(customer_phone, wa_text)

    return jsonify(
        {
            "ok": True,
            "quote_id": qid,
            "share_url": share_url,
            "whatsapp_url": wa_url,
            "whatsapp_text": wa_text,
        }
    )


def _parse_iso_date_query(s: Optional[str]) -> Optional[date]:
    if not s or not str(s).strip():
        return None
    try:
        return datetime.strptime(str(s).strip()[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _leads_date_filter_from_request(args) -> tuple:
    """Return (date_from_iso, date_to_iso, template_ctx) for quotation listings."""
    mode = (args.get("range") or "all").strip().lower()
    if mode not in ("all", "day", "period", "month", "year"):
        mode = "all"
    today = date.today()
    d_from: Optional[date] = None
    d_to: Optional[date] = None
    ctx = {
        "filter_range": mode,
        "filter_date": (args.get("date") or "").strip(),
        "filter_start": (args.get("start") or "").strip(),
        "filter_end": (args.get("end") or "").strip(),
        "filter_month": (args.get("month") or "").strip(),
        "filter_year": (args.get("year") or "").strip(),
    }
    if mode == "day":
        d = _parse_iso_date_query(args.get("date")) or today
        d_from = d_to = d
        ctx["filter_date"] = d.isoformat()
    elif mode == "period":
        a = _parse_iso_date_query(args.get("start"))
        b = _parse_iso_date_query(args.get("end"))
        if a and b:
            if a > b:
                a, b = b, a
            d_from, d_to = a, b
        elif a:
            d_from = d_to = a
        elif b:
            d_from = d_to = b
    elif mode == "month":
        raw = ctx["filter_month"]
        parsed = False
        if raw and len(raw) >= 7:
            try:
                y = int(raw[:4])
                m = int(raw[5:7])
                if 1 <= m <= 12:
                    last = calendar.monthrange(y, m)[1]
                    d_from = date(y, m, 1)
                    d_to = date(y, m, last)
                    parsed = True
            except (ValueError, TypeError):
                parsed = False
        if not parsed:
            y, m = today.year, today.month
            last = calendar.monthrange(y, m)[1]
            d_from = date(y, m, 1)
            d_to = date(y, m, last)
            ctx["filter_month"] = f"{y:04d}-{m:02d}"
    elif mode == "year":
        y_raw = ctx["filter_year"] or str(today.year)
        try:
            y = int(str(y_raw).strip())
            if 2000 <= y <= 2100:
                d_from = date(y, 1, 1)
                d_to = date(y, 12, 31)
                ctx["filter_year"] = str(y)
        except (ValueError, TypeError):
            pass
        if not d_from:
            y = today.year
            d_from = date(y, 1, 1)
            d_to = date(y, 12, 31)
            ctx["filter_year"] = str(y)

    df = d_from.isoformat() if d_from else None
    dt = d_to.isoformat() if d_to else None
    return df, dt, ctx


@app.route("/shops/<int:shop_id>/leads")
def shop_leads(shop_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    df, dt, filt = _leads_date_filter_from_request(request.args)
    try:
        from database import list_shop_pos_quotations

        quotes = list_shop_pos_quotations(shop_id, limit=500, date_from=df, date_to=dt)
    except Exception:
        quotes = []
    quote_lines_by_id = {str(q["id"]): (q.get("lines") or []) for q in quotes}
    return render_template(
        "shop_leads.html",
        shop=shop,
        pos_allow_credit_sale=_shop_pos_allow_credit_sale(shop),
        quotes=quotes,
        quote_lines_by_id=quote_lines_by_id,
        quote_whatsapp_by_id=_quotation_whatsapp_by_id(quotes),
        company_whatsapp_phone=_company_whatsapp_phone(),
        leads_filter=filt,
        theme_key=f"richcom-theme-shop-{shop['id']}",
        theme_default=shop.get("default_theme") or "dark",
        font_family=shop.get("font_family") or "Plus Jakarta Sans",
        primary_color_rgb=_hex_to_rgb_triplet(shop.get("primary_color") or "#10b981"),
        accent_color_rgb=_hex_to_rgb_triplet(shop.get("accent_color") or "#14b8a6"),
    )


@app.route("/it_support/leads")
@login_required
def it_support_leads():
    _it_support_or_super_admin_only()
    df, dt, filt = _leads_date_filter_from_request(request.args)
    try:
        from database import list_all_pos_quotations_for_it

        quotes = list_all_pos_quotations_for_it(limit=2000, date_from=df, date_to=dt)
    except Exception:
        quotes = []
    quote_lines_enriched_by_id, quote_customer_share_by_id = _quotation_leads_detail_context(quotes)
    return render_template(
        "it_support_leads.html",
        quotes=quotes,
        quote_lines_enriched_by_id=quote_lines_enriched_by_id,
        quote_customer_share_by_id=quote_customer_share_by_id,
        quote_whatsapp_by_id=_quotation_whatsapp_by_id(quotes),
        company_whatsapp_phone=_company_whatsapp_phone(),
        quotation_share_domain=_quotation_share_domain_info(),
        leads_filter=filt,
        leads_nav_active="list",
    )


@app.route("/it_support/leads/share-quotation")
@login_required
def it_support_leads_share_quotation():
    _it_support_or_super_admin_only()
    try:
        from database import list_website_catalog_items

        catalog = list_website_catalog_items(limit=500) or []
    except Exception:
        catalog = []
    picker_items = []
    for row in catalog:
        ip = (row.get("image_path") or "").strip()
        picker_items.append(
            {
                "id": int(row.get("id") or 0),
                "category": (row.get("category") or "").strip() or "General",
                "name": (row.get("name") or "").strip() or "Product",
                "description": (row.get("description") or "").strip(),
                "price": float(row.get("price") or 0),
                "image_url": _public_static_upload_url(ip) if ip else "",
            }
        )
    company = (_load_company_identity_settings().get("company_name") or "Our shop").strip()
    return render_template(
        "it_support_leads_share_quotation.html",
        picker_items=picker_items,
        company_name=company,
        quotation_share_domain=_quotation_share_domain_info(),
        leads_nav_active="share",
    )


@app.route("/api/it-support/quotation-share-link", methods=["POST"])
@login_required
def api_it_support_quotation_share_link():
    _it_support_or_super_admin_only()
    data = request.get_json(force=True, silent=True) or {}
    item_ids = _normalize_quotation_share_item_ids(data.get("item_ids"))
    if not item_ids:
        return jsonify({"ok": False, "error": "Select at least one item."}), 400
    try:
        token = _make_quotation_share_token(item_ids)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    items = _quotation_share_items_for_public(item_ids)
    if not items:
        return jsonify({"ok": False, "error": "No active items found for this selection."}), 400
    share_url = _quotation_share_public_url(token)
    wa_text = _format_quotation_share_whatsapp_message(items, share_url)
    phone_raw = (data.get("customer_phone") or data.get("whatsapp_phone") or "").strip()
    phone = _normalize_whatsapp_phone(phone_raw) if phone_raw else ""
    if phone_raw and len(phone) < 12:
        return jsonify(
            {"ok": False, "error": "Enter a valid WhatsApp number (e.g. 0712345678)."}
        ), 400
    return jsonify(
        {
            "ok": True,
            "url": share_url,
            "whatsapp_text": wa_text,
            "whatsapp_url": _whatsapp_send_url(phone, wa_text),
            "item_count": len(items),
            "customer_phone": phone_raw,
        }
    )


@app.route("/quotation/share/<token>")
def quotation_share_public(token: str):
    """Public quotation page — image, name, description, and price for shared items (no login)."""
    try:
        item_ids = _parse_quotation_share_token(token)
    except ValueError as exc:
        return _render_quotation_share_public_response(error=str(exc), http_status=400)
    items = _quotation_share_items_for_public(item_ids)
    if not items:
        return _render_quotation_share_public_response(
            error="These items are no longer available.",
            http_status=404,
        )
    identity = _load_company_identity_settings()
    company = (identity.get("company_name") or "Our shop").strip()
    total = round(sum(float(i.get("price") or 0) for i in items), 2)
    company_phone = (identity.get("company_phone") or "").strip()
    wa_contact_url = ""
    if company_phone and _company_whatsapp_phone():
        wa_contact_url = _whatsapp_send_url(
            company_phone,
            f"Hello {company}, I received your quotation and would like to discuss it.",
        )
    generated_date = datetime.now().strftime("%d %b %Y")
    logo_path = (identity.get("app_icon") or "").strip()
    company_logo_url = _public_static_upload_url(logo_path) if logo_path else ""
    return _render_quotation_share_public_response(
        items=items,
        company_name=company,
        company_logo_url=company_logo_url,
        total_amount=total,
        share_url=_quotation_share_public_url(token),
        generated_date=generated_date,
        whatsapp_contact_url=wa_contact_url,
    )


@app.route("/q/<token>")
def pos_quotation_share_public(token: str):
    """Public POS quotation page — saved quote lines, no employee login required."""
    try:
        quote_id, shop_id = _parse_pos_quotation_share_token(token)
    except ValueError as exc:
        return _render_quotation_share_public_response(error=str(exc), http_status=400)
    try:
        from database import get_shop_pos_quotation_by_id

        quote = get_shop_pos_quotation_by_id(quote_id, shop_id=shop_id)
    except Exception:
        quote = None
    if not quote:
        return _render_quotation_share_public_response(
            error="This quotation could not be found.",
            http_status=404,
        )
    enriched = _enrich_quotation_lines_for_display(quote.get("lines") or [])
    items = _pos_quote_lines_to_public_items(enriched)
    if not items:
        return _render_quotation_share_public_response(
            error="This quotation has no items to display.",
            http_status=404,
        )
    identity = _load_company_identity_settings()
    company = (identity.get("company_name") or "Our shop").strip()
    try:
        total = float(quote.get("total_amount") or 0)
    except (TypeError, ValueError):
        total = round(sum(float(i.get("price") or 0) for i in items), 2)
    company_phone = (identity.get("company_phone") or "").strip()
    wa_contact_url = ""
    if company_phone and _company_whatsapp_phone():
        wa_contact_url = _whatsapp_send_url(
            company_phone,
            f"Hello {company}, I received quotation #{quote_id} and would like to discuss it.",
        )
    created = quote.get("created_at")
    generated_date = ""
    if created is not None:
        try:
            if hasattr(created, "strftime"):
                generated_date = created.strftime("%d %b %Y")
            else:
                generated_date = datetime.strptime(str(created)[:10], "%Y-%m-%d").strftime("%d %b %Y")
        except (TypeError, ValueError):
            generated_date = ""
    if not generated_date:
        generated_date = datetime.now().strftime("%d %b %Y")
    logo_path = (identity.get("app_icon") or "").strip()
    company_logo_url = _public_static_upload_url(logo_path) if logo_path else ""
    return _render_quotation_share_public_response(
        items=items,
        company_name=company,
        company_logo_url=company_logo_url,
        total_amount=total,
        share_url=_pos_quotation_share_public_url(token),
        generated_date=generated_date,
        whatsapp_contact_url=wa_contact_url,
    )


@app.route("/it_support/receipts")
@login_required
def it_support_receipts():
    _it_support_or_super_admin_only()
    analytics_filter = _build_analytics_filter()
    rq = (request.args.get("receipt_q") or "").strip()
    shop_filter_arg = request.args.get("shop_id", type=int)
    shop_filter_id = shop_filter_arg if shop_filter_arg and shop_filter_arg > 0 else None
    shops = []
    try:
        from database import list_all_shops_pos_sales_receipt_rows, list_shops

        shops = list_shops(limit=500) or []
        rows = list_all_shops_pos_sales_receipt_rows(
            analytics_filter,
            shop_id=shop_filter_id,
            sale_id_search=rq,
            limit=2000,
        )
    except Exception:
        logger.exception("it_support_receipts list failed")
        rows = []
        try:
            from database import list_shops

            shops = list_shops(limit=500) or []
        except Exception:
            shops = []

    return render_template(
        "it_support_receipts.html",
        analytics_filter=analytics_filter,
        receipt_rows=rows,
        receipt_rows_total=len(rows),
        receipt_q=rq,
        shops=shops,
        shop_filter_id=shop_filter_id or 0,
    )


@app.route("/it_support/withheld-holds")
@login_required
def it_support_withheld_holds():
    _it_support_or_super_admin_only()
    if _coerce_pos_cart_mode(_load_printing_settings().get("pos_cart_mode")) != "withhold":
        flash("Withhold POS is not enabled for this company. Enable it under Point of sale settings.", "info")
        return redirect(url_for("it_support_receipts"))
    analytics_filter = _build_analytics_filter()
    shop_filter_arg = request.args.get("shop_id", type=int)
    shop_filter_id = shop_filter_arg if shop_filter_arg and shop_filter_arg > 0 else None
    shops = []
    rows = []
    try:
        from database import list_all_pos_held_orders_register_rows, list_shops

        shops = list_shops(limit=5000) or []
        rows = list_all_pos_held_orders_register_rows(
            analytics_filter,
            shop_id=shop_filter_id,
            limit=8000,
        )
    except Exception:
        logger.exception("it_support_withheld_holds list failed")
        rows = []
        try:
            from database import list_shops

            shops = list_shops(limit=5000) or []
        except Exception:
            shops = []
    return render_template(
        "it_support_withheld_holds.html",
        analytics_filter=analytics_filter,
        hold_rows=rows,
        hold_rows_total=len(rows),
        shops=shops,
        shop_filter_id=shop_filter_id or 0,
    )


@app.route("/it_support/withheld-holds/detail")
@login_required
def it_support_withheld_holds_detail():
    _it_support_or_super_admin_only()
    shop_id = request.args.get("shop_id", type=int)
    hold_id = request.args.get("hold_id", type=int)
    if not shop_id or not hold_id:
        return jsonify({"ok": False, "error": "Missing hold reference."}), 400
    try:
        from database import pos_held_order_get

        row = pos_held_order_get(int(shop_id), int(hold_id))
    except Exception:
        logger.exception("it_support_withheld_holds_detail failed shop_id=%s hold_id=%s", shop_id, hold_id)
        return jsonify({"ok": False, "error": "Could not load held order."}), 500
    if not row:
        return jsonify({"ok": False, "error": "Held order not found."}), 404
    st = (row.get("status") or "open").strip().lower()
    if st == "finalized":
        register_status = "completed"
    elif st == "voided":
        register_status = "returned"
    else:
        register_status = "pending"
    return jsonify(
        {
            "ok": True,
            "hold": {
                "id": row.get("id"),
                "shop_id": row.get("shop_id"),
                "status": row.get("status"),
                "register_status": register_status,
                "customer_name": row.get("customer_name"),
                "customer_phone": row.get("customer_phone"),
                "label": row.get("label"),
                "total_amount": float(row.get("total_amount") or 0),
                "item_count": float(row.get("item_count") or 0),
                "saves_count": int(row.get("saves_count") or 0),
                "last_save_at": str(row.get("last_save_at") or ""),
                "created_at": str(row.get("created_at") or ""),
                "finalized_sale_id": row.get("finalized_sale_id"),
                "finalized_at": str(row.get("finalized_at") or ""),
                "voided_at": str(row.get("voided_at") or ""),
                "employee_code": row.get("employee_code"),
                "employee_name": row.get("employee_name"),
                "inventory_mode": row.get("inventory_mode"),
            },
            "lines": row.get("cart_lines") or [],
            "committed": row.get("committed_map") or {},
        }
    )


@app.route("/it_support/receipts/mark", methods=["POST"])
@login_required
def it_support_receipts_mark():
    _it_support_or_super_admin_only()
    data = request.get_json(force=True, silent=True) or {}
    raw_ids = data.get("sale_ids")
    mark_status = (data.get("mark_status") or "").strip().lower()
    if not isinstance(raw_ids, list):
        return jsonify({"ok": False, "error": "Select at least one receipt."}), 400
    try:
        from database import bulk_mark_shop_pos_receipts

        affected = bulk_mark_shop_pos_receipts(
            sale_ids=raw_ids,
            mark_status=mark_status,
        )
    except Exception:
        logger.exception("it_support_receipts_mark failed")
        return jsonify({"ok": False, "error": "Could not update receipt marks."}), 500
    if affected <= 0:
        return jsonify({"ok": False, "error": "No receipts were updated."}), 400
    return jsonify({"ok": True, "updated": int(affected), "mark_status": mark_status})


@app.route("/it_support/receipts/detail")
@login_required
def it_support_receipts_detail():
    _it_support_or_super_admin_only()
    shop_id = request.args.get("shop_id", type=int)
    sale_id = request.args.get("sale_id", type=int)
    if not shop_id or not sale_id:
        return jsonify({"ok": False, "error": "Missing receipt reference."}), 400
    try:
        from database import get_shop_pos_sale_detail

        d = get_shop_pos_sale_detail(shop_id=int(shop_id), sale_id=int(sale_id)) or {}
    except Exception:
        logger.exception("it_support_receipts_detail failed shop_id=%s sale_id=%s", shop_id, sale_id)
        return jsonify({"ok": False, "error": "Could not load receipt detail."}), 500
    if not d.get("sale"):
        return jsonify({"ok": False, "error": "Receipt not found."}), 404
    return jsonify({"ok": True, "sale": d.get("sale") or {}, "items": d.get("items") or []})


@app.route("/it_support/receipts/return-lines", methods=["POST"])
@login_required
def it_support_receipts_return_lines():
    _it_support_or_super_admin_only()
    data = request.get_json(force=True, silent=True) or {}
    shop_id = int(data.get("shop_id") or 0)
    sale_id = int(data.get("sale_id") or 0)
    line_ids = data.get("line_ids")
    if shop_id <= 0 or sale_id <= 0:
        return jsonify({"ok": False, "error": "Invalid receipt reference."}), 400
    if not isinstance(line_ids, list):
        return jsonify({"ok": False, "error": "Select at least one item to return."}), 400
    try:
        from database import return_shop_pos_sale_lines

        ok, err, meta = return_shop_pos_sale_lines(
            shop_id=shop_id,
            sale_id=sale_id,
            line_ids=line_ids,
        )
    except Exception:
        logger.exception("it_support_receipts_return_lines failed shop_id=%s sale_id=%s", shop_id, sale_id)
        return jsonify({"ok": False, "error": "Could not process return."}), 500
    if not ok:
        return jsonify({"ok": False, "error": err or "Could not process return."}), 400
    return jsonify({"ok": True, "meta": meta or {}})


def _normalize_subnet_scan_prefix(raw) -> str:
    """Turn user input into ``a.b.c`` for LAN scans.

    Accepts ``192.168.1``, ``192.168.1.0``, ``192.168.1.50``, ``192.168.1.0/24``, etc.
    Returns ``""`` if nothing usable.
    """
    if raw is None:
        return ""
    s = str(raw).strip()
    if not s:
        return ""
    if "/" in s:
        s = s.split("/", 1)[0].strip()
    parts = [p for p in s.split(".") if p != ""]
    if len(parts) >= 4:
        s = ".".join(parts[:3])
    elif len(parts) == 3:
        s = ".".join(parts)
    else:
        return ""
    if not re.match(r"^(\d{1,3})\.(\d{1,3})\.(\d{1,3})$", s):
        return ""
    try:
        octets = [int(x) for x in s.split(".")]
    except ValueError:
        return ""
    if any(p > 255 for p in octets):
        return ""
    return s


def _tcp_probe_host_port(host: str, port: int, timeout: float) -> bool:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        r = s.connect_ex((host, port))
        s.close()
        return r == 0
    except OSError:
        return False


# Common raw / thermal / JetDirect-style TCP ports (UI primary port is tried first).
_THERMAL_RAW_TCP_PORTS_DEFAULT = (
    9100,
    9101,
    9102,
    9103,
    9104,
    4000,
    5000,
    4028,
    9200,
    515,
    631,
)


def _thermal_scan_ports_tcp(primary: int, data=None) -> tuple[int, ...]:
    """Ordered port list: user primary first, then defaults; optional ``scan_ports`` overrides."""
    data = data or {}
    raw = data.get("scan_ports") or data.get("ports")
    if isinstance(raw, list) and raw:
        out = []
        seen = set()
        for x in raw:
            try:
                p = int(float(str(x).strip()))
            except (TypeError, ValueError):
                continue
            if 1 <= p <= 65535 and p not in seen:
                seen.add(p)
                out.append(p)
        if out:
            return tuple(out)
    try:
        primary = int(float(str(primary).strip()))
    except (TypeError, ValueError):
        primary = 9100
    if primary < 1 or primary > 65535:
        primary = 9100
    merged = []
    seen = set()
    for p in (primary, *_THERMAL_RAW_TCP_PORTS_DEFAULT):
        if p not in seen and 1 <= p <= 65535:
            seen.add(p)
            merged.append(p)
    return tuple(merged)


def _scan_subnet_thermal_endpoints(
    subnet_prefix: str,
    ports: tuple[int, ...],
    timeout: float = 0.14,
) -> list[dict]:
    """Probe .1–.254 for open TCP on any of ``ports``; return ``[{host, port}, ...]`` sorted."""
    subnet_prefix = (subnet_prefix or "").strip()
    if not re.match(r"^(\d{1,3})\.(\d{1,3})\.(\d{1,3})$", subnet_prefix):
        return []
    parts = [int(x) for x in subnet_prefix.split(".")]
    if any(p > 255 for p in parts):
        return []

    def probe_pair(last_oct: int, pr: int):
        host = f"{subnet_prefix}.{last_oct}"
        if _tcp_probe_host_port(host, pr, timeout):
            return {"host": host, "port": int(pr)}
        return None

    found: list[dict] = []
    tasks = [(i, p) for i in range(1, 255) for p in ports]
    max_workers = min(48, max(8, len(tasks)))
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = {ex.submit(probe_pair, i, p): (i, p) for i, p in tasks}
            for fut in as_completed(futs):
                row = fut.result()
                if row:
                    found.append(row)
    except RuntimeError:
        # Fallback when host process is out of thread resources.
        for i, p in tasks:
            row = probe_pair(i, p)
            if row:
                found.append(row)

    def sort_key(d):
        h = d["host"]
        return (tuple(int(x) for x in h.split(".")), d["port"])

    found.sort(key=sort_key)
    return found


def _iter_subnet_scan_batches(
    subnet_prefix: str,
    ports: tuple[int, ...],
    timeout: float = 0.14,
    batch_size: int = 36,
):
    """Yield progress for each IP batch on a /24 (multi-port thermal scan)."""
    subnet_prefix = (subnet_prefix or "").strip()
    if not re.match(r"^(\d{1,3})\.(\d{1,3})\.(\d{1,3})$", subnet_prefix):
        return
    parts = [int(x) for x in subnet_prefix.split(".")]
    if any(p > 255 for p in parts):
        return

    ranges = []
    lo = 1
    while lo <= 254:
        hi = min(lo + batch_size - 1, 254)
        ranges.append((lo, hi))
        lo = hi + 1
    batch_n = len(ranges)
    for batch_i, (lo, hi) in enumerate(ranges, start=1):

        def probe_pair(last_oct: int, pr: int):
            host = f"{subnet_prefix}.{last_oct}"
            if _tcp_probe_host_port(host, pr, timeout):
                return {"host": host, "port": int(pr)}
            return None

        pair_tasks = [(i, p) for i in range(lo, hi + 1) for p in ports]
        batch_eps: list[dict] = []
        max_workers = min(48, max(8, len(pair_tasks)))
        try:
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                futs = {ex.submit(probe_pair, i, p): (i, p) for i, p in pair_tasks}
                for fut in as_completed(futs):
                    row = fut.result()
                    if row:
                        batch_eps.append(row)
        except RuntimeError:
            # Fallback when host process is out of thread resources.
            for i, p in pair_tasks:
                row = probe_pair(i, p)
                if row:
                    batch_eps.append(row)

        def sort_key(d):
            h = d["host"]
            return (tuple(int(x) for x in h.split(".")), d["port"])

        batch_eps.sort(key=sort_key)
        yield {
            "type": "batch",
            "prefix": subnet_prefix,
            "batch": batch_i,
            "batches": batch_n,
            "lo": lo,
            "hi": hi,
            "found": batch_eps,
            "scan_ports": list(ports),
        }


def _get_client_ipv4(request) -> str | None:
    """Best-effort client IPv4 from the incoming HTTP connection (for same-LAN hints)."""
    xff = (request.headers.get("X-Forwarded-For") or "").strip()
    cand = xff.split(",")[0].strip() if xff else (request.remote_addr or "").strip()
    if not cand or cand in ("127.0.0.1", "::1"):
        return None
    if not re.match(r"^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$", cand):
        return None
    parts = [int(x) for x in cand.split(".")]
    if any(p > 255 for p in parts):
        return None
    return cand


def _lan_prefix_from_ipv4(ip: str) -> str | None:
    if not ip or not re.match(r"^(\d{1,3}\.){3}\d{1,3}$", ip):
        return None
    return ".".join(ip.split(".")[:3])


def _guess_primary_lan_ipv4() -> str | None:
    """Best-effort LAN IPv4 when the browser talks to Flask via 127.0.0.1 (no X-Forwarded-For)."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(0.25)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        if ip and not ip.startswith("127.") and not ip.startswith("169.254."):
            return ip
    except OSError:
        pass
    return None


def _get_server_lan_prefixes() -> list:
    """Non-loopback /24 roots for this machine (so 192.168.100.x is scanned when the app runs locally)."""
    out = []
    seen = set()

    guess = _guess_primary_lan_ipv4()
    if guess:
        pfx = _lan_prefix_from_ipv4(guess)
        if pfx:
            seen.add(pfx)
            out.append(pfx)

    try:
        hostname = socket.gethostname()
        for info in socket.getaddrinfo(hostname, None, socket.AF_INET, socket.SOCK_STREAM):
            ip = info[4][0]
            if ip.startswith("127.") or ip.startswith("169.254."):
                continue
            pfx = _lan_prefix_from_ipv4(ip)
            if pfx and pfx not in seen:
                seen.add(pfx)
                out.append(pfx)
    except OSError:
        pass
    return out


def _build_auto_scan_prefixes(request) -> tuple:
    """Client /24 hint, then this server's LAN interfaces, then common defaults."""
    client_ip = _get_client_ipv4(request)
    prefixes = []
    if client_ip:
        pfx = _lan_prefix_from_ipv4(client_ip)
        if pfx:
            prefixes.append(pfx)
    for pfx in _get_server_lan_prefixes():
        if pfx not in prefixes:
            prefixes.append(pfx)
    for d in _DEFAULT_AUTO_SCAN_PREFIXES:
        if d not in prefixes:
            prefixes.append(d)
    prefixes = prefixes[:_DEFAULT_AUTO_SCAN_MAX_PREFIXES]
    return prefixes, client_ip


# Common LAN /24 roots (many routers use 192.168.100.x); merged with client + server hints.
_DEFAULT_AUTO_SCAN_PREFIXES = (
    "192.168.1",
    "192.168.0",
    "192.168.100",
    "192.168.10",
    "192.168.2",
    "192.168.50",
    "10.0.0",
    "10.0.1",
    "172.16.0",
    "192.168.4",
    "192.168.88",
    "10.1.1",
)
_DEFAULT_AUTO_SCAN_MAX_PREFIXES = 14


def _probe_host_thermal_endpoints(host_raw: str, ports: tuple[int, ...], timeout: float = 0.55) -> list[dict]:
    """Try each thermal port on an IPv4 literal or resolved hostname."""
    host_raw = (host_raw or "").strip()
    if not host_raw or len(host_raw) > 253:
        return []
    if re.match(r"^(\d{1,3}\.){3}\d{1,3}$", host_raw):
        parts = [int(x) for x in host_raw.split(".")]
        if any(p > 255 for p in parts):
            return []
        out = []
        for pr in ports:
            if _tcp_probe_host_port(host_raw, pr, timeout):
                out.append({"host": host_raw, "port": int(pr)})
        return out
    try:
        infos = socket.getaddrinfo(host_raw, None, socket.AF_INET, socket.SOCK_STREAM)
    except socket.gaierror:
        return []
    if not infos:
        return []
    ip = infos[0][4][0]
    out = []
    for pr in ports:
        if _tcp_probe_host_port(ip, pr, timeout):
            out.append({"host": ip, "port": int(pr)})
    return out


@app.route("/shops/<int:shop_id>/shop-pos/printer", methods=["GET", "POST"])
def shop_pos_printer(shop_id: int):
    """Load or save POS receipt printer pairing (Bluetooth / USB / network metadata)."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    from database import delete_shop_printer_settings, get_shop_printer_settings, upsert_shop_printer_settings

    if request.method == "GET":
        row = get_shop_printer_settings(shop_id)
        if not row:
            return jsonify({"ok": True, "shop_id": shop_id, "printer": None})
        try:
            cfg = json.loads(row["config_json"] or "{}")
        except json.JSONDecodeError:
            cfg = {}
        updated = row.get("updated_at")
        printer_payload = {
            "shop_id": shop_id,
            "printer_type": row["printer_type"],
            "device_label": row["device_label"],
            "config": cfg,
            "updated_at": updated.isoformat() if hasattr(updated, "isoformat") else None,
        }
        ps = _effective_printing_settings_for_shop(shop)
        pt = (row.get("printer_type") or "").strip().lower()
        if not _printer_type_allowed_by_printing_settings(ps, pt):
            return jsonify(
                {
                    "ok": True,
                    "shop_id": shop_id,
                    "printer": None,
                    "stale_printer": printer_payload,
                    "stale_reason": "not_allowed_by_it_settings",
                }
            )
        return jsonify({"ok": True, "shop_id": shop_id, "printer": printer_payload})

    data = request.get_json(force=True, silent=True) or {}
    if data.get("clear"):
        delete_shop_printer_settings(shop_id)
        return jsonify({"ok": True, "shop_id": shop_id, "printer": None})

    pt = (data.get("printer_type") or "").strip().lower()
    if pt not in ("bluetooth", "network", "usb"):
        return jsonify({"ok": False, "error": "Invalid printer_type"}), 400
    ps = _effective_printing_settings_for_shop(shop)
    if not _printer_type_allowed_by_printing_settings(ps, pt):
        return jsonify(
            {
                "ok": False,
                "error": f"{pt.capitalize()} printing is disabled in IT settings for this company.",
            }
        ), 400
    label = (data.get("device_label") or "")[:255]
    cfg = data.get("config")
    if not isinstance(cfg, dict):
        return jsonify({"ok": False, "error": "config must be an object"}), 400
    try:
        j = json.dumps(cfg, separators=(",", ":"))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "config not serializable"}), 400
    if len(j) > 12000:
        return jsonify({"ok": False, "error": "config too large"}), 400
    if not upsert_shop_printer_settings(shop_id, printer_type=pt, device_label=label, config_json=j):
        return jsonify({"ok": False, "error": "Could not save"}), 500
    return jsonify(
        {
            "ok": True,
            "shop_id": shop_id,
            "printer": {"shop_id": shop_id, "printer_type": pt, "device_label": label, "config": cfg},
        }
    )


@app.route("/shops/<int:shop_id>/shop-pos/printer/tcp-reachable", methods=["GET"])
def shop_pos_printer_tcp_reachable(shop_id: int):
    """Return whether the saved network printer port accepts TCP (or print-agent mode is active)."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    row, cfg = _printer_config_dict(shop_id)
    if not row or (row.get("printer_type") or "").strip().lower() != "network":
        return jsonify({"ok": True, "reachable": False, "reason": "no_network_printer_saved"})
    if cfg.get("print_agent_enabled") and (cfg.get("print_agent_token") or "").strip():
        return jsonify({"ok": True, "reachable": True, "mode": "print_agent"})
    host = (cfg.get("host") or "").strip()
    try:
        port = int(cfg.get("port") or 9100)
    except (TypeError, ValueError):
        port = 9100
    if not host or port < 1 or port > 65535:
        return jsonify(
            {
                "ok": True,
                "reachable": False,
                "reason": "invalid_address",
                "host": host,
                "port": port,
            }
        )
    ok = _tcp_probe_host_port(host, port, 3.0)
    return jsonify({"ok": True, "reachable": ok, "mode": "direct_tcp", "host": host, "port": port})


@app.route("/shops/<int:shop_id>/shop-pos/printer/scan-network", methods=["POST"])
def shop_pos_printer_scan_network(shop_id: int):
    """Scan LAN for thermal / raw TCP printers.

    JSON body:
    - ``port``: primary port (default 9100); also tries common thermal ports unless ``scan_ports`` is set.
    - ``scan_ports``: optional list of ints to probe instead of the default thermal set.
    - ``probe_host``: check one IPv4 or hostname on all selected ports.
    - ``subnet_prefix``: scan only that /24.
    - Otherwise: heuristic multi-subnet scan.

    Response includes ``endpoints`` as ``[{ "host", "port" }, ...]`` when applicable.
    """
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    data = request.get_json(force=True, silent=True) or {}
    port_raw = data.get("port", 9100)
    if port_raw in (None, ""):
        port = 9100
    else:
        try:
            port = int(float(str(port_raw).strip()))
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": "Invalid port"}), 400
    if port < 1 or port > 65535:
        return jsonify({"ok": False, "error": "Invalid port"}), 400

    probe_host = (data.get("probe_host") or "").strip()
    subnet_prefix = _normalize_subnet_scan_prefix(data.get("subnet_prefix") or data.get("subnet"))
    scan_ports = _thermal_scan_ports_tcp(port, data)

    if probe_host:
        endpoints = _probe_host_thermal_endpoints(probe_host, scan_ports)
        if not endpoints:
            return jsonify(
                {
                    "ok": True,
                    "hosts": [],
                    "endpoints": [],
                    "port": port,
                    "scan_ports": list(scan_ports),
                    "probe": True,
                    "probe_target": probe_host[:253],
                    "resolved": None,
                }
            )
        uniq_hosts = sorted({e["host"] for e in endpoints}, key=lambda x: tuple(int(p) for p in x.split(".")))
        resolved = endpoints[0]["host"]
        return jsonify(
            {
                "ok": True,
                "hosts": uniq_hosts,
                "endpoints": endpoints,
                "port": port,
                "scan_ports": list(scan_ports),
                "probe": True,
                "probe_target": probe_host[:253],
                "resolved": resolved,
            }
        )

    # Single-subnet scan when prefix is provided; otherwise LAN heuristic (same as explicit auto:true).
    if subnet_prefix:
        endpoints = _scan_subnet_thermal_endpoints(subnet_prefix, scan_ports)
        hosts = sorted({e["host"] for e in endpoints}, key=lambda x: tuple(int(p) for p in x.split(".")))
        return jsonify(
            {
                "ok": True,
                "hosts": hosts,
                "endpoints": endpoints,
                "port": port,
                "scan_ports": list(scan_ports),
                "subnet_prefix": subnet_prefix,
            }
        )

    prefixes, client_ip = _build_auto_scan_prefixes(request)
    seen_ep = set()
    endpoints = []
    for pref in prefixes:
        for e in _scan_subnet_thermal_endpoints(pref, scan_ports):
            key = (e["host"], e["port"])
            if key not in seen_ep:
                seen_ep.add(key)
                endpoints.append(e)

    def ep_sort_key(d):
        h = d["host"]
        return (tuple(int(x) for x in h.split(".")), d["port"])

    endpoints.sort(key=ep_sort_key)
    hosts = sorted({e["host"] for e in endpoints}, key=lambda x: tuple(int(p) for p in x.split(".")))
    return jsonify(
        {
            "ok": True,
            "hosts": hosts,
            "endpoints": endpoints,
            "port": port,
            "scan_ports": list(scan_ports),
            "auto": True,
            "scanned_prefixes": prefixes,
            "client_hint_ip": client_ip,
        }
    )


def _iter_scan_network_ndjson(port: int, probe_host: str, subnet_prefix: str, request, data):
    """Yield NDJSON event dicts for printer LAN scan (probe, single subnet, or auto)."""
    data = data or {}
    scan_ports = _thermal_scan_ports_tcp(port, data)
    client_ip = _get_client_ipv4(request)

    if probe_host:
        endpoints = _probe_host_thermal_endpoints(probe_host, scan_ports)
        uniq_hosts = sorted({e["host"] for e in endpoints}, key=lambda x: tuple(int(p) for p in x.split(".")))
        resolved = endpoints[0]["host"] if endpoints else None
        yield {
            "type": "done",
            "ok": True,
            "hosts": uniq_hosts,
            "endpoints": endpoints,
            "port": port,
            "scan_ports": list(scan_ports),
            "probe": True,
            "probe_target": probe_host[:253],
            "resolved": resolved,
        }
        return

    if subnet_prefix:
        prefixes = [subnet_prefix]
        mode = "subnet"
    else:
        prefixes, client_ip = _build_auto_scan_prefixes(request)
        mode = "auto"

    yield {
        "type": "start",
        "mode": mode,
        "port": port,
        "scan_ports": list(scan_ports),
        "prefixes": prefixes,
        "client_hint_ip": client_ip,
    }

    seen_ep = set()
    all_endpoints = []
    total_prefixes = len(prefixes)
    for pi, pref in enumerate(prefixes, start=1):
        yield {
            "type": "subnet",
            "status": "start",
            "index": pi,
            "total": total_prefixes,
            "prefix": pref,
        }
        for batch in _iter_subnet_scan_batches(pref, scan_ports):
            batch["subnet_index"] = pi
            for e in batch.get("found") or []:
                key = (e["host"], e["port"])
                if key not in seen_ep:
                    seen_ep.add(key)
                    all_endpoints.append(e)
            all_endpoints.sort(
                key=lambda d: (tuple(int(x) for x in d["host"].split(".")), d["port"])
            )
            batch["hosts_cumulative"] = list(all_endpoints)
            yield batch
        yield {
            "type": "subnet",
            "status": "done",
            "index": pi,
            "total": total_prefixes,
            "prefix": pref,
        }

    hosts = sorted({e["host"] for e in all_endpoints}, key=lambda x: tuple(int(p) for p in x.split(".")))
    yield {
        "type": "done",
        "ok": True,
        "hosts": hosts,
        "endpoints": all_endpoints,
        "port": port,
        "scan_ports": list(scan_ports),
        "auto": mode == "auto",
        "subnet_prefix": subnet_prefix if mode == "subnet" else None,
        "scanned_prefixes": prefixes,
        "client_hint_ip": client_ip,
    }


@app.route("/shops/<int:shop_id>/shop-pos/printer/scan-network-stream", methods=["POST"])
def shop_pos_printer_scan_network_stream(shop_id: int):
    """Same inputs as ``scan-network``, NDJSON stream with per-batch progress."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    data = request.get_json(force=True, silent=True) or {}
    port_raw = data.get("port", 9100)
    if port_raw in (None, ""):
        port = 9100
    else:
        try:
            port = int(float(str(port_raw).strip()))
        except (TypeError, ValueError):

            def err_gen():
                yield json.dumps({"type": "done", "ok": False, "error": "Invalid port"}) + "\n"

            return Response(
                stream_with_context(err_gen()),
                mimetype="application/x-ndjson",
                status=400,
            )
    if port < 1 or port > 65535:

        def err_gen2():
            yield json.dumps({"type": "done", "ok": False, "error": "Invalid port"}) + "\n"

        return Response(
            stream_with_context(err_gen2()),
            mimetype="application/x-ndjson",
            status=400,
        )

    probe_host = (data.get("probe_host") or "").strip()
    subnet_prefix = _normalize_subnet_scan_prefix(data.get("subnet_prefix") or data.get("subnet"))

    def generate():
        try:
            for evt in _iter_scan_network_ndjson(port, probe_host, subnet_prefix, request, data):
                yield json.dumps(evt, separators=(",", ":")) + "\n"
        except Exception as e:
            logger.exception("scan-network-stream")
            yield json.dumps({"type": "done", "ok": False, "error": str(e) or "Scan failed"}) + "\n"

    return Response(
        stream_with_context(generate()),
        mimetype="application/x-ndjson",
        headers={
            "Cache-Control": "no-store",
            "X-Accel-Buffering": "no",
        },
    )


def _printer_config_dict(shop_id: int):
    from database import get_shop_printer_settings

    row = get_shop_printer_settings(shop_id)
    if not row:
        return None, None
    try:
        cfg = json.loads(row["config_json"] or "{}")
    except json.JSONDecodeError:
        cfg = {}
    return row, cfg


def _require_print_agent_token(shop_id: int):
    """Bearer / X-Print-Agent-Token must match saved ``print_agent_token`` when agent is enabled."""
    row, cfg = _printer_config_dict(shop_id)
    if not row or (row.get("printer_type") or "").strip().lower() != "network":
        return jsonify({"ok": False, "error": "No network printer for this shop."}), 403
    expected = (cfg.get("print_agent_token") or "").strip()
    if not expected or not cfg.get("print_agent_enabled"):
        return jsonify({"ok": False, "error": "Print agent is not enabled."}), 403
    token = ""
    auth = (request.headers.get("Authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        token = auth[7:].strip()
    if not token:
        token = (request.headers.get("X-Print-Agent-Token") or "").strip()
    if not token or not hmac.compare_digest(expected, token):
        return jsonify({"ok": False, "error": "Invalid agent token."}), 401
    return None


@app.route("/shops/<int:shop_id>/shop-pos/printer/send-escpos", methods=["POST"])
def shop_pos_printer_send_escpos(shop_id: int):
    """Send raw ESC/POS to the shop printer, or queue for the LAN print agent when enabled."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    import base64
    import socket

    from database import enqueue_shop_print_agent_job, get_shop_printer_settings

    row = get_shop_printer_settings(shop_id)
    if not row or (row.get("printer_type") or "").strip().lower() != "network":
        return jsonify({"ok": False, "error": "No network printer saved for this shop."}), 400
    try:
        cfg = json.loads(row["config_json"] or "{}")
    except json.JSONDecodeError:
        cfg = {}

    data = request.get_json(force=True, silent=True) or {}
    b64 = (data.get("data_b64") or "").strip()
    try:
        raw = base64.b64decode(b64, validate=True)
    except Exception:
        return jsonify({"ok": False, "error": "Invalid base64 payload."}), 400
    if len(raw) > 500_000:
        return jsonify({"ok": False, "error": "Payload too large."}), 400
    if len(raw) == 0:
        return jsonify({"ok": False, "error": "Empty payload."}), 400

    if cfg.get("print_agent_enabled") and (cfg.get("print_agent_token") or "").strip():
        job_id = enqueue_shop_print_agent_job(shop_id, raw)
        if not job_id:
            return jsonify({"ok": False, "error": "Could not queue print job."}), 500
        return jsonify({"ok": True, "queued": True, "job_id": job_id})

    host = (cfg.get("host") or "").strip()
    try:
        port = int(cfg.get("port") or 9100)
    except (TypeError, ValueError):
        port = 9100
    if not host or port < 1 or port > 65535:
        return jsonify({"ok": False, "error": "Invalid saved network printer address."}), 400

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(15.0)
        sock.connect((host, port))
        sock.sendall(raw)
        sock.shutdown(socket.SHUT_WR)
        sock.close()
    except OSError as e:
        return jsonify({"ok": False, "error": f"Could not reach printer: {e}"}), 502
    return jsonify({"ok": True, "queued": False})


@app.route("/shops/<int:shop_id>/shop-pos/print-agent/status", methods=["GET"])
def shop_pos_print_agent_status(shop_id: int):
    """Whether LAN print agent is on; token is never returned in full (session auth)."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    row, cfg = _printer_config_dict(shop_id)
    if not row or (row.get("printer_type") or "").strip().lower() != "network":
        return jsonify({"ok": True, "available": False, "enabled": False, "has_token": False})
    tok = (cfg.get("print_agent_token") or "").strip()
    hint = tok[-4:] if len(tok) >= 4 else ""
    return jsonify(
        {
            "ok": True,
            "available": True,
            "enabled": bool(cfg.get("print_agent_enabled")),
            "has_token": bool(tok),
            "token_hint": hint,
        }
    )


@app.route("/shops/<int:shop_id>/shop-pos/print-agent/configure", methods=["POST"])
def shop_pos_print_agent_configure(shop_id: int):
    """Enable/disable agent; generate token on first enable. Returns full token only when newly created."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    from database import get_shop_printer_settings, upsert_shop_printer_settings

    row = get_shop_printer_settings(shop_id)
    if not row or (row.get("printer_type") or "").strip().lower() != "network":
        return jsonify({"ok": False, "error": "Save a network printer first."}), 400
    try:
        cfg = json.loads(row["config_json"] or "{}")
    except json.JSONDecodeError:
        cfg = {}
    data = request.get_json(force=True, silent=True) or {}
    enabled = data.get("enabled") is True or str(data.get("enabled") or "").lower() in ("1", "true", "yes")
    rotate = data.get("rotate_token") is True or str(data.get("rotate_token") or "").lower() in ("1", "true")

    new_token = None
    if enabled:
        if rotate or not (cfg.get("print_agent_token") or "").strip():
            new_token = secrets.token_hex(32)
            cfg["print_agent_token"] = new_token
        cfg["print_agent_enabled"] = True
    else:
        cfg["print_agent_enabled"] = False

    try:
        j = json.dumps(cfg, separators=(",", ":"))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Invalid config"}), 400
    if not upsert_shop_printer_settings(
        shop_id,
        printer_type="network",
        device_label=(row.get("device_label") or "")[:255],
        config_json=j,
    ):
        return jsonify({"ok": False, "error": "Could not save"}), 500
    out = {"ok": True, "enabled": bool(cfg.get("print_agent_enabled"))}
    if new_token:
        out["token"] = new_token
        out["message"] = "Store this token in the print agent environment; it will not be shown again."
    return jsonify(out)


@app.route("/shops/<int:shop_id>/shop-pos/print-agent/jobs", methods=["GET"])
def shop_pos_print_agent_poll_jobs(shop_id: int):
    """LAN agent polls for pending ESC/POS jobs (Bearer agent token)."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_print_agent_token(shop_id)
    if gate is not None:
        return gate
    import base64

    from database import list_pending_shop_print_agent_jobs

    rows = list_pending_shop_print_agent_jobs(shop_id, limit=20)
    jobs = []
    for r in rows:
        payload = r.get("payload") or b""
        jobs.append(
            {
                "id": int(r["id"]),
                "data_b64": base64.b64encode(payload).decode("ascii"),
            }
        )
    return jsonify({"ok": True, "jobs": jobs})


@app.route("/shops/<int:shop_id>/shop-pos/print-agent/jobs/<int:job_id>/ack", methods=["POST"])
def shop_pos_print_agent_ack_job(shop_id: int, job_id: int):
    """Agent confirms print succeeded or failed."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_print_agent_token(shop_id)
    if gate is not None:
        return gate
    from database import ack_shop_print_agent_job

    data = request.get_json(force=True, silent=True) or {}
    failed = data.get("ok") is False or data.get("failed") is True
    err = (data.get("error") or "").strip()[:500]
    ok = ack_shop_print_agent_job(shop_id, job_id, failed=failed, error_message=err if failed else None)
    if not ok:
        return jsonify({"ok": False, "error": "Job not found or already finalized."}), 404
    return jsonify({"ok": True})


@app.route("/shops/<int:shop_id>/shop-pos/printer/qr-escpos", methods=["POST"])
def shop_pos_printer_qr_escpos(shop_id: int):
    """ESC/POS bytes for a QR as a raster bitmap (same path as python-escpos image QR).

    Many Bluetooth thermal printers ignore or mishandle GS (k) native QR; bitmap output
    matches typical network/USB ESC/POS behavior and scans reliably.
    """
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    data = request.get_json(force=True, silent=True) or {}
    text = (data.get("text") or "").strip()
    if not text:
        return jsonify({"ok": False, "error": "Missing text"}), 400
    if len(text) > 2000:
        return jsonify({"ok": False, "error": "Text too long"}), 400
    try:
        from escpos.constants import QR_ECLEVEL_M
        from escpos.printer import Dummy
    except ImportError:
        return jsonify({"ok": False, "error": "QR rendering unavailable (install python-escpos, qrcode, pillow)"}), 503
    try:
        d = Dummy()
        d.qr(text, ec=QR_ECLEVEL_M, size=3, native=False, center=True)
        raw: bytes = d.output
    except Exception:
        logger.exception("shop_pos_printer_qr_escpos failed")
        return jsonify({"ok": False, "error": "Could not build QR"}), 500
    return Response(raw, mimetype="application/octet-stream")


@app.route("/shops/<int:shop_id>/role-preview", methods=["POST"])
def shop_role_preview_set(shop_id: int):
    """Super admin / IT: simulate a branch role in the shop UI (session flag; APIs remain elevated)."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    actual = str(session.get("employee_role") or "").strip().lower()
    if actual not in COMPANY_PORTAL_ROLES:
        abort(403)
    role = (request.form.get("role") or "").strip().lower()
    next_url = (request.form.get("next") or "").strip()
    if role in ("", "clear", "actual", "none"):
        session.pop("shop_role_preview", None)
    elif role in SHOP_UI_PREVIEW_ROLE_SET:
        session["shop_role_preview"] = role
    else:
        flash("Invalid preview role.", "error")
    if _safe_login_next(next_url):
        return redirect(next_url)
    return redirect(url_for("shop_dashboard", shop_id=shop_id))


@app.route("/shops/<int:shop_id>/shop-dashboard")
def shop_dashboard(shop_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    day_opening = _shop_day_opening_boot_payload(shop)
    return render_template(
        "shop_dashboard.html",
        shop=shop,
        pos_allow_credit_sale=_shop_pos_allow_credit_sale(shop),
        pos_inventory_mode=_pos_inventory_mode(shop),
        shop_day_opening_boot=day_opening,
        shop_day_opening_api=url_for("shop_pos_day_opening_submit", shop_id=shop_id),
        shop_day_closing_api=url_for("shop_pos_day_closing_submit", shop_id=shop_id),
        theme_key=f"richcom-theme-shop-{shop['id']}",
        theme_default=shop.get("default_theme") or "dark",
        font_family=shop.get("font_family") or "Plus Jakarta Sans",
        primary_color_rgb=_hex_to_rgb_triplet(shop.get("primary_color") or "#10b981"),
        accent_color_rgb=_hex_to_rgb_triplet(shop.get("accent_color") or "#14b8a6"),
    )


@app.route("/shops/<int:shop_id>/shop-expenses/search.json")
def shop_expenses_search_json(shop_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    kind = (request.args.get("kind") or "items").strip().lower()
    q = (request.args.get("q") or "").strip()
    category = (request.args.get("category") or "").strip()
    category_id = request.args.get("category_id", type=int)
    req_limit = request.args.get("limit", type=int)
    try:
        from database import init_operational_expense_tables, search_expense_catalog_items, search_expense_categories

        init_operational_expense_tables()
        if kind == "categories":
            rows = search_expense_categories(q or None, limit=min(req_limit or 25, 200))
            payload = [{"id": int(r["id"]), "name": r.get("name") or ""} for r in rows]
        else:
            rows = search_expense_catalog_items(
                category_id=category_id,
                category_name=category or None,
                query=q or None,
                limit=min(req_limit or 25, 100),
            )
            payload = [
                {
                    "id": int(r["id"]),
                    "name": r.get("name") or "",
                    "category_id": int(r.get("category_id") or 0),
                    "category_name": r.get("category_name") or "",
                }
                for r in rows
            ]
    except Exception:
        payload = []
    return jsonify({"ok": True, "results": payload})


@app.route("/shops/<int:shop_id>/shop-expenses", methods=["GET", "POST"])
def shop_expenses(shop_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    if not shop_shell_can("expenses"):
        flash("You do not have access to shop expenses.", "error")
        return redirect(url_for("shop_dashboard", shop_id=shop_id))

    if request.method == "POST":
        employee_code = (request.form.get("employee_code") or "").strip()
        emp_row, emp_err, emp_status = _shop_pos_validate_employee_code(shop_id, employee_code)
        if emp_err:
            flash(emp_err, "error")
            return redirect(url_for("shop_expenses", shop_id=shop_id))
        try:
            from database import init_operational_expense_tables, register_shop_operational_expense

            init_operational_expense_tables()
            ok, _, err_msg = register_shop_operational_expense(
                shop_id=shop_id,
                category_name=(request.form.get("expense_category") or "").strip().upper(),
                expense_name=(request.form.get("expense_name") or "").strip().upper(),
                qty=(request.form.get("qty") or "").strip(),
                unit_price=(request.form.get("unit_price") or "").strip(),
                seller_phone=(request.form.get("seller_phone") or "").strip(),
                supplier_name=(request.form.get("seller_name") or "").strip().upper(),
                note=(request.form.get("note") or "").strip().upper() or None,
                created_by_employee_id=int(emp_row["id"]),
                payment_status="pending_payment",
            )
        except Exception:
            ok = False
            err_msg = "Could not register expense."
        flash(
            "Expense registered." if ok else (err_msg or "Could not register expense."),
            "success" if ok else "error",
        )
        return redirect(url_for("shop_expenses", shop_id=shop_id))

    analytics_filter = _build_analytics_filter()
    expense_rows: list = []
    expense_totals = {"total_cost": 0.0, "amount_paid": 0.0, "balance": 0.0, "count": 0}
    categories: list = []
    try:
        from database import (
            init_operational_expense_tables,
            list_shop_operational_expenses,
            list_shop_stock_purchases,
            search_expense_categories,
        )

        init_operational_expense_tables()
        op_rows = list_shop_operational_expenses(
            shop_id=shop_id, analytics_filter=analytics_filter, limit=8000
        ) or []
        stock_rows = list_shop_stock_purchases(
            shop_id=shop_id, analytics_filter=analytics_filter, limit=8000
        ) or []
        expense_rows = op_rows + stock_rows
        expense_rows.sort(
            key=lambda r: (str(r.get("created_at_iso") or r.get("created_at") or ""), int(r.get("id") or 0)),
            reverse=True,
        )
        categories = search_expense_categories(limit=200) or []
        expense_totals["total_cost"] = sum(float(r.get("total_cost") or 0) for r in expense_rows)
        expense_totals["amount_paid"] = sum(float(r.get("amount_paid") or 0) for r in expense_rows)
        expense_totals["balance"] = sum(float(r.get("balance") or 0) for r in expense_rows)
        expense_totals["count"] = len(expense_rows)
    except Exception:
        logger.exception("shop_expenses list failed shop_id=%s", shop_id)

    return render_template(
        "shop_expenses.html",
        shop=shop,
        analytics_filter=analytics_filter,
        expense_rows=expense_rows,
        expense_totals=expense_totals,
        expense_categories=categories,
        pos_allow_credit_sale=_shop_pos_allow_credit_sale(shop),
        pos_inventory_mode=_pos_inventory_mode(shop),
        theme_key=f"richcom-theme-shop-{shop['id']}",
        theme_default=shop.get("default_theme") or "dark",
        font_family=shop.get("font_family") or "Plus Jakarta Sans",
        primary_color_rgb=_hex_to_rgb_triplet(shop.get("primary_color") or "#10b981"),
        accent_color_rgb=_hex_to_rgb_triplet(shop.get("accent_color") or "#14b8a6"),
    )


@app.route("/shops/<int:shop_id>/shop-expenses/<int:expense_id>/payment", methods=["POST"])
def shop_expenses_payment_update(shop_id: int, expense_id: int):
    """Record partial or full payment against a shop operational expense."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    if not shop_shell_can("expenses"):
        return jsonify({"ok": False, "error": "You do not have access to shop expenses."}), 403

    wants_json = _request_wants_json()
    data = request.get_json(force=True, silent=True) if wants_json else None
    if not isinstance(data, dict):
        data = request.form

    pay_mode = _request_data_str(data, "pay_mode").lower()
    add_raw = _request_data_str(data, "additional_payment")
    amount_raw = _request_data_str(data, "amount_paid")

    try:
        from database import init_operational_expense_tables, update_shop_operational_expense_payment

        init_operational_expense_tables()
        if pay_mode == "full":
            from database import get_cursor

            with get_cursor() as cur:
                cur.execute(
                    """
                    SELECT total_amount, COALESCE(amount_paid, 0) AS amount_paid
                    FROM shop_operational_expenses
                    WHERE id=%s AND shop_id=%s
                    LIMIT 1
                    """,
                    (int(expense_id), int(shop_id)),
                )
                exp_row = cur.fetchone()
            if not exp_row:
                return jsonify({"ok": False, "error": "Expense not found."}), 404
            total = round(float(exp_row.get("total_amount") or 0), 2)
            row = update_shop_operational_expense_payment(
                shop_id, expense_id, amount_paid=total
            )
        elif add_raw != "":
            try:
                additional = float(add_raw)
            except Exception:
                return jsonify({"ok": False, "error": "Invalid payment amount."}), 400
            if additional <= 0:
                return jsonify({"ok": False, "error": "Payment amount must be greater than zero."}), 400
            row = update_shop_operational_expense_payment(
                shop_id, expense_id, additional_payment=additional
            )
        elif amount_raw != "":
            try:
                amount_paid = float(amount_raw)
            except Exception:
                return jsonify({"ok": False, "error": "Invalid amount paid."}), 400
            if amount_paid < 0:
                return jsonify({"ok": False, "error": "Amount paid cannot be negative."}), 400
            row = update_shop_operational_expense_payment(
                shop_id, expense_id, amount_paid=amount_paid
            )
        else:
            return jsonify({"ok": False, "error": "Enter a payment amount."}), 400
    except Exception:
        logger.exception("shop_expenses_payment_update failed shop_id=%s expense_id=%s", shop_id, expense_id)
        return jsonify({"ok": False, "error": "Could not update payment."}), 500

    if not row:
        return jsonify({"ok": False, "error": "Expense not found."}), 404
    return jsonify({"ok": True, "row": row})


@app.route(
    "/it_support/company-operational-expenses/<int:shop_id>/<int:expense_id>/payment",
    methods=["POST"],
)
@login_required
def it_support_company_operational_expense_payment(shop_id: int, expense_id: int):
    """Record partial or full payment against a shop operational expense (company portal)."""
    _it_support_only()
    data = request.get_json(force=True, silent=True) if _request_wants_json() else None
    if not isinstance(data, dict):
        data = request.form

    pay_mode = _request_data_str(data, "pay_mode").lower()
    add_raw = _request_data_str(data, "additional_payment")
    amount_raw = _request_data_str(data, "amount_paid")

    try:
        sid = int(shop_id)
        eid = int(expense_id)
    except Exception:
        return jsonify({"ok": False, "error": "Invalid expense reference."}), 400
    if sid <= 0 or eid <= 0:
        return jsonify({"ok": False, "error": "Invalid expense reference."}), 400

    try:
        from database import init_operational_expense_tables, update_shop_operational_expense_payment

        init_operational_expense_tables()
        if pay_mode == "full":
            from database import get_cursor

            with get_cursor() as cur:
                cur.execute(
                    """
                    SELECT total_amount, COALESCE(amount_paid, 0) AS amount_paid
                    FROM shop_operational_expenses
                    WHERE id=%s AND shop_id=%s
                    LIMIT 1
                    """,
                    (eid, sid),
                )
                exp_row = cur.fetchone()
            if not exp_row:
                return jsonify({"ok": False, "error": "Expense not found."}), 404
            total = round(float(exp_row.get("total_amount") or 0), 2)
            row = update_shop_operational_expense_payment(sid, eid, amount_paid=total)
        elif add_raw != "":
            try:
                additional = float(add_raw)
            except Exception:
                return jsonify({"ok": False, "error": "Invalid payment amount."}), 400
            if additional <= 0:
                return jsonify({"ok": False, "error": "Payment amount must be greater than zero."}), 400
            row = update_shop_operational_expense_payment(
                sid, eid, additional_payment=additional
            )
        elif amount_raw != "":
            try:
                amount_paid = float(amount_raw)
            except Exception:
                return jsonify({"ok": False, "error": "Invalid amount paid."}), 400
            if amount_paid < 0:
                return jsonify({"ok": False, "error": "Amount paid cannot be negative."}), 400
            row = update_shop_operational_expense_payment(sid, eid, amount_paid=amount_paid)
        else:
            return jsonify({"ok": False, "error": "Enter a payment amount."}), 400
    except Exception:
        logger.exception(
            "it_support_company_operational_expense_payment failed shop_id=%s expense_id=%s",
            shop_id,
            expense_id,
        )
        return jsonify({"ok": False, "error": "Could not update payment."}), 500

    if not row:
        return jsonify({"ok": False, "error": "Expense not found."}), 404
    return jsonify({"ok": True, "row": row})


@app.route("/shops/<int:shop_id>/shop-report")
def shop_report(shop_id: int):
    """Single-shop daily report: till balances, revenue vs expense, and items sold."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    if not _shop_report_has_filter_params():
        return redirect(
            url_for(
                "shop_report",
                shop_id=shop_id,
                mode="single_day",
                single_day=date.today().isoformat(),
            )
        )
    analytics_filter = _build_analytics_filter()
    analytics_scope = _analytics_scope_from_request()
    report_data = {
        "total_revenue": 0.0,
        "sale_revenue": 0.0,
        "credit_revenue": 0.0,
        "cash_revenue": 0.0,
        "mpesa_revenue": 0.0,
        "total_expenditure": 0.0,
        "paid_expenditure": 0.0,
        "balance_expenditure": 0.0,
        "paid_credit": 0.0,
        "unpaid_credit": 0.0,
        "collected_revenue": 0.0,
        "net_profit": 0.0,
        "stock_cost_sold": 0.0,
        "stock_cost_stock_out": 0.0,
        "stock_cost_total": 0.0,
        "stock_cost_sale_only": 0.0,
        "stock_cost_credit_total": 0.0,
        "accrual_cogs": 0.0,
        "accrual_cogs_sale": 0.0,
        "accrual_cogs_credit": 0.0,
        "accrual_cogs_stock_out": 0.0,
        "accrual_gross_profit": 0.0,
        "accrual_net_profit": 0.0,
        "accrual_net_profit_collected": 0.0,
        "accrual_operating_expenses": 0.0,
        "estimated_sale_gross_profit": 0.0,
        "items": [],
    }
    try:
        from database import get_shop_report

        report_data = get_shop_report(
            shop_id=int(shop_id),
            analytics_filter=analytics_filter,
            analytics_scope=analytics_scope,
        )
    except Exception:
        logger.exception("shop_report failed shop_id=%s", shop_id)
    report_should_print = (request.args.get("print") or "").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    share_qs = request.query_string.decode("utf-8") if request.query_string else ""
    share_path = url_for("shop_report", shop_id=shop_id)
    share_url = f"{_public_share_base_url()}{share_path}"
    if share_qs:
        share_url = f"{share_url}?{share_qs}"
    report_json_url = url_for("shop_report_json", shop_id=shop_id)
    if share_qs:
        report_json_url = f"{report_json_url}?{share_qs}"
    report_mode = (analytics_filter.get("mode") or "single_day").strip().lower()
    report_business_date = (analytics_filter.get("single_day") or date.today().isoformat()).strip()
    day_opening_boot = _shop_day_opening_boot_payload(shop)
    report_closing_ctx = (
        _shop_day_closing_payload(shop, report_business_date)
        if report_mode == "single_day"
        else None
    )
    report_is_single_day = report_mode == "single_day"
    report_show_opening_link = bool(
        report_is_single_day
        and report_business_date == date.today().isoformat()
        and not day_opening_boot.get("completed")
    )
    report_show_closing_link = bool(
        report_is_single_day
        and report_business_date == date.today().isoformat()
        and day_opening_boot.get("completed")
        and not day_opening_boot.get("today_closed")
        and _shop_session_can_submit_day_closing()
    )
    report_can_submit_opening = bool(
        report_show_opening_link and day_opening_boot.get("can_submit")
    )
    report_can_submit_closing = report_show_closing_link
    report_till_actions_enabled = report_is_single_day
    report_closing_submitted = bool(
        report_business_date == date.today().isoformat()
        and day_opening_boot.get("today_closed")
    ) or bool(
        report_closing_ctx and report_closing_ctx.get("submitted")
    )
    report_has_closing_pending = bool(
        report_show_closing_link
        and report_closing_ctx
        and not report_closing_ctx.get("submitted")
    )
    closing_reminder_boot = {
        "business_date": report_business_date,
        "can_submit": _shop_session_can_submit_day_closing(),
        "can_close_today": report_show_closing_link,
        "requires_employee_code": True,
        "pending": report_closing_ctx if (report_closing_ctx and not report_closing_ctx.get("submitted")) else None,
        "auto_show": False,
        "close_time": day_opening_boot.get("closing_reminder", {}).get("close_time") or "",
        "minutes_until_close": day_opening_boot.get("closing_reminder", {}).get("minutes_until_close"),
    }
    return render_template(
        "shop_report.html",
        shop=shop,
        analytics_filter=analytics_filter,
        analytics_scope=analytics_scope,
        report_data=report_data,
        items_sold_rows=report_data.get("items_sold") or [],
        expenditure_rows=report_data.get("expenditure_rows") or [],
        report_share_url=share_url,
        report_json_url=report_json_url,
        report_live_enabled=_shop_report_live_enabled(analytics_filter),
        report_period_label=_shop_report_period_label(analytics_filter),
        report_today_iso=date.today().isoformat(),
        report_business_date=report_business_date,
        report_is_single_day=report_is_single_day,
        report_show_opening_link=report_show_opening_link,
        report_show_closing_link=report_show_closing_link,
        report_can_submit_closing=report_can_submit_closing,
        report_can_submit_opening=report_can_submit_opening,
        report_till_actions_enabled=report_till_actions_enabled,
        report_closing_submitted=report_closing_submitted,
        report_has_closing_pending=report_has_closing_pending,
        report_closing_ctx=report_closing_ctx,
        shop_day_opening_boot=day_opening_boot,
        shop_day_opening_api=url_for("shop_pos_day_opening_submit", shop_id=shop_id),
        shop_day_closing_reminder_boot=closing_reminder_boot,
        shop_day_closing_api=url_for("shop_pos_day_closing_submit", shop_id=shop_id),
        report_should_print=report_should_print,
        report_generated_at=datetime.now().strftime("%d %b %Y %H:%M"),
        pos_allow_credit_sale=_shop_pos_allow_credit_sale(shop),
        pos_inventory_mode=_pos_inventory_mode(shop),
        shop_portal=True,
        theme_key=f"richcom-theme-shop-{shop['id']}",
        theme_default=shop.get("default_theme") or "dark",
        font_family=shop.get("font_family") or "Plus Jakarta Sans",
        primary_color_rgb=_hex_to_rgb_triplet(shop.get("primary_color") or "#10b981"),
        accent_color_rgb=_hex_to_rgb_triplet(shop.get("accent_color") or "#14b8a6"),
    )


@app.route("/shops/<int:shop_id>/shop-report.json")
def shop_report_json(shop_id: int):
    """JSON snapshot of shop period report (used for live refresh on today's view)."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    analytics_filter = _build_analytics_filter()
    analytics_scope = _analytics_scope_from_request()
    report_data = {
        "total_revenue": 0.0,
        "sale_revenue": 0.0,
        "credit_revenue": 0.0,
        "cash_revenue": 0.0,
        "mpesa_revenue": 0.0,
        "total_expenditure": 0.0,
        "paid_expenditure": 0.0,
        "balance_expenditure": 0.0,
        "paid_credit": 0.0,
        "unpaid_credit": 0.0,
        "collected_revenue": 0.0,
        "net_profit": 0.0,
        "stock_cost_sold": 0.0,
        "stock_cost_stock_out": 0.0,
        "stock_cost_total": 0.0,
        "stock_cost_sale_only": 0.0,
        "stock_cost_credit_total": 0.0,
        "accrual_cogs": 0.0,
        "accrual_cogs_sale": 0.0,
        "accrual_cogs_credit": 0.0,
        "accrual_cogs_stock_out": 0.0,
        "accrual_gross_profit": 0.0,
        "accrual_net_profit": 0.0,
        "accrual_net_profit_collected": 0.0,
        "accrual_operating_expenses": 0.0,
        "estimated_sale_gross_profit": 0.0,
        "items": [],
        "items_sold": [],
        "items_sold_sale": [],
        "items_sold_credit": [],
        "expenditure_rows": [],
        "till_summary": {},
    }
    try:
        from database import get_shop_report

        report_data = get_shop_report(
            shop_id=int(shop_id),
            analytics_filter=analytics_filter,
            analytics_scope=analytics_scope,
        )
    except Exception:
        logger.exception("shop_report_json failed shop_id=%s", shop_id)
    return jsonify(
        {
            "ok": True,
            "report": report_data,
            "generated_at": datetime.now().strftime("%d %b %Y %H:%M"),
            "live_enabled": _shop_report_live_enabled(analytics_filter),
        }
    )


@app.route("/shops/<int:shop_id>/shop-report.pdf")
def shop_report_pdf(shop_id: int):
    """Download shop period report as PDF (same filters as HTML page)."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    analytics_filter = _build_analytics_filter()
    analytics_scope = _analytics_scope_from_request()
    report_data = {
        "total_revenue": 0.0,
        "sale_revenue": 0.0,
        "credit_revenue": 0.0,
        "cash_revenue": 0.0,
        "mpesa_revenue": 0.0,
        "total_expenditure": 0.0,
        "net_profit": 0.0,
        "items": [],
    }
    try:
        from database import get_shop_report

        report_data = get_shop_report(
            shop_id=int(shop_id),
            analytics_filter=analytics_filter,
            analytics_scope=analytics_scope,
        )
    except Exception:
        logger.exception("shop_report_pdf failed shop_id=%s", shop_id)
    generated_at = datetime.now().strftime("%d %b %Y %H:%M")
    scope_label = (
        "Confirmed receipts only"
        if (analytics_scope or "general") == "actual"
        else "All receipt marks"
    )
    try:
        from report_pdf import build_period_report_pdf

        pdf_bytes = build_period_report_pdf(
            title="Shop report",
            entity_name=(shop.get("shop_name") or f"Shop {shop_id}"),
            period_label=(analytics_filter or {}).get("range_label") or "Today",
            scope_label=scope_label,
            report_data=report_data,
            generated_at=generated_at,
            primary_color=(shop.get("primary_color") or "").strip() or None,
        )
    except Exception:
        logger.exception("shop_report_pdf build failed shop_id=%s", shop_id)
        abort(500, description="Could not generate PDF for this report.")
    shop_slug = secure_filename(
        str(shop.get("shop_name") or f"shop-{shop_id}").replace(" ", "-").lower()
    )
    filename = secure_filename(f"{shop_slug}-report.pdf")
    return Response(
        pdf_bytes,
        mimetype="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.route("/shops/<int:shop_id>/shop-receipts")
def shop_receipts(shop_id: int):
    """POS receipts register: persisted sale/credit rows with date scopes (default today)."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    analytics_filter = _build_analytics_filter()
    rq = (request.args.get("receipt_q") or "").strip()
    try:
        from database import list_shop_pos_sales_receipt_rows

        rows = list_shop_pos_sales_receipt_rows(
            int(shop_id),
            analytics_filter,
            sale_id_search=rq,
            limit=2000,
        )
    except Exception:
        logger.exception("shop_receipts list failed shop_id=%s", shop_id)
        rows = []
    return render_template(
        "shop_receipts.html",
        shop=shop,
        analytics_filter=analytics_filter,
        analytics_scope=_analytics_scope_from_request(),
        receipt_rows=rows,
        receipt_rows_total=len(rows),
        receipt_q=rq,
        can_mark_receipts=shop_shell_can("receipt_mark"),
        pos_allow_credit_sale=_shop_pos_allow_credit_sale(shop),
        pos_inventory_mode=_pos_inventory_mode(shop),
        shop_portal=True,
        theme_key=f"richcom-theme-shop-{shop['id']}",
        theme_default=shop.get("default_theme") or "dark",
        font_family=shop.get("font_family") or "Plus Jakarta Sans",
        primary_color_rgb=_hex_to_rgb_triplet(shop.get("primary_color") or "#10b981"),
        accent_color_rgb=_hex_to_rgb_triplet(shop.get("accent_color") or "#14b8a6"),
    )


@app.route("/shops/<int:shop_id>/shop-receipts/list-json")
def shop_receipts_list_json(shop_id: int):
    """JSON list for POS receipts popup (defaults to today's rows)."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    analytics_filter = _build_analytics_filter()
    rq = (request.args.get("receipt_q") or "").strip()
    lim_raw = request.args.get("limit", type=int)
    lim = int(lim_raw) if lim_raw is not None else 2000
    lim = max(1, min(lim, 5000))
    try:
        from database import list_shop_pos_sales_receipt_rows

        rows = list_shop_pos_sales_receipt_rows(
            int(shop_id),
            analytics_filter,
            sale_id_search=rq,
            limit=lim,
        )
    except Exception:
        logger.exception("shop_receipts_list_json failed shop_id=%s", shop_id)
        return jsonify({"ok": False, "error": "Could not load receipts."}), 500
    return jsonify(
        {
            "ok": True,
            "rows": rows or [],
            "range_label": analytics_filter.get("range_label") or "",
        }
    )


@app.route("/shops/<int:shop_id>/shop-receipts/mark", methods=["POST"])
def shop_receipts_mark(shop_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    role_key = (session.get("employee_role") or "").strip().lower()
    if role_key not in ("admin", "manager", "super_admin", "it_support", "company_manager"):
        return jsonify({"ok": False, "error": "Only admin or manager can mark receipts."}), 403
    data = request.get_json(force=True, silent=True) or {}
    raw_ids = data.get("sale_ids")
    mark_status = (data.get("mark_status") or "").strip().lower()
    if not isinstance(raw_ids, list):
        return jsonify({"ok": False, "error": "Select at least one receipt."}), 400
    try:
        from database import bulk_mark_shop_pos_receipts

        affected = bulk_mark_shop_pos_receipts(
            sale_ids=raw_ids,
            mark_status=mark_status,
            shop_id=int(shop_id),
        )
    except Exception:
        logger.exception("shop_receipts_mark failed shop_id=%s", shop_id)
        return jsonify({"ok": False, "error": "Could not update receipt marks."}), 500
    if affected <= 0:
        return jsonify({"ok": False, "error": "No receipts were updated."}), 400
    return jsonify({"ok": True, "updated": int(affected), "mark_status": mark_status})


@app.route("/shops/<int:shop_id>/shop-receipts/detail")
def shop_receipts_detail(shop_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    sale_id = request.args.get("sale_id", type=int)
    if not sale_id:
        return jsonify({"ok": False, "error": "Missing receipt reference."}), 400
    try:
        from database import get_shop_pos_sale_detail

        d = get_shop_pos_sale_detail(shop_id=int(shop_id), sale_id=int(sale_id)) or {}
    except Exception:
        logger.exception("shop_receipts_detail failed shop_id=%s sale_id=%s", shop_id, sale_id)
        return jsonify({"ok": False, "error": "Could not load receipt detail."}), 500
    if not d.get("sale"):
        return jsonify({"ok": False, "error": "Receipt not found."}), 404
    return jsonify({"ok": True, "sale": d.get("sale") or {}, "items": d.get("items") or []})


@app.route("/shops/<int:shop_id>/shop-receipts/return-lines", methods=["POST"])
def shop_receipts_return_lines(shop_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    role_key = (session.get("employee_role") or "").strip().lower()
    if role_key not in ("admin", "manager", "super_admin", "it_support", "company_manager"):
        return jsonify({"ok": False, "error": "Only admin or manager can return items."}), 403
    data = request.get_json(force=True, silent=True) or {}
    sale_id = int(data.get("sale_id") or 0)
    line_ids = data.get("line_ids")
    if sale_id <= 0:
        return jsonify({"ok": False, "error": "Invalid receipt reference."}), 400
    if not isinstance(line_ids, list):
        return jsonify({"ok": False, "error": "Select at least one item to return."}), 400
    try:
        from database import return_shop_pos_sale_lines

        ok, err, meta = return_shop_pos_sale_lines(
            shop_id=int(shop_id),
            sale_id=sale_id,
            line_ids=line_ids,
        )
    except Exception:
        logger.exception("shop_receipts_return_lines failed shop_id=%s sale_id=%s", shop_id, sale_id)
        return jsonify({"ok": False, "error": "Could not process return."}), 500
    if not ok:
        return jsonify({"ok": False, "error": err or "Could not process return."}), 400
    return jsonify({"ok": True, "meta": meta or {}})


@app.route("/shops/<int:shop_id>/kitchen-portion-analytics")
def shop_kitchen_portion_analytics(shop_id: int):
    """Kitchen portion sales for this shop only (same data as IT analytics, shop session access)."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    if _pos_inventory_mode(shop) not in ("kitchen", "both"):
        return redirect(url_for("shop_dashboard", shop_id=shop_id))
    from database import (
        kitchen_portion_analytics_by_day,
        kitchen_portion_analytics_by_item,
        list_kitchen_portion_analytics_lines,
    )

    params = _it_support_kitchen_analytics_params()
    params["shop_id"] = shop_id
    df = params["date_from"]
    dt = params["date_to"]

    raw_lines = list_kitchen_portion_analytics_lines(
        date_from=df,
        date_to=dt,
        shop_id=shop_id,
        limit=50000,
    )
    by_item = kitchen_portion_analytics_by_item(
        date_from=df,
        date_to=dt,
        shop_id=shop_id,
        limit=40,
    )
    by_day = kitchen_portion_analytics_by_day(
        date_from=df,
        date_to=dt,
        shop_id=shop_id,
    )

    tab_raw_url = _shop_kitchen_analytics_tab_url(shop_id, params, "raw")
    tab_visuals_url = _shop_kitchen_analytics_tab_url(shop_id, params, "visuals")

    return render_template(
        "shop_kitchen_portion_analytics.html",
        shop=shop,
        analytics=params,
        raw_lines=raw_lines,
        chart_by_item=by_item,
        chart_by_day=by_day,
        tab_raw_url=tab_raw_url,
        tab_visuals_url=tab_visuals_url,
        pos_allow_credit_sale=_shop_pos_allow_credit_sale(shop),
        pos_inventory_mode=_pos_inventory_mode(shop),
        theme_key=f"richcom-theme-shop-{shop['id']}",
        theme_default=shop.get("default_theme") or "dark",
        font_family=shop.get("font_family") or "Plus Jakarta Sans",
        primary_color_rgb=_hex_to_rgb_triplet(shop.get("primary_color") or "#10b981"),
        accent_color_rgb=_hex_to_rgb_triplet(shop.get("accent_color") or "#14b8a6"),
    )


@app.route("/shops/<int:shop_id>/shop-kitchen-portions", methods=["GET", "POST"])
def shop_kitchen_portions(shop_id: int):
    """Kitchen portion quantities for this shop (POS kitchen mode)."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    if _pos_inventory_mode(shop) not in ("kitchen", "both"):
        return redirect(url_for("shop_dashboard", shop_id=shop_id))

    if request.method == "POST":
        try:
            from database import upsert_shop_kitchen_portion_qty

            for key, raw in request.form.items():
                if not key.startswith("portion_"):
                    continue
                try:
                    iid = int(key.replace("portion_", "", 1))
                except (TypeError, ValueError):
                    continue
                try:
                    q = int(str(raw or "").strip() or "0")
                except (TypeError, ValueError):
                    q = 0
                try:
                    price = round(
                        float(str(request.form.get(f"prod_price_{iid}", "") or "").strip() or "0"),
                        2,
                    )
                except (TypeError, ValueError):
                    price = 0.0
                if price < 0:
                    price = 0.0
                upsert_shop_kitchen_portion_qty(shop_id, iid, q, estimated_production_price=price)
            flash("Kitchen portions updated.", "success")
        except Exception:
            flash("Could not save kitchen portions.", "error")
        return redirect(url_for("shop_kitchen_portions", shop_id=shop_id))

    kitchen_rows = []
    try:
        from database import ensure_shop_items_for_shop, list_shop_kitchen_portion_editor_rows

        ensure_shop_items_for_shop(shop_id)
        kitchen_rows = list_shop_kitchen_portion_editor_rows(
            shop_id=shop_id, limit=5000, only_displayed_on_pos=False
        )
    except Exception:
        kitchen_rows = []

    return render_template(
        "shop_kitchen_portions.html",
        shop=shop,
        pos_allow_credit_sale=_shop_pos_allow_credit_sale(shop),
        pos_inventory_mode=_pos_inventory_mode(shop),
        kitchen_portion_rows=kitchen_rows,
        theme_key=f"richcom-theme-shop-{shop['id']}",
        theme_default=shop.get("default_theme") or "dark",
        font_family=shop.get("font_family") or "Plus Jakarta Sans",
        primary_color_rgb=_hex_to_rgb_triplet(shop.get("primary_color") or "#10b981"),
        accent_color_rgb=_hex_to_rgb_triplet(shop.get("accent_color") or "#14b8a6"),
    )


@app.route("/shops/<int:shop_id>/shop-credit-payments")
def shop_credit_payments(shop_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    if not _shop_pos_allow_credit_sale(shop):
        return redirect(url_for("shop_dashboard", shop_id=shop_id))
    role_key = (session.get("employee_role") or "").strip().lower()
    company_credit_mode = role_key == "admin"
    try:
        if company_credit_mode:
            from database import list_company_credit_customers

            rows = list_company_credit_customers(limit=2000, analytics_filter=None, shop_id=None, customer_q=None)
            customers = [
                {
                    "customer_name": r.get("customer_name") or "WALK IN",
                    "customer_phone": r.get("customer_phone") or "-",
                    "tx_count": int(r.get("tx_count") or 0),
                    "credit_total": float(r.get("total_amount") or 0),
                    "paid_total": float(r.get("paid_amount") or 0),
                    "balance": float(r.get("remaining_amount") or 0),
                }
                for r in (rows or [])
            ]
        else:
            from database import list_shop_credit_customers_with_balance

            customers = list_shop_credit_customers_with_balance(shop_id=shop_id, limit=2000)
    except Exception:
        customers = []
    return render_template(
        "shop_credit_payments.html",
        shop=shop,
        customers=customers,
        company_credit_mode=company_credit_mode,
        theme_key=f"richcom-theme-shop-{shop['id']}",
        theme_default=shop.get("default_theme") or "dark",
        font_family=shop.get("font_family") or "Plus Jakarta Sans",
        primary_color_rgb=_hex_to_rgb_triplet(shop.get("primary_color") or "#10b981"),
        accent_color_rgb=_hex_to_rgb_triplet(shop.get("accent_color") or "#14b8a6"),
    )


def _all_time_analytics_filter() -> dict:
    """Wide date range for full customer account credit note."""
    return {
        "mode": "period",
        "single_day": date.today().isoformat(),
        "start_date": "2000-01-01",
        "end_date": "2099-12-31",
        "month": date.today().strftime("%Y-%m"),
        "year": str(date.today().year),
        "range_start": "2000-01-01",
        "range_end_exclusive": "2100-01-01",
        "range_label": "Full account",
    }


def _credit_sale_is_unpaid(sale_id: int, credit_acct_by_id: dict) -> bool:
    acct = credit_acct_by_id.get(int(sale_id) or 0)
    if not acct:
        return False
    if (acct.get("credit_status") or "").strip().lower() == "paid":
        return False
    return float(acct.get("remaining_amount") or 0) > 0.0001


def _credit_item_pick_date(val) -> str:
    """Format credit sale date for unpaid item rows."""
    if val is None:
        return ""
    if isinstance(val, datetime):
        return val.strftime("%d %b %Y")
    s = str(val).strip()
    if not s:
        return ""
    if len(s) >= 10 and s[4:5] == "-":
        try:
            return datetime.strptime(s[:10], "%Y-%m-%d").strftime("%d %b %Y")
        except ValueError:
            pass
    return s[:16] if len(s) > 16 else s


def _credit_note_unpaid_lists(
    credit_sales_period: list,
    credit_acct_by_id: dict,
    items_by_sale_id: dict,
) -> tuple:
    """Outstanding credit sales and line items for the credit note."""
    unpaid: list = []
    for tx in credit_sales_period or []:
        try:
            sale_id = int(tx.get("id") or 0)
        except Exception:
            sale_id = 0
        if sale_id <= 0 or not _credit_sale_is_unpaid(sale_id, credit_acct_by_id):
            continue
        unpaid.append(tx)
    unpaid_ids = {int(tx.get("id") or 0) for tx in unpaid if int(tx.get("id") or 0) > 0}
    note_items = {k: v for k, v in (items_by_sale_id or {}).items() if k in unpaid_ids}
    unpaid_balance = sum(
        float(credit_acct_by_id.get(int(tx.get("id") or 0), {}).get("remaining_amount") or 0)
        for tx in unpaid
    )
    return unpaid, note_items, unpaid_balance, len(unpaid)


def _shop_customer_credit_note_context(
    shop_id: int,
    customer_name: str,
    customer_phone: str,
    *,
    analytics_filter: Optional[dict] = None,
    analytics_scope: str = "general",
) -> dict:
    """Shared template context for shop customer credit note (period or full account)."""
    f = analytics_filter if analytics_filter is not None else _all_time_analytics_filter()
    all_time = f.get("range_label") == "Full account"
    items_by_sale_id: Dict[int, list] = {}
    credit_account_txs: list = []
    credit_payments_all: list = []
    credit_sales_period: list = []
    credit_payments_period: list = []
    try:
        from database import (
            get_it_support_customer_transaction_items,
            get_it_support_customer_transactions,
            get_shop_customer_credit_payments,
            get_shop_customer_credit_transactions,
        )

        credit_account_txs = get_shop_customer_credit_transactions(
            shop_id=shop_id,
            customer_name=customer_name,
            customer_phone=customer_phone,
            limit=5000,
        )
        credit_payments_all = get_shop_customer_credit_payments(
            shop_id=shop_id,
            customer_name=customer_name,
            customer_phone=customer_phone,
            limit=5000,
        )
        if all_time:
            credit_sales_period = [
                {
                    "id": int(t.get("id") or 0),
                    "total_amount": float(t.get("total_amount") or 0),
                    "created_at": t.get("created_at"),
                    "employee_name": t.get("employee_name") or "Unknown",
                    "employee_code": t.get("employee_code") or "",
                    "sale_type": "credit",
                }
                for t in credit_account_txs
            ]
            credit_payments_period = list(credit_payments_all)
        else:
            transactions = get_it_support_customer_transactions(
                customer_name=customer_name,
                customer_phone=customer_phone,
                analytics_filter=f,
                shop_id=shop_id,
                limit=3000,
                analytics_scope=analytics_scope,
            )
            credit_sales_period = [
                t
                for t in transactions
                if (t.get("sale_type") or "").strip().lower() == "credit"
                and _row_in_analytics_filter(t, f)
            ]
            credit_payments_period = [
                p for p in credit_payments_all if _row_in_analytics_filter(p, f)
            ]
        tx_item_rows = get_it_support_customer_transaction_items(
            customer_name=customer_name,
            customer_phone=customer_phone,
            limit=8000,
            analytics_filter=f,
            shop_id=shop_id,
            analytics_scope=analytics_scope,
        )
        for row in tx_item_rows or []:
            if (row.get("sale_type") or "").strip().lower() != "credit":
                continue
            try:
                sale_id = int(row.get("sale_id") or 0)
            except Exception:
                sale_id = 0
            if sale_id <= 0:
                continue
            items_by_sale_id.setdefault(sale_id, []).append(
                {
                    "item_name": row.get("item_name") or "Item",
                    "qty": int(row.get("qty") or 0),
                    "amount": float(row.get("amount") or 0),
                    "picked_at": _credit_item_pick_date(row.get("created_at")),
                }
            )
    except Exception:
        pass
    account_credit_total = sum(float(t.get("total_amount") or 0) for t in credit_account_txs)
    account_paid_total = sum(float(t.get("paid_amount") or 0) for t in credit_account_txs)
    account_balance_due = max(account_credit_total - account_paid_total, 0.0)
    period_credit_total = sum(float(t.get("total_amount") or 0) for t in credit_sales_period)
    period_payments_total = sum(float(p.get("amount") or 0) for p in credit_payments_period)
    credit_acct_by_id = {
        int(t.get("id") or 0): t for t in credit_account_txs if int(t.get("id") or 0) > 0
    }
    credit_sales_unpaid, credit_note_items_by_sale_id, unpaid_balance_total, unpaid_sales_count = (
        _credit_note_unpaid_lists(credit_sales_period, credit_acct_by_id, items_by_sale_id)
    )
    return {
        "f": f,
        "analytics_scope": analytics_scope,
        "transaction_items_by_sale_id": items_by_sale_id,
        "credit_note_items_by_sale_id": credit_note_items_by_sale_id,
        "credit_account_txs": credit_account_txs,
        "credit_acct_by_id": credit_acct_by_id,
        "credit_sales_period": credit_sales_period,
        "credit_sales_unpaid": credit_sales_unpaid,
        "credit_payments_period": credit_payments_period,
        "credit_payments_all": credit_payments_all,
        "account_credit_total": account_credit_total,
        "account_paid_total": account_paid_total,
        "account_balance_due": account_balance_due,
        "period_credit_total": period_credit_total,
        "period_payments_total": period_payments_total,
        "unpaid_balance_total": unpaid_balance_total,
        "unpaid_sales_count": unpaid_sales_count,
        "credit_note_ref": f"CN-{shop_id}-{datetime.utcnow().strftime('%Y%m%d')}",
        "credit_note_all_time": all_time,
        "daraja_pos_boot": _daraja_pos_boot_payload(),
        "credit_pay_link": _credit_pay_public_link(
            shop_id, customer_name, customer_phone, account_balance_due
        ),
        "credit_pay_link_full": _credit_pay_public_link_full(
            shop_id, customer_name, customer_phone, account_balance_due
        ),
        "credit_note_public_url": _credit_note_public_share_url(
            shop_id, customer_name, customer_phone, company_scope=False
        ),
    }


def _credit_note_unpaid_item_rows(note_ctx: dict, *, company_scope: bool = False) -> list:
    rows: list = []
    unpaid_sales = note_ctx.get("credit_sales_unpaid") or []
    note_items = note_ctx.get("credit_note_items_by_sale_id") or {}
    credit_acct_by_id = note_ctx.get("credit_acct_by_id") or {}
    for tx in unpaid_sales:
        try:
            sale_id = int(tx.get("id") or 0)
        except Exception:
            sale_id = 0
        sale_items = note_items.get(sale_id) or []
        pick_date = _credit_item_pick_date(tx.get("created_at"))
        if sale_items:
            for it in sale_items:
                rows.append(
                    {
                        "shop_name": (it.get("shop_name") or tx.get("shop_name") or "").strip(),
                        "picked_at": (it.get("picked_at") or pick_date or "").strip(),
                        "item_name": (it.get("item_name") or "Item").strip(),
                        "qty": int(it.get("qty") or 0),
                        "amount": float(it.get("amount") or 0),
                    }
                )
            continue
        acct = credit_acct_by_id.get(sale_id) or {}
        rows.append(
            {
                "shop_name": (tx.get("shop_name") or "").strip(),
                "picked_at": pick_date,
                "item_name": f"Credit sale #{sale_id}" if sale_id else "Credit sale",
                "qty": None,
                "amount": float(acct.get("remaining_amount") or tx.get("total_amount") or 0),
            }
        )
    if company_scope:
        return rows
    return [{k: v for k, v in r.items() if k != "shop_name"} for r in rows]


def _credit_note_pdf_response(
    *,
    shop_name: str,
    company_name: str,
    customer_name: str,
    customer_phone: str,
    note_ctx: dict,
    company_scope: bool = False,
    primary_color: str | None = None,
    focus_sale: Optional[dict] = None,
    filename_slug: str = "credit-note",
) -> Response:
    from credit_note_pdf import build_credit_note_pdf

    f = note_ctx.get("f") or {}
    all_time = bool(note_ctx.get("credit_note_all_time"))
    scope_label = (
        "Confirmed receipts only"
        if (note_ctx.get("analytics_scope") or "general") == "actual"
        else "All receipt marks"
    )
    try:
        pdf_bytes = build_credit_note_pdf(
            company_name=company_name,
            shop_name=shop_name,
            credit_note_ref=str(note_ctx.get("credit_note_ref") or "CN"),
            period_label=f.get("range_label") or "Full account",
            all_time=all_time,
            scope_label=scope_label if not all_time else "",
            customer_name=customer_name,
            customer_phone=customer_phone,
            balance_due=float(note_ctx.get("account_balance_due") or 0),
            outstanding=float(note_ctx.get("unpaid_balance_total") or 0),
            paid_to_date=float(note_ctx.get("account_paid_total") or 0),
            unpaid_sales_count=int(note_ctx.get("unpaid_sales_count") or 0),
            unpaid_items=_credit_note_unpaid_item_rows(
                note_ctx, company_scope=company_scope
            ),
            company_scope=company_scope,
            pay_link=str(
                note_ctx.get("credit_pay_link_full")
                or note_ctx.get("credit_pay_link")
                or ""
            ),
            focus_sale=focus_sale,
            generated_at=datetime.now().strftime("%d %b %Y %H:%M"),
            primary_color=primary_color,
        )
    except Exception:
        logger.exception("credit_note_pdf build failed")
        abort(500, description="Could not generate credit note PDF.")
    filename = secure_filename(f"{filename_slug}.pdf")
    return Response(
        pdf_bytes,
        mimetype="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _company_customer_credit_note_context(customer_name: str, customer_phone: str) -> dict:
    """Credit note + payments context for company-wide customer credit page."""
    f = _all_time_analytics_filter()
    credit_account_txs: list = []
    credit_payments_all: list = []
    items_by_sale_id: Dict[int, list] = {}
    try:
        from database import (
            get_company_customer_credit_items,
            get_company_customer_credit_payments,
            get_company_customer_credit_transactions,
        )

        credit_account_txs = get_company_customer_credit_transactions(
            customer_name=customer_name,
            customer_phone=customer_phone,
            limit=5000,
        )
        payments_raw = get_company_customer_credit_payments(
            customer_name=customer_name,
            customer_phone=customer_phone,
            limit=5000,
        )
        item_rows = get_company_customer_credit_items(
            customer_name=customer_name,
            customer_phone=customer_phone,
            limit=20000,
        )
        for p in payments_raw or []:
            parts = [p.get("shop_name") or ""]
            method = (p.get("payment_method") or "").strip().upper()
            if method:
                parts.append(method)
            raw_note = (p.get("note") or "").strip()
            if raw_note and raw_note.lower() not in ("method=cash", "method=mpesa", "method=bank", "method=other"):
                parts.append(raw_note)
            credit_payments_all.append(
                {
                    "amount": float(p.get("amount") or 0),
                    "created_at": p.get("created_at"),
                    "note": " · ".join(x for x in parts if x),
                }
            )
        for row in item_rows or []:
            try:
                sale_id = int(row.get("sale_id") or 0)
            except Exception:
                sale_id = 0
            if sale_id <= 0:
                continue
            items_by_sale_id.setdefault(sale_id, []).append(
                {
                    "item_name": row.get("item_name") or "Item",
                    "qty": int(row.get("qty") or 0),
                    "amount": float(row.get("line_total") or 0),
                    "shop_name": row.get("shop_name") or "",
                    "picked_at": _credit_item_pick_date(row.get("created_at")),
                }
            )
    except Exception:
        pass
    account_credit_total = sum(float(t.get("total_amount") or 0) for t in credit_account_txs)
    account_paid_total = sum(float(t.get("paid_amount") or 0) for t in credit_account_txs)
    account_balance_due = max(account_credit_total - account_paid_total, 0.0)
    credit_sales_period = [
        {
            "id": int(t.get("id") or 0),
            "total_amount": float(t.get("total_amount") or 0),
            "created_at": t.get("created_at"),
            "employee_name": t.get("employee_name") or "Unknown",
            "employee_code": t.get("employee_code") or "",
            "sale_type": "credit",
            "shop_name": t.get("shop_name") or "",
        }
        for t in credit_account_txs
    ]
    credit_payments_period = list(credit_payments_all)
    credit_acct_by_id = {
        int(t.get("id") or 0): t for t in credit_account_txs if int(t.get("id") or 0) > 0
    }
    credit_sales_unpaid, credit_note_items_by_sale_id, unpaid_balance_total, unpaid_sales_count = (
        _credit_note_unpaid_lists(credit_sales_period, credit_acct_by_id, items_by_sale_id)
    )
    return {
        "f": f,
        "analytics_scope": "general",
        "company_credit_scope": True,
        "transaction_items_by_sale_id": items_by_sale_id,
        "credit_note_items_by_sale_id": credit_note_items_by_sale_id,
        "credit_account_txs": credit_account_txs,
        "credit_acct_by_id": credit_acct_by_id,
        "credit_sales_period": credit_sales_period,
        "credit_sales_unpaid": credit_sales_unpaid,
        "credit_payments_period": credit_payments_period,
        "credit_payments_all": credit_payments_all,
        "account_credit_total": account_credit_total,
        "account_paid_total": account_paid_total,
        "account_balance_due": account_balance_due,
        "period_credit_total": account_credit_total,
        "period_payments_total": sum(float(p.get("amount") or 0) for p in credit_payments_period),
        "unpaid_balance_total": unpaid_balance_total,
        "unpaid_sales_count": unpaid_sales_count,
        "credit_note_ref": f"CN-CO-{datetime.utcnow().strftime('%Y%m%d')}",
        "credit_note_all_time": True,
        "credit_pay_link": _credit_pay_public_link(
            0, customer_name, customer_phone, account_balance_due
        ),
        "credit_pay_link_full": _credit_pay_public_link_full(
            0, customer_name, customer_phone, account_balance_due
        ),
        "credit_note_public_url": _credit_note_public_share_url(
            0, customer_name, customer_phone, company_scope=True
        ),
    }


@app.route("/shops/<int:shop_id>/shop-credit-payments/customer")
def shop_credit_payments_customer(shop_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    if not _shop_pos_allow_credit_sale(shop):
        return redirect(url_for("shop_dashboard", shop_id=shop_id))
    customer_name = (request.args.get("customer_name") or "").strip() or "WALK IN"
    customer_phone = (request.args.get("customer_phone") or "").strip() or "-"
    note_ctx = _shop_customer_credit_note_context(
        shop_id, customer_name, customer_phone, analytics_filter=None
    )
    return render_template(
        "shop_credit_payments_customer.html",
        shop=shop,
        customer_name=customer_name,
        customer_phone=customer_phone,
        embed_record_payment=True,
        payment_return_to="customer",
        pos_allow_credit_sale=True,
        **note_ctx,
        theme_key=f"richcom-theme-shop-{shop['id']}",
        theme_default=shop.get("default_theme") or "dark",
        font_family=shop.get("font_family") or "Plus Jakarta Sans",
        primary_color_rgb=_hex_to_rgb_triplet(shop.get("primary_color") or "#10b981"),
        accent_color_rgb=_hex_to_rgb_triplet(shop.get("accent_color") or "#14b8a6"),
    )


@app.route("/shops/<int:shop_id>/shop-credit-payments/customer.pdf")
def shop_credit_payments_customer_pdf(shop_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    if not _shop_pos_allow_credit_sale(shop):
        abort(404)
    customer_name = (request.args.get("customer_name") or "").strip() or "WALK IN"
    customer_phone = (request.args.get("customer_phone") or "").strip() or "-"
    note_ctx = _shop_customer_credit_note_context(
        shop_id, customer_name, customer_phone, analytics_filter=None
    )
    company_name = ""
    try:
        from database import get_site_settings

        company_name = (get_site_settings(["company_name"]).get("company_name") or "").strip()
    except Exception:
        company_name = ""
    slug = secure_filename(
        f"credit-note-{(customer_name or 'customer').replace(' ', '-').lower()}"
    )
    return _credit_note_pdf_response(
        shop_name=shop.get("shop_name") or f"Shop {shop_id}",
        company_name=company_name or "Company",
        customer_name=customer_name,
        customer_phone=customer_phone,
        note_ctx=note_ctx,
        primary_color=(shop.get("primary_color") or "").strip() or None,
        filename_slug=slug or "credit-note",
    )


@app.route("/shops/<int:shop_id>/shop-credit-payments/pay", methods=["POST"])
def shop_credit_payments_pay(shop_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    if not _shop_pos_allow_credit_sale(shop):
        flash("Credit sales are disabled for this shop.", "error")
        return redirect(url_for("shop_dashboard", shop_id=shop_id))
    customer_name = (request.form.get("customer_name") or "").strip() or "WALK IN"
    customer_phone = (request.form.get("customer_phone") or "").strip() or "-"
    amount_raw = (request.form.get("amount") or "").strip()
    note = (request.form.get("note") or "").strip() or None
    try:
        amount = float(amount_raw)
    except Exception:
        flash("Enter a valid amount.", "error")
        return redirect(
            url_for(
                "shop_credit_payments_customer",
                shop_id=shop_id,
                customer_name=customer_name,
                customer_phone=customer_phone,
            )
        )
    try:
        from database import apply_shop_credit_payment_fifo

        res = apply_shop_credit_payment_fifo(
            shop_id=shop_id,
            customer_name=customer_name,
            customer_phone=customer_phone,
            amount=amount,
            note=note,
        )
    except Exception:
        res = {"ok": False, "error": "Could not apply payment."}
    if not res.get("ok"):
        flash(res.get("error") or "Could not apply payment.", "error")
    else:
        unused = float(res.get("unused") or 0)
        if unused > 0.0001:
            flash(f"Payment applied. Unused amount: {unused:.2f}", "success")
        else:
            flash("Payment applied successfully.", "success")
    return_to = (request.form.get("return_to") or "customer").strip().lower()
    if return_to == "analytics":
        redirect_kwargs = {
            "shop_id": shop_id,
            "customer_name": customer_name,
            "customer_phone": customer_phone,
            "mode": (request.form.get("mode") or "single_day").strip(),
            "single_day": (request.form.get("single_day") or date.today().isoformat()).strip(),
            "start_date": (request.form.get("start_date") or date.today().isoformat()).strip(),
            "end_date": (request.form.get("end_date") or date.today().isoformat()).strip(),
            "month": (request.form.get("month") or date.today().strftime("%Y-%m")).strip(),
            "year": (request.form.get("year") or str(date.today().year)).strip(),
            "analytics_scope": (request.form.get("analytics_scope") or "general").strip(),
        }
        target = url_for("shop_customer_analytics_detail", **redirect_kwargs)
    else:
        target = url_for(
            "shop_credit_payments_customer",
            shop_id=shop_id,
            customer_name=customer_name,
            customer_phone=customer_phone,
        )
    return redirect(f"{target}#shop-credit-payments")


@app.route("/shops/<int:shop_id>/shop-credit-payments/sale/<int:sale_id>")
def shop_credit_sale_detail(shop_id: int, sale_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    if not _shop_pos_allow_credit_sale(shop):
        return redirect(url_for("shop_dashboard", shop_id=shop_id))
    try:
        from database import get_shop_credit_sale_detail

        d = get_shop_credit_sale_detail(shop_id=shop_id, sale_id=sale_id) or {}
    except Exception:
        d = {}
    if not d.get("sale"):
        abort(404)
    return render_template(
        "shop_credit_sale_detail.html",
        shop=shop,
        sale=d["sale"],
        items=d.get("items") or [],
        theme_key=f"richcom-theme-shop-{shop['id']}",
        theme_default=shop.get("default_theme") or "dark",
        font_family=shop.get("font_family") or "Plus Jakarta Sans",
        primary_color_rgb=_hex_to_rgb_triplet(shop.get("primary_color") or "#10b981"),
        accent_color_rgb=_hex_to_rgb_triplet(shop.get("accent_color") or "#14b8a6"),
    )


@app.route("/shops/<int:shop_id>/shop-credit-payments/sale/<int:sale_id>.pdf")
def shop_credit_sale_detail_pdf(shop_id: int, sale_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    if not _shop_pos_allow_credit_sale(shop):
        abort(404)
    try:
        from database import get_shop_credit_sale_detail

        d = get_shop_credit_sale_detail(shop_id=shop_id, sale_id=sale_id) or {}
    except Exception:
        d = {}
    if not d.get("sale"):
        abort(404)
    sale = d["sale"]
    customer_name = (sale.get("customer_name") or "").strip() or "WALK IN"
    customer_phone = (sale.get("customer_phone") or "").strip() or "-"
    note_ctx = _shop_customer_credit_note_context(
        shop_id, customer_name, customer_phone, analytics_filter=None
    )
    company_name = ""
    try:
        from database import get_site_settings

        company_name = (get_site_settings(["company_name"]).get("company_name") or "").strip()
    except Exception:
        company_name = ""
    return _credit_note_pdf_response(
        shop_name=shop.get("shop_name") or f"Shop {shop_id}",
        company_name=company_name or "Company",
        customer_name=customer_name,
        customer_phone=customer_phone,
        note_ctx=note_ctx,
        primary_color=(shop.get("primary_color") or "").strip() or None,
        focus_sale=d,
        filename_slug=secure_filename(f"credit-sale-{sale_id}") or "credit-sale",
    )


@app.route("/shops/<int:shop_id>/shop-stock-management/source-stock.json")
def shop_stock_management_source_stock_json(shop_id: int):
    """Stock quantities at the selected request source (company or another shop) for the request-stock table."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    raw = (request.args.get("batch_request_source") or "company").strip().lower()
    try:
        from database import get_request_source_stock_snapshot

        label, stock_map = get_request_source_stock_snapshot(
            requesting_shop_id=shop_id,
            batch_request_source=raw,
        )
    except Exception:
        label, stock_map = "Company", {}
    return jsonify(
        {
            "ok": True,
            "label": label,
            "stock_by_item_id": {str(k): float(v) for k, v in (stock_map or {}).items()},
        }
    )


@app.route("/shops/<int:shop_id>/shop-item-management")
def shop_item_management(shop_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    try:
        from database import ensure_shop_items_for_shop, list_shop_items

        ensure_shop_items_for_shop(shop_id)
        items = list_shop_items(shop_id=shop_id, limit=500)
    except Exception:
        items = []

    show_register_form = (request.args.get("register") or "").strip() == "1"
    if show_register_form and not shop_shell_can("manage_items"):
        return redirect(url_for("shop_item_management", shop_id=shop_id))

    return render_template(
        "shop_item_management.html",
        shop=shop,
        items=items,
        can_register_items=shop_shell_can("manage_items"),
        show_register_form=show_register_form,
        pos_inventory_mode=_pos_inventory_mode(shop),
        theme_key=f"richcom-theme-shop-{shop['id']}",
        theme_default=shop.get("default_theme") or "dark",
        font_family=shop.get("font_family") or "Plus Jakarta Sans",
        primary_color_rgb=_hex_to_rgb_triplet(shop.get("primary_color") or "#10b981"),
        accent_color_rgb=_hex_to_rgb_triplet(shop.get("accent_color") or "#14b8a6"),
    )


@app.route("/shops/<int:shop_id>/shop-item-management/register-item", methods=["GET", "POST"])
def shop_register_item(shop_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    if not shop_shell_can("manage_items"):
        flash("Only shop managers and admins can register items.", "error")
        return redirect(url_for("shop_item_management", shop_id=shop_id))

    if request.method == "GET":
        return redirect(
            url_for("shop_item_management", shop_id=shop_id, register=1)
            + "#shop-register-new-item"
        )

    register_back = (
        url_for("shop_item_management", shop_id=shop_id, register=1)
        + "#shop-register-new-item"
    )

    category = (request.form.get("category") or "").strip().upper()
    name = (request.form.get("name") or "").strip().upper()
    description = (request.form.get("description") or "").strip().upper()
    price_raw = (request.form.get("price") or "").strip()
    selling_raw = (request.form.get("selling_price") or "").strip()

    if not category or not name or not price_raw:
        flash("Please fill item category, item name, and original selling price.", "error")
        return redirect(register_back)

    try:
        price = float(price_raw)
        if price < 0:
            raise ValueError()
    except Exception:
        flash("Original selling price must be a valid number.", "error")
        return redirect(register_back)

    selling_price = None
    if selling_raw:
        try:
            selling_price = float(selling_raw)
            if selling_price < 0:
                raise ValueError()
        except Exception:
            flash("Selling price must be a valid number.", "error")
            return redirect(register_back)

    img = request.files.get("image")
    image_path = _save_item_upload(img) if img and img.filename else None
    if img and img.filename and image_path is None:
        flash("Item image must be PNG, JPG, GIF, or WebP.", "error")
        return redirect(register_back)

    new_item_id = None
    try:
        from database import create_item, init_shop_items_table, seed_shop_items_for_company_item

        init_shop_items_table()
        new_item_id = create_item(
            category=category,
            name=name,
            description=description,
            price=price,
            selling_price=selling_price,
            image_path=image_path,
            status="active",
            created_by_employee_id=session.get("employee_id"),
        )
        if new_item_id:
            seed_shop_items_for_company_item(int(new_item_id), origin_shop_id=shop_id)
    except Exception:
        flash("Could not register item. Check database connection.", "error")
        return redirect(register_back)

    if not new_item_id:
        flash("Could not register item.", "error")
        return redirect(register_back)

    _log_hr_activity_safe(
        "register",
        target_type="item",
        target_id=int(new_item_id),
        description=f"Shop '{shop.get('shop_name')}' registered item '{name}' ({category})",
    )
    flash(
        "Item registered in the company catalog. It is displayed for this shop only; other branches start hidden.",
        "success",
    )
    return redirect(url_for("shop_item_management", shop_id=shop_id))


_SHOP_STORE_STOCK_MEASURE_UNITS = frozenset(
    {"pcs", "kg", "g", "l", "ml", "pack", "crate", "box", "dozen", "portion"}
)


def _parse_shop_store_stock_item_form() -> tuple[str, str, str, str] | None:
    """Return (category, name, description, measure_unit) or None if invalid."""
    cat = (request.form.get("category") or "").strip()
    nm = (request.form.get("name") or "").strip()
    desc = (request.form.get("description") or "").strip()
    measure = (request.form.get("measure_unit") or "").strip().lower()
    if not cat or not nm:
        return None
    if measure not in _SHOP_STORE_STOCK_MEASURE_UNITS:
        return None
    return cat, nm, desc, measure


_SHOP_STOCK_MGMT_VIEWS = frozenset({"auto", "manual", "manual_out"})


def _shop_stock_mgmt_view(raw: str | None) -> str:
    v = (raw or "auto").strip().lower()
    return v if v in _SHOP_STOCK_MGMT_VIEWS else "auto"


def _shop_stock_mgmt_is_manual(view: str) -> bool:
    return view in ("manual", "manual_out")


@app.route("/shops/<int:shop_id>/shop-stock-management", methods=["GET", "POST"])
def shop_stock_management(shop_id: int, mode: str | None = None, item_id: int | None = None):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate

    store_registration_enabled = False
    try:
        from database import column_exists, init_shop_items_table

        init_shop_items_table()
        store_registration_enabled = column_exists("shop_items", "store_stock_registered")
    except Exception:
        store_registration_enabled = False

    mode = (mode or request.args.get("mode") or "in").strip().lower()
    if mode not in ("in", "out"):
        mode = "in"

    if request.method == "POST":
        view_arg = _shop_stock_mgmt_view(request.form.get("view") or request.args.get("view"))
    else:
        view_arg = _shop_stock_mgmt_view(request.args.get("view"))

    if request.method == "POST":
        action = (request.form.get("action") or "").strip()

        if action == "create_shop_store_stock_item":
            if _pos_inventory_mode(shop) != "both":
                flash("Store stock SKUs are only used when POS inventory is set to Both (kitchen + shelf).", "error")
                return redirect(url_for("shop_stock_management", shop_id=shop_id, view=view_arg))
            parsed = _parse_shop_store_stock_item_form()
            if not parsed:
                flash("Name, category, and a valid measure unit are required.", "error")
                return redirect(url_for("shop_stock_management", shop_id=shop_id, view=view_arg))
            cat, nm, desc, measure = parsed
            try:
                from database import create_store_stock_item, init_store_stock_items_table

                init_store_stock_items_table()
                new_id = create_store_stock_item(
                    shop_id=shop_id,
                    category=cat,
                    name=nm,
                    description=desc,
                    measure_unit=measure,
                    created_by_employee_id=session.get("employee_id"),
                )
                flash(
                    "Store stock item registered for this shop." if new_id else "Could not register stock item.",
                    "success" if new_id else "error",
                )
            except Exception as e:
                app.logger.exception("create_shop_store_stock_item: %s", e)
                flash("Could not register stock item.", "error")
            return redirect(url_for("shop_stock_management", shop_id=shop_id, view=view_arg))

        if action == "update_shop_store_stock_item":
            if _pos_inventory_mode(shop) != "both":
                flash("Store stock items are only managed when POS inventory is Both (kitchen + shelf).", "error")
                return redirect(url_for("shop_stock_management", shop_id=shop_id, view=view_arg))
            try:
                store_item_id = int(request.form.get("store_item_id") or 0)
            except (TypeError, ValueError):
                store_item_id = 0
            if store_item_id <= 0:
                flash("Invalid store stock item.", "error")
                return redirect(url_for("shop_stock_management", shop_id=shop_id, view=view_arg))
            parsed = _parse_shop_store_stock_item_form()
            if not parsed:
                flash("Name, category, and a valid measure unit are required.", "error")
                return redirect(
                    url_for("shop_stock_management", shop_id=shop_id, view=view_arg, manage_item=store_item_id)
                )
            cat, nm, desc, measure = parsed
            try:
                from database import update_store_stock_item_for_shop

                ok = update_store_stock_item_for_shop(
                    shop_id=shop_id,
                    store_item_id=store_item_id,
                    category=cat,
                    name=nm,
                    description=desc,
                    measure_unit=measure,
                )
                flash(
                    "Store stock item updated." if ok else "Could not update store stock item.",
                    "success" if ok else "error",
                )
            except Exception as e:
                app.logger.exception("update_shop_store_stock_item: %s", e)
                flash("Could not update store stock item.", "error")
            return redirect(url_for("shop_stock_management", shop_id=shop_id, view=view_arg))

        if action == "toggle_shop_store_stock_item_status":
            if _pos_inventory_mode(shop) != "both":
                flash("Store stock items are only managed when POS inventory is Both (kitchen + shelf).", "error")
                return redirect(url_for("shop_stock_management", shop_id=shop_id, view=view_arg))
            try:
                store_item_id = int(request.form.get("store_item_id") or 0)
            except (TypeError, ValueError):
                store_item_id = 0
            if store_item_id <= 0:
                flash("Invalid store stock item.", "error")
                return redirect(url_for("shop_stock_management", shop_id=shop_id, view=view_arg))
            try:
                from database import toggle_store_stock_item_status_for_shop

                new_status = toggle_store_stock_item_status_for_shop(shop_id, store_item_id)
            except Exception as e:
                app.logger.exception("toggle_shop_store_stock_item_status: %s", e)
                new_status = None
            if new_status == "active":
                flash("Store stock item is active again (unsuspended).", "success")
            elif new_status == "inactive":
                flash("Store stock item suspended — it is hidden from stock in/out until unsuspended.", "success")
            else:
                flash("Could not change store stock item status.", "error")
            return redirect(url_for("shop_stock_management", shop_id=shop_id, view=view_arg))

        if action == "delete_shop_store_stock_item":
            if _pos_inventory_mode(shop) != "both":
                flash("Store stock items are only managed when POS inventory is Both (kitchen + shelf).", "error")
                return redirect(url_for("shop_stock_management", shop_id=shop_id, view=view_arg))
            try:
                store_item_id = int(request.form.get("store_item_id") or 0)
            except (TypeError, ValueError):
                store_item_id = 0
            if store_item_id <= 0:
                flash("Invalid store stock item.", "error")
                return redirect(url_for("shop_stock_management", shop_id=shop_id, view=view_arg))
            try:
                from database import delete_store_stock_item_for_shop

                ok, msg = delete_store_stock_item_for_shop(shop_id, store_item_id)
            except Exception as e:
                app.logger.exception("delete_shop_store_stock_item: %s", e)
                ok, msg = False, "Could not delete store stock item."
            flash(msg, "success" if ok else "error")
            return redirect(url_for("shop_stock_management", shop_id=shop_id, view=view_arg))

        if action in ("bulk_store_in", "bulk_store_out"):
            if _pos_inventory_mode(shop) != "both":
                flash("Bulk store stock is only used when POS inventory is set to Both (kitchen + shelf).", "error")
                return redirect(url_for("shop_stock_management", shop_id=shop_id, view=view_arg))
            direction = "in" if action == "bulk_store_in" else "out"
            try:
                from database import (
                    create_store_stock_transactions_batch,
                    list_store_stock_items_for_shop_management,
                    normalize_stock_move_qty,
                    resolve_seller_name_and_phone,
                )

                store_items_for_post = list_store_stock_items_for_shop_management(
                    shop_id=shop_id, limit=2000
                )
            except Exception:
                store_items_for_post = []
            if not store_items_for_post:
                flash("No registered store stock items found for this shop.", "error")
                return redirect(url_for("shop_stock_management", shop_id=shop_id, view=view_arg))

            allowed_reasons = {"return", "waste", "display"}
            allowed_pay = frozenset({"pending_payment", "partially_paid", "paid"})
            errors: list[str] = []
            ops: list[dict] = []

            for it in store_items_for_post:
                try:
                    iid = int(it.get("id"))
                except Exception:
                    continue
                label = (it.get("name") or f"Store item #{iid}")
                if direction == "in":
                    qty_raw = (request.form.get(f"in_qty_{iid}") or "").strip()
                    bp_raw = (request.form.get(f"in_buying_price_{iid}") or "").strip()
                    place = (request.form.get(f"in_place_{iid}") or "").strip()
                    phone_raw = (request.form.get(f"in_seller_phone_{iid}") or "").strip()
                    pay_raw = (request.form.get(f"in_payment_status_{iid}") or "").strip().lower()
                    note_raw = (request.form.get(f"in_note_{iid}") or "").strip()
                    pay_selected = pay_raw in allowed_pay
                    partial_without_qty = bool(bp_raw or place or phone_raw or pay_selected or note_raw)
                    if not qty_raw:
                        if partial_without_qty:
                            errors.append(
                                f"{label}: enter a stock-in quantity or clear buying price, seller, phone, payment, and note."
                            )
                        continue
                    qty = normalize_stock_move_qty(qty_raw)
                    if qty is None:
                        errors.append(f"{label}: invalid quantity.")
                        continue
                    if not bp_raw:
                        errors.append(f"{label}: buying price is required when quantity is set.")
                        continue
                    if not place:
                        errors.append(f"{label}: place bought is required when quantity is set.")
                        continue
                    if pay_raw not in allowed_pay:
                        errors.append(f"{label}: select payment (Not paid, Partially paid, or Paid).")
                        continue
                    payment_status = pay_raw
                    place_final = place.upper()
                    resolved_phone = None
                    if phone_raw:
                        rn, rp = resolve_seller_name_and_phone(phone_raw, place)
                        if not rn or not rp:
                            errors.append(
                                f"{label}: seller phone must be valid (07… or 254…). If new, enter seller name in the seller field."
                            )
                            continue
                        place_final = (rn or place).strip().upper()
                        resolved_phone = rp
                    try:
                        buying_price = float(bp_raw)
                        if buying_price < 0:
                            raise ValueError()
                    except Exception:
                        errors.append(f"{label}: buying price must be a valid number.")
                        continue
                    ops.append(
                        {
                            "store_stock_item_id": iid,
                            "direction": "in",
                            "qty": qty,
                            "buying_price": buying_price,
                            "place_brought_from": place_final,
                            "seller_phone": resolved_phone,
                            "stock_out_reason": None,
                            "refunded": False,
                            "refund_amount": None,
                            "note": note_raw.upper() if note_raw else None,
                            "payment_status": payment_status,
                            "amount_paid": None,
                        }
                    )
                else:
                    qty_raw = (request.form.get(f"out_qty_{iid}") or "").strip()
                    reason = (request.form.get(f"out_reason_{iid}") or "").strip().lower()
                    ram = (request.form.get(f"out_refund_amount_{iid}") or "").strip()
                    note_raw = (request.form.get(f"out_note_{iid}") or "").strip()
                    partial_out = reason in allowed_reasons or bool(ram) or bool(note_raw)
                    if not qty_raw:
                        if partial_out:
                            errors.append(
                                f"{label}: enter a quantity out or clear reason, refund amount, and note."
                            )
                        continue
                    qty = normalize_stock_move_qty(qty_raw)
                    if qty is None:
                        errors.append(f"{label}: invalid quantity.")
                        continue
                    if reason not in allowed_reasons:
                        errors.append(f"{label}: choose a stock out reason.")
                        continue
                    refunded_raw = (request.form.get(f"out_refunded_{iid}") or "").strip().lower()
                    if refunded_raw not in ("yes", "no"):
                        errors.append(f"{label}: choose whether this line is refunded.")
                        continue
                    refunded = refunded_raw == "yes"
                    refund_amount = None
                    if refunded:
                        if not ram:
                            errors.append(f"{label}: refund amount is required when refunded is yes.")
                            continue
                        try:
                            refund_amount = float(ram)
                            if refund_amount < 0:
                                raise ValueError()
                        except Exception:
                            errors.append(f"{label}: refund amount must be a valid number.")
                            continue
                    ops.append(
                        {
                            "store_stock_item_id": iid,
                            "direction": "out",
                            "qty": qty,
                            "buying_price": None,
                            "place_brought_from": None,
                            "stock_out_reason": reason.upper(),
                            "refunded": refunded,
                            "refund_amount": refund_amount,
                            "note": note_raw.upper() if note_raw else None,
                        }
                    )

            if errors:
                flash(" ".join(errors[:6]) + (" …" if len(errors) > 6 else ""), "error")
                return redirect(url_for("shop_stock_management", shop_id=shop_id, view=view_arg))
            if not ops:
                flash("Enter a quantity on at least one row to apply.", "error")
                return redirect(url_for("shop_stock_management", shop_id=shop_id, view=view_arg))
            try:
                ok, msg = create_store_stock_transactions_batch(
                    operations=ops,
                    created_by_employee_id=session.get("employee_id"),
                )
            except Exception as e:
                app.logger.exception("bulk_store_%s failed: %s", direction, e)
                ok, msg = False, f"Could not apply store stock {direction}."
            flash(msg, "success" if ok else "error")
            return redirect(url_for("shop_stock_management", shop_id=shop_id, view=view_arg))

        if action == "register_store_items":
            view_arg = _shop_stock_mgmt_view(request.form.get("view") or request.args.get("view"))
            if _pos_inventory_mode(shop) != "both":
                flash("Shelf registration is only used when POS inventory is set to Both (kitchen + shelf).", "error")
                return redirect(url_for("shop_stock_management", shop_id=shop_id, view=view_arg))
            try:
                from database import ensure_shop_items_for_shop, set_shop_store_stock_registered

                ensure_shop_items_for_shop(shop_id)
                registered = 0
                for raw in request.form.getlist("register_item_ids"):
                    try:
                        iid = int(raw)
                    except (TypeError, ValueError):
                        continue
                    if set_shop_store_stock_registered(shop_id, iid, True):
                        registered += 1
                flash(
                    f"Registered {registered} item(s) for shelf stock (stock in/out)." if registered else "Nothing was registered.",
                    "success" if registered else "warning",
                )
            except Exception as e:
                app.logger.exception("register_store_items: %s", e)
                flash("Could not save shelf registration.", "error")
            return redirect(url_for("shop_stock_management", shop_id=shop_id, view=view_arg))

        if action == "register_all_store_items":
            view_arg = _shop_stock_mgmt_view(request.form.get("view") or request.args.get("view"))
            if _pos_inventory_mode(shop) != "both":
                flash("Shelf registration is only used when POS inventory is set to Both (kitchen + shelf).", "error")
                return redirect(url_for("shop_stock_management", shop_id=shop_id, view=view_arg))
            try:
                from database import (
                    ensure_shop_items_for_shop,
                    list_shop_store_stock_registration_candidates,
                    set_shop_store_stock_registered,
                )

                ensure_shop_items_for_shop(shop_id)
                registered = 0
                for row in list_shop_store_stock_registration_candidates(shop_id=shop_id, limit=5000):
                    try:
                        iid = int(row.get("id") or 0)
                    except (TypeError, ValueError):
                        continue
                    if iid <= 0:
                        continue
                    if set_shop_store_stock_registered(shop_id, iid, True):
                        registered += 1
                flash(
                    f"Registered all {registered} remaining catalogue line(s) for shelf stock." if registered else "Every item was already registered.",
                    "success" if registered else "warning",
                )
            except Exception as e:
                app.logger.exception("register_all_store_items: %s", e)
                flash("Could not register all items for shelf stock.", "error")
            return redirect(url_for("shop_stock_management", shop_id=shop_id, view=view_arg))

        if action == "unregister_store_stock":
            view_arg = _shop_stock_mgmt_view(request.form.get("view") or request.args.get("view"))
            if _pos_inventory_mode(shop) != "both":
                flash("This action applies only when the branch uses kitchen + shop stock.", "error")
                return redirect(url_for("shop_stock_management", shop_id=shop_id, view=view_arg))
            iid_raw = (request.form.get("item_id") or "").strip()
            try:
                iid = int(iid_raw)
            except (TypeError, ValueError):
                flash("Invalid item.", "error")
                return redirect(url_for("shop_stock_management", shop_id=shop_id, view=view_arg))
            try:
                from database import set_shop_store_stock_registered

                ok_un = set_shop_store_stock_registered(shop_id, iid, False)
            except Exception:
                ok_un = False
            flash("Removed from shelf stock list." if ok_un else "Could not unregister item.", "success" if ok_un else "error")
            return redirect(url_for("shop_stock_management", shop_id=shop_id, view=view_arg))

        if action == "batch_request_stock":
            try:
                from database import ensure_shop_items_for_shop, create_shop_stock_request

                ensure_shop_items_for_shop(shop_id)
                ws = _load_shop_stock_workspace_settings(shop)
                source_target = (request.form.get("batch_request_source") or "company").strip().lower()
                source_type = "company"
                source_shop_id = None
                if source_target.startswith("shop:"):
                    source_type = "shop"
                    try:
                        source_shop_id = int(source_target.split(":", 1)[1])
                    except Exception:
                        source_shop_id = None
                    if not source_shop_id or int(source_shop_id) == int(shop_id):
                        flash("Choose a valid other shop to request stock from.", "error")
                        return redirect(url_for("shop_stock_management", shop_id=shop_id, view=view_arg))
                batch_note = (request.form.get("batch_note") or "").strip().upper() or None
                from database import get_request_source_stock_snapshot, normalize_stock_move_qty

                try:
                    _, stock_map = get_request_source_stock_snapshot(
                        requesting_shop_id=shop_id,
                        batch_request_source=source_target,
                    )
                except Exception:
                    stock_map = {}
                ok_count = 0
                fail_count = 0
                fail_note = 0
                for key in request.form:
                    m = re.match(r"^qty_(\d+)$", key)
                    if not m:
                        continue
                    iid = int(m.group(1))
                    q_raw = (request.form.get(key) or "").strip()
                    if not q_raw:
                        continue
                    q = normalize_stock_move_qty(q_raw)
                    if q is None:
                        fail_count += 1
                        continue
                    per_note = (request.form.get(f"note_{iid}") or "").strip().upper() or None
                    line_note = per_note or batch_note
                    if ws.get("require_request_stock_notes") and not line_note:
                        fail_note += 1
                        fail_count += 1
                        continue
                    avail = float((stock_map or {}).get(iid) or 0)
                    if q > avail + 1e-9:
                        fail_count += 1
                        continue
                    req_id = create_shop_stock_request(
                        requesting_shop_id=shop_id,
                        request_type="stock_in",
                        source_type=source_type,
                        source_shop_id=source_shop_id,
                        item_id=iid,
                        qty=q,
                        note=line_note,
                        requested_by_employee_id=session.get("employee_id"),
                    )
                    if req_id:
                        _notify_new_shop_stock_request(
                            req_id=int(req_id),
                            requesting_shop_id=shop_id,
                            source_type=source_type,
                            source_shop_id=source_shop_id,
                            request_type="stock_in",
                            item_id=iid,
                            qty=q,
                        )
                        ok_count += 1
                    else:
                        fail_count += 1
                if ok_count and not fail_count:
                    flash(f"Submitted {ok_count} stock request(s) for approval.", "success")
                elif ok_count:
                    flash(
                        f"Submitted {ok_count} request(s). {fail_count} line(s) were skipped "
                        "(quantity over available stock at source, invalid numbers, missing notes, or could not be saved).",
                        "warning",
                    )
                else:
                    bits = [
                        "No requests submitted. Enter at least one quantity that does not exceed "
                        "stock at the selected source (company warehouse or other shop)."
                    ]
                    if fail_note:
                        bits.append(f"{fail_note} line(s) missing required notes.")
                    flash(" ".join(bits), "error")
            except Exception as e:
                app.logger.exception("Batch stock request failed: %s", e)
                flash(f"Could not submit requests. {type(e).__name__}: {e}", "error")
            return redirect(url_for("shop_stock_management", shop_id=shop_id, view=view_arg))

        if action == "batch_manual_in":
            try:
                from database import (
                    ensure_shop_items_for_shop,
                    normalize_stock_move_qty,
                    resolve_seller_name_and_phone,
                    shop_manual_stock_in,
                )

                ensure_shop_items_for_shop(shop_id)
                ws = _load_shop_stock_workspace_settings(shop)
                allowed_pay = frozenset({"pending_payment", "partially_paid", "paid"})
                # Shared seller: merged into lines that are actually stocked in (qty > 0 below).
                apply_sp = (request.form.get("apply_all_seller_phone") or "").strip()
                apply_sn = (request.form.get("apply_all_seller_name") or "").strip().upper()
                apply_note = (request.form.get("apply_all_note") or "").strip().upper() or None
                ok_count = 0
                fail_count = 0
                fail_qty = 0
                fail_seller = 0
                fail_manual = 0
                fail_note = 0
                lines_attempted = 0
                for key in request.form:
                    m = re.match(r"^qty_(\d+)$", key)
                    if not m:
                        continue
                    iid = int(m.group(1))
                    q_raw = (request.form.get(key) or "").strip()
                    if not q_raw:
                        continue
                    lines_attempted += 1
                    q = normalize_stock_move_qty(q_raw)
                    if q is None:
                        fail_qty += 1
                        fail_count += 1
                        continue
                    bp = (request.form.get(f"buying_price_{iid}") or "").strip()
                    sp = (request.form.get(f"seller_phone_{iid}") or "").strip() or apply_sp
                    sn = (request.form.get(f"seller_name_{iid}") or "").strip().upper() or apply_sn
                    pay_raw = (request.form.get(f"payment_status_{iid}") or "").strip().lower()
                    payment_status = pay_raw if pay_raw in allowed_pay else "pending_payment"
                    row_note = (request.form.get(f"note_{iid}") or "").strip().upper() or None
                    line_note = row_note or apply_note
                    if ws.get("require_manual_in_notes") and not line_note:
                        fail_note += 1
                        fail_count += 1
                        continue
                    require_supplier = bool(ws.get("require_manual_in_supplier"))
                    has_seller_input = bool(sp or sn)
                    if require_supplier or has_seller_input:
                        if require_supplier and not has_seller_input:
                            fail_seller += 1
                            fail_count += 1
                            continue
                        resolved_name, resolved_phone = resolve_seller_name_and_phone(
                            seller_phone=sp,
                            seller_name=sn,
                        )
                        if not resolved_name or not resolved_phone:
                            fail_seller += 1
                            fail_count += 1
                            continue
                        # Same value as legacy single-field flow: place on the receipt matches registered seller name.
                        place_final = (resolved_name or "").strip()
                        if not place_final:
                            fail_seller += 1
                            fail_count += 1
                            continue
                    else:
                        place_final = None
                        resolved_phone = None
                    ok = shop_manual_stock_in(
                        shop_id=shop_id,
                        item_id=iid,
                        qty=q,
                        buying_price=bp,
                        place_brought_from=place_final,
                        seller_phone=resolved_phone,
                        payment_status=payment_status,
                        note=line_note,
                        created_by_employee_id=session.get("employee_id"),
                    )
                    if ok:
                        ok_count += 1
                    else:
                        fail_manual += 1
                        fail_count += 1
                if ok_count and not fail_count:
                    flash(f"Recorded {ok_count} manual stock-in line(s).", "success")
                elif ok_count:
                    extras = []
                    if fail_qty:
                        extras.append(f"bad qty {fail_qty}")
                    if fail_seller:
                        extras.append(f"seller {fail_seller}")
                    if fail_note:
                        extras.append(f"missing note {fail_note}")
                    if fail_manual:
                        extras.append(f"not saved {fail_manual}")
                    tail = f" Details: {', '.join(extras)}." if extras else ""
                    flash(
                        f"Recorded {ok_count} line(s). {fail_count} line(s) failed.{tail}",
                        "warning",
                    )
                else:
                    bits = []
                    if lines_attempted == 0:
                        bits.append("No lines had a quantity entered.")
                    if fail_qty:
                        bits.append(f"{fail_qty} line(s) had an invalid quantity.")
                    if fail_seller:
                        bits.append(
                            f"{fail_seller} line(s) had seller issues — use phone 07… or 254… (12 digits); "
                            "for a new seller enter at least 2 letters of name; for registered sellers with a blank name, fill seller name once."
                        )
                    if fail_note:
                        bits.append(f"{fail_note} line(s) missing required notes.")
                    if fail_manual:
                        bits.append(
                            f"{fail_manual} line(s) could not be saved — item may be missing shelf registration (POS “Both”), "
                            "or shop stock updates may be off for that item (company / branch settings)."
                        )
                    flash(
                        "No manual stock-ins recorded. " + " ".join(bits),
                        "error",
                    )
            except Exception as e:
                app.logger.exception("Batch manual stock-in failed: %s", e)
                flash(f"Could not record stock-in. {type(e).__name__}: {e}", "error")
            return redirect(url_for("shop_stock_management", shop_id=shop_id, view=view_arg))

        if action == "batch_manual_out":
            try:
                from database import (
                    ensure_shop_items_for_shop,
                    normalize_stock_move_qty,
                    shop_manual_stock_out,
                )

                ensure_shop_items_for_shop(shop_id)
                ws = _load_shop_stock_workspace_settings(shop)
                allowed_reasons = {"return", "waste", "display"}
                ok_count = 0
                fail_count = 0
                fail_qty = 0
                fail_reason = 0
                fail_stock = 0
                fail_refund = 0
                fail_note = 0
                lines_attempted = 0
                for key in request.form:
                    m = re.match(r"^out_qty_(\d+)$", key)
                    if not m:
                        continue
                    iid = int(m.group(1))
                    q_raw = (request.form.get(key) or "").strip()
                    if not q_raw:
                        continue
                    lines_attempted += 1
                    q = normalize_stock_move_qty(q_raw)
                    if q is None:
                        fail_qty += 1
                        fail_count += 1
                        continue
                    reason = (request.form.get(f"out_reason_{iid}") or "").strip().lower()
                    if reason not in allowed_reasons:
                        fail_reason += 1
                        fail_count += 1
                        continue
                    refunded_raw = (request.form.get(f"out_refunded_{iid}") or "no").strip().lower()
                    refunded = refunded_raw == "yes"
                    refund_amount = None
                    if refunded:
                        ram = (request.form.get(f"out_refund_amount_{iid}") or "").strip()
                        if not ram:
                            fail_refund += 1
                            fail_count += 1
                            continue
                        try:
                            refund_amount = float(ram)
                            if refund_amount < 0:
                                raise ValueError()
                        except Exception:
                            fail_refund += 1
                            fail_count += 1
                            continue
                    row_note = (request.form.get(f"out_note_{iid}") or "").strip().upper() or None
                    if ws.get("require_manual_out_notes") and not row_note:
                        fail_note += 1
                        fail_count += 1
                        continue
                    ok = shop_manual_stock_out(
                        shop_id=shop_id,
                        item_id=iid,
                        qty=q,
                        reason=reason,
                        refunded=refunded,
                        refund_amount=refund_amount,
                        note=row_note,
                        created_by_employee_id=session.get("employee_id"),
                    )
                    if ok:
                        ok_count += 1
                    else:
                        fail_stock += 1
                        fail_count += 1
                if ok_count and not fail_count:
                    flash(f"Recorded {ok_count} manual stock-out line(s).", "success")
                elif ok_count:
                    extras = []
                    if fail_qty:
                        extras.append(f"bad qty {fail_qty}")
                    if fail_reason:
                        extras.append(f"missing reason {fail_reason}")
                    if fail_refund:
                        extras.append(f"refund {fail_refund}")
                    if fail_note:
                        extras.append(f"missing note {fail_note}")
                    if fail_stock:
                        extras.append(f"insufficient stock {fail_stock}")
                    tail = f" Details: {', '.join(extras)}." if extras else ""
                    flash(
                        f"Recorded {ok_count} line(s). {fail_count} line(s) failed.{tail}",
                        "warning",
                    )
                else:
                    bits = []
                    if lines_attempted == 0:
                        bits.append("No lines had a quantity entered.")
                    if fail_qty:
                        bits.append(f"{fail_qty} line(s) had an invalid quantity.")
                    if fail_reason:
                        bits.append(f"{fail_reason} line(s) need a reason (Return, Waste, or Display).")
                    if fail_refund:
                        bits.append(f"{fail_refund} line(s) need a refund amount when Refund is Yes.")
                    if fail_note:
                        bits.append(f"{fail_note} line(s) missing required notes.")
                    if fail_stock:
                        bits.append(
                            f"{fail_stock} line(s) could not be saved — shop stock may be insufficient, "
                            "or stock updates may be off for that item."
                        )
                    flash(
                        "No manual stock-outs recorded. " + " ".join(bits),
                        "error",
                    )
            except Exception as e:
                app.logger.exception("Batch manual stock-out failed: %s", e)
                flash(f"Could not record stock-out. {type(e).__name__}: {e}", "error")
            return redirect(url_for("shop_stock_management", shop_id=shop_id, view=view_arg))

        item_id_raw = (request.form.get("item_id") or "").strip()
        qty_raw = (request.form.get("qty") or "").strip()
        buying_price_raw = (request.form.get("buying_price") or "").strip()
        seller_phone = (request.form.get("seller_phone") or "").strip()
        seller_name = (request.form.get("seller_name") or "").strip().upper()
        payment_status = "pending_payment"
        reason = (request.form.get("reason") or "").strip().lower()
        note = (request.form.get("note") or "").strip().upper()

        try:
            item_id = int(item_id_raw)
        except Exception:
            flash("Invalid item.", "error")
            return redirect(url_for("shop_stock_management", shop_id=shop_id, view=view_arg))

        try:
            from database import (
                create_shop_stock_request,
                ensure_shop_items_for_shop,
                normalize_stock_move_qty,
                resolve_seller_name_and_phone,
                shop_manual_stock_in,
                shop_manual_stock_out,
                shop_request_stock_from_company,
                shop_return_stock_to_company,
            )

            ensure_shop_items_for_shop(shop_id)

            qty = normalize_stock_move_qty(qty_raw)
            if qty is None:
                flash("Quantity must be a positive number (decimals allowed, e.g. 0.15).", "error")
                return redirect(url_for("shop_stock_management", shop_id=shop_id, view=view_arg))

            if action == "request_stock":
                source_target = (request.form.get("request_source_target") or "company").strip().lower()
                source_type = "company"
                source_shop_id = None
                if source_target.startswith("shop:"):
                    source_type = "shop"
                    try:
                        source_shop_id = int(source_target.split(":", 1)[1])
                    except Exception:
                        source_shop_id = None
                req_id = create_shop_stock_request(
                    requesting_shop_id=shop_id,
                    request_type="stock_in",
                    source_type=source_type,
                    source_shop_id=source_shop_id,
                    item_id=item_id,
                    qty=qty,
                    note=note or None,
                    requested_by_employee_id=session.get("employee_id"),
                )
                if not req_id:
                    flash(
                        "Could not create stock request. Quantity must not exceed available stock "
                        "at the selected source, and stock updates must be enabled where required.",
                        "error",
                    )
                else:
                    _notify_new_shop_stock_request(
                        req_id=int(req_id),
                        requesting_shop_id=shop_id,
                        source_type=source_type,
                        source_shop_id=source_shop_id,
                        request_type="stock_in",
                        item_id=item_id,
                        qty=qty,
                    )
                    flash("Stock request submitted for approval.", "success")
            elif action == "request_return":
                req_id = create_shop_stock_request(
                    requesting_shop_id=shop_id,
                    request_type="return_to_company",
                    source_type="company",
                    source_shop_id=None,
                    item_id=item_id,
                    qty=qty,
                    note=note or None,
                    requested_by_employee_id=session.get("employee_id"),
                )
                if not req_id:
                    flash("Could not create return request. Check shop stock availability.", "error")
                else:
                    _notify_new_shop_stock_request(
                        req_id=int(req_id),
                        requesting_shop_id=shop_id,
                        source_type="company",
                        source_shop_id=None,
                        request_type="return_to_company",
                        item_id=item_id,
                        qty=qty,
                    )
                    flash("Return request submitted for company approval.", "success")
            elif action == "request_company":
                ok = shop_request_stock_from_company(
                    shop_id=shop_id,
                    item_id=item_id,
                    qty=qty,
                    note=note or None,
                    created_by_employee_id=session.get("employee_id"),
                )
                if not ok:
                    flash("Could not request stock. Ensure shop stock update is ON and company stock is enough.", "error")
                else:
                    flash("Stock requested from company.", "success")
            elif action == "return_company":
                ok = shop_return_stock_to_company(
                    shop_id=shop_id,
                    item_id=item_id,
                    qty=qty,
                    reason=reason,
                    refunded=False,
                    refund_amount=None,
                    note=note or None,
                    created_by_employee_id=session.get("employee_id"),
                )
                if not ok:
                    flash("Could not return stock. Ensure shop stock update is ON and shop stock is enough.", "error")
                else:
                    flash("Stock returned to company.", "success")
            elif action == "manual_in":
                place_brought_from_raw = (request.form.get("place_brought_from") or "").strip().upper()
                resolved_name, resolved_phone = resolve_seller_name_and_phone(
                    seller_phone=seller_phone,
                    seller_name=seller_name,
                )
                if not resolved_name or not resolved_phone:
                    flash("Seller phone must be valid. If new, provide seller name to register.", "error")
                    return redirect(url_for("shop_stock_management", shop_id=shop_id, view=view_arg))
                place_use = place_brought_from_raw or (resolved_name or "")
                if not place_use:
                    flash("Place bought from or seller name is required.", "error")
                    return redirect(url_for("shop_stock_management", shop_id=shop_id, view=view_arg))
                ok = shop_manual_stock_in(
                    shop_id=shop_id,
                    item_id=item_id,
                    qty=qty,
                    buying_price=buying_price_raw,
                    place_brought_from=place_use,
                    seller_phone=resolved_phone,
                    payment_status=payment_status,
                    note=note or None,
                    created_by_employee_id=session.get("employee_id"),
                )
                if not ok:
                    flash("Could not stock in manually. Check your inputs.", "error")
                else:
                    flash("Shop stock updated.", "success")
            elif action == "manual_out":
                ok = shop_manual_stock_out(
                    shop_id=shop_id,
                    item_id=item_id,
                    qty=qty,
                    reason=reason,
                    refunded=False,
                    refund_amount=None,
                    note=note or None,
                    created_by_employee_id=session.get("employee_id"),
                )
                if not ok:
                    flash("Could not stock out manually. Check your inputs and shop stock.", "error")
                else:
                    flash("Shop stock updated.", "success")
            else:
                flash("Invalid action.", "error")
        except Exception as e:
            app.logger.exception("Shop stock management failed: %s", e)
            flash(f"Could not update shop stock. {type(e).__name__}: {e}", "error")

        return redirect(url_for("shop_stock_management", shop_id=shop_id, view=view_arg))

    pos_mode = _pos_inventory_mode(shop)
    store_items: list = []
    store_catalog_items: list = []
    store_tx: list = []
    inline_register_measure_options: tuple = ()
    manage_item_id: int | None = None
    try:
        manage_item_id = int(request.args.get("manage_item") or 0) or None
    except (TypeError, ValueError):
        manage_item_id = None
    try:
        from database import (
            ensure_shop_items_for_shop,
            list_shop_stock_manage_items,
            list_shop_stock_transactions,
            list_shop_store_stock_registration_candidates,
            list_shops,
            list_stock_requests_for_session,
        )

        ensure_shop_items_for_shop(shop_id)
        items = list_shop_stock_manage_items(shop_id=shop_id, limit=500)
        store_reg_candidates = list_shop_store_stock_registration_candidates(shop_id=shop_id, limit=500)
        txs = []
        if not _shop_stock_mgmt_is_manual(view_arg):
            txs = list_shop_stock_transactions(shop_id=shop_id, item_id=None, limit=200)
        request_rows = list_stock_requests_for_session(
            role_key=(session.get("employee_role") or "employee"),
            viewer_shop_id=shop_id,
            limit=200,
        )
        all_shops = list_shops(limit=500) or []
        other_shops = [s for s in all_shops if int(s.get("id") or 0) != int(shop_id)]
    except Exception:
        items, txs, request_rows, other_shops, store_reg_candidates = [], [], [], [], []

    if pos_mode == "both":
        try:
            from database import (
                init_store_stock_items_table,
                list_store_stock_items_for_shop_catalog,
                list_store_stock_items_for_shop_management,
                list_store_stock_transactions_for_shop,
            )

            init_store_stock_items_table()
            store_items = list_store_stock_items_for_shop_management(
                shop_id=shop_id, limit=2000
            )
            store_catalog_items = list_store_stock_items_for_shop_catalog(
                shop_id=shop_id, limit=2000
            )
            store_tx = list_store_stock_transactions_for_shop(shop_id=shop_id, limit=200)
        except Exception:
            store_items, store_tx = [], []
        inline_register_measure_options = (
            "pcs", "kg", "g", "l", "ml", "pack", "crate", "box", "dozen", "portion",
        )

    return render_template(
        "shop_stock_management.html",
        shop=shop,
        items=items,
        stock_workspace_settings=_load_shop_stock_workspace_settings(shop),
        store_reg_candidates=store_reg_candidates or [],
        store_registration_enabled=store_registration_enabled,
        pos_inventory_mode=pos_mode,
        other_shops=other_shops,
        view=view_arg,
        stock_requests=request_rows,
        item_stock_requests=request_rows,
        transactions=txs,
        store_items=store_items,
        store_catalog_items=store_catalog_items,
        store_tx=store_tx,
        manage_item_id=manage_item_id,
        inline_register_measure_options=inline_register_measure_options,
        theme_key=f"richcom-theme-shop-{shop['id']}",
        theme_default=shop.get("default_theme") or "dark",
        font_family=shop.get("font_family") or "Plus Jakarta Sans",
        primary_color_rgb=_hex_to_rgb_triplet(shop.get("primary_color") or "#10b981"),
        accent_color_rgb=_hex_to_rgb_triplet(shop.get("accent_color") or "#14b8a6"),
    )


@app.route(
    "/shops/<int:shop_id>/shop-stock-management/<string:stock_path_segment>",
    methods=["GET", "POST"],
)
def shop_stock_management_legacy_segment(shop_id: int, stock_path_segment: str):
    """
    Old bookmarks used /shop-stock-management/in or /out without an item id.
    The item-specific route is /shop-stock-management/<mode>/<item_id>; a single
    trailing segment would otherwise 404.
    """
    seg = (stock_path_segment or "").strip().lower()
    code = 302 if request.method == "POST" else 301
    if seg in ("in", "auto"):
        return redirect(url_for("shop_stock_management", shop_id=shop_id, view="auto"), code=code)
    if seg in ("out", "manual_out"):
        return redirect(url_for("shop_stock_management", shop_id=shop_id, view="manual_out"), code=code)
    if seg == "manual":
        return redirect(url_for("shop_stock_management", shop_id=shop_id, view="manual"), code=code)
    abort(404)


@app.route(
    "/shops/<int:shop_id>/shop-stock-management/<string:mode>/<int:item_id>",
    methods=["GET"],
    endpoint="shop_stock_management_item",
)
def shop_stock_management_item(shop_id: int, mode: str, item_id: int):
    # Legacy URLs; stock management is a single bulk page now.
    return redirect(url_for("shop_stock_management", shop_id=shop_id, view="auto"))


def _can_user_review_stock_request(row: dict, *, role_key: str, viewer_shop_id: int | None) -> bool:
    role_key = (role_key or "employee").strip().lower()
    request_type = (row.get("request_type") or "stock_in").strip().lower()
    source_type = (row.get("source_type") or "").strip().lower()
    if role_key in COMPANY_PORTAL_ROLES:
        if request_type == "return_to_company":
            return True
        if source_type == "company":
            return True
        if source_type == "shop":
            return True
    if request_type == "return_to_company":
        return False
    if source_type == "company":
        return False
    if source_type == "shop":
        try:
            return int(viewer_shop_id or 0) == int(row.get("source_shop_id") or 0)
        except Exception:
            return False
    return False


def _effective_viewer_shop_id(role_key: str) -> int | None:
    """Best-effort shop scope for non-admin users (session first, then assigned shop)."""
    role_key = (role_key or "employee").strip().lower()
    sid = session.get("shop_id")
    if sid:
        try:
            return int(sid)
        except Exception:
            return None
    if role_key in COMPANY_PORTAL_ROLES:
        return None
    uid = session.get("employee_id")
    if not uid:
        return None
    try:
        from database import get_employee_by_id

        row = get_employee_by_id(int(uid)) or {}
        shop_id = row.get("shop_id")
        return int(shop_id) if shop_id else None
    except Exception:
        return None


@app.route("/notifications")
@login_required
def notifications():
    role_key = (session.get("employee_role") or "employee").strip().lower()
    shop_id = _effective_viewer_shop_id(role_key)
    if shop_id:
        return redirect(url_for("shop_notifications", shop_id=int(shop_id)))
    try:
        from database import list_notifications_for_session, list_stock_requests_for_session, can_fulfill_stock_request

        notifications = list_notifications_for_session(
            employee_id=session.get("employee_id"),
            shop_id=None,
            role_key=role_key,
            limit=300,
            outcomes_only=True,
        )
        stock_requests = list_stock_requests_for_session(
            role_key=role_key,
            viewer_shop_id=None,
            limit=300,
            status="pending",
            approval_queue_only=True,
        )
    except Exception:
        notifications, stock_requests = [], []
    viewer_shop_id = _effective_viewer_shop_id(role_key)
    for r in stock_requests or []:
        r["can_review"] = _can_user_review_stock_request(r, role_key=role_key, viewer_shop_id=viewer_shop_id)
        r["can_approve_now"] = can_fulfill_stock_request(int(r.get("id") or 0))

    pending_request_count = len(stock_requests or [])
    return render_template(
        "notifications.html",
        notifications=notifications,
        stock_requests=stock_requests,
        pending_request_count=pending_request_count,
    )


@app.route("/shops/<int:shop_id>/notifications")
@login_required
def shop_notifications(shop_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    role_key = (session.get("employee_role") or "employee").strip().lower()
    try:
        from database import list_notifications_for_session, list_stock_requests_for_session, can_fulfill_stock_request

        rows = list_notifications_for_session(
            employee_id=session.get("employee_id"),
            shop_id=shop_id,
            role_key=role_key,
            limit=300,
            outcomes_only=True,
        )
        stock_requests = list_stock_requests_for_session(
            role_key=role_key,
            viewer_shop_id=shop_id,
            limit=300,
            status="pending",
            approval_queue_only=True,
        )
    except Exception:
        rows, stock_requests = [], []
    sr_outcome_max_id = 0
    try:
        from database import list_shop_stock_request_alerts_for_shop

        sr_rows = list_shop_stock_request_alerts_for_shop(shop_id=shop_id, limit=1)
        if sr_rows:
            sr_outcome_max_id = int(sr_rows[0].get("id") or 0)
    except Exception:
        sr_outcome_max_id = 0
    for r in stock_requests or []:
        r["can_review"] = _can_user_review_stock_request(r, role_key=role_key, viewer_shop_id=shop_id)
        r["can_approve_now"] = can_fulfill_stock_request(int(r.get("id") or 0))

    pending_request_count = len(stock_requests or [])
    return render_template(
        "shop_notifications.html",
        shop=shop,
        notifications=rows,
        stock_requests=stock_requests,
        pending_request_count=pending_request_count,
        sr_outcome_max_id=sr_outcome_max_id,
        theme_key=f"richcom-theme-shop-{shop['id']}",
        theme_default=shop.get("default_theme") or "dark",
        font_family=shop.get("font_family") or "Plus Jakarta Sans",
        primary_color_rgb=_hex_to_rgb_triplet(shop.get("primary_color") or "#10b981"),
        accent_color_rgb=_hex_to_rgb_triplet(shop.get("accent_color") or "#14b8a6"),
    )


@app.route("/notifications/stock-requests/<int:request_id>/approve", methods=["POST"])
@login_required
def approve_stock_request(request_id: int):
    role_key = (session.get("employee_role") or "employee").strip().lower()
    shop_id = session.get("shop_id")
    try:
        from database import review_stock_request

        ok, err_msg = review_stock_request(
            request_id=request_id,
            approve=True,
            approver_employee_id=session.get("employee_id"),
            approver_role=role_key,
            approver_shop_id=int(shop_id) if shop_id else None,
            review_note=(request.form.get("review_note") or "").strip() or None,
        )
    except Exception:
        ok, err_msg = False, "Could not approve stock request."
    flash(
        "Stock request approved. Stock levels were updated." if ok else (err_msg or "Could not approve stock request."),
        "success" if ok else "error",
    )
    if shop_id:
        return redirect(url_for("shop_notifications", shop_id=int(shop_id)))
    return redirect(url_for("notifications"))


@app.route("/notifications/stock-requests/<int:request_id>/reject", methods=["POST"])
@login_required
def reject_stock_request(request_id: int):
    role_key = (session.get("employee_role") or "employee").strip().lower()
    shop_id = session.get("shop_id")
    try:
        from database import review_stock_request

        ok, err_msg = review_stock_request(
            request_id=request_id,
            approve=False,
            approver_employee_id=session.get("employee_id"),
            approver_role=role_key,
            approver_shop_id=int(shop_id) if shop_id else None,
            review_note=(request.form.get("review_note") or "").strip() or None,
        )
    except Exception:
        ok, err_msg = False, "Could not reject stock request."
    flash(
        "Stock request cancelled." if ok else (err_msg or "Could not cancel stock request."),
        "success" if ok else "error",
    )
    if shop_id:
        return redirect(url_for("shop_notifications", shop_id=int(shop_id)))
    return redirect(url_for("notifications"))


@app.route("/shops/<int:shop_id>/notifications/stock-request-alerts.json")
@login_required
def shop_stock_request_alerts_json(shop_id: int):
    """Recent stock-request outcome alerts for popup on POS and shop pages."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    try:
        from database import list_shop_stock_request_alerts_for_shop

        rows = list_shop_stock_request_alerts_for_shop(shop_id=shop_id, limit=20)
    except Exception:
        rows = []
    out = []
    for r in rows or []:
        ca = r.get("created_at")
        if hasattr(ca, "isoformat"):
            ca = ca.isoformat(sep=" ", timespec="seconds")
        else:
            ca = str(ca) if ca is not None else None
        title = (r.get("title") or "").strip()
        cancelled = "cancel" in title.lower()
        out.append(
            {
                "id": int(r.get("id") or 0),
                "title": title or "Stock request update",
                "message": (r.get("message") or "").strip(),
                "link_url": (r.get("link_url") or "").strip() or None,
                "created_at": ca,
                "kind": "cancelled" if cancelled else "approved",
            }
        )
    return jsonify({"ok": True, "alerts": out})


def _shop_active_sku_count(shop_id: int) -> int:
    try:
        from database import get_cursor

        with get_cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) AS c
                FROM shop_items si
                JOIN items i ON i.id = si.item_id
                WHERE si.shop_id=%s AND i.status='active'
                """,
                (int(shop_id),),
            )
            return int((cur.fetchone() or {}).get("c") or 0)
    except Exception:
        return 0


def _load_shop_stock_live_report_rows(
    shop_id: int, analytics_filter: dict, reorder_threshold: int
) -> Dict[str, Any]:
    low_stock_rows: list = []
    fast_moving_rows: list = []
    valuation_rows: list = []
    highest_value_rows: list = []
    stagnant_rows: list = []
    total_valuation = 0.0
    try:
        from database import _analytics_where_clause, get_cursor, list_shop_stock_manage_items

        items = list_shop_stock_manage_items(shop_id=shop_id, limit=3000) or []
        sst_where, sst_params = _analytics_where_clause(analytics_filter, "sst")
        tx_count_map: Dict[int, int] = {}
        with get_cursor() as cur:
            cur.execute(
                f"""
                SELECT sst.item_id, COUNT(*) AS tx_count
                FROM shop_stock_transactions sst
                WHERE sst.shop_id=%s AND {sst_where}
                GROUP BY sst.item_id
                """,
                tuple([int(shop_id)] + list(sst_params)),
            )
            for rr in (cur.fetchall() or []):
                try:
                    tx_count_map[int(rr.get("item_id") or 0)] = int(rr.get("tx_count") or 0)
                except Exception:
                    continue

        avg_buy_map: Dict[int, float] = {}
        with get_cursor() as cur:
            cur.execute(
                """
                SELECT
                    sst.item_id,
                    SUM(COALESCE(sst.buying_price,0) * sst.qty) AS buy_value,
                    SUM(sst.qty) AS buy_qty
                FROM shop_stock_transactions sst
                WHERE sst.shop_id=%s AND sst.direction='in' AND sst.buying_price IS NOT NULL
                GROUP BY sst.item_id
                """,
                (int(shop_id),),
            )
            for rr in (cur.fetchall() or []):
                try:
                    iid = int(rr.get("item_id") or 0)
                    bq = int(rr.get("buy_qty") or 0)
                    bv = float(rr.get("buy_value") or 0)
                except Exception:
                    continue
                if iid > 0 and bq > 0:
                    avg_buy_map[iid] = bv / bq

        for it in items:
            try:
                iid = int(it.get("id") or 0)
            except Exception:
                continue
            if iid <= 0:
                continue
            stock_qty = int(it.get("shop_stock_qty") or 0)
            tx_count = int(tx_count_map.get(iid, 0))
            avg_buy = float(avg_buy_map.get(iid) or 0.0)
            stock_value = max(stock_qty, 0) * avg_buy
            total_valuation += stock_value
            row = {
                "item_id": iid,
                "name": (it.get("name") or "").strip() or f"Item #{iid}",
                "category": (it.get("category") or "").strip(),
                "stock_qty": stock_qty,
                "tx_count": tx_count,
                "avg_buying_price": avg_buy,
                "stock_value": stock_value,
            }
            valuation_rows.append(row)
            if stock_qty <= reorder_threshold:
                low_stock_rows.append(row)
            if stock_qty > 0 and tx_count == 0:
                stagnant_rows.append(row)

        fast_moving_rows = sorted(
            [r for r in valuation_rows if (r.get("tx_count") or 0) > 0],
            key=lambda r: (r.get("tx_count") or 0),
            reverse=True,
        )
        valuation_rows.sort(key=lambda r: (r.get("name") or "").lower())
        highest_value_rows = sorted(valuation_rows, key=lambda r: r.get("stock_value") or 0, reverse=True)
        low_stock_rows.sort(key=lambda r: (r.get("stock_qty") or 0, -(r.get("tx_count") or 0)))
        stagnant_rows.sort(key=lambda r: (r.get("stock_value") or 0), reverse=True)
    except Exception:
        pass
    return {
        "low_stock_rows": low_stock_rows[:120],
        "fast_moving_rows": fast_moving_rows[:120],
        "valuation_rows": valuation_rows[:300],
        "highest_value_rows": highest_value_rows[:120],
        "stagnant_rows": stagnant_rows[:120],
        "total_valuation": total_valuation,
    }


@app.route("/shops/<int:shop_id>/shop-stock-analytics")
def shop_stock_analytics(shop_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    analytics_filter = _build_analytics_filter()
    reorder_threshold = request.args.get("reorder_threshold", type=int)
    if reorder_threshold is None:
        reorder_threshold = 5
    reorder_threshold = max(0, min(500, int(reorder_threshold)))
    stock_data = {
        "tx_count": 0,
        "qty_in": 0,
        "qty_out": 0,
        "net_qty": 0,
        "distinct_items": 0,
        "top_in_items": [],
        "top_out_items": [],
        "daily": [],
        "source_rows": [],
    }
    sku_count = 0
    try:
        from database import get_shop_stock_analytics

        fetched = get_shop_stock_analytics(
            shop_id=shop_id, analytics_filter=analytics_filter
        )
        if fetched:
            stock_data = fetched
        sku_count = _shop_active_sku_count(shop_id)
    except Exception:
        pass
    return render_template(
        "shop_stock_analytics.html",
        shop=shop,
        analytics_filter=analytics_filter,
        stock_data=stock_data,
        sku_count=sku_count,
        reorder_threshold=reorder_threshold,
        shop_stock_sidebar_focus="analytics",
        theme_key=f"richcom-theme-shop-{shop['id']}",
        theme_default=shop.get("default_theme") or "dark",
        font_family=shop.get("font_family") or "Plus Jakarta Sans",
        primary_color_rgb=_hex_to_rgb_triplet(shop.get("primary_color") or "#10b981"),
        accent_color_rgb=_hex_to_rgb_triplet(shop.get("accent_color") or "#14b8a6"),
    )


def _shop_stock_reports_canonical_query(
    *,
    selected_view: str,
    analytics_filter: dict,
    reorder_threshold: int,
) -> Dict[str, Any]:
    """Stable query dict for shop stock reports URLs (matches redirect normalization)."""
    clean_query: Dict[str, Any] = {
        "view": selected_view,
        "mode": analytics_filter.get("mode") or "single_day",
        "reorder_threshold": reorder_threshold,
    }
    if clean_query["mode"] == "single_day" and analytics_filter.get("single_day"):
        clean_query["single_day"] = analytics_filter.get("single_day")
    elif clean_query["mode"] == "period":
        if analytics_filter.get("start_date"):
            clean_query["start_date"] = analytics_filter.get("start_date")
        if analytics_filter.get("end_date"):
            clean_query["end_date"] = analytics_filter.get("end_date")
    elif clean_query["mode"] == "month" and analytics_filter.get("month"):
        clean_query["month"] = analytics_filter.get("month")
    elif clean_query["mode"] == "year" and analytics_filter.get("year"):
        clean_query["year"] = analytics_filter.get("year")
    return clean_query


@app.route("/shops/<int:shop_id>/shop-stock-profitability-analysis")
def shop_stock_profitability_analysis(shop_id: int):
    """Per-item profitability for this shop (margins, POS period sales, shop stock-in cost basis)."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    analytics_filter = _build_analytics_filter()
    selected_view = (request.args.get("view") or "margin").strip().lower()
    allowed_views = {
        "margin",
        "stock_value",
        "velocity",
        "leakage",
        "low_margin_high_volume",
        "low_stock",
    }
    if selected_view not in allowed_views:
        selected_view = "margin"
    reorder_threshold = request.args.get("reorder_threshold", type=int)
    if reorder_threshold is None:
        reorder_threshold = 5
    reorder_threshold = max(0, min(500, int(reorder_threshold)))

    item_rows = []
    avg_margin_pct = 0.0
    total_stock_value = 0.0
    dead_stock_value = 0.0
    dead_stock_count = 0
    high_value_zero_stock = []
    low_margin_high_volume = []
    low_stock_items = []
    top_velocity_items = []
    margin_rows = []
    stock_value_rows = []

    try:
        from database import (
            get_shop_avg_buying_price_by_item,
            get_shop_item_sales_totals_by_item,
            list_shop_items,
        )

        items = list_shop_items(shop_id=shop_id, limit=3000) or []
        sales_map = get_shop_item_sales_totals_by_item(shop_id, analytics_filter)
        avg_buy_map = get_shop_avg_buying_price_by_item(shop_id)

        margin_sum = 0.0
        margin_n = 0
        for it in items:
            try:
                iid = int(it.get("id") or 0)
            except Exception:
                continue
            if iid <= 0:
                continue

            try:
                selling_price = float(
                    it.get("selling_price")
                    if it.get("selling_price") is not None
                    else (it.get("price") or 0)
                )
            except Exception:
                selling_price = 0.0
            avg_buying_price = avg_buy_map.get(iid)
            qty_sold = int((sales_map.get(iid) or {}).get("qty_sold") or 0)
            revenue = float((sales_map.get(iid) or {}).get("revenue") or 0)
            stock_qty = int(it.get("shop_stock_qty") or 0)
            company_low_stock_threshold = max(0, int(it.get("low_stock_threshold") or 0))
            company_reorder_level = max(0, int(it.get("reorder_level") or 0))
            shop_low_stock_threshold = max(0, int(it.get("shop_low_stock_threshold") or 0))
            shop_reorder_level = max(0, int(it.get("shop_reorder_level") or 0))
            row_low_stock_threshold = (
                shop_low_stock_threshold if shop_low_stock_threshold > 0 else company_low_stock_threshold
            )
            row_reorder_level = shop_reorder_level if shop_reorder_level > 0 else company_reorder_level

            margin_amount = None
            margin_pct = None
            if avg_buying_price is not None and selling_price > 0:
                margin_amount = selling_price - avg_buying_price
                margin_pct = (margin_amount / selling_price) * 100.0
                margin_sum += margin_pct
                margin_n += 1

            stock_value = max(stock_qty, 0) * (avg_buying_price or 0.0)
            velocity = qty_sold / max(stock_qty, 1) if qty_sold > 0 else 0.0

            row = {
                "item_id": iid,
                "category": (it.get("category") or "").strip(),
                "name": (it.get("name") or "").strip() or f"Item #{iid}",
                "selling_price": selling_price,
                "avg_buying_price": avg_buying_price,
                "margin_amount": margin_amount,
                "margin_pct": margin_pct,
                "stock_qty": stock_qty,
                "stock_value": stock_value,
                "qty_sold": qty_sold,
                "revenue": revenue,
                "velocity": velocity,
                "low_stock_threshold": row_low_stock_threshold,
                "reorder_level": row_reorder_level,
            }
            item_rows.append(row)
            total_stock_value += stock_value

            if stock_qty > 0 and qty_sold == 0:
                dead_stock_count += 1
                dead_stock_value += stock_value
            if stock_qty <= 0 and revenue > 0:
                high_value_zero_stock.append(row)
            if row_low_stock_threshold > 0 and stock_qty <= row_low_stock_threshold:
                low_stock_items.append(row)
            if margin_pct is not None and margin_pct <= 18.0 and qty_sold >= 5:
                low_margin_high_volume.append(row)
            if margin_pct is not None:
                margin_rows.append(row)

        avg_margin_pct = (margin_sum / margin_n) if margin_n else 0.0
        margin_rows.sort(key=lambda r: (r.get("margin_pct") if r.get("margin_pct") is not None else -9999))
        stock_value_rows = sorted(item_rows, key=lambda r: r.get("stock_value") or 0, reverse=True)
        high_value_zero_stock.sort(key=lambda r: r.get("revenue") or 0, reverse=True)
        low_margin_high_volume.sort(
            key=lambda r: ((r.get("qty_sold") or 0), -(r.get("margin_pct") or 0)), reverse=True
        )
        low_stock_items.sort(key=lambda r: (r.get("stock_qty") or 0, -(r.get("qty_sold") or 0)))
        top_velocity_items = sorted(item_rows, key=lambda r: r.get("velocity") or 0, reverse=True)
    except Exception:
        item_rows = []

    return render_template(
        "shop_stock_profitability_analysis.html",
        shop=shop,
        analytics_filter=analytics_filter,
        shop_stock_sidebar_focus="profitability",
        selected_view=selected_view,
        reorder_threshold=reorder_threshold,
        item_rows=item_rows,
        avg_margin_pct=avg_margin_pct,
        total_stock_value=total_stock_value,
        dead_stock_value=dead_stock_value,
        dead_stock_count=dead_stock_count,
        margin_rows=(margin_rows[:100]),
        stock_value_rows=(stock_value_rows[:100]),
        high_value_zero_stock=(high_value_zero_stock[:100]),
        low_margin_high_volume=(low_margin_high_volume[:100]),
        low_stock_items=(low_stock_items[:150]),
        top_velocity_items=(top_velocity_items[:100]),
        theme_key=f"richcom-theme-shop-{shop['id']}",
        theme_default=shop.get("default_theme") or "dark",
        font_family=shop.get("font_family") or "Plus Jakarta Sans",
        primary_color_rgb=_hex_to_rgb_triplet(shop.get("primary_color") or "#10b981"),
        accent_color_rgb=_hex_to_rgb_triplet(shop.get("accent_color") or "#14b8a6"),
    )


@app.route("/shops/<int:shop_id>/shop-stock-reports")
def shop_stock_reports(shop_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    analytics_filter = _build_analytics_filter()
    selected_view = (request.args.get("view") or "low_stock").strip().lower()
    allowed_views = {"low_stock", "fast_moving", "valuation", "highest_value", "stagnant"}
    if selected_view not in allowed_views:
        selected_view = "low_stock"
    reorder_threshold = request.args.get("reorder_threshold", type=int)
    if reorder_threshold is None:
        reorder_threshold = 5
    reorder_threshold = max(0, min(500, int(reorder_threshold)))
    # Canonicalize query params so the page URL does not keep empty fields.
    clean_query = _shop_stock_reports_canonical_query(
        selected_view=selected_view,
        analytics_filter=analytics_filter,
        reorder_threshold=reorder_threshold,
    )
    if request.args.to_dict(flat=True) != {k: str(v) for k, v in clean_query.items()}:
        return redirect(url_for("shop_stock_reports", shop_id=shop_id, **clean_query))
    bundle = _load_shop_stock_live_report_rows(shop_id, analytics_filter, reorder_threshold)
    return render_template(
        "shop_stock_reports.html",
        shop=shop,
        analytics_filter=analytics_filter,
        shop_stock_sidebar_focus="reports",
        selected_view=selected_view,
        reorder_threshold=reorder_threshold,
        total_valuation=bundle["total_valuation"],
        low_stock_rows=bundle["low_stock_rows"],
        fast_moving_rows=bundle["fast_moving_rows"],
        valuation_rows=bundle["valuation_rows"],
        highest_value_rows=bundle["highest_value_rows"],
        stagnant_rows=bundle["stagnant_rows"],
        theme_key=f"richcom-theme-shop-{shop['id']}",
        theme_default=shop.get("default_theme") or "dark",
        font_family=shop.get("font_family") or "Plus Jakarta Sans",
        primary_color_rgb=_hex_to_rgb_triplet(shop.get("primary_color") or "#10b981"),
        accent_color_rgb=_hex_to_rgb_triplet(shop.get("accent_color") or "#14b8a6"),
    )


@app.route("/shops/<int:shop_id>/shop-stock-audits")
def shop_stock_audits(shop_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    row_limit = request.args.get("limit", type=int)
    if row_limit is None or row_limit < 1:
        row_limit = 25000
    row_limit = min(row_limit, 50000)

    _audit_all = {
        "mode": "all",
        "range_label": f"All dates (newest first, up to {row_limit:,} rows)",
    }
    mode_arg = (request.args.get("mode") or "").strip().lower()
    if not request.args or mode_arg == "all":
        analytics_filter = _audit_all
    elif mode_arg in ("single_day", "period", "month", "year"):
        analytics_filter = _build_analytics_filter()
    else:
        analytics_filter = _audit_all
    direction = (request.args.get("direction") or "").strip().lower()
    if direction not in ("", "in", "out"):
        direction = ""
    source_f = (request.args.get("source") or "").strip().lower()
    if source_f not in ("", "company", "manual", "transfer"):
        source_f = ""
    item_id = request.args.get("item_id", type=int)
    q = (request.args.get("q") or "").strip()

    txs = []
    filter_items = []
    try:
        from database import list_shop_items, list_shop_stock_audit_rows

        filter_items = list_shop_items(shop_id=shop_id, limit=3000) or []
        txs = list_shop_stock_audit_rows(
            shop_id=shop_id,
            limit=row_limit,
            analytics_filter=analytics_filter,
            direction=direction or None,
            source=source_f or None,
            item_id=item_id if item_id and item_id > 0 else None,
            search=q or None,
        )
    except Exception:
        txs = []
    return render_template(
        "shop_stock_audits.html",
        shop=shop,
        transactions=txs,
        analytics_filter=analytics_filter,
        audit_direction=direction,
        audit_source=source_f,
        audit_item_id=item_id if item_id and item_id > 0 else None,
        audit_q=q,
        audit_row_limit=row_limit,
        filter_items=filter_items,
        theme_key=f"richcom-theme-shop-{shop['id']}",
        theme_default=shop.get("default_theme") or "dark",
        font_family=shop.get("font_family") or "Plus Jakarta Sans",
        primary_color_rgb=_hex_to_rgb_triplet(shop.get("primary_color") or "#10b981"),
        accent_color_rgb=_hex_to_rgb_triplet(shop.get("accent_color") or "#14b8a6"),
    )


@app.route("/shops/<int:shop_id>/shop-item/<int:item_id>/toggle-display", methods=["POST"])
def shop_item_toggle_displayed(shop_id: int, item_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate

    wants_json = _request_wants_json()
    ok_msg = "Shop item display updated."
    err_msg = (
        "Could not update shop display — to turn it on, the company item must be active under IT item management."
    )
    try:
        from database import ensure_shop_items_for_shop, list_shop_items, toggle_shop_item_displayed

        ensure_shop_items_for_shop(shop_id)
        ok = toggle_shop_item_displayed(shop_id=shop_id, item_id=item_id)
        row = None
        if ok:
            for it in list_shop_items(shop_id=shop_id, limit=500) or []:
                if int(it.get("id") or 0) == int(item_id):
                    row = it
                    break
    except Exception:
        ok, row = False, None

    if wants_json:
        if ok and row is not None:
            return jsonify(
                {
                    "ok": True,
                    "message": ok_msg,
                    "item_id": item_id,
                    "displayed": int(row.get("displayed") or 0) == 1,
                }
            )
        return jsonify({"ok": False, "error": err_msg}), 400

    if ok:
        flash(ok_msg, "success")
    else:
        flash(err_msg, "error")
    return redirect(url_for("shop_item_management", shop_id=shop_id))


@app.route("/shops/<int:shop_id>/shop-item/<int:item_id>/toggle-stock-update", methods=["POST"])
def shop_item_toggle_stock_update_enabled(shop_id: int, item_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate

    wants_json = _request_wants_json()
    mode = _pos_inventory_mode(shop)
    try:
        from database import ensure_shop_items_for_shop, list_shop_items, toggle_shop_item_stock_update_enabled

        ensure_shop_items_for_shop(shop_id)
        ok = toggle_shop_item_stock_update_enabled(shop_id=shop_id, item_id=item_id)
        row = None
        if ok:
            for it in list_shop_items(shop_id=shop_id, limit=500) or []:
                if int(it.get("id") or 0) == int(item_id):
                    row = it
                    break
    except Exception:
        ok, row = False, None

    if ok:
        if mode == "kitchen":
            msg = "Kitchen portion update setting updated."
        elif mode == "shop":
            msg = "Shop stock update setting updated."
        elif mode == "both":
            msg = "Kitchen portion (POS) toggle updated. Shelf stock uses Stock management registration separately."
        else:
            msg = "Branch POS inventory toggle updated."
    elif mode == "shop":
        msg = (
            "Stock update toggle was not saved. To turn ON, the company item must be active and company-wide "
            "stock updates must be enabled under IT catalog."
        )
    elif mode == "kitchen":
        msg = (
            "Kitchen portion toggle was not saved. To turn ON, the company item must be active and IT must enable "
            "the same master toggle under item management (kitchen mode uses this for portion deductions)."
        )
    elif mode == "both":
        msg = (
            "Toggle was not saved. To turn ON, the company item must be active, IT must enable the catalog master "
            "toggle, and stock rules must apply (both kitchen portions and shop stock use this toggle)."
        )
    else:
        msg = (
            "Toggle was not saved. To turn ON, the company item must be active and IT must enable the master POS "
            "inventory toggle under item management."
        )

    if wants_json:
        if ok and row is not None:
            return jsonify(
                {
                    "ok": True,
                    "message": msg,
                    "item_id": item_id,
                    "stock_update_enabled": int(row.get("stock_update_enabled") or 0) == 1,
                }
            )
        return jsonify({"ok": False, "error": msg}), 400

    if ok:
        flash(msg, "success")
    else:
        flash(msg, "error")
    return redirect(url_for("shop_item_management", shop_id=shop_id))


@app.route("/shops/<int:shop_id>/shop-item-management/bulk-display", methods=["POST"])
def shop_item_bulk_display(shop_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    wants_json = _request_wants_json()
    state = (request.form.get("state") or "").strip().lower()
    displayed = state == "on"
    if state not in ("on", "off"):
        if wants_json:
            return jsonify({"ok": False, "error": "Invalid bulk display action."}), 400
        flash("Invalid bulk display action.", "error")
        return redirect(url_for("shop_item_management", shop_id=shop_id))
    try:
        from database import ensure_shop_items_for_shop, set_all_shop_items_displayed

        ensure_shop_items_for_shop(shop_id)
        count = set_all_shop_items_displayed(shop_id, displayed)
    except Exception:
        count = 0
    if count > 0:
        msg = f"Display turned {'on' if displayed else 'off'} for {count} item(s) at this shop."
        if wants_json:
            return jsonify(
                {
                    "ok": True,
                    "message": msg,
                    "count": count,
                    "displayed": displayed,
                }
            )
        flash(msg, "success")
    else:
        msg = (
            "No items updated."
            if not displayed
            else "No items updated — company items must be active before display can be turned on."
        )
        if wants_json:
            return jsonify({"ok": False, "error": msg}), 400
        flash(msg, "error" if displayed else "success")
    return redirect(url_for("shop_item_management", shop_id=shop_id))


@app.route("/shops/<int:shop_id>/shop-item-management/bulk-stock-update", methods=["POST"])
def shop_item_bulk_stock_update(shop_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    wants_json = _request_wants_json()
    state = (request.form.get("state") or "").strip().lower()
    enabled = state == "on"
    if state not in ("on", "off"):
        if wants_json:
            return jsonify({"ok": False, "error": "Invalid bulk stock action."}), 400
        flash("Invalid bulk stock action.", "error")
        return redirect(url_for("shop_item_management", shop_id=shop_id))
    try:
        from database import ensure_shop_items_for_shop, set_all_shop_items_stock_update_enabled

        ensure_shop_items_for_shop(shop_id)
        count = set_all_shop_items_stock_update_enabled(shop_id, enabled)
    except Exception:
        count = 0
    mode = _pos_inventory_mode(shop)
    if count > 0:
        if enabled:
            if mode == "kitchen":
                msg = f"Kitchen portion updates turned on for {count} item(s)."
            elif mode == "shop":
                msg = f"Shop stock updates turned on for {count} item(s)."
            elif mode == "both":
                msg = f"Kitchen portion (POS) updates turned on for {count} item(s)."
            else:
                msg = f"POS inventory updates turned on for {count} item(s)."
        else:
            if mode == "kitchen":
                msg = f"Kitchen portion updates turned off for {count} item(s)."
            elif mode == "shop":
                msg = f"Shop stock updates turned off for {count} item(s)."
            elif mode == "both":
                msg = f"Kitchen portion (POS) updates turned off for {count} item(s)."
            else:
                msg = f"POS inventory updates turned off for {count} item(s)."
        if wants_json:
            return jsonify(
                {
                    "ok": True,
                    "message": msg,
                    "count": count,
                    "stock_update_enabled": enabled,
                }
            )
        flash(msg, "success")
    else:
        msg = (
            "No items updated."
            if not enabled
            else "No items updated — company items must be active with stock master on before shop stock can be turned on."
        )
        if wants_json:
            return jsonify({"ok": False, "error": msg}), 400
        flash(msg, "error" if enabled else "success")
    return redirect(url_for("shop_item_management", shop_id=shop_id))


@app.route("/shops/<int:shop_id>/shop-item/<int:item_id>/selling-price", methods=["POST"])
def shop_item_update_selling_price(shop_id: int, item_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate

    raw = (request.form.get("selling_price") or "").strip()
    try:
        sp = float(raw)
        if sp < 0:
            raise ValueError()
    except Exception:
        flash("Enter a valid selling price.", "error")
        return redirect(url_for("shop_item_management", shop_id=shop_id))

    try:
        from database import ensure_shop_items_for_shop, update_item_selling_price_for_shop

        ensure_shop_items_for_shop(shop_id)
        ok = update_item_selling_price_for_shop(shop_id=shop_id, item_id=item_id, selling_price=sp)
    except Exception:
        ok = False

    flash("Selling price updated." if ok else "Could not update selling price.", "success" if ok else "error")
    return redirect(url_for("shop_item_management", shop_id=shop_id))


def _render_shop_analytics_view(shop_id: int, analytics_view: str):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    if analytics_view == "credit" and not _shop_pos_allow_credit_sale(shop):
        return redirect(url_for("shop_analytics", shop_id=shop_id))
    analytics_filter = _build_analytics_filter()
    analytics_scope = _analytics_scope_from_request()
    revenue_data = None
    if analytics_view == "revenue":
        try:
            from database import get_shop_revenue_analytics

            revenue_data = get_shop_revenue_analytics(
                shop_id=shop_id,
                analytics_filter=analytics_filter,
                analytics_scope=analytics_scope,
            )
        except Exception:
            revenue_data = {
                "sale": {"amount": 0.0, "count": 0},
                "credit": {"amount": 0.0, "count": 0},
                "total_amount": 0.0,
                "total_count": 0,
                "daily": [],
            }
    item_data = None
    if analytics_view == "item":
        try:
            from database import get_shop_item_analytics

            item_data = get_shop_item_analytics(
                shop_id=shop_id,
                analytics_filter=analytics_filter,
                analytics_scope=analytics_scope,
            )
        except Exception:
            item_data = {
                "total_qty": 0,
                "total_revenue": 0.0,
                "line_count": 0,
                "distinct_items": 0,
                "top_items": [],
                "peak_day": None,
                "peak_hour": None,
            }
    period_data = None
    if analytics_view == "period":
        try:
            from database import get_shop_period_analytics

            period_data = get_shop_period_analytics(
                shop_id=shop_id,
                analytics_filter=analytics_filter,
                analytics_scope=analytics_scope,
            )
        except Exception:
            period_data = {
                "total_tx_count": 0,
                "total_revenue": 0.0,
                "daily": [],
                "hourly": [],
                "peak_day": None,
                "peak_hour": None,
                "employees": [],
                "top_employee": None,
                "least_employee": None,
            }
    sales_data = None
    if analytics_view == "sales":
        try:
            from database import get_shop_sales_analytics

            sales_data = get_shop_sales_analytics(
                shop_id=shop_id,
                analytics_filter=analytics_filter,
                analytics_scope=analytics_scope,
            )
        except Exception:
            sales_data = {
                "total_tx_count": 0,
                "total_revenue": 0.0,
                "daily": [],
                "hourly": [],
                "peak_day": None,
                "peak_hour": None,
                "employees": [],
                "top_employee": None,
                "least_employee": None,
            }
    credit_data = None
    customer_data = None
    if analytics_view == "credit":
        try:
            from database import get_shop_credit_analytics

            credit_data = get_shop_credit_analytics(
                shop_id=shop_id,
                analytics_filter=analytics_filter,
                analytics_scope=analytics_scope,
            )
        except Exception:
            credit_data = {
                "total_tx_count": 0,
                "total_revenue": 0.0,
                "daily": [],
                "hourly": [],
                "peak_day": None,
                "peak_hour": None,
                "employees": [],
                "top_employee": None,
                "least_employee": None,
                "customers": [],
                "top_customer": None,
                "customer_details": {},
            }
    if analytics_view == "customer":
        try:
            from database import get_shop_customer_analytics

            customer_data = get_shop_customer_analytics(
                shop_id=shop_id,
                analytics_filter=analytics_filter,
                analytics_scope=analytics_scope,
            )
        except Exception:
            customer_data = {
                "total_tx_count": 0,
                "total_amount": 0.0,
                "distinct_customers": 0,
                "customers": [],
            }
    stock_data = None
    shop_sku_count = None
    reorder_threshold = 5
    if analytics_view == "stock":
        reorder_threshold = request.args.get("reorder_threshold", type=int)
        if reorder_threshold is None:
            reorder_threshold = 5
        reorder_threshold = max(0, min(500, int(reorder_threshold)))
        stock_data = {
            "tx_count": 0,
            "qty_in": 0,
            "qty_out": 0,
            "net_qty": 0,
            "distinct_items": 0,
            "top_in_items": [],
            "top_out_items": [],
            "daily": [],
            "source_rows": [],
        }
        try:
            from database import get_shop_stock_analytics

            fetched = get_shop_stock_analytics(
                shop_id=shop_id, analytics_filter=analytics_filter
            )
            if fetched:
                stock_data = fetched
            shop_sku_count = _shop_active_sku_count(shop_id)
        except Exception:
            pass
    shop_view_data_by_view = {
        "revenue": revenue_data,
        "item": item_data,
        "period": period_data,
        "sales": sales_data,
        "credit": credit_data,
        "customer": customer_data,
        "stock": stock_data,
    }
    shop_view_data = shop_view_data_by_view.get(analytics_view)
    return render_template(
        "shop_analytics.html",
        shop=shop,
        shop_view=analytics_view,
        analytics_view=analytics_view,
        shop_view_data=shop_view_data,
        analytics_filter=analytics_filter,
        analytics_scope=analytics_scope,
        revenue_data=revenue_data,
        item_data=item_data,
        period_data=period_data,
        sales_data=sales_data,
        credit_data=credit_data,
        customer_data=customer_data,
        stock_data=stock_data,
        shop_sku_count=shop_sku_count,
        reorder_threshold=reorder_threshold if analytics_view == "stock" else None,
        shop_portal=True,
        selected_shop_id=shop_id,
        pos_allow_credit_sale=_shop_pos_allow_credit_sale(shop),
        pos_inventory_mode=_pos_inventory_mode(shop),
        theme_key=f"richcom-theme-shop-{shop['id']}",
        theme_default=shop.get("default_theme") or "dark",
        font_family=shop.get("font_family") or "Plus Jakarta Sans",
        primary_color_rgb=_hex_to_rgb_triplet(shop.get("primary_color") or "#10b981"),
        accent_color_rgb=_hex_to_rgb_triplet(shop.get("accent_color") or "#14b8a6"),
    )


@app.route("/shops/<int:shop_id>/shop-analytics")
def shop_analytics(shop_id: int):
    return _render_shop_analytics_view(shop_id, "revenue")


@app.route("/shops/<int:shop_id>/shop-revenue-analytics")
def shop_revenue_analytics(shop_id: int):
    return _render_shop_analytics_view(shop_id, "revenue")


@app.route("/shops/<int:shop_id>/shop-item-analytics")
def shop_item_analytics(shop_id: int):
    return _render_shop_analytics_view(shop_id, "item")


@app.route("/shops/<int:shop_id>/shop-period-analytics")
def shop_period_analytics(shop_id: int):
    return _render_shop_analytics_view(shop_id, "period")


@app.route("/shops/<int:shop_id>/shop-sales-analytics")
def shop_sales_analytics(shop_id: int):
    return _render_shop_analytics_view(shop_id, "sales")


@app.route("/shops/<int:shop_id>/shop-credit-analytics")
def shop_credit_analytics(shop_id: int):
    return _render_shop_analytics_view(shop_id, "credit")


@app.route("/shops/<int:shop_id>/shop-customer-analytics")
def shop_customer_analytics(shop_id: int):
    return _render_shop_analytics_view(shop_id, "customer")


def _parse_row_created_at(value) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    s = str(value).strip()
    if not s:
        return None
    try:
        if len(s) >= 19:
            return datetime.strptime(s[:19].replace("T", " "), "%Y-%m-%d %H:%M:%S")
        return datetime.strptime(s[:10], "%Y-%m-%d")
    except Exception:
        return None


def _row_in_analytics_filter(row: dict, analytics_filter: dict, key: str = "created_at") -> bool:
    dt = _parse_row_created_at(row.get(key))
    if not dt:
        return True
    try:
        d = dt.date()
        rs = date.fromisoformat(str(analytics_filter.get("range_start") or ""))
        re = date.fromisoformat(str(analytics_filter.get("range_end_exclusive") or ""))
        return rs <= d < re
    except Exception:
        return True


@app.route("/shops/<int:shop_id>/shop-customer-analytics/detail")
def shop_customer_analytics_detail(shop_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    customer_name = (request.args.get("customer_name") or "").strip() or "WALK IN"
    customer_phone = (request.args.get("customer_phone") or "").strip() or "-"
    analytics_filter = _build_analytics_filter()
    analytics_scope = _analytics_scope_from_request()
    items_by_sale_id: Dict[int, list] = {}
    try:
        from database import (
            get_it_support_customer_detail_analytics,
            get_it_support_customer_transaction_items,
            get_it_support_customer_transactions,
        )

        customer_analytics = get_it_support_customer_detail_analytics(
            customer_name=customer_name,
            customer_phone=customer_phone,
            analytics_filter=analytics_filter,
            shop_id=shop_id,
            analytics_scope=analytics_scope,
        )
        transactions = get_it_support_customer_transactions(
            customer_name=customer_name,
            customer_phone=customer_phone,
            analytics_filter=analytics_filter,
            shop_id=shop_id,
            limit=3000,
            analytics_scope=analytics_scope,
        )
        tx_item_rows = get_it_support_customer_transaction_items(
            customer_name=customer_name,
            customer_phone=customer_phone,
            limit=5000,
            analytics_filter=analytics_filter,
            shop_id=shop_id,
            analytics_scope=analytics_scope,
        )
        for row in tx_item_rows or []:
            try:
                sale_id = int(row.get("sale_id") or 0)
            except Exception:
                sale_id = 0
            if sale_id <= 0:
                continue
            items_by_sale_id.setdefault(sale_id, []).append(
                {
                    "item_name": row.get("item_name") or "Item",
                    "qty": int(row.get("qty") or 0),
                    "amount": float(row.get("amount") or 0),
                    "picked_at": _credit_item_pick_date(row.get("created_at")),
                }
            )
    except Exception:
        customer_analytics = {
            "total_amount": 0.0,
            "total_tx_count": 0,
            "sale_amount": 0.0,
            "sale_tx_count": 0,
            "credit_amount": 0.0,
            "credit_tx_count": 0,
            "total_item_qty": 0,
            "distinct_items": 0,
            "avg_ticket": 0.0,
            "daily": [],
            "hourly": [],
            "daily_sale": [],
            "daily_credit": [],
            "hourly_sale": [],
            "hourly_credit": [],
            "shops": [],
            "employees": [],
            "top_items": [],
        }
        transactions = []
        items_by_sale_id = {}
    note_ctx = _shop_customer_credit_note_context(
        shop_id,
        customer_name,
        customer_phone,
        analytics_filter=analytics_filter,
        analytics_scope=analytics_scope,
    )
    # note_ctx also carries analytics_scope and credit-only line items; keep full tx map for raw view.
    note_ctx.pop("transaction_items_by_sale_id", None)
    allow_credit = _shop_pos_allow_credit_sale(shop)
    return render_template(
        "shop_customer_analytics_detail.html",
        shop=shop,
        customer_name=customer_name,
        customer_phone=customer_phone,
        analytics_filter=analytics_filter,
        customer_analytics=customer_analytics,
        transactions=transactions,
        transaction_items_by_sale_id=items_by_sale_id,
        embed_record_payment=allow_credit,
        payment_return_to="analytics",
        pos_allow_credit_sale=allow_credit,
        **note_ctx,
        theme_key=f"richcom-theme-shop-{shop['id']}",
        theme_default=shop.get("default_theme") or "dark",
        font_family=shop.get("font_family") or "Plus Jakarta Sans",
        primary_color_rgb=_hex_to_rgb_triplet(shop.get("primary_color") or "#10b981"),
        accent_color_rgb=_hex_to_rgb_triplet(shop.get("accent_color") or "#14b8a6"),
    )


@app.route("/shops/<int:shop_id>/shop-hr-management")
def shop_hr_management(shop_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    return render_template(
        "shop_hr_management.html",
        shop=shop,
        theme_key=f"richcom-theme-shop-{shop['id']}",
        theme_default=shop.get("default_theme") or "dark",
        font_family=shop.get("font_family") or "Plus Jakarta Sans",
        primary_color_rgb=_hex_to_rgb_triplet(shop.get("primary_color") or "#10b981"),
        accent_color_rgb=_hex_to_rgb_triplet(shop.get("accent_color") or "#14b8a6"),
    )


@app.route("/shops/<int:shop_id>/shop-audits")
def shop_audits(shop_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    return render_template(
        "shop_audits.html",
        shop=shop,
        theme_key=f"richcom-theme-shop-{shop['id']}",
        theme_default=shop.get("default_theme") or "dark",
        font_family=shop.get("font_family") or "Plus Jakarta Sans",
        primary_color_rgb=_hex_to_rgb_triplet(shop.get("primary_color") or "#10b981"),
        accent_color_rgb=_hex_to_rgb_triplet(shop.get("accent_color") or "#14b8a6"),
    )


def _shop_settings_template_extra(shop: dict) -> dict:
    sid = shop["id"]
    return {
        "printing_settings": _effective_printing_settings_for_shop(shop),
        "receipt_settings": _effective_receipt_settings_for_shop(shop),
        "appearance_settings": _effective_appearance_settings_for_shop(shop),
        "shop_printing_custom": _shop_has_printing_override(shop),
        "shop_receipt_custom": _shop_has_receipt_override(shop),
        "shop_appearance_custom": _shop_has_appearance_override(shop),
        "company_settings": _effective_company_settings_for_shop(shop),
        "shop_company_custom": _shop_has_company_override(shop),
        "company_opening_hours": _shop_opening_hours_for_form(shop),
        "shop_opening_hours_custom": _shop_has_opening_hours_override(shop),
        "company_weekdays": COMPANY_WEEKDAYS,
        "pos_allow_credit_sale": _shop_pos_allow_credit_sale(shop),
        **_shop_theme_template_vars(shop),
    }


@app.route("/shops/<int:shop_id>/shop-settings")
def shop_settings(shop_id: int):
    return redirect(url_for("shop_settings_appearance", shop_id=shop_id), code=302)


@app.route("/shops/<int:shop_id>/shop-current-stock")
def shop_current_stock(shop_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    items: list = []
    movement_lookback_days = 30
    try:
        from database import ensure_shop_items_for_shop, get_shop_item_stock_movement_summary, list_shop_items

        ensure_shop_items_for_shop(shop_id)
        items = list_shop_items(shop_id=shop_id, limit=5000) or []
        movement_map = get_shop_item_stock_movement_summary(
            shop_id=shop_id, lookback_days=movement_lookback_days
        ) or {}
        for it in items:
            try:
                iid = int(it.get("id") or 0)
            except Exception:
                iid = 0
            mv = movement_map.get(iid) or {}
            out_qty = float(mv.get("out_qty") or 0)
            days = max(1, int(mv.get("lookback_days") or movement_lookback_days))
            it["avg_used_per_day"] = round(out_qty / days, 1) if out_qty > 0 else 0
        items.sort(
            key=lambda it: (
                -float(it.get("avg_used_per_day") or 0),
                str(it.get("name") or "").lower(),
            )
        )
    except Exception:
        items = []
        movement_lookback_days = 30

    return render_template(
        "shop_current_stock.html",
        shop=shop,
        items=items,
        item_count=len(items),
        movement_lookback_days=movement_lookback_days,
        shop_stock_sidebar_focus="current_stock",
        **_shop_theme_template_vars(shop),
    )


@app.route("/shops/<int:shop_id>/shop-stock-settings", methods=["GET", "POST"])
def shop_stock_settings(shop_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    try:
        from database import (
            get_shop_item_stock_movement_summary,
            init_shop_items_table,
            list_shop_items,
            set_shop_item_stock_alert_levels,
        )

        init_shop_items_table()
        items = list_shop_items(shop_id=shop_id, limit=5000) or []
        movement_map = get_shop_item_stock_movement_summary(shop_id=shop_id, lookback_days=30) or {}
    except Exception:
        items = []
        movement_map = {}

    # Suggest reorder levels only (does not mutate stock). Suggestions adapt to sales + stock movement + current stock.
    for it in items:
        try:
            iid = int(it.get("id") or 0)
        except Exception:
            iid = 0
        suggested_int = _compute_shop_reorder_suggestion(
            stock_qty=int(it.get("shop_stock_qty") or 0),
            movement_row=(movement_map.get(iid) or {}),
        )

        it["suggested_reorder_level"] = suggested_int
        # Use suggestions as dynamic defaults only when shop override isn't set.
        if int(it.get("shop_reorder_level") or 0) <= 0 and suggested_int > 0:
            it["shop_reorder_level"] = suggested_int

    if request.method == "POST":
        wants_json = _request_wants_json()
        scope = (request.form.get("save_scope") or "all").strip().lower()
        if scope not in ("workspace", "item", "all"):
            scope = "all"
        updated = 0
        ws_saved = False
        try:
            from database import set_shop_item_stock_alert_levels

            if scope in ("workspace", "all"):
                ws_saved = _save_shop_stock_workspace_settings(
                    shop_id, _shop_stock_workspace_settings_from_form()
                )

            if scope in ("item", "all"):
                if scope == "item":
                    try:
                        iid = int(request.form.get("item_id") or 0)
                    except Exception:
                        iid = 0
                    item_rows = [it for it in items if int(it.get("id") or 0) == iid]
                else:
                    item_rows = items

                for it in item_rows:
                    iid = int(it.get("id") or 0)
                    if iid <= 0:
                        continue
                    lo_raw = (request.form.get(f"shop_low_stock_threshold_{iid}") or "0").strip()
                    rl_raw = (request.form.get(f"shop_reorder_level_{iid}") or "0").strip()
                    try:
                        lo_v = int(float(lo_raw or 0))
                    except Exception:
                        lo_v = 0
                    try:
                        rl_v = int(float(rl_raw or 0))
                    except Exception:
                        rl_v = 0
                    if set_shop_item_stock_alert_levels(shop_id, iid, lo_v, rl_v):
                        updated += 1

            if wants_json:
                if scope == "item" and not item_rows:
                    return jsonify({"ok": False, "error": "Invalid item."}), 400
                saved = ws_saved or updated > 0
                return jsonify(
                    {
                        "ok": True,
                        "saved": saved,
                        "scope": scope,
                        "updated": updated,
                        "workspace_saved": ws_saved,
                        "message": "Saved." if saved else "No change.",
                    }
                )

            if ws_saved or updated:
                flash("Shop stock settings updated.", "success")
            else:
                flash("No stock settings changed.", "success")
        except Exception:
            if wants_json:
                return jsonify({"ok": False, "error": "Could not update shop stock settings."}), 500
            flash("Could not update shop stock settings.", "error")
        return redirect(url_for("shop_stock_settings", shop_id=shop_id))

    stock_workspace_settings = _load_shop_stock_workspace_settings(shop)

    return render_template(
        "shop_stock_settings.html",
        shop=shop,
        items=items,
        stock_workspace_settings=stock_workspace_settings,
        theme_key=f"richcom-theme-shop-{shop['id']}",
        theme_default=shop.get("default_theme") or "dark",
        font_family=shop.get("font_family") or "Plus Jakarta Sans",
        primary_color_rgb=_hex_to_rgb_triplet(shop.get("primary_color") or "#10b981"),
        accent_color_rgb=_hex_to_rgb_triplet(shop.get("accent_color") or "#14b8a6"),
    )


@app.route("/shops/<int:shop_id>/shop-settings/appearance", methods=["GET", "POST"])
def shop_settings_appearance(shop_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    if request.method == "POST":
        use_custom_appearance = (request.form.get("shop_appearance_custom") or "").strip() == "1"
        default_theme = (request.form.get("default_theme") or "").strip().lower()
        from theme_presets import normalize_font_family

        font_family = normalize_font_family((request.form.get("font_family") or "").strip())
        primary_color = (request.form.get("primary_color") or "").strip()
        accent_color = (request.form.get("accent_color") or "").strip()
        appearance_json = None
        if use_custom_appearance:
            appearance_json = json.dumps(
                {
                    "default_theme": default_theme,
                    "font_family": font_family,
                    "primary_color": primary_color,
                    "accent_color": accent_color,
                },
                separators=(",", ":"),
            )
        try:
            from database import update_shop_settings

            ok = update_shop_settings(
                shop["id"],
                default_theme=default_theme if use_custom_appearance else shop["default_theme"],
                font_family=font_family if use_custom_appearance else shop["font_family"],
                primary_color=primary_color if use_custom_appearance else shop["primary_color"],
                accent_color=accent_color if use_custom_appearance else shop["accent_color"],
                printing_settings_json=shop.get("printing_settings_json"),
                receipt_settings_json=shop.get("receipt_settings_json"),
                appearance_settings_json=appearance_json,
                company_settings_json=shop.get("company_settings_json"),
            )
        except Exception:
            ok = False
        flash(
            "Appearance settings updated." if ok else "Could not update appearance settings.",
            "success" if ok else "error",
        )
        return redirect(url_for("shop_settings_appearance", shop_id=shop["id"]))
    return render_template("shop_settings_appearance.html", shop=shop, **_shop_settings_template_extra(shop))


@app.route("/shops/<int:shop_id>/shop-settings/shop", methods=["GET", "POST"])
def shop_settings_shop(shop_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    if request.method == "POST":
        use_custom = (request.form.get("shop_company_custom") or "").strip() == "1"
        payload = _shop_company_settings_payload_from_form(shop)
        company_json = json.dumps(payload, separators=(",", ":")) if payload else None
        logo_path = shop.get("shop_logo")
        update_logo = False
        if use_custom:
            remove_logo = (request.form.get("remove_shop_logo") or "").strip() == "1"
            logo_file = request.files.get("shop_logo")
            if remove_logo:
                logo_path = ""
                update_logo = True
            elif logo_file and getattr(logo_file, "filename", ""):
                saved = _save_branding_upload(logo_file)
                if saved is None:
                    flash("Shop logo must be PNG, JPG, GIF, WebP, ICO, or SVG.", "error")
                    return redirect(url_for("shop_settings_shop", shop_id=shop["id"]))
                logo_path = saved
                update_logo = True
        elif not payload:
            logo_path = ""
            update_logo = True
        try:
            from database import update_shop_company_settings

            ok = update_shop_company_settings(
                shop["id"],
                company_settings_json=company_json,
                shop_logo=logo_path,
                update_shop_logo=update_logo,
            )
        except Exception:
            ok = False
        flash(
            "Shop settings updated." if ok else "Could not update shop settings.",
            "success" if ok else "error",
        )
        return redirect(url_for("shop_settings_shop", shop_id=shop["id"]))
    return render_template("shop_settings_shop.html", shop=shop, **_shop_settings_template_extra(shop))


@app.route("/shops/<int:shop_id>/shop-settings/pos-printing", methods=["GET", "POST"])
def shop_settings_pos_printing(shop_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    if request.method == "POST":
        use_custom_printing = (request.form.get("shop_printing_custom") or "").strip() == "1"
        # Full replace from form (unchecked checkboxes are absent → false), same as IT system settings.
        printing_json = (
            json.dumps(_printing_settings_from_form(), separators=(",", ":")) if use_custom_printing else None
        )
        try:
            from database import update_shop_settings

            ok = update_shop_settings(
                shop["id"],
                default_theme=shop["default_theme"],
                font_family=shop["font_family"],
                primary_color=shop["primary_color"],
                accent_color=shop["accent_color"],
                printing_settings_json=printing_json,
                receipt_settings_json=shop.get("receipt_settings_json"),
                appearance_settings_json=shop.get("appearance_settings_json"),
                company_settings_json=shop.get("company_settings_json"),
            )
        except Exception:
            ok = False
        flash("Point of sale settings updated." if ok else "Could not update point of sale settings.", "success" if ok else "error")
        return redirect(url_for("shop_settings_pos_printing", shop_id=shop["id"]))
    return render_template("shop_settings_pos_printing.html", shop=shop, **_shop_settings_template_extra(shop))


@app.route("/shops/<int:shop_id>/shop-settings/receipt", methods=["GET", "POST"])
def shop_settings_receipt(shop_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    if request.method == "POST":
        use_custom_receipt = (request.form.get("shop_receipt_custom") or "").strip() == "1"
        receipt_json = json.dumps(_receipt_settings_from_form(), separators=(",", ":")) if use_custom_receipt else None
        try:
            from database import update_shop_settings

            ok = update_shop_settings(
                shop["id"],
                default_theme=shop["default_theme"],
                font_family=shop["font_family"],
                primary_color=shop["primary_color"],
                accent_color=shop["accent_color"],
                printing_settings_json=shop.get("printing_settings_json"),
                receipt_settings_json=receipt_json,
                appearance_settings_json=shop.get("appearance_settings_json"),
                company_settings_json=shop.get("company_settings_json"),
            )
        except Exception:
            ok = False
        flash("Receipt settings updated." if ok else "Could not update receipt settings.", "success" if ok else "error")
        return redirect(url_for("shop_settings_receipt", shop_id=shop["id"]))
    return render_template("shop_settings_receipt.html", shop=shop, **_shop_settings_template_extra(shop))


# Legacy non-prefixed shop links -> canonical shop-* links.
@app.route("/shops/<int:shop_id>/item-management")
def shop_item_management_legacy(shop_id: int):
    return redirect(url_for("shop_item_management", shop_id=shop_id), code=301)


@app.route("/shops/<int:shop_id>/analytics")
def shop_analytics_legacy(shop_id: int):
    return redirect(url_for("shop_analytics", shop_id=shop_id), code=301)


@app.route("/shops/<int:shop_id>/hr-management")
def shop_hr_management_legacy(shop_id: int):
    return redirect(url_for("shop_hr_management", shop_id=shop_id), code=301)


@app.route("/shops/<int:shop_id>/audits")
def shop_audits_legacy(shop_id: int):
    return redirect(url_for("shop_audits", shop_id=shop_id), code=301)


@app.route("/shops/<int:shop_id>/settings")
def shop_settings_legacy(shop_id: int):
    return redirect(url_for("shop_settings", shop_id=shop_id), code=301)


@app.route("/it_support/hr-management")
@login_required
def it_support_hr_management():
    _it_support_only()
    try:
        from database import get_hr_employee_shop_link_mode, list_employees, list_shops

        employees = list_employees(limit=2000)
        shops = list_shops(limit=500)
        hr_employee_shop_link_mode = get_hr_employee_shop_link_mode()
    except Exception:
        employees, shops = [], []
        hr_employee_shop_link_mode = "single"
    return render_template(
        "it_support_hr_management.html",
        employees=employees,
        shops=shops,
        hr_employee_shop_link_mode=hr_employee_shop_link_mode,
    )


def _collect_hr_analytics_data(lookback_days: int) -> dict:
    """Shared data fetch for the HR analytics page and its live polling endpoint."""
    try:
        from database import (
            get_hr_activity_daily_totals,
            get_hr_activity_summary_for_employees,
            list_employees,
        )

        employees = list_employees(limit=2000) or []
        emp_ids = tuple(int(e.get("id")) for e in employees if e.get("id") is not None)
        summary_map = get_hr_activity_summary_for_employees(
            emp_ids, lookback_days=lookback_days
        )
        daily_totals = get_hr_activity_daily_totals(
            lookback_days=min(lookback_days, 90)
        )
    except Exception:
        employees = []
        summary_map = {}
        daily_totals = []

    decorated = []
    total_sessions = 0
    total_hours = 0.0
    total_changes = 0
    active_today = 0
    today = date.today()
    for emp in employees:
        try:
            eid = int(emp.get("id"))
        except (TypeError, ValueError):
            continue
        s = summary_map.get(eid) or {}
        row = dict(emp)
        row["activity_summary"] = s
        last_login = s.get("last_login_at")
        if today and last_login:
            try:
                if last_login.date() == today:
                    active_today += 1
            except Exception:
                pass
        total_sessions += int(s.get("session_count") or 0)
        try:
            total_hours += float(s.get("total_seconds") or 0) / 3600.0
        except (TypeError, ValueError):
            pass
        total_changes += int(s.get("change_count") or 0)
        decorated.append(row)

    totals = {
        "employees": len(decorated),
        "sessions": total_sessions,
        "hours": round(total_hours, 1),
        "changes": total_changes,
        "active_today": active_today,
    }

    chart_payload = _build_hr_analytics_chart_payload(
        decorated, daily_totals, lookback_days=lookback_days, totals=totals
    )
    return {
        "employees": decorated,
        "totals": totals,
        "chart_payload": chart_payload,
        "lookback_days": lookback_days,
    }


@app.route("/it_support/hr-analytics")
@login_required
def it_support_hr_analytics():
    _it_support_only()
    try:
        lookback_days = int(request.args.get("lookback", 90) or 90)
    except (TypeError, ValueError):
        lookback_days = 90
    lookback_days = max(1, min(3650, lookback_days))
    data = _collect_hr_analytics_data(lookback_days)
    return render_template(
        "it_support_hr_analytics.html",
        employees=data["employees"],
        analytics_totals=data["totals"],
        lookback_days=data["lookback_days"],
        chart_payload=data["chart_payload"],
    )


@app.route("/it_support/hr-analytics/data.json")
@login_required
def it_support_hr_analytics_data():
    """Live-polling payload used to refresh the HR analytics page in-place."""
    _it_support_only()
    try:
        lookback_days = int(request.args.get("lookback", 90) or 90)
    except (TypeError, ValueError):
        lookback_days = 90
    lookback_days = max(1, min(3650, lookback_days))
    data = _collect_hr_analytics_data(lookback_days)

    def _iso(value):
        try:
            return value.isoformat(sep=" ", timespec="seconds") if value else None
        except Exception:
            return str(value) if value else None

    employees_out = []
    for emp in data["employees"]:
        s = emp.get("activity_summary") or {}
        try:
            eid = int(emp.get("id"))
        except (TypeError, ValueError):
            continue
        employees_out.append(
            {
                "id": eid,
                "full_name": emp.get("full_name") or "",
                "employee_code": emp.get("employee_code") or "",
                "email": emp.get("email") or "",
                "role": emp.get("role") or "",
                "status": emp.get("status") or "",
                "shop_name": emp.get("shop_name") or "",
                "session_count": int(s.get("session_count") or 0),
                "total_seconds": int(s.get("total_seconds") or 0),
                "total_hours_label": s.get("total_hours_label") or "0h 0m",
                "change_count": int(s.get("change_count") or 0),
                "login_count": int(s.get("login_count") or 0),
                "logout_count": int(s.get("logout_count") or 0),
                "last_login_at": _iso(s.get("last_login_at")),
                "last_logout_at": _iso(s.get("last_logout_at")),
            }
        )

    return jsonify(
        {
            "ok": True,
            "server_time": _iso(datetime.now()),
            "lookback_days": data["lookback_days"],
            "totals": data["totals"],
            "employees": employees_out,
            "chart_payload": data["chart_payload"],
        }
    )


def _build_hr_analytics_chart_payload(
    employees: list,
    daily_totals: list,
    *,
    lookback_days: int,
    totals: dict,
) -> dict:
    """Shape the data the HR analytics charts consume in the browser."""
    enriched = []
    for emp in employees or []:
        s = (emp or {}).get("activity_summary") or {}
        try:
            eid = int(emp.get("id"))
        except (TypeError, ValueError):
            continue
        enriched.append(
            {
                "id": eid,
                "name": emp.get("full_name") or "",
                "code": emp.get("employee_code") or "",
                "role": (emp.get("role") or "").lower(),
                "shop_name": emp.get("shop_name") or "",
                "status": emp.get("status") or "",
                "total_hours": round(float(s.get("total_seconds") or 0) / 3600.0, 2),
                "total_hours_label": s.get("total_hours_label") or "0h 0m",
                "session_count": int(s.get("session_count") or 0),
                "login_count": int(s.get("login_count") or 0),
                "logout_count": int(s.get("logout_count") or 0),
                "change_count": int(s.get("change_count") or 0),
                "kind_counts": s.get("kind_counts") or {},
            }
        )

    by_hours_desc = sorted(enriched, key=lambda x: x["total_hours"], reverse=True)
    by_sessions_desc = sorted(enriched, key=lambda x: x["session_count"], reverse=True)
    by_changes_desc = sorted(enriched, key=lambda x: x["change_count"], reverse=True)

    def _take(rows, n=15):
        return [r for r in rows[:n] if (r["total_hours"] or r["session_count"] or r["change_count"])]

    top_hours = _take(by_hours_desc, 15)
    top_sessions = _take(by_sessions_desc, 15)
    top_changes = _take(by_changes_desc, 15)

    # Action-mix dataset: take the top 12 most active employees overall, then expose
    # per-kind counts for stacked bars.
    def _activity_total(row):
        kc = row.get("kind_counts") or {}
        return sum(int(v or 0) for v in kc.values())

    by_activity = sorted(enriched, key=_activity_total, reverse=True)
    mix_rows = [r for r in by_activity[:12] if _activity_total(r) > 0]

    daily_out = []
    for d in daily_totals or []:
        day = d.get("date")
        if hasattr(day, "isoformat"):
            day_label = day.isoformat()
        else:
            day_label = str(day) if day else ""
        daily_out.append(
            {
                "date": day_label,
                "login": int(d.get("login") or 0),
                "logout": int(d.get("logout") or 0),
                "register": int(d.get("register") or 0),
                "edit": int(d.get("edit") or 0),
                "update": int(d.get("update") or 0),
                "delete": int(d.get("delete") or 0),
                "other": int(d.get("other") or 0),
                "total": int(d.get("total") or 0),
            }
        )

    # Aggregate role-level totals so we can compare by role too.
    role_agg: dict[str, dict] = {}
    for r in enriched:
        role = r["role"] or "employee"
        bucket = role_agg.setdefault(
            role,
            {
                "role": role,
                "employees": 0,
                "total_hours": 0.0,
                "session_count": 0,
                "change_count": 0,
            },
        )
        bucket["employees"] += 1
        bucket["total_hours"] += float(r["total_hours"] or 0.0)
        bucket["session_count"] += int(r["session_count"] or 0)
        bucket["change_count"] += int(r["change_count"] or 0)
    role_rows = sorted(
        (
            {
                "role": v["role"],
                "employees": v["employees"],
                "total_hours": round(v["total_hours"], 2),
                "session_count": v["session_count"],
                "change_count": v["change_count"],
            }
            for v in role_agg.values()
        ),
        key=lambda x: x["total_hours"],
        reverse=True,
    )

    return {
        "lookback_days": int(lookback_days),
        "totals": totals or {},
        "top_hours": top_hours,
        "top_sessions": top_sessions,
        "top_changes": top_changes,
        "activity_mix": mix_rows,
        "daily": daily_out,
        "roles": role_rows,
    }


@app.route("/it_support/hr-analytics/employee/<int:emp_id>.json")
@login_required
def it_support_hr_analytics_employee_detail(emp_id: int):
    """JSON detail used by the HR analytics modal."""
    _it_support_only()
    try:
        lookback_days = int(request.args.get("lookback", 90) or 90)
    except (TypeError, ValueError):
        lookback_days = 90
    lookback_days = max(1, min(3650, lookback_days))
    try:
        recent_limit = int(request.args.get("recent_limit", 80) or 80)
    except (TypeError, ValueError):
        recent_limit = 80
    recent_limit = max(1, min(500, recent_limit))
    try:
        from database import (
            get_employee_by_id,
            get_employee_session_analytics,
        )

        emp = get_employee_by_id(emp_id) or {}
        analytics = get_employee_session_analytics(
            emp_id, lookback_days=lookback_days, recent_limit=recent_limit
        )
    except Exception:
        emp = {}
        analytics = {}

    def _iso(value):
        try:
            return value.isoformat(sep=" ", timespec="seconds") if value else None
        except Exception:
            return str(value) if value else None

    sessions_out = []
    for s in analytics.get("sessions") or []:
        sessions_out.append(
            {
                "login_at": _iso(s.get("login_at")),
                "logout_at": _iso(s.get("logout_at")),
                "duration_seconds": int(s.get("duration_seconds") or 0),
                "duration_label": s.get("duration_label") or "0m",
                "ip_address": s.get("ip_address"),
                "open": bool(s.get("open")),
            }
        )

    activities_out = []
    for a in analytics.get("recent_activities") or []:
        activities_out.append(
            {
                "id": int(a.get("id") or 0),
                "action_kind": a.get("action_kind"),
                "target_type": a.get("target_type"),
                "target_id": a.get("target_id"),
                "description": a.get("description"),
                "ip_address": a.get("ip_address"),
                "created_at": _iso(a.get("created_at")),
            }
        )

    payload = {
        "ok": bool(emp),
        "employee": {
            "id": emp.get("id"),
            "full_name": emp.get("full_name"),
            "email": emp.get("email"),
            "phone": emp.get("phone"),
            "employee_code": emp.get("employee_code"),
            "role": emp.get("role"),
            "status": emp.get("status"),
            "shop_id": emp.get("shop_id"),
            "shop_name": emp.get("shop_name"),
        },
        "analytics": {
            "session_count": int(analytics.get("session_count") or 0),
            "open_session_count": int(analytics.get("open_session_count") or 0),
            "total_seconds": int(analytics.get("total_seconds") or 0),
            "total_hours_label": analytics.get("total_hours_label") or "0h 0m",
            "total_hours_decimal": float(analytics.get("total_hours_decimal") or 0.0),
            "avg_session_seconds": int(analytics.get("avg_session_seconds") or 0),
            "avg_session_label": analytics.get("avg_session_label") or "0m",
            "longest_session_seconds": int(analytics.get("longest_session_seconds") or 0),
            "longest_session_label": analytics.get("longest_session_label") or "0m",
            "last_login_at": _iso(analytics.get("last_login_at")),
            "last_logout_at": _iso(analytics.get("last_logout_at")),
            "first_seen_at": _iso(analytics.get("first_seen_at")),
            "kind_counts": analytics.get("kind_counts") or {},
            "lookback_days": int(analytics.get("lookback_days") or lookback_days),
        },
        "sessions": sessions_out,
        "recent_activities": activities_out,
    }
    return jsonify(payload)


HR_AUTH_ROLE_ORDER = (
    ("super_admin", "Super admin"),
    ("it_support", "IT support"),
    ("company_manager", "Company manager"),
    ("admin", "Admin"),
    ("manager", "Manager"),
    ("sales", "Sales"),
    ("finance", "Finance"),
    ("employee", "Employee"),
    ("rider", "Rider"),
)

# Template-only: HR employee module authorization matrix (checkbox names / UI). Not persisted or enforced.
MODULE_PERMISSION_COLUMNS: tuple[tuple[str, str], ...] = (
    ("view", "View"),
    ("edit", "Edit"),
    ("delete", "Delete"),
    ("suspend", "Suspend"),
    ("approve", "Approve"),
    ("generate", "Generate"),
)

# Item management card only — matches IT item management workflows (UI draft).
MODULE_ITEM_MANAGEMENT_PERMISSION_COLUMNS: tuple[tuple[str, str], ...] = (
    ("view_items", "View items"),
    ("register_items", "Register items"),
    ("edit_items", "Edit items"),
    ("suspend_items", "Suspend items"),
    ("delete_items", "Delete items"),
    ("approve_items", "Approve items"),
    ("view_item_analytics", "View analytics of the items"),
)

# Shop / POS surfaces — same checklist on every HR auth playbook role (preview UI only until enforced).
# Labels stress: options only matter for the shop the user has open in session, and shop-level toggles (credit, discounts, buys, etc.) must allow them.
MODULE_SHOP_PERMISSION_COLUMNS: tuple[tuple[str, str], ...] = (
    ("access_shop", "Access shop (while in branch session)"),
    ("make_sale", "Make sale (at shop in session)"),
    ("make_credit_sale", "Make credit sale (only if enabled for shop in session)"),
    ("give_discount", "Give discount (only if enabled for shop in session)"),
    ("buy_items", "Buy items (only if enabled for shop in session)"),
)

_SHOP_MODULE_ACTIVITIES_GENERAL: tuple[str, ...] = (
    "Everything below applies only while the user is signed into a branch session—the shop context is whatever they have open, not global.",
    "Credit sales, discounts, stock buys, and similar actions appear only when that shop's settings enable them on top of the user's permissions.",
)


def _enrich_modules_with_auth_slug(modules: list) -> list:
    import re

    enriched: list = []
    for i, m in enumerate(modules or [], start=1):
        row = dict(m)
        title = str(row.get("title") or "module")
        slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-") or "module"
        row["auth_slug"] = f"{i}-{slug}"
        enriched.append(row)
    return enriched


_PORTAL_MODULES_COMPANY_IT: tuple[dict, ...] = (
    {
        "title": "Item management",
        "subtitle": "Company-wide product catalog",
        "permission_columns": MODULE_ITEM_MANAGEMENT_PERMISSION_COLUMNS,
        "activities": (
            "Register new items and attach images, categories, and pricing fields seen across branches.",
            "Edit item listings, labels, and POS-facing details anytime from the master catalog grid.",
            "Suspend items to block new sales, then activate them again when stock or compliance allows.",
            "Operate the company stock update and kitchen portion update master toggles that branches inherit.",
            "Deactivate or reactivate branch autonomy for stock and kitchen toggles depending on inventory mode.",
            "Open company stock management for stock-in/out, transfers, and reconciliation views.",
            "Use item analytics and item audit to trace who changed what.",
        ),
    },
    {
        "title": "Store management",
        "subtitle": "Branches and shop access",
        "activities": (
            "Register shops and keep branch names, codes, and device login details current.",
            "Review which menus, stock buckets, or POS sessions belong to each store.",
            "Coordinate rollout of catalogue or pricing updates per branch.",
        ),
    },
    {
        "title": "Shop module",
        "subtitle": "Scoped to whichever branch session is open—tenders and buys follow that shop's enabled settings",
        "permission_columns": MODULE_SHOP_PERMISSION_COLUMNS,
        "activities": _SHOP_MODULE_ACTIVITIES_GENERAL
        + (
            "IT and super admins verify branch POS behaviour matches policy across shops.",
        ),
    },
    {
        "title": "Kitchen management",
        "subtitle": "Portions & production",
        "activities": (
            "Use the kitchen portions dashboard as the launch point for recipes and deductions.",
            "Adjust portion mappings per branch so POS kitchen mode stays accurate.",
            "Open portion analytics to compare throughput and deductions across branches.",
            "Maintain kitchen layouts or planning views linked from the dashboard where configured.",
        ),
    },
    {
        "title": "HR management",
        "subtitle": "People, payroll, and access",
        "activities": (
            "Approve new employees, edit profiles, assign roles, and link branches (single or multi-shop mode).",
            "Suspend, reactivate, or remove accounts.",
            "Open HR authorization to review staff grouped by role with status and allocations.",
            "Configure HR settings for how employees tie to branches for POS authorization.",
            "Register salaries and advances from the salaries hub; oversee payroll summaries.",
        ),
    },
    {
        "title": "Analytics management",
        "subtitle": "Company performance",
        "activities": (
            "Open the main analytics workspace for revenue, items, periods, shops, employees, credits, customers, etc.",
            "Compare trends and export insight for leadership.",
            "Use company stock analytics and stock status summaries when diagnosing inventory posture.",
            "Combine with portion analytics where kitchen throughput matters.",
        ),
    },
    {
        "title": "Leads and quotations",
        "subtitle": "Pipeline & quoting",
        "activities": (
            "Capture inbound leads from marketing or storefront traffic.",
            "Prepare and track quotations until they convert (or lapse).",
            "Share statuses with managers so follow-up stays coordinated.",
        ),
    },
    {
        "title": "Website management",
        "subtitle": "Public storefront & content",
        "activities": (
            "Update headlines, visuals, contact blocks, or marketing copy shown on public pages.",
            "Keep brochureware aligned with promotions running in-branch.",
        ),
    },
    {
        "title": "Credit payments",
        "subtitle": "Accounts receivable (all branches)",
        "activities": (
            "Monitor credit sales and settlement progress across branches from the consolidated credit workspace.",
            "Drill into customer timelines when reconciliation is needed.",
        ),
    },
)


_PORTAL_MODULES_BRANCH_LEADERSHIP: tuple[dict, ...] = (
    {
        "title": "Item management",
        "subtitle": "At your assigned branch(es)",
        "permission_columns": MODULE_ITEM_MANAGEMENT_PERMISSION_COLUMNS,
        "activities": (
            "Coordinate with IT to register master items company-wide when new stock lines are approved.",
            "Use shop item management to edit branch listings, imagery, and how items appear on menus and POS.",
            "Suspend or re-activate branch-facing items when stock runs out or returns (subject to IT master rules).",
            "Manage stock updates and kitchen portion deductions at branch level when masters allow toggles.",
            "Turn branch display or fulfilment switches on/off so cashiers only see items you want live.",
            "Escalate catalogue-wide suspensions or pricing policy changes back to IT / super admin.",
        ),
    },
    {
        "title": "Store management",
        "subtitle": "Branch operations",
        "activities": (
            "Open the shop dashboard for your branch to review quick links and status.",
            "Ensure shop codes and device sign-in practices stay secure for cashiers.",
            "Coordinate with IT for new devices or shop setting changes.",
        ),
    },
    {
        "title": "Shop module",
        "subtitle": "Only for the shop open in your session; credit, discount, and buy options if that shop enables them",
        "permission_columns": MODULE_SHOP_PERMISSION_COLUMNS,
        "activities": _SHOP_MODULE_ACTIVITIES_GENERAL,
    },
    {
        "title": "Kitchen management",
        "subtitle": "Portions & production",
        "activities": (
            "Maintain kitchen portion counts and deduction rules for your branch.",
            "Review portion analytics for your location to catch waste or mis-posting early.",
            "Align production plans with what POS is selling in real time.",
        ),
    },
    {
        "title": "HR management",
        "subtitle": "Team on the floor",
        "activities": (
            "Use shop HR management for branch-level staff notes or visibility your deployment enables.",
            "Escalate hiring, suspensions, or payroll changes to IT / super admin as required.",
        ),
    },
    {
        "title": "Analytics management",
        "subtitle": "Branch insight",
        "activities": (
            "Open shop analytics for sales, stock, and customer behaviour tied to your branch.",
            "Compare current period performance with prior weeks to brief leadership.",
        ),
    },
    {
        "title": "Leads and quotations",
        "subtitle": "Sales follow-up",
        "activities": (
            "Work leads and quotations assigned to your branch or territory.",
            "Keep pipeline hygiene so finance and fulfilment see accurate expectations.",
        ),
    },
    {
        "title": "Website management",
        "subtitle": "Usually IT-owned",
        "activities": (
            "Most teams route public website edits through IT or marketing super users.",
            "When you have access, mirror in-branch promotions on the public site after approval.",
        ),
    },
)


_PORTAL_MODULES_FINANCE: tuple[dict, ...] = (
    {
        "title": "Finance & payroll visibility",
        "subtitle": "Numbers and compliance",
        "activities": (
            "Review payroll registers, advances, and loans when IT grants access to the salaries workspace.",
            "Track company credit exposure using credit payment summaries and customer ledgers.",
            "Pair analytics revenue views with cash / M-Pesa settlement reports from operations.",
        ),
    },
    {
        "title": "Analytics management",
        "subtitle": "Financial lenses",
        "activities": (
            "Use analytics filters focused on revenue, credit, and customer concentration.",
            "Export or snapshot figures for leadership packs.",
        ),
    },
    {
        "title": "Shop module",
        "subtitle": "When POS access applies—still scoped to the shop in session and that shop's enabled features",
        "permission_columns": MODULE_SHOP_PERMISSION_COLUMNS,
        "activities": _SHOP_MODULE_ACTIVITIES_GENERAL
        + (
            "Use read-only or limited POS access when finance validates settlements on the floor.",
        ),
    },
    {
        "title": "Leads and quotations",
        "subtitle": "Commercial pipeline",
        "activities": (
            "Monitor large quotes that affect margin or payment terms before approval.",
        ),
    },
)


_PORTAL_MODULES_SALES: tuple[dict, ...] = (
    {
        "title": "Leads and quotations",
        "subtitle": "Pipeline",
        "activities": (
            "Create and nurture leads; convert them to quotations and sales orders.",
            "Share updates with managers so stock and kitchen plans stay realistic.",
        ),
    },
    {
        "title": "Shop module",
        "subtitle": "Linked branch + active shop session; credit/discount/buy only if that shop allows",
        "permission_columns": MODULE_SHOP_PERMISSION_COLUMNS,
        "activities": _SHOP_MODULE_ACTIVITIES_GENERAL
        + (
            "Use POS and shop dashboards for day-to-day selling when your login is branch-scoped.",
            "Reference shop analytics for personal or team targets.",
        ),
    },
)


_PORTAL_MODULES_STAFF: tuple[dict, ...] = (
    {
        "title": "Shop module",
        "subtitle": "Cashier/session shop only—credit, discounts, and buys appear if that shop has them enabled",
        "permission_columns": MODULE_SHOP_PERMISSION_COLUMNS,
        "activities": _SHOP_MODULE_ACTIVITIES_GENERAL
        + (
            "Cashiers often use cash and M-Pesa tenders; rider flows may emphasize fulfilment when that shop enables it.",
        ),
    },
    {
        "title": "Branch operations",
        "subtitle": "Day-to-day",
        "activities": (
            "Sign in to the assigned shop session or POS with your six-digit employee code.",
            "Process sales, stock movements, or kitchen tasks your manager enables.",
            "Use notifications and profile settings to keep contact details current.",
        ),
    },
)


def _portal_module_playbook(*, role_key: str, is_pending: bool) -> tuple[list, str]:
    """Return (modules_list, scope_note) describing typical portal areas for a role."""
    rk = (role_key or "employee").strip().lower()
    if rk in COMPANY_PORTAL_ROLES:
        modules = [dict(m) for m in _PORTAL_MODULES_COMPANY_IT]
        note = (
            "Full company management portal: super admin, IT support, and company manager accounts use the same "
            "employee dashboard shortcuts (items, stores, kitchen, HR, analytics, leads, website, credit)."
        )
    elif rk == "admin":
        modules = [dict(m) for m in _PORTAL_MODULES_BRANCH_LEADERSHIP]
        note = (
            "Admin playbook emphasises branch leadership. Deep company-catalog changes still flow through IT "
            "unless your deployment grants additional routes."
        )
    elif rk == "manager":
        modules = [dict(m) for m in _PORTAL_MODULES_BRANCH_LEADERSHIP]
        note = (
            "Manager playbook mirrors operational leadership: focus on execution at assigned branches, with "
            "escalation paths to IT for master data."
        )
    elif rk == "finance":
        modules = [dict(m) for m in _PORTAL_MODULES_FINANCE]
        note = "Finance roles centre on payroll visibility, credit exposure, and analytics cuts your session can open."
    elif rk == "sales":
        modules = [dict(m) for m in _PORTAL_MODULES_SALES]
        note = "Sales roles stress leads, quotations, and branch selling tools when a shop is linked to the account."
    elif rk in ("employee", "rider"):
        modules = [dict(m) for m in _PORTAL_MODULES_STAFF]
        note = (
            "Standard staff and rider accounts typically work from branch POS and limited portal pages; "
            "rider-specific dispatch screens appear here when the product enables them."
        )
    else:
        modules = [dict(m) for m in _PORTAL_MODULES_STAFF]
        note = "Generic staff playbook — confirm with IT which routes are enabled for this custom role label."

    modules = _enrich_modules_with_auth_slug(modules)
    if is_pending:
        note = "This account is still pending approval. The modules below preview what applies once activated. " + note
    return modules, note


@app.route("/it-support/hr-authorization")
@app.route("/it_support/hr-authorization")
@login_required
def it_support_hr_authorization():
    _it_support_only()
    from collections import defaultdict, OrderedDict

    ordered_role_keys = [k for k, _ in HR_AUTH_ROLE_ORDER]
    role_title = dict(HR_AUTH_ROLE_ORDER)
    try:
        from database import get_hr_employee_shop_link_mode, list_employees

        employees = list_employees(limit=3500)
        hr_employee_shop_link_mode = get_hr_employee_shop_link_mode()
    except Exception:
        employees = []
        hr_employee_shop_link_mode = "single"

    buckets = OrderedDict((rk, []) for rk in ordered_role_keys)
    other = defaultdict(list)
    for row in employees or []:
        rk = str(row.get("role") or "employee").strip().lower()
        if rk in buckets:
            buckets[rk].append(row)
        else:
            other[rk].append(row)

    role_sections = []
    for rk in ordered_role_keys:
        lst = buckets.get(rk) or []
        if lst:
            role_sections.append({"key": rk, "title": role_title[rk], "employees": lst})
    for rk in sorted(other.keys()):
        lst = other[rk]
        if not lst:
            continue
        label = rk.replace("_", " ").strip().title() or "Other role"
        role_sections.append({"key": rk, "title": label, "employees": lst})

    return render_template(
        "it_support_hr_authorization.html",
        role_sections=role_sections,
        hr_employee_shop_link_mode=hr_employee_shop_link_mode,
    )


def _hr_employee_allocated_label(
    *,
    emp: dict,
    emp_id: int,
    hr_mode: str,
    get_shop_by_id,
    get_employee_accessible_shop_ids,
) -> Optional[str]:
    """Human-readable branch list matching HR authorization semantics."""
    if hr_mode == "multi":
        parts: list = []
        for sid in get_employee_accessible_shop_ids(int(emp_id)):
            shop = get_shop_by_id(int(sid))
            if not shop:
                continue
            name = (shop.get("shop_name") or "").strip()
            code = (shop.get("shop_code") or "").strip()
            if name and code:
                parts.append(f"{name} ({code})")
            elif name:
                parts.append(name)
            elif code:
                parts.append(code)
            else:
                parts.append(f"Branch #{sid}")
        if parts:
            return ", ".join(parts)
    sid = emp.get("shop_id")
    if sid is None:
        return None
    try:
        sh = get_shop_by_id(int(sid))
    except (TypeError, ValueError):
        sh = None
    if not sh:
        return None
    name = (sh.get("shop_name") or "").strip()
    code = (sh.get("shop_code") or "").strip()
    if name and code:
        return f"{name} ({code})"
    return name or code or None


@app.route("/it_support/employees/<int:emp_id>")
@login_required
def it_support_hr_employee_detail(emp_id: int):
    _it_support_only()
    from database import (
        get_employee_accessible_shop_ids,
        get_employee_by_id,
        get_hr_employee_shop_link_mode,
        get_shop_by_id,
    )

    try:
        emp = get_employee_by_id(int(emp_id))
    except Exception:
        emp = None
    if not emp:
        flash("Employee not found.", "error")
        return redirect(url_for("it_support_hr_authorization"))

    rank = dict(HR_AUTH_ROLE_ORDER)
    role_key = str(emp.get("role") or "employee").strip().lower()
    role_label = rank.get(role_key) or role_key.replace("_", " ").strip().title() or "Other role"

    try:
        hr_employee_shop_link_mode = get_hr_employee_shop_link_mode()
    except Exception:
        hr_employee_shop_link_mode = "single"

    try:
        allocated_display = _hr_employee_allocated_label(
            emp=emp,
            emp_id=int(emp_id),
            hr_mode=str(hr_employee_shop_link_mode or "single"),
            get_shop_by_id=get_shop_by_id,
            get_employee_accessible_shop_ids=get_employee_accessible_shop_ids,
        )
    except Exception:
        allocated_display = None

    is_pending = (emp.get("status") or "") == "pending_approval"

    module_playbook, playbook_note = _portal_module_playbook(
        role_key=role_key,
        is_pending=is_pending,
    )

    return render_template(
        "it_support_hr_employee_detail.html",
        employee=emp,
        role_label=role_label,
        allocated_display=allocated_display,
        hr_employee_shop_link_mode=hr_employee_shop_link_mode,
        is_pending=is_pending,
        module_playbook=module_playbook,
        playbook_note=playbook_note,
        module_permission_columns=MODULE_PERMISSION_COLUMNS,
    )


@app.route("/it_support/hr-settings", methods=["GET", "POST"])
@login_required
def it_support_hr_settings():
    _it_support_only()
    from database import get_hr_employee_shop_link_mode, set_hr_employee_shop_link_mode

    if request.method == "POST":
        mode = (request.form.get("employee_shop_link_mode") or "").strip().lower()
        ok = set_hr_employee_shop_link_mode(mode)
        flash(
            "HR branch link setting saved." if ok else "Could not save HR setting. Try again.",
            "success" if ok else "error",
        )
        return redirect(url_for("it_support_hr_settings"))

    mode = get_hr_employee_shop_link_mode()
    return render_template("it_support_hr_settings.html", hr_employee_shop_link_mode=mode)


@app.route("/it_support/company-report")
@login_required
def it_support_company_report():
    """Company-wide report: revenue, expenditure, and per-item stock + sales."""
    _it_support_only()
    analytics_filter = _build_analytics_filter()
    analytics_scope = _analytics_scope_from_request()
    shop_filter_id = _company_report_shop_filter_from_request()
    shops: list = []
    shop_filter_name = ""
    try:
        from database import list_shops

        shops = list_shops(limit=500) or []
        if shop_filter_id:
            shop_filter_name = next(
                (
                    (s.get("shop_name") or "").strip()
                    for s in shops
                    if int(s.get("id") or 0) == int(shop_filter_id)
                ),
                "",
            )
    except Exception:
        shops = []
    report_data = {
        "total_revenue": 0.0,
        "sale_revenue": 0.0,
        "credit_revenue": 0.0,
        "paid_credit": 0.0,
        "unpaid_credit": 0.0,
        "cash_revenue": 0.0,
        "mpesa_revenue": 0.0,
        "collected_revenue": 0.0,
        "total_expenditure": 0.0,
        "paid_expenditure": 0.0,
        "balance_expenditure": 0.0,
        "net_profit": 0.0,
        "accrual_cogs": 0.0,
        "accrual_cogs_sale": 0.0,
        "accrual_cogs_credit": 0.0,
        "accrual_cogs_stock_out": 0.0,
        "accrual_gross_profit": 0.0,
        "accrual_net_profit": 0.0,
        "accrual_net_profit_collected": 0.0,
        "accrual_operating_expenses": 0.0,
        "items": [],
        "expenditure_rows": [],
    }
    try:
        from database import get_company_report

        report_data = get_company_report(
            analytics_filter=analytics_filter,
            analytics_scope=analytics_scope,
            shop_id=shop_filter_id,
        )
    except Exception:
        logger.exception("it_support_company_report failed")
    report_should_print = (request.args.get("print") or "").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    share_qs = request.query_string.decode("utf-8") if request.query_string else ""
    share_path = url_for("it_support_company_report")
    share_url = f"{_public_share_base_url()}{share_path}"
    if share_qs:
        share_url = f"{share_url}?{share_qs}"
    company_name = ""
    try:
        from database import get_site_settings

        company_name = (get_site_settings(["company_name"]).get("company_name") or "").strip()
    except Exception:
        company_name = ""
    report_generated_at = datetime.now().strftime("%d %b %Y %H:%M")
    report_json_url = url_for("it_support_company_report_json")
    if share_qs:
        report_json_url = f"{report_json_url}?{share_qs}"
    return render_template(
        "it_support_company_report.html",
        analytics_filter=analytics_filter,
        analytics_scope=analytics_scope,
        report_data=report_data,
        item_rows=report_data.get("items") or [],
        items_sold_rows=report_data.get("items_sold") or [],
        expenditure_rows=report_data.get("expenditure_rows") or [],
        report_share_url=share_url,
        report_json_url=report_json_url,
        report_live_enabled=_shop_report_live_enabled(analytics_filter),
        report_company_name=company_name,
        report_should_print=report_should_print,
        report_generated_at=report_generated_at,
        report_period_label=_shop_report_period_label(analytics_filter),
        report_is_single_day=(analytics_filter.get("mode") or "single_day") == "single_day",
        report_today_iso=date.today().isoformat(),
        shops=shops,
        shop_filter_id=shop_filter_id or 0,
        shop_filter_name=shop_filter_name,
        pos_allow_credit_sale=report_data.get("pos_allow_credit_sale", True),
        pos_inventory_mode=report_data.get("pos_inventory_mode", "shop"),
    )


@app.route("/it_support/company-report.json")
@login_required
def it_support_company_report_json():
    """JSON snapshot of company period report (live refresh on today's view)."""
    _it_support_only()
    analytics_filter = _build_analytics_filter()
    analytics_scope = _analytics_scope_from_request()
    shop_filter_id = _company_report_shop_filter_from_request()
    report_data = {
        "total_revenue": 0.0,
        "sale_revenue": 0.0,
        "credit_revenue": 0.0,
        "paid_credit": 0.0,
        "unpaid_credit": 0.0,
        "cash_revenue": 0.0,
        "mpesa_revenue": 0.0,
        "collected_revenue": 0.0,
        "total_expenditure": 0.0,
        "paid_expenditure": 0.0,
        "balance_expenditure": 0.0,
        "net_profit": 0.0,
        "accrual_cogs": 0.0,
        "accrual_cogs_sale": 0.0,
        "accrual_cogs_credit": 0.0,
        "accrual_cogs_stock_out": 0.0,
        "accrual_gross_profit": 0.0,
        "accrual_net_profit": 0.0,
        "accrual_net_profit_collected": 0.0,
        "accrual_operating_expenses": 0.0,
        "items": [],
        "items_sold": [],
        "items_sold_sale": [],
        "items_sold_credit": [],
        "expenditure_rows": [],
        "till_summary": {},
    }
    try:
        from database import get_company_report

        report_data = get_company_report(
            analytics_filter=analytics_filter,
            analytics_scope=analytics_scope,
            shop_id=shop_filter_id,
        )
    except Exception:
        logger.exception("it_support_company_report_json failed")
    return jsonify(
        {
            "ok": True,
            "report": report_data,
            "generated_at": datetime.now().strftime("%d %b %Y %H:%M"),
            "live_enabled": _shop_report_live_enabled(analytics_filter),
        }
    )


@app.route("/it_support/company-report.pdf")
@login_required
def it_support_company_report_pdf():
    """Download company period report as PDF (same filters as HTML page)."""
    _it_support_only()
    analytics_filter = _build_analytics_filter()
    analytics_scope = _analytics_scope_from_request()
    shop_filter_id = _company_report_shop_filter_from_request()
    report_data = {
        "total_revenue": 0.0,
        "sale_revenue": 0.0,
        "credit_revenue": 0.0,
        "cash_revenue": 0.0,
        "mpesa_revenue": 0.0,
        "total_expenditure": 0.0,
        "net_profit": 0.0,
        "items": [],
    }
    try:
        from database import get_company_report

        report_data = get_company_report(
            analytics_filter=analytics_filter,
            analytics_scope=analytics_scope,
            shop_id=shop_filter_id,
        )
    except Exception:
        logger.exception("it_support_company_report_pdf failed")
    company_name = ""
    try:
        from database import get_site_settings

        company_name = (get_site_settings(["company_name"]).get("company_name") or "").strip()
    except Exception:
        company_name = ""
    generated_at = datetime.now().strftime("%d %b %Y %H:%M")
    primary_color = ""
    try:
        from database import get_site_settings

        primary_color = (get_site_settings(["primary_color"]).get("primary_color") or "").strip()
    except Exception:
        primary_color = ""
    scope_label = (
        "Confirmed receipts only"
        if (analytics_scope or "general") == "actual"
        else "All receipt marks"
    )
    try:
        from report_pdf import build_period_report_pdf

        pdf_bytes = build_period_report_pdf(
            title="Company report",
            entity_name=company_name or "Company",
            period_label=(analytics_filter or {}).get("range_label") or "Today",
            scope_label=scope_label,
            report_data=report_data,
            generated_at=generated_at,
            primary_color=primary_color or None,
        )
    except Exception:
        logger.exception("it_support_company_report_pdf build failed")
        abort(500, description="Could not generate PDF for this report.")
    filename = secure_filename(
        f"{(company_name or 'company').replace(' ', '-').lower()}-report.pdf"
    )
    return Response(
        pdf_bytes,
        mimetype="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.route("/it_support/accounts")
@login_required
def it_support_accounts():
    """Accounts hub: payroll, advances, and loans."""
    _it_support_only()
    return render_template("it_support_accounts.html")


@app.route("/it_support/accounts/incomes")
@login_required
def it_support_accounts_incomes():
    """Company incomes: cash and M-Pesa from POS sales and credit settlements."""
    _it_support_only()
    analytics_filter = _build_analytics_filter()
    shop_filter_arg = request.args.get("shop_id", type=int)
    shop_filter_id = shop_filter_arg if shop_filter_arg and shop_filter_arg > 0 else None
    payment_method_filter = (request.args.get("payment_method") or "").strip().lower()
    if payment_method_filter not in ("cash", "mpesa"):
        payment_method_filter = ""
    income_totals = {"rows": [], "total_cash": 0.0, "total_mpesa": 0.0, "total_income": 0.0, "count": 0}
    shops = []
    try:
        from database import list_all_shops_income_payments, list_shops

        shops = list_shops(limit=500) or []
        income_totals = list_all_shops_income_payments(
            analytics_filter,
            shop_id=shop_filter_id,
            payment_method=payment_method_filter or None,
            limit=8000,
        )
    except Exception:
        logger.exception("it_support_accounts_incomes list failed")
        try:
            from database import list_shops

            shops = list_shops(limit=500) or []
        except Exception:
            shops = []
    return render_template(
        "it_support_accounts_incomes.html",
        analytics_filter=analytics_filter,
        income_rows=income_totals.get("rows") or [],
        income_totals=income_totals,
        shops=shops,
        shop_filter_id=shop_filter_id or 0,
        payment_method_filter=payment_method_filter,
    )


@app.route("/it_support/accounts/expenses")
@login_required
def it_support_accounts_expenses():
    """Company expenses: stock purchases (items bought) with amounts paid."""
    _it_support_only()
    from database import _income_row_created_iso, list_company_supplier_stock_ins, list_shops

    analytics_filter = _build_analytics_filter()
    shop_filter_arg = request.args.get("shop_id", type=int)
    shop_filter_id = shop_filter_arg if shop_filter_arg and shop_filter_arg > 0 else None
    supplier_q = (request.args.get("supplier_q") or "").strip()
    expense_rows: list = []
    expense_totals = {
        "total_cost": 0.0,
        "amount_paid": 0.0,
        "balance": 0.0,
        "count": 0,
    }
    shops = []
    try:
        shops = list_shops(limit=500) or []
        expense_rows = list_company_supplier_stock_ins(
            analytics_filter=analytics_filter,
            shop_id=shop_filter_id,
            supplier_search=supplier_q or None,
            limit=8000,
        ) or []
        for row in expense_rows:
            cat = row.get("created_at")
            row["created_at_iso"] = _income_row_created_iso(cat)
        expense_totals["total_cost"] = sum(float(r.get("total_cost") or 0) for r in expense_rows)
        expense_totals["amount_paid"] = sum(float(r.get("amount_paid") or 0) for r in expense_rows)
        expense_totals["balance"] = sum(float(r.get("balance") or 0) for r in expense_rows)
        expense_totals["count"] = len(expense_rows)
    except Exception:
        logger.exception("it_support_accounts_expenses list failed")
        try:
            shops = list_shops(limit=500) or []
        except Exception:
            shops = []
    return render_template(
        "it_support_accounts_expenses.html",
        analytics_filter=analytics_filter,
        expense_rows=expense_rows,
        expense_totals=expense_totals,
        shops=shops,
        shop_filter_id=shop_filter_id or 0,
        supplier_q=supplier_q,
        mpesa_payout=_daraja_expenses_mpesa_context(
            url_for("daraja_mpesa_stk_callback", _external=True)
        ),
    )


@app.route("/it_support/company-operational-expenses", methods=["GET", "POST"])
@login_required
def it_support_company_operational_expenses():
    """Company-wide expenditure across all shops (stock, operational, stock out)."""
    _it_support_only()
    from database import (
        _income_row_created_iso,
        _sum_shop_expenditure_totals,
        list_company_expenditure_for_report,
        list_shops,
    )

    analytics_filter = _build_analytics_filter()
    shop_filter_arg = request.args.get("shop_id", type=int)
    shop_filter_id = shop_filter_arg if shop_filter_arg and shop_filter_arg > 0 else None
    expense_rows: list = []
    expense_totals = {"total_cost": 0.0, "amount_paid": 0.0, "balance": 0.0, "count": 0}
    shops = []
    try:
        shops = list_shops(limit=500) or []
        expense_rows = list_company_expenditure_for_report(
            analytics_filter=analytics_filter,
            shop_id=shop_filter_id,
        ) or []
        for row in expense_rows:
            row["created_at_iso"] = _income_row_created_iso(row.get("created_at"))
        totals = _sum_shop_expenditure_totals(expense_rows)
        expense_totals["total_cost"] = totals["total_expenditure"]
        expense_totals["amount_paid"] = totals["paid_expenditure"]
        expense_totals["balance"] = totals["balance_expenditure"]
        expense_totals["count"] = len(expense_rows)
    except Exception:
        logger.exception("it_support_company_operational_expenses list failed")
        try:
            shops = list_shops(limit=500) or []
        except Exception:
            shops = []

    return render_template(
        "it_support_company_operational_expenses.html",
        analytics_filter=analytics_filter,
        expense_rows=expense_rows,
        expense_totals=expense_totals,
        shops=shops,
        shop_filter_id=shop_filter_id or 0,
        report_today_iso=date.today().isoformat(),
    )


@app.route("/it_support/accounts/expenses/mpesa-pay", methods=["POST"])
@login_required
def it_support_accounts_expenses_mpesa_pay():
    """Send M-Pesa B2C payment to a supplier for an expense line."""
    _it_support_only()
    from daraja_api import (
        DarajaApiError,
        balance_callback_ngrok_warning,
        initiate_b2c_payment,
        normalize_msisdn,
        resolve_b2c_callbacks_detailed,
    )
    from database import get_company_stock_in_transaction, get_shop_manual_stock_in_transaction

    data = request.get_json(force=True, silent=True) or {}
    try:
        tx_id = int(data.get("tx_id") or 0)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Invalid expense reference."}), 400
    tx_scope = str(data.get("tx_scope") or "").strip().lower()
    if tx_scope not in ("company", "shop"):
        return jsonify({"ok": False, "error": "Invalid expense scope."}), 400

    if tx_scope == "company":
        row = get_company_stock_in_transaction(tx_id)
    else:
        row = get_shop_manual_stock_in_transaction(tx_id)
    if not row:
        return jsonify({"ok": False, "error": "Expense line not found."}), 404

    balance = float(row.get("balance") or 0)
    if balance <= 0:
        return jsonify({"ok": False, "error": "Nothing left to pay on this line."}), 400

    phone = (data.get("phone") or row.get("seller_phone") or "").strip()
    if phone in ("", "-"):
        return jsonify(
            {"ok": False, "error": "Supplier phone number is missing on this purchase."}
        ), 400

    try:
        amount = float(data.get("amount") if data.get("amount") is not None else balance)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Invalid payment amount."}), 400
    if amount <= 0:
        return jsonify({"ok": False, "error": "Amount must be greater than zero."}), 400
    if amount > balance + 0.01:
        return jsonify(
            {
                "ok": False,
                "error": f"Amount exceeds balance ({balance:.2f}).",
            }
        ), 400
    amount = round(min(amount, balance), 2)

    settings = _load_daraja_settings()
    if not settings.get("daraja_enabled"):
        return jsonify({"ok": False, "error": "Daraja M-Pesa is not enabled."}), 403

    request_hint = url_for("daraja_mpesa_stk_callback", _external=True)
    detailed = resolve_b2c_callbacks_detailed(settings, request_hint, probe=True)
    result_url = detailed.get("result_url") or ""
    timeout_url = detailed.get("timeout_url") or ""
    if not result_url or not timeout_url:
        return jsonify(
            {
                "ok": False,
                "error": detailed.get("error")
                or "M-Pesa payout callback URL is missing or unreachable.",
            }
        ), 400

    item_name = str(row.get("item_name") or "Stock").strip()[:40]
    seller_name = str(row.get("seller_name") or "Supplier").strip()[:40]
    remarks = f"Expense {item_name}"[:100]
    occasion = f"{seller_name}"[:100]

    try:
        result = initiate_b2c_payment(
            settings,
            phone=phone,
            amount=amount,
            result_url=result_url,
            timeout_url=timeout_url,
            remarks=remarks,
            occasion=occasion,
            command_id="BusinessPayment",
        )
    except DarajaApiError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception:
        logger.exception("M-Pesa B2C expense payout failed")
        return jsonify({"ok": False, "error": "Could not start M-Pesa payout."}), 500

    cid = str(result.get("conversation_id") or "").strip()
    orig = str(result.get("originator_conversation_id") or "").strip()
    _daraja_b2c_register_pending(
        cid,
        orig,
        meta={
            "expense_tx_id": tx_id,
            "expense_tx_scope": tx_scope,
            "payout_amount": amount,
            "payout_phone": normalize_msisdn(phone),
            "item_name": item_name,
            "seller_name": seller_name,
        },
    )
    ngrok_warn = balance_callback_ngrok_warning(
        result_url,
        callback_mode=detailed.get("callback_mode") or "",
        probe=detailed.get("probe"),
        hosted_fallback=bool(detailed.get("hosted_fallback")),
    )
    return jsonify(
        {
            "ok": True,
            "conversation_id": cid,
            "originator_conversation_id": orig,
            "result_url": result_url,
            "callback_mode": detailed.get("callback_mode") or "hosted",
            "hosted_fallback": bool(detailed.get("hosted_fallback")),
            "ngrok_warning": ngrok_warn or None,
            "message": ngrok_warn or "M-Pesa payout sent. Waiting for Safaricom confirmation…",
        }
    )


@app.route("/it_support/accounts/expenses/mpesa-pay-status")
@login_required
def it_support_accounts_expenses_mpesa_pay_status():
    """Poll status of an in-flight M-Pesa B2C expense payout."""
    _it_support_only()
    cid = str(request.args.get("conversation_id") or "").strip()
    orig = str(request.args.get("originator_conversation_id") or "").strip()
    if not cid and not orig:
        return jsonify({"ok": False, "error": "conversation_id is required."}), 400
    row = _daraja_b2c_status_get_any([cid, orig]) or {}
    if not row:
        return jsonify({"ok": True, "pending": True, "completed": False})
    if row.get("timed_out"):
        return jsonify(
            {
                "ok": False,
                "completed": True,
                "pending": False,
                "error": row.get("result_desc") or "M-Pesa payout timed out.",
            }
        )
    if row.get("completed"):
        if int(row.get("result_code") or -1) != 0:
            return jsonify(
                {
                    "ok": False,
                    "completed": True,
                    "pending": False,
                    "error": row.get("result_desc") or "M-Pesa payout failed.",
                }
            )
        if not row.get("payment_applied"):
            if _daraja_b2c_apply_expense_payment(row):
                row = {**row, "payment_applied": True}
                for key in (
                    str(row.get("conversation_id") or "").strip(),
                    cid,
                    orig,
                ):
                    if key:
                        _daraja_b2c_status_store(key, row)
        return jsonify(
            {
                "ok": True,
                "completed": True,
                "pending": False,
                "payment_applied": bool(row.get("payment_applied")),
                "transaction_receipt": row.get("transaction_receipt") or "",
                "amount": row.get("amount") or row.get("payout_amount"),
                "result_desc": row.get("result_desc") or "Payment sent.",
            }
        )
    return jsonify({"ok": True, "pending": True, "completed": False})


@app.route("/it_support/accounts/company-account")
@login_required
def it_support_accounts_company_account():
    """Company M-Pesa paybill / till account overview."""
    _it_support_only()
    request_hint = url_for("daraja_mpesa_stk_callback", _external=True)
    ctx = _daraja_company_account_context(request_hint)
    return render_template(
        "it_support_accounts_company.html",
        mpesa_account=ctx.get("mpesa_account") or {},
        mpesa_balance=ctx.get("mpesa_balance") or {},
        daraja_endpoints=ctx.get("daraja_endpoints") or {},
    )


@app.route("/it_support/accounts/company")
@login_required
def it_support_accounts_company_redirect():
    """Legacy URL → company account page."""
    return redirect(url_for("it_support_accounts_company_account"))


@app.route("/it_support/accounts/company-account/balance-refresh", methods=["POST"])
@app.route("/it_support/accounts/company/balance-refresh", methods=["POST"])
@login_required
def it_support_accounts_company_account_balance_refresh():
    """Trigger a live M-Pesa account balance lookup via Daraja."""
    _it_support_only()
    from daraja_api import (
        DarajaApiError,
        balance_callback_ngrok_warning,
        initiate_account_balance_query,
        probe_callback_url,
        resolve_balance_callbacks_detailed,
    )

    settings = _load_daraja_settings()
    request_hint = url_for("daraja_mpesa_stk_callback", _external=True)
    detailed = resolve_balance_callbacks_detailed(settings, request_hint, probe=True)
    result_url = detailed.get("result_url") or ""
    timeout_url = detailed.get("timeout_url") or ""
    if not result_url or not timeout_url:
        return jsonify(
            {
                "ok": False,
                "error": detailed.get("error")
                or "Balance callback URL is missing or unreachable.",
                "skipped_callbacks": detailed.get("skipped_callbacks") or [],
                "probe": detailed.get("probe"),
            }
        ), 400
    ngrok_warn = balance_callback_ngrok_warning(
        result_url,
        callback_mode=detailed.get("callback_mode") or "",
        probe=detailed.get("probe"),
        hosted_fallback=bool(detailed.get("hosted_fallback")),
    )
    try:
        result = initiate_account_balance_query(
            settings,
            result_url=result_url,
            timeout_url=timeout_url,
        )
    except DarajaApiError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception:
        logger.exception("M-Pesa balance refresh failed")
        return jsonify({"ok": False, "error": "Could not request balance. Try again."}), 500

    cid = str(result.get("conversation_id") or "").strip()
    orig = str(result.get("originator_conversation_id") or "").strip()
    _daraja_balance_register_pending(cid, orig)
    message = "Balance request sent. Safaricom will respond shortly."
    if ngrok_warn:
        message = ngrok_warn
    return jsonify(
        {
            "ok": True,
            "conversation_id": cid,
            "originator_conversation_id": orig,
            "result_url": result_url,
            "timeout_url": timeout_url,
            "api_url": result.get("api_url") or "",
            "ngrok_warning": ngrok_warn or None,
            "message": message,
            "callback_mode": detailed.get("callback_mode") or "hosted",
            "is_local_session": bool(detailed.get("is_local_session")),
            "hosted_fallback": bool(detailed.get("hosted_fallback")),
            "probe": detailed.get("probe"),
            "skipped_callbacks": detailed.get("skipped_callbacks") or [],
        }
    )


@app.route("/it_support/accounts/company-account/callback-test", methods=["POST"])
@app.route("/it_support/accounts/company/callback-test", methods=["POST"])
@login_required
def it_support_accounts_company_account_callback_test():
    """Probe whether Safaricom can POST to the active balance callback URL."""
    _it_support_only()
    from daraja_api import balance_callback_url_options, probe_callback_url

    settings = _load_daraja_settings()
    request_hint = url_for("daraja_mpesa_stk_callback", _external=True)
    opts = balance_callback_url_options(settings, request_hint)
    targets = [
        ("local", opts.get("local_result_url") or ""),
        ("hosted", opts.get("hosted_result_url") or ""),
        ("active", opts.get("active_result_url") or ""),
    ]
    seen: set[str] = set()
    results = []
    for label, url in targets:
        u = str(url or "").strip()
        if not u or u in seen:
            continue
        seen.add(u)
        results.append({"label": label, "url": u, **probe_callback_url(u)})
    return jsonify({"ok": True, "results": results})


@app.route("/it_support/accounts/company-account/balance-status")
@app.route("/it_support/accounts/company/balance-status")
@login_required
def it_support_accounts_company_account_balance_status():
    """Poll status of an in-flight M-Pesa balance lookup."""
    _it_support_only()
    cid = str(request.args.get("conversation_id") or "").strip()
    orig = str(request.args.get("originator_conversation_id") or "").strip()
    if not cid and not orig:
        return jsonify({"ok": False, "error": "conversation_id is required."}), 400
    row = _daraja_balance_status_get_any([cid, orig]) or {}
    if not row:
        return jsonify({"ok": True, "pending": True, "completed": False})
    if row.get("timed_out"):
        return jsonify(
            {
                "ok": False,
                "completed": True,
                "pending": False,
                "error": row.get("result_desc") or "Balance request timed out.",
            }
        )
    if row.get("completed"):
        if int(row.get("result_code") or -1) != 0:
            return jsonify(
                {
                    "ok": False,
                    "completed": True,
                    "pending": False,
                    "error": row.get("result_desc") or "Balance lookup failed.",
                }
            )
        balance = row.get("balance")
        if balance is None:
            return jsonify(
                {
                    "ok": False,
                    "completed": True,
                    "pending": False,
                    "error": "Balance response received but amount could not be parsed.",
                }
            )
        return jsonify(
            {
                "ok": True,
                "completed": True,
                "pending": False,
                "balance": float(balance),
                "currency": row.get("currency") or "KES",
                "account_label": row.get("account_label") or "",
                "result_desc": row.get("result_desc") or "",
            }
        )
    return jsonify({"ok": True, "pending": True, "completed": False})


@app.route("/it_support/salaries")
@login_required
def it_support_company_salaries():
    _it_support_only()
    from database import (
        compute_payroll_period_balance,
        list_company_payroll_overview,
        list_employee_payroll_all_history,
    )

    _pay_freq_allowed = frozenset({"monthly", "weekly", "biweekly", "daily"})

    def _norm_freq_arg(raw: str):
        v = (raw or "").strip().lower()
        if v in ("", "all"):
            return None
        return v if v in _pay_freq_allowed else None

    roster_freq = _norm_freq_arg(request.args.get("pay_frequency") or "")
    history_freq = _norm_freq_arg(request.args.get("history_frequency") or "")

    overview_rows = []
    history_rows = []
    try:
        raw_overview = list_company_payroll_overview(
            pay_frequency_filter=roster_freq, limit=2000
        )
        for r in raw_overview:
            row = dict(r)
            pf = row.get("pay_frequency")
            if pf:
                row["period_balance"] = compute_payroll_period_balance(
                    row["employee_id"], pf
                )
            else:
                row["period_balance"] = None
            overview_rows.append(row)
        history_rows = list_employee_payroll_all_history(
            pay_frequency_filter=history_freq, limit=400
        )
    except Exception:
        overview_rows, history_rows = [], []

    return render_template(
        "it_support_company_salaries.html",
        overview_rows=overview_rows,
        history_rows=history_rows,
        roster_pay_frequency=roster_freq or "all",
        history_pay_frequency=history_freq or "all",
    )


@app.route("/it_support/salaries/register", methods=["GET", "POST"])
@login_required
def it_support_register_salary():
    _it_support_only()
    from database import (
        list_employee_payroll_recent,
        list_employees_payroll_eligible,
        register_employee_payroll,
        update_employee_payout_details,
    )

    try:
        employees = list_employees_payroll_eligible(limit=2000)
        recent_payroll = list_employee_payroll_recent(limit=50)
    except Exception:
        employees, recent_payroll = [], []

    if request.method == "POST":
        emp_raw = (request.form.get("employee_id") or "").strip()
        gross_raw = (request.form.get("gross_amount") or "").strip()
        freq = (request.form.get("pay_frequency") or "").strip()
        eff = (request.form.get("effective_from") or "").strip()
        notes = (request.form.get("notes") or "").strip()
        payout_method = (request.form.get("preferred_payment_method") or "").strip().lower()
        pay_holder = (request.form.get("payment_account_holder") or "").strip()
        pay_bank = (request.form.get("payment_bank_or_provider") or "").strip()
        pay_num = (request.form.get("payment_account_number") or "").strip()
        try:
            employee_id = int(emp_raw)
        except Exception:
            employee_id = 0

        payout_ok = True
        if payout_method not in ("mpesa", "bank", "cash"):
            flash("Select a payment method: M-Pesa, bank, or cash.", "error")
            payout_ok = False
        elif payout_method == "mpesa" and not pay_num:
            flash("Enter the M-Pesa phone number.", "error")
            payout_ok = False
        elif payout_method == "bank" and (not pay_bank or not pay_num):
            flash("Enter bank name and account number.", "error")
            payout_ok = False

        new_id = None
        if payout_ok:
            new_id = register_employee_payroll(employee_id, gross_raw, freq, eff, notes)
        if new_id:
            if not update_employee_payout_details(
                employee_id,
                preferred_payment_method=payout_method,
                payment_account_holder=pay_holder or None,
                payment_bank_or_provider=pay_bank or None,
                payment_account_number=pay_num or None,
            ):
                flash(
                    "Payroll was saved, but payout details could not be updated. You can edit the employee in HR management.",
                    "warning",
                )
            else:
                flash("Payroll registered and payout details saved.", "success")
            return redirect(url_for("it_support_register_salary"))
        if payout_ok:
            flash(
                "Could not register payroll. Choose an active employee, enter a positive gross amount, and a valid effective date.",
                "error",
            )
        try:
            recent_payroll = list_employee_payroll_recent(limit=50)
        except Exception:
            pass

    return render_template(
        "it_support_register_salary.html",
        employees=employees,
        recent_payroll=recent_payroll or [],
    )


@app.route("/it_support/salaries/advance", methods=["GET", "POST"])
@login_required
def it_support_register_advance():
    _it_support_only()
    from database import (
        get_payroll_advance_period_summary,
        list_employees_payroll_eligible,
        list_payroll_advances_recent,
        register_payroll_advance,
    )

    try:
        employees = list_employees_payroll_eligible(limit=2000)
        recent_advances = list_payroll_advances_recent(limit=50)
    except Exception:
        employees, recent_advances = [], []

    if request.method == "POST":
        emp_raw = (request.form.get("employee_id") or "").strip()
        amount_raw = (request.form.get("amount") or "").strip()
        freq = (request.form.get("pay_frequency") or "").strip().lower()
        notes = (request.form.get("notes") or "").strip()
        period_month = (request.form.get("period_month") or "").strip()
        period_date = (request.form.get("period_date") or "").strip()
        try:
            employee_id = int(emp_raw)
        except Exception:
            employee_id = 0

        ref = None
        if freq == "monthly":
            if len(period_month) == 7:
                try:
                    ref = datetime.strptime(period_month + "-01", "%Y-%m-%d").date()
                except ValueError:
                    ref = None
        else:
            if len(period_date) >= 10:
                try:
                    ref = datetime.strptime(period_date[:10], "%Y-%m-%d").date()
                except ValueError:
                    ref = None

        if employee_id <= 0:
            flash("Select an employee.", "error")
        elif freq not in ("monthly", "weekly", "biweekly", "daily"):
            flash("Select pay frequency.", "error")
        elif ref is None:
            flash("Choose the payroll month (monthly) or period date (other frequencies).", "error")
        else:
            summary = None
            try:
                summary = get_payroll_advance_period_summary(employee_id, freq, ref)
            except Exception:
                summary = None
            new_id = register_payroll_advance(
                employee_id, amount_raw, freq, ref, notes or None
            )
            if new_id:
                if summary and summary.get("has_payroll_rate"):
                    try:
                        from decimal import Decimal

                        rem = Decimal(str(summary.get("remaining_before_new") or "0"))
                        amt = Decimal(str(amount_raw).replace(",", "").strip() or "0")
                        if amt > rem:
                            flash(
                                "Advance saved. It is larger than the remaining gross for this period "
                                "after earlier advances—review totals.",
                                "warning",
                            )
                        else:
                            flash("Advance registered against this pay period.", "success")
                    except Exception:
                        flash("Advance registered against this pay period.", "success")
                else:
                    flash(
                        "Advance saved. No payroll gross is on file for that period yet—register salary first to track deductions.",
                        "warning",
                    )
                return redirect(url_for("it_support_register_advance"))
            flash("Could not save advance. Check amount and employee.", "error")
        try:
            recent_advances = list_payroll_advances_recent(limit=50)
        except Exception:
            pass

    return render_template(
        "it_support_register_advance.html",
        employees=employees,
        recent_advances=recent_advances or [],
    )


@app.route("/it_support/salaries/advance/period-summary")
@login_required
def it_support_advance_period_summary():
    _it_support_only()
    emp_raw = (request.args.get("employee_id") or "").strip()
    freq = (request.args.get("pay_frequency") or "").strip().lower()
    period_raw = (request.args.get("period") or "").strip()
    try:
        employee_id = int(emp_raw)
    except Exception:
        return jsonify({"error": "invalid_employee"}), 400
    if freq not in ("monthly", "weekly", "biweekly", "daily"):
        return jsonify({"error": "invalid_frequency"}), 400

    ref = None
    if freq == "monthly" and len(period_raw) == 7:
        try:
            ref = datetime.strptime(period_raw + "-01", "%Y-%m-%d").date()
        except ValueError:
            ref = None
    elif len(period_raw) >= 10:
        try:
            ref = datetime.strptime(period_raw[:10], "%Y-%m-%d").date()
        except ValueError:
            ref = None
    if ref is None:
        return jsonify({"error": "invalid_period"}), 400

    try:
        from database import get_payroll_advance_period_summary

        summary = get_payroll_advance_period_summary(employee_id, freq, ref)
    except Exception:
        summary = None
    if not summary:
        return jsonify({"error": "not_found"}), 404
    return jsonify(summary)


@app.route("/it_support/salaries/loan")
@login_required
def it_support_register_loan():
    _it_support_only()
    return render_template("it_support_register_loan.html")


@app.route("/it_support/employees/<int:emp_id>/approve", methods=["POST"])
@login_required
def it_support_employee_approve(emp_id: int):
    _it_support_only()
    role = (request.form.get("role") or "").strip().lower()
    shop_id_raw = (request.form.get("shop_id") or "").strip()
    shop_id = None
    if shop_id_raw:
        try:
            shop_id = int(shop_id_raw)
        except Exception:
            shop_id = None
    linked_shop_ids = []
    for x in request.form.getlist("shop_ids"):
        try:
            linked_shop_ids.append(int(x))
        except (TypeError, ValueError):
            continue
    try:
        from database import approve_employee, get_employee_by_id, get_hr_employee_shop_link_mode

        emp_row = get_employee_by_id(emp_id)
        ok = approve_employee(
            emp_id,
            role=role,
            shop_id=shop_id,
            linked_shop_ids=linked_shop_ids if get_hr_employee_shop_link_mode() == "multi" else None,
        )
    except Exception:
        ok = False
        emp_row = None
    if ok:
        _log_hr_activity_safe(
            "approve",
            target_type="employee",
            target_id=int(emp_id),
            description=f"Approved employee #{emp_id} as {role or 'employee'}",
        )
        if emp_row:
            _notify_employee_approved_email(emp_row, role)
    flash_msg = "Employee approved."
    if ok:
        try:
            from mail_service import is_mail_configured

            if is_mail_configured() and emp_row and (emp_row.get("email") or "").strip():
                flash_msg += f" Approval email sent to {emp_row.get('email')}."
        except Exception:
            pass
    flash(flash_msg if ok else "Could not approve employee. Check role/shop selection.", "success" if ok else "error")
    return redirect(url_for("it_support_hr_management"))


@app.route("/it_support/employees/<int:emp_id>/edit", methods=["POST"])
@login_required
def it_support_employee_edit(emp_id: int):
    _it_support_only()
    full_name = (request.form.get("full_name") or "").strip()
    email = (request.form.get("email") or "").strip()
    phone = (request.form.get("phone") or "").strip()
    role = (request.form.get("role") or "").strip().lower()
    employee_code = (request.form.get("employee_code") or "").strip()
    pwd = (request.form.get("password") or "").strip()
    pwd_confirm = (request.form.get("password_confirm") or "").strip()
    shop_id_raw = (request.form.get("shop_id") or "").strip()
    shop_id = None
    if shop_id_raw:
        try:
            shop_id = int(shop_id_raw)
        except (TypeError, ValueError):
            shop_id = None
    linked_shop_ids = []
    for x in request.form.getlist("shop_ids"):
        try:
            linked_shop_ids.append(int(x))
        except (TypeError, ValueError):
            continue

    preferred_payment_method = (request.form.get("preferred_payment_method") or "").strip()
    payment_account_holder = (request.form.get("payment_account_holder") or "").strip()
    payment_bank_or_provider = (request.form.get("payment_bank_or_provider") or "").strip()
    payment_account_number = (request.form.get("payment_account_number") or "").strip()

    if not full_name or not email or "@" not in email:
        flash("Enter full name and a valid email.", "error")
        return redirect(url_for("it_support_hr_management"))

    if not re.fullmatch(r"\d{6}", employee_code):
        flash("Employee login code must be exactly 6 digits.", "error")
        return redirect(url_for("it_support_hr_management"))

    password_hash = None
    if pwd or pwd_confirm:
        if pwd != pwd_confirm:
            flash("Password and confirmation do not match.", "error")
            return redirect(url_for("it_support_hr_management"))
        if len(pwd) < 6:
            flash("New password must be at least 6 characters.", "error")
            return redirect(url_for("it_support_hr_management"))
        password_hash = generate_password_hash(pwd)

    try:
        from database import get_hr_employee_shop_link_mode, update_employee_by_it_hr

        hm = get_hr_employee_shop_link_mode()
        ok = update_employee_by_it_hr(
            emp_id,
            full_name=full_name,
            email=email,
            phone=phone,
            role=role,
            shop_id=shop_id,
            employee_code=employee_code,
            linked_shop_ids=(linked_shop_ids if hm == "multi" else None),
            password_hash=password_hash,
            preferred_payment_method=preferred_payment_method or None,
            payment_account_holder=payment_account_holder or None,
            payment_bank_or_provider=payment_bank_or_provider or None,
            payment_account_number=payment_account_number or None,
        )
    except Exception:
        ok = False
    if ok:
        _log_hr_activity_safe(
            "edit",
            target_type="employee",
            target_id=int(emp_id),
            description=(
                f"Edited employee #{emp_id} ({full_name or 'employee'}) — "
                f"role={role or 'unchanged'}"
                + (", password changed" if password_hash else "")
            ),
        )
        flash("Employee updated." + (" Password changed." if password_hash else ""), "success")
    else:
        flash(
            "Could not update employee. They may still be pending approval, the email or login code may be "
            "in use by someone else, or the role/shop combination is invalid.",
            "error",
        )
    return redirect(url_for("it_support_hr_management"))


@app.route("/it_support/employees/<int:emp_id>/suspend", methods=["POST"])
@login_required
def it_support_employee_suspend(emp_id: int):
    _it_support_only()
    try:
        sid = int(session.get("employee_id") or 0)
    except (TypeError, ValueError):
        sid = 0
    if sid and int(emp_id) == sid:
        flash("You cannot suspend your own account.", "error")
        return redirect(url_for("it_support_hr_management"))
    try:
        from database import set_employee_suspended

        ok = set_employee_suspended(emp_id, suspended=True)
    except Exception:
        ok = False
    if ok:
        _log_hr_activity_safe(
            "suspend",
            target_type="employee",
            target_id=int(emp_id),
            description=f"Suspended employee #{emp_id}",
        )
    flash("Employee suspended." if ok else "Could not suspend employee.", "success" if ok else "error")
    return redirect(url_for("it_support_hr_management"))


@app.route("/it_support/employees/<int:emp_id>/unsuspend", methods=["POST"])
@login_required
def it_support_employee_unsuspend(emp_id: int):
    _it_support_only()
    try:
        from database import set_employee_suspended

        ok = set_employee_suspended(emp_id, suspended=False)
    except Exception:
        ok = False
    if ok:
        _log_hr_activity_safe(
            "unsuspend",
            target_type="employee",
            target_id=int(emp_id),
            description=f"Reactivated employee #{emp_id}",
        )
    flash("Employee reactivated." if ok else "Could not reactivate employee.", "success" if ok else "error")
    return redirect(url_for("it_support_hr_management"))


@app.route("/it_support/employees/<int:emp_id>/delete", methods=["POST"])
@login_required
def it_support_employee_delete(emp_id: int):
    _it_support_only()
    try:
        sid = int(session.get("employee_id") or 0)
    except (TypeError, ValueError):
        sid = 0
    if sid and int(emp_id) == sid:
        flash("You cannot delete your own account.", "error")
        return redirect(url_for("it_support_hr_management"))
    try:
        from database import delete_employee_if_approved

        ok = delete_employee_if_approved(emp_id)
    except Exception:
        ok = False
    if ok:
        _log_hr_activity_safe(
            "delete",
            target_type="employee",
            target_id=int(emp_id),
            description=f"Deleted employee #{emp_id}",
        )
    flash("Employee deleted." if ok else "Could not delete employee.", "success" if ok else "error")
    return redirect(url_for("it_support_hr_management"))


@app.route("/it_support/website-management")
@login_required
def it_support_website_management():
    _it_support_only()
    return redirect(url_for("it_support_system_settings", tab="theme"))


@app.route("/it_support/website-management/settings", methods=["GET", "POST"])
@login_required
def it_support_website_settings_removed():
    """Legacy URL — website appearance now lives in System settings."""
    _it_support_only()
    return redirect(url_for("it_support_system_settings", tab="company"), code=301)


@app.route("/it_support/website-management/designs", methods=["GET", "POST"])
@login_required
def it_support_website_designs():
    _it_support_only()
    ws = _load_website_settings()
    design_defaults = _default_website_design()

    if request.method == "POST":
        action = (request.form.get("action") or "save").strip().lower()
        if action == "toggle_enabled":
            enabled = (request.form.get("public_website_enabled") or "").strip().lower() in {
                "1",
                "on",
                "true",
                "yes",
            }
            updated = {**ws, "public_website_enabled": enabled}
            ok = _save_website_settings(updated)
            if ok:
                if enabled:
                    flash("Public website is on — visitors see your shop homepage.", "success")
                else:
                    flash("Public website is off — visitors see the shop login page instead.", "success")
            else:
                flash("Could not update website visibility. Try again.", "error")
            return redirect(url_for("it_support_website_designs"))
        if action == "reset_auto":
            ids: list[int] = []
            design = _website_design_from_form(ws.get("design") or design_defaults)
        else:
            ids = _normalize_featured_item_ids(request.form.get("featured_item_ids") or "[]")
            design = _website_design_from_form(ws.get("design") or design_defaults)
        enabled = (request.form.get("public_website_enabled") or "").strip().lower() in {
            "1",
            "on",
            "true",
            "yes",
        }
        updated = {**ws, "featured_item_ids": ids, "design": design, "public_website_enabled": enabled}
        ok = _save_website_settings(updated)
        if ok:
            if action == "reset_auto":
                flash("Homepage products reset to automatic best-sellers from POS.", "success")
            else:
                flash("Website saved — homepage content, SEO, and featured products updated.", "success")
        else:
            flash("Could not save website. Try again.", "error")
        return redirect(url_for("it_support_website_designs"))

    products = _website_featured_products(limit=WEBSITE_HOMEPAGE_FEATURED_MAX)
    try:
        from database import list_website_catalog_items

        catalog_rows = list_website_catalog_items(limit=300)
    except Exception:
        catalog_rows = []
    catalog_products = [_serialize_website_product_row(r) for r in catalog_rows if int(r.get("id") or 0) > 0]
    selected_ids = ws.get("featured_item_ids") or []
    using_auto = not selected_ids
    return render_template(
        "it_support_website_designs.html",
        website_ws=ws,
        website_design=ws.get("design") or design_defaults,
        website_featured_products=products,
        website_catalog_products=catalog_products,
        website_product_categories=_website_product_categories(products),
        website_selected_ids=selected_ids,
        website_using_auto_products=using_auto,
        preview_url=_public_storefront_url(),
        website_homepage_featured_max=WEBSITE_HOMEPAGE_FEATURED_MAX,
        catalog_preview_url=url_for("marketing_catalog"),
    )


@app.route("/ai/my-accountant/summary")
@login_required
def ai_my_accountant_summary():
    """Quick financial + stock snapshot for the floating 'My Accountant' assistant.

    Scope rules:
      * Super admin / IT support / company manager  → all shops (company-wide).
        If they're explicitly viewing a specific shop (shop_id query param sent
        by the partial when included from shop_layout.html), the snapshot
        narrows to just that shop.
      * Everyone else (shop manager / staff / employee) → always their shop.
    """
    role_key = (session.get("employee_role") or "employee").strip().lower()

    qs_shop_id_raw = (request.args.get("shop_id") or "").strip()
    qs_shop_id = None
    if qs_shop_id_raw.isdigit():
        try:
            qs_shop_id = int(qs_shop_id_raw)
            if qs_shop_id <= 0:
                qs_shop_id = None
        except ValueError:
            qs_shop_id = None

    if role_key in COMPANY_PORTAL_ROLES:
        shop_id = qs_shop_id
    else:
        assigned = _effective_viewer_shop_id(role_key)
        if qs_shop_id and assigned and qs_shop_id == assigned:
            shop_id = qs_shop_id
        else:
            shop_id = assigned

    today = date.today()
    month_start = today.replace(day=1)
    audit_window_start = today - timedelta(days=7)

    result = {
        "ok": True,
        "scope": "shop" if shop_id else "company",
        "shop_id": int(shop_id) if shop_id else None,
        "revenue": {
            "today": 0.0,
            "today_credit": 0.0,
            "today_sale": 0.0,
            "month": 0.0,
        },
        "unpaid_credits": {"count": 0, "balance": 0.0},
        "low_stock": {"kitchen_count": 0, "store_count": 0, "total": 0},
        "audits": {
            "cancelled_sales": 0,
            "returned_sales": 0,
            "stock_outs": 0,
            "total": 0,
            "window_days": 7,
        },
        "links": {},
    }

    try:
        from database import get_cursor

        with get_cursor() as cur:
            try:
                if shop_id:
                    cur.execute(
                        """
                        SELECT
                            COALESCE(SUM(CASE WHEN DATE(created_at) = %s THEN total_amount ELSE 0 END), 0) AS today_total,
                            COALESCE(SUM(CASE WHEN DATE(created_at) = %s AND sale_type='credit' THEN total_amount ELSE 0 END), 0) AS today_credit,
                            COALESCE(SUM(CASE WHEN DATE(created_at) = %s AND sale_type='sale' THEN total_amount ELSE 0 END), 0) AS today_sale,
                            COALESCE(SUM(CASE WHEN created_at >= %s THEN total_amount ELSE 0 END), 0) AS month_total
                        FROM shop_pos_sales
                        WHERE shop_id = %s
                        """,
                        (today, today, today, month_start, shop_id),
                    )
                else:
                    cur.execute(
                        """
                        SELECT
                            COALESCE(SUM(CASE WHEN DATE(created_at) = %s THEN total_amount ELSE 0 END), 0) AS today_total,
                            COALESCE(SUM(CASE WHEN DATE(created_at) = %s AND sale_type='credit' THEN total_amount ELSE 0 END), 0) AS today_credit,
                            COALESCE(SUM(CASE WHEN DATE(created_at) = %s AND sale_type='sale' THEN total_amount ELSE 0 END), 0) AS today_sale,
                            COALESCE(SUM(CASE WHEN created_at >= %s THEN total_amount ELSE 0 END), 0) AS month_total
                        FROM shop_pos_sales
                        """,
                        (today, today, today, month_start),
                    )
                r = cur.fetchone() or {}
                result["revenue"]["today"] = float(r.get("today_total") or 0)
                result["revenue"]["today_credit"] = float(r.get("today_credit") or 0)
                result["revenue"]["today_sale"] = float(r.get("today_sale") or 0)
                result["revenue"]["month"] = float(r.get("month_total") or 0)
            except Exception:
                pass

            try:
                if shop_id:
                    cur.execute(
                        """
                        SELECT COUNT(*) AS cnt,
                               COALESCE(SUM(GREATEST(total_amount - credit_paid_amount, 0)), 0) AS bal
                        FROM shop_pos_sales
                        WHERE shop_id=%s AND sale_type='credit'
                          AND COALESCE(credit_status, 'not_paid') <> 'paid'
                          AND (total_amount - credit_paid_amount) > 0.0001
                        """,
                        (shop_id,),
                    )
                else:
                    cur.execute(
                        """
                        SELECT COUNT(*) AS cnt,
                               COALESCE(SUM(GREATEST(total_amount - credit_paid_amount, 0)), 0) AS bal
                        FROM shop_pos_sales
                        WHERE sale_type='credit'
                          AND COALESCE(credit_status, 'not_paid') <> 'paid'
                          AND (total_amount - credit_paid_amount) > 0.0001
                        """
                    )
                r = cur.fetchone() or {}
                result["unpaid_credits"]["count"] = int(r.get("cnt") or 0)
                result["unpaid_credits"]["balance"] = float(r.get("bal") or 0)
            except Exception:
                pass

            try:
                if shop_id:
                    cur.execute(
                        """
                        SELECT COUNT(*) AS cnt
                        FROM shop_items si
                        JOIN items i ON i.id = si.item_id
                        WHERE si.shop_id = %s
                          AND COALESCE(NULLIF(si.low_stock_threshold, 0), i.low_stock_threshold, 0) > 0
                          AND si.shop_stock_qty <= COALESCE(NULLIF(si.low_stock_threshold, 0), i.low_stock_threshold, 0)
                        """,
                        (shop_id,),
                    )
                else:
                    cur.execute(
                        """
                        SELECT COUNT(*) AS cnt
                        FROM shop_items si
                        JOIN items i ON i.id = si.item_id
                        WHERE COALESCE(NULLIF(si.low_stock_threshold, 0), i.low_stock_threshold, 0) > 0
                          AND si.shop_stock_qty <= COALESCE(NULLIF(si.low_stock_threshold, 0), i.low_stock_threshold, 0)
                        """
                    )
                r = cur.fetchone() or {}
                result["low_stock"]["store_count"] = int(r.get("cnt") or 0)
            except Exception:
                pass

            try:
                if shop_id:
                    cur.execute(
                        """
                        SELECT COUNT(*) AS cnt
                        FROM shop_kitchen_portions
                        WHERE shop_id=%s AND portions_remaining > 0 AND portions_remaining <= 5
                        """,
                        (shop_id,),
                    )
                else:
                    cur.execute(
                        """
                        SELECT COUNT(*) AS cnt
                        FROM shop_kitchen_portions
                        WHERE portions_remaining > 0 AND portions_remaining <= 5
                        """
                    )
                r = cur.fetchone() or {}
                result["low_stock"]["kitchen_count"] = int(r.get("cnt") or 0)
            except Exception:
                pass

            result["low_stock"]["total"] = (
                int(result["low_stock"]["kitchen_count"])
                + int(result["low_stock"]["store_count"])
            )

            try:
                if shop_id:
                    cur.execute(
                        """
                        SELECT
                            COALESCE(SUM(CASE WHEN COALESCE(receipt_mark_status,'pending') = 'cancelled' THEN 1 ELSE 0 END), 0) AS cancelled_cnt,
                            COALESCE(SUM(CASE WHEN COALESCE(receipt_mark_status,'pending') IN ('returned','partial_return') THEN 1 ELSE 0 END), 0) AS returned_cnt
                        FROM shop_pos_sales
                        WHERE shop_id=%s AND created_at >= %s
                        """,
                        (shop_id, audit_window_start),
                    )
                else:
                    cur.execute(
                        """
                        SELECT
                            COALESCE(SUM(CASE WHEN COALESCE(receipt_mark_status,'pending') = 'cancelled' THEN 1 ELSE 0 END), 0) AS cancelled_cnt,
                            COALESCE(SUM(CASE WHEN COALESCE(receipt_mark_status,'pending') IN ('returned','partial_return') THEN 1 ELSE 0 END), 0) AS returned_cnt
                        FROM shop_pos_sales
                        WHERE created_at >= %s
                        """,
                        (audit_window_start,),
                    )
                r = cur.fetchone() or {}
                result["audits"]["cancelled_sales"] = int(r.get("cancelled_cnt") or 0)
                result["audits"]["returned_sales"] = int(r.get("returned_cnt") or 0)
            except Exception:
                pass

            stock_outs = 0
            try:
                if shop_id:
                    cur.execute(
                        """
                        SELECT COUNT(*) AS cnt
                        FROM shop_stock_transactions
                        WHERE shop_id=%s AND direction='out' AND created_at >= %s
                        """,
                        (shop_id, audit_window_start),
                    )
                    rr = cur.fetchone() or {}
                    stock_outs = int(rr.get("cnt") or 0)
                else:
                    cur.execute(
                        """
                        SELECT COUNT(*) AS cnt
                        FROM shop_stock_transactions
                        WHERE direction='out' AND created_at >= %s
                        """,
                        (audit_window_start,),
                    )
                    rr = cur.fetchone() or {}
                    stock_outs = int(rr.get("cnt") or 0)
                    try:
                        cur.execute(
                            """
                            SELECT COUNT(*) AS cnt
                            FROM stock_transactions
                            WHERE direction='out' AND created_at >= %s
                            """,
                            (audit_window_start,),
                        )
                        rr2 = cur.fetchone() or {}
                        stock_outs += int(rr2.get("cnt") or 0)
                    except Exception:
                        pass
                result["audits"]["stock_outs"] = stock_outs
            except Exception:
                pass

            result["audits"]["total"] = (
                int(result["audits"]["cancelled_sales"])
                + int(result["audits"]["returned_sales"])
                + int(result["audits"]["stock_outs"])
            )
    except Exception:
        result["ok"] = False

    try:
        if shop_id:
            audits_url = (
                url_for("shop_receipts", shop_id=int(shop_id))
                if "shop_receipts" in app.view_functions
                else url_for("shop_audits", shop_id=int(shop_id))
            )
            stockouts_url = (
                url_for("shop_stock_audits", shop_id=int(shop_id))
                if "shop_stock_audits" in app.view_functions
                else url_for("shop_stock_management", shop_id=int(shop_id))
            )
            result["links"] = {
                "analytics": url_for("shop_analytics", shop_id=int(shop_id)),
                "credits": url_for("shop_credit_payments", shop_id=int(shop_id)),
                "stock": url_for("shop_current_stock", shop_id=int(shop_id)),
                "audits": audits_url,
                "stockouts": stockouts_url,
            }
        else:
            stock_url = (
                url_for("it_support_stock_status")
                if "it_support_stock_status" in app.view_functions
                else url_for("it_support_stock_management")
            )
            audits_url = (
                url_for("it_support_receipts")
                if "it_support_receipts" in app.view_functions
                else url_for("it_support_credit_payments_audit")
            )
            stockouts_url = (
                url_for("it_support_stock_movement_analysis")
                if "it_support_stock_movement_analysis" in app.view_functions
                else stock_url
            )
            result["links"] = {
                "analytics": url_for("it_support_analytics"),
                "credits": url_for("it_support_credit_payments"),
                "stock": stock_url,
                "audits": audits_url,
                "stockouts": stockouts_url,
            }
    except Exception:
        result["links"] = {
            "analytics": "", "credits": "", "stock": "",
            "audits": "", "stockouts": "",
        }

    return jsonify(result)


if __name__ == "__main__":
    # Listen on all interfaces so LAN devices (e.g. phone) can use http://<this-PC-LAN-IP>:5000
    app.run(debug=True, host="0.0.0.0", port=5000)
