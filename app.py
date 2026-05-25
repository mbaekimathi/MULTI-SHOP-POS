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
from urllib.parse import urlparse

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
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename

logger = logging.getLogger(__name__)

# Portal footer "Powered by JOS" — override with env JOS_VERSION
JOS_VERSION = (os.getenv("JOS_VERSION") or "1.0.0").strip()

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-secret-change-me")
app.config["MAX_CONTENT_LENGTH"] = 5 * 1024 * 1024  # 5 MB uploads

UPLOAD_FOLDER_REL = "uploads/profiles"
ALLOWED_PROFILE_EXT = {"png", "jpg", "jpeg", "gif", "webp"}
ALLOWED_APP_ICON_EXT = {"png", "jpg", "jpeg", "gif", "webp", "ico", "svg"}

CODE_RE = re.compile(r"^\d{6}$")

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
}


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


def _safe_login_next(next_url: str) -> bool:
    """Reject open redirects (``//evil``) while allowing same-origin paths."""
    u = (next_url or "").strip()
    return u.startswith("/") and not u.startswith("//")


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
    if _safe_login_next(nxt):
        return redirect(url_for("employee_login", next=nxt))
    return redirect(url_for("employee_login"))


def _session_may_follow_login_next(next_url: str):
    """
    Logged-in portal user visits GET /login?next=...
    Honor branch URLs when IT/support or employee may use that shop (multi-branch aware).
    """
    if not _safe_login_next(next_url):
        return None
    target_sid = _parse_next_shop_id(next_url)
    if target_sid is None:
        return None
    role_key = session.get("employee_role") or "employee"
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
    """Shop primary/accent when set; otherwise company (IT) defaults from site settings."""
    company_pri = "#f97316"
    company_acc = "#fb923c"
    try:
        from database import get_site_settings

        ss = get_site_settings(["primary_color", "accent_color"]) or {}
        if (ss.get("primary_color") or "").strip():
            company_pri = str(ss["primary_color"]).strip()
        if (ss.get("accent_color") or "").strip():
            company_acc = str(ss["accent_color"]).strip()
    except Exception:
        pass
    sp = (shop.get("primary_color") or "").strip()
    sa = (shop.get("accent_color") or "").strip()
    return (
        _hex_to_rgb_triplet(sp if sp else company_pri),
        _hex_to_rgb_triplet(sa if sa else company_acc),
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
    return {
        "site_settings": merged,
        "theme_default": merged["default_theme"],
        "font_family": site_font,
        "portal_font_family": portal_font,
        "font_family_stack": font_css_stack(site_font),
        "theme_preset": preset_key,
        "theme_presets_css": THEME_PRESETS,
        "theme_font_google_url": google_fonts_url(portal_font),
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
    if request.endpoint == "shop_pos":
        return {"notification_count": 0, "notifications_url": None, "notification_scope": None}

    uid = session.get("employee_id")
    if not uid:
        return {"notification_count": 0, "notifications_url": None, "notification_scope": None}
    role_key = (session.get("employee_role") or "employee").strip().lower()
    shop_id = _effective_viewer_shop_id(role_key)
    try:
        from database import count_notifications_for_session

        notification_count = count_notifications_for_session(
            employee_id=int(uid),
            shop_id=int(shop_id) if shop_id else None,
            role_key=role_key,
        )
    except Exception:
        notification_count = 0
    notifications_url = (
        url_for("shop_notifications", shop_id=int(shop_id)) if shop_id else url_for("notifications")
    )
    notification_scope = f"shop-{int(shop_id)}" if shop_id else "portal"
    return {
        "notification_count": int(notification_count or 0),
        "notifications_url": notifications_url,
        "notification_scope": notification_scope,
    }


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("employee_id"):
            return redirect(url_for("employee_login", next=request.path))
        return f(*args, **kwargs)

    return decorated


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


@app.route("/")
def index():
    """Simple home hub with sign-in and site links."""
    return render_template("home.html")


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

        session["shop_id"] = int(shop["id"])
        session["shop_name"] = shop.get("shop_name")
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


@app.route("/logout", methods=["POST", "GET"])
def employee_logout():
    had_employee = bool(session.get("employee_id"))
    session.pop("employee_id", None)
    session.pop("employee_name", None)
    session.pop("employee_role", None)
    session.pop("shop_id", None)
    session.pop("shop_name", None)
    session.pop("shop_role_preview", None)
    flash("You have been signed out.", "success")
    if had_employee:
        return redirect(url_for("employee_login"))
    return redirect(url_for("public_shop_login"))


@app.route("/signup", methods=["GET", "POST"])
def employee_signup():
    if session.get("employee_id"):
        return _redirect_to_employee_dashboard()

    if request.method == "POST":
        full_name = (request.form.get("full_name") or "").strip().upper()
        email = (request.form.get("email") or "").strip()
        phone = (request.form.get("phone") or "").strip()
        code = (request.form.get("employee_code") or "").strip()
        password = request.form.get("password") or ""
        confirm = request.form.get("confirm_password") or ""

        errors = []
        if len(full_name) < 2:
            errors.append("Enter your full name.")
        if not email or "@" not in email:
            errors.append("Enter a valid email.")
        if len(phone) < 8:
            errors.append("Enter a valid phone number.")
        if not CODE_RE.match(code):
            errors.append("Employee code must be exactly 6 digits.")
        if len(password) < 6:
            errors.append("Password must be at least 6 characters (numbers-only is allowed, e.g. 123456).")
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
        except Exception:
            flash("Could not complete registration. Check database settings or try again later.", "error")
            return redirect(url_for("employee_signup"))

        flash(
            "Registration submitted. Your status is pending approval — an administrator will activate your account.",
            "success",
        )
        return redirect(url_for("employee_login"))

    return render_template("signup.html")


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


def _shop_has_printing_override(shop: dict) -> bool:
    return _parse_shop_settings_json(shop.get("printing_settings_json")) is not None


def _shop_has_receipt_override(shop: dict) -> bool:
    return _parse_shop_settings_json(shop.get("receipt_settings_json")) is not None


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

            values = {
                "company_name": company_name,
                "company_email": company_email,
                "company_phone": company_phone,
                "company_facebook": company_facebook,
                "company_instagram": company_instagram,
                "primary_color": primary_color,
                "accent_color": accent_color,
                "font_family": font_family,
                "default_theme": default_theme,
                "theme_preset": theme_preset,
                "receipt_settings_json": json.dumps(_receipt_settings_from_form(), separators=(",", ":")),
                "printing_settings_json": json.dumps(_printing_settings_from_form(), separators=(",", ":")),
            }
            if app_icon_path is not None:
                values["app_icon"] = app_icon_path
            ok = set_site_settings(values)
        except Exception:
            ok = False

        if ok:
            if wants_json:
                ps_after = _load_printing_settings()
                return jsonify(
                    {
                        "ok": True,
                        "printing_pos_patch": _printing_settings_pos_panel_client_payload(ps_after),
                        "printing_compulsory_sale": bool(ps_after.get("print_compulsory_sale")),
                    }
                )
            flash("Settings updated.", "success")
        else:
            if wants_json:
                return jsonify(
                    {"ok": False, "error": "Could not update settings. Check database connection."}
                ), 500
            flash("Could not update settings. Check database connection.", "error")
        return redirect(url_for("it_support_system_settings") + (request.form.get("return_hash") or ""))
    _tab_raw = (request.args.get("tab") or "").strip().lower()
    _tab_q = {"printing": "receipt"}.get(_tab_raw, _tab_raw)
    _valid_tabs = ("system", "company", "website", "pos", "receipt")
    initial_settings_tab = _tab_q if _tab_q in _valid_tabs else None
    from theme_presets import fonts_for_template, google_fonts_url, theme_presets_for_template

    font_ids = [f["id"] for f in fonts_for_template()]
    return render_template(
        "it_support_system_settings.html",
        receipt_settings=_load_receipt_settings(),
        printing_settings=_load_printing_settings(),
        initial_settings_tab=initial_settings_tab,
        appearance_presets=theme_presets_for_template(),
        appearance_fonts=fonts_for_template(),
        appearance_fonts_google_url=google_fonts_url(*font_ids),
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
                }
            item_map[iid]["portions"][sid] = int(r.get("portions_remaining") or 0)

    for data in item_map.values():
        for sid in shop_ids:
            if sid not in data["portions"]:
                data["portions"][sid] = None

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
        for key, raw in request.form.items():
            m = re.match(r"^p_(\d+)_(\d+)$", key)
            if not m:
                continue
            shop_id = int(m.group(1))
            item_id = int(m.group(2))
            shop = _get_shop_or_404(shop_id)
            if _pos_inventory_mode(shop) not in ("kitchen", "both"):
                continue
            try:
                q = int(str(raw or "").strip() or "0")
            except (TypeError, ValueError):
                q = 0
            if upsert_shop_kitchen_portion_qty(shop_id, item_id, q):
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
        if emp:
            flash("You don't have access to this shop.", "error")
            return _redirect_to_employee_dashboard()

    # Shop password sessions (no employee portal): must match branch exactly.
    if int(session.get("shop_id") or 0) != shop_id_int:
        return redirect(url_for("shop_login", shop_id=shop_id_int))

    if shop.get("status") != "active":
        flash("This shop is suspended. Contact IT support.", "error")
        return redirect(url_for("shop_login", shop_id=shop_id_int))

    return None


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
            from database import create_item

            create_item(
                category=category,
                name=name,
                description=description,
                price=price,
                selling_price=selling_price,
                image_path=image_path,
                status="active",
                created_by_employee_id=session.get("employee_id"),
            )
        except Exception:
            flash("Could not register item. Check database connection.", "error")
            return redirect(url_for("it_support_register_item"))

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
        return _it_support_stock_management_post()
    global_mode = _global_pos_inventory_mode()
    is_store_stock = global_mode == "both"
    show_register_stock_item_link = is_store_stock
    items: list = []
    try:
        if is_store_stock:
            from database import (
                init_store_stock_items_table,
                init_store_stock_transactions_table,
                list_store_stock_items_for_management,
            )

            init_store_stock_items_table()
            init_store_stock_transactions_table()
            items = list_store_stock_items_for_management(limit=2000)
        else:
            from database import list_stock_manage_items

            items = list_stock_manage_items(limit=500)
    except Exception:
        items = []
    return render_template(
        "it_support_stock_management.html",
        items=items,
        show_register_stock_item_link=show_register_stock_item_link,
        is_store_stock=is_store_stock,
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

    if request.method == "POST":
        if not both_shops:
            flash("No shops are currently set to Both mode.", "error")
            return redirect(url_for("it_support_register_stock_item"))
        action = (request.form.get("action") or "").strip().lower()
        if action == "create_stock_item":
            cat = (request.form.get("category") or "").strip()
            nm = (request.form.get("name") or "").strip()
            desc = (request.form.get("description") or "").strip()
            measure = (request.form.get("measure_unit") or "").strip().lower()
            if measure not in measure_options:
                flash("Choose a valid measure unit.", "error")
                return redirect(url_for("it_support_register_stock_item"))
            if not cat or not nm:
                flash("Category and stock item name are required.", "error")
                return redirect(url_for("it_support_register_stock_item"))
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
        return redirect(url_for("it_support_register_stock_item"))

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
    try:
        from database import toggle_item_status

        ok = toggle_item_status(item_id)
    except Exception:
        ok = False
    flash("Item status updated." if ok else "Could not update item status.", "success" if ok else "error")
    return redirect(url_for("it_support_item_management"))


@app.route("/it_support/items/<int:item_id>/toggle-stock-update", methods=["POST"])
@login_required
def it_support_item_toggle_stock_update(item_id: int):
    _it_support_only()
    try:
        from database import toggle_stock_update

        ok = toggle_stock_update(item_id)
    except Exception:
        ok = False
    m = _pos_inventory_mode_from_ps(_load_printing_settings())
    if ok:
        if m == "kitchen":
            flash("Kitchen portion update setting updated.", "success")
        elif m == "shop":
            flash("Shop stock update setting updated.", "success")
        elif m == "both":
            flash("Kitchen portion (POS) toggle updated. Shelf stock is managed separately in branch Stock management.", "success")
        else:
            flash("Company POS inventory toggle updated.", "success")
    else:
        flash("Could not update company setting.", "error")
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

    sales = []
    shops = []
    try:
        from database import list_all_shops_credit_sales, list_shops

        shops = list_shops(limit=500) or []
        sales = list_all_shops_credit_sales(
            limit=5000,
            analytics_filter=None if all_time else analytics_filter,
            analytics_scope="general",
            shop_id=filter_shop_id,
            customer_q=customer_q or None,
        )
    except Exception:
        sales, shops = [], []
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
        credit_sales=sales,
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
    shop_id = request.args.get("shop_id", type=int)
    if not shop_id:
        flash("Select a shop customer to view credit payments.", "error")
        return redirect(url_for("it_support_credit_payments"))
    shop = _get_shop_or_404(shop_id)
    customer_name = (request.args.get("customer_name") or "").strip() or "WALK IN"
    customer_phone = (request.args.get("customer_phone") or "").strip() or "-"
    note_ctx = _shop_customer_credit_note_context(
        shop_id, customer_name, customer_phone, analytics_filter=None
    )
    return render_template(
        "it_support_credit_payments_customer.html",
        shop=shop,
        shop_id=shop_id,
        customer_name=customer_name,
        customer_phone=customer_phone,
        embed_record_payment=True,
        credit_payment_return_to="customer",
        **note_ctx,
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

    def _it_support_credit_pay_redirect():
        if return_to == "sale" and sale_id and shop_id:
            return url_for(
                "it_support_credit_sale_detail",
                shop_id=shop_id,
                sale_id=sale_id,
            )
        return url_for(
            "it_support_credit_payments_customer",
            shop_id=shop_id,
            customer_name=customer_name,
            customer_phone=customer_phone,
        )

    try:
        amount = float(amount_raw)
    except Exception:
        flash("Enter a valid amount.", "error")
        return redirect(_it_support_credit_pay_redirect())
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
    return render_template(
        "it_support_credit_sale_detail.html",
        shop=shop,
        shop_id=shop_id,
        sale=sale,
        items=d.get("items") or [],
        customer_name=customer_name,
        customer_phone=customer_phone,
        embed_record_payment=True,
        credit_payment_return_to="sale",
        focus_sale_id=sale_id,
        **note_ctx,
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

        if not shop_name or not shop_code or not shop_password or not shop_location:
            flash("Please fill shop name, shop code, shop password, and shop location.", "error")
            return redirect(url_for("it_support_register_shop"))
        if not CODE_RE.match(shop_code):
            flash("Shop code must be exactly 6 digits.", "error")
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
                created_by_employee_id=session.get("employee_id"),
            )
        except Exception:
            flash("Could not register shop. Check database connection.", "error")
            return redirect(url_for("it_support_register_shop"))

        flash("Shop registered.", "success")
        return redirect(url_for("it_support_register_shop"))

    try:
        from database import list_shops

        shops = list_shops(limit=500)
    except Exception:
        shops = []
    return render_template("it_support_register_shop.html", shops=shops)


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
        from database import update_shop_details

        ok = update_shop_details(
            shop_id=shop_id,
            shop_name=shop_name,
            shop_code=shop_code,
            shop_location=shop_location,
            status=status,
            shop_password_hash=generate_password_hash(shop_password) if shop_password else None,
        )
    except Exception:
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

        session["shop_id"] = int(shop["id"])
        session["shop_name"] = shop.get("shop_name")
        flash(f"Welcome to {shop.get('shop_name')}.", "success")
        return redirect(url_for("shop_pos", shop_id=shop_id))

    return render_template("shop_login.html", shop=shop)


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

    pri_rgb, acc_rgb = _effective_pos_theme_colors(shop)
    return render_template(
        "shop_pos.html",
        shop=shop,
        items=items,
        buy_items_catalog=buy_items_catalog,
        is_store_stock_mode=is_store_stock_mode,
        pos_printing_settings=_effective_printing_settings_for_shop(shop),
        pos_inventory_mode=inventory_mode,
        pos_receipt_settings=_effective_receipt_settings_for_shop(shop),
        theme_key=f"richcom-theme-shop-{shop['id']}",
        theme_default=shop.get("default_theme") or "dark",
        font_family=shop.get("font_family") or "Plus Jakarta Sans",
        primary_color_rgb=pri_rgb,
        accent_color_rgb=acc_rgb,
    )


@app.route("/pos-sw.js")
def pos_service_worker():
    """Serve POS service worker from app root for broad scope."""
    resp = send_from_directory(app.static_folder, "pos-sw.js")
    try:
        resp.headers["Service-Worker-Allowed"] = "/"
        resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    except Exception:
        pass
    return resp


def _pwa_brand_context():
    """Resolve current PWA branding (name, short_name, icon path, colors)."""
    try:
        from database import get_site_settings

        stored = (
            get_site_settings(
                [
                    "company_name",
                    "app_icon",
                    "primary_color",
                    "accent_color",
                    "default_theme",
                ]
            )
            or {}
        )
    except Exception:
        stored = {}
    name = (stored.get("company_name") or "Point of Sale").strip() or "Point of Sale"
    short = name
    if len(short) > 12:
        short = short.split()[0][:12]
    icon_rel = (stored.get("app_icon") or "").strip()
    icon_path = icon_rel if icon_rel else "app-icon.svg"
    primary = (stored.get("primary_color") or "#f97316").strip() or "#f97316"
    accent = (stored.get("accent_color") or "#fb923c").strip() or "#fb923c"
    default_theme = (stored.get("default_theme") or "dark").strip().lower()
    bg = "#0b1220" if default_theme != "light" else "#f8fafc"
    return {
        "name": name,
        "short_name": short,
        "icon_url": url_for("static", filename=icon_path),
        "primary": primary,
        "accent": accent,
        "background_color": bg,
        "theme_color": primary,
    }


@app.route("/manifest.webmanifest")
def pwa_manifest():
    """Dynamic Web App Manifest reflecting current branding."""
    brand = _pwa_brand_context()
    icon_url = brand["icon_url"]
    icon_ext = (icon_url.rsplit(".", 1)[-1] or "").lower().split("?")[0]
    icon_mime = {
        "svg": "image/svg+xml",
        "png": "image/png",
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
        "gif": "image/gif",
        "webp": "image/webp",
        "ico": "image/x-icon",
    }.get(icon_ext, "image/svg+xml")
    icons = [
        {
            "src": url_for("static", filename="icons/app-icon-192.png"),
            "sizes": "192x192",
            "type": "image/png",
            "purpose": "any",
        },
        {
            "src": url_for("static", filename="icons/app-icon-512.png"),
            "sizes": "512x512",
            "type": "image/png",
            "purpose": "any",
        },
        {
            "src": url_for("static", filename="icons/app-icon-192-maskable.png"),
            "sizes": "192x192",
            "type": "image/png",
            "purpose": "maskable",
        },
        {
            "src": url_for("static", filename="icons/app-icon-512-maskable.png"),
            "sizes": "512x512",
            "type": "image/png",
            "purpose": "maskable",
        },
        {
            "src": icon_url,
            "sizes": "any",
            "type": icon_mime,
            "purpose": "any",
        },
        {
            "src": url_for("static", filename="icons/app-icon-monochrome.svg"),
            "sizes": "any",
            "type": "image/svg+xml",
            "purpose": "monochrome",
        },
    ]
    manifest = {
        "name": brand["name"],
        "short_name": brand["short_name"],
        "description": f"{brand['name']} — multi-shop point of sale, stock, and reports.",
        "start_url": "/?source=pwa",
        "scope": "/",
        "display": "standalone",
        "display_override": ["standalone", "minimal-ui"],
        "orientation": "any",
        "background_color": brand["background_color"],
        "theme_color": brand["theme_color"],
        "lang": "en",
        "dir": "ltr",
        "categories": ["business", "productivity", "finance"],
        "icons": icons,
        "shortcuts": [
            {
                "name": "Open POS",
                "short_name": "POS",
                "description": "Jump to your shop point of sale.",
                "url": "/?source=pwa-shortcut-pos",
                "icons": [
                    {
                        "src": url_for("static", filename="icons/app-icon-192.png"),
                        "sizes": "192x192",
                        "type": "image/png",
                    }
                ],
            },
            {
                "name": "Sign in",
                "short_name": "Sign in",
                "description": "Employee or shop sign in.",
                "url": "/?source=pwa-shortcut-signin",
                "icons": [
                    {
                        "src": url_for("static", filename="icons/app-icon-192.png"),
                        "sizes": "192x192",
                        "type": "image/png",
                    }
                ],
            },
        ],
        "prefer_related_applications": False,
    }
    resp = jsonify(manifest)
    resp.headers["Content-Type"] = "application/manifest+json; charset=utf-8"
    resp.headers["Cache-Control"] = "public, max-age=300"
    return resp


@app.route("/offline")
def offline_page():
    """Lightweight offline fallback page served by the service worker."""
    return render_template("offline.html")


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

    is_store_stock_mode = _pos_inventory_mode(shop) == "both"

    item_id_raw = (request.form.get("item_id") or "").strip()
    qty_raw = (request.form.get("qty") or "").strip()
    buying_price_raw = (request.form.get("buying_price") or "").strip()
    seller_phone = (request.form.get("seller_phone") or "").strip()
    seller_name = (request.form.get("seller_name") or "").strip().upper()
    payment_status = "pending_payment"
    note = (request.form.get("note") or "").strip().upper()
    employee_code = (request.form.get("employee_code") or "").strip()

    emp_row, emp_err, emp_status = _shop_pos_validate_employee_code(shop_id, employee_code)
    if emp_err:
        if wants_json:
            return jsonify({"ok": False, "error": emp_err}), emp_status
        flash(emp_err, "error")
        return redirect(url_for("shop_pos", shop_id=shop_id))
    created_by_employee_id = int(emp_row["id"])

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
                        "shop_stock_in_receipt", shop_id=shop_id, tx_id=tx_id, kind="store"
                    )
                else:
                    receipt_url = url_for(
                        "shop_stock_in_receipt", shop_id=shop_id, tx_id=tx_id
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
    try:
        if kind == "store":
            from database import get_shop_store_stock_in_receipt_row

            receipt = get_shop_store_stock_in_receipt_row(shop_id=shop_id, tx_id=tx_id)
        else:
            from database import get_shop_stock_in_receipt_row

            receipt = get_shop_stock_in_receipt_row(shop_id=shop_id, tx_id=tx_id)
    except Exception:
        receipt = None
    if not receipt:
        flash("Stock-in receipt not found.", "error")
        return redirect(url_for("shop_stock_management", shop_id=shop_id, view="manual"))
    return render_template(
        "shop_stock_in_receipt.html",
        shop=shop,
        receipt=receipt,
        pos_receipt_settings=_effective_receipt_settings_for_shop(shop),
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
        )
    except Exception as e:
        logger.exception("shop_pos_incoming_stock_request_review: %s", e)
        ok, err_msg = False, "Something went wrong. Please try again."

    if not ok:
        return jsonify({"ok": False, "error": err_msg or "Could not update this request."}), 400

    return jsonify({"ok": True, "action": action})


def _shop_pos_validate_employee_code(shop_id: int, code: str) -> Tuple[Optional[dict], Optional[str], int]:
    """
    Shared POS rule: 6-digit code, active employee, shop assignment (unless IT/super_admin).
    Returns (employee_row, None, 200) on success, or (None, error_message, http_status) on failure.
    """
    code = (code or "").strip()
    if not re.fullmatch(r"\d{6}", code):
        return None, "Enter a valid 6-digit employee code.", 400

    from database import employee_may_use_shop_branch, get_employee_by_code

    row = get_employee_by_code(code)
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

    from database import employee_may_use_shop_branch, list_employees

    out = []
    try:
        rows = list_employees(limit=5000) or []
    except Exception:
        rows = []
    for row in rows:
        code = str((row or {}).get("employee_code") or "").strip()
        if not re.fullmatch(r"\d{6}", code):
            continue
        status = str((row or {}).get("status") or "").strip().lower()
        if status != "active":
            continue
        role_key = str((row or {}).get("role") or "employee").strip().lower()
        if role_key not in COMPANY_PORTAL_ROLES:
            try:
                if not employee_may_use_shop_branch(dict(row), int(shop_id)):
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
    return jsonify({"ok": True, "quote_id": qid})


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
    quote_lines_by_id = {str(q["id"]): (q.get("lines") or []) for q in quotes}
    return render_template(
        "it_support_leads.html",
        quotes=quotes,
        quote_lines_by_id=quote_lines_by_id,
        leads_filter=filt,
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
    return render_template(
        "shop_dashboard.html",
        shop=shop,
        pos_allow_credit_sale=_shop_pos_allow_credit_sale(shop),
        pos_inventory_mode=_pos_inventory_mode(shop),
        theme_key=f"richcom-theme-shop-{shop['id']}",
        theme_default=shop.get("default_theme") or "dark",
        font_family=shop.get("font_family") or "Plus Jakarta Sans",
        primary_color_rgb=_hex_to_rgb_triplet(shop.get("primary_color") or "#10b981"),
        accent_color_rgb=_hex_to_rgb_triplet(shop.get("accent_color") or "#14b8a6"),
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
        shop_portal=True,
        pos_inventory_mode=_pos_inventory_mode(shop),
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
                upsert_shop_kitchen_portion_qty(shop_id, iid, q)
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
    }


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

    return render_template(
        "shop_item_management.html",
        shop=shop,
        items=items,
        pos_inventory_mode=_pos_inventory_mode(shop),
        theme_key=f"richcom-theme-shop-{shop['id']}",
        theme_default=shop.get("default_theme") or "dark",
        font_family=shop.get("font_family") or "Plus Jakarta Sans",
        primary_color_rgb=_hex_to_rgb_triplet(shop.get("primary_color") or "#10b981"),
        accent_color_rgb=_hex_to_rgb_triplet(shop.get("accent_color") or "#14b8a6"),
    )


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
        view_arg = (request.form.get("view") or request.args.get("view") or "auto").strip().lower()
        if view_arg not in ("auto", "manual"):
            view_arg = "auto"
    else:
        view_arg = (request.args.get("view") or "auto").strip().lower()
        if view_arg not in ("auto", "manual"):
            view_arg = "auto"

    if request.method == "POST":
        action = (request.form.get("action") or "").strip()

        if action == "register_store_items":
            view_arg = (request.form.get("view") or request.args.get("view") or "auto").strip().lower()
            if view_arg not in ("auto", "manual"):
                view_arg = "auto"
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
            view_arg = (request.form.get("view") or request.args.get("view") or "auto").strip().lower()
            if view_arg not in ("auto", "manual"):
                view_arg = "auto"
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
            view_arg = (request.form.get("view") or request.args.get("view") or "auto").strip().lower()
            if view_arg not in ("auto", "manual"):
                view_arg = "auto"
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
                    avail = float((stock_map or {}).get(iid) or 0)
                    if q > avail + 1e-9:
                        fail_count += 1
                        continue
                    per_note = (request.form.get(f"note_{iid}") or "").strip().upper() or None
                    line_note = per_note or batch_note
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
                        "(quantity over available stock at source, invalid numbers, or could not be saved).",
                        "warning",
                    )
                else:
                    flash(
                        "No requests submitted. Enter at least one quantity that does not exceed "
                        "stock at the selected source (company warehouse or other shop).",
                        "error",
                    )
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
        if view_arg != "manual":
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

    return render_template(
        "shop_stock_management.html",
        shop=shop,
        items=items,
        store_reg_candidates=store_reg_candidates or [],
        store_registration_enabled=store_registration_enabled,
        pos_inventory_mode=_pos_inventory_mode(shop),
        other_shops=other_shops,
        view=view_arg,
        stock_requests=request_rows,
        item_stock_requests=request_rows,
        transactions=txs,
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
    if seg in ("out", "manual"):
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
    if role_key in COMPANY_PORTAL_ROLES:
        return True
    # Company-only approvals for return-to-company requests.
    if (row.get("request_type") or "").lower() == "return_to_company":
        return False
    # Shop-to-shop: only the source shop can approve/decline.
    if (row.get("source_type") or "").lower() == "shop":
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
        from database import list_stock_requests_for_session, can_fulfill_stock_request

        stock_requests = list_stock_requests_for_session(
            role_key=role_key,
            viewer_shop_id=None,
            limit=300,
        )
    except Exception:
        stock_requests = []
    viewer_shop_id = _effective_viewer_shop_id(role_key)
    for r in stock_requests or []:
        r["can_review"] = _can_user_review_stock_request(r, role_key=role_key, viewer_shop_id=viewer_shop_id)
        r["can_approve_now"] = can_fulfill_stock_request(int(r.get("id") or 0)) if (r.get("status") == "pending") else False
    return render_template("notifications.html", stock_requests=stock_requests)


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
        )
        stock_requests = list_stock_requests_for_session(
            role_key=role_key,
            viewer_shop_id=shop_id,
            limit=300,
        )
    except Exception:
        rows, stock_requests = [], []
    for r in stock_requests or []:
        r["can_review"] = _can_user_review_stock_request(r, role_key=role_key, viewer_shop_id=shop_id)
        r["can_approve_now"] = can_fulfill_stock_request(int(r.get("id") or 0)) if (r.get("status") == "pending") else False
    return render_template(
        "shop_notifications.html",
        shop=shop,
        notifications=rows,
        stock_requests=stock_requests,
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
        "Stock request approved." if ok else (err_msg or "Could not approve stock request."),
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
        "Stock request rejected." if ok else (err_msg or "Could not reject stock request."),
        "success" if ok else "error",
    )
    if shop_id:
        return redirect(url_for("shop_notifications", shop_id=int(shop_id)))
    return redirect(url_for("notifications"))


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
    return _render_shop_analytics_view(shop_id, "stock")


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

    try:
        from database import ensure_shop_items_for_shop, toggle_shop_item_displayed

        ensure_shop_items_for_shop(shop_id)
        ok = toggle_shop_item_displayed(shop_id=shop_id, item_id=item_id)
    except Exception:
        ok = False

    if ok:
        flash("Shop item display updated.", "success")
    else:
        flash(
            "Could not update shop display — to turn it on, the company item must be active under IT item management.",
            "error",
        )
    return redirect(url_for("shop_item_management", shop_id=shop_id))


@app.route("/shops/<int:shop_id>/shop-item/<int:item_id>/toggle-stock-update", methods=["POST"])
def shop_item_toggle_stock_update_enabled(shop_id: int, item_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate

    try:
        from database import ensure_shop_items_for_shop, toggle_shop_item_stock_update_enabled

        ensure_shop_items_for_shop(shop_id)
        ok = toggle_shop_item_stock_update_enabled(shop_id=shop_id, item_id=item_id)
    except Exception:
        ok = False

    mode = _pos_inventory_mode(shop)
    if ok:
        if mode == "kitchen":
            flash("Kitchen portion update setting updated.", "success")
        elif mode == "shop":
            flash("Shop stock update setting updated.", "success")
        elif mode == "both":
            flash("Kitchen portion (POS) toggle updated. Shelf stock uses Stock management registration separately.", "success")
        else:
            flash("Branch POS inventory toggle updated.", "success")
    elif mode == "shop":
        flash(
            "Stock update toggle was not saved. To turn ON, the company item must be active and company-wide "
            "stock updates must be enabled under IT catalog.",
            "error",
        )
    elif mode == "kitchen":
        flash(
            "Kitchen portion toggle was not saved. To turn ON, the company item must be active and IT must enable "
            "the same master toggle under item management (kitchen mode uses this for portion deductions).",
            "error",
        )
    elif mode == "both":
        flash(
            "Toggle was not saved. To turn ON, the company item must be active, IT must enable the catalog master "
            "toggle, and stock rules must apply (both kitchen portions and shop stock use this toggle).",
            "error",
        )
    else:
        flash(
            "Toggle was not saved. To turn ON, the company item must be active and IT must enable the master POS "
            "inventory toggle under item management.",
            "error",
        )
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

            stock_data = get_shop_stock_analytics(
                shop_id=shop_id, analytics_filter=analytics_filter
            )
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
    allow_credit = _shop_pos_allow_credit_sale(shop)
    return render_template(
        "shop_customer_analytics_detail.html",
        shop=shop,
        customer_name=customer_name,
        customer_phone=customer_phone,
        analytics_filter=analytics_filter,
        analytics_scope=analytics_scope,
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
        "shop_printing_custom": _shop_has_printing_override(shop),
        "shop_receipt_custom": _shop_has_receipt_override(shop),
        "pos_allow_credit_sale": _shop_pos_allow_credit_sale(shop),
        "theme_key": f"richcom-theme-shop-{sid}",
        "theme_default": shop.get("default_theme") or "dark",
        "font_family": shop.get("font_family") or "Plus Jakarta Sans",
        "primary_color_rgb": _hex_to_rgb_triplet(shop.get("primary_color") or "#10b981"),
        "accent_color_rgb": _hex_to_rgb_triplet(shop.get("accent_color") or "#14b8a6"),
    }


@app.route("/shops/<int:shop_id>/shop-settings")
def shop_settings(shop_id: int):
    return redirect(url_for("shop_settings_appearance", shop_id=shop_id), code=302)


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
        updated = 0
        try:
            from database import set_shop_item_stock_alert_levels

            for it in items:
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
            flash("Shop stock settings updated." if updated else "No stock settings changed.", "success")
        except Exception:
            flash("Could not update shop stock settings.", "error")
        return redirect(url_for("shop_stock_settings", shop_id=shop_id))

    return render_template(
        "shop_stock_settings.html",
        shop=shop,
        items=items,
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
        default_theme = (request.form.get("default_theme") or "").strip().lower()
        from theme_presets import normalize_font_family

        font_family = normalize_font_family((request.form.get("font_family") or "").strip())
        primary_color = (request.form.get("primary_color") or "").strip()
        accent_color = (request.form.get("accent_color") or "").strip()
        try:
            from database import update_shop_settings

            ok = update_shop_settings(
                shop["id"],
                default_theme=default_theme,
                font_family=font_family,
                primary_color=primary_color,
                accent_color=accent_color,
                printing_settings_json=shop.get("printing_settings_json"),
                receipt_settings_json=shop.get("receipt_settings_json"),
            )
        except Exception:
            ok = False
        flash("Appearance updated." if ok else "Could not update appearance.", "success" if ok else "error")
        return redirect(url_for("shop_settings_appearance", shop_id=shop["id"]))
    return render_template("shop_settings_appearance.html", shop=shop, **_shop_settings_template_extra(shop))


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
            )
        except Exception:
            ok = False
        flash("POS printing settings updated." if ok else "Could not update POS printing.", "success" if ok else "error")
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
        from database import approve_employee, get_hr_employee_shop_link_mode

        ok = approve_employee(
            emp_id,
            role=role,
            shop_id=shop_id,
            linked_shop_ids=linked_shop_ids if get_hr_employee_shop_link_mode() == "multi" else None,
        )
    except Exception:
        ok = False
    flash("Employee approved." if ok else "Could not approve employee. Check role/shop selection.", "success" if ok else "error")
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
    flash("Employee deleted." if ok else "Could not delete employee (may still be pending approval).", "success" if ok else "error")
    return redirect(url_for("it_support_hr_management"))


@app.route("/it_support/website-management")
@login_required
def it_support_website_management():
    _it_support_only()
    return render_template("it_support_website_management.html")


if __name__ == "__main__":
    # Listen on all interfaces so LAN devices (e.g. phone) can use http://<this-PC-LAN-IP>:5000
    app.run(debug=True, host="0.0.0.0", port=5000)
