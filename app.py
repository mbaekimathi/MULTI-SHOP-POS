import calendar
import hmac
import json
import logging
import os
import re
import secrets
import socket
import uuid
from datetime import date, datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from dotenv import load_dotenv
from flask import (
    Flask,
    Response,
    abort,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    stream_with_context,
    url_for,
)
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename

load_dotenv(Path(__file__).resolve().parent / ".env")

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


def _redirect_to_employee_dashboard():
    """Redirect super_admin / it_support to company portal; others to allocated shop if set."""
    role_key = session.get("employee_role") or "employee"
    if role_key in ("super_admin", "it_support"):
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


# Ensure MySQL database exists, tables are created, and column migrations are applied.
try:
    from database import init_schema

    if not init_schema():
        logger.warning(
            "Database schema initialization did not complete successfully; check MySQL credentials and server logs."
        )
except Exception:
    logger.exception("Database schema initialization failed.")


@app.context_processor
def inject_jos_version():
    return {"jos_version": JOS_VERSION}


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


@app.context_processor
def inject_site_settings():
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
    }
    try:
        from database import get_site_settings

        stored = get_site_settings(list(defaults.keys()))
    except Exception:
        stored = {}
    merged = {**defaults, **{k: v for k, v in (stored or {}).items() if v is not None and v != ""}}
    merged["primary_color_rgb"] = _hex_to_rgb_triplet(merged.get("primary_color"))
    merged["accent_color_rgb"] = _hex_to_rgb_triplet(merged.get("accent_color"))
    return {"site_settings": merged}


@app.context_processor
def inject_portal_context():
    """Nav + portal shell: profile, role, links (DB row when logged in)."""
    uid = session.get("employee_id")
    if not uid:
        return {"nav_employee": None, "portal_employee": None}
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
    return {
        "nav_employee": {
            "name": pe["name"],
            "role_label": pe["role_label"],
            "dashboard_url": pe["dashboard_url"],
        },
        "portal_employee": pe,
    }


@app.context_processor
def inject_notification_context():
    uid = session.get("employee_id")
    if not uid:
        return {"notification_count": 0, "notifications_url": None}
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
    return {
        "notification_count": int(notification_count or 0),
        "notifications_url": notifications_url,
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
    return render_template("index.html")


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


@app.route("/services")
def services():
    return render_template("services.html")


@app.route("/solutions")
def solutions():
    return render_template("solutions.html")


@app.route("/equipment")
def equipment():
    from collections import OrderedDict

    try:
        from database import list_public_equipment_catalog

        catalog_rows = list_public_equipment_catalog(limit_items=500)
    except Exception:
        catalog_rows = []

    by_category: OrderedDict = OrderedDict()
    for row in catalog_rows:
        cat = (row.get("category") or "Other").strip() or "Other"
        if cat not in by_category:
            by_category[cat] = []
        by_category[cat].append(row)
    for cat in list(by_category.keys()):
        by_category[cat].sort(key=lambda r: (-int(r.get("qty_sold") or 0), (r.get("name") or "").upper()))

    sorted_categories = sorted(by_category.keys(), key=lambda c: c.upper())
    catalog_by_category = OrderedDict((c, by_category[c]) for c in sorted_categories)

    featured_items = catalog_rows[:8] if catalog_rows else []

    return render_template(
        "equipment.html",
        catalog_by_category=catalog_by_category,
        featured_items=featured_items,
        catalog_count=len(catalog_rows),
    )


@app.route("/quote")
def quote():
    try:
        from database import list_public_equipment_catalog

        quote_items = list_public_equipment_catalog(limit_items=1000)
    except Exception:
        quote_items = []
    quote_categories = sorted(
        {((r.get("category") or "Other").strip() or "Other") for r in quote_items},
        key=lambda x: x.upper(),
    )
    try:
        from database import get_site_settings

        raw_phone = (get_site_settings(["company_phone"]) or {}).get("company_phone") or ""
    except Exception:
        raw_phone = ""
    quote_whatsapp_digits = re.sub(r"\D", "", str(raw_phone))
    return render_template(
        "quote.html",
        quote_items=quote_items,
        quote_categories=quote_categories,
        quote_whatsapp_digits=quote_whatsapp_digits,
    )


@app.route("/contact", methods=["GET", "POST"])
def contact():
    if request.method == "POST":
        return _handle_contact_post()

    return render_template("contact.html")


def _handle_contact_post():
    name = (request.form.get("name") or "").strip()
    email = (request.form.get("email") or "").strip()
    company = (request.form.get("company") or "").strip()
    message = (request.form.get("message") or "").strip()

    if not name or not email or not message:
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "error": "Please fill name, email, and message."}), 400
        flash("Please fill name, email, and message.", "error")
        return redirect(url_for("contact"))

    try:
        from database import save_contact_message

        save_contact_message(name, email, company, message)
    except Exception:
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": "Could not save your message. Check database settings or try again later.",
                    }
                ),
                503,
            )
        flash("Could not save your message. Please try again later.", "error")
        return redirect(url_for("contact"))

    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return jsonify({"ok": True, "message": "Thanks — we will get back to you shortly."})

    flash("Thanks — we will get back to you shortly.", "success")
    return redirect(url_for("contact"))


@app.route("/login", methods=["GET", "POST"])
def employee_login():
    if session.get("employee_id"):
        return _redirect_to_employee_dashboard()

    if request.method == "POST":
        code = (request.form.get("employee_code") or "").strip()
        password = request.form.get("password") or ""

        if not CODE_RE.match(code) or not password:
            flash("Enter your 6-digit code and password.", "error")
            return redirect(url_for("employee_login"))

        try:
            from database import get_employee_by_code

            row = get_employee_by_code(code)
        except Exception:
            flash("Unable to sign in right now. Try again later.", "error")
            return redirect(url_for("employee_login"))

        if not row or not check_password_hash(row["password_hash"], password):
            flash("Invalid employee code or password.", "error")
            return redirect(url_for("employee_login"))

        status = row["status"]
        if status == "pending_approval":
            flash("Your account is pending approval. You will be notified when it is active.", "warning")
            return redirect(url_for("employee_login"))
        if status == "suspended":
            flash("Your account is suspended. Contact your administrator.", "warning")
            return redirect(url_for("employee_login"))
        if status != "active":
            flash("You cannot sign in with this account.", "warning")
            return redirect(url_for("employee_login"))

        # Replace any shop-password session; employee login owns the session from here.
        session.pop("shop_id", None)
        session.pop("shop_name", None)

        session["employee_id"] = row["id"]
        session["employee_name"] = row["full_name"]
        session["employee_role"] = row.get("role") or "employee"
        role_key = session.get("employee_role") or "employee"
        next_url = (request.form.get("next") or "").strip()

        if role_key in ("super_admin", "it_support"):
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
            return redirect(url_for("employee_login"))

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
            return redirect(url_for("employee_login"))

        session["shop_id"] = int(alloc_shop["id"])
        session["shop_name"] = alloc_shop.get("shop_name")

        if next_url.startswith("/") and not next_url.startswith("//"):
            next_path = urlparse(next_url).path.rstrip("/")
            # If login started from POS, shop staff should land on their shop role page,
            # not be sent back to the POS screen immediately.
            if next_path == f"/shops/{alloc_shop_id}/shop-pos":
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


def _effective_printing_settings_for_shop(shop: dict) -> dict:
    base = _load_printing_settings()
    data = _parse_shop_settings_json(shop.get("printing_settings_json"))
    if not data:
        return base
    merged = {**base, **data}
    for k in (
        "print_compulsory_sale",
        "allow_line_price_edit",
        "printer_allow_bluetooth",
        "printer_allow_network",
        "printer_allow_usb",
    ):
        merged[k] = merged.get(k) in (True, "true", "1", 1, "True")
    rc = str(merged.get("receipt_copies") or "1").strip()
    if rc not in ("1", "2", "3"):
        rc = "1"
    merged["receipt_copies"] = rc
    return merged


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


def _default_printing_settings() -> dict:
    return {
        "print_compulsory_sale": False,
        "allow_line_price_edit": False,
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
    for k in (
        "print_compulsory_sale",
        "allow_line_price_edit",
        "printer_allow_bluetooth",
        "printer_allow_network",
        "printer_allow_usb",
    ):
        merged[k] = merged.get(k) in (True, "true", "1", 1, "True")
    rc = str(merged.get("receipt_copies") or "1").strip()
    if rc not in ("1", "2", "3"):
        rc = "1"
    merged["receipt_copies"] = rc
    return merged


def _printing_settings_from_form() -> dict:
    def _b(key: str) -> bool:
        return (request.form.get(key) or "").strip() == "1"

    rc = (request.form.get("printing_receipt_copies") or "1").strip()
    if rc not in ("1", "2", "3"):
        rc = "1"
    return {
        "print_compulsory_sale": _b("printing_compulsory_sale"),
        "allow_line_price_edit": _b("printing_allow_line_price_edit"),
        "receipt_copies": rc,
        "printer_allow_bluetooth": _b("printing_allow_bluetooth"),
        "printer_allow_network": _b("printing_allow_network"),
        "printer_allow_usb": _b("printing_allow_usb"),
    }


@app.route("/it_support/system-settings", methods=["GET", "POST"])
@login_required
def it_support_system_settings():
    role_key = session.get("employee_role") or "employee"
    if role_key != "it_support":
        abort(403)
    if request.method == "POST":
        company_name = (request.form.get("company_name") or "").strip() or "Point of Sale"
        company_email = (request.form.get("company_email") or "").strip()
        company_phone = (request.form.get("company_phone") or "").strip()
        company_facebook = (request.form.get("company_facebook") or "").strip()
        company_instagram = (request.form.get("company_instagram") or "").strip()
        primary_color = (request.form.get("primary_color") or "#f97316").strip()
        accent_color = (request.form.get("accent_color") or "#fb923c").strip()
        font_family = (request.form.get("font_family") or "Plus Jakarta Sans").strip()
        default_theme = (request.form.get("default_theme") or "dark").strip()
        app_icon_file = request.files.get("app_icon")
        remove_app_icon = (request.form.get("remove_app_icon") or "").strip() == "1"

        def _ok_hex(s: str) -> bool:
            s = (s or "").strip().lstrip("#")
            return len(s) in (3, 6) and all(c in "0123456789abcdefABCDEF" for c in s)

        if not _ok_hex(primary_color):
            primary_color = "#f97316"
        if not _ok_hex(accent_color):
            accent_color = "#fb923c"
        if default_theme not in ("dark", "light", "system"):
            default_theme = "dark"

        app_icon_path = None
        if remove_app_icon:
            app_icon_path = ""
        elif app_icon_file and getattr(app_icon_file, "filename", ""):
            app_icon_path = _save_branding_upload(app_icon_file)
            if app_icon_path is None:
                flash("App icon must be PNG, JPG, GIF, WebP, ICO, or SVG.", "error")
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
                "receipt_settings_json": json.dumps(_receipt_settings_from_form(), separators=(",", ":")),
                "printing_settings_json": json.dumps(_printing_settings_from_form(), separators=(",", ":")),
            }
            if app_icon_path is not None:
                values["app_icon"] = app_icon_path
            ok = set_site_settings(values)
        except Exception:
            ok = False

        if ok:
            flash("Settings updated.", "success")
        else:
            flash("Could not update settings. Check database connection.", "error")
        return redirect(url_for("it_support_system_settings") + (request.form.get("return_hash") or ""))
    return render_template(
        "it_support_system_settings.html",
        receipt_settings=_load_receipt_settings(),
        printing_settings=_load_printing_settings(),
    )


def _it_support_only():
    role_key = session.get("employee_role") or "employee"
    if role_key != "it_support":
        abort(403)


def _it_support_or_super_admin_only():
    role_key = session.get("employee_role") or "employee"
    if role_key not in ("it_support", "super_admin"):
        abort(403)


def _get_shop_or_404(shop_id: int):
    try:
        from database import get_shop_by_id

        shop = get_shop_by_id(shop_id)
    except Exception:
        shop = None
    if not shop:
        abort(404)
    return shop


def _require_shop_access(shop: dict):
    # IT support / super admin can open any shop session view directly.
    if session.get("employee_role") in ("it_support", "super_admin"):
        session["shop_id"] = int(shop["id"])
        session["shop_name"] = shop.get("shop_name")
        return None

    # Shop users must have authenticated session for this exact shop.
    if int(session.get("shop_id") or 0) != int(shop["id"]):
        return redirect(url_for("shop_login", shop_id=shop["id"]))

    if shop.get("status") != "active":
        flash("This shop is suspended. Contact IT support.", "error")
        return redirect(url_for("shop_login", shop_id=shop["id"]))

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
    return render_template("it_support_item_management.html", items=items)


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
        place_brought_from = (request.form.get("place_brought_from") or "").strip().upper()
        stock_out_reason = (request.form.get("stock_out_reason") or "").strip().lower()
        refunded_raw = (request.form.get("refunded") or "").strip().lower()
        refund_amount_raw = (request.form.get("refund_amount") or "").strip()
        note = (request.form.get("note") or "").strip().upper()

        try:
            item_id = int(item_id_raw)
        except Exception:
            flash("Invalid item.", "error")
            return redirect(url_for("it_support_stock_in" if direction == "in" else "it_support_stock_out"))

        try:
            qty = int(float(qty_raw))
        except Exception:
            flash("Quantity must be a whole number.", "error")
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
            if not place_brought_from:
                flash("Place brought from is required for stock in.", "error")
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
            from database import create_stock_transaction

            ok = create_stock_transaction(
                item_id=item_id,
                direction=direction,
                qty=qty,
                buying_price=buying_price,
                place_brought_from=place_brought_from or None,
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
    try:
        from database import create_stock_transactions_batch, list_stock_manage_items

        items = list_stock_manage_items(limit=500)
    except Exception:
        items = []
    if not items:
        flash("No eligible items found.", "error")
        return redirect(url_for("it_support_stock_management"))

    allowed_reasons = {"return", "waste", "display"}
    errors: list[str] = []
    operations: list[dict] = []

    for it in items:
        try:
            iid = int(it.get("id"))
        except Exception:
            continue
        if direction == "in":
            qty_raw = (request.form.get(f"in_qty_{iid}") or "").strip()
            if not qty_raw:
                continue
            try:
                qty = int(float(qty_raw))
            except Exception:
                errors.append(f"{it.get('name') or ('Item #' + str(iid))}: invalid quantity.")
                continue
            if qty <= 0:
                continue
            bp_raw = (request.form.get(f"in_buying_price_{iid}") or "").strip()
            place = (request.form.get(f"in_place_{iid}") or "").strip()
            label = it.get("name") or f"Item #{iid}"
            if not bp_raw:
                errors.append(f"{label}: buying price is required when quantity is set.")
                continue
            if not place:
                errors.append(f"{label}: place bought is required when quantity is set.")
                continue
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
                    "place_brought_from": place.upper(),
                    "stock_out_reason": None,
                    "refunded": False,
                    "refund_amount": None,
                    "note": None,
                }
            )
        else:
            qty_raw = (request.form.get(f"out_qty_{iid}") or "").strip()
            if not qty_raw:
                continue
            try:
                qty = int(float(qty_raw))
            except Exception:
                errors.append(f"{it.get('name') or ('Item #' + str(iid))}: invalid quantity.")
                continue
            if qty <= 0:
                continue
            label = it.get("name") or f"Item #{iid}"
            reason = (request.form.get(f"out_reason_{iid}") or "").strip().lower()
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
                ram = (request.form.get(f"out_refund_amount_{iid}") or "").strip()
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


@app.route("/it_support/item-management/stock-management", methods=["GET", "POST"])
@login_required
def it_support_stock_management():
    """Company stock grid: choose stock in or out, fill rows, submit once."""
    _it_support_only()
    if request.method == "POST":
        return _it_support_stock_management_post()
    try:
        from database import list_stock_manage_items

        items = list_stock_manage_items(limit=500)
    except Exception:
        items = []
    return render_template("it_support_stock_management.html", items=items)


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
    try:
        from database import get_company_stock_status

        shops, stock_rows = get_company_stock_status(limit_items=2000)
    except Exception:
        shops, stock_rows = [], []
    return render_template(
        "it_support_stock_analytics.html",
        analytics_filter=analytics_filter,
        stock_data=stock_data,
        stock_shops=shops,
        stock_rows=stock_rows,
    )


@app.route("/it_support/stock-status/section")
@login_required
def it_support_stock_status():
    _it_support_or_super_admin_only()
    return redirect(url_for("it_support_company_stock_analytics") + "#stock-status")


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
    selected_shop_id = request.args.get("shop_id", type=int)
    try:
        from database import (
            get_company_stock_movement_analytics,
            list_company_stock_movements,
            list_shops,
        )

        shops = list_shops(limit=500)
        movement = get_company_stock_movement_analytics(
            analytics_filter=analytics_filter,
            shop_id=selected_shop_id,
        )
        movement_rows = list_company_stock_movements(
            analytics_filter=analytics_filter,
            shop_id=selected_shop_id,
            limit=1500,
        )
    except Exception:
        shops = []
        movement = {}
        movement_rows = []
    return render_template(
        "it_support_stock_movement_analysis.html",
        analytics_filter=analytics_filter,
        movement_data=movement,
        movement_rows=movement_rows,
        shops=shops,
        selected_shop_id=selected_shop_id,
    )


@app.route("/it_support/stock-profitability-analysis")
@login_required
def it_support_stock_profitability_analysis():
    _it_support_or_super_admin_only()
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
    low_stock_threshold = request.args.get("low_stock_threshold", type=int)
    if low_stock_threshold is None:
        low_stock_threshold = 5
    low_stock_threshold = max(0, min(500, int(low_stock_threshold)))

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
            get_company_stock_status,
            get_it_support_item_analytics,
            get_cursor,
            list_stock_manage_items,
        )

        items = list_stock_manage_items(limit=3000) or []
        _, stock_rows = get_company_stock_status(limit_items=3000)
        item_sales = get_it_support_item_analytics(analytics_filter=analytics_filter)

        stock_map = {}
        for r in stock_rows or []:
            try:
                iid = int(r.get("id") or 0)
            except Exception:
                continue
            if iid > 0:
                stock_map[iid] = int(r.get("total_stock_qty") or 0)

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
            stock_qty = int(stock_map.get(iid, int(it.get("stock_qty") or 0)))

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
            }
            item_rows.append(row)
            total_stock_value += stock_value

            if stock_qty > 0 and qty_sold == 0:
                dead_stock_count += 1
                dead_stock_value += stock_value
            if stock_qty <= 0 and revenue > 0:
                high_value_zero_stock.append(row)
            if stock_qty <= low_stock_threshold:
                low_stock_items.append(row)
            if margin_pct is not None and margin_pct <= 15.0 and qty_sold >= 20:
                low_margin_high_volume.append(row)
            if margin_pct is not None:
                margin_rows.append(row)

        avg_margin_pct = (margin_sum / margin_n) if margin_n else 0.0
        margin_rows.sort(key=lambda r: (r.get("margin_pct") if r.get("margin_pct") is not None else -9999))
        stock_value_rows = sorted(item_rows, key=lambda r: r.get("stock_value") or 0, reverse=True)
        high_value_zero_stock.sort(key=lambda r: r.get("revenue") or 0, reverse=True)
        low_margin_high_volume.sort(key=lambda r: ((r.get("qty_sold") or 0), -(r.get("margin_pct") or 0)), reverse=True)
        low_stock_items.sort(key=lambda r: (r.get("stock_qty") or 0, -(r.get("qty_sold") or 0)))
        top_velocity_items = sorted(item_rows, key=lambda r: r.get("velocity") or 0, reverse=True)
    except Exception:
        item_rows = []

    return render_template(
        "it_support_stock_profitability_analysis.html",
        analytics_filter=analytics_filter,
        selected_view=selected_view,
        low_stock_threshold=low_stock_threshold,
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
    reorder_threshold = request.args.get("reorder_threshold", type=int)
    if reorder_threshold is None:
        reorder_threshold = 5
    reorder_threshold = max(0, min(500, int(reorder_threshold)))

    low_stock_rows = []
    fast_moving_rows = []
    valuation_rows = []
    highest_value_rows = []
    stagnant_rows = []
    total_valuation = 0.0

    try:
        from database import (
            _analytics_where_clause,
            get_company_stock_status,
            get_cursor,
            list_stock_manage_items,
        )

        items = list_stock_manage_items(limit=3000) or []
        _, stock_rows = get_company_stock_status(limit_items=3000)
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

            row = {
                "item_id": iid,
                "name": (it.get("name") or "").strip() or f"Item #{iid}",
                "category": (it.get("category") or "").strip(),
                "stock_qty": stock_qty,
                "tx_count": tx_count,
                "updated_at": it.get("updated_at"),
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
            key=lambda r: ((r.get("tx_count") or 0), str(r.get("updated_at") or "")),
            reverse=True,
        )
        valuation_rows.sort(key=lambda r: (r.get("name") or "").lower())
        highest_value_rows = sorted(valuation_rows, key=lambda r: r.get("stock_value") or 0, reverse=True)
        low_stock_rows.sort(key=lambda r: (r.get("stock_qty") or 0, -(r.get("tx_count") or 0)))
        stagnant_rows.sort(key=lambda r: (r.get("stock_value") or 0), reverse=True)
    except Exception:
        pass

    return render_template(
        "it_support_stock_reports.html",
        analytics_filter=analytics_filter,
        selected_view=selected_view,
        reorder_threshold=reorder_threshold,
        total_valuation=total_valuation,
        low_stock_rows=low_stock_rows[:120],
        fast_moving_rows=fast_moving_rows[:120],
        valuation_rows=valuation_rows[:300],
        highest_value_rows=highest_value_rows[:120],
        stagnant_rows=stagnant_rows[:120],
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


@app.route("/it_support/stock-reports/suppliers")
@login_required
def it_support_stock_suppliers():
    _it_support_or_super_admin_only()
    try:
        from database import get_company_item_supplier_summary

        rows = get_company_item_supplier_summary(limit_items=2500) or []
    except Exception:
        rows = []
    return render_template("it_support_stock_suppliers.html", supplier_rows=rows)


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
    try:
        from database import init_items_table, list_items_for_company_stock_settings, set_item_stock_alert_levels

        init_items_table()
        items = list_items_for_company_stock_settings(limit=8000) or []
    except Exception:
        items = []

    if request.method == "POST":
        allowed_ids = sorted({int(i.get("id") or 0) for i in items if i.get("id")} - {0})
        ok = True
        for iid in allowed_ids:
            try:
                low_v = int(float((request.form.get(f"low_stock_{iid}") or "0").strip() or 0))
                rl_v = int(float((request.form.get(f"reorder_level_{iid}") or "0").strip() or 0))
            except Exception:
                low_v, rl_v = 0, 0
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
    try:
        from database import get_company_stock_status, list_shops, list_stock_manage_items

        shops = list_shops(limit=500)
        items = list_stock_manage_items(limit=2000)
        _, stock_rows = get_company_stock_status(limit_items=2000)
    except Exception:
        shops, items, stock_rows = [], [], []

    if request.method == "POST":
        action = (request.form.get("action") or "").strip().lower()
        note = (request.form.get("note") or "").strip().upper() or None

        item_ids = request.form.getlist("item_id[]")
        qtys = request.form.getlist("qty[]")

        lines = []
        for i in range(min(len(item_ids), len(qtys), 200)):
            iid_raw = (item_ids[i] or "").strip()
            qty_raw = (qtys[i] or "").strip()
            if not iid_raw or not qty_raw:
                continue
            try:
                iid = int(iid_raw)
                q = int(float(qty_raw))
            except Exception:
                continue
            if q <= 0:
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
    )


@app.route("/it_support/item-management/item-analytics")
@login_required
def it_support_item_analytics():
    _it_support_only()
    return render_template("it_support_item_analytics.html")


@app.route("/it_support/item-management/item-audit")
@login_required
def it_support_item_audit():
    _it_support_only()
    return render_template("it_support_item_audit.html")


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
    flash(
        "Stock update setting updated." if ok else "Could not update stock setting.",
        "success" if ok else "error",
    )
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

            revenue_data = get_it_support_revenue_analytics(analytics_filter=analytics_filter)
        except Exception:
            revenue_data = {
                "tx_count": 0,
                "sale_amount": 0.0,
                "credit_amount": 0.0,
                "total_amount": 0.0,
                "shops": [],
                "daily": [],
                "transactions": [],
            }
    if view_key == "item":
        try:
            from database import get_it_support_item_analytics

            item_data = get_it_support_item_analytics(analytics_filter=analytics_filter)
        except Exception:
            item_data = {
                "total_qty": 0,
                "total_revenue": 0.0,
                "line_count": 0,
                "distinct_items": 0,
                "top_items": [],
                "shops": [],
                "lines": [],
            }
    if view_key == "period":
        try:
            from database import get_it_support_period_analytics

            period_data = get_it_support_period_analytics(analytics_filter=analytics_filter)
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

            employee_data = get_it_support_employee_analytics(analytics_filter=analytics_filter)
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

            sales_data = get_it_support_sales_analytics(analytics_filter=analytics_filter)
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

            credit_data = get_it_support_credit_analytics(analytics_filter=analytics_filter)
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

            customer_data = get_it_support_customer_analytics(analytics_filter=analytics_filter)
        except Exception:
            customer_data = {
                "total_tx_count": 0,
                "total_amount": 0.0,
                "distinct_customers": 0,
                "customers": [],
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
                        shop_id=selected_shop_id, analytics_filter=analytics_filter
                    )
                elif shop_view == "sales":
                    shop_view_data = get_shop_sales_analytics(
                        shop_id=selected_shop_id, analytics_filter=analytics_filter
                    )
                elif shop_view == "credit":
                    shop_view_data = get_shop_credit_analytics(
                        shop_id=selected_shop_id, analytics_filter=analytics_filter
                    )
                elif shop_view == "period":
                    shop_view_data = get_shop_period_analytics(
                        shop_id=selected_shop_id, analytics_filter=analytics_filter
                    )
                elif shop_view == "stock":
                    shop_view_data = get_shop_stock_analytics(
                        shop_id=selected_shop_id, analytics_filter=analytics_filter
                    )
                elif shop_view == "customer":
                    shop_view_data = get_shop_customer_analytics(
                        shop_id=selected_shop_id, analytics_filter=analytics_filter
                    )
                else:
                    shop_view_data = get_shop_revenue_analytics(
                        shop_id=selected_shop_id, analytics_filter=analytics_filter
                    )
            except Exception:
                shop_view_data = None
    return render_template(
        "it_support_analytics_page.html",
        analytics_key=view_key,
        analytics_title=labels[view_key],
        analytics_filter=analytics_filter,
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
    )


@app.route("/it_support/analytics/revenue")
@login_required
def it_support_revenue_analytics():
    return _render_it_support_analytics_page("revenue")


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
    try:
        from database import (
            get_it_support_customer_detail_analytics,
            get_it_support_customer_transactions,
        )

        txs = get_it_support_customer_transactions(
            customer_name=customer_name,
            customer_phone=customer_phone,
            limit=3000,
            analytics_filter=analytics_filter,
            shop_id=scoped_shop_id,
        )
        analytics = get_it_support_customer_detail_analytics(
            customer_name=customer_name,
            customer_phone=customer_phone,
            analytics_filter=analytics_filter,
            shop_id=scoped_shop_id,
        )
    except Exception:
        txs = []
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
        customer_analytics=analytics,
        transactions=txs,
    )


@app.route("/it_support/credit-payments")
@login_required
def it_support_credit_payments():
    _it_support_or_super_admin_only()
    try:
        from database import list_all_shops_credit_customers_with_balance

        customers = list_all_shops_credit_customers_with_balance(limit=5000)
    except Exception:
        customers = []
    return render_template("it_support_credit_payments.html", customers=customers)


@app.route("/it_support/credit-payments/customer")
@login_required
def it_support_credit_payments_customer():
    _it_support_or_super_admin_only()
    shop_id = request.args.get("shop_id", type=int)
    if not shop_id:
        flash("Select a shop customer to view credit payments.", "error")
        return redirect(url_for("it_support_credit_payments"))
    customer_name = (request.args.get("customer_name") or "").strip() or "WALK IN"
    customer_phone = (request.args.get("customer_phone") or "").strip() or "-"
    try:
        from database import get_shop_customer_credit_transactions

        txs = get_shop_customer_credit_transactions(
            shop_id=shop_id, customer_name=customer_name, customer_phone=customer_phone, limit=3000
        )
    except Exception:
        txs = []
    total_credit = sum(float(t.get("total_amount") or 0) for t in txs)
    total_paid = sum(float(t.get("paid_amount") or 0) for t in txs)
    total_due = max(total_credit - total_paid, 0.0)
    return render_template(
        "it_support_credit_payments_customer.html",
        shop_id=shop_id,
        customer_name=customer_name,
        customer_phone=customer_phone,
        transactions=txs,
        total_credit=total_credit,
        total_paid=total_paid,
        total_due=total_due,
    )


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
    amount_raw = (request.form.get("amount") or "").strip()
    note = (request.form.get("note") or "").strip() or None
    try:
        amount = float(amount_raw)
    except Exception:
        flash("Enter a valid amount.", "error")
        return redirect(
            url_for(
                "it_support_credit_payments_customer",
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
    return redirect(
        url_for(
            "it_support_credit_payments_customer",
            shop_id=shop_id,
            customer_name=customer_name,
            customer_phone=customer_phone,
        )
    )


@app.route("/it_support/credit-payments/sale")
@login_required
def it_support_credit_sale_detail():
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
    return render_template(
        "it_support_credit_sale_detail.html",
        shop_id=shop_id,
        sale=d["sale"],
        items=d.get("items") or [],
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

    After sign-out, users are redirected to the site home page (``/``, ``index``).
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
    return redirect(url_for("index"), code=303)


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


@app.route("/shops/<int:shop_id>/shop-pos")
def shop_pos(shop_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    try:
        from database import ensure_shop_items_for_shop, list_shop_pos_items

        ensure_shop_items_for_shop(shop_id)
        items = list_shop_pos_items(shop_id=shop_id, limit=2000)
    except Exception:
        items = []
    pri_rgb, acc_rgb = _effective_pos_theme_colors(shop)
    return render_template(
        "shop_pos.html",
        shop=shop,
        items=items,
        pos_printing_settings=_effective_printing_settings_for_shop(shop),
        pos_receipt_settings=_effective_receipt_settings_for_shop(shop),
        theme_key=f"richcom-theme-shop-{shop['id']}",
        theme_default=shop.get("default_theme") or "dark",
        font_family=shop.get("font_family") or "Plus Jakarta Sans",
        primary_color_rgb=pri_rgb,
        accent_color_rgb=acc_rgb,
    )


@app.route("/shops/<int:shop_id>/shop-pos/catalog.json")
def shop_pos_catalog_json(shop_id: int):
    """Lightweight catalog snapshot for live stock/price refresh without reloading the page."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    try:
        from database import ensure_shop_items_for_shop, list_shop_pos_items

        ensure_shop_items_for_shop(shop_id)
        rows = list_shop_pos_items(shop_id=shop_id, limit=2000)
    except Exception:
        rows = []
    items = []
    for it in rows:
        try:
            price = float(it.get("price") or 0)
        except (TypeError, ValueError):
            price = 0.0
        try:
            orig = float(it.get("original_selling_price") or 0)
        except (TypeError, ValueError):
            orig = 0.0
        items.append(
            {
                "id": int(it.get("id") or 0),
                "shop_stock_qty": int(it.get("shop_stock_qty") or 0),
                "price": round(price, 2),
                "original_selling_price": round(orig, 2),
            }
        )
    return jsonify({"ok": True, "items": items})


@app.route("/shops/<int:shop_id>/shop-pos/authorize-employee", methods=["POST"])
def shop_pos_authorize_employee(shop_id: int):
    """Validate 6-digit employee code for POS authorization."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate

    data = request.get_json(force=True, silent=True) or {}
    code = (data.get("employee_code") or "").strip()
    if not re.fullmatch(r"\d{6}", code):
        return jsonify({"ok": False, "error": "Enter a valid 6-digit employee code."}), 400

    from database import get_employee_by_code

    row = get_employee_by_code(code)
    if not row:
        return jsonify({"ok": False, "error": "Employee code not registered. Try another code."}), 404

    if (row.get("status") or "").lower() != "active":
        return jsonify({"ok": False, "error": "Employee is not active. Enter another active employee code."}), 403

    role_key = (row.get("role") or "employee").lower()
    try:
        emp_shop_id = int(row.get("shop_id")) if row.get("shop_id") is not None else None
    except (TypeError, ValueError):
        emp_shop_id = None
    if role_key not in ("super_admin", "it_support") and emp_shop_id != int(shop_id):
        return jsonify({"ok": False, "error": "Employee is not assigned to this shop. Enter another code."}), 403

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
    sale_type = (data.get("sale_type") or "sale").strip().lower()
    if sale_type not in ("sale", "credit"):
        return jsonify({"ok": False, "error": "Invalid sale type."}), 400

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
    credit_due_raw = (data.get("credit_due_date") or "").strip() or None
    if sale_type != "credit":
        credit_due_raw = None
    try:
        from database import create_shop_pos_sale

        ok, sale_err = create_shop_pos_sale(
            shop_id=shop_id,
            sale_type=sale_type,
            total_amount=total_amount,
            item_count=item_count,
            customer_name=(data.get("customer_name") or "").strip() or None,
            customer_phone=(data.get("customer_phone") or "").strip() or None,
            employee_id=emp.get("id"),
            employee_code=(emp.get("employee_code") or "").strip() or None,
            employee_name=(emp.get("full_name") or "").strip() or None,
            credit_due_date=credit_due_raw,
            lines=data.get("lines") if isinstance(data.get("lines"), list) else [],
        )
    except Exception:
        ok, sale_err = False, None
    if not ok:
        msg = sale_err or "Could not record sale."
        status = 400 if sale_err else 500
        return jsonify({"ok": False, "error": msg}), status
    return jsonify({"ok": True})


@app.route("/shops/<int:shop_id>/shop-pos/record-quote", methods=["POST"])
def shop_pos_record_quote(shop_id: int):
    """Save a POS quotation (lead): no sale row, no stock movement. Used before printing a quote receipt."""
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate

    data = request.get_json(force=True, silent=True) or {}
    quote_basis = (data.get("quote_basis") or "sale").strip().lower()
    if quote_basis not in ("sale", "credit"):
        return jsonify({"ok": False, "error": "Invalid quotation type."}), 400

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


@app.route("/quote/submit", methods=["POST"])
def public_quote_submit():
    """Persist website /quote cart as an online lead (no shop branch, no stock change)."""
    data = request.get_json(force=True, silent=True) or {}
    lines_in = data.get("lines") if isinstance(data.get("lines"), list) else []
    if not lines_in:
        return jsonify({"ok": False, "error": "No items selected."}), 400
    if len(lines_in) > 80:
        return jsonify({"ok": False, "error": "Too many items."}), 400
    lines = []
    total_qty = 0
    total_amount = 0.0
    for ln in lines_in[:80]:
        if not isinstance(ln, dict):
            continue
        nm = (ln.get("name") or "").strip()
        if not nm:
            continue
        try:
            qty = int(ln.get("qty") or 1)
        except (TypeError, ValueError):
            qty = 1
        if qty < 1:
            qty = 1
        if qty > 999:
            qty = 999
        try:
            unit = float(ln.get("price") or 0)
        except (TypeError, ValueError):
            unit = 0.0
        lt = ln.get("total")
        try:
            lt = float(lt) if lt is not None else unit * qty
        except (TypeError, ValueError):
            lt = unit * qty
        iid = ln.get("id")
        try:
            iid = int(iid) if iid is not None else None
        except (TypeError, ValueError):
            iid = None
        lines.append({"id": iid, "name": nm[:200], "qty": qty, "price": unit, "total": lt})
        total_qty += qty
        total_amount += lt
    if not lines:
        return jsonify({"ok": False, "error": "No valid line items."}), 400

    raw_phone = (data.get("customer_phone") or "").strip()
    digits = re.sub(r"\D", "", raw_phone)
    customer_phone = raw_phone.strip()[:40] if raw_phone else None
    customer_name = (data.get("customer_name") or "").strip()[:190] if data.get("customer_name") else ""
    # Phone-first registration: if user provided at least 10 digits, require a name and upsert customer.
    if digits and len(digits) >= 10:
        if len(customer_name) < 2:
            return jsonify({"ok": False, "error": "Enter your name to register this phone number."}), 400
        try:
            from database import upsert_public_customer

            upsert_public_customer(customer_name, customer_phone or digits)
        except Exception:
            pass
    try:
        from database import create_shop_pos_quotation

        qid, err = create_shop_pos_quotation(
            shop_id=None,
            quote_basis="sale",
            quote_channel="online",
            total_amount=total_amount,
            item_count=total_qty,
            customer_name=customer_name or None,
            customer_phone=customer_phone or None,
            employee_id=None,
            employee_code=None,
            employee_name=None,
            lines=lines,
        )
    except Exception:
        qid, err = None, None
    if not qid:
        msg = err or "Could not save quotation."
        status = 400 if err else 500
        return jsonify({"ok": False, "error": msg}), status
    return jsonify({"ok": True, "quote_id": qid})


@app.route("/quote/customer-lookup", methods=["POST"])
def public_quote_customer_lookup():
    data = request.get_json(force=True, silent=True) or {}
    phone = (data.get("phone") or "").strip()
    digits = re.sub(r"\D", "", phone)
    if len(digits) < 10:
        return jsonify({"ok": False, "error": "Enter a valid phone number."}), 400
    try:
        from database import get_public_customer_by_phone

        row = get_public_customer_by_phone(phone)
    except Exception:
        row = None
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
        return jsonify(
            {
                "ok": True,
                "shop_id": shop_id,
                "printer": {
                    "shop_id": shop_id,
                    "printer_type": row["printer_type"],
                    "device_label": row["device_label"],
                    "config": cfg,
                    "updated_at": updated.isoformat() if hasattr(updated, "isoformat") else None,
                },
            }
        )

    data = request.get_json(force=True, silent=True) or {}
    if data.get("clear"):
        delete_shop_printer_settings(shop_id)
        return jsonify({"ok": True, "shop_id": shop_id, "printer": None})

    pt = (data.get("printer_type") or "").strip().lower()
    if pt not in ("bluetooth", "network", "usb"):
        return jsonify({"ok": False, "error": "Invalid printer_type"}), 400
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


@app.route("/shops/<int:shop_id>/shop-dashboard")
def shop_dashboard(shop_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    return render_template(
        "shop_dashboard.html",
        shop=shop,
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
    try:
        from database import list_shop_credit_customers_with_balance

        customers = list_shop_credit_customers_with_balance(shop_id=shop_id, limit=2000)
    except Exception:
        customers = []
    return render_template(
        "shop_credit_payments.html",
        shop=shop,
        customers=customers,
        theme_key=f"richcom-theme-shop-{shop['id']}",
        theme_default=shop.get("default_theme") or "dark",
        font_family=shop.get("font_family") or "Plus Jakarta Sans",
        primary_color_rgb=_hex_to_rgb_triplet(shop.get("primary_color") or "#10b981"),
        accent_color_rgb=_hex_to_rgb_triplet(shop.get("accent_color") or "#14b8a6"),
    )


@app.route("/shops/<int:shop_id>/shop-credit-payments/customer")
def shop_credit_payments_customer(shop_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    customer_name = (request.args.get("customer_name") or "").strip() or "WALK IN"
    customer_phone = (request.args.get("customer_phone") or "").strip() or "-"
    try:
        from database import get_shop_customer_credit_transactions

        txs = get_shop_customer_credit_transactions(
            shop_id=shop_id, customer_name=customer_name, customer_phone=customer_phone, limit=3000
        )
    except Exception:
        txs = []
    total_credit = sum(float(t.get("total_amount") or 0) for t in txs)
    total_paid = sum(float(t.get("paid_amount") or 0) for t in txs)
    total_due = max(total_credit - total_paid, 0.0)
    return render_template(
        "shop_credit_payments_customer.html",
        shop=shop,
        customer_name=customer_name,
        customer_phone=customer_phone,
        transactions=txs,
        total_credit=total_credit,
        total_paid=total_paid,
        total_due=total_due,
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
    return redirect(
        url_for(
            "shop_credit_payments_customer",
            shop_id=shop_id,
            customer_name=customer_name,
            customer_phone=customer_phone,
        )
    )


@app.route("/shops/<int:shop_id>/shop-credit-payments/sale/<int:sale_id>")
def shop_credit_sale_detail(shop_id: int, sale_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
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

    mode = (mode or request.args.get("mode") or "in").strip().lower()
    if mode not in ("in", "out"):
        mode = "in"

    if request.method == "POST":
        action = (request.form.get("action") or "").strip()
        item_id_raw = (request.form.get("item_id") or "").strip()
        qty_raw = (request.form.get("qty") or "").strip()
        buying_price_raw = (request.form.get("buying_price") or "").strip()
        place_brought_from = (request.form.get("place_brought_from") or "").strip().upper()
        reason = (request.form.get("reason") or "").strip().lower()
        note = (request.form.get("note") or "").strip().upper()

        try:
            item_id = int(item_id_raw)
        except Exception:
            flash("Invalid item.", "error")
            return redirect(url_for("shop_stock_management", shop_id=shop_id))

        try:
            qty = int(float(qty_raw))
        except Exception:
            flash("Quantity must be a whole number.", "error")
            return redirect(url_for("shop_stock_management", shop_id=shop_id, item_id=item_id))

        try:
            from database import (
                ensure_shop_items_for_shop,
                create_notification,
                create_shop_stock_request,
                shop_manual_stock_in,
                shop_manual_stock_out,
                shop_request_stock_from_company,
                shop_return_stock_to_company,
            )

            ensure_shop_items_for_shop(shop_id)

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
                    flash("Could not create stock request. Check source and stock availability.", "error")
                else:
                    create_notification(
                        title="New stock request",
                        message=f"Shop #{shop_id} requested item #{item_id} qty {qty}.",
                        shop_id=shop_id,
                        audience_role="admin_only",
                        link_url=url_for("notifications"),
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
                    create_notification(
                        title="New return request",
                        message=f"Shop #{shop_id} requested return of item #{item_id} qty {qty}.",
                        shop_id=shop_id,
                        audience_role="admin_only",
                        link_url=url_for("notifications"),
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
                ok = shop_manual_stock_in(
                    shop_id=shop_id,
                    item_id=item_id,
                    qty=qty,
                    buying_price=buying_price_raw,
                    place_brought_from=place_brought_from,
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

        if item_id:
            return redirect(url_for("shop_stock_management_item", shop_id=shop_id, mode=mode, item_id=item_id))
        return redirect(url_for("shop_stock_management", shop_id=shop_id, mode=mode))

    selected_item_id = item_id or request.args.get("item_id", type=int)
    try:
        from database import (
            ensure_shop_items_for_shop,
            list_shop_stock_manage_items,
            list_shop_stock_transactions,
            list_shops,
            list_stock_requests_for_session,
            get_shop_stock_qty_map_for_item,
        )

        ensure_shop_items_for_shop(shop_id)
        items = list_shop_stock_manage_items(shop_id=shop_id, limit=500)
        txs = list_shop_stock_transactions(shop_id=shop_id, item_id=selected_item_id, limit=200) if selected_item_id else []
        selected_item = None
        if selected_item_id and items:
            selected_item = next((it for it in items if int(it.get("id", 0)) == int(selected_item_id)), None)
        source_shops = [s for s in (list_shops(limit=500) or []) if int(s.get("id") or 0) != int(shop_id)]
        shop_qty_map = get_shop_stock_qty_map_for_item(selected_item_id) if selected_item_id else {}
        source_targets = [
            {
                "value": "company",
                "label": "Company",
                "qty_left": int((selected_item or {}).get("company_stock_qty") or 0),
            }
        ]
        for s in source_shops:
            sid = int(s.get("id") or 0)
            source_targets.append(
                {
                    "value": f"shop:{sid}",
                    "label": s.get("shop_name") or f"Shop #{sid}",
                    "qty_left": int(shop_qty_map.get(sid) or 0),
                }
            )
        request_rows = list_stock_requests_for_session(
            role_key=(session.get("employee_role") or "employee"),
            viewer_shop_id=shop_id,
            limit=200,
        )
        item_request_rows = (
            [r for r in (request_rows or []) if int(r.get("item_id") or 0) == int(selected_item_id)]
            if selected_item_id
            else (request_rows or [])
        )
    except Exception:
        items, txs, selected_item, source_shops, source_targets, request_rows, item_request_rows = [], [], None, [], [], [], []

    return render_template(
        "shop_stock_management.html",
        shop=shop,
        items=items,
        selected_item_id=selected_item_id,
        selected_item=selected_item,
        item_page=(request.endpoint == "shop_stock_management_item"),
        source_shops=source_shops,
        source_targets=source_targets,
        stock_requests=request_rows,
        item_stock_requests=item_request_rows,
        mode=mode,
        transactions=txs,
        theme_key=f"richcom-theme-shop-{shop['id']}",
        theme_default=shop.get("default_theme") or "dark",
        font_family=shop.get("font_family") or "Plus Jakarta Sans",
        primary_color_rgb=_hex_to_rgb_triplet(shop.get("primary_color") or "#10b981"),
        accent_color_rgb=_hex_to_rgb_triplet(shop.get("accent_color") or "#14b8a6"),
    )


@app.route(
    "/shops/<int:shop_id>/shop-stock-management/<string:mode>/<int:item_id>",
    methods=["GET"],
    endpoint="shop_stock_management_item",
)
def shop_stock_management_item(shop_id: int, mode: str, item_id: int):
    return shop_stock_management(shop_id=shop_id, mode=mode, item_id=item_id)


def _can_user_review_stock_request(row: dict, *, role_key: str, viewer_shop_id: int | None) -> bool:
    role_key = (role_key or "employee").strip().lower()
    if role_key in ("it_support", "super_admin"):
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
    if role_key in ("it_support", "super_admin"):
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

        rows = list_notifications_for_session(
            employee_id=session.get("employee_id"),
            shop_id=None,
            role_key=role_key,
            limit=300,
        )
        stock_requests = list_stock_requests_for_session(
            role_key=role_key,
            viewer_shop_id=None,
            limit=300,
        )
    except Exception:
        rows, stock_requests = [], []
    viewer_shop_id = _effective_viewer_shop_id(role_key)
    for r in stock_requests or []:
        r["can_review"] = _can_user_review_stock_request(r, role_key=role_key, viewer_shop_id=viewer_shop_id)
        r["can_approve_now"] = can_fulfill_stock_request(int(r.get("id") or 0)) if (r.get("status") == "pending") else False
    return render_template("notifications.html", notifications=rows, stock_requests=stock_requests)


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

        ok = review_stock_request(
            request_id=request_id,
            approve=True,
            approver_employee_id=session.get("employee_id"),
            approver_role=role_key,
            approver_shop_id=int(shop_id) if shop_id else None,
            review_note=(request.form.get("review_note") or "").strip() or None,
        )
    except Exception:
        ok = False
    flash("Stock request approved." if ok else "Could not approve stock request.", "success" if ok else "error")
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

        ok = review_stock_request(
            request_id=request_id,
            approve=False,
            approver_employee_id=session.get("employee_id"),
            approver_role=role_key,
            approver_shop_id=int(shop_id) if shop_id else None,
            review_note=(request.form.get("review_note") or "").strip() or None,
        )
    except Exception:
        ok = False
    flash("Stock request rejected." if ok else "Could not reject stock request.", "success" if ok else "error")
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
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    analytics_filter = _build_analytics_filter()
    reorder_threshold = request.args.get("reorder_threshold", type=int)
    if reorder_threshold is None:
        reorder_threshold = 5
    reorder_threshold = max(0, min(500, int(reorder_threshold)))
    stock_data: Dict[str, Any] = {
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

        stock_data = get_shop_stock_analytics(shop_id=shop_id, analytics_filter=analytics_filter)
        sku_count = _shop_active_sku_count(shop_id)
    except Exception:
        pass
    return render_template(
        "shop_stock_analytics.html",
        shop=shop,
        analytics_filter=analytics_filter,
        stock_data=stock_data,
        shop_stock_sidebar_focus="analytics",
        sku_count=sku_count,
        reorder_threshold=reorder_threshold,
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
    try:
        from database import list_shop_stock_audit_rows

        txs = list_shop_stock_audit_rows(shop_id=shop_id, limit=2000)
    except Exception:
        txs = []
    return render_template(
        "shop_stock_audits.html",
        shop=shop,
        transactions=txs,
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

    flash("Shop item display updated." if ok else "Could not update shop item display.", "success" if ok else "error")
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

    flash("Shop stock update setting updated." if ok else "Could not update shop stock setting.", "success" if ok else "error")
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
    analytics_filter = _build_analytics_filter()
    revenue_data = None
    if analytics_view == "revenue":
        try:
            from database import get_shop_revenue_analytics

            revenue_data = get_shop_revenue_analytics(shop_id=shop_id, analytics_filter=analytics_filter)
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

            item_data = get_shop_item_analytics(shop_id=shop_id, analytics_filter=analytics_filter)
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

            period_data = get_shop_period_analytics(shop_id=shop_id, analytics_filter=analytics_filter)
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

            sales_data = get_shop_sales_analytics(shop_id=shop_id, analytics_filter=analytics_filter)
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

            credit_data = get_shop_credit_analytics(shop_id=shop_id, analytics_filter=analytics_filter)
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

            customer_data = get_shop_customer_analytics(shop_id=shop_id, analytics_filter=analytics_filter)
        except Exception:
            customer_data = {
                "total_tx_count": 0,
                "total_amount": 0.0,
                "distinct_customers": 0,
                "customers": [],
            }
    return render_template(
        "shop_analytics.html",
        shop=shop,
        analytics_view=analytics_view,
        analytics_filter=analytics_filter,
        revenue_data=revenue_data,
        item_data=item_data,
        period_data=period_data,
        sales_data=sales_data,
        credit_data=credit_data,
        customer_data=customer_data,
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


@app.route("/shops/<int:shop_id>/shop-customer-analytics/detail")
def shop_customer_analytics_detail(shop_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    customer_name = (request.args.get("customer_name") or "").strip() or "WALK IN"
    customer_phone = (request.args.get("customer_phone") or "").strip() or "-"
    analytics_filter = _build_analytics_filter()
    try:
        from database import (
            get_it_support_customer_detail_analytics,
            get_it_support_customer_transactions,
        )

        customer_analytics = get_it_support_customer_detail_analytics(
            customer_name=customer_name,
            customer_phone=customer_phone,
            analytics_filter=analytics_filter,
            shop_id=shop_id,
        )
        transactions = get_it_support_customer_transactions(
            customer_name=customer_name,
            customer_phone=customer_phone,
            analytics_filter=analytics_filter,
            shop_id=shop_id,
            limit=3000,
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
            "shops": [],
            "employees": [],
            "top_items": [],
        }
        transactions = []
    return render_template(
        "shop_customer_analytics_detail.html",
        shop=shop,
        customer_name=customer_name,
        customer_phone=customer_phone,
        analytics_filter=analytics_filter,
        customer_analytics=customer_analytics,
        transactions=transactions,
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
        "theme_key": f"richcom-theme-shop-{sid}",
        "theme_default": shop.get("default_theme") or "dark",
        "font_family": shop.get("font_family") or "Plus Jakarta Sans",
        "primary_color_rgb": _hex_to_rgb_triplet(shop.get("primary_color") or "#10b981"),
        "accent_color_rgb": _hex_to_rgb_triplet(shop.get("accent_color") or "#14b8a6"),
    }


@app.route("/shops/<int:shop_id>/shop-settings")
def shop_settings(shop_id: int):
    return redirect(url_for("shop_settings_appearance", shop_id=shop_id), code=302)


@app.route("/shops/<int:shop_id>/shop-settings/appearance", methods=["GET", "POST"])
def shop_settings_appearance(shop_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    if request.method == "POST":
        default_theme = (request.form.get("default_theme") or "").strip().lower()
        font_family = (request.form.get("font_family") or "").strip()
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
        printing_json = json.dumps(_printing_settings_from_form(), separators=(",", ":")) if use_custom_printing else None
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
        from database import list_employees, list_shops

        employees = list_employees(limit=2000)
        shops = list_shops(limit=500)
    except Exception:
        employees, shops = [], []
    return render_template("it_support_hr_management.html", employees=employees, shops=shops)


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
    try:
        from database import approve_employee

        ok = approve_employee(emp_id, role=role, shop_id=shop_id)
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
        from database import update_employee_by_it_hr

        ok = update_employee_by_it_hr(
            emp_id,
            full_name=full_name,
            email=email,
            phone=phone,
            role=role,
            shop_id=shop_id,
            employee_code=employee_code,
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
    app.run(debug=True, port=5000)
