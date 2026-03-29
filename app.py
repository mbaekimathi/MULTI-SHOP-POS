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
from typing import Optional
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
    return render_template("equipment.html")


@app.route("/quote")
def quote():
    return render_template("quote.html")


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

        if not category or not name or not price_raw:
            flash("Please fill item category, item name, and item price.", "error")
            return redirect(url_for("it_support_register_item"))

        try:
            price = float(price_raw)
            if price < 0:
                raise ValueError()
        except Exception:
            flash("Item price must be a valid number.", "error")
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


@app.route("/it_support/item-management/stock-management", methods=["GET", "POST"])
@login_required
def it_support_stock_management():
    _it_support_only()

    if request.method == "POST":
        action = (request.form.get("action") or "").strip()
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
            return redirect(url_for("it_support_stock_management"))

        direction = "in" if action == "stock_in" else "out" if action == "stock_out" else None
        if direction is None:
            flash("Invalid action.", "error")
            return redirect(url_for("it_support_stock_management"))

        try:
            qty = int(float(qty_raw))
        except Exception:
            flash("Quantity must be a whole number.", "error")
            return redirect(url_for("it_support_stock_management", item_id=item_id, mode=direction))

        buying_price = None
        refund_amount = None
        refunded = refunded_raw == "yes"
        if direction == "in":
            if not buying_price_raw:
                flash("Buying price is required for stock in.", "error")
                return redirect(url_for("it_support_stock_management", item_id=item_id, mode=direction))
            if not place_brought_from:
                flash("Place brought from is required for stock in.", "error")
                return redirect(url_for("it_support_stock_management", item_id=item_id, mode=direction))
            try:
                buying_price = float(buying_price_raw)
                if buying_price < 0:
                    raise ValueError()
            except Exception:
                flash("Buying price must be a valid number.", "error")
                return redirect(url_for("it_support_stock_management", item_id=item_id, mode=direction))
        else:
            allowed_reasons = {"return", "waste", "display"}
            if stock_out_reason not in allowed_reasons:
                flash("Please choose a valid stock out reason.", "error")
                return redirect(url_for("it_support_stock_management", item_id=item_id, mode=direction))
            if refunded_raw not in ("yes", "no"):
                flash("Please choose if the stock out is refunded.", "error")
                return redirect(url_for("it_support_stock_management", item_id=item_id, mode=direction))
            if refunded:
                if not refund_amount_raw:
                    flash("Refund amount is required when refunded is YES.", "error")
                    return redirect(url_for("it_support_stock_management", item_id=item_id, mode=direction))
                try:
                    refund_amount = float(refund_amount_raw)
                    if refund_amount < 0:
                        raise ValueError()
                except Exception:
                    flash("Refund amount must be a valid number.", "error")
                    return redirect(url_for("it_support_stock_management", item_id=item_id, mode=direction))

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
            return redirect(url_for("it_support_stock_management", item_id=item_id, mode=direction))

        flash("Stock updated.", "success")
        return redirect(url_for("it_support_stock_management", item_id=item_id, mode=direction))

    mode = (request.args.get("mode") or "in").strip().lower()
    if mode not in ("in", "out"):
        mode = "in"
    item_id = request.args.get("item_id", type=int)

    try:
        from database import list_stock_manage_items, list_stock_transactions

        items = list_stock_manage_items(limit=500)
        txs = list_stock_transactions(item_id, direction=mode, limit=200) if item_id else []
    except Exception:
        items, txs = [], []

    return render_template(
        "it_support_stock_management.html",
        items=items,
        selected_item_id=item_id,
        mode=mode,
        transactions=txs,
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

        if not category or not name or not price_raw:
            flash("Please fill item category, item name, and item price.", "error")
            return redirect(url_for("it_support_item_edit", item_id=item_id))

        try:
            price = float(price_raw)
            if price < 0:
                raise ValueError()
        except Exception:
            flash("Item price must be a valid number.", "error")
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
    return render_template("it_support_store_management.html")


@app.route("/it_support/register-shop", methods=["GET", "POST"])
@login_required
def it_support_register_shop():
    _it_support_only()

    if request.method == "POST":
        shop_name = (request.form.get("shop_name") or "").strip().upper()
        shop_code = (request.form.get("shop_code") or "").strip().upper()
        shop_password = (request.form.get("shop_password") or "").strip()
        shop_location = (request.form.get("shop_location") or "").strip().upper()

        if not shop_name or not shop_code or not shop_password or not shop_location:
            flash("Please fill shop name, shop code, shop password, and shop location.", "error")
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
    """End shop password session for this branch (does not clear employee portal session)."""
    shop = _get_shop_or_404(shop_id)
    try:
        sid = int(session.get("shop_id") or 0)
    except (TypeError, ValueError):
        sid = 0
    if sid == int(shop_id):
        session.pop("shop_id", None)
        session.pop("shop_name", None)
        flash("You have been signed out from this shop.", "success")
    return redirect(url_for("shop_login", shop_id=shop_id))


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
    return render_template(
        "shop_pos.html",
        shop=shop,
        items=items,
        pos_printing_settings=_effective_printing_settings_for_shop(shop),
        pos_receipt_settings=_effective_receipt_settings_for_shop(shop),
        theme_key=f"richcom-theme-shop-{shop['id']}",
        theme_default=shop.get("default_theme") or "dark",
        font_family=shop.get("font_family") or "Plus Jakarta Sans",
        primary_color_rgb=_hex_to_rgb_triplet(shop.get("primary_color") or "#10b981"),
        accent_color_rgb=_hex_to_rgb_triplet(shop.get("accent_color") or "#14b8a6"),
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
        items.append(
            {
                "id": int(it.get("id") or 0),
                "shop_stock_qty": int(it.get("shop_stock_qty") or 0),
                "price": round(price, 2),
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
            lines=data.get("lines") if isinstance(data.get("lines"), list) else [],
        )
    except Exception:
        ok, sale_err = False, None
    if not ok:
        msg = sale_err or "Could not record sale."
        status = 400 if sale_err else 500
        return jsonify({"ok": False, "error": msg}), status
    return jsonify({"ok": True})


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
    max_workers = min(192, max(32, len(tasks)))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(probe_pair, i, p): (i, p) for i, p in tasks}
        for fut in as_completed(futs):
            row = fut.result()
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
        max_workers = min(192, max(32, len(pair_tasks)))
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = {ex.submit(probe_pair, i, p): (i, p) for i, p in pair_tasks}
            for fut in as_completed(futs):
                row = fut.result()
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
        d.qr(text, ec=QR_ECLEVEL_M, size=6, native=False, center=True)
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
def shop_stock_management(shop_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate

    mode = (request.args.get("mode") or "in").strip().lower()
    if mode not in ("in", "out"):
        mode = "in"

    if request.method == "POST":
        action = (request.form.get("action") or "").strip()
        item_id_raw = (request.form.get("item_id") or "").strip()
        qty_raw = (request.form.get("qty") or "").strip()
        buying_price_raw = (request.form.get("buying_price") or "").strip()
        place_brought_from = (request.form.get("place_brought_from") or "").strip().upper()
        reason = (request.form.get("reason") or "").strip().lower()
        refunded_raw = (request.form.get("refunded") or "").strip().lower()
        refund_amount_raw = (request.form.get("refund_amount") or "").strip()
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
                shop_manual_stock_in,
                shop_manual_stock_out,
                shop_request_stock_from_company,
                shop_return_stock_to_company,
            )

            ensure_shop_items_for_shop(shop_id)

            if action == "request_company":
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
                refunded = refunded_raw == "yes"
                refund_amount = None
                if refunded:
                    refund_amount = refund_amount_raw
                ok = shop_return_stock_to_company(
                    shop_id=shop_id,
                    item_id=item_id,
                    qty=qty,
                    reason=reason,
                    refunded=refunded,
                    refund_amount=refund_amount,
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
                refunded = refunded_raw == "yes"
                refund_amount = None
                if refunded:
                    refund_amount = refund_amount_raw
                ok = shop_manual_stock_out(
                    shop_id=shop_id,
                    item_id=item_id,
                    qty=qty,
                    reason=reason,
                    refunded=refunded,
                    refund_amount=refund_amount,
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

        return redirect(url_for("shop_stock_management", shop_id=shop_id, item_id=item_id, mode=mode))

    selected_item_id = request.args.get("item_id", type=int)
    try:
        from database import ensure_shop_items_for_shop, list_shop_stock_manage_items, list_shop_stock_transactions

        ensure_shop_items_for_shop(shop_id)
        items = list_shop_stock_manage_items(shop_id=shop_id, limit=500)
        txs = list_shop_stock_transactions(shop_id=shop_id, item_id=selected_item_id, limit=200) if selected_item_id else []
    except Exception:
        items, txs = [], []

    return render_template(
        "shop_stock_management.html",
        shop=shop,
        items=items,
        selected_item_id=selected_item_id,
        mode=mode,
        transactions=txs,
        theme_key=f"richcom-theme-shop-{shop['id']}",
        theme_default=shop.get("default_theme") or "dark",
        font_family=shop.get("font_family") or "Plus Jakarta Sans",
        primary_color_rgb=_hex_to_rgb_triplet(shop.get("primary_color") or "#10b981"),
        accent_color_rgb=_hex_to_rgb_triplet(shop.get("accent_color") or "#14b8a6"),
    )


@app.route("/shops/<int:shop_id>/shop-stock-analytics")
def shop_stock_analytics(shop_id: int):
    shop = _get_shop_or_404(shop_id)
    gate = _require_shop_access(shop)
    if gate is not None:
        return gate
    analytics_filter = _build_analytics_filter()
    try:
        from database import get_shop_stock_analytics

        stock_data = get_shop_stock_analytics(shop_id=shop_id, analytics_filter=analytics_filter)
    except Exception:
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
    return render_template(
        "shop_stock_analytics.html",
        shop=shop,
        analytics_filter=analytics_filter,
        stock_data=stock_data,
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


@app.route("/it_support/website-management")
@login_required
def it_support_website_management():
    _it_support_only()
    return render_template("it_support_website_management.html")


if __name__ == "__main__":
    app.run(debug=True, port=5000)
