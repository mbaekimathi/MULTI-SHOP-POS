"""PyMySQL helpers for Richcom Technologies.

All MySQL access uses PyMySQL (`pymysql`). Credentials come from the environment:
``project_env`` loads tracked ``.env.example`` first, then optional ``.env`` (overrides),
from the project root so ``MYSQL_*`` work even if this module is imported before ``app.py`` runs.
"""

import json
import logging
import math
import os
import re
from contextlib import contextmanager
import calendar
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pymysql
from pymysql.cursors import DictCursor

import project_env  # noqa: F401 — loads .env.example then .env

logger = logging.getLogger(__name__)

# Stock movements (manual stock in/out, company grid, transfers) allow fractional qty e.g. 0.15 kg.
STOCK_QTY_DECIMAL_PLACES = 4


def normalize_stock_move_qty(qty) -> Optional[float]:
    """Parse quantity for stock in/out; returns positive float rounded to STOCK_QTY_DECIMAL_PLACES or None."""
    if isinstance(qty, str):
        t = qty.strip().replace("\u00a0", "").replace(" ", "")
        if not t:
            return None
        # Accept "1,25" / "0,5" when no '.' is present (common decimal comma locales).
        if "," in t and "." not in t:
            t = t.replace(",", ".")
        try:
            q = float(t)
        except ValueError:
            return None
    else:
        try:
            q = float(qty)
        except (TypeError, ValueError):
            return None
    if not math.isfinite(q) or q <= 0:
        return None
    q = round(q, STOCK_QTY_DECIMAL_PLACES)
    if q <= 0:
        return None
    return q


def ensure_stock_qty_decimal_schema() -> None:
    """Best-effort ALTER: INT qty columns → DECIMAL(14,4) for fractional inventory."""
    alters: list[str] = []
    if table_exists("items"):
        alters.append(
            "ALTER TABLE items MODIFY COLUMN stock_qty DECIMAL(14,4) NOT NULL DEFAULT 0"
        )
    if table_exists("shop_items"):
        alters.append(
            "ALTER TABLE shop_items MODIFY COLUMN shop_stock_qty DECIMAL(14,4) NOT NULL DEFAULT 0"
        )
    if table_exists("stock_transactions"):
        alters.extend(
            [
                "ALTER TABLE stock_transactions MODIFY COLUMN qty DECIMAL(14,4) NOT NULL",
                "ALTER TABLE stock_transactions MODIFY COLUMN stock_before DECIMAL(14,4) NOT NULL",
                "ALTER TABLE stock_transactions MODIFY COLUMN stock_after DECIMAL(14,4) NOT NULL",
            ]
        )
    if table_exists("shop_stock_transactions"):
        alters.extend(
            [
                "ALTER TABLE shop_stock_transactions MODIFY COLUMN qty DECIMAL(14,4) NOT NULL",
                "ALTER TABLE shop_stock_transactions MODIFY COLUMN shop_stock_before DECIMAL(14,4) NOT NULL",
                "ALTER TABLE shop_stock_transactions MODIFY COLUMN shop_stock_after DECIMAL(14,4) NOT NULL",
            ]
        )
    if table_exists("shop_stock_requests") and column_exists("shop_stock_requests", "qty"):
        alters.append("ALTER TABLE shop_stock_requests MODIFY COLUMN qty DECIMAL(14,4) NOT NULL")
    if table_exists("shop_pos_sale_items") and column_exists("shop_pos_sale_items", "qty"):
        alters.append(
            "ALTER TABLE shop_pos_sale_items MODIFY COLUMN qty DECIMAL(14,4) NOT NULL DEFAULT 0"
        )
    if table_exists("shop_pos_sales") and column_exists("shop_pos_sales", "item_count"):
        alters.append(
            "ALTER TABLE shop_pos_sales MODIFY COLUMN item_count DECIMAL(14,4) NOT NULL DEFAULT 0"
        )
    if table_exists("store_stock_items") and column_exists("store_stock_items", "stock_qty"):
        alters.append(
            "ALTER TABLE store_stock_items MODIFY COLUMN stock_qty DECIMAL(14,4) NOT NULL DEFAULT 0"
        )
    if table_exists("store_stock_transactions"):
        alters.extend(
            [
                "ALTER TABLE store_stock_transactions MODIFY COLUMN qty DECIMAL(14,4) NOT NULL",
                "ALTER TABLE store_stock_transactions MODIFY COLUMN stock_before DECIMAL(14,4) NOT NULL",
                "ALTER TABLE store_stock_transactions MODIFY COLUMN stock_after DECIMAL(14,4) NOT NULL",
            ]
        )
    for sql in alters:
        try:
            with get_cursor(commit=True) as cur:
                cur.execute(sql)
        except pymysql.Error as e:
            logger.warning("Stock qty decimal migration skipped/failed (%s): %s", sql[:72], e)


# Avoid spamming logs when MySQL rejects credentials (e.g. Flask debug reloader / repeated init).
_MYSQL_1045_LOGGED = False

# Set RICHCOM_HOSTED=1 (or true/yes) on the production server so MySQL uses hosted defaults below.
# Local/dev: omit it and keep root defaults unless MYSQL_* env vars are set.


def is_hosted_deployment() -> bool:
    """True when app runs on hosted/production (set RICHCOM_HOSTED=1 on the server)."""
    v = (os.getenv("RICHCOM_HOSTED") or "").strip().lower()
    return v in ("1", "true", "yes")


DEFAULT_MYSQL_DATABASE = "richcom"


def _safe_database_name(raw: Optional[str]) -> str:
    """Allow only safe MySQL identifier characters; default richcom."""
    name = (raw or DEFAULT_MYSQL_DATABASE).strip()
    if not name or not re.match(r"^[a-zA-Z0-9_-]+$", name):
        return DEFAULT_MYSQL_DATABASE
    return name[:64]


def get_database_name() -> str:
    raw = os.getenv("MYSQL_DATABASE")
    if raw:
        return _safe_database_name(raw)
    if is_hosted_deployment():
        return _safe_database_name(DEFAULT_MYSQL_DATABASE)
    return _safe_database_name(None)


def _config(include_database: bool = True):
    if is_hosted_deployment():
        cfg = {
            "host": os.getenv("MYSQL_HOST", "127.0.0.1"),
            "port": int(os.getenv("MYSQL_PORT", "3306")),
            "user": os.getenv("MYSQL_USER", "twigabea_pos"),
            "password": os.getenv("MYSQL_PASSWORD", "Itskimathi007"),
            "charset": "utf8mb4",
            "cursorclass": DictCursor,
        }
    else:
        cfg = {
            "host": os.getenv("MYSQL_HOST", "127.0.0.1"),
            "port": int(os.getenv("MYSQL_PORT", "3306")),
            "user": os.getenv("MYSQL_USER", "root"),
            "password": os.getenv("MYSQL_PASSWORD", ""),
            "charset": "utf8mb4",
            "cursorclass": DictCursor,
        }
    if include_database:
        cfg["database"] = get_database_name()
    return cfg


def get_connection():
    """Open a connection to the configured database (default ``richcom``).

    If the schema does not exist yet (MySQL 1049), create it with
    :func:`ensure_database_exists` and retry once.
    """
    try:
        return pymysql.connect(**_config())
    except pymysql.Error as e:
        errno = e.args[0] if getattr(e, "args", None) else None
        if errno == 1049:
            ensure_database_exists()
            return pymysql.connect(**_config())
        raise


def _analytics_where_clause(analytics_filter: dict, alias: str = ""):
    """Return SQL and params for canonical analytics date range."""
    f = analytics_filter or {}
    prefix = f"{alias}." if alias else ""
    start = (f.get("range_start") or "").strip()
    end = (f.get("range_end_exclusive") or "").strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", start) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", end):
        return f"{prefix}created_at >= %s AND {prefix}created_at < %s", [start, end]
    day = (f.get("single_day") or "").strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", day):
        return f"DATE({prefix}created_at) = %s", [day]
    return "1=1", []


def _analytics_shop_filter_clause(alias: str, shop_id: Optional[int]):
    """Optional single-shop scope for IT support analytics queries."""
    if shop_id is None:
        return "", []
    try:
        sid = int(shop_id)
    except (TypeError, ValueError):
        return "", []
    if sid <= 0:
        return "", []
    return f" AND {alias}.shop_id = %s", [sid]


def _analytics_shops_row_filter(shop_id: Optional[int], shop_alias: str = "sh"):
    """Restrict shop breakdown rows when one branch is selected."""
    if shop_id is None:
        return "", []
    try:
        sid = int(shop_id)
    except (TypeError, ValueError):
        return "", []
    if sid <= 0:
        return "", []
    return f" WHERE {shop_alias}.id = %s", [sid]


def _analytics_business_date_where(analytics_filter: dict, column: str = "business_date"):
    """Return SQL and params for filtering DATE columns to the analytics period."""
    f = analytics_filter or {}
    start = (f.get("range_start") or "").strip()
    end = (f.get("range_end_exclusive") or "").strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", start) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", end):
        return f"{column} >= %s AND {column} < %s", [start, end]
    day = (f.get("single_day") or "").strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", day):
        return f"{column} = %s", [day]
    return "1=1", []


def _resolve_credit_payment_status(*, amount: float, amount_paid: float) -> str:
    """Map credit line totals to paid / partially_paid / not_paid."""
    total = round(float(amount or 0), 2)
    paid = round(float(amount_paid or 0), 2)
    remaining = round(max(0.0, total - paid), 2)
    if remaining <= 0.009:
        return "paid"
    if paid > 0.009:
        return "partially_paid"
    return "not_paid"


def _credit_stock_cost_for_payment(
    credit_stock_cost_full: float, credit_amount: float, credit_paid_amount: float
) -> float:
    """Count FIFO buying price on credit lines only for paid (or partially paid) amounts."""
    full = round(float(credit_stock_cost_full or 0), 2)
    amount = round(float(credit_amount or 0), 2)
    paid = round(float(credit_paid_amount or 0), 2)
    if full <= 0 or amount <= 0.009 or paid <= 0.009:
        return 0.0
    ratio = min(1.0, paid / amount)
    return round(full * ratio, 2)


def _build_report_items_sold_lists(items: list) -> dict:
    """Split sold items into cash-sale and credit lists with payment status."""
    sale_rows: list = []
    credit_rows: list = []
    for i in items or []:
        stock_sold = int(i.get("stock_sold") or 0)
        revenue = float(i.get("revenue") or 0)
        if stock_sold <= 0 and revenue <= 0:
            continue
        base = {
            "item_id": i.get("item_id"),
            "name": i.get("name"),
            "category": i.get("category"),
        }
        sale_qty = int(i.get("sale_qty") or 0)
        sale_amount = round(float(i.get("sale_amount") or 0), 2)
        sale_stock_cost = round(float(i.get("sale_stock_cost") or 0), 2)
        if sale_qty > 0 or sale_amount > 0:
            sale_rows.append(
                {
                    **base,
                    "qty": sale_qty,
                    "amount": sale_amount,
                    "stock_cost": sale_stock_cost,
                    "payment_status": "paid",
                }
            )
        credit_qty = int(i.get("credit_qty") or 0)
        credit_amount = round(float(i.get("credit_amount") or 0), 2)
        credit_stock_cost = round(float(i.get("credit_stock_cost") or 0), 2)
        credit_stock_cost_full = round(
            float(i.get("credit_stock_cost_full") or i.get("credit_stock_cost") or 0), 2
        )
        if credit_qty > 0 or credit_amount > 0:
            credit_paid = round(float(i.get("credit_paid_amount") or 0), 2)
            credit_rows.append(
                {
                    **base,
                    "qty": credit_qty,
                    "amount": credit_amount,
                    "stock_cost": credit_stock_cost_full,
                    "stock_cost_paid": credit_stock_cost,
                    "amount_paid": credit_paid,
                    "balance": round(max(0.0, credit_amount - credit_paid), 2),
                    "payment_status": _resolve_credit_payment_status(
                        amount=credit_amount, amount_paid=credit_paid
                    ),
                }
            )
    sale_rows.sort(key=lambda x: (-float(x.get("amount") or 0), x.get("name") or ""))
    credit_rows.sort(key=lambda x: (-float(x.get("amount") or 0), x.get("name") or ""))
    combined = [
        i
        for i in (items or [])
        if int(i.get("stock_sold") or 0) > 0 or float(i.get("revenue") or 0) > 0
    ]
    combined.sort(key=lambda x: (-float(x.get("revenue") or 0), x.get("name") or ""))
    return {
        "items_sold": combined,
        "items_sold_sale": sale_rows,
        "items_sold_credit": credit_rows,
    }


def _enrich_period_report_financials(out: dict) -> dict:
    """Add till opening/closing balances and a sold-items list to period reports."""
    opening_cash = float(out.get("opening_cash_total") or 0)
    opening_mpesa = float(out.get("opening_mpesa_total") or 0)
    cash_revenue = float(out.get("cash_revenue") or 0)
    mpesa_revenue = float(out.get("mpesa_revenue") or 0)
    out["till_summary"] = {
        "opening_cash": round(opening_cash, 2),
        "opening_mpesa": round(opening_mpesa, 2),
        "opening_total": round(opening_cash + opening_mpesa, 2),
        "closing_cash": round(opening_cash + cash_revenue, 2),
        "closing_mpesa": round(opening_mpesa + mpesa_revenue, 2),
        "closing_total": round(opening_cash + opening_mpesa + cash_revenue + mpesa_revenue, 2),
    }
    if out.get("items_sold_sale") is None and out.get("items_sold_credit") is None:
        sold_lists = _build_report_items_sold_lists(list(out.get("items") or []))
        out["items_sold"] = sold_lists["items_sold"]
        out["items_sold_sale"] = sold_lists["items_sold_sale"]
        out["items_sold_credit"] = sold_lists["items_sold_credit"]
    elif out.get("items_sold") is None:
        combined = list(out.get("items_sold_sale") or []) + list(out.get("items_sold_credit") or [])
        out["items_sold"] = combined
    return _apply_shop_report_summary_cards(out)


def _analytics_receipt_scope_clause(analytics_scope: Optional[str], alias: str = ""):
    """Return SQL and params for analytics data scope.

    - general: include all receipt marks (pending/confirmed/cancelled/partial_return/returned)
    - actual: only confirmed receipts
    """
    scope = (analytics_scope or "general").strip().lower()
    prefix = f"{alias}." if alias else ""
    if scope == "actual":
        return f"COALESCE({prefix}receipt_mark_status, 'pending') = 'confirmed'", []
    return "1=1", []


@contextmanager
def get_cursor(commit=False):
    conn = get_connection()
    try:
        cur = conn.cursor()
        yield cur
        if commit:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def ensure_database_exists() -> bool:
    """
    Create the MySQL schema (database) if it does not exist.
    Connects without a default database; requires CREATE privilege on the server.
    """
    db_name = get_database_name()
    try:
        conn = pymysql.connect(**_config(include_database=False))
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"CREATE DATABASE IF NOT EXISTS `{db_name}` "
                    "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                )
            conn.commit()
            logger.info("MySQL database %r is ready.", db_name)
            return True
        finally:
            conn.close()
    except pymysql.Error as e:
        global _MYSQL_1045_LOGGED
        errno = e.args[0] if getattr(e, "args", None) else None
        if errno == 1045 and _MYSQL_1045_LOGGED:
            return False
        logger.warning("Could not create or verify database %r: %s", db_name, e)
        if errno == 1045:
            _MYSQL_1045_LOGGED = True
            if not (os.getenv("MYSQL_PASSWORD") or "").strip():
                logger.warning(
                    "MySQL refused the connection with no password. Set MYSQL_PASSWORD in your .env file "
                    "(project root, next to app.py) to match your MySQL user, then restart the app. "
                    "Example: MYSQL_USER=root and MYSQL_PASSWORD=your_secret"
                )
            else:
                logger.warning(
                    "MySQL access denied: check MYSQL_USER and MYSQL_PASSWORD in .env; the account needs "
                    "CREATE DATABASE and normal privileges on the configured host."
                )
        return False


def _schema_ident(name: str) -> Optional[str]:
    """Return a table/column name if safe for INFORMATION_SCHEMA queries, else None."""
    n = (name or "").strip()
    if not n or not re.match(r"^[a-zA-Z0-9_]+$", n):
        return None
    return n[:64]


def table_exists(table: str) -> bool:
    """True if the table exists in the configured application database."""
    t = _schema_ident(table)
    if not t:
        return False
    db = get_database_name()
    try:
        with get_cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                LIMIT 1
                """,
                (db, t),
            )
            return cur.fetchone() is not None
    except pymysql.Error:
        return False


def column_exists(table: str, column: str) -> bool:
    """True if the column exists on the table in the configured database."""
    t = _schema_ident(table)
    c = _schema_ident(column)
    if not t or not c:
        return False
    db = get_database_name()
    try:
        with get_cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s
                LIMIT 1
                """,
                (db, t, c),
            )
            return cur.fetchone() is not None
    except pymysql.Error:
        return False


def init_contact_table():
    """Create contact_messages if missing. Safe to call at startup."""
    sql = """
    CREATE TABLE IF NOT EXISTS contact_messages (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(120) NOT NULL,
        email VARCHAR(190) NOT NULL,
        company VARCHAR(190) DEFAULT NULL,
        message TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql)
        logger.info("Table contact_messages is ready.")
        return True
    except pymysql.Error as e:
        logger.warning("Could not init contact_messages: %s", e)
        return False


def save_contact_message(name, email, company, message):
    sql = """
    INSERT INTO contact_messages (name, email, company, message)
    VALUES (%s, %s, %s, %s)
    """
    with get_cursor(commit=True) as cur:
        cur.execute(sql, (name, email, company or None, message))
        return cur.lastrowid


def init_employees_table():
    """Create employees if missing. Safe to call at startup."""
    sql = """
    CREATE TABLE IF NOT EXISTS employees (
        id INT AUTO_INCREMENT PRIMARY KEY,
        full_name VARCHAR(200) NOT NULL,
        email VARCHAR(190) NOT NULL,
        phone VARCHAR(40) NOT NULL,
        employee_code CHAR(6) NOT NULL,
        password_hash VARCHAR(255) NOT NULL,
        status ENUM('pending_approval', 'active', 'suspended') NOT NULL DEFAULT 'pending_approval',
        role ENUM(
            'super_admin', 'it_support', 'company_manager', 'admin', 'manager',
            'sales', 'finance', 'employee', 'rider'
        ) NOT NULL DEFAULT 'employee',
        shop_id INT NULL,
        profile_image VARCHAR(500) DEFAULT NULL,
        preferred_payment_method VARCHAR(32) NULL DEFAULT NULL,
        payment_account_holder VARCHAR(200) NULL DEFAULT NULL,
        payment_bank_or_provider VARCHAR(120) NULL DEFAULT NULL,
        payment_account_number VARCHAR(128) NULL DEFAULT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uq_employee_code (employee_code),
        UNIQUE KEY uq_employee_email (email)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql)
            # Backward compatible migration.
            cur.execute("SHOW COLUMNS FROM employees LIKE 'shop_id'")
            if not cur.fetchone():
                cur.execute("ALTER TABLE employees ADD COLUMN shop_id INT NULL AFTER role")
            for col, col_sql in (
                ("preferred_payment_method", "VARCHAR(32) NULL DEFAULT NULL"),
                ("payment_account_holder", "VARCHAR(200) NULL DEFAULT NULL"),
                ("payment_bank_or_provider", "VARCHAR(120) NULL DEFAULT NULL"),
                ("payment_account_number", "VARCHAR(128) NULL DEFAULT NULL"),
            ):
                cur.execute("SHOW COLUMNS FROM employees LIKE %s", (col,))
                if not cur.fetchone():
                    cur.execute(
                        f"ALTER TABLE employees ADD COLUMN {col} {col_sql} AFTER profile_image"
                    )
            try:
                cur.execute(
                    "UPDATE employees SET preferred_payment_method = %s "
                    "WHERE preferred_payment_method = %s",
                    ("mpesa", "mobile_money"),
                )
                cur.execute(
                    "UPDATE employees SET preferred_payment_method = %s "
                    "WHERE preferred_payment_method = %s",
                    ("bank", "bank_transfer"),
                )
                cur.execute(
                    "UPDATE employees SET preferred_payment_method = NULL "
                    "WHERE preferred_payment_method = %s",
                    ("other",),
                )
            except pymysql.Error:
                pass
            cur.execute("SHOW COLUMNS FROM employees LIKE 'role'")
            role_col = cur.fetchone() or {}
            role_type = str(role_col.get("Type") or "")
            if "company_manager" not in role_type.lower():
                cur.execute(
                    """
                    ALTER TABLE employees MODIFY COLUMN role ENUM(
                        'super_admin', 'it_support', 'company_manager', 'admin', 'manager',
                        'sales', 'finance', 'employee', 'rider'
                    ) NOT NULL DEFAULT 'employee'
                    """
                )
        logger.info("Table employees is ready.")
        return True
    except pymysql.Error as e:
        logger.warning("Could not init employees: %s", e)
        return False


def init_employee_password_resets_table():
    """One-time password reset verification codes for active employees."""
    sql = """
    CREATE TABLE IF NOT EXISTS employee_password_resets (
        id INT AUTO_INCREMENT PRIMARY KEY,
        employee_id INT NOT NULL,
        email VARCHAR(190) NOT NULL,
        code_hash VARCHAR(255) NOT NULL,
        expires_at DATETIME NOT NULL,
        used_at DATETIME NULL DEFAULT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        KEY idx_epr_employee (employee_id),
        KEY idx_epr_expires (expires_at),
        CONSTRAINT fk_epr_employee FOREIGN KEY (employee_id) REFERENCES employees(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql)
        logger.info("Table employee_password_resets is ready.")
        return True
    except pymysql.Error as e:
        logger.warning("Could not init employee_password_resets: %s", e)
        return False


def init_employee_shop_access_table():
    """Many-to-many: which branches an employee may use POS for (HR multi-shop mode)."""
    sql = """
    CREATE TABLE IF NOT EXISTS employee_shop_access (
        employee_id INT NOT NULL,
        shop_id INT NOT NULL,
        PRIMARY KEY (employee_id, shop_id),
        KEY idx_esa_shop (shop_id),
        CONSTRAINT fk_esa_employee FOREIGN KEY (employee_id)
            REFERENCES employees(id) ON DELETE CASCADE,
        CONSTRAINT fk_esa_shop FOREIGN KEY (shop_id)
            REFERENCES shops(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql)
        logger.info("Table employee_shop_access is ready.")
        return True
    except pymysql.Error as e:
        logger.warning("Could not init employee_shop_access: %s", e)
        return False


def init_employee_payroll_table():
    """Payroll registrations (gross pay and frequency) per employee. Safe to call at startup."""
    sql = """
    CREATE TABLE IF NOT EXISTS employee_payroll (
        id INT AUTO_INCREMENT PRIMARY KEY,
        employee_id INT NOT NULL,
        gross_amount DECIMAL(14, 2) NOT NULL,
        pay_frequency ENUM('monthly', 'weekly', 'biweekly', 'daily') NOT NULL DEFAULT 'monthly',
        effective_from DATE NOT NULL,
        notes VARCHAR(500) DEFAULT NULL,
        registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        KEY idx_employee_payroll_employee (employee_id),
        KEY idx_employee_payroll_effective (effective_from),
        CONSTRAINT fk_employee_payroll_employee FOREIGN KEY (employee_id) REFERENCES employees(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql)
        logger.info("Table employee_payroll is ready.")
        return True
    except pymysql.Error as e:
        logger.warning("Could not init employee_payroll: %s", e)
        return False


def init_employee_payroll_advances_table():
    """Salary advances tied to a pay period; deducted from payroll gross for that period."""
    sql = """
    CREATE TABLE IF NOT EXISTS employee_payroll_advances (
        id INT AUTO_INCREMENT PRIMARY KEY,
        employee_id INT NOT NULL,
        amount DECIMAL(14, 2) NOT NULL,
        pay_frequency ENUM('monthly', 'weekly', 'biweekly', 'daily') NOT NULL,
        period_start DATE NOT NULL,
        notes VARCHAR(500) DEFAULT NULL,
        registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        KEY idx_epa_employee (employee_id),
        KEY idx_epa_period (employee_id, pay_frequency, period_start),
        CONSTRAINT fk_epa_employee FOREIGN KEY (employee_id) REFERENCES employees(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql)
        logger.info("Table employee_payroll_advances is ready.")
        return True
    except pymysql.Error as e:
        logger.warning("Could not init employee_payroll_advances: %s", e)
        return False


HR_ACTIVITY_ACTION_KINDS: tuple[str, ...] = (
    "login",
    "logout",
    "register",
    "edit",
    "update",
    "delete",
    "suspend",
    "unsuspend",
    "approve",
    "view",
    "other",
)


def init_hr_activity_log_table():
    """Activity log feeding the IT HR analytics page (logins, logouts, CRUD audit)."""
    sql = """
    CREATE TABLE IF NOT EXISTS hr_activity_log (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        employee_id INT NULL,
        employee_full_name VARCHAR(200) NULL,
        employee_role VARCHAR(40) NULL,
        action_kind VARCHAR(40) NOT NULL,
        target_type VARCHAR(40) NULL,
        target_id INT NULL,
        description VARCHAR(500) NULL,
        ip_address VARCHAR(64) NULL,
        user_agent VARCHAR(255) NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        KEY idx_hr_activity_employee (employee_id, created_at),
        KEY idx_hr_activity_kind (action_kind, created_at),
        KEY idx_hr_activity_created (created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql)
        logger.info("Table hr_activity_log is ready.")
        return True
    except pymysql.Error as e:
        logger.warning("Could not init hr_activity_log: %s", e)
        return False


def init_shop_day_openings_table() -> bool:
    """Daily shop opening balances (cash/M-Pesa) and stock confirmation."""
    sql = """
    CREATE TABLE IF NOT EXISTS shop_day_openings (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        shop_id INT NOT NULL,
        business_date DATE NOT NULL,
        opening_cash DECIMAL(14,2) NOT NULL DEFAULT 0,
        opening_mpesa DECIMAL(14,2) NOT NULL DEFAULT 0,
        stock_confirmed TINYINT(1) NOT NULL DEFAULT 0,
        submitted_by_employee_id INT NULL,
        submitted_by_code VARCHAR(20) NULL,
        submitted_by_name VARCHAR(200) NULL,
        submitted_by_role VARCHAR(40) NULL,
        closing_cash DECIMAL(14,2) NULL,
        closing_mpesa DECIMAL(14,2) NULL,
        closing_submitted_at TIMESTAMP NULL,
        closing_submitted_by_employee_id INT NULL,
        closing_submitted_by_code VARCHAR(20) NULL,
        closing_submitted_by_name VARCHAR(200) NULL,
        closing_submitted_by_role VARCHAR(40) NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uq_shop_day_openings_shop_date (shop_id, business_date),
        KEY idx_shop_day_openings_date (business_date),
        KEY idx_shop_day_openings_shop (shop_id, business_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql)
        ensure_shop_day_closing_schema()
        logger.info("Table shop_day_openings is ready.")
        return True
    except pymysql.Error as e:
        logger.warning("Could not init shop_day_openings: %s", e)
        return False


def ensure_shop_day_closing_schema() -> bool:
    """Add end-of-day closing balance columns to shop_day_openings on existing installs."""
    if not table_exists("shop_day_openings"):
        return False
    cols = [
        ("closing_cash", "DECIMAL(14,2) NULL"),
        ("closing_mpesa", "DECIMAL(14,2) NULL"),
        ("closing_submitted_at", "TIMESTAMP NULL"),
        ("closing_submitted_by_employee_id", "INT NULL"),
        ("closing_submitted_by_code", "VARCHAR(20) NULL"),
        ("closing_submitted_by_name", "VARCHAR(200) NULL"),
        ("closing_submitted_by_role", "VARCHAR(40) NULL"),
    ]
    try:
        with get_cursor(commit=True) as cur:
            for col, ddl in cols:
                if column_exists("shop_day_openings", col):
                    continue
                cur.execute(f"ALTER TABLE shop_day_openings ADD COLUMN {col} {ddl}")
        return True
    except pymysql.Error as e:
        logger.warning("Could not ensure shop_day_openings closing schema: %s", e)
        return False


def _normalize_business_date(value) -> Optional[date]:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    raw = str(value or "").strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
        try:
            return date.fromisoformat(raw)
        except ValueError:
            return None
    return None


def _serialize_shop_day_opening_row(row: Optional[dict]) -> Optional[dict]:
    if not row:
        return None
    biz = row.get("business_date")
    if hasattr(biz, "isoformat"):
        biz_out = biz.isoformat()
    else:
        biz_out = str(biz or "")
    created = row.get("created_at")
    if hasattr(created, "isoformat"):
        created_out = created.isoformat(sep=" ", timespec="seconds")
    else:
        created_out = str(created or "")
    closing_at = row.get("closing_submitted_at")
    if hasattr(closing_at, "isoformat"):
        closing_at_out = closing_at.isoformat(sep=" ", timespec="seconds")
    else:
        closing_at_out = str(closing_at or "") if closing_at else ""
    return {
        "id": int(row.get("id") or 0),
        "shop_id": int(row.get("shop_id") or 0),
        "business_date": biz_out,
        "opening_cash": float(row.get("opening_cash") or 0),
        "opening_mpesa": float(row.get("opening_mpesa") or 0),
        "stock_confirmed": bool(int(row.get("stock_confirmed") or 0)),
        "submitted_by_employee_id": row.get("submitted_by_employee_id"),
        "submitted_by_code": row.get("submitted_by_code") or "",
        "submitted_by_name": row.get("submitted_by_name") or "",
        "submitted_by_role": row.get("submitted_by_role") or "",
        "created_at": created_out,
        "closing_cash": float(row.get("closing_cash") or 0) if row.get("closing_submitted_at") else None,
        "closing_mpesa": float(row.get("closing_mpesa") or 0) if row.get("closing_submitted_at") else None,
        "closing_submitted_at": closing_at_out or None,
        "closing_submitted_by_employee_id": row.get("closing_submitted_by_employee_id"),
        "closing_submitted_by_code": row.get("closing_submitted_by_code") or "",
        "closing_submitted_by_name": row.get("closing_submitted_by_name") or "",
        "closing_submitted_by_role": row.get("closing_submitted_by_role") or "",
        "shop_name": row.get("shop_name") or "",
        "shop_code": row.get("shop_code") or "",
    }


def get_shop_day_opening(shop_id: int, business_date) -> Optional[dict]:
    """Return today's (or given date's) opening record for a shop, if any."""
    try:
        sid = int(shop_id)
    except Exception:
        return None
    if sid <= 0:
        return None
    biz = _normalize_business_date(business_date)
    if not biz:
        return None
    sql = """
    SELECT
        id, shop_id, business_date, opening_cash, opening_mpesa, stock_confirmed,
        submitted_by_employee_id, submitted_by_code, submitted_by_name, submitted_by_role, created_at,
        closing_cash, closing_mpesa, closing_submitted_at,
        closing_submitted_by_employee_id, closing_submitted_by_code,
        closing_submitted_by_name, closing_submitted_by_role
    FROM shop_day_openings
    WHERE shop_id = %s AND business_date = %s
    LIMIT 1
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (sid, biz))
            row = cur.fetchone()
        return _serialize_shop_day_opening_row(row)
    except pymysql.Error as e:
        logger.warning("get_shop_day_opening failed shop=%s date=%s: %s", sid, biz, e)
        return None


def save_shop_day_opening(
    shop_id: int,
    business_date,
    *,
    opening_cash: float,
    opening_mpesa: float,
    stock_confirmed: bool,
    require_stock_confirmation: bool = True,
    employee_id: Optional[int] = None,
    employee_code: Optional[str] = None,
    employee_name: Optional[str] = None,
    employee_role: Optional[str] = None,
) -> Tuple[bool, Optional[str], Optional[dict]]:
    """Insert the daily opening record once per shop/date. Returns existing row if already saved."""
    try:
        sid = int(shop_id)
    except Exception:
        return False, "Invalid shop.", None
    if sid <= 0:
        return False, "Invalid shop.", None
    biz = _normalize_business_date(business_date)
    if not biz:
        return False, "Invalid business date.", None

    existing = get_shop_day_opening(sid, biz)
    if existing:
        if existing.get("closing_submitted_at"):
            return (
                False,
                "Shop is closed for today. Submit opening balances with a manager or admin code to reopen.",
                None,
            )
        return True, None, existing

    if require_stock_confirmation and not stock_confirmed:
        return False, "Confirm that stock is up to date before submitting.", None

    stock_confirmed_val = bool(stock_confirmed) if require_stock_confirmation else False

    try:
        cash_val = float(opening_cash or 0)
        mpesa_val = float(opening_mpesa or 0)
    except (TypeError, ValueError):
        return False, "Enter valid opening cash and M-Pesa amounts.", None
    if cash_val < 0 or mpesa_val < 0:
        return False, "Opening balances cannot be negative.", None

    emp_id_val: Optional[int] = None
    if employee_id is not None:
        try:
            emp_id_val = int(employee_id)
            if emp_id_val <= 0:
                emp_id_val = None
        except Exception:
            emp_id_val = None

    sql = """
    INSERT INTO shop_day_openings (
        shop_id, business_date, opening_cash, opening_mpesa, stock_confirmed,
        submitted_by_employee_id, submitted_by_code, submitted_by_name, submitted_by_role
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    params = (
        sid,
        biz,
        round(cash_val, 2),
        round(mpesa_val, 2),
        1 if stock_confirmed_val else 0,
        emp_id_val,
        (employee_code or "").strip() or None,
        (employee_name or "").strip() or None,
        (employee_role or "").strip() or None,
    )
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql, params)
    except pymysql.IntegrityError:
        existing = get_shop_day_opening(sid, biz)
        if existing:
            return True, None, existing
        return False, "Opening balances were already submitted for today.", None
    except pymysql.Error as e:
        logger.warning("save_shop_day_opening failed shop=%s date=%s: %s", sid, biz, e)
        return False, "Could not save opening balances. Try again.", None

    saved = get_shop_day_opening(sid, biz)
    return True, None, saved


def _format_business_date_label(business_date) -> str:
    biz = _normalize_business_date(business_date)
    if not biz:
        return str(business_date or "")
    try:
        return biz.strftime("%d %b %Y")
    except Exception:
        return biz.isoformat()


def _shop_day_payment_totals(shop_id: int, business_date) -> dict:
    """Cash and M-Pesa collected on a shop business date (POS sales)."""
    try:
        sid = int(shop_id)
    except Exception:
        return {"cash_revenue": 0.0, "mpesa_revenue": 0.0}
    if sid <= 0:
        return {"cash_revenue": 0.0, "mpesa_revenue": 0.0}
    biz = _normalize_business_date(business_date)
    if not biz:
        return {"cash_revenue": 0.0, "mpesa_revenue": 0.0}
    sql = """
    SELECT
        COALESCE(SUM(s.cash_amount), 0) AS cash_revenue,
        COALESCE(SUM(s.mpesa_amount), 0) AS mpesa_revenue
    FROM shop_pos_sales s
    WHERE s.shop_id = %s AND DATE(s.created_at) = %s
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (sid, biz))
            row = cur.fetchone() or {}
        return {
            "cash_revenue": float(row.get("cash_revenue") or 0),
            "mpesa_revenue": float(row.get("mpesa_revenue") or 0),
        }
    except pymysql.Error as e:
        logger.warning(
            "_shop_day_payment_totals failed shop=%s date=%s: %s", sid, biz, e
        )
        return {"cash_revenue": 0.0, "mpesa_revenue": 0.0}


def get_pending_shop_day_closing(shop_id: int, as_of=None) -> Optional[dict]:
    """Most recent day before as_of with opening submitted but no closing."""
    try:
        sid = int(shop_id)
    except Exception:
        return None
    if sid <= 0:
        return None
    as_of_date = _normalize_business_date(as_of) or date.today()
    ensure_shop_day_closing_schema()
    sql = """
    SELECT
        id, shop_id, business_date, opening_cash, opening_mpesa, stock_confirmed,
        submitted_by_employee_id, submitted_by_code, submitted_by_name, submitted_by_role, created_at,
        closing_cash, closing_mpesa, closing_submitted_at,
        closing_submitted_by_employee_id, closing_submitted_by_code,
        closing_submitted_by_name, closing_submitted_by_role
    FROM shop_day_openings
    WHERE shop_id = %s AND business_date < %s AND closing_submitted_at IS NULL
    ORDER BY business_date DESC
    LIMIT 1
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (sid, as_of_date))
            row = cur.fetchone()
        if not row:
            return None
        record = _serialize_shop_day_opening_row(row)
        if not record:
            return None
        payments = _shop_day_payment_totals(sid, record.get("business_date"))
        opening_cash = float(record.get("opening_cash") or 0)
        opening_mpesa = float(record.get("opening_mpesa") or 0)
        cash_revenue = float(payments.get("cash_revenue") or 0)
        mpesa_revenue = float(payments.get("mpesa_revenue") or 0)
        return {
            "required": True,
            "business_date": record.get("business_date") or "",
            "label": _format_business_date_label(record.get("business_date")),
            "opening_cash": opening_cash,
            "opening_mpesa": opening_mpesa,
            "cash_revenue": cash_revenue,
            "mpesa_revenue": mpesa_revenue,
            "suggested_closing_cash": round(opening_cash + cash_revenue, 2),
            "suggested_closing_mpesa": round(opening_mpesa + mpesa_revenue, 2),
        }
    except pymysql.Error as e:
        logger.warning("get_pending_shop_day_closing failed shop=%s: %s", sid, e)
        return None


def save_shop_day_closing(
    shop_id: int,
    business_date,
    *,
    closing_cash: float,
    closing_mpesa: float,
    employee_id: Optional[int] = None,
    employee_code: Optional[str] = None,
    employee_name: Optional[str] = None,
    employee_role: Optional[str] = None,
) -> Tuple[bool, Optional[str], Optional[dict]]:
    """Record end-of-day closing balances once per shop/date."""
    try:
        sid = int(shop_id)
    except Exception:
        return False, "Invalid shop.", None
    if sid <= 0:
        return False, "Invalid shop.", None
    biz = _normalize_business_date(business_date)
    if not biz:
        return False, "Invalid business date.", None
    if biz > date.today():
        return False, "Closing balances cannot be submitted for a future date.", None

    existing = get_shop_day_opening(sid, biz)
    if not existing:
        return False, "No opening record found for that day.", None
    if existing.get("closing_submitted_at"):
        return True, None, existing

    try:
        cash_val = float(closing_cash or 0)
        mpesa_val = float(closing_mpesa or 0)
    except (TypeError, ValueError):
        return False, "Enter valid closing cash and M-Pesa amounts.", None
    if cash_val < 0 or mpesa_val < 0:
        return False, "Closing balances cannot be negative.", None

    emp_id_val: Optional[int] = None
    if employee_id is not None:
        try:
            emp_id_val = int(employee_id)
            if emp_id_val <= 0:
                emp_id_val = None
        except Exception:
            emp_id_val = None

    sql = """
    UPDATE shop_day_openings SET
        closing_cash = %s,
        closing_mpesa = %s,
        closing_submitted_at = CURRENT_TIMESTAMP,
        closing_submitted_by_employee_id = %s,
        closing_submitted_by_code = %s,
        closing_submitted_by_name = %s,
        closing_submitted_by_role = %s
    WHERE shop_id = %s AND business_date = %s AND closing_submitted_at IS NULL
    """
    params = (
        round(cash_val, 2),
        round(mpesa_val, 2),
        emp_id_val,
        (employee_code or "").strip() or None,
        (employee_name or "").strip() or None,
        (employee_role or "").strip() or None,
        sid,
        biz,
    )
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql, params)
            if cur.rowcount <= 0:
                existing = get_shop_day_opening(sid, biz)
                if existing and existing.get("closing_submitted_at"):
                    return True, None, existing
                return False, "Could not save closing balances. Try again.", None
    except pymysql.Error as e:
        logger.warning("save_shop_day_closing failed shop=%s date=%s: %s", sid, biz, e)
        return False, "Could not save closing balances. Try again.", None

    saved = get_shop_day_opening(sid, biz)
    return True, None, saved


def reopen_shop_day_till(
    shop_id: int,
    business_date,
    *,
    opening_cash: float,
    opening_mpesa: float,
    stock_confirmed: bool = False,
    require_stock_confirmation: bool = True,
    employee_id: Optional[int] = None,
    employee_code: Optional[str] = None,
    employee_name: Optional[str] = None,
    employee_role: Optional[str] = None,
) -> Tuple[bool, Optional[str], Optional[dict]]:
    """Clear today's closing submission so sales can resume; optionally refresh opening balances."""
    try:
        sid = int(shop_id)
    except Exception:
        return False, "Invalid shop.", None
    if sid <= 0:
        return False, "Invalid shop.", None
    biz = _normalize_business_date(business_date)
    if not biz:
        return False, "Invalid business date.", None

    existing = get_shop_day_opening(sid, biz)
    if not existing:
        return False, "No opening record for this day.", None
    if not existing.get("closing_submitted_at"):
        return True, None, existing

    if require_stock_confirmation and not stock_confirmed:
        return False, "Confirm that stock is up to date before reopening.", None

    try:
        cash_val = float(opening_cash or 0)
        mpesa_val = float(opening_mpesa or 0)
    except (TypeError, ValueError):
        return False, "Enter valid opening cash and M-Pesa amounts.", None
    if cash_val < 0 or mpesa_val < 0:
        return False, "Opening balances cannot be negative.", None

    emp_id_val: Optional[int] = None
    if employee_id is not None:
        try:
            emp_id_val = int(employee_id)
            if emp_id_val <= 0:
                emp_id_val = None
        except Exception:
            emp_id_val = None

    stock_val = 1 if (stock_confirmed if require_stock_confirmation else bool(existing.get("stock_confirmed"))) else 0

    sql = """
    UPDATE shop_day_openings SET
        opening_cash = %s,
        opening_mpesa = %s,
        stock_confirmed = %s,
        closing_cash = NULL,
        closing_mpesa = NULL,
        closing_submitted_at = NULL,
        closing_submitted_by_employee_id = NULL,
        closing_submitted_by_code = NULL,
        closing_submitted_by_name = NULL,
        closing_submitted_by_role = NULL,
        submitted_by_employee_id = %s,
        submitted_by_code = %s,
        submitted_by_name = %s,
        submitted_by_role = %s
    WHERE shop_id = %s AND business_date = %s AND closing_submitted_at IS NOT NULL
    """
    params = (
        round(cash_val, 2),
        round(mpesa_val, 2),
        stock_val,
        emp_id_val,
        (employee_code or "").strip() or None,
        (employee_name or "").strip() or None,
        (employee_role or "").strip() or None,
        sid,
        biz,
    )
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql, params)
            if cur.rowcount <= 0:
                row = get_shop_day_opening(sid, biz)
                if row and not row.get("closing_submitted_at"):
                    return True, None, row
                return False, "Could not reopen the till. Try again.", None
    except pymysql.Error as e:
        logger.warning("reopen_shop_day_till failed shop=%s date=%s: %s", sid, biz, e)
        return False, "Could not reopen the till. Try again.", None

    saved = get_shop_day_opening(sid, biz)
    return True, None, saved


def get_shop_day_closing_context(shop_id: int, business_date) -> Optional[dict]:
    """Opening + sales summary for submitting closing balances on a business date."""
    try:
        sid = int(shop_id)
    except Exception:
        return None
    if sid <= 0:
        return None
    biz = _normalize_business_date(business_date)
    if not biz:
        return None
    record = get_shop_day_opening(sid, biz)
    if not record:
        return None
    if record.get("closing_submitted_at"):
        return {
            "submitted": True,
            "business_date": record.get("business_date") or biz.isoformat(),
            "label": _format_business_date_label(biz),
            "closing_cash": float(record.get("closing_cash") or 0),
            "closing_mpesa": float(record.get("closing_mpesa") or 0),
            "closing_submitted_at": record.get("closing_submitted_at") or "",
            "closing_submitted_by_name": record.get("closing_submitted_by_name") or "",
        }
    payments = _shop_day_payment_totals(sid, biz)
    opening_cash = float(record.get("opening_cash") or 0)
    opening_mpesa = float(record.get("opening_mpesa") or 0)
    cash_revenue = float(payments.get("cash_revenue") or 0)
    mpesa_revenue = float(payments.get("mpesa_revenue") or 0)
    return {
        "submitted": False,
        "business_date": record.get("business_date") or biz.isoformat(),
        "label": _format_business_date_label(biz),
        "opening_cash": opening_cash,
        "opening_mpesa": opening_mpesa,
        "cash_revenue": cash_revenue,
        "mpesa_revenue": mpesa_revenue,
        "suggested_closing_cash": round(opening_cash + cash_revenue, 2),
        "suggested_closing_mpesa": round(opening_mpesa + mpesa_revenue, 2),
    }


def list_shop_day_openings_for_report(
    analytics_filter: dict,
    shop_id: Optional[int] = None,
) -> list:
    """Opening balances submitted during the analytics period."""
    date_where, date_params = _analytics_business_date_where(analytics_filter, "sdo.business_date")
    shop_clause = ""
    params: list = list(date_params)
    if shop_id is not None:
        try:
            sid = int(shop_id)
        except Exception:
            sid = 0
        if sid > 0:
            shop_clause = " AND sdo.shop_id = %s"
            params.append(sid)
    sql = f"""
    SELECT
        sdo.id, sdo.shop_id, sdo.business_date, sdo.opening_cash, sdo.opening_mpesa,
        sdo.stock_confirmed, sdo.submitted_by_employee_id, sdo.submitted_by_code,
        sdo.submitted_by_name, sdo.submitted_by_role, sdo.created_at,
        s.shop_name, s.shop_code
    FROM shop_day_openings sdo
    JOIN shops s ON s.id = sdo.shop_id
    WHERE {date_where}{shop_clause}
    ORDER BY sdo.business_date DESC, s.shop_name ASC, sdo.id DESC
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
        out = []
        for row in rows:
            ser = _serialize_shop_day_opening_row(row)
            if ser:
                out.append(ser)
        return out
    except pymysql.Error as e:
        logger.warning("list_shop_day_openings_for_report failed: %s", e)
        return []


def _stock_out_expenditure_payment(total_cost: float, refunded: bool, refund_amount) -> dict:
    """Map manual stock-out refund fields to expenditure payment status."""
    total = round(float(total_cost or 0), 2)
    if not refunded or total <= 0:
        return {
            "payment_status": "paid",
            "amount_paid": total,
            "balance": 0.0,
            "refund_amount": 0.0,
        }
    try:
        refund = round(float(refund_amount or 0), 2)
    except Exception:
        refund = 0.0
    refund = max(0.0, min(refund, total))
    if refund >= total - 0.009:
        return {
            "payment_status": "cancelled_out",
            "amount_paid": 0.0,
            "balance": 0.0,
            "refund_amount": refund,
        }
    if refund > 0.009:
        net = round(total - refund, 2)
        return {
            "payment_status": "partially_refunded",
            "amount_paid": net,
            "balance": refund,
            "refund_amount": refund,
        }
    return {
        "payment_status": "paid",
        "amount_paid": total,
        "balance": 0.0,
        "refund_amount": 0.0,
    }


def _serialize_shop_stock_out_expenditure_row(r: dict) -> dict:
    try:
        qty = float(r.get("qty") or 0)
    except Exception:
        qty = 0.0
    try:
        total = round(float(r.get("cost_total") or 0), 2)
    except Exception:
        total = 0.0
    if total <= 0 and qty > 0:
        try:
            unit_fallback = float(r.get("buying_price") or 0)
        except Exception:
            unit_fallback = 0.0
        total = round(qty * unit_fallback, 2)
    unit = round(total / qty, 2) if qty > 0 else round(float(r.get("buying_price") or 0), 2)
    refunded = int(r.get("refunded") or 0) == 1
    pay = _stock_out_expenditure_payment(total, refunded, r.get("refund_amount"))
    reason = (r.get("reason") or "").strip().upper() or "STOCK OUT"
    created = r.get("created_at")
    if hasattr(created, "strftime"):
        created_out = created.strftime("%d %b %Y %H:%M")
        created_iso = created.strftime("%Y-%m-%dT%H:%M:%S")
    else:
        created_out = str(created or "")
        created_iso = created_out
    moved_by = (r.get("created_by") or "").strip()
    return {
        "id": int(r.get("id") or 0),
        "shop_id": int(r.get("shop_id") or 0),
        "category": reason.title(),
        "name": (r.get("item_name") or "").strip() or f"Item #{r.get('item_id') or ''}",
        "qty": qty,
        "unit_price": unit,
        "buying_price": unit,
        "total_cost": total,
        "payment_status": pay["payment_status"],
        "amount_paid": pay["amount_paid"],
        "balance": pay["balance"],
        "refund_amount": pay["refund_amount"],
        "note": (r.get("note") or "").strip(),
        "supplier_name": moved_by,
        "seller_phone": "",
        "supplier": moved_by or reason.title(),
        "created_by": moved_by,
        "created_at": created_out,
        "created_at_iso": created_iso,
        "expense_kind": "stock_out",
        "stock_out_reason": reason,
        "shop_name": (r.get("shop_name") or "").strip(),
    }


def list_shop_stock_outs_for_report(shop_id: int, analytics_filter: dict, limit: int = 5000) -> list:
    """Manual stock-out rows (return / waste / display) for shop expenditure report."""
    try:
        sid = int(shop_id)
    except Exception:
        return []
    if sid <= 0:
        return []
    init_shop_stock_transactions_table()
    if not column_exists("shop_stock_transactions", "cost_total"):
        return []
    lim = max(1, min(int(limit or 5000), 8000))
    where, params = _analytics_where_clause(analytics_filter or {}, "sst")
    where = (
        f"({where}) AND sst.shop_id = %s AND sst.direction = 'out' AND sst.source = 'manual' "
        f"AND UPPER(COALESCE(sst.reason, '')) IN ('RETURN', 'WASTE', 'DISPLAY')"
    )
    params = list(params) + [sid]
    sql = f"""
    SELECT
        sst.id,
        sst.shop_id,
        sst.item_id,
        sst.qty,
        sst.buying_price,
        sst.cost_total,
        sst.reason,
        sst.refunded,
        sst.refund_amount,
        sst.note,
        sst.created_at,
        i.name AS item_name,
        i.category,
        COALESCE(e.full_name, '') AS created_by
    FROM shop_stock_transactions sst
    JOIN items i ON i.id = sst.item_id
    LEFT JOIN employees e ON e.id = sst.created_by_employee_id
    WHERE {where}
    ORDER BY sst.created_at DESC, sst.id DESC
    LIMIT %s
    """
    params.append(lim)
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
        return [_serialize_shop_stock_out_expenditure_row(dict(r)) for r in rows]
    except pymysql.Error as e:
        logger.warning("list_shop_stock_outs_for_report failed shop=%s: %s", sid, e)
        return []


def list_shop_expenditure_for_report(shop_id: int, analytics_filter: dict) -> list:
    """Stock purchases and operational expenses for one shop in the analytics period."""
    try:
        sid = int(shop_id)
    except Exception:
        return []
    if sid <= 0:
        return []
    out: list = []
    for r in list_shop_stock_purchases(shop_id=sid, analytics_filter=analytics_filter, limit=5000):
        row = dict(r)
        row["buying_price"] = round(float(row.get("unit_price") or row.get("buying_price") or 0), 2)
        row["supplier"] = (row.get("supplier_name") or "—").strip() or "—"
        row["expense_kind"] = "stock"
        out.append(row)
    op_rows = list_shop_operational_expenses_for_report(sid, analytics_filter)
    for r in op_rows:
        r["supplier"] = (r.get("supplier_name") or r.get("supplier") or "—").strip() or "—"
    out.extend(op_rows)
    out.extend(list_shop_stock_outs_for_report(sid, analytics_filter))
    out.sort(
        key=lambda x: (str(x.get("created_at_iso") or x.get("created_at") or ""), int(x.get("id") or 0)),
        reverse=True,
    )
    return out


def _sum_shop_expenditure_breakdown(expenditure_rows: list) -> dict:
    """Split stock purchases and operational bills into paid vs unpaid totals."""
    paid_stock = 0.0
    unpaid_stock = 0.0
    paid_bills = 0.0
    unpaid_bills = 0.0
    for r in expenditure_rows or []:
        ps = (r.get("payment_status") or "pending_payment").strip().lower()
        kind = (r.get("expense_kind") or "").strip().lower()
        if ps == "cancelled_out" or kind == "stock_out":
            continue
        try:
            row_total = float(r.get("total_cost") or r.get("total_amount") or 0)
        except Exception:
            row_total = 0.0
        try:
            row_paid = float(r.get("amount_paid") or 0)
        except Exception:
            row_paid = 0.0
        row_balance = max(0.0, row_total - row_paid)
        if kind == "operational":
            paid_bills += row_paid
            unpaid_bills += row_balance
        else:
            paid_stock += row_paid
            unpaid_stock += row_balance
    return {
        "paid_stock_expenditure": round(paid_stock, 2),
        "unpaid_stock_expenditure": round(unpaid_stock, 2),
        "paid_bills_expenditure": round(paid_bills, 2),
        "unpaid_bills_expenditure": round(unpaid_bills, 2),
    }


def _apply_shop_report_pos_settings(out: dict) -> dict:
    """Apply POS settings to report totals (credit off, kitchen/both stock cost rules)."""
    allow_credit = bool(out.get("pos_allow_credit_sale", True))
    inv_mode = (out.get("pos_inventory_mode") or "shop").strip().lower()
    out["pos_allow_credit_sale"] = allow_credit
    out["pos_inventory_mode"] = inv_mode
    out["stock_cost_shelf_only"] = inv_mode in ("kitchen", "both")

    if not allow_credit:
        sale_rev = round(float(out.get("sale_revenue") or 0), 2)
        out["credit_revenue"] = 0.0
        out["paid_credit"] = 0.0
        out["unpaid_credit"] = 0.0
        out["total_revenue"] = sale_rev
        out["accrual_cogs_credit"] = 0.0
        out["stock_cost_credit_total"] = 0.0
        out["items_sold_credit"] = []
        out["items_sold"] = list(out.get("items_sold_sale") or [])
        for item in out.get("items") or []:
            if not isinstance(item, dict):
                continue
            item["credit_qty"] = 0
            item["credit_amount"] = 0.0
            item["credit_paid_amount"] = 0.0
            item["credit_stock_cost"] = 0.0
            item["credit_stock_cost_full"] = 0.0

    if inv_mode in ("kitchen", "both"):
        cogs_out = round(float(out.get("accrual_cogs_stock_out") or 0), 2)
        cogs_portions = round(float(out.get("accrual_cogs_kitchen_portions") or 0), 2)
        out["accrual_cogs_sale"] = 0.0
        out["accrual_cogs_credit"] = 0.0
        out["stock_cost_sale_only"] = 0.0
        # Kitchen/both: portion est. production price is buying price (COGS). Shelf stock-out
        # counts under expenditure (kitchen + both). Legacy shop-mode sales keep FIFO COGS.
        shop_mode_fifo = round(float(out.get("accrual_cogs_shop_mode_sales") or 0), 2)
        out["shelf_stock_out_expenditure"] = cogs_out if inv_mode in ("kitchen", "both") else 0.0
        out["accrual_cogs"] = round(cogs_portions + shop_mode_fifo, 2)
        out["stock_cost_sold"] = round(cogs_portions + shop_mode_fifo, 2)
        out["stock_cost_total"] = out["accrual_cogs"]
        credit_portion = round(
            sum(
                float(i.get("credit_stock_cost_full") or 0)
                for i in out.get("items") or []
                if isinstance(i, dict)
            ),
            2,
        )
        out["stock_cost_credit_total"] = credit_portion
    elif inv_mode == "none":
        cogs_out = round(float(out.get("accrual_cogs_stock_out") or 0), 2)
        out["accrual_cogs_sale"] = 0.0
        out["accrual_cogs_credit"] = 0.0
        out["accrual_cogs_kitchen_portions"] = 0.0
        out["accrual_cogs_shop_mode_sales"] = 0.0
        out["accrual_cogs"] = cogs_out
        out["stock_cost_sold"] = 0.0
        out["stock_cost_total"] = cogs_out
        out["shelf_stock_out_expenditure"] = 0.0
        out["stock_cost_credit_total"] = 0.0
    elif not allow_credit:
        cogs_sale = round(float(out.get("accrual_cogs_sale") or 0), 2)
        cogs_out = round(float(out.get("accrual_cogs_stock_out") or 0), 2)
        out["accrual_cogs"] = round(cogs_sale + cogs_out, 2)
        out["shelf_stock_out_expenditure"] = 0.0
    else:
        out.setdefault("shelf_stock_out_expenditure", 0.0)

    return out


def _apply_shop_report_summary_cards(out: dict) -> dict:
    """Card totals for the simplified revenue / expenditure / stock-cost summary."""
    out = _apply_shop_report_pos_settings(out)
    breakdown = _sum_shop_expenditure_breakdown(out.get("expenditure_rows") or [])
    out.update(breakdown)
    till = out.get("till_summary")
    if isinstance(till, dict) and till:
        opening_total = round(float(till.get("opening_total") or 0), 2)
    else:
        opening_total = round(
            float(out.get("opening_cash_total") or 0) + float(out.get("opening_mpesa_total") or 0),
            2,
        )
    out["opening_balance_expense"] = opening_total
    cash_mpesa = round(
        float(out.get("cash_revenue") or 0) + float(out.get("mpesa_revenue") or 0),
        2,
    )
    out["summary_cash_mpesa"] = cash_mpesa
    paid_credit = round(float(out.get("paid_credit") or 0), 2)
    out["summary_collected_revenue"] = round(cash_mpesa + paid_credit, 2)
    out["collected_revenue"] = out["summary_collected_revenue"]
    out["summary_revenue_total"] = out["summary_collected_revenue"]
    out["summary_revenue_settled"] = out["summary_collected_revenue"]
    shelf_stock_out = round(float(out.get("shelf_stock_out_expenditure") or 0), 2)
    out["shelf_stock_out_expenditure"] = shelf_stock_out
    out["summary_expenditure_total"] = round(
        breakdown["paid_stock_expenditure"]
        + breakdown["paid_bills_expenditure"]
        + float(out.get("unpaid_credit") or 0)
        + breakdown["unpaid_stock_expenditure"]
        + breakdown["unpaid_bills_expenditure"]
        + opening_total
        + shelf_stock_out,
        2,
    )
    out["summary_expenditure_collected"] = out["summary_expenditure_total"]
    accrual_net, accrual_net_collected = _shop_report_accrual_net_profit_pair(out)
    out["accrual_net_profit"] = accrual_net
    out["accrual_net_profit_collected"] = accrual_net_collected
    out["net_profit"] = accrual_net_collected
    return out


def _sum_shop_expenditure_totals(expenditure_rows: list) -> dict:
    """Aggregate total, paid, and balance from expenditure line rows."""
    total = 0.0
    paid = 0.0
    for r in expenditure_rows or []:
        ps = (r.get("payment_status") or "pending_payment").strip().lower()
        kind = (r.get("expense_kind") or "").strip().lower()
        try:
            row_total = float(r.get("total_cost") or r.get("total_amount") or 0)
        except Exception:
            row_total = 0.0
        try:
            row_paid = float(r.get("amount_paid") or 0)
        except Exception:
            row_paid = 0.0
        if ps == "cancelled_out":
            continue
        if kind == "stock_out":
            total += row_total
            paid += row_paid
        else:
            total += row_total
            paid += row_paid
    total = round(total, 2)
    paid = round(paid, 2)
    return {
        "total_expenditure": total,
        "paid_expenditure": paid,
        "balance_expenditure": round(max(0.0, total - paid), 2),
    }


def _sum_accrual_operating_expenses(expenditure_rows: list) -> float:
    """Stock purchases + operational bills for accrual net profit (excludes stock-out COGS)."""
    total = 0.0
    for r in expenditure_rows or []:
        ps = (r.get("payment_status") or "pending_payment").strip().lower()
        kind = (r.get("expense_kind") or "").strip().lower()
        if ps == "cancelled_out" or kind == "stock_out":
            continue
        try:
            total += float(r.get("total_cost") or r.get("total_amount") or 0)
        except Exception:
            pass
    return round(total, 2)


def _shop_report_accrual_net_profit_pair(out: dict) -> tuple[float, float]:
    """Net profit aligned with the three summary cards (no double-counting paid items)."""
    total_revenue = round(float(out.get("total_revenue") or 0), 2)
    collected_revenue = round(
        float(out.get("summary_collected_revenue") or 0)
        or float(out.get("summary_cash_mpesa") or 0) + float(out.get("paid_credit") or 0)
        or float(out.get("sale_revenue") or 0) + float(out.get("paid_credit") or 0),
        2,
    )
    stock_cost = round(float(out.get("accrual_cogs") or 0), 2)
    expenditure_total = round(float(out.get("summary_expenditure_total") or 0), 2)
    accrual_net = round(total_revenue - expenditure_total - stock_cost, 2)
    accrual_net_collected = round(collected_revenue - expenditure_total - stock_cost, 2)
    out["summary_collected_revenue"] = collected_revenue
    out["collected_revenue"] = collected_revenue
    return accrual_net, accrual_net_collected


def _apply_shop_report_accrual_summary(out: dict, *, stock_cost_stock_out: float) -> dict:
    """Compute accrual profit metrics including COGS (deducted from net profit on finalize)."""
    items = list(out.get("items") or [])
    sale_only = round(sum(float(i.get("sale_stock_cost") or 0) for i in items), 2)
    credit_full = round(sum(float(i.get("credit_stock_cost_full") or 0) for i in items), 2)
    paid_credit_cost = round(sum(float(i.get("credit_stock_cost") or 0) for i in items), 2)
    stock_out_cost = round(float(stock_cost_stock_out or 0), 2)

    out["stock_cost_sale_only"] = sale_only
    out["stock_cost_credit_total"] = credit_full
    out["stock_cost_stock_out"] = stock_out_cost
    out["stock_cost_sold"] = round(sale_only + paid_credit_cost, 2)
    out["stock_cost_total"] = round(out["stock_cost_sold"] + stock_out_cost, 2)
    out["estimated_sale_gross_profit"] = round(float(out.get("sale_revenue") or 0) - sale_only, 2)

    cogs_sale = sale_only
    cogs_credit = credit_full
    cogs_stock_out = stock_out_cost
    accrual_cogs = round(cogs_sale + cogs_credit + cogs_stock_out, 2)
    accrual_revenue = round(float(out.get("total_revenue") or 0), 2)
    accrual_operating = _sum_accrual_operating_expenses(out.get("expenditure_rows") or [])
    accrual_gross = accrual_revenue
    accrual_net, accrual_net_collected = _shop_report_accrual_net_profit_pair(out)

    out["accrual_cogs"] = accrual_cogs
    out["accrual_cogs_sale"] = cogs_sale
    out["accrual_cogs_credit"] = cogs_credit
    out["accrual_cogs_stock_out"] = cogs_stock_out
    out["accrual_gross_profit"] = accrual_gross
    out["accrual_operating_expenses"] = accrual_operating
    out["accrual_net_profit"] = accrual_net
    out["accrual_net_profit_collected"] = accrual_net_collected
    return out


def _serialize_company_warehouse_stock_expenditure_row(r: dict) -> dict:
    try:
        qty = float(r.get("qty") or 0)
    except Exception:
        qty = 0.0
    try:
        unit = float(r.get("buying_price") or 0)
    except Exception:
        unit = 0.0
    total = round(qty * unit, 2)
    amount_paid = round(float(r.get("amount_paid") or 0), 2)
    created = r.get("created_at")
    if hasattr(created, "strftime"):
        created_out = created.strftime("%d %b %Y %H:%M")
        created_iso = created.strftime("%Y-%m-%dT%H:%M:%S")
    else:
        created_out = str(created or "")
        created_iso = created_out
    supplier = (r.get("seller_name") or "").strip() or "—"
    seller_phone = (r.get("seller_phone") or "").strip()
    if seller_phone in ("", "-"):
        seller_phone = ""
    return {
        "id": int(r.get("tx_id") or 0),
        "shop_id": 0,
        "shop_name": "Company warehouse",
        "name": (r.get("item_name") or "").strip(),
        "category": "",
        "qty": qty,
        "unit_price": round(unit, 2),
        "buying_price": round(unit, 2),
        "total_cost": total,
        "payment_status": (r.get("payment_status") or "pending_payment").strip().lower(),
        "amount_paid": amount_paid,
        "balance": round(max(0.0, total - amount_paid), 2),
        "supplier": supplier,
        "supplier_name": supplier,
        "seller_phone": seller_phone,
        "created_by": (r.get("moved_by") or "").strip(),
        "created_at": created_out,
        "created_at_iso": created_iso,
        "expense_kind": "stock",
        "stock_source": "company",
    }


def _merge_company_sold_item_rows(target: dict, row: dict, *, credit: bool) -> dict:
    merged = dict(target)
    for key in ("qty", "amount", "stock_cost", "amount_paid", "stock_cost_paid", "balance"):
        merged[key] = round(float(merged.get(key) or 0) + float(row.get(key) or 0), 2)
    if credit:
        merged["payment_status"] = _resolve_credit_payment_status(
            amount=merged.get("amount"),
            amount_paid=merged.get("amount_paid"),
        )
    return merged


def _aggregate_company_financials_from_shops(
    out: dict,
    analytics_filter: dict,
    analytics_scope: str = "general",
    shop_id: Optional[int] = None,
) -> dict:
    """Sum FIFO COGS and sold-item lists across shops; reconcile accrual net from expenditure rows."""
    shop_rows = list_shops(limit=500) or []
    if shop_id is not None:
        try:
            sid_filter = int(shop_id)
        except Exception:
            sid_filter = 0
        if sid_filter > 0:
            shop_rows = [s for s in shop_rows if int(s.get("id") or 0) == sid_filter]

    cogs_keys = (
        "accrual_cogs",
        "accrual_cogs_sale",
        "accrual_cogs_credit",
        "accrual_cogs_stock_out",
        "accrual_cogs_kitchen_portions",
        "accrual_cogs_shop_mode_sales",
        "stock_cost_sold",
        "stock_cost_stock_out",
        "stock_cost_total",
        "stock_cost_sale_only",
        "stock_cost_credit_total",
        "estimated_sale_gross_profit",
        "shelf_stock_out_expenditure",
    )
    for key in cogs_keys:
        out[key] = 0.0

    sale_merge: dict = {}
    credit_merge: dict = {}
    for shop in shop_rows:
        try:
            sid = int(shop.get("id") or 0)
        except Exception:
            continue
        if sid <= 0:
            continue
        sr = get_shop_report(sid, analytics_filter, analytics_scope)
        for key in cogs_keys:
            out[key] = round(float(out.get(key) or 0) + float(sr.get(key) or 0), 2)
        for row in sr.get("items_sold_sale") or []:
            merge_key = (row.get("item_id"), (row.get("name") or "").strip().lower())
            if merge_key in sale_merge:
                sale_merge[merge_key] = _merge_company_sold_item_rows(sale_merge[merge_key], row, credit=False)
            else:
                sale_merge[merge_key] = dict(row)
        for row in sr.get("items_sold_credit") or []:
            merge_key = (row.get("item_id"), (row.get("name") or "").strip().lower())
            if merge_key in credit_merge:
                credit_merge[merge_key] = _merge_company_sold_item_rows(credit_merge[merge_key], row, credit=True)
            else:
                credit_merge[merge_key] = dict(row)

    sale_rows = list(sale_merge.values())
    credit_rows = list(credit_merge.values())
    sale_rows.sort(key=lambda x: (-float(x.get("amount") or 0), x.get("name") or ""))
    credit_rows.sort(key=lambda x: (-float(x.get("amount") or 0), x.get("name") or ""))
    out["items_sold_sale"] = sale_rows
    out["items_sold_credit"] = credit_rows
    out["items_sold"] = sale_rows + credit_rows

    accrual_revenue = round(float(out.get("total_revenue") or 0), 2)
    accrual_operating = _sum_accrual_operating_expenses(out.get("expenditure_rows") or [])
    out["accrual_gross_profit"] = accrual_revenue
    out["accrual_operating_expenses"] = accrual_operating
    accrual_net, accrual_net_collected = _shop_report_accrual_net_profit_pair(out)
    out["accrual_net_profit"] = accrual_net
    out["accrual_net_profit_collected"] = accrual_net_collected
    out["net_profit"] = accrual_net_collected
    return out


def _normalize_expense_label(raw: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (raw or "").strip().upper())


def _operational_expense_settled_clause(alias: str = "soe") -> str:
    """Operational expenses included in company/period reports (partial or full payment)."""
    a = (alias or "soe").strip() or "soe"
    return (
        f"LOWER(COALESCE({a}.payment_status, 'pending_payment')) "
        f"IN ('partially_paid', 'paid')"
    )


def init_operational_expense_tables() -> bool:
    """Expense categories, catalog names, and shop operational expense records."""
    sql_categories = """
    CREATE TABLE IF NOT EXISTS expense_categories (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(120) NOT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uq_expense_category_name (name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    sql_catalog = """
    CREATE TABLE IF NOT EXISTS expense_catalog_items (
        id INT AUTO_INCREMENT PRIMARY KEY,
        category_id INT NOT NULL,
        name VARCHAR(200) NOT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uq_expense_catalog_cat_name (category_id, name),
        KEY idx_expense_catalog_cat (category_id),
        CONSTRAINT fk_expense_catalog_category
            FOREIGN KEY (category_id) REFERENCES expense_categories(id)
            ON DELETE RESTRICT
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    sql_expenses = """
    CREATE TABLE IF NOT EXISTS shop_operational_expenses (
        id INT AUTO_INCREMENT PRIMARY KEY,
        shop_id INT NOT NULL,
        category_id INT NOT NULL,
        expense_catalog_id INT NOT NULL,
        category_name VARCHAR(120) NOT NULL,
        expense_name VARCHAR(200) NOT NULL,
        qty DECIMAL(18,4) NOT NULL,
        unit_price DECIMAL(18,2) NOT NULL,
        total_amount DECIMAL(18,2) NOT NULL,
        payment_status VARCHAR(32) NOT NULL DEFAULT 'pending_payment',
        amount_paid DECIMAL(18,2) NOT NULL DEFAULT 0,
        note TEXT NULL,
        supplier_name VARCHAR(255) NULL,
        seller_phone VARCHAR(40) NULL,
        created_by_employee_id INT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        KEY idx_shop_op_exp_shop (shop_id),
        KEY idx_shop_op_exp_created (created_at),
        KEY idx_shop_op_exp_category (category_id),
        CONSTRAINT fk_shop_op_exp_shop FOREIGN KEY (shop_id) REFERENCES shops(id) ON DELETE RESTRICT,
        CONSTRAINT fk_shop_op_exp_category FOREIGN KEY (category_id) REFERENCES expense_categories(id) ON DELETE RESTRICT,
        CONSTRAINT fk_shop_op_exp_catalog FOREIGN KEY (expense_catalog_id) REFERENCES expense_catalog_items(id) ON DELETE RESTRICT
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql_categories)
            cur.execute(sql_catalog)
            cur.execute(sql_expenses)
            if not column_exists("shop_operational_expenses", "supplier_name"):
                cur.execute(
                    "ALTER TABLE shop_operational_expenses "
                    "ADD COLUMN supplier_name VARCHAR(255) NULL AFTER note"
                )
            if not column_exists("shop_operational_expenses", "seller_phone"):
                cur.execute(
                    "ALTER TABLE shop_operational_expenses "
                    "ADD COLUMN seller_phone VARCHAR(40) NULL AFTER supplier_name"
                )
        return True
    except pymysql.Error as e:
        logger.warning("init_operational_expense_tables failed: %s", e)
        return False


def get_or_create_expense_category(name: str) -> Optional[int]:
    label = _normalize_expense_label(name)
    if not label:
        return None
    init_operational_expense_tables()
    with get_cursor(commit=True) as cur:
        cur.execute("SELECT id FROM expense_categories WHERE name=%s LIMIT 1", (label,))
        row = cur.fetchone()
        if row:
            return int(row["id"])
        cur.execute("INSERT INTO expense_categories (name) VALUES (%s)", (label,))
        return int(cur.lastrowid)


def get_or_create_expense_catalog_item(category_id: int, name: str) -> Optional[int]:
    try:
        cid = int(category_id)
    except Exception:
        return None
    if cid <= 0:
        return None
    label = _normalize_expense_label(name)
    if not label:
        return None
    init_operational_expense_tables()
    with get_cursor(commit=True) as cur:
        cur.execute(
            "SELECT id FROM expense_catalog_items WHERE category_id=%s AND name=%s LIMIT 1",
            (cid, label),
        )
        row = cur.fetchone()
        if row:
            return int(row["id"])
        cur.execute(
            "INSERT INTO expense_catalog_items (category_id, name) VALUES (%s, %s)",
            (cid, label),
        )
        return int(cur.lastrowid)


def search_expense_categories(query: Optional[str] = None, limit: int = 20) -> list:
    init_operational_expense_tables()
    lim = max(1, min(int(limit or 20), 100))
    q = _normalize_expense_label(query)
    if q:
        like = f"%{q}%"
        sql = "SELECT id, name FROM expense_categories WHERE name LIKE %s ORDER BY name ASC LIMIT %s"
        params = (like, lim)
    else:
        sql = "SELECT id, name FROM expense_categories ORDER BY name ASC LIMIT %s"
        params = (lim,)
    try:
        with get_cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall() or []
    except pymysql.Error:
        return []


def search_expense_catalog_items(
    category_id: Optional[int] = None,
    category_name: Optional[str] = None,
    query: Optional[str] = None,
    limit: int = 20,
) -> list:
    init_operational_expense_tables()
    lim = max(1, min(int(limit or 20), 100))
    q = _normalize_expense_label(query)
    clauses = ["1=1"]
    params: list = []
    if category_id is not None:
        try:
            cid = int(category_id)
        except Exception:
            cid = 0
        if cid > 0:
            clauses.append("eci.category_id = %s")
            params.append(cid)
    elif category_name:
        cat = _normalize_expense_label(category_name)
        if cat:
            clauses.append("ec.name = %s")
            params.append(cat)
    if q:
        clauses.append("eci.name LIKE %s")
        params.append(f"%{q}%")
    sql = f"""
    SELECT eci.id, eci.name, eci.category_id, ec.name AS category_name
    FROM expense_catalog_items eci
    JOIN expense_categories ec ON ec.id = eci.category_id
    WHERE {' AND '.join(clauses)}
    ORDER BY eci.name ASC
    LIMIT %s
    """
    params.append(lim)
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(params))
            return cur.fetchall() or []
    except pymysql.Error:
        return []


def register_shop_operational_expense(
    *,
    shop_id: int,
    category_name: str,
    expense_name: str,
    qty,
    unit_price,
    seller_phone: Optional[str] = None,
    supplier_name: Optional[str] = None,
    note: Optional[str] = None,
    created_by_employee_id: Optional[int] = None,
    payment_status: str = "pending_payment",
) -> tuple[bool, Optional[int], Optional[str]]:
    """Register a non-stock shop expense. Creates category/catalog entries when new."""
    try:
        sid = int(shop_id)
    except Exception:
        return False, None, "Invalid shop."
    if sid <= 0:
        return False, None, "Invalid shop."
    cat_label = _normalize_expense_label(category_name)
    name_label = _normalize_expense_label(expense_name)
    if not cat_label:
        return False, None, "Expense category is required."
    if not name_label:
        return False, None, "Expense name is required."
    n = normalize_stock_move_qty(qty)
    if n is None:
        return False, None, "Quantity must be greater than zero."
    try:
        if isinstance(unit_price, str):
            s = unit_price.strip().replace("\u00a0", "").replace(" ", "")
            if "," in s and "." not in s:
                s = s.replace(",", ".")
            unit = float(s)
        else:
            unit = float(unit_price)
        if not math.isfinite(unit) or unit < 0:
            raise ValueError()
    except Exception:
        return False, None, "Unit price must be a valid number."
    total = round(float(n) * unit, 2)
    payment_status = (payment_status or "pending_payment").strip().lower()
    if payment_status not in {"pending_payment", "partially_paid", "paid"}:
        payment_status = "pending_payment"
    note_clean = _normalize_expense_label(note) if (note or "").strip() else None

    resolved_name, resolved_phone = resolve_seller_name_and_phone(
        seller_phone=seller_phone or "",
        seller_name=supplier_name or "",
    )
    if not resolved_name or not resolved_phone:
        return False, None, "Supplier phone must be valid. If new, provide supplier name to register."

    init_operational_expense_tables()
    category_id = get_or_create_expense_category(cat_label)
    if not category_id:
        return False, None, "Could not save expense category."
    catalog_id = get_or_create_expense_catalog_item(category_id, name_label)
    if not catalog_id:
        return False, None, "Could not save expense name."

    sql = """
    INSERT INTO shop_operational_expenses
        (shop_id, category_id, expense_catalog_id, category_name, expense_name,
         qty, unit_price, total_amount, payment_status, amount_paid, note,
         supplier_name, seller_phone, created_by_employee_id)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,0,%s,%s,%s,%s)
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(
                sql,
                (
                    sid,
                    int(category_id),
                    int(catalog_id),
                    cat_label,
                    name_label,
                    n,
                    round(unit, 2),
                    total,
                    payment_status,
                    note_clean,
                    resolved_name,
                    resolved_phone,
                    int(created_by_employee_id) if created_by_employee_id else None,
                ),
            )
            return True, int(cur.lastrowid), None
    except pymysql.Error as e:
        logger.warning("register_shop_operational_expense failed shop=%s: %s", sid, e)
        return False, None, "Could not register expense."


def update_shop_operational_expense_payment(
    shop_id: int,
    expense_id: int,
    amount_paid: Optional[float] = None,
    *,
    additional_payment: Optional[float] = None,
) -> Optional[dict]:
    """Update amount paid and payment status for one shop operational expense."""
    try:
        sid = int(shop_id)
        eid = int(expense_id)
    except Exception:
        return None
    if sid <= 0 or eid <= 0:
        return None
    init_operational_expense_tables()
    with get_cursor(commit=True) as cur:
        cur.execute(
            """
            SELECT id, shop_id, total_amount, COALESCE(amount_paid, 0) AS amount_paid
            FROM shop_operational_expenses
            WHERE id=%s AND shop_id=%s
            FOR UPDATE
            """,
            (eid, sid),
        )
        row = cur.fetchone() or {}
        if not row:
            return None
        total_cost = round(float(row.get("total_amount") or 0), 2)
        current_paid = round(float(row.get("amount_paid") or 0), 2)

        if additional_payment is not None:
            try:
                add = round(float(additional_payment), 2)
            except Exception:
                return None
            if add < 0:
                return None
            new_paid = round(current_paid + add, 2)
        else:
            try:
                new_paid = round(float(amount_paid or 0), 2)
            except Exception:
                return None
            if new_paid < 0:
                return None

        if total_cost <= 0:
            status = "paid"
        elif new_paid <= 0:
            status = "pending_payment"
        elif new_paid < total_cost:
            status = "partially_paid"
        else:
            status = "paid"
        cur.execute(
            """
            UPDATE shop_operational_expenses
            SET amount_paid=%s, payment_status=%s
            WHERE id=%s AND shop_id=%s
            """,
            (new_paid, status, eid, sid),
        )
        return {
            "id": eid,
            "shop_id": sid,
            "total_cost": total_cost,
            "amount_paid": new_paid,
            "balance": round(max(0.0, total_cost - new_paid), 2),
            "payment_status": status,
        }


def update_shop_operational_expense(
    shop_id: int,
    expense_id: int,
    *,
    category_name: str,
    expense_name: str,
    qty,
    unit_price,
    supplier_name: Optional[str] = None,
    seller_phone: Optional[str] = None,
    note: Optional[str] = None,
    amount_paid: Optional[float] = None,
) -> Tuple[bool, str, Optional[dict]]:
    """Edit one shop operational expense line."""
    try:
        sid = int(shop_id)
        eid = int(expense_id)
    except Exception:
        return False, "Invalid expense reference.", None
    if sid <= 0 or eid <= 0:
        return False, "Invalid expense reference.", None

    cat_label = _normalize_expense_label(category_name)
    name_label = _normalize_expense_label(expense_name)
    if not cat_label:
        return False, "Expense category is required.", None
    if not name_label:
        return False, "Expense name is required.", None
    n = normalize_stock_move_qty(qty)
    if n is None:
        return False, "Quantity must be greater than zero.", None
    try:
        if isinstance(unit_price, str):
            s = unit_price.strip().replace("\u00a0", "").replace(" ", "")
            if "," in s and "." not in s:
                s = s.replace(",", ".")
            unit = float(s)
        else:
            unit = float(unit_price)
        if not math.isfinite(unit) or unit < 0:
            raise ValueError()
    except Exception:
        return False, "Unit price must be a valid number.", None

    resolved_name, resolved_phone = resolve_seller_name_and_phone(
        seller_phone=seller_phone or "",
        seller_name=supplier_name or "",
    )
    if not resolved_name or not resolved_phone:
        return False, "Supplier phone must be valid. If new, provide supplier name to register.", None

    total = round(float(n) * unit, 2)
    note_clean = _normalize_expense_label(note) if (note or "").strip() else None
    init_operational_expense_tables()

    try:
        with get_cursor(commit=True) as cur:
            cur.execute(
                """
                SELECT id, COALESCE(amount_paid, 0) AS amount_paid
                FROM shop_operational_expenses
                WHERE id=%s AND shop_id=%s
                FOR UPDATE
                """,
                (eid, sid),
            )
            row = cur.fetchone()
            if not row:
                return False, "Expense not found.", None

            paid_raw = amount_paid if amount_paid is not None else float(row.get("amount_paid") or 0)
            try:
                paid = round(float(paid_raw or 0), 2)
            except Exception:
                return False, "Invalid amount paid.", None
            if paid < 0:
                return False, "Amount paid cannot be negative.", None
            if total <= 0:
                status = "paid"
                paid = 0.0
            elif paid <= 0:
                status = "pending_payment"
            elif paid + 0.009 < total:
                status = "partially_paid"
            else:
                status = "paid"
                paid = total

            category_id = get_or_create_expense_category(cat_label)
            if not category_id:
                return False, "Could not save expense category.", None
            catalog_id = get_or_create_expense_catalog_item(category_id, name_label)
            if not catalog_id:
                return False, "Could not save expense name.", None

            cur.execute(
                """
                UPDATE shop_operational_expenses
                SET category_id=%s, expense_catalog_id=%s, category_name=%s, expense_name=%s,
                    qty=%s, unit_price=%s, total_amount=%s, payment_status=%s, amount_paid=%s,
                    note=%s, supplier_name=%s, seller_phone=%s
                WHERE id=%s AND shop_id=%s
                """,
                (
                    int(category_id),
                    int(catalog_id),
                    cat_label,
                    name_label,
                    n,
                    round(unit, 2),
                    total,
                    status,
                    paid,
                    note_clean,
                    resolved_name,
                    resolved_phone,
                    eid,
                    sid,
                ),
            )
        return True, "Expense updated.", {
            "id": eid,
            "shop_id": sid,
            "total_cost": total,
            "amount_paid": paid,
            "balance": round(max(0.0, total - paid), 2),
            "payment_status": status,
        }
    except pymysql.Error as e:
        logger.warning("update_shop_operational_expense failed shop=%s id=%s: %s", sid, eid, e)
        return False, "Could not update expense.", None


def delete_shop_operational_expense(shop_id: int, expense_id: int) -> Tuple[bool, str]:
    """Remove one shop operational expense line."""
    try:
        sid = int(shop_id)
        eid = int(expense_id)
    except Exception:
        return False, "Invalid expense reference."
    if sid <= 0 or eid <= 0:
        return False, "Invalid expense reference."
    init_operational_expense_tables()
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(
                "SELECT id FROM shop_operational_expenses WHERE id=%s AND shop_id=%s FOR UPDATE",
                (eid, sid),
            )
            if not cur.fetchone():
                return False, "Expense not found."
            cur.execute(
                "DELETE FROM shop_operational_expenses WHERE id=%s AND shop_id=%s",
                (eid, sid),
            )
        return True, "Expense deleted."
    except pymysql.Error as e:
        logger.warning("delete_shop_operational_expense failed shop=%s id=%s: %s", sid, eid, e)
        return False, "Could not delete expense."


def delete_shop_manual_stock_out(tx_id: int) -> Tuple[bool, str]:
    """Remove a manual stock-out and restore shop stock (+ FIFO layers)."""
    allowed_reasons = {"RETURN", "WASTE", "DISPLAY"}
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(
                """
                SELECT id, shop_id, item_id, qty, direction, source, reason
                FROM shop_stock_transactions
                WHERE id=%s
                FOR UPDATE
                """,
                (int(tx_id),),
            )
            tx = cur.fetchone()
            if not tx:
                return False, "Transaction not found."
            if (tx.get("source") or "").strip().lower() != "manual":
                return False, "Only manual stock-out lines can be deleted."
            if (tx.get("direction") or "").strip().lower() != "out":
                return False, "Only stock-out lines can be deleted here."
            reason = (tx.get("reason") or "").strip().upper()
            if reason not in allowed_reasons:
                return False, "This stock-out type cannot be deleted from here."

            qty = round(float(tx.get("qty") or 0), STOCK_QTY_DECIMAL_PLACES)
            if qty <= 0:
                return False, "Invalid transaction quantity."
            shop_id = int(tx.get("shop_id") or 0)
            item_id = int(tx.get("item_id") or 0)
            if shop_id <= 0 or item_id <= 0:
                return False, "Invalid shop or item on this transaction."

            if ensure_shop_stock_fifo_schema():
                cur.execute(
                    """
                    SELECT id, layer_id, qty
                    FROM shop_stock_fifo_consumptions
                    WHERE out_transaction_id=%s
                    FOR UPDATE
                    """,
                    (int(tx_id),),
                )
                for c in cur.fetchall() or []:
                    lid = int(c.get("layer_id") or 0)
                    q = round(float(c.get("qty") or 0), STOCK_QTY_DECIMAL_PLACES)
                    if lid > 0 and q > 0:
                        cur.execute(
                            """
                            UPDATE shop_stock_cost_layers
                            SET qty_remaining = qty_remaining + %s
                            WHERE id=%s
                            """,
                            (q, lid),
                        )
                    cur.execute(
                        "DELETE FROM shop_stock_fifo_consumptions WHERE id=%s",
                        (int(c.get("id") or 0),),
                    )

            cur.execute(
                f"SELECT {_shop_items_physical_select_sql()} FROM shop_items WHERE shop_id=%s AND item_id=%s FOR UPDATE",
                (shop_id, item_id),
            )
            si = cur.fetchone()
            if not si:
                return False, "Shop item not found."
            shop_now = round(float(si.get("shop_stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
            shop_after = round(shop_now + qty, STOCK_QTY_DECIMAL_PLACES)
            cur.execute(
                "UPDATE shop_items SET shop_stock_qty=%s WHERE shop_id=%s AND item_id=%s",
                (shop_after, shop_id, item_id),
            )
            cur.execute("DELETE FROM shop_stock_transactions WHERE id=%s", (int(tx_id),))
        return True, "Stock out deleted."
    except pymysql.Error as e:
        logger.warning("delete_shop_manual_stock_out failed: %s", e)
        return False, "Could not delete stock out."


def update_shop_manual_stock_out(
    tx_id: int,
    *,
    note: Optional[str] = None,
) -> Tuple[bool, str, Optional[dict]]:
    """Edit notes on a manual stock-out expenditure line."""
    allowed_reasons = {"RETURN", "WASTE", "DISPLAY"}
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(
                """
                SELECT id, direction, source, reason
                FROM shop_stock_transactions
                WHERE id=%s
                FOR UPDATE
                """,
                (int(tx_id),),
            )
            tx = cur.fetchone()
            if not tx:
                return False, "Transaction not found.", None
            if (tx.get("source") or "").strip().lower() != "manual":
                return False, "Only manual stock-out lines can be edited.", None
            if (tx.get("direction") or "").strip().lower() != "out":
                return False, "Only stock-out lines can be edited here.", None
            reason = (tx.get("reason") or "").strip().upper()
            if reason not in allowed_reasons:
                return False, "This stock-out type cannot be edited from here.", None
            cur.execute(
                "UPDATE shop_stock_transactions SET note=%s WHERE id=%s",
                ((note or "").strip() or None, int(tx_id)),
            )
        return True, "Stock out updated.", {"id": int(tx_id)}
    except pymysql.Error as e:
        logger.warning("update_shop_manual_stock_out failed: %s", e)
        return False, "Could not update stock out.", None


def _serialize_operational_expense_row(r: dict) -> dict:
    try:
        qty = float(r.get("qty") or 0)
    except Exception:
        qty = 0.0
    try:
        unit = float(r.get("unit_price") or 0)
    except Exception:
        unit = 0.0
    total = round(float(r.get("total_amount") or qty * unit), 2)
    created = r.get("created_at")
    if hasattr(created, "strftime"):
        created_out = created.strftime("%d %b %Y %H:%M")
        created_iso = created.strftime("%Y-%m-%dT%H:%M:%S")
    else:
        created_out = str(created or "")
        created_iso = created_out
    return {
        "id": int(r.get("id") or 0),
        "shop_id": int(r.get("shop_id") or 0),
        "shop_name": (r.get("shop_name") or "").strip(),
        "shop_code": (r.get("shop_code") or "").strip(),
        "category_id": int(r.get("category_id") or 0),
        "expense_catalog_id": int(r.get("expense_catalog_id") or 0),
        "category": (r.get("category_name") or "").strip(),
        "name": (r.get("expense_name") or "").strip(),
        "qty": qty,
        "unit_price": round(unit, 2),
        "total_cost": total,
        "total_amount": total,
        "payment_status": (r.get("payment_status") or "pending_payment").strip().lower(),
        "amount_paid": round(float(r.get("amount_paid") or 0), 2),
        "balance": round(max(0.0, total - float(r.get("amount_paid") or 0)), 2),
        "note": (r.get("note") or "").strip(),
        "supplier_name": (r.get("supplier_name") or "").strip(),
        "seller_phone": (r.get("seller_phone") or "").strip(),
        "created_by": (r.get("created_by") or "").strip(),
        "created_by_employee_id": int(r.get("created_by_employee_id") or 0) or None,
        "created_at": created_out,
        "created_at_iso": created_iso,
        "expense_kind": "operational",
        "supplier": (r.get("supplier_name") or "").strip() or "—",
        "buying_price": round(unit, 2),
    }


def list_shop_operational_expenses_for_report(shop_id: int, analytics_filter: dict) -> list:
    rows = list_shop_operational_expenses(shop_id=shop_id, analytics_filter=analytics_filter, limit=5000)
    return [
        {
            **r,
            "expense_kind": "operational",
            "supplier": (r.get("supplier_name") or "—").strip() or "—",
            "buying_price": r.get("unit_price"),
        }
        for r in rows
    ]


def list_shop_operational_expenses(
    *,
    shop_id: int,
    analytics_filter: Optional[dict] = None,
    limit: int = 500,
) -> list:
    try:
        sid = int(shop_id)
    except Exception:
        return []
    if sid <= 0:
        return []
    init_operational_expense_tables()
    lim = max(1, min(int(limit or 500), 8000))
    where, params = _analytics_where_clause(analytics_filter or {}, "soe")
    where = f"({where}) AND soe.shop_id = %s"
    params = list(params) + [sid]
    sql = f"""
    SELECT
        soe.*,
        COALESCE(e.full_name, '') AS created_by
    FROM shop_operational_expenses soe
    LEFT JOIN employees e ON e.id = soe.created_by_employee_id
    WHERE {where}
    ORDER BY soe.created_at DESC, soe.id DESC
    LIMIT %s
    """
    params.append(lim)
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
        return [_serialize_operational_expense_row(r) for r in rows]
    except pymysql.Error as e:
        logger.warning("list_shop_operational_expenses failed shop=%s: %s", sid, e)
        return []


def _serialize_shop_stock_purchase_row(r: dict) -> dict:
    try:
        qty = float(r.get("qty") or 0)
    except Exception:
        qty = 0.0
    try:
        unit = float(r.get("buying_price") or 0)
    except Exception:
        unit = 0.0
    total = round(qty * unit, 2)
    amount_paid = round(float(r.get("amount_paid") or 0), 2)
    created = r.get("created_at")
    if hasattr(created, "strftime"):
        created_out = created.strftime("%d %b %Y %H:%M")
        created_iso = created.strftime("%Y-%m-%dT%H:%M:%S")
    else:
        created_out = str(created or "")
        created_iso = created_out
    supplier_name = (r.get("place_brought_from") or "").strip()
    seller_phone = (r.get("seller_phone") or "").strip()
    return {
        "id": int(r.get("id") or 0),
        "shop_id": int(r.get("shop_id") or 0),
        "category": (r.get("category") or "").strip(),
        "name": (r.get("item_name") or "").strip() or f"Item #{r.get('item_id') or r.get('store_stock_item_id') or ''}",
        "qty": qty,
        "unit_price": round(unit, 2),
        "buying_price": round(unit, 2),
        "total_cost": total,
        "payment_status": (r.get("payment_status") or "pending_payment").strip().lower(),
        "amount_paid": amount_paid,
        "balance": round(max(0.0, total - amount_paid), 2),
        "note": (r.get("note") or "").strip(),
        "supplier_name": supplier_name,
        "seller_phone": seller_phone,
        "supplier": supplier_name or "—",
        "created_by": (r.get("created_by") or "").strip(),
        "created_at": created_out,
        "created_at_iso": created_iso,
        "expense_kind": "stock",
        "stock_source": (r.get("stock_source") or "shop").strip(),
        "shop_name": (r.get("shop_name") or "").strip(),
    }


def list_shop_stock_purchases(
    *,
    shop_id: int,
    analytics_filter: Optional[dict] = None,
    limit: int = 8000,
) -> list:
    """Manual stock-in purchases for one shop (catalog + store stock in both mode)."""
    try:
        sid = int(shop_id)
    except Exception:
        return []
    if sid <= 0:
        return []
    init_shop_stock_transactions_table()
    init_store_stock_transactions_table()
    lim = max(1, min(int(limit or 8000), 8000))
    out: list = []

    where, params = _analytics_where_clause(analytics_filter or {}, "sst")
    where = f"({where}) AND sst.shop_id = %s AND sst.direction = 'in' AND sst.source = 'manual'"
    params = list(params) + [sid]
    sql_shop = f"""
    SELECT
        sst.id,
        sst.shop_id,
        sst.item_id,
        sst.qty,
        sst.buying_price,
        sst.place_brought_from,
        sst.seller_phone,
        sst.payment_status,
        sst.amount_paid,
        sst.note,
        sst.created_at,
        i.name AS item_name,
        i.category,
        COALESCE(e.full_name, '') AS created_by,
        'shop' AS stock_source
    FROM shop_stock_transactions sst
    JOIN items i ON i.id = sst.item_id
    LEFT JOIN employees e ON e.id = sst.created_by_employee_id
    WHERE {where}
    ORDER BY sst.created_at DESC, sst.id DESC
    LIMIT %s
    """
    params_shop = list(params) + [lim]
    try:
        with get_cursor() as cur:
            cur.execute(sql_shop, tuple(params_shop))
            rows = cur.fetchall() or []
        out.extend(_serialize_shop_stock_purchase_row(dict(r)) for r in rows)
    except pymysql.Error as e:
        logger.warning("list_shop_stock_purchases shop items failed shop=%s: %s", sid, e)

    where2, params2 = _analytics_where_clause(analytics_filter or {}, "sst")
    where2 = f"({where2}) AND sst.shop_id = %s AND sst.direction = 'in'"
    params2 = list(params2) + [sid]
    sql_store = f"""
    SELECT
        sst.id,
        sst.shop_id,
        sst.store_stock_item_id,
        sst.qty,
        sst.buying_price,
        sst.place_brought_from,
        sst.seller_phone,
        sst.payment_status,
        sst.amount_paid,
        sst.note,
        sst.created_at,
        ssi.name AS item_name,
        ssi.category,
        COALESCE(e.full_name, '') AS created_by,
        'store' AS stock_source
    FROM store_stock_transactions sst
    JOIN store_stock_items ssi ON ssi.id = sst.store_stock_item_id
    LEFT JOIN employees e ON e.id = sst.created_by_employee_id
    WHERE {where2}
    ORDER BY sst.created_at DESC, sst.id DESC
    LIMIT %s
    """
    params_store = list(params2) + [lim]
    try:
        with get_cursor() as cur:
            cur.execute(sql_store, tuple(params_store))
            rows = cur.fetchall() or []
        out.extend(_serialize_shop_stock_purchase_row(dict(r)) for r in rows)
    except pymysql.Error as e:
        logger.warning("list_shop_stock_purchases store items failed shop=%s: %s", sid, e)

    out.sort(key=lambda x: (str(x.get("created_at_iso") or ""), int(x.get("id") or 0)), reverse=True)
    return out[:lim]


def list_company_operational_expenses(
    *,
    analytics_filter: Optional[dict] = None,
    shop_id: Optional[int] = None,
    limit: int = 8000,
    settled_only: bool = False,
) -> list:
    init_operational_expense_tables()
    lim = max(1, min(int(limit or 8000), 8000))
    where, params = _analytics_where_clause(analytics_filter or {}, "soe")
    if settled_only:
        where = f"({where}) AND {_operational_expense_settled_clause('soe')}"
    if shop_id is not None:
        try:
            sid = int(shop_id)
        except Exception:
            sid = 0
        if sid > 0:
            where = f"({where}) AND soe.shop_id = %s"
            params = list(params) + [sid]
    sql = f"""
    SELECT
        soe.*,
        COALESCE(e.full_name, '') AS created_by,
        s.shop_name,
        s.shop_code
    FROM shop_operational_expenses soe
    JOIN shops s ON s.id = soe.shop_id
    LEFT JOIN employees e ON e.id = soe.created_by_employee_id
    WHERE {where}
    ORDER BY soe.created_at DESC, soe.id DESC
    LIMIT %s
    """
    params.append(lim)
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
        return [_serialize_operational_expense_row(r) for r in rows]
    except pymysql.Error as e:
        logger.warning("list_company_operational_expenses failed: %s", e)
        return []


def sum_shop_operational_expenses(shop_id: int, analytics_filter: dict) -> float:
    try:
        sid = int(shop_id)
    except Exception:
        return 0.0
    if sid <= 0:
        return 0.0
    init_operational_expense_tables()
    where, params = _analytics_where_clause(analytics_filter, "soe")
    where = f"({where}) AND soe.shop_id = %s"
    params = list(params) + [sid]
    sql = f"SELECT COALESCE(SUM(soe.total_amount), 0) AS total FROM shop_operational_expenses soe WHERE {where}"
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(params))
            row = cur.fetchone() or {}
        return round(float(row.get("total") or 0), 2)
    except pymysql.Error:
        return 0.0


def sum_company_operational_expenses(analytics_filter: dict, shop_id: Optional[int] = None) -> float:
    """Sum amount paid on settled (partially paid or paid) operational expenses."""
    init_operational_expense_tables()
    where, params = _analytics_where_clause(analytics_filter, "soe")
    where = f"({where}) AND {_operational_expense_settled_clause('soe')}"
    if shop_id is not None:
        try:
            sid = int(shop_id)
        except Exception:
            sid = 0
        if sid > 0:
            where = f"({where}) AND soe.shop_id = %s"
            params = list(params) + [sid]
    sql = f"SELECT COALESCE(SUM(soe.amount_paid), 0) AS total FROM shop_operational_expenses soe WHERE {where}"
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(params))
            row = cur.fetchone() or {}
        return round(float(row.get("total") or 0), 2)
    except pymysql.Error:
        return 0.0


def list_company_operational_expenses_for_report(
    analytics_filter: Optional[dict] = None,
    shop_id: Optional[int] = None,
    limit: int = 5000,
) -> list:
    """Settled operational expenses (partially paid or paid) for company period report."""
    return list_company_operational_expenses(
        analytics_filter=analytics_filter,
        shop_id=shop_id,
        limit=limit,
        settled_only=True,
    )


def _expenditure_shop_sql_filter(shop_filter: Optional[int], alias: str = "sst") -> tuple:
    """Optional shop_id constraint for company-wide expenditure bulk queries."""
    if shop_filter is not None:
        try:
            sid = int(shop_filter)
        except Exception:
            sid = 0
        if sid > 0:
            a = (alias or "sst").strip() or "sst"
            return f" AND {a}.shop_id = %s", [sid]
    return "", []


def _list_company_shop_stock_purchases_bulk(
    analytics_filter: dict,
    shop_filter: Optional[int] = None,
    limit: int = 8000,
) -> list:
    """Manual stock-in purchases across shops (or one shop) in a single round-trip."""
    af = analytics_filter or {}
    init_shop_stock_transactions_table()
    init_store_stock_transactions_table()
    lim = max(1, min(int(limit or 8000), 8000))
    out: list = []

    where, params = _analytics_where_clause(af, "sst")
    shop_clause, shop_params = _expenditure_shop_sql_filter(shop_filter, "sst")
    where = f"({where}) AND sst.direction = 'in' AND sst.source = 'manual'{shop_clause}"
    params = list(params) + list(shop_params)
    sql_shop = f"""
    SELECT
        sst.id,
        sst.shop_id,
        sst.item_id,
        sst.qty,
        sst.buying_price,
        sst.place_brought_from,
        sst.seller_phone,
        sst.payment_status,
        sst.amount_paid,
        sst.note,
        sst.created_at,
        i.name AS item_name,
        i.category,
        COALESCE(e.full_name, '') AS created_by,
        COALESCE(sh.shop_name, '') AS shop_name,
        'shop' AS stock_source
    FROM shop_stock_transactions sst
    JOIN items i ON i.id = sst.item_id
    JOIN shops sh ON sh.id = sst.shop_id
    LEFT JOIN employees e ON e.id = sst.created_by_employee_id
    WHERE {where}
    ORDER BY sst.created_at DESC, sst.id DESC
    LIMIT %s
    """
    params_shop = list(params) + [lim]
    try:
        with get_cursor() as cur:
            cur.execute(sql_shop, tuple(params_shop))
            rows = cur.fetchall() or []
        out.extend(_serialize_shop_stock_purchase_row(dict(r)) for r in rows)
    except pymysql.Error as e:
        logger.warning("_list_company_shop_stock_purchases_bulk catalog failed: %s", e)

    where2, params2 = _analytics_where_clause(af, "sst")
    shop_clause2, shop_params2 = _expenditure_shop_sql_filter(shop_filter, "sst")
    where2 = f"({where2}) AND sst.direction = 'in'{shop_clause2}"
    params2 = list(params2) + list(shop_params2)
    sql_store = f"""
    SELECT
        sst.id,
        sst.shop_id,
        sst.store_stock_item_id,
        sst.qty,
        sst.buying_price,
        sst.place_brought_from,
        sst.seller_phone,
        sst.payment_status,
        sst.amount_paid,
        sst.note,
        sst.created_at,
        ssi.name AS item_name,
        ssi.category,
        COALESCE(e.full_name, '') AS created_by,
        COALESCE(sh.shop_name, '') AS shop_name,
        'store' AS stock_source
    FROM store_stock_transactions sst
    JOIN store_stock_items ssi ON ssi.id = sst.store_stock_item_id
    JOIN shops sh ON sh.id = sst.shop_id
    LEFT JOIN employees e ON e.id = sst.created_by_employee_id
    WHERE {where2}
    ORDER BY sst.created_at DESC, sst.id DESC
    LIMIT %s
    """
    params_store = list(params2) + [lim]
    try:
        with get_cursor() as cur:
            cur.execute(sql_store, tuple(params_store))
            rows = cur.fetchall() or []
        out.extend(_serialize_shop_stock_purchase_row(dict(r)) for r in rows)
    except pymysql.Error as e:
        logger.warning("_list_company_shop_stock_purchases_bulk store failed: %s", e)

    out.sort(key=lambda x: (str(x.get("created_at_iso") or ""), int(x.get("id") or 0)), reverse=True)
    return out[:lim]


def _list_company_shop_stock_outs_bulk(
    analytics_filter: dict,
    shop_filter: Optional[int] = None,
    limit: int = 8000,
) -> list:
    """Manual stock-out rows across shops (or one shop) in a single query."""
    init_shop_stock_transactions_table()
    if not column_exists("shop_stock_transactions", "cost_total"):
        return []
    af = analytics_filter or {}
    lim = max(1, min(int(limit or 8000), 8000))
    where, params = _analytics_where_clause(af, "sst")
    shop_clause, shop_params = _expenditure_shop_sql_filter(shop_filter, "sst")
    where = (
        f"({where}) AND sst.direction = 'out' AND sst.source = 'manual' "
        f"AND UPPER(COALESCE(sst.reason, '')) IN ('RETURN', 'WASTE', 'DISPLAY'){shop_clause}"
    )
    params = list(params) + list(shop_params)
    sql = f"""
    SELECT
        sst.id,
        sst.shop_id,
        sst.item_id,
        sst.qty,
        sst.buying_price,
        sst.cost_total,
        sst.reason,
        sst.refunded,
        sst.refund_amount,
        sst.note,
        sst.created_at,
        i.name AS item_name,
        i.category,
        COALESCE(e.full_name, '') AS created_by,
        COALESCE(sh.shop_name, '') AS shop_name
    FROM shop_stock_transactions sst
    JOIN items i ON i.id = sst.item_id
    JOIN shops sh ON sh.id = sst.shop_id
    LEFT JOIN employees e ON e.id = sst.created_by_employee_id
    WHERE {where}
    ORDER BY sst.created_at DESC, sst.id DESC
    LIMIT %s
    """
    params.append(lim)
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
        return [_serialize_shop_stock_out_expenditure_row(dict(r)) for r in rows]
    except pymysql.Error as e:
        logger.warning("_list_company_shop_stock_outs_bulk failed: %s", e)
        return []


def list_company_expenditure_for_report(analytics_filter: dict, shop_id: Optional[int] = None) -> list:
    """All expenditure across company warehouse and shops (stock, operational, stock out)."""
    af = analytics_filter or {}
    out: list = []
    shop_filter: Optional[int] = None
    if shop_id is not None:
        try:
            sid = int(shop_id)
        except Exception:
            sid = 0
        if sid > 0:
            shop_filter = sid

    if shop_filter is None:
        for r in list_company_supplier_stock_ins(analytics_filter=af, shop_id=None, limit=5000) or []:
            scope = str(r.get("tx_scope") or "").strip().lower()
            if scope != "company" and int(r.get("shop_id") or 0) != 0:
                continue
            out.append(_serialize_company_warehouse_stock_expenditure_row(dict(r)))

    for row in _list_company_shop_stock_purchases_bulk(af, shop_filter=shop_filter, limit=8000):
        item = dict(row)
        if not (item.get("shop_name") or "").strip():
            sid = int(item.get("shop_id") or 0)
            item["shop_name"] = f"Shop #{sid}" if sid > 0 else "—"
        out.append(item)

    for row in list_company_operational_expenses(
        analytics_filter=af,
        shop_id=shop_filter,
        limit=8000,
        settled_only=False,
    ):
        out.append(dict(row))

    out.extend(_list_company_shop_stock_outs_bulk(af, shop_filter=shop_filter, limit=8000))

    out.sort(
        key=lambda x: (str(x.get("created_at_iso") or x.get("created_at") or ""), int(x.get("id") or 0)),
        reverse=True,
    )
    return out


def _expenditure_total_by_shop_map(
    analytics_filter: dict, shop_id: Optional[int] = None
) -> dict:
    """Total expenditure per shop for the analytics period (matches report rows)."""
    totals: dict[int, float] = {}
    rows = list_company_expenditure_for_report(analytics_filter or {}, shop_id=shop_id)
    for r in rows:
        ps = (r.get("payment_status") or "pending_payment").strip().lower()
        if ps == "cancelled_out":
            continue
        try:
            sid = int(r.get("shop_id") or 0)
        except Exception:
            continue
        if sid <= 0:
            continue
        try:
            row_total = float(r.get("total_cost") or r.get("total_amount") or 0)
        except Exception:
            row_total = 0.0
        totals[sid] = round(totals.get(sid, 0.0) + row_total, 2)
    return totals


def get_it_support_expense_analytics(
    analytics_filter: dict, shop_id: Optional[int] = None, *, lines_limit: int = 120
):
    """Expense analytics across shops for IT support/super admin."""
    try:
        lim = max(1, min(500, int(lines_limit)))
    except (TypeError, ValueError):
        lim = 120
    rows = list_company_expenditure_for_report(analytics_filter or {}, shop_id=shop_id)
    active = [
        r
        for r in rows
        if (r.get("payment_status") or "pending_payment").strip().lower() != "cancelled_out"
    ]

    stock_total = 0.0
    operational_total = 0.0
    stock_out_total = 0.0
    shop_map: dict[int, dict] = {}

    for r in active:
        try:
            amt = float(r.get("total_cost") or r.get("total_amount") or 0)
        except Exception:
            amt = 0.0
        kind = (r.get("expense_kind") or "").strip().lower()
        if kind == "operational":
            operational_total += amt
        elif kind == "stock_out":
            stock_out_total += amt
        else:
            stock_total += amt

        try:
            sid = int(r.get("shop_id") or 0)
        except Exception:
            sid = 0
        if sid <= 0:
            continue
        if sid not in shop_map:
            shop_map[sid] = {
                "shop_id": sid,
                "shop_name": (r.get("shop_name") or f"Shop #{sid}").strip(),
                "stock_amount": 0.0,
                "operational_amount": 0.0,
                "stock_out_amount": 0.0,
                "total_amount": 0.0,
            }
        entry = shop_map[sid]
        if kind == "operational":
            entry["operational_amount"] += amt
        elif kind == "stock_out":
            entry["stock_out_amount"] += amt
        else:
            entry["stock_amount"] += amt
        entry["total_amount"] += amt

    shops = sorted(shop_map.values(), key=lambda x: (-x["total_amount"], x["shop_name"]))
    for s in shops:
        for key in ("stock_amount", "operational_amount", "stock_out_amount", "total_amount"):
            s[key] = round(float(s[key] or 0), 2)

    lines = []
    for r in active[:lim]:
        try:
            amt = float(r.get("total_cost") or r.get("total_amount") or 0)
        except Exception:
            amt = 0.0
        lines.append(
            {
                "created_at": r.get("created_at") or "",
                "shop_name": (r.get("shop_name") or "—").strip() or "—",
                "expense_kind": (r.get("expense_kind") or "stock").strip().lower(),
                "name": (r.get("name") or r.get("expense_name") or "—").strip() or "—",
                "category": (r.get("category") or "—").strip() or "—",
                "total_amount": round(amt, 2),
                "amount_paid": round(float(r.get("amount_paid") or 0), 2),
                "payment_status": (r.get("payment_status") or "").strip().lower(),
                "supplier": (r.get("supplier") or r.get("supplier_name") or "—").strip() or "—",
            }
        )

    return {
        "total_amount": round(stock_total + operational_total + stock_out_total, 2),
        "stock_amount": round(stock_total, 2),
        "operational_amount": round(operational_total, 2),
        "stock_out_amount": round(stock_out_total, 2),
        "line_count": len(active),
        "shops": shops,
        "lines": lines,
    }


def log_hr_activity(
    employee_id: Optional[int],
    action_kind: str,
    *,
    target_type: Optional[str] = None,
    target_id: Optional[int] = None,
    description: Optional[str] = None,
    ip_address: Optional[str] = None,
    user_agent: Optional[str] = None,
    employee_full_name: Optional[str] = None,
    employee_role: Optional[str] = None,
) -> bool:
    """Append a row to hr_activity_log. Silent on failure so callers never break."""
    kind = (action_kind or "other").strip().lower() or "other"
    if kind not in HR_ACTIVITY_ACTION_KINDS:
        kind = "other"
    emp_id_val: Optional[int]
    try:
        emp_id_val = int(employee_id) if employee_id is not None else None
        if emp_id_val is not None and emp_id_val <= 0:
            emp_id_val = None
    except (TypeError, ValueError):
        emp_id_val = None
    name_snap = (employee_full_name or "").strip() or None
    role_snap = (employee_role or "").strip().lower() or None
    if emp_id_val is not None and (not name_snap or not role_snap):
        try:
            with get_cursor() as cur:
                cur.execute(
                    "SELECT full_name, role FROM employees WHERE id = %s LIMIT 1",
                    (emp_id_val,),
                )
                row = cur.fetchone() or {}
                name_snap = name_snap or (row.get("full_name") or None)
                role_snap = role_snap or (row.get("role") or None)
        except pymysql.Error:
            pass
    tgt_id_val: Optional[int]
    try:
        tgt_id_val = int(target_id) if target_id is not None else None
    except (TypeError, ValueError):
        tgt_id_val = None
    desc = (description or "").strip() or None
    if desc and len(desc) > 500:
        desc = desc[:500]
    ip_val = (ip_address or "").strip() or None
    if ip_val and len(ip_val) > 64:
        ip_val = ip_val[:64]
    ua = (user_agent or "").strip() or None
    if ua and len(ua) > 255:
        ua = ua[:255]
    sql = (
        "INSERT INTO hr_activity_log "
        "(employee_id, employee_full_name, employee_role, action_kind, target_type, "
        "target_id, description, ip_address, user_agent) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    )
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(
                sql,
                (
                    emp_id_val,
                    name_snap,
                    role_snap,
                    kind,
                    (target_type or None),
                    tgt_id_val,
                    desc,
                    ip_val,
                    ua,
                ),
            )
        return True
    except pymysql.Error as e:
        logger.warning("log_hr_activity failed: %s", e)
        return False


def list_hr_activity_for_employee(
    employee_id: int,
    *,
    limit: int = 500,
    action_kinds: Optional[Tuple[str, ...]] = None,
):
    """Return ordered activity rows (latest first) for a single employee."""
    try:
        emp_id_val = int(employee_id)
    except (TypeError, ValueError):
        return []
    if emp_id_val <= 0:
        return []
    try:
        lim = int(limit)
    except (TypeError, ValueError):
        lim = 500
    lim = max(1, min(5000, lim))

    where = ["employee_id = %s"]
    params: list[Any] = [emp_id_val]
    if action_kinds:
        kinds = tuple(k for k in action_kinds if k in HR_ACTIVITY_ACTION_KINDS)
        if kinds:
            placeholders = ", ".join(["%s"] * len(kinds))
            where.append(f"action_kind IN ({placeholders})")
            params.extend(kinds)
    sql = (
        "SELECT id, employee_id, employee_full_name, employee_role, action_kind, "
        "target_type, target_id, description, ip_address, user_agent, created_at "
        "FROM hr_activity_log "
        f"WHERE {' AND '.join(where)} "
        "ORDER BY created_at DESC, id DESC LIMIT %s"
    )
    params.append(lim)
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(params))
            return cur.fetchall() or []
    except pymysql.Error:
        return []


def _seconds_to_hms(total_seconds: float) -> str:
    try:
        total = int(max(0, float(total_seconds)))
    except (TypeError, ValueError):
        return "0h 0m"
    hours = total // 3600
    minutes = (total % 3600) // 60
    if hours <= 0:
        return f"{minutes}m"
    return f"{hours}h {minutes}m"


def get_employee_session_analytics(
    employee_id: int,
    *,
    lookback_days: int = 90,
    recent_limit: int = 50,
) -> dict:
    """Aggregate sessions and activity counts for a single employee.

    Pairs each ``login`` event with the next chronological ``logout``. Lingering
    open sessions (login without a paired logout) are reported separately.
    """
    empty = {
        "employee_id": int(employee_id) if employee_id else 0,
        "session_count": 0,
        "open_session_count": 0,
        "total_seconds": 0,
        "total_hours_label": "0h 0m",
        "total_hours_decimal": 0.0,
        "avg_session_seconds": 0,
        "avg_session_label": "0m",
        "longest_session_seconds": 0,
        "longest_session_label": "0m",
        "last_login_at": None,
        "last_logout_at": None,
        "first_seen_at": None,
        "kind_counts": {k: 0 for k in HR_ACTIVITY_ACTION_KINDS},
        "sessions": [],
        "recent_activities": [],
        "lookback_days": int(lookback_days) if lookback_days else 90,
    }
    try:
        emp_id_val = int(employee_id)
    except (TypeError, ValueError):
        return empty
    if emp_id_val <= 0:
        return empty
    try:
        lookback = int(lookback_days)
    except (TypeError, ValueError):
        lookback = 90
    lookback = max(1, min(3650, lookback))
    cutoff = datetime.now() - timedelta(days=lookback)

    # Pull every event in the window in chronological order to build session pairs.
    sql_events = (
        "SELECT id, action_kind, target_type, target_id, description, ip_address, "
        "user_agent, created_at "
        "FROM hr_activity_log "
        "WHERE employee_id = %s AND created_at >= %s "
        "ORDER BY created_at ASC, id ASC"
    )
    try:
        with get_cursor() as cur:
            cur.execute(sql_events, (emp_id_val, cutoff))
            events = cur.fetchall() or []
    except pymysql.Error:
        events = []

    kind_counts: Dict[str, int] = {k: 0 for k in HR_ACTIVITY_ACTION_KINDS}
    sessions: list[dict] = []
    pending_login: Optional[dict] = None
    open_sessions = 0
    last_login_at = None
    last_logout_at = None
    first_seen_at = None
    longest_seconds = 0
    total_seconds = 0

    for ev in events:
        kind = (ev.get("action_kind") or "other").lower()
        if kind not in kind_counts:
            kind_counts[kind] = 0
        kind_counts[kind] += 1
        ts = ev.get("created_at")
        if first_seen_at is None and ts is not None:
            first_seen_at = ts
        if kind == "login":
            last_login_at = ts
            # If there was already an unclosed login, close it as "open" so we don't double-pair.
            if pending_login is not None:
                sessions.append(
                    {
                        "login_at": pending_login.get("created_at"),
                        "logout_at": None,
                        "duration_seconds": 0,
                        "duration_label": "open",
                        "ip_address": pending_login.get("ip_address"),
                        "open": True,
                    }
                )
                open_sessions += 1
            pending_login = ev
        elif kind == "logout":
            last_logout_at = ts
            if pending_login is not None:
                start = pending_login.get("created_at")
                end = ts
                duration = 0
                if isinstance(start, datetime) and isinstance(end, datetime):
                    duration = max(0, int((end - start).total_seconds()))
                if duration > longest_seconds:
                    longest_seconds = duration
                total_seconds += duration
                sessions.append(
                    {
                        "login_at": start,
                        "logout_at": end,
                        "duration_seconds": duration,
                        "duration_label": _seconds_to_hms(duration),
                        "ip_address": pending_login.get("ip_address"),
                        "open": False,
                    }
                )
                pending_login = None
            else:
                # Logout without a paired login (older session). Record as orphan logout.
                sessions.append(
                    {
                        "login_at": None,
                        "logout_at": ts,
                        "duration_seconds": 0,
                        "duration_label": "unknown",
                        "ip_address": ev.get("ip_address"),
                        "open": False,
                    }
                )
    if pending_login is not None:
        sessions.append(
            {
                "login_at": pending_login.get("created_at"),
                "logout_at": None,
                "duration_seconds": 0,
                "duration_label": "open",
                "ip_address": pending_login.get("ip_address"),
                "open": True,
            }
        )
        open_sessions += 1

    closed_sessions = [s for s in sessions if not s["open"] and s["login_at"] is not None]
    session_count = len(closed_sessions)
    avg_seconds = int(total_seconds / session_count) if session_count else 0

    # Show newest sessions first for the UI.
    sessions_ui = list(reversed(sessions))

    # Recent activities — most recent first, capped.
    try:
        rec_limit = int(recent_limit)
    except (TypeError, ValueError):
        rec_limit = 50
    rec_limit = max(1, min(500, rec_limit))
    sql_recent = (
        "SELECT id, action_kind, target_type, target_id, description, ip_address, "
        "user_agent, created_at "
        "FROM hr_activity_log "
        "WHERE employee_id = %s "
        "ORDER BY created_at DESC, id DESC LIMIT %s"
    )
    try:
        with get_cursor() as cur:
            cur.execute(sql_recent, (emp_id_val, rec_limit))
            recent_rows = cur.fetchall() or []
    except pymysql.Error:
        recent_rows = []

    return {
        "employee_id": emp_id_val,
        "session_count": session_count,
        "open_session_count": open_sessions,
        "total_seconds": int(total_seconds),
        "total_hours_label": _seconds_to_hms(total_seconds),
        "total_hours_decimal": round(total_seconds / 3600.0, 2),
        "avg_session_seconds": avg_seconds,
        "avg_session_label": _seconds_to_hms(avg_seconds),
        "longest_session_seconds": int(longest_seconds),
        "longest_session_label": _seconds_to_hms(longest_seconds),
        "last_login_at": last_login_at,
        "last_logout_at": last_logout_at,
        "first_seen_at": first_seen_at,
        "kind_counts": kind_counts,
        "sessions": sessions_ui,
        "recent_activities": recent_rows,
        "lookback_days": lookback,
    }


def get_hr_activity_summary_for_employees(
    employee_ids: Tuple[int, ...],
    *,
    lookback_days: int = 90,
) -> dict:
    """Lightweight bulk summary used by the employee list (no per-employee detail).

    Returns ``{ employee_id: { last_login, last_logout, session_count, total_seconds,
    total_hours_label, login_count, logout_count, change_count } }``.
    """
    out: dict[int, dict] = {}
    ids = []
    for x in employee_ids or ():
        try:
            v = int(x)
        except (TypeError, ValueError):
            continue
        if v > 0:
            ids.append(v)
    if not ids:
        return out
    try:
        lookback = int(lookback_days)
    except (TypeError, ValueError):
        lookback = 90
    lookback = max(1, min(3650, lookback))
    cutoff = datetime.now() - timedelta(days=lookback)

    placeholders = ", ".join(["%s"] * len(ids))
    sql_events = (
        "SELECT employee_id, action_kind, created_at "
        f"FROM hr_activity_log WHERE employee_id IN ({placeholders}) AND created_at >= %s "
        "ORDER BY employee_id ASC, created_at ASC, id ASC"
    )
    params = tuple(ids) + (cutoff,)
    try:
        with get_cursor() as cur:
            cur.execute(sql_events, params)
            rows = cur.fetchall() or []
    except pymysql.Error:
        rows = []

    by_emp: dict[int, list] = {i: [] for i in ids}
    for r in rows:
        eid = r.get("employee_id")
        try:
            eid_int = int(eid)
        except (TypeError, ValueError):
            continue
        if eid_int in by_emp:
            by_emp[eid_int].append(r)

    sql_last_login = (
        "SELECT employee_id, MAX(created_at) AS last_login "
        f"FROM hr_activity_log WHERE employee_id IN ({placeholders}) AND action_kind = 'login' "
        "GROUP BY employee_id"
    )
    sql_last_logout = (
        "SELECT employee_id, MAX(created_at) AS last_logout "
        f"FROM hr_activity_log WHERE employee_id IN ({placeholders}) AND action_kind = 'logout' "
        "GROUP BY employee_id"
    )
    last_login_map: dict[int, Any] = {}
    last_logout_map: dict[int, Any] = {}
    try:
        with get_cursor() as cur:
            cur.execute(sql_last_login, tuple(ids))
            for row in cur.fetchall() or []:
                try:
                    last_login_map[int(row.get("employee_id"))] = row.get("last_login")
                except (TypeError, ValueError):
                    continue
            cur.execute(sql_last_logout, tuple(ids))
            for row in cur.fetchall() or []:
                try:
                    last_logout_map[int(row.get("employee_id"))] = row.get("last_logout")
                except (TypeError, ValueError):
                    continue
    except pymysql.Error:
        pass

    for eid in ids:
        evs = by_emp.get(eid, [])
        pending_login_ts = None
        total_seconds = 0
        session_count = 0
        kind_counts: Dict[str, int] = {k: 0 for k in HR_ACTIVITY_ACTION_KINDS}
        change_count = 0
        for ev in evs:
            kind = (ev.get("action_kind") or "").lower()
            if kind not in kind_counts:
                kind_counts[kind] = 0
            kind_counts[kind] += 1
            ts = ev.get("created_at")
            if kind == "login":
                pending_login_ts = ts
            elif kind == "logout":
                if pending_login_ts is not None and isinstance(pending_login_ts, datetime) and isinstance(ts, datetime):
                    dur = max(0, int((ts - pending_login_ts).total_seconds()))
                    total_seconds += dur
                    session_count += 1
                pending_login_ts = None
            elif kind in ("register", "edit", "update", "delete", "suspend", "unsuspend", "approve"):
                change_count += 1
        out[eid] = {
            "employee_id": eid,
            "last_login_at": last_login_map.get(eid),
            "last_logout_at": last_logout_map.get(eid),
            "session_count": session_count,
            "total_seconds": int(total_seconds),
            "total_hours_label": _seconds_to_hms(total_seconds),
            "login_count": int(kind_counts.get("login") or 0),
            "logout_count": int(kind_counts.get("logout") or 0),
            "change_count": change_count,
            "kind_counts": kind_counts,
        }
    return out


def get_hr_activity_daily_totals(
    *,
    lookback_days: int = 30,
    employee_ids: Optional[Tuple[int, ...]] = None,
) -> list:
    """Daily breakdown of activity counts across all (or selected) employees.

    Returns a list ordered by date ascending of dicts with keys
    ``date`` (date), ``login``, ``logout``, ``register``, ``edit``, ``update``,
    ``delete``, ``other`` and ``total``.
    """
    try:
        lookback = int(lookback_days)
    except (TypeError, ValueError):
        lookback = 30
    lookback = max(1, min(3650, lookback))
    cutoff = datetime.now() - timedelta(days=lookback)

    params: list[Any] = [cutoff]
    emp_filter = ""
    if employee_ids:
        ids = [int(x) for x in employee_ids if x is not None]
        ids = [v for v in ids if v > 0]
        if ids:
            placeholders = ", ".join(["%s"] * len(ids))
            emp_filter = f" AND employee_id IN ({placeholders})"
            params.extend(ids)
    sql = (
        "SELECT DATE(created_at) AS d, action_kind, COUNT(*) AS c "
        "FROM hr_activity_log "
        f"WHERE created_at >= %s{emp_filter} "
        "GROUP BY DATE(created_at), action_kind "
        "ORDER BY DATE(created_at) ASC"
    )
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
    except pymysql.Error:
        rows = []

    by_day: Dict[Any, Dict[str, int]] = {}
    for r in rows:
        d = r.get("d")
        if d is None:
            continue
        kind = (r.get("action_kind") or "other").lower()
        if kind not in HR_ACTIVITY_ACTION_KINDS:
            kind = "other"
        bucket = by_day.setdefault(d, {k: 0 for k in HR_ACTIVITY_ACTION_KINDS})
        try:
            bucket[kind] = int(r.get("c") or 0)
        except (TypeError, ValueError):
            bucket[kind] = 0

    # Fill missing days so the chart has a continuous x-axis.
    end_date = date.today()
    start_date = end_date - timedelta(days=lookback - 1)
    out: list[dict] = []
    cur_day = start_date
    while cur_day <= end_date:
        bucket = by_day.get(cur_day, {k: 0 for k in HR_ACTIVITY_ACTION_KINDS})
        total = sum(int(v or 0) for v in bucket.values())
        out.append(
            {
                "date": cur_day,
                "login": int(bucket.get("login") or 0),
                "logout": int(bucket.get("logout") or 0),
                "register": int(bucket.get("register") or 0),
                "edit": int(bucket.get("edit") or 0),
                "update": int(bucket.get("update") or 0),
                "delete": int(bucket.get("delete") or 0),
                "other": int(
                    (bucket.get("other") or 0)
                    + (bucket.get("suspend") or 0)
                    + (bucket.get("unsuspend") or 0)
                    + (bucket.get("approve") or 0)
                    + (bucket.get("view") or 0)
                ),
                "total": int(total),
            }
        )
        cur_day = cur_day + timedelta(days=1)
    return out


def init_site_settings_table():
    """Key/value site settings (company name, theme, etc)."""
    sql = """
    CREATE TABLE IF NOT EXISTS site_settings (
        `k` VARCHAR(120) NOT NULL PRIMARY KEY,
        `v` TEXT NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql)
        logger.info("Table site_settings is ready.")
        return True
    except pymysql.Error as e:
        logger.warning("Could not init site_settings: %s", e)
        return False


def init_items_table():
    """Inventory items (registered by IT Support)."""
    sql = """
    CREATE TABLE IF NOT EXISTS items (
        id INT AUTO_INCREMENT PRIMARY KEY,
        category VARCHAR(120) NOT NULL,
        name VARCHAR(200) NOT NULL,
        description TEXT NULL,
        price DECIMAL(12,2) NOT NULL DEFAULT 0.00,
        selling_price DECIMAL(12,2) NOT NULL DEFAULT 0.00,
        image_path VARCHAR(500) NULL,
        stock_qty INT NOT NULL DEFAULT 0,
        stock_update_enabled TINYINT(1) NOT NULL DEFAULT 1,
        status ENUM('active', 'suspended') NOT NULL DEFAULT 'active',
        created_by_employee_id INT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        KEY idx_items_status (status),
        KEY idx_items_created_at (created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql)
            if not column_exists("items", "stock_qty"):
                cur.execute("ALTER TABLE items ADD COLUMN stock_qty INT NOT NULL DEFAULT 0")
            if not column_exists("items", "stock_update_enabled"):
                cur.execute("ALTER TABLE items ADD COLUMN stock_update_enabled TINYINT(1) NOT NULL DEFAULT 1")
            if not column_exists("items", "selling_price"):
                cur.execute(
                    "ALTER TABLE items ADD COLUMN selling_price DECIMAL(12,2) NOT NULL DEFAULT 0.00 AFTER price"
                )
                cur.execute("UPDATE items SET selling_price = price")
            if not column_exists("items", "low_stock_threshold"):
                cur.execute(
                    "ALTER TABLE items ADD COLUMN low_stock_threshold INT NOT NULL DEFAULT 0 AFTER stock_update_enabled"
                )
            if not column_exists("items", "reorder_level"):
                cur.execute(
                    "ALTER TABLE items ADD COLUMN reorder_level INT NOT NULL DEFAULT 0 AFTER low_stock_threshold"
                )
            if not column_exists("items", "website_name"):
                cur.execute(
                    "ALTER TABLE items ADD COLUMN website_name VARCHAR(200) NULL AFTER reorder_level"
                )
            if not column_exists("items", "website_description"):
                cur.execute("ALTER TABLE items ADD COLUMN website_description TEXT NULL AFTER website_name")
            if not column_exists("items", "website_published"):
                cur.execute(
                    "ALTER TABLE items ADD COLUMN website_published TINYINT(1) NOT NULL DEFAULT 1 AFTER website_description"
                )
                cur.execute("UPDATE items SET website_published = 1 WHERE website_published IS NULL")
        logger.info("Table items is ready.")
        return True
    except pymysql.Error as e:
        logger.warning("Could not init items: %s", e)
        return False


def create_item(
    category: str,
    name: str,
    description: str,
    price,
    image_path: Optional[str],
    status: str = "active",
    created_by_employee_id: Optional[int] = None,
    selling_price=None,
    *,
    website_name: Optional[str] = None,
    website_description: Optional[str] = None,
    website_published: bool = True,
):
    """``price`` = original selling price (catalog baseline). ``selling_price`` = current POS price (defaults to ``price``)."""
    try:
        original_p = float(price)
    except (TypeError, ValueError):
        original_p = 0.0
    if selling_price is None:
        sell_p = original_p
    else:
        try:
            sell_p = float(selling_price)
        except (TypeError, ValueError):
            sell_p = original_p
    sql = """
    INSERT INTO items (
        category, name, description, price, selling_price, image_path,
        stock_qty, stock_update_enabled,
        status, created_by_employee_id,
        website_name, website_description, website_published
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    with get_cursor(commit=True) as cur:
        cur.execute(
            sql,
            (
                category,
                name,
                description or None,
                original_p,
                sell_p,
                image_path or None,
                0,
                1,
                status,
                created_by_employee_id,
                (website_name or "").strip() or None,
                (website_description or "").strip() or None,
                1 if website_published else 0,
            ),
        )
        return cur.lastrowid


def list_items(limit: int = 200):
    sql = """
    SELECT id, category, name, description, price, selling_price, image_path, stock_qty, stock_update_enabled, status, created_at
    FROM items
    ORDER BY created_at DESC
    LIMIT %s
    """
    with get_cursor() as cur:
        cur.execute(sql, (int(limit),))
        return cur.fetchall() or []


def list_items_for_company_stock_settings(limit: int = 5000, inventory_mode: Optional[str] = None):
    """All catalog items with alert fields for the company stock settings page."""
    m = (inventory_mode or "").strip().lower()
    if m == "kitchen":
        return []
    if m == "both":
        init_store_stock_items_table()
        sql = """
        SELECT
            ssi.id,
            ssi.category,
            ssi.name,
            NULL AS image_path,
            0 AS stock_qty,
            1 AS stock_update_enabled,
            ssi.status
        FROM store_stock_items ssi
        WHERE ssi.status = 'active'
        ORDER BY ssi.name ASC, ssi.id ASC
        LIMIT %s
        """
        with get_cursor() as cur:
            cur.execute(sql, (int(limit),))
            rows = cur.fetchall() or []
        for r in rows:
            r["low_stock_threshold"] = 0
            r["reorder_level"] = 0
        return rows
    mode_filter = ""
    has_levels = column_exists("items", "low_stock_threshold") and column_exists("items", "reorder_level")
    if has_levels:
        sql = f"""
        SELECT id, category, name, image_path, stock_qty, stock_update_enabled, status,
               low_stock_threshold, reorder_level
        FROM items
        {mode_filter}
        ORDER BY name ASC, id ASC
        LIMIT %s
        """
    else:
        sql = f"""
        SELECT id, category, name, image_path, stock_qty, stock_update_enabled, status
        FROM items
        {mode_filter}
        ORDER BY name ASC, id ASC
        LIMIT %s
        """
    with get_cursor() as cur:
        cur.execute(sql, (int(limit),))
        rows = cur.fetchall() or []
    for r in rows:
        if has_levels:
            r["low_stock_threshold"] = int(r.get("low_stock_threshold") or 0)
            r["reorder_level"] = int(r.get("reorder_level") or 0)
        else:
            r["low_stock_threshold"] = 0
            r["reorder_level"] = 0
    return rows


def set_item_stock_alert_levels(item_id: int, low_stock_threshold: int, reorder_level: int) -> bool:
    """Persist per-item low stock and reorder level (company stock settings)."""
    if not column_exists("items", "low_stock_threshold") or not column_exists("items", "reorder_level"):
        return False
    try:
        lo = max(0, min(999999, int(low_stock_threshold)))
        rl = max(0, min(999999, int(reorder_level)))
        iid = int(item_id)
    except Exception:
        return False
    sql = """
    UPDATE items
    SET low_stock_threshold=%s, reorder_level=%s
    WHERE id=%s
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql, (lo, rl, iid))
        return True
    except Exception:
        return False


def get_company_reorder_totals_from_shops() -> Dict[int, int]:
    """Sum per-item reorder levels across all shops."""
    if not column_exists("shop_items", "reorder_level"):
        return {}
    sql = """
    SELECT item_id, COALESCE(SUM(GREATEST(reorder_level, 0)), 0) AS reorder_total
    FROM shop_items
    GROUP BY item_id
    """
    with get_cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall() or []
    totals: Dict[int, int] = {}
    for r in rows:
        try:
            iid = int(r.get("item_id") or 0)
        except Exception:
            continue
        if iid <= 0:
            continue
        totals[iid] = int(r.get("reorder_total") or 0)
    return totals


def list_active_items(limit: int = 200):
    sql = """
    SELECT id, category, name, description, price, selling_price, image_path, stock_qty, stock_update_enabled, status, created_at
    FROM items
    WHERE status='active'
    ORDER BY created_at DESC
    LIMIT %s
    """
    with get_cursor() as cur:
        cur.execute(sql, (int(limit),))
        return cur.fetchall() or []


def list_public_equipment_catalog(limit_items: int = 500):
    """
    Active catalog items (POS sales ranking): ordered by total POS qty sold (all shops),
    then category and name. Includes qty_sold for badges and featured picks.
    """
    sql = """
    SELECT
        i.id,
        i.category,
        i.name,
        i.description,
        i.price,
        i.selling_price,
        i.image_path,
        COALESCE(sq.qty_sold, 0) AS qty_sold
    FROM items i
    LEFT JOIN (
        SELECT item_id, SUM(qty) AS qty_sold
        FROM shop_pos_sale_items
        WHERE item_id IS NOT NULL
        GROUP BY item_id
    ) sq ON sq.item_id = i.id
    WHERE i.status = 'active'
    ORDER BY qty_sold DESC, i.category ASC, i.name ASC
    LIMIT %s
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (int(limit_items),))
            rows = cur.fetchall() or []
    except pymysql.Error:
        return []
    out = []
    for r in rows:
        rr = dict(r)
        try:
            rr["qty_sold"] = int(rr.get("qty_sold") or 0)
        except (TypeError, ValueError):
            rr["qty_sold"] = 0
        try:
            sell = float(rr.get("selling_price") if rr.get("selling_price") is not None else rr.get("price") or 0)
        except (TypeError, ValueError):
            sell = 0.0
        try:
            orig = float(rr.get("price") or 0)
        except (TypeError, ValueError):
            orig = 0.0
        rr["display_price"] = round(sell, 2)
        rr["original_price"] = round(orig, 2)
        ip = rr.get("image_path")
        if isinstance(ip, bytes):
            ip = ip.decode("utf-8", errors="replace")
        ip = (str(ip).strip() if ip is not None else "") or None
        if ip:
            ip = ip.replace("\\", "/")
            while ip.startswith("/"):
                ip = ip[1:]
            if ip.lower().startswith("static/"):
                ip = ip[7:]
            rr["image_path"] = ip
        else:
            rr["image_path"] = None
        out.append(rr)
    return out


def get_item_by_id(item_id: int):
    sql = """
    SELECT id, category, name, description, price, selling_price, image_path, stock_qty, stock_update_enabled, status,
           website_name, website_description, website_published, created_at
    FROM items
    WHERE id = %s
    LIMIT 1
    """
    with get_cursor() as cur:
        cur.execute(sql, (int(item_id),))
        return cur.fetchone()


def update_item(
    item_id: int,
    *,
    category: str,
    name: str,
    description: str,
    price,
    selling_price,
    image_path,
    stock_qty,
    website_name: Optional[str] = None,
    website_description: Optional[str] = None,
    website_published: bool = True,
):
    sql = """
    UPDATE items
    SET category=%s, name=%s, description=%s, price=%s, selling_price=%s, image_path=%s, stock_qty=%s,
        website_name=%s, website_description=%s, website_published=%s
    WHERE id=%s
    """
    with get_cursor(commit=True) as cur:
        cur.execute(
            sql,
            (
                category,
                name,
                description or None,
                price,
                selling_price,
                image_path,
                round(float(stock_qty), STOCK_QTY_DECIMAL_PLACES),
                (website_name or "").strip() or None,
                (website_description or "").strip() or None,
                1 if website_published else 0,
                int(item_id),
            ),
        )
        return True


def update_item_selling_price_for_shop(shop_id: int, item_id: int, selling_price) -> bool:
    """Update per-shop selling price on ``shop_items`` (does not change other branches)."""
    try:
        sp = float(selling_price)
    except (TypeError, ValueError):
        return False
    if sp < 0:
        return False
    init_shop_items_table()
    sql = """
    UPDATE shop_items
    SET selling_price = %s
    WHERE shop_id = %s AND item_id = %s
    """
    with get_cursor(commit=True) as cur:
        cur.execute(sql, (sp, int(shop_id), int(item_id)))
        return cur.rowcount > 0


def sync_shop_items_from_company_item(item_id: int) -> None:
    """
    Apply company item master flags to all shop_items rows for this item.
    - Suspended at company: force shop display off and shop stock updates off.
    - Active but company stock updates off: force shop stock updates off (display unchanged).
    - Active and company stock on: no automatic shop-level override (each branch keeps its own toggles).
    """
    sql_item = "SELECT status, stock_update_enabled FROM items WHERE id=%s LIMIT 1"
    with get_cursor() as cur:
        cur.execute(sql_item, (int(item_id),))
        row = cur.fetchone()
    if not row:
        return
    active = (row.get("status") or "") == "active"
    comp_stock = int(row.get("stock_update_enabled") or 0) == 1
    with get_cursor(commit=True) as cur:
        if not active:
            cur.execute(
                "UPDATE shop_items SET displayed=0, stock_update_enabled=0 WHERE item_id=%s",
                (int(item_id),),
            )
        elif not comp_stock:
            cur.execute(
                "UPDATE shop_items SET stock_update_enabled=0 WHERE item_id=%s",
                (int(item_id),),
            )


def toggle_item_status(item_id: int) -> bool:
    sql = """
    UPDATE items
    SET status = IF(status='active','suspended','active')
    WHERE id=%s
    """
    with get_cursor(commit=True) as cur:
        cur.execute(sql, (int(item_id),))
        ok = cur.rowcount > 0
    if ok:
        sync_shop_items_from_company_item(item_id)
    return ok


def toggle_stock_update(item_id: int) -> bool:
    sql = """
    UPDATE items
    SET stock_update_enabled = IF(stock_update_enabled=1,0,1)
    WHERE id=%s
    """
    with get_cursor(commit=True) as cur:
        cur.execute(sql, (int(item_id),))
        ok = cur.rowcount > 0
    if ok:
        sync_shop_items_from_company_item(item_id)
    return ok


def set_all_items_status(active: bool) -> int:
    """Activate or suspend every catalog item; sync shop rows to match company master flags."""
    status = "active" if active else "suspended"
    with get_cursor(commit=True) as cur:
        cur.execute("UPDATE items SET status=%s", (status,))
        count = int(cur.rowcount or 0)
    if count <= 0:
        return 0
    with get_cursor(commit=True) as cur:
        if active:
            cur.execute(
                """
                UPDATE shop_items si
                INNER JOIN items i ON i.id = si.item_id
                SET
                  si.displayed = IF(i.stock_update_enabled=1, 1, si.displayed),
                  si.stock_update_enabled = IF(i.stock_update_enabled=1, 1, 0)
                WHERE i.status = 'active'
                """
            )
        else:
            cur.execute("UPDATE shop_items SET displayed=0, stock_update_enabled=0")
    return count


def set_all_items_stock_update(enabled: bool) -> int:
    """Enable or disable company stock/POS inventory master for every catalog item."""
    val = 1 if enabled else 0
    with get_cursor(commit=True) as cur:
        cur.execute("UPDATE items SET stock_update_enabled=%s", (val,))
        count = int(cur.rowcount or 0)
    if count <= 0:
        return 0
    with get_cursor(commit=True) as cur:
        if enabled:
            cur.execute(
                """
                UPDATE shop_items si
                INNER JOIN items i ON i.id = si.item_id
                SET
                  si.displayed = IF(i.status='active', 1, si.displayed),
                  si.stock_update_enabled = IF(i.status='active', 1, 0)
                """
            )
        else:
            cur.execute("UPDATE shop_items SET stock_update_enabled=0")
    return count


def delete_item(item_id: int) -> bool:
    sql = "DELETE FROM items WHERE id=%s"
    with get_cursor(commit=True) as cur:
        cur.execute(sql, (int(item_id),))
        return cur.rowcount > 0


def init_stock_transactions_table():
    sql = """
    CREATE TABLE IF NOT EXISTS stock_transactions (
        id INT AUTO_INCREMENT PRIMARY KEY,
        item_id INT NOT NULL,
        direction ENUM('in', 'out') NOT NULL,
        qty INT NOT NULL,
        stock_before INT NOT NULL,
        stock_after INT NOT NULL,
        buying_price DECIMAL(12,2) NULL,
        place_brought_from VARCHAR(255) NULL,
        seller_phone VARCHAR(40) NULL,
        stock_out_reason VARCHAR(50) NULL,
        refunded TINYINT(1) NOT NULL DEFAULT 0,
        refund_amount DECIMAL(12,2) NULL,
        payment_status ENUM('pending_payment','partially_paid','paid') NOT NULL DEFAULT 'pending_payment',
        amount_paid DECIMAL(12,2) NOT NULL DEFAULT 0.00,
        note VARCHAR(255) NULL,
        created_by_employee_id INT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        KEY idx_tx_item (item_id),
        KEY idx_tx_created_at (created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql)
            if not column_exists("stock_transactions", "buying_price"):
                cur.execute("ALTER TABLE stock_transactions ADD COLUMN buying_price DECIMAL(12,2) NULL AFTER stock_after")
            if not column_exists("stock_transactions", "place_brought_from"):
                cur.execute("ALTER TABLE stock_transactions ADD COLUMN place_brought_from VARCHAR(255) NULL AFTER buying_price")
            if not column_exists("stock_transactions", "seller_phone"):
                cur.execute("ALTER TABLE stock_transactions ADD COLUMN seller_phone VARCHAR(40) NULL AFTER place_brought_from")
            if not column_exists("stock_transactions", "stock_out_reason"):
                cur.execute("ALTER TABLE stock_transactions ADD COLUMN stock_out_reason VARCHAR(50) NULL AFTER seller_phone")
            if not column_exists("stock_transactions", "refunded"):
                cur.execute("ALTER TABLE stock_transactions ADD COLUMN refunded TINYINT(1) NOT NULL DEFAULT 0 AFTER stock_out_reason")
            if not column_exists("stock_transactions", "refund_amount"):
                cur.execute("ALTER TABLE stock_transactions ADD COLUMN refund_amount DECIMAL(12,2) NULL AFTER refunded")
            if not column_exists("stock_transactions", "payment_status"):
                cur.execute(
                    "ALTER TABLE stock_transactions ADD COLUMN payment_status "
                    "ENUM('pending_payment','partially_paid','paid') NOT NULL DEFAULT 'pending_payment' "
                    "AFTER refund_amount"
                )
            if not column_exists("stock_transactions", "amount_paid"):
                cur.execute(
                    "ALTER TABLE stock_transactions ADD COLUMN amount_paid DECIMAL(12,2) NOT NULL DEFAULT 0.00 "
                    "AFTER payment_status"
                )
        logger.info("Table stock_transactions is ready.")
        return True
    except pymysql.Error as e:
        logger.warning("Could not init stock_transactions: %s", e)
        return False


def list_stock_manage_items(limit: int = 500, inventory_mode: Optional[str] = None):
    """Items eligible for stock management: active + stock updates enabled."""
    m = (inventory_mode or "").strip().lower()
    if m == "both":
        if not column_exists("shop_items", "store_stock_registered"):
            return []
        mode_filter = """
    AND EXISTS (
      SELECT 1 FROM shop_items si0
      WHERE si0.item_id = i.id AND COALESCE(si0.store_stock_registered,0) = 1
      LIMIT 1
    )"""
    else:
        mode_filter = ""
    has_levels = column_exists("items", "low_stock_threshold") and column_exists("items", "reorder_level")
    levels_cols = "i.low_stock_threshold, i.reorder_level," if has_levels else ""
    sql = """
    SELECT
        i.id,
        i.category,
        i.name,
        i.price,
        i.selling_price,
        i.image_path,
        i.stock_qty,
        i.stock_update_enabled,
        i.status,
        {levels_cols}
        (
            SELECT st.buying_price
            FROM stock_transactions st
            WHERE st.item_id = i.id
              AND st.direction = 'in'
              AND st.buying_price IS NOT NULL
            ORDER BY st.id DESC
            LIMIT 1
        ) AS last_buying_price
    FROM items i
    WHERE status='active' AND stock_update_enabled=1
    {mode_filter}
    ORDER BY COALESCE(NULLIF(TRIM(i.category), ''), 'Uncategorized') ASC, i.name ASC
    LIMIT %s
    """.format(levels_cols=levels_cols, mode_filter=mode_filter)
    with get_cursor() as cur:
        cur.execute(sql, (int(limit),))
        rows = cur.fetchall() or []
    if has_levels:
        for r in rows:
            r["low_stock_threshold"] = int(r.get("low_stock_threshold") or 0)
            r["reorder_level"] = int(r.get("reorder_level") or 0)
    else:
        for r in rows:
            r["low_stock_threshold"] = 0
            r["reorder_level"] = 0
    return rows


# IT profitability / reports: include every catalog row the business treats as sellable,
# not only items flagged for stock management updates.
IT_SUPPORT_ANALYTICS_ITEMS_MAX = 100_000


def list_active_catalog_items_for_it_analytics(
    limit: int = IT_SUPPORT_ANALYTICS_ITEMS_MAX,
    inventory_mode: Optional[str] = None,
):
    """Active catalog items for IT analytics views (margin, velocity, stock reports).

    Same shape as ``list_stock_manage_items`` but does not require
    ``stock_update_enabled=1``. Respects ``inventory_mode='both'`` when set.
    """
    m = (inventory_mode or "").strip().lower()
    if m == "both":
        if not column_exists("shop_items", "store_stock_registered"):
            return []
        mode_filter = """
    AND EXISTS (
      SELECT 1 FROM shop_items si0
      WHERE si0.item_id = i.id AND COALESCE(si0.store_stock_registered,0) = 1
      LIMIT 1
    )"""
    else:
        mode_filter = ""
    has_levels = column_exists("items", "low_stock_threshold") and column_exists("items", "reorder_level")
    levels_cols = "i.low_stock_threshold, i.reorder_level," if has_levels else ""
    lim = int(limit) if limit is not None else IT_SUPPORT_ANALYTICS_ITEMS_MAX
    lim = max(1, min(lim, IT_SUPPORT_ANALYTICS_ITEMS_MAX))
    sql = """
    SELECT
        i.id,
        i.category,
        i.name,
        i.price,
        i.selling_price,
        i.image_path,
        i.stock_qty,
        i.stock_update_enabled,
        i.status,
        {levels_cols}
        (
            SELECT st.buying_price
            FROM stock_transactions st
            WHERE st.item_id = i.id
              AND st.direction = 'in'
              AND st.buying_price IS NOT NULL
            ORDER BY st.id DESC
            LIMIT 1
        ) AS last_buying_price
    FROM items i
    WHERE i.status='active'
    {mode_filter}
    ORDER BY COALESCE(NULLIF(TRIM(i.category), ''), 'Uncategorized') ASC, i.name ASC
    LIMIT %s
    """.format(levels_cols=levels_cols, mode_filter=mode_filter)
    with get_cursor() as cur:
        cur.execute(sql, (lim,))
        rows = cur.fetchall() or []
    if has_levels:
        for r in rows:
            r["low_stock_threshold"] = int(r.get("low_stock_threshold") or 0)
            r["reorder_level"] = int(r.get("reorder_level") or 0)
    else:
        for r in rows:
            r["low_stock_threshold"] = 0
            r["reorder_level"] = 0
    return rows


def _website_item_row_from_db(r: dict) -> dict:
    """Normalize a DB item row for website/catalog use (POS fields are fallbacks)."""
    try:
        price = float(r.get("selling_price") if r.get("selling_price") is not None else r.get("price") or 0)
    except (TypeError, ValueError):
        price = 0.0
    try:
        orig = float(r.get("price") or 0)
    except (TypeError, ValueError):
        orig = 0.0
    try:
        qty_sold = float(r.get("qty_sold") or 0)
    except (TypeError, ValueError):
        qty_sold = 0.0
    ip = r.get("image_path")
    if isinstance(ip, bytes):
        ip = ip.decode("utf-8", errors="replace")
    ip = (str(ip).strip() if ip is not None else "") or ""
    pos_name = (r.get("name") or "").strip() or "Product"
    web_name = (r.get("website_name") or "").strip()
    pos_desc = (r.get("description") or "").strip()
    web_desc = (r.get("website_description") or "").strip()
    return {
        "id": int(r.get("id") or 0),
        "category": (r.get("category") or "").strip() or "General",
        "name": web_name or pos_name,
        "description": web_desc or pos_desc,
        "price": round(price, 2),
        "original_price": round(orig, 2),
        "image_path": ip,
        "qty_sold": qty_sold,
    }


def _website_catalog_item_select_cols(qty_col: str) -> str:
    return f"""
        i.id,
        i.category,
        i.name,
        i.description,
        i.website_name,
        i.website_description,
        i.website_published,
        i.price,
        i.selling_price,
        i.image_path,
        {qty_col} AS qty_sold"""


def _website_catalog_published_where() -> str:
    return " AND COALESCE(i.website_published, 1) = 1"


def _website_sales_join_sql() -> tuple[str, str]:
    if table_exists("shop_pos_sale_items") and table_exists("shop_pos_sales"):
        sales_join = """
    LEFT JOIN (
        SELECT si.item_id, COALESCE(SUM(si.qty), 0) AS qty_sold
        FROM shop_pos_sale_items si
        JOIN shop_pos_sales s ON s.id = si.sale_id
        WHERE si.item_id IS NOT NULL AND si.item_id > 0
        GROUP BY si.item_id
    ) sales ON sales.item_id = i.id"""
        qty_col = "COALESCE(sales.qty_sold, 0)"
        return sales_join, qty_col
    return "", "0"


def list_website_featured_products(limit: int = 12) -> list:
    """Catalog items for the public storefront, ranked by all-time POS qty sold."""
    lim = max(1, min(int(limit), 48))
    if not table_exists("items"):
        return []
    sales_join, qty_col = _website_sales_join_sql()
    cols = _website_catalog_item_select_cols(qty_col)
    sql = f"""
    SELECT{cols}
    FROM items i
    {sales_join}
    WHERE i.status = 'active'{_website_catalog_published_where()}
    ORDER BY {qty_col} DESC, i.name ASC
    LIMIT %s
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (lim,))
            rows = cur.fetchall() or []
    except Exception:
        rows = []
    return [_website_item_row_from_db(dict(r)) for r in rows]


def list_website_catalog_items(limit: int = 300) -> list:
    """All active catalog items for the website product picker."""
    lim = max(1, min(int(limit), 500))
    if not table_exists("items"):
        return []
    sales_join, qty_col = _website_sales_join_sql()
    cols = _website_catalog_item_select_cols(qty_col)
    sql = f"""
    SELECT{cols}
    FROM items i
    {sales_join}
    WHERE i.status = 'active'{_website_catalog_published_where()}
    ORDER BY i.category ASC, i.name ASC
    LIMIT %s
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (lim,))
            rows = cur.fetchall() or []
    except Exception:
        rows = []
    return [_website_item_row_from_db(dict(r)) for r in rows if int(dict(r).get("id") or 0) > 0]


def get_website_catalog_item(item_id: int) -> dict | None:
    """Single active catalog item with sales stats (for public product pages)."""
    try:
        iid = int(item_id)
    except (TypeError, ValueError):
        return None
    if iid <= 0 or not table_exists("items"):
        return None
    sales_join, qty_col = _website_sales_join_sql()
    cols = _website_catalog_item_select_cols(qty_col)
    sql = f"""
    SELECT{cols}
    FROM items i
    {sales_join}
    WHERE i.id = %s AND i.status = 'active'{_website_catalog_published_where()}
    LIMIT 1
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (iid,))
            row = cur.fetchone()
    except Exception:
        row = None
    if not row:
        return None
    return _website_item_row_from_db(dict(row))


def list_website_products_by_ids(item_ids: list) -> list:
    """Active website products in the exact order of ``item_ids``."""
    ids: list[int] = []
    seen: set[int] = set()
    for raw in item_ids or []:
        try:
            iid = int(raw)
        except (TypeError, ValueError):
            continue
        if iid <= 0 or iid in seen:
            continue
        seen.add(iid)
        ids.append(iid)
    if not ids or not table_exists("items"):
        return []
    sales_join, qty_col = _website_sales_join_sql()
    placeholders = ",".join(["%s"] * len(ids))
    field_order = ",".join(str(i) for i in ids)
    cols = _website_catalog_item_select_cols(qty_col)
    sql = f"""
    SELECT{cols}
    FROM items i
    {sales_join}
    WHERE i.status = 'active'{_website_catalog_published_where()} AND i.id IN ({placeholders})
    ORDER BY FIELD(i.id, {field_order})
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(ids))
            rows = cur.fetchall() or []
    except Exception:
        return []
    return [_website_item_row_from_db(dict(r)) for r in rows if int(dict(r).get("id") or 0) > 0]


def search_seller_names(prefix: str, limit: int = 8):
    q = (prefix or "").strip()
    if len(q) < 1:
        return []
    sql = """
    SELECT seller_name
    FROM sellers
    WHERE seller_name IS NOT NULL
      AND TRIM(seller_name) <> ''
      AND seller_name LIKE %s
    ORDER BY seller_name ASC
    LIMIT %s
    """
    with get_cursor() as cur:
        cur.execute(sql, (q + "%", int(limit)))
        rows = cur.fetchall() or []
    out = []
    for r in rows:
        name = (r.get("seller_name") or "").strip()
        if name:
            out.append(name)
    return out


def search_sellers_for_pos(query: str, limit: int = 10):
    """Substring name/phone search for POS seller/supplier autofill suggestions."""
    q = (query or "").strip()
    if len(q) < 1:
        return []
    try:
        lim = max(1, min(int(limit), 25))
    except Exception:
        lim = 10
    like = f"%{q}%"
    digits = re.sub(r"\D", "", q)
    phone_like = f"%{digits}%" if len(digits) >= 3 else None

    seller_where = "TRIM(seller_name) <> '' AND (LOWER(seller_name) LIKE LOWER(%s)"
    seller_params: list[Any] = [like]
    if phone_like:
        seller_where += " OR phone LIKE %s"
        seller_params.append(phone_like)
    seller_where += ")"

    stock_where = (
        "direction='in' AND TRIM(COALESCE(place_brought_from, '')) <> '' "
        "AND TRIM(COALESCE(seller_phone, '')) NOT IN ('', '-') "
        "AND LOWER(place_brought_from) LIKE LOWER(%s)"
    )
    expense_where = (
        "TRIM(COALESCE(supplier_name, '')) <> '' "
        "AND TRIM(COALESCE(seller_phone, '')) NOT IN ('', '-')"
        " AND LOWER(supplier_name) LIKE LOWER(%s)"
    )

    name_col = "CONVERT(TRIM({col}) USING utf8mb4) COLLATE utf8mb4_unicode_ci"
    phone_col = "CONVERT(TRIM({col}) USING utf8mb4) COLLATE utf8mb4_unicode_ci"

    sql = f"""
    SELECT id, seller_name, phone, sort_ts FROM (
        SELECT id,
               {name_col.format(col='seller_name')} AS seller_name,
               {phone_col.format(col='phone')} AS phone,
               updated_at AS sort_ts
        FROM sellers
        WHERE {seller_where}
        UNION ALL
        SELECT NULL AS id,
               {name_col.format(col='place_brought_from')} AS seller_name,
               {phone_col.format(col='seller_phone')} AS phone,
               MAX(created_at) AS sort_ts
        FROM stock_transactions
        WHERE {stock_where}
        GROUP BY TRIM(place_brought_from), TRIM(seller_phone)
        UNION ALL
        SELECT NULL AS id,
               {name_col.format(col='supplier_name')} AS seller_name,
               {phone_col.format(col='seller_phone')} AS phone,
               MAX(created_at) AS sort_ts
        FROM shop_operational_expenses
        WHERE {expense_where}
        GROUP BY TRIM(supplier_name), TRIM(seller_phone)
    ) AS hits
    WHERE TRIM(seller_name) <> ''
    ORDER BY (id IS NOT NULL) DESC, sort_ts DESC, seller_name ASC
    LIMIT %s
    """
    params = tuple(seller_params + [like, like, lim])
    try:
        with get_cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall() or []
        seen: set[str] = set()
        out: list[dict] = []
        for r in rows:
            name = (r.get("seller_name") or "").strip()
            phone = (r.get("phone") or "").strip()
            if not name:
                continue
            key = f"{name.lower()}|{re.sub(r'\D', '', phone)}"
            if key in seen:
                continue
            seen.add(key)
            out.append(
                {
                    "id": r.get("id"),
                    "seller_name": name,
                    "phone": phone,
                }
            )
            if len(out) >= lim:
                break
        return out
    except pymysql.Error:
        return []


def list_registered_sellers_for_report(
    *,
    name_or_phone_contains: Optional[str] = None,
    limit: int = 5000,
) -> list[dict]:
    """Every row in ``sellers`` (optional substring on name or phone) for supplier directory UIs."""
    try:
        lim = max(1, min(int(limit), 20000))
    except Exception:
        lim = 5000
    q = (name_or_phone_contains or "").strip()
    if q:
        like = f"%{q}%"
        sql = """
        SELECT id, seller_name, phone
        FROM sellers
        WHERE (seller_name LIKE %s OR phone LIKE %s)
        ORDER BY seller_name ASC, phone ASC
        LIMIT %s
        """
        params = (like, like, lim)
    else:
        sql = """
        SELECT id, seller_name, phone
        FROM sellers
        ORDER BY seller_name ASC, phone ASC
        LIMIT %s
        """
        params = (lim,)
    try:
        with get_cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall() or []
    except pymysql.Error:
        return []
    out: list[dict] = []
    for r in rows:
        nm = (r.get("seller_name") or "").strip() or "-"
        ph = (r.get("phone") or "").strip() or "-"
        if nm == "-" and ph == "-":
            continue
        out.append(
            {
                "id": int(r.get("id") or 0),
                "seller_name": nm,
                "seller_phone": ph,
            }
        )
    return out


def list_stock_transactions(item_id: int, direction: Optional[str] = None, limit: int = 100):
    if direction in ("in", "out"):
        sql = """
        SELECT
            st.id,
            st.direction,
            st.qty,
            st.stock_before,
            st.stock_after,
            st.buying_price,
            st.place_brought_from,
            st.seller_phone,
            st.stock_out_reason,
            st.refunded,
            st.refund_amount,
            st.payment_status,
            st.amount_paid,
            st.note,
            st.created_at,
            COALESCE(e.full_name, 'UNKNOWN') AS stocked_by
        FROM stock_transactions st
        LEFT JOIN employees e ON e.id = st.created_by_employee_id
        WHERE st.item_id=%s AND st.direction=%s
        ORDER BY created_at DESC
        LIMIT %s
        """
        params = (int(item_id), direction, int(limit))
    else:
        sql = """
        SELECT
            st.id,
            st.direction,
            st.qty,
            st.stock_before,
            st.stock_after,
            st.buying_price,
            st.place_brought_from,
            st.seller_phone,
            st.stock_out_reason,
            st.refunded,
            st.refund_amount,
            st.payment_status,
            st.amount_paid,
            st.note,
            st.created_at,
            COALESCE(e.full_name, 'UNKNOWN') AS stocked_by
        FROM stock_transactions st
        LEFT JOIN employees e ON e.id = st.created_by_employee_id
        WHERE st.item_id=%s
        ORDER BY created_at DESC
        LIMIT %s
        """
        params = (int(item_id), int(limit))
    with get_cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall() or []


def get_company_item_supplier_summary(*, limit_items: int = 2000) -> list:
    """
    Per company-managed item: weighted average buying price (all stock-ins with price),
    optional best supplier (lowest weighted-average unit cost among named suppliers).
    """
    items = list_stock_manage_items(limit=int(limit_items)) or []
    item_by_id = {}
    for it in items:
        try:
            iid = int(it.get("id") or 0)
        except Exception:
            continue
        if iid > 0:
            item_by_id[iid] = it

    overall: dict = {}
    sup_rows: list = []
    try:
        with get_cursor() as cur:
            cur.execute(
                """
                SELECT
                    item_id,
                    SUM(qty * buying_price) / NULLIF(SUM(qty), 0) AS avg_buy,
                    SUM(qty) AS total_in_qty,
                    COUNT(*) AS in_tx_count
                FROM stock_transactions
                WHERE direction = 'in' AND buying_price IS NOT NULL
                GROUP BY item_id
                """
            )
            for r in cur.fetchall() or []:
                try:
                    overall[int(r.get("item_id") or 0)] = r
                except Exception:
                    continue

            cur.execute(
                """
                SELECT
                    item_id,
                    TRIM(place_brought_from) AS supplier,
                    SUM(qty) AS s_qty,
                    SUM(qty * buying_price) / NULLIF(SUM(qty), 0) AS w_avg
                FROM stock_transactions
                WHERE direction = 'in'
                  AND buying_price IS NOT NULL
                  AND TRIM(COALESCE(place_brought_from, '')) != ''
                GROUP BY item_id, TRIM(place_brought_from)
                """
            )
            sup_rows = cur.fetchall() or []
    except Exception:
        return []

    by_item_suppliers: dict = {}
    for r in sup_rows:
        try:
            iid = int(r.get("item_id") or 0)
        except Exception:
            continue
        if iid <= 0:
            continue
        by_item_suppliers.setdefault(iid, []).append(r)

    out: list = []
    for iid, it in sorted(item_by_id.items(), key=lambda x: ((x[1].get("name") or "").lower(), x[0])):
        o = overall.get(iid)
        avg_buy = None
        total_in_qty = 0
        in_tx_count = 0
        if o:
            total_in_qty = int(o.get("total_in_qty") or 0)
            in_tx_count = int(o.get("in_tx_count") or 0)
            if total_in_qty > 0 and o.get("avg_buy") is not None:
                avg_buy = float(o.get("avg_buy") or 0)

        best_sup = None
        best_avg = None
        best_qty = -1
        for sr in by_item_suppliers.get(iid, []):
            try:
                wa = float(sr.get("w_avg") or 0)
                sq = int(sr.get("s_qty") or 0)
            except Exception:
                continue
            name = (sr.get("supplier") or "").strip() or "—"
            if (
                best_sup is None
                or wa < (best_avg or 0) - 1e-9
                or (abs(wa - (best_avg or 0)) < 1e-9 and sq > best_qty)
            ):
                best_sup = name
                best_avg = wa
                best_qty = sq

        out.append(
            {
                "item_id": iid,
                "name": (it.get("name") or "").strip() or f"Item #{iid}",
                "category": (it.get("category") or "").strip(),
                "avg_buying_price": avg_buy,
                "total_in_qty": total_in_qty,
                "in_transaction_count": in_tx_count,
                "best_supplier": best_sup,
                "best_supplier_avg_price": best_avg,
                "company_stock_qty": round(float(it.get("stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES),
            }
        )
    return out


def _apply_stock_transaction_on_cursor(
    cur,
    *,
    item_id: int,
    direction: str,
    qty,
    buying_price: Optional[float] = None,
    place_brought_from: Optional[str] = None,
    seller_phone: Optional[str] = None,
    stock_out_reason: Optional[str] = None,
    refunded: bool = False,
    refund_amount: Optional[float] = None,
    note: Optional[str] = None,
    created_by_employee_id: Optional[int] = None,
    payment_status: Optional[str] = None,
    amount_paid: Optional[float] = None,
) -> bool:
    """Apply one stock movement using an existing cursor (caller manages transaction)."""
    if direction not in ("in", "out"):
        return False
    n = normalize_stock_move_qty(qty)
    if n is None:
        return False

    cur.execute(
        """
        SELECT stock_qty, status, stock_update_enabled
        FROM items
        WHERE id=%s
        FOR UPDATE
        """,
        (int(item_id),),
    )
    row = cur.fetchone()
    if not row:
        return False
    if row.get("status") != "active" or int(row.get("stock_update_enabled") or 0) != 1:
        return False

    before = round(float(row.get("stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
    if direction == "in":
        after = round(before + n, STOCK_QTY_DECIMAL_PLACES)
    else:
        after = round(before - n, STOCK_QTY_DECIMAL_PLACES)
        if after < 0:
            return False

    cur.execute("UPDATE items SET stock_qty=%s WHERE id=%s", (after, int(item_id)))
    if direction == "in":
        ps = (payment_status or "pending_payment").strip().lower()
        if ps not in {"pending_payment", "partially_paid", "paid"}:
            ps = "pending_payment"
        if amount_paid is not None:
            try:
                ap = max(0.0, float(amount_paid))
                if not math.isfinite(ap):
                    ap = 0.0
            except (TypeError, ValueError):
                ap = 0.0
        elif ps == "paid":
            bp = float(buying_price or 0.0)
            ap = max(0.0, round(float(n) * bp, 2))
        else:
            ap = 0.0
    else:
        ps = "pending_payment"
        ap = 0.0

    cur.execute(
        """
        INSERT INTO stock_transactions
            (
                item_id, direction, qty, stock_before, stock_after,
                buying_price, place_brought_from, seller_phone, stock_out_reason, refunded, refund_amount,
                payment_status, amount_paid,
                note, created_by_employee_id
            )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            int(item_id),
            direction,
            n,
            before,
            after,
            buying_price if buying_price is not None else None,
            place_brought_from or None,
            _stored_seller_phone(seller_phone),
            stock_out_reason or None,
            1 if refunded else 0,
            refund_amount if refund_amount is not None else None,
            ps,
            ap,
            note or None,
            created_by_employee_id,
        ),
    )
    return True


def create_stock_transaction(
    *,
    item_id: int,
    direction: str,
    qty,
    buying_price: Optional[float] = None,
    place_brought_from: Optional[str] = None,
    seller_phone: Optional[str] = None,
    stock_out_reason: Optional[str] = None,
    refunded: bool = False,
    refund_amount: Optional[float] = None,
    note: Optional[str] = None,
    created_by_employee_id: Optional[int] = None,
    payment_status: Optional[str] = None,
    amount_paid: Optional[float] = None,
) -> bool:
    """
    Create a stock in/out transaction and update item stock atomically.
    Only works if item is active and stock_update_enabled=1.
    """
    with get_cursor(commit=True) as cur:
        return _apply_stock_transaction_on_cursor(
            cur,
            item_id=item_id,
            direction=direction,
            qty=qty,
            buying_price=buying_price,
            place_brought_from=place_brought_from,
            seller_phone=seller_phone,
            stock_out_reason=stock_out_reason,
            refunded=refunded,
            refund_amount=refund_amount,
            note=note,
            created_by_employee_id=created_by_employee_id,
            payment_status=payment_status,
            amount_paid=amount_paid,
        )


def create_stock_transactions_batch(
    *,
    operations: list,
    created_by_employee_id: Optional[int] = None,
) -> tuple[bool, str]:
    """
    Apply multiple stock movements in a single DB transaction (all succeed or none).
    Each operation dict supports the same kwargs as create_stock_transaction (item_id, direction, qty, ...).
    """
    if not operations:
        return False, "No line items to apply."
    try:
        with get_cursor(commit=True) as cur:
            for op in operations:
                ok = _apply_stock_transaction_on_cursor(
                    cur,
                    item_id=int(op["item_id"]),
                    direction=str(op["direction"]),
                    qty=op["qty"],
                    buying_price=op.get("buying_price"),
                    place_brought_from=op.get("place_brought_from"),
                    seller_phone=op.get("seller_phone"),
                    stock_out_reason=op.get("stock_out_reason"),
                    refunded=bool(op.get("refunded")),
                    refund_amount=op.get("refund_amount"),
                    note=op.get("note"),
                    created_by_employee_id=created_by_employee_id,
                    payment_status=op.get("payment_status"),
                    amount_paid=op.get("amount_paid"),
                )
                if not ok:
                    return False, (
                        f"Could not apply line for item #{op.get('item_id')} "
                        "(check quantity vs company stock and that the item is active)."
                    )
    except Exception:
        return False, "Could not update stock. Check database connection and item eligibility."

    n = len(operations)
    d0 = str(operations[0].get("direction") or "")
    dir_label = "stock in" if d0 == "in" else "stock out"
    return True, f"Applied {dir_label} for {n} item(s)."


def init_shops_table():
    sql = """
    CREATE TABLE IF NOT EXISTS shops (
        id INT AUTO_INCREMENT PRIMARY KEY,
        shop_name VARCHAR(200) NOT NULL,
        shop_code VARCHAR(80) NOT NULL,
        shop_password_hash VARCHAR(255) NOT NULL,
        shop_location VARCHAR(255) NOT NULL,
        status ENUM('active', 'suspended') NOT NULL DEFAULT 'active',
        default_theme ENUM('dark', 'light') NOT NULL DEFAULT 'dark',
        font_family VARCHAR(60) NOT NULL DEFAULT 'Plus Jakarta Sans',
        primary_color CHAR(7) NOT NULL DEFAULT '#10b981',
        accent_color CHAR(7) NOT NULL DEFAULT '#14b8a6',
        created_by_employee_id INT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uq_shop_code (shop_code),
        KEY idx_shop_created_at (created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql)
            if not column_exists("shops", "status"):
                cur.execute(
                    "ALTER TABLE shops ADD COLUMN status ENUM('active','suspended') NOT NULL DEFAULT 'active' AFTER shop_location"
                )
            if not column_exists("shops", "default_theme"):
                cur.execute(
                    "ALTER TABLE shops ADD COLUMN default_theme ENUM('dark','light') NOT NULL DEFAULT 'dark' AFTER status"
                )
            if not column_exists("shops", "font_family"):
                cur.execute(
                    "ALTER TABLE shops ADD COLUMN font_family VARCHAR(60) NOT NULL DEFAULT 'Plus Jakarta Sans' AFTER default_theme"
                )
            if not column_exists("shops", "primary_color"):
                cur.execute(
                    "ALTER TABLE shops ADD COLUMN primary_color CHAR(7) NOT NULL DEFAULT '#10b981' AFTER font_family"
                )
            if not column_exists("shops", "accent_color"):
                cur.execute(
                    "ALTER TABLE shops ADD COLUMN accent_color CHAR(7) NOT NULL DEFAULT '#14b8a6' AFTER primary_color"
                )
            if not column_exists("shops", "shop_logo"):
                cur.execute(
                    "ALTER TABLE shops ADD COLUMN shop_logo VARCHAR(500) NULL AFTER accent_color"
                )
            if not column_exists("shops", "printing_settings_json"):
                cur.execute(
                    "ALTER TABLE shops ADD COLUMN printing_settings_json LONGTEXT NULL AFTER shop_logo"
                )
            if not column_exists("shops", "receipt_settings_json"):
                cur.execute(
                    "ALTER TABLE shops ADD COLUMN receipt_settings_json LONGTEXT NULL AFTER printing_settings_json"
                )
            if not column_exists("shops", "appearance_settings_json"):
                cur.execute(
                    "ALTER TABLE shops ADD COLUMN appearance_settings_json LONGTEXT NULL AFTER receipt_settings_json"
                )
            if not column_exists("shops", "company_settings_json"):
                cur.execute(
                    "ALTER TABLE shops ADD COLUMN company_settings_json LONGTEXT NULL AFTER appearance_settings_json"
                )
            if not column_exists("shops", "stock_workspace_settings_json"):
                cur.execute(
                    "ALTER TABLE shops ADD COLUMN stock_workspace_settings_json LONGTEXT NULL AFTER company_settings_json"
                )
        ensure_shop_location_description_column()
        logger.info("Table shops is ready.")
        return True
    except pymysql.Error as e:
        logger.warning("Could not init shops: %s", e)
        return False


def ensure_shop_location_description_column() -> bool:
    """Add shops.shop_location_description when missing (safe before reads/writes)."""
    if column_exists("shops", "shop_location_description"):
        return True
    try:
        with get_cursor(commit=True) as cur:
            try:
                cur.execute(
                    "ALTER TABLE shops ADD COLUMN shop_location_description TEXT NULL AFTER shop_location"
                )
            except pymysql.Error:
                cur.execute(
                    "ALTER TABLE shops ADD COLUMN shop_location_description TEXT NULL"
                )
        return column_exists("shops", "shop_location_description")
    except pymysql.Error as e:
        logger.warning("Could not add shops.shop_location_description column: %s", e)
        return False


def ensure_shop_phone_column() -> bool:
    """Public contact phone for a shop branch (homepage, customer-facing)."""
    try:
        with get_cursor(commit=True) as cur:
            if not column_exists("shops", "shop_phone"):
                cur.execute(
                    "ALTER TABLE shops ADD COLUMN shop_phone VARCHAR(40) NULL AFTER shop_location_description"
                )
        return column_exists("shops", "shop_phone")
    except pymysql.Error as e:
        logger.warning("Could not add shops.shop_phone column: %s", e)
        return False


def create_shop(
    *,
    shop_name: str,
    shop_code: str,
    shop_password_hash: str,
    shop_location: str,
    created_by_employee_id: Optional[int] = None,
    shop_logo: Optional[str] = None,
    shop_location_description: Optional[str] = None,
    shop_phone: Optional[str] = None,
) -> int:
    if not ensure_shop_location_description_column():
        raise RuntimeError("shops.shop_location_description column is not available")
    ensure_shop_phone_column()
    phone = (shop_phone or "").strip() or None
    sql = """
    INSERT INTO shops
        (shop_name, shop_code, shop_password_hash, shop_location, shop_location_description, shop_phone, created_by_employee_id, shop_logo)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    with get_cursor(commit=True) as cur:
        desc = (shop_location_description or "").strip().upper() or None
        cur.execute(
            sql,
            (
                shop_name.strip().upper(),
                shop_code.strip().upper(),
                shop_password_hash,
                shop_location.strip().upper(),
                desc,
                phone,
                created_by_employee_id,
                (shop_logo or "").strip() or None,
            ),
        )
        return int(cur.lastrowid)


def shop_code_available(shop_code: str) -> bool:
    sql = "SELECT 1 FROM shops WHERE shop_code = %s LIMIT 1"
    with get_cursor() as cur:
        cur.execute(sql, ((shop_code or "").strip().upper(),))
        return cur.fetchone() is None


def list_shops(limit: int = 500):
    ensure_shop_location_description_column()
    ensure_shop_phone_column()
    sql = """
    SELECT id, shop_name, shop_code, shop_location, shop_location_description, shop_phone, status, default_theme, font_family, primary_color, accent_color, shop_logo,
           printing_settings_json, receipt_settings_json, appearance_settings_json, company_settings_json,
           stock_workspace_settings_json, created_at
    FROM shops
    ORDER BY created_at DESC
    LIMIT %s
    """
    with get_cursor() as cur:
        cur.execute(sql, (int(limit),))
        return cur.fetchall() or []


def get_shop_by_id(shop_id: int):
    ensure_shop_location_description_column()
    ensure_shop_phone_column()
    sql = """
    SELECT id, shop_name, shop_code, shop_password_hash, shop_location, shop_location_description, shop_phone, status, default_theme, font_family, primary_color, accent_color, shop_logo,
           printing_settings_json, receipt_settings_json, appearance_settings_json, company_settings_json,
           stock_workspace_settings_json, created_at
    FROM shops
    WHERE id=%s
    LIMIT 1
    """
    with get_cursor() as cur:
        cur.execute(sql, (int(shop_id),))
        return cur.fetchone()


def get_shop_by_code(shop_code: str):
    sql = """
    SELECT id, shop_name, shop_code, shop_password_hash, shop_location, shop_location_description, status, default_theme, font_family, primary_color, accent_color, shop_logo,
           printing_settings_json, receipt_settings_json, appearance_settings_json, company_settings_json,
           stock_workspace_settings_json, created_at
    FROM shops
    WHERE shop_code=%s
    LIMIT 1
    """
    with get_cursor() as cur:
        cur.execute(sql, ((shop_code or "").strip().upper(),))
        return cur.fetchone()


def update_shop_settings(
    shop_id: int,
    *,
    default_theme: str,
    font_family: str,
    primary_color: str,
    accent_color: str,
    printing_settings_json: Optional[str] = None,
    receipt_settings_json: Optional[str] = None,
    appearance_settings_json: Optional[str] = None,
    company_settings_json: Optional[str] = None,
) -> bool:
    if default_theme not in ("dark", "light"):
        return False
    try:
        from theme_presets import ALLOWED_FONTS as allowed_fonts_set
    except ImportError:
        allowed_fonts_set = {"Plus Jakarta Sans", "Inter", "System UI"}
    if font_family not in allowed_fonts_set:
        return False
    if not re.match(r"^#[0-9a-fA-F]{6}$", (primary_color or "")) or not re.match(
        r"^#[0-9a-fA-F]{6}$", (accent_color or "")
    ):
        return False
    sql = """
    UPDATE shops
    SET default_theme=%s, font_family=%s, primary_color=%s, accent_color=%s,
        printing_settings_json=%s, receipt_settings_json=%s, appearance_settings_json=%s,
        company_settings_json=%s
    WHERE id=%s
    """
    with get_cursor(commit=True) as cur:
        cur.execute(
            sql,
            (
                default_theme,
                font_family,
                primary_color,
                accent_color,
                printing_settings_json,
                receipt_settings_json,
                appearance_settings_json,
                company_settings_json,
                int(shop_id),
            ),
        )
        # MySQL reports 0 affected rows when values are unchanged; still a successful save.
        if cur.rowcount > 0:
            return True
        cur.execute("SELECT 1 FROM shops WHERE id=%s LIMIT 1", (int(shop_id),))
        return cur.fetchone() is not None


def update_shop_stock_workspace_settings_json(shop_id: int, settings_json: Optional[str]) -> bool:
    """Persist shop stock workspace form-field rules (required vs optional optional columns)."""
    if not column_exists("shops", "stock_workspace_settings_json"):
        return False
    payload = (settings_json or "").strip() or "{}"
    with get_cursor(commit=True) as cur:
        cur.execute(
            "UPDATE shops SET stock_workspace_settings_json=%s WHERE id=%s",
            (payload, int(shop_id)),
        )
        if cur.rowcount > 0:
            return True
        cur.execute("SELECT 1 FROM shops WHERE id=%s LIMIT 1", (int(shop_id),))
        return cur.fetchone() is not None


def update_shop_company_settings(
    shop_id: int,
    *,
    company_settings_json: Optional[str],
    shop_logo: Optional[str] = None,
    update_shop_logo: bool = False,
) -> bool:
    if update_shop_logo:
        sql = """
        UPDATE shops
        SET company_settings_json=%s, shop_logo=%s
        WHERE id=%s
        """
        params = (company_settings_json, shop_logo, int(shop_id))
    else:
        sql = """
        UPDATE shops
        SET company_settings_json=%s
        WHERE id=%s
        """
        params = (company_settings_json, int(shop_id))
    with get_cursor(commit=True) as cur:
        cur.execute(sql, params)
        if cur.rowcount > 0:
            return True
        cur.execute("SELECT 1 FROM shops WHERE id=%s LIMIT 1", (int(shop_id),))
        return cur.fetchone() is not None


def toggle_shop_status(shop_id: int) -> bool:
    sql = """
    UPDATE shops
    SET status = IF(status='active', 'suspended', 'active')
    WHERE id=%s
    """
    with get_cursor(commit=True) as cur:
        cur.execute(sql, (int(shop_id),))
        return cur.rowcount > 0


def delete_shop(shop_id: int) -> bool:
    sql = "DELETE FROM shops WHERE id=%s"
    with get_cursor(commit=True) as cur:
        cur.execute(sql, (int(shop_id),))
        return cur.rowcount > 0


def update_shop_details(
    shop_id: int,
    *,
    shop_name: str,
    shop_code: str,
    shop_location: str,
    status: str,
    shop_password_hash: Optional[str] = None,
    shop_logo: Optional[str] = None,
    update_shop_logo: bool = False,
    shop_location_description: Optional[str] = None,
    shop_phone: Optional[str] = None,
) -> bool:
    if not ensure_shop_location_description_column():
        return False
    ensure_shop_phone_column()
    status = (status or "").strip().lower()
    if status not in {"active", "suspended"}:
        return False

    shop_name_clean = (shop_name or "").strip().upper()
    shop_code_clean = (shop_code or "").strip()
    shop_location_clean = (shop_location or "").strip().upper()
    if not shop_name_clean or not shop_code_clean or not shop_location_clean:
        return False

    with get_cursor(commit=True) as cur:
        cur.execute("SELECT id FROM shops WHERE shop_code=%s AND id!=%s LIMIT 1", (shop_code_clean, int(shop_id)))
        if cur.fetchone():
            return False

        sets = ["shop_name=%s", "shop_code=%s", "shop_location=%s", "shop_location_description=%s", "shop_phone=%s", "status=%s"]
        params: list = [
            shop_name_clean,
            shop_code_clean,
            shop_location_clean,
            (shop_location_description or "").strip().upper() or None,
            (shop_phone or "").strip() or None,
            status,
        ]

        if shop_password_hash:
            sets.insert(2, "shop_password_hash=%s")
            params.insert(2, shop_password_hash)

        if update_shop_logo:
            sets.append("shop_logo=%s")
            params.append((shop_logo or "").strip() or None)

        params.append(int(shop_id))
        sql = f"UPDATE shops SET {', '.join(sets)} WHERE id=%s"
        cur.execute(sql, tuple(params))
        if cur.rowcount > 0:
            return True
        cur.execute("SELECT 1 FROM shops WHERE id=%s LIMIT 1", (int(shop_id),))
        return cur.fetchone() is not None


def init_shop_items_table():
    sql = """
    CREATE TABLE IF NOT EXISTS shop_items (
        shop_id INT NOT NULL,
        item_id INT NOT NULL,
        shop_stock_qty INT NOT NULL DEFAULT 0,
        stock_update_enabled TINYINT(1) NOT NULL DEFAULT 1,
        displayed TINYINT(1) NOT NULL DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (shop_id, item_id),
        KEY idx_shop_items_item (item_id),
        KEY idx_shop_items_shop (shop_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql)
            if not column_exists("shop_items", "stock_update_enabled"):
                cur.execute(
                    "ALTER TABLE shop_items ADD COLUMN stock_update_enabled TINYINT(1) NOT NULL DEFAULT 1 AFTER shop_stock_qty"
                )
            if not column_exists("shop_items", "displayed"):
                cur.execute(
                    "ALTER TABLE shop_items ADD COLUMN displayed TINYINT(1) NOT NULL DEFAULT 1 AFTER stock_update_enabled"
                )
            if not column_exists("shop_items", "low_stock_threshold"):
                cur.execute(
                    "ALTER TABLE shop_items ADD COLUMN low_stock_threshold INT NOT NULL DEFAULT 0 AFTER displayed"
                )
            if not column_exists("shop_items", "reorder_level"):
                cur.execute(
                    "ALTER TABLE shop_items ADD COLUMN reorder_level INT NOT NULL DEFAULT 0 AFTER low_stock_threshold"
                )
            if not column_exists("shop_items", "store_stock_registered"):
                cur.execute(
                    "ALTER TABLE shop_items ADD COLUMN store_stock_registered TINYINT(1) NOT NULL DEFAULT 0 AFTER reorder_level"
                )
            if not column_exists("shop_items", "selling_price"):
                cur.execute(
                    "ALTER TABLE shop_items ADD COLUMN selling_price DECIMAL(12,2) NULL DEFAULT NULL AFTER displayed"
                )
        logger.info("Table shop_items is ready.")
        return True
    except pymysql.Error as e:
        logger.warning("Could not init shop_items: %s", e)
        return False


def init_store_stock_items_table() -> bool:
    """Dedicated store stock item master (separate from `items`)."""
    sql = """
    CREATE TABLE IF NOT EXISTS store_stock_items (
        id INT AUTO_INCREMENT PRIMARY KEY,
        shop_id INT NOT NULL,
        category VARCHAR(100) NOT NULL,
        name VARCHAR(255) NOT NULL,
        description TEXT NULL,
        measure_unit VARCHAR(50) NOT NULL,
        stock_qty DECIMAL(14,4) NOT NULL DEFAULT 0,
        status ENUM('active','inactive') NOT NULL DEFAULT 'active',
        created_by_employee_id INT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        KEY idx_store_stock_items_shop (shop_id),
        KEY idx_store_stock_items_name (name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql)
            if not column_exists("store_stock_items", "stock_qty"):
                cur.execute(
                    "ALTER TABLE store_stock_items ADD COLUMN stock_qty DECIMAL(14,4) NOT NULL DEFAULT 0 AFTER measure_unit"
                )
        logger.info("Table store_stock_items is ready.")
        return True
    except pymysql.Error as e:
        logger.warning("Could not init store_stock_items: %s", e)
        return False


def init_store_stock_transactions_table() -> bool:
    """Movement log for ``store_stock_items`` (Both-mode shelf SKUs separate from sales catalog)."""
    sql = """
    CREATE TABLE IF NOT EXISTS store_stock_transactions (
        id INT AUTO_INCREMENT PRIMARY KEY,
        store_stock_item_id INT NOT NULL,
        shop_id INT NOT NULL,
        direction ENUM('in','out') NOT NULL,
        qty DECIMAL(14,4) NOT NULL,
        stock_before DECIMAL(14,4) NOT NULL,
        stock_after DECIMAL(14,4) NOT NULL,
        buying_price DECIMAL(12,2) NULL,
        place_brought_from VARCHAR(255) NULL,
        seller_phone VARCHAR(40) NULL,
        stock_out_reason VARCHAR(50) NULL,
        refunded TINYINT(1) NOT NULL DEFAULT 0,
        refund_amount DECIMAL(12,2) NULL,
        payment_status ENUM('pending_payment','partially_paid','paid') NOT NULL DEFAULT 'pending_payment',
        amount_paid DECIMAL(12,2) NOT NULL DEFAULT 0.00,
        note VARCHAR(255) NULL,
        created_by_employee_id INT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        KEY idx_sst_item (store_stock_item_id),
        KEY idx_sst_shop (shop_id),
        KEY idx_sst_created_at (created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql)
        logger.info("Table store_stock_transactions is ready.")
        return True
    except pymysql.Error as e:
        logger.warning("Could not init store_stock_transactions: %s", e)
        return False


def create_store_stock_item(
    *,
    shop_id: int,
    category: str,
    name: str,
    description: Optional[str],
    measure_unit: str,
    created_by_employee_id: Optional[int] = None,
) -> Optional[int]:
    if not init_store_stock_items_table():
        return None
    try:
        sid = int(shop_id)
    except Exception:
        return None
    if sid <= 0:
        return None
    cat = (category or "").strip()
    nm = (name or "").strip()
    desc = (description or "").strip() or None
    mu = (measure_unit or "").strip().lower()
    if not cat or not nm or not mu:
        return None
    if len(cat) > 100:
        cat = cat[:100]
    if len(nm) > 255:
        nm = nm[:255]
    if desc and len(desc) > 2000:
        desc = desc[:2000]
    if len(mu) > 50:
        mu = mu[:50]
    sql = """
    INSERT INTO store_stock_items
      (shop_id, category, name, description, measure_unit, status, created_by_employee_id)
    VALUES (%s, %s, %s, %s, %s, 'active', %s)
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql, (sid, cat, nm, desc, mu, created_by_employee_id))
            return int(cur.lastrowid or 0) or None
    except Exception:
        return None


STORE_STOCK_MEASURE_UNITS: frozenset[str] = frozenset(
    {"pcs", "kg", "g", "l", "ml", "pack", "crate", "box", "dozen", "portion"}
)


def _normalize_store_stock_item_row(row: Optional[dict]) -> Optional[dict]:
    if not row:
        return None
    try:
        row["stock_qty"] = round(float(row.get("stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
    except (TypeError, ValueError):
        row["stock_qty"] = 0.0
    return row


def get_store_stock_item_by_id(store_item_id: int) -> Optional[dict]:
    """Single ``store_stock_items`` row by primary key (IT catalog / admin)."""
    if not init_store_stock_items_table():
        return None
    try:
        iid = int(store_item_id)
    except (TypeError, ValueError):
        return None
    if iid <= 0:
        return None
    qty_col = "stock_qty" if column_exists("store_stock_items", "stock_qty") else "0 AS stock_qty"
    sql = f"""
    SELECT id, shop_id, category, name, description, measure_unit, {qty_col}, status, created_at
    FROM store_stock_items
    WHERE id = %s
    LIMIT 1
    """
    with get_cursor() as cur:
        cur.execute(sql, (iid,))
        row = cur.fetchone()
    return _normalize_store_stock_item_row(row)


def get_store_stock_item_for_shop(shop_id: int, store_item_id: int) -> Optional[dict]:
    """Single ``store_stock_items`` row scoped to a branch."""
    row = get_store_stock_item_by_id(store_item_id)
    if not row:
        return None
    try:
        if int(row.get("shop_id") or 0) != int(shop_id):
            return None
    except (TypeError, ValueError):
        return None
    return row


def update_store_stock_item_by_id(
    *,
    store_item_id: int,
    category: str,
    name: str,
    description: Optional[str],
    measure_unit: str,
) -> bool:
    row = get_store_stock_item_by_id(store_item_id)
    if not row:
        return False
    return update_store_stock_item_for_shop(
        shop_id=int(row["shop_id"]),
        store_item_id=int(store_item_id),
        category=category,
        name=name,
        description=description,
        measure_unit=measure_unit,
    )


def toggle_store_stock_item_status_by_id(store_item_id: int) -> Optional[str]:
    row = get_store_stock_item_by_id(store_item_id)
    if not row:
        return None
    return toggle_store_stock_item_status_for_shop(int(row["shop_id"]), int(store_item_id))


def delete_store_stock_item_by_id(store_item_id: int) -> Tuple[bool, str]:
    row = get_store_stock_item_by_id(store_item_id)
    if not row:
        return False, "Store stock item not found."
    return delete_store_stock_item_for_shop(int(row["shop_id"]), int(store_item_id))


def update_store_stock_item_for_shop(
    *,
    shop_id: int,
    store_item_id: int,
    category: str,
    name: str,
    description: Optional[str],
    measure_unit: str,
) -> bool:
    if not get_store_stock_item_for_shop(shop_id, store_item_id):
        return False
    cat = (category or "").strip()
    nm = (name or "").strip()
    desc = (description or "").strip() or None
    mu = (measure_unit or "").strip().lower()
    if not cat or not nm or mu not in STORE_STOCK_MEASURE_UNITS:
        return False
    if len(cat) > 100:
        cat = cat[:100]
    if len(nm) > 255:
        nm = nm[:255]
    if desc and len(desc) > 2000:
        desc = desc[:2000]
    if len(mu) > 50:
        mu = mu[:50]
    sql = """
    UPDATE store_stock_items
    SET category = %s, name = %s, description = %s, measure_unit = %s
    WHERE shop_id = %s AND id = %s
    """
    with get_cursor(commit=True) as cur:
        cur.execute(sql, (cat, nm, desc, mu, int(shop_id), int(store_item_id)))
        return cur.rowcount > 0


def toggle_store_stock_item_status_for_shop(shop_id: int, store_item_id: int) -> Optional[str]:
    """Toggle active/inactive (suspended) for a branch store SKU. Returns new status or None."""
    if not init_store_stock_items_table():
        return None
    try:
        sid = int(shop_id)
        iid = int(store_item_id)
    except (TypeError, ValueError):
        return None
    if sid <= 0 or iid <= 0:
        return None
    sql = """
    UPDATE store_stock_items
    SET status = IF(status = 'active', 'inactive', 'active')
    WHERE shop_id = %s AND id = %s
    """
    with get_cursor(commit=True) as cur:
        cur.execute(sql, (sid, iid))
        if cur.rowcount <= 0:
            return None
    row = get_store_stock_item_for_shop(sid, iid)
    return (row or {}).get("status")


def store_stock_item_has_transactions(store_item_id: int) -> bool:
    init_store_stock_transactions_table()
    try:
        iid = int(store_item_id)
    except (TypeError, ValueError):
        return False
    if iid <= 0:
        return False
    sql = "SELECT 1 FROM store_stock_transactions WHERE store_stock_item_id = %s LIMIT 1"
    with get_cursor() as cur:
        cur.execute(sql, (iid,))
        return bool(cur.fetchone())


def delete_store_stock_item_for_shop(shop_id: int, store_item_id: int) -> Tuple[bool, str]:
    """Hard-delete a store SKU when stock is zero and there is no movement history."""
    row = get_store_stock_item_for_shop(shop_id, store_item_id)
    if not row:
        return False, "Store stock item not found for this branch."
    try:
        qty = float(row.get("stock_qty") or 0)
    except (TypeError, ValueError):
        qty = 0.0
    if qty > 0:
        return (
            False,
            "Cannot delete while stock quantity is greater than zero. Stock out first or suspend the item.",
        )
    if store_stock_item_has_transactions(store_item_id):
        return False, "Cannot delete an item with stock history. Suspend it instead."
    sql = "DELETE FROM store_stock_items WHERE shop_id = %s AND id = %s"
    with get_cursor(commit=True) as cur:
        cur.execute(sql, (int(shop_id), int(store_item_id)))
        if cur.rowcount > 0:
            return True, "Store stock item deleted."
    return False, "Could not delete store stock item."


def list_store_stock_items_for_shop_catalog(shop_id: int, limit: int = 2000) -> list:
    """All store SKUs for a branch (active and suspended) for the manage-items table."""
    if not column_exists("store_stock_items", "id"):
        init_store_stock_items_table()
    try:
        sid = int(shop_id)
    except (TypeError, ValueError):
        return []
    if sid <= 0:
        return []
    qty_col = "stock_qty" if column_exists("store_stock_items", "stock_qty") else "0 AS stock_qty"
    sql = f"""
    SELECT id, shop_id, category, name, description, measure_unit, {qty_col}, status, created_at
    FROM store_stock_items
    WHERE shop_id = %s
    ORDER BY status ASC, category ASC, name ASC, id DESC
    LIMIT %s
    """
    with get_cursor() as cur:
        cur.execute(sql, (sid, int(limit)))
        rows = cur.fetchall() or []
    for r in rows:
        try:
            r["stock_qty"] = round(float(r.get("stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
        except (TypeError, ValueError):
            r["stock_qty"] = 0.0
    return rows


def list_store_stock_items(shop_id: int, *, limit: int = 5000, active_only: bool = True) -> list:
    if not column_exists("store_stock_items", "id"):
        init_store_stock_items_table()
    try:
        sid = int(shop_id)
    except Exception:
        return []
    if sid <= 0:
        return []
    where = "WHERE shop_id=%s"
    if active_only:
        where += " AND status='active'"
    qty_col = "stock_qty" if column_exists("store_stock_items", "stock_qty") else "0 AS stock_qty"
    sql = f"""
    SELECT id, shop_id, category, name, description, measure_unit, {qty_col}, status, created_at
    FROM store_stock_items
    {where}
    ORDER BY category ASC, name ASC, id DESC
    LIMIT %s
    """
    with get_cursor() as cur:
        cur.execute(sql, (sid, int(limit)))
        rows = cur.fetchall() or []
    for r in rows:
        try:
            r["stock_qty"] = round(float(r.get("stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
        except (TypeError, ValueError):
            r["stock_qty"] = 0.0
    return rows


def list_store_stock_items_for_shop_management(shop_id: int, limit: int = 2000) -> list:
    """Per-shop ``store_stock_items`` list for the shop stock-management bulk grid (Both mode).

    Mirrors :func:`list_store_stock_items_for_management` but scoped to one shop and
    includes ``last_buying_price`` so the bulk grid can prefill buy price.
    """
    if not column_exists("store_stock_items", "id"):
        init_store_stock_items_table()
    init_store_stock_transactions_table()
    try:
        sid = int(shop_id)
    except (TypeError, ValueError):
        return []
    if sid <= 0:
        return []
    qty_col = "ssi.stock_qty" if column_exists("store_stock_items", "stock_qty") else "0"
    sql = f"""
    SELECT
        ssi.id,
        ssi.shop_id,
        ssi.category,
        ssi.name,
        ssi.description,
        ssi.measure_unit,
        {qty_col} AS stock_qty,
        ssi.status,
        (
            SELECT sst.buying_price
            FROM store_stock_transactions sst
            WHERE sst.store_stock_item_id = ssi.id
              AND sst.direction = 'in'
              AND sst.buying_price IS NOT NULL
            ORDER BY sst.id DESC
            LIMIT 1
        ) AS last_buying_price
    FROM store_stock_items ssi
    WHERE ssi.shop_id = %s AND ssi.status = 'active'
    ORDER BY ssi.category ASC, ssi.name ASC, ssi.id DESC
    LIMIT %s
    """
    with get_cursor() as cur:
        cur.execute(sql, (sid, int(limit)))
        rows = cur.fetchall() or []
    for r in rows:
        try:
            r["stock_qty"] = round(float(r.get("stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
        except (TypeError, ValueError):
            r["stock_qty"] = 0.0
        lp = r.get("last_buying_price")
        try:
            r["last_buying_price"] = float(lp) if lp is not None else None
        except (TypeError, ValueError):
            r["last_buying_price"] = None
        r["image_path"] = None
    return rows


def list_store_stock_transactions_for_shop(shop_id: int, limit: int = 200) -> list:
    """Recent ``store_stock_transactions`` for one shop with ``item_name`` joined in.

    Used to render the recent-activity log on the shop stock-management page in Both mode.
    """
    init_store_stock_transactions_table()
    try:
        sid = int(shop_id)
    except (TypeError, ValueError):
        return []
    if sid <= 0:
        return []
    sql = """
    SELECT
        sst.id,
        sst.store_stock_item_id,
        sst.shop_id,
        sst.direction,
        sst.qty,
        sst.stock_before,
        sst.stock_after,
        sst.buying_price,
        sst.place_brought_from,
        sst.seller_phone,
        sst.stock_out_reason,
        sst.refunded,
        sst.refund_amount,
        sst.payment_status,
        sst.amount_paid,
        sst.note,
        sst.created_at,
        ssi.name AS item_name,
        ssi.category AS item_category,
        ssi.measure_unit AS item_measure_unit
    FROM store_stock_transactions sst
    LEFT JOIN store_stock_items ssi ON ssi.id = sst.store_stock_item_id
    WHERE sst.shop_id = %s
    ORDER BY sst.id DESC
    LIMIT %s
    """
    with get_cursor() as cur:
        cur.execute(sql, (sid, int(limit)))
        return cur.fetchall() or []


def list_store_stock_items_for_management(limit: int = 2000) -> list:
    """Cross-shop list of store_stock_items for the IT support stock-management page.

    Includes ``shop_name`` and ``last_buying_price`` (from latest stock in) so the
    bulk grid can prefill buying price and show shop grouping for Both-mode SKUs.
    """
    if not column_exists("store_stock_items", "id"):
        init_store_stock_items_table()
    init_store_stock_transactions_table()
    qty_col = "ssi.stock_qty" if column_exists("store_stock_items", "stock_qty") else "0"
    sql = f"""
    SELECT
        ssi.id,
        ssi.shop_id,
        ssi.category,
        ssi.name,
        ssi.description,
        ssi.measure_unit,
        {qty_col} AS stock_qty,
        ssi.status,
        COALESCE(s.shop_name, CONCAT('Shop #', ssi.shop_id)) AS shop_name,
        (
            SELECT sst.buying_price
            FROM store_stock_transactions sst
            WHERE sst.store_stock_item_id = ssi.id
              AND sst.direction = 'in'
              AND sst.buying_price IS NOT NULL
            ORDER BY sst.id DESC
            LIMIT 1
        ) AS last_buying_price
    FROM store_stock_items ssi
    LEFT JOIN shops s ON s.id = ssi.shop_id
    WHERE ssi.status = 'active'
    ORDER BY ssi.category ASC, ssi.name ASC, shop_name ASC, ssi.id DESC
    LIMIT %s
    """
    with get_cursor() as cur:
        cur.execute(sql, (int(limit),))
        rows = cur.fetchall() or []
    for r in rows:
        try:
            r["stock_qty"] = round(float(r.get("stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
        except (TypeError, ValueError):
            r["stock_qty"] = 0.0
        lp = r.get("last_buying_price")
        try:
            r["last_buying_price"] = float(lp) if lp is not None else None
        except (TypeError, ValueError):
            r["last_buying_price"] = None
        r["image_path"] = None
    return rows


def list_store_stock_items_for_management_catalog(limit: int = 5000) -> list:
    """Cross-shop store SKUs (active + suspended) for IT manage-items table."""
    if not column_exists("store_stock_items", "id"):
        init_store_stock_items_table()
    init_store_stock_transactions_table()
    qty_col = "ssi.stock_qty" if column_exists("store_stock_items", "stock_qty") else "0"
    sql = f"""
    SELECT
        ssi.id,
        ssi.shop_id,
        ssi.category,
        ssi.name,
        ssi.description,
        ssi.measure_unit,
        {qty_col} AS stock_qty,
        ssi.status,
        COALESCE(s.shop_name, CONCAT('Shop #', ssi.shop_id)) AS shop_name,
        (
            SELECT sst.buying_price
            FROM store_stock_transactions sst
            WHERE sst.store_stock_item_id = ssi.id
              AND sst.direction = 'in'
              AND sst.buying_price IS NOT NULL
            ORDER BY sst.id DESC
            LIMIT 1
        ) AS last_buying_price
    FROM store_stock_items ssi
    LEFT JOIN shops s ON s.id = ssi.shop_id
    ORDER BY ssi.status ASC, shop_name ASC, ssi.category ASC, ssi.name ASC, ssi.id DESC
    LIMIT %s
    """
    with get_cursor() as cur:
        cur.execute(sql, (int(limit),))
        rows = cur.fetchall() or []
    for r in rows:
        try:
            r["stock_qty"] = round(float(r.get("stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
        except (TypeError, ValueError):
            r["stock_qty"] = 0.0
        lp = r.get("last_buying_price")
        try:
            r["last_buying_price"] = float(lp) if lp is not None else None
        except (TypeError, ValueError):
            r["last_buying_price"] = None
        r["image_path"] = None
    return rows


def list_store_stock_transactions(
    store_item_id: int,
    direction: Optional[str] = None,
    limit: int = 200,
) -> list:
    """Movement history for a single ``store_stock_items`` row."""
    init_store_stock_transactions_table()
    try:
        sid = int(store_item_id)
    except (TypeError, ValueError):
        return []
    if sid <= 0:
        return []
    where = "WHERE sst.store_stock_item_id = %s"
    params: list = [sid]
    if direction in ("in", "out"):
        where += " AND sst.direction = %s"
        params.append(direction)
    sql = f"""
    SELECT
        sst.id,
        sst.direction,
        sst.qty,
        sst.stock_before,
        sst.stock_after,
        sst.buying_price,
        sst.place_brought_from,
        sst.seller_phone,
        sst.stock_out_reason,
        sst.refunded,
        sst.refund_amount,
        sst.payment_status,
        sst.amount_paid,
        sst.note,
        sst.created_at
    FROM store_stock_transactions sst
    {where}
    ORDER BY sst.id DESC
    LIMIT %s
    """
    params.append(int(limit))
    with get_cursor() as cur:
        cur.execute(sql, tuple(params))
        return cur.fetchall() or []


def _apply_store_stock_transaction_on_cursor(
    cur,
    *,
    store_stock_item_id: int,
    direction: str,
    qty,
    buying_price: Optional[float] = None,
    place_brought_from: Optional[str] = None,
    seller_phone: Optional[str] = None,
    stock_out_reason: Optional[str] = None,
    refunded: bool = False,
    refund_amount: Optional[float] = None,
    note: Optional[str] = None,
    created_by_employee_id: Optional[int] = None,
    payment_status: Optional[str] = None,
    amount_paid: Optional[float] = None,
) -> bool:
    """Apply one stock movement against ``store_stock_items`` using an open cursor."""
    if direction not in ("in", "out"):
        return False
    n = normalize_stock_move_qty(qty)
    if n is None:
        return False

    cur.execute(
        """
        SELECT shop_id, stock_qty, status
        FROM store_stock_items
        WHERE id=%s
        FOR UPDATE
        """,
        (int(store_stock_item_id),),
    )
    row = cur.fetchone()
    if not row:
        return False
    if (row.get("status") or "") != "active":
        return False

    shop_id_val = int(row.get("shop_id") or 0)
    before = round(float(row.get("stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
    if direction == "in":
        after = round(before + n, STOCK_QTY_DECIMAL_PLACES)
    else:
        after = round(before - n, STOCK_QTY_DECIMAL_PLACES)
        if after < 0:
            return False

    cur.execute(
        "UPDATE store_stock_items SET stock_qty=%s WHERE id=%s",
        (after, int(store_stock_item_id)),
    )

    if direction == "in":
        ps = (payment_status or "pending_payment").strip().lower()
        if ps not in {"pending_payment", "partially_paid", "paid"}:
            ps = "pending_payment"
        if amount_paid is not None:
            try:
                ap = max(0.0, float(amount_paid))
                if not math.isfinite(ap):
                    ap = 0.0
            except (TypeError, ValueError):
                ap = 0.0
        elif ps == "paid":
            bp = float(buying_price or 0.0)
            ap = max(0.0, round(float(n) * bp, 2))
        else:
            ap = 0.0
    else:
        ps = "pending_payment"
        ap = 0.0

    cur.execute(
        """
        INSERT INTO store_stock_transactions
            (
                store_stock_item_id, shop_id, direction, qty, stock_before, stock_after,
                buying_price, place_brought_from, seller_phone, stock_out_reason, refunded, refund_amount,
                payment_status, amount_paid,
                note, created_by_employee_id
            )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            int(store_stock_item_id),
            shop_id_val,
            direction,
            n,
            before,
            after,
            buying_price if buying_price is not None else None,
            place_brought_from or None,
            _stored_seller_phone(seller_phone),
            stock_out_reason or None,
            1 if refunded else 0,
            refund_amount if refund_amount is not None else None,
            ps,
            ap,
            note or None,
            created_by_employee_id,
        ),
    )
    return True


def create_store_stock_transactions_batch(
    *,
    operations: list,
    created_by_employee_id: Optional[int] = None,
) -> tuple:
    """Apply multiple store-stock movements atomically (all succeed or none)."""
    if not operations:
        return False, "No line items to apply."
    init_store_stock_transactions_table()
    try:
        with get_cursor(commit=True) as cur:
            for op in operations:
                ok = _apply_store_stock_transaction_on_cursor(
                    cur,
                    store_stock_item_id=int(op["store_stock_item_id"]),
                    direction=str(op["direction"]),
                    qty=op["qty"],
                    buying_price=op.get("buying_price"),
                    place_brought_from=op.get("place_brought_from"),
                    seller_phone=op.get("seller_phone"),
                    stock_out_reason=op.get("stock_out_reason"),
                    refunded=bool(op.get("refunded")),
                    refund_amount=op.get("refund_amount"),
                    note=op.get("note"),
                    created_by_employee_id=created_by_employee_id,
                    payment_status=op.get("payment_status"),
                    amount_paid=op.get("amount_paid"),
                )
                if not ok:
                    return False, (
                        f"Could not apply line for store item #{op.get('store_stock_item_id')} "
                        "(check quantity vs current shelf stock and that the SKU is active)."
                    )
    except Exception:
        return False, "Could not update store stock. Check database connection and item eligibility."

    n = len(operations)
    d0 = str(operations[0].get("direction") or "")
    dir_label = "stock in" if d0 == "in" else "stock out"
    return True, f"Applied {dir_label} for {n} store stock item(s)."


def list_store_stock_items_for_shop_buy(shop_id: int, limit: int = 2000) -> list:
    """Per-shop active store stock SKUs for the POS Buy items dropdown (Both mode)."""
    if not column_exists("store_stock_items", "id"):
        init_store_stock_items_table()
    try:
        sid = int(shop_id)
    except (TypeError, ValueError):
        return []
    if sid <= 0:
        return []
    qty_col = "stock_qty" if column_exists("store_stock_items", "stock_qty") else "0 AS stock_qty"
    sql = f"""
    SELECT id, shop_id, category, name, measure_unit, {qty_col}, status
    FROM store_stock_items
    WHERE shop_id=%s AND status='active'
    ORDER BY category ASC, name ASC, id ASC
    LIMIT %s
    """
    with get_cursor() as cur:
        cur.execute(sql, (sid, int(limit)))
        rows = cur.fetchall() or []
    for r in rows:
        try:
            r["stock_qty"] = round(float(r.get("stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
        except (TypeError, ValueError):
            r["stock_qty"] = 0.0
    return rows


def shop_manual_store_stock_in(
    *,
    shop_id: int,
    store_stock_item_id: int,
    qty,
    buying_price,
    place_brought_from: str,
    seller_phone: str,
    payment_status: str = "pending_payment",
    note: Optional[str] = None,
    created_by_employee_id: Optional[int] = None,
) -> bool:
    """Manual POS stock-in against a ``store_stock_items`` SKU (Both-mode shelf)."""
    place_brought_from = (place_brought_from or "").strip()
    if not place_brought_from:
        return False
    seller_phone = normalize_supplier_phone(seller_phone)
    if len(re.sub(r"\D", "", seller_phone)) < 9:
        return False

    if buying_price is None:
        bp = 0.0
    elif isinstance(buying_price, str):
        s = buying_price.strip().replace("\u00a0", "").replace(" ", "")
        if not s:
            bp = 0.0
        else:
            if "," in s and "." not in s:
                s = s.replace(",", ".")
            try:
                bp = float(s)
            except ValueError:
                return False
            if not math.isfinite(bp):
                return False
    else:
        try:
            bp = float(buying_price)
        except (TypeError, ValueError):
            return False
        if not math.isfinite(bp):
            return False
    if bp < 0:
        return False

    init_store_stock_transactions_table()
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(
                "SELECT shop_id, status FROM store_stock_items WHERE id=%s LIMIT 1",
                (int(store_stock_item_id),),
            )
            row = cur.fetchone()
            if not row or int(row.get("shop_id") or 0) != int(shop_id):
                return False
            if (row.get("status") or "") != "active":
                return False
            return _apply_store_stock_transaction_on_cursor(
                cur,
                store_stock_item_id=int(store_stock_item_id),
                direction="in",
                qty=qty,
                buying_price=bp,
                place_brought_from=place_brought_from.upper(),
                seller_phone=seller_phone,
                stock_out_reason=None,
                refunded=False,
                refund_amount=None,
                note=(note.strip().upper() if isinstance(note, str) and note.strip() else None),
                created_by_employee_id=created_by_employee_id,
                payment_status=payment_status,
                amount_paid=None,
            )
    except Exception:
        return False


def get_latest_shop_manual_store_stock_in_tx_id(
    shop_id: int,
    store_stock_item_id: int,
    created_by_employee_id: Optional[int] = None,
) -> Optional[int]:
    """Most recent store-stock manual stock-in tx id for receipt redirects."""
    init_store_stock_transactions_table()
    params: list = [int(shop_id), int(store_stock_item_id)]
    where_emp = ""
    if created_by_employee_id:
        where_emp = " AND created_by_employee_id=%s"
        params.append(int(created_by_employee_id))
    sql = f"""
    SELECT id
    FROM store_stock_transactions
    WHERE shop_id=%s
      AND store_stock_item_id=%s
      AND direction='in'
      {where_emp}
    ORDER BY id DESC
    LIMIT 1
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(params))
            row = cur.fetchone() or {}
            tid = int(row.get("id") or 0)
            return tid if tid > 0 else None
    except pymysql.Error:
        return None


def get_shop_store_stock_in_receipt_row(shop_id: int, tx_id: int):
    """Single store-stock stock-in tx row enriched for receipt printing.

    Returns the same shape as ``get_shop_stock_in_receipt_row`` so the existing
    ``shop_stock_in_receipt.html`` template can render it without changes.
    """
    init_store_stock_transactions_table()
    sql = """
    SELECT
        sst.id,
        sst.shop_id,
        sst.store_stock_item_id AS item_id,
        'manual' AS source,
        sst.direction,
        sst.qty,
        sst.buying_price,
        sst.place_brought_from,
        sst.seller_phone,
        sst.payment_status,
        sst.amount_paid,
        sst.note,
        sst.created_at,
        sh.shop_name,
        sh.shop_code,
        sh.shop_location,
        ssi.name AS item_name,
        ssi.category AS item_category,
        COALESCE(e.full_name, 'UNKNOWN') AS served_by
    FROM store_stock_transactions sst
    JOIN shops sh ON sh.id = sst.shop_id
    JOIN store_stock_items ssi ON ssi.id = sst.store_stock_item_id
    LEFT JOIN employees e ON e.id = sst.created_by_employee_id
    WHERE sst.shop_id=%s AND sst.id=%s AND sst.direction='in'
    LIMIT 1
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (int(shop_id), int(tx_id)))
            row = cur.fetchone() or None
    except pymysql.Error:
        return None
    if not row:
        return None
    r = dict(row)
    try:
        r["qty"] = float(r.get("qty") or 0)
    except (TypeError, ValueError):
        r["qty"] = 0.0
    try:
        r["buying_price"] = float(r.get("buying_price") or 0.0)
    except (TypeError, ValueError):
        r["buying_price"] = 0.0
    try:
        r["amount_paid"] = float(r.get("amount_paid") or 0.0)
    except (TypeError, ValueError):
        r["amount_paid"] = 0.0
    r["total_cost"] = float(r["qty"] * r["buying_price"])
    r["place_brought_from"] = (r.get("place_brought_from") or "").strip() or "-"
    r["seller_phone"] = (r.get("seller_phone") or "").strip() or "-"
    r["served_by"] = (r.get("served_by") or "").strip() or "UNKNOWN"
    r["note"] = (r.get("note") or "").strip()
    r["payment_status"] = (r.get("payment_status") or "pending_payment").strip().lower()
    return r


def init_shop_printer_settings_table():
    """Persist default receipt printer choice per shop (POS)."""
    sql = """
    CREATE TABLE IF NOT EXISTS shop_printer_settings (
        shop_id INT NOT NULL PRIMARY KEY,
        printer_type ENUM('bluetooth', 'network', 'usb') NOT NULL,
        device_label VARCHAR(255) DEFAULT NULL,
        config_json TEXT NOT NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        KEY idx_shop_printer_updated (updated_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql)
        logger.info("Table shop_printer_settings is ready.")
        return True
    except pymysql.Error as e:
        logger.warning("Could not init shop_printer_settings: %s", e)
        return False


def init_shop_customers_table():
    """Persist shop customers for POS phone lookup/autofill."""
    sql = """
    CREATE TABLE IF NOT EXISTS shop_customers (
        id INT AUTO_INCREMENT PRIMARY KEY,
        shop_id INT NOT NULL,
        customer_name VARCHAR(190) NOT NULL,
        phone VARCHAR(40) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uq_shop_customer_phone (shop_id, phone),
        KEY idx_shop_customer_name (shop_id, customer_name),
        KEY idx_shop_customer_updated (updated_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql)
        logger.info("Table shop_customers is ready.")
        return True
    except pymysql.Error as e:
        logger.warning("Could not init shop_customers: %s", e)
        return False


def init_public_customers_table() -> bool:
    """Persist website (/quote) customers for phone-first lookup/autofill."""
    sql = """
    CREATE TABLE IF NOT EXISTS public_customers (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        customer_name VARCHAR(190) NOT NULL,
        phone VARCHAR(40) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uq_public_customer_phone (phone),
        KEY idx_public_customer_updated (updated_at),
        KEY idx_public_customer_name (customer_name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql)
        logger.info("Table public_customers is ready.")
        return True
    except pymysql.Error as e:
        logger.warning("Could not init public_customers: %s", e)
        return False


def init_sellers_table() -> bool:
    """Global sellers/suppliers registry for stock-in workflows."""
    sql = """
    CREATE TABLE IF NOT EXISTS sellers (
        id INT AUTO_INCREMENT PRIMARY KEY,
        seller_name VARCHAR(190) NOT NULL,
        phone VARCHAR(40) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uq_seller_phone (phone),
        KEY idx_seller_name (seller_name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql)
        logger.info("Table sellers is ready.")
        return True
    except pymysql.Error as e:
        logger.warning("Could not init sellers: %s", e)
        return False


def _normalize_phone(phone: str) -> str:
    raw = (phone or "").strip()
    return "".join(ch for ch in raw if ch.isdigit() or ch == "+")


def _is_valid_seller_phone(phone: str) -> bool:
    p = _normalize_phone(phone)
    digits = re.sub(r"\D", "", p)
    if len(digits) == 10:
        return digits.startswith("07") or digits.startswith("01")
    # Kenyan international: 254 + 9-digit national number (mobile often 7…).
    if len(digits) == 12:
        return digits.startswith("254")
    return False


def _seller_phone_lookup_keys(phone: str) -> list[str]:
    """Possible ``sellers.phone`` values for the same Kenyan subscriber (DB rows vary)."""
    p = _normalize_phone(phone or "")
    digits = re.sub(r"\D", "", p)
    keys: list[str] = []

    def add(x: str) -> None:
        x = (x or "").strip()
        if x and x not in keys:
            keys.append(x)

    if len(digits) == 10 and digits.startswith(("07", "01")):
        intl = "254" + digits[1:]
        add(intl)
        add(digits)
        add("+" + intl)
    elif len(digits) == 12 and digits.startswith("254"):
        add(digits)
        add("0" + digits[3:])
        add("+" + digits)
    elif len(digits) == 9 and digits[0] in ("7", "1"):
        intl = "254" + digits
        add(intl)
        add("0" + digits)
        add("+" + intl)
        add(digits)
    add(p)
    return keys


def get_seller_by_phone(phone: str):
    p = _normalize_phone(phone)
    if not _is_valid_seller_phone(p):
        return None
    sql = "SELECT id, seller_name, phone, created_at, updated_at FROM sellers WHERE phone=%s LIMIT 1"
    try:
        with get_cursor() as cur:
            for key in _seller_phone_lookup_keys(p):
                cur.execute(sql, (key,))
                row = cur.fetchone()
                if row:
                    return row
            return None
    except pymysql.Error:
        return None


def upsert_seller(seller_name: str, phone: str) -> bool:
    name = (seller_name or "").strip()
    p = normalize_supplier_phone(phone)
    if len(name) < 2 or not _is_valid_seller_phone(p):
        return False
    sql = """
    INSERT INTO sellers (seller_name, phone)
    VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE
        seller_name=VALUES(seller_name),
        updated_at=CURRENT_TIMESTAMP
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql, (name[:190], p[:40]))
            return True
    except pymysql.Error:
        return False


def resolve_seller_name_and_phone(seller_phone: str, seller_name: str) -> tuple[str, str] | tuple[None, None]:
    """
    Validate/resolve seller by phone.
    - If phone is registered, return registered name + phone.
    - If not registered and name provided, auto-register and return it.
    """
    p = normalize_supplier_phone(seller_phone)
    if not _is_valid_seller_phone(p):
        return (None, None)
    existing = get_seller_by_phone(p) or {}
    if existing:
        nm = (existing.get("seller_name") or "").strip()
        if len(nm) < 2:
            alt = (seller_name or "").strip().upper()
            if len(alt) >= 2 and upsert_seller(alt, p):
                nm = alt
        if len(nm) < 2:
            return (None, None)
        stored = normalize_supplier_phone(existing.get("phone") or p) or p
        return (nm, stored)
    n = (seller_name or "").strip().upper()
    if len(n) < 2:
        return (None, None)
    if not upsert_seller(n, p):
        return (None, None)
    return (n, p)


def get_public_customer_by_phone(phone: str):
    p = _normalize_phone(phone or "")
    if not p:
        return None
    sql = "SELECT id, customer_name, phone, created_at, updated_at FROM public_customers WHERE phone=%s LIMIT 1"
    try:
        with get_cursor() as cur:
            for key in _seller_phone_lookup_keys(p):
                cur.execute(sql, (key,))
                row = cur.fetchone()
                if row:
                    return row
            return None
    except pymysql.Error:
        return None


def _meaningful_customer_name(name: str) -> str:
    n = (name or "").strip()
    if len(n) < 2:
        return ""
    low = re.sub(r"\s+", " ", n.lower().replace("-", " ")).strip()
    if low in ("", "walk in", "walk in customer", "walkin", "walkin customer"):
        return ""
    return n


def lookup_storefront_customer_by_phone(phone: str):
    """Read-only name lookup for public storefront autofill (never registers)."""
    digits = re.sub(r"\D", "", _normalize_phone(phone or ""))
    if len(digits) < 7:
        return None

    def _pack(name: str, phone_val: str | None):
        clean = _meaningful_customer_name(name)
        if not clean:
            return None
        ph = (phone_val or "").strip() or _canonical_kenya_phone(phone)
        return {"customer_name": clean, "phone": ph}

    pub = get_public_customer_by_phone(phone)
    if pub:
        row = _pack(pub.get("customer_name") or "", pub.get("phone"))
        if row:
            return row

    keys = _seller_phone_lookup_keys(phone)
    if not keys:
        return None
    placeholders = ",".join(["%s"] * len(keys))
    params = tuple(keys * 3)
    sql = f"""
    SELECT customer_name, phone, sort_ts FROM (
        SELECT customer_name, phone, updated_at AS sort_ts
        FROM shop_customers
        WHERE phone IN ({placeholders})
          AND TRIM(customer_name) <> ''
        UNION ALL
        SELECT customer_name, customer_phone AS phone, created_at AS sort_ts
        FROM shop_pos_sales
        WHERE customer_phone IN ({placeholders})
          AND TRIM(COALESCE(customer_name, '')) <> ''
          AND TRIM(COALESCE(customer_phone, '')) NOT IN ('', '-')
        UNION ALL
        SELECT customer_name, customer_phone AS phone, created_at AS sort_ts
        FROM shop_pos_quotations
        WHERE customer_phone IN ({placeholders})
          AND TRIM(COALESCE(customer_name, '')) <> ''
          AND TRIM(COALESCE(customer_phone, '')) <> ''
    ) AS hits
    ORDER BY sort_ts DESC
    LIMIT 1
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            if row:
                packed = _pack(row.get("customer_name") or "", row.get("phone"))
                if packed:
                    return packed
    except pymysql.Error:
        pass
    return None


def upsert_public_customer(customer_name: str, phone: str) -> bool:
    name = (customer_name or "").strip()
    p = normalize_customer_phone(phone)
    if len(name) < 2 or not p or p == "-":
        return False
    sql = """
    INSERT INTO public_customers (customer_name, phone)
    VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE
      customer_name = VALUES(customer_name),
      updated_at = CURRENT_TIMESTAMP
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql, (name[:190], p[:40]))
            return cur.rowcount > 0
    except pymysql.Error:
        return False


def init_shop_pos_sales_table():
    """Persist POS checkouts for analytics (sale vs credit revenue)."""
    sql = """
    CREATE TABLE IF NOT EXISTS shop_pos_sales (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        shop_id INT NOT NULL,
        sale_type ENUM('sale', 'credit') NOT NULL,
        payment_method ENUM('cash', 'mpesa', 'both', 'credit') NULL,
        cash_amount DECIMAL(12,2) NOT NULL DEFAULT 0.00,
        mpesa_amount DECIMAL(12,2) NOT NULL DEFAULT 0.00,
        total_amount DECIMAL(12,2) NOT NULL DEFAULT 0.00,
        credit_paid_amount DECIMAL(12,2) NOT NULL DEFAULT 0.00,
        credit_status ENUM('not_paid', 'partially_paid', 'paid') NULL,
        item_count INT NOT NULL DEFAULT 0,
        customer_name VARCHAR(190) NULL,
        customer_phone VARCHAR(40) NULL,
        employee_id INT NULL,
        employee_code CHAR(6) NULL,
        employee_name VARCHAR(190) NULL,
        receipt_number VARCHAR(96) NULL,
        receipt_scope_key VARCHAR(32) NULL,
        receipt_sequence INT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        KEY idx_shop_pos_sales_shop_created (shop_id, created_at),
        KEY idx_shop_pos_sales_type_created (sale_type, created_at),
        KEY idx_shop_pos_sales_receipt_seq (shop_id, receipt_scope_key, receipt_sequence)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql)
        logger.info("Table shop_pos_sales is ready.")
        return True
    except pymysql.Error as e:
        logger.warning("Could not init shop_pos_sales: %s", e)
        return False


_ENSURED_POS_SALES_INVENTORY_MODE = False
_ENSURED_POS_SALES_RECEIPT_COLUMNS = False


def ensure_shop_pos_sales_inventory_mode_column() -> bool:
    """Persist checkout inventory mode (shop / kitchen / none) for sales-based kitchen analytics."""
    global _ENSURED_POS_SALES_INVENTORY_MODE
    if _ENSURED_POS_SALES_INVENTORY_MODE:
        return True
    init_shop_pos_sales_table()
    try:
        with get_cursor(commit=True) as cur:
            if not column_exists("shop_pos_sales", "inventory_mode"):
                cur.execute(
                    """
                    ALTER TABLE shop_pos_sales
                    ADD COLUMN inventory_mode VARCHAR(16) NULL DEFAULT NULL
                    COMMENT 'shop|kitchen|none at checkout'
                    AFTER employee_name
                    """
                )
        _ENSURED_POS_SALES_INVENTORY_MODE = True
        return True
    except pymysql.Error as e:
        logger.warning("Could not add shop_pos_sales.inventory_mode: %s", e)
        return False


def ensure_shop_pos_sales_receipt_columns() -> bool:
    """Persist server-generated receipt identity per POS checkout.

    Cached per process: the first call performs schema migration, later calls
    are a no-op so receipt list pages don't pay an ALTER TABLE round-trip.
    """
    global _ENSURED_POS_SALES_RECEIPT_COLUMNS
    if _ENSURED_POS_SALES_RECEIPT_COLUMNS:
        return True
    init_shop_pos_sales_table()
    try:
        with get_cursor(commit=True) as cur:
            if not column_exists("shop_pos_sales", "receipt_number"):
                cur.execute(
                    """
                    ALTER TABLE shop_pos_sales
                    ADD COLUMN receipt_number VARCHAR(96) NULL DEFAULT NULL
                    AFTER employee_name
                    """
                )
            if not column_exists("shop_pos_sales", "receipt_scope_key"):
                cur.execute(
                    """
                    ALTER TABLE shop_pos_sales
                    ADD COLUMN receipt_scope_key VARCHAR(32) NULL DEFAULT NULL
                    AFTER receipt_number
                    """
                )
            if not column_exists("shop_pos_sales", "receipt_sequence"):
                cur.execute(
                    """
                    ALTER TABLE shop_pos_sales
                    ADD COLUMN receipt_sequence INT NULL DEFAULT NULL
                    AFTER receipt_scope_key
                    """
                )
            need_modify = False
            if not column_exists("shop_pos_sales", "receipt_mark_status"):
                cur.execute(
                    """
                    ALTER TABLE shop_pos_sales
                    ADD COLUMN receipt_mark_status ENUM('pending','confirmed','cancelled','partial_return','returned') NOT NULL DEFAULT 'pending'
                    AFTER receipt_sequence
                    """
                )
            else:
                # Only run MODIFY if the enum definition is out of date.
                try:
                    cur.execute(
                        """
                        SELECT COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'shop_pos_sales' AND COLUMN_NAME = 'receipt_mark_status'
                        LIMIT 1
                        """,
                        (get_database_name(),),
                    )
                    row = cur.fetchone()
                    col_type = ""
                    if row:
                        col_type = str((row.get("COLUMN_TYPE") if isinstance(row, dict) else row[0]) or "").lower()
                    if "partial_return" not in col_type or "returned" not in col_type:
                        need_modify = True
                except pymysql.Error:
                    need_modify = True
            if need_modify:
                cur.execute(
                    """
                    ALTER TABLE shop_pos_sales
                    MODIFY COLUMN receipt_mark_status ENUM('pending','confirmed','cancelled','partial_return','returned') NOT NULL DEFAULT 'pending'
                    """
                )
            if not column_exists("shop_pos_sales", "receipt_return_restocked"):
                cur.execute(
                    """
                    ALTER TABLE shop_pos_sales
                    ADD COLUMN receipt_return_restocked TINYINT(1) NOT NULL DEFAULT 0
                    AFTER receipt_mark_status
                    """
                )
            if not column_exists("shop_pos_sales", "reprint_count"):
                cur.execute(
                    """
                    ALTER TABLE shop_pos_sales
                    ADD COLUMN reprint_count INT NOT NULL DEFAULT 0
                    AFTER receipt_return_restocked
                    """
                )
            try:
                cur.execute(
                    """
                    CREATE INDEX idx_shop_pos_sales_receipt_seq
                    ON shop_pos_sales (shop_id, receipt_scope_key, receipt_sequence)
                    """
                )
            except pymysql.Error:
                pass
            # Composite index that backs the receipts register list query.
            try:
                cur.execute(
                    """
                    CREATE INDEX idx_shop_pos_sales_shop_created
                    ON shop_pos_sales (shop_id, created_at)
                    """
                )
            except pymysql.Error:
                pass
        _ENSURED_POS_SALES_RECEIPT_COLUMNS = True
        return True
    except pymysql.Error as e:
        logger.warning("Could not add shop_pos_sales receipt columns: %s", e)
        return False


def ensure_shop_pos_quotations_client_txn_column() -> bool:
    """Adds client_txn_id for idempotent offline quotation sync (same pattern as POS sales)."""
    init_shop_pos_quotations_table()
    try:
        with get_cursor(commit=True) as cur:
            if not column_exists("shop_pos_quotations", "client_txn_id"):
                cur.execute(
                    """
                    ALTER TABLE shop_pos_quotations
                    ADD COLUMN client_txn_id VARCHAR(64) NULL DEFAULT NULL
                    AFTER employee_name
                    """
                )
            try:
                cur.execute(
                    """
                    CREATE UNIQUE INDEX uq_shop_pos_quotations_shop_client_txn
                    ON shop_pos_quotations (shop_id, client_txn_id)
                    """
                )
            except pymysql.Error:
                pass
        return True
    except pymysql.Error as e:
        logger.warning("Could not add shop_pos_quotations.client_txn_id: %s", e)
        return False


def ensure_shop_pos_sales_client_txn_column() -> bool:
    """Adds client transaction ID for idempotent offline sync retries."""
    init_shop_pos_sales_table()
    try:
        with get_cursor(commit=True) as cur:
            if not column_exists("shop_pos_sales", "client_txn_id"):
                cur.execute(
                    """
                    ALTER TABLE shop_pos_sales
                    ADD COLUMN client_txn_id VARCHAR(64) NULL DEFAULT NULL
                    AFTER receipt_sequence
                    """
                )
            try:
                cur.execute(
                    """
                    CREATE UNIQUE INDEX uq_shop_pos_sales_client_txn
                    ON shop_pos_sales (shop_id, client_txn_id)
                    """
                )
            except pymysql.Error:
                pass
        return True
    except pymysql.Error as e:
        logger.warning("Could not add shop_pos_sales.client_txn_id: %s", e)
        return False


def ensure_shop_pos_sales_mpesa_receipt_column() -> bool:
    """Store Safaricom M-Pesa receipt code for POS STK/checkouts."""
    init_shop_pos_sales_table()
    try:
        with get_cursor(commit=True) as cur:
            if not column_exists("shop_pos_sales", "mpesa_receipt_number"):
                cur.execute(
                    """
                    ALTER TABLE shop_pos_sales
                    ADD COLUMN mpesa_receipt_number VARCHAR(32) NULL DEFAULT NULL
                    AFTER mpesa_amount
                    """
                )
        return True
    except pymysql.Error as e:
        logger.warning("Could not add shop_pos_sales.mpesa_receipt_number: %s", e)
        return False


_MPESA_STK_REQUESTS_TABLE_READY = False


def init_mpesa_stk_requests_table() -> bool:
    """Persist Lipa Na M-Pesa STK Push status and Safaricom receipt references."""
    global _MPESA_STK_REQUESTS_TABLE_READY
    if _MPESA_STK_REQUESTS_TABLE_READY and table_exists("mpesa_stk_requests"):
        return True
    sql = """
    CREATE TABLE IF NOT EXISTS mpesa_stk_requests (
        checkout_request_id VARCHAR(64) NOT NULL PRIMARY KEY,
        merchant_request_id VARCHAR(64) NULL,
        shop_id INT NULL,
        phone VARCHAR(24) NULL,
        amount DECIMAL(12,2) NULL,
        result_code VARCHAR(16) NULL,
        result_desc VARCHAR(512) NULL,
        mpesa_receipt_number VARCHAR(32) NULL,
        completed TINYINT(1) NOT NULL DEFAULT 0,
        pending TINYINT(1) NOT NULL DEFAULT 1,
        status_json JSON NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_mpesa_stk_shop (shop_id),
        INDEX idx_mpesa_stk_receipt (mpesa_receipt_number),
        INDEX idx_mpesa_stk_created (created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql)
        _MPESA_STK_REQUESTS_TABLE_READY = True
        return True
    except pymysql.Error as e:
        logger.warning("Could not init mpesa_stk_requests: %s", e)
        return False


def _mpesa_receipt_from_stk_payload(data: dict) -> str:
    direct = str((data or {}).get("mpesa_receipt_number") or "").strip()
    if direct:
        return direct[:32]
    meta = (data or {}).get("metadata")
    if not isinstance(meta, dict):
        return ""
    for key in ("MpesaReceiptNumber", "mpesa_receipt_number", "ReceiptNumber"):
        val = str(meta.get(key) or "").strip()
        if val:
            return val[:32]
    return ""


def _mpesa_stk_status_json(data: dict) -> str:
    try:
        return json.dumps(data or {}, ensure_ascii=False, default=str)
    except Exception:
        return "{}"


def _mpesa_stk_status_from_row(row: Optional[dict]) -> Optional[dict]:
    if not row:
        return None
    raw = row.get("status_json")
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="replace")
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass
    out = {
        "checkout_request_id": row.get("checkout_request_id"),
        "merchant_request_id": row.get("merchant_request_id"),
        "shop_id": row.get("shop_id"),
        "phone": row.get("phone"),
        "amount": float(row.get("amount") or 0) if row.get("amount") is not None else None,
        "result_code": row.get("result_code"),
        "result_desc": row.get("result_desc"),
        "mpesa_receipt_number": row.get("mpesa_receipt_number"),
        "completed": bool(row.get("completed")),
        "pending": bool(row.get("pending")),
    }
    receipt = str(row.get("mpesa_receipt_number") or "").strip()
    if receipt:
        out["metadata"] = {"MpesaReceiptNumber": receipt}
    return out


def upsert_mpesa_stk_request(checkout_request_id: str, data: dict) -> bool:
    """Insert or update STK push status (merged payload stored as JSON)."""
    cid = str(checkout_request_id or "").strip()
    if not cid or not init_mpesa_stk_requests_table():
        return False
    d = data if isinstance(data, dict) else {}
    receipt = _mpesa_receipt_from_stk_payload(d)
    if receipt:
        d = {**d, "mpesa_receipt_number": receipt}
    rc = d.get("result_code")
    result_code = "" if rc is None else str(rc).strip()[:16]
    result_desc = str(d.get("result_desc") or "").strip()[:512]
    completed = 1 if d.get("completed") else 0
    pending = 1 if d.get("pending") else 0
    if completed and str(rc) == "0":
        pending = 0
    sql = """
    INSERT INTO mpesa_stk_requests (
        checkout_request_id, merchant_request_id, shop_id, phone, amount,
        result_code, result_desc, mpesa_receipt_number, completed, pending, status_json
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        merchant_request_id = COALESCE(VALUES(merchant_request_id), merchant_request_id),
        shop_id = COALESCE(VALUES(shop_id), shop_id),
        phone = COALESCE(VALUES(phone), phone),
        amount = COALESCE(VALUES(amount), amount),
        result_code = COALESCE(NULLIF(VALUES(result_code), ''), result_code),
        result_desc = COALESCE(NULLIF(VALUES(result_desc), ''), result_desc),
        mpesa_receipt_number = COALESCE(NULLIF(VALUES(mpesa_receipt_number), ''), mpesa_receipt_number),
        completed = GREATEST(completed, VALUES(completed)),
        pending = CASE
            WHEN VALUES(completed) = 1 AND VALUES(result_code) = '0' THEN 0
            WHEN VALUES(pending) = 0 THEN 0
            ELSE pending
        END,
        status_json = VALUES(status_json),
        updated_at = CURRENT_TIMESTAMP
    """
    try:
        shop_id = d.get("shop_id")
        try:
            shop_id_val = int(shop_id) if shop_id is not None else None
        except (TypeError, ValueError):
            shop_id_val = None
        amt = d.get("amount")
        try:
            amount_val = round(float(amt), 2) if amt is not None else None
        except (TypeError, ValueError):
            amount_val = None
        with get_cursor(commit=True) as cur:
            cur.execute(
                sql,
                (
                    cid[:64],
                    str(d.get("merchant_request_id") or "").strip()[:64] or None,
                    shop_id_val,
                    str(d.get("phone") or "").strip()[:24] or None,
                    amount_val,
                    result_code or None,
                    result_desc or None,
                    receipt or None,
                    completed,
                    pending,
                    _mpesa_stk_status_json(d),
                ),
            )
        return True
    except pymysql.Error as e:
        logger.warning("upsert_mpesa_stk_request failed for %s: %s", cid, e)
        return False


def get_mpesa_stk_request(checkout_request_id: str) -> Optional[dict]:
    cid = str(checkout_request_id or "").strip()
    if not cid or not init_mpesa_stk_requests_table():
        return None
    sql = """
    SELECT checkout_request_id, merchant_request_id, shop_id, phone, amount,
           result_code, result_desc, mpesa_receipt_number, completed, pending, status_json
    FROM mpesa_stk_requests
    WHERE checkout_request_id = %s
    LIMIT 1
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (cid[:64],))
            row = cur.fetchone()
        return _mpesa_stk_status_from_row(row)
    except pymysql.Error as e:
        logger.warning("get_mpesa_stk_request failed for %s: %s", cid, e)
        return None


def _safe_receipt_settings_dict(raw: Any) -> dict:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    if not isinstance(raw, str):
        return {}
    s = raw.strip()
    if not s:
        return {}
    try:
        parsed = json.loads(s)
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _effective_receipt_number_settings(shop_receipt_json: Any) -> tuple[str, str, int]:
    defaults = {
        "receipt_number_format": "sequential",
        "receipt_number_prefix": "T",
        "starting_number": "1001",
    }
    site_raw = (get_site_settings(["receipt_settings_json"]).get("receipt_settings_json") or "").strip() or "{}"
    merged: dict = dict(defaults)
    merged.update(_safe_receipt_settings_dict(site_raw))
    merged.update(_safe_receipt_settings_dict(shop_receipt_json))

    fmt = str(merged.get("receipt_number_format") or "sequential").strip().lower()
    if fmt not in ("sequential", "daily", "per_month"):
        fmt = "sequential"
    prefix = str(merged.get("receipt_number_prefix") or "T").strip() or "T"
    prefix = prefix[:32]
    try:
        start = int(str(merged.get("starting_number") or "1001").strip())
    except Exception:
        start = 1001
    if start < 0:
        start = 1001
    return fmt, prefix, start


def _receipt_scope_key(fmt: str, now_dt: datetime) -> str:
    f = (fmt or "sequential").strip().lower()
    if f == "daily":
        return "d-" + now_dt.strftime("%Y-%m-%d")
    if f == "per_month":
        return "m-" + now_dt.strftime("%Y-%m")
    return "seq"


def list_kitchen_portion_analytics_lines(
    *,
    date_from: date,
    date_to: date,
    shop_id: Optional[int] = None,
    limit: int = 50000,
):
    """Detail rows for portion-based POS sales (``kitchen`` and ``both`` inventory modes)."""
    ensure_shop_pos_sales_inventory_mode_column()
    init_shop_pos_sale_items_table()
    dt_start = datetime.combine(date_from, datetime.min.time())
    dt_end_excl = datetime.combine(date_to + timedelta(days=1), datetime.min.time())
    # Kitchen + "both" POS modes deduct portions at checkout (never shelf in those flows).
    where = ["s.created_at >= %s", "s.created_at < %s", "s.inventory_mode IN ('kitchen', 'both')"]
    params: list = [dt_start, dt_end_excl]
    if shop_id is not None and int(shop_id) > 0:
        where.append("s.shop_id = %s")
        params.append(int(shop_id))
    sql = f"""
    SELECT
        s.id AS sale_id,
        s.shop_id,
        COALESCE(sh.shop_name, '') AS shop_name,
        sh.shop_code AS shop_code,
        s.created_at AS sold_at,
        spi.item_id,
        spi.item_name,
        spi.qty AS portions_qty,
        s.sale_type
    FROM shop_pos_sale_items spi
    INNER JOIN shop_pos_sales s ON s.id = spi.sale_id AND s.shop_id = spi.shop_id
    LEFT JOIN shops sh ON sh.id = s.shop_id
    WHERE {' AND '.join(where)}
    ORDER BY s.created_at DESC, spi.id DESC
    LIMIT %s
    """
    params.append(int(limit))
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
    except pymysql.Error:
        return []
    for r in rows:
        if r.get("portions_qty") is not None:
            r["portions_qty"] = int(r["portions_qty"] or 0)
        if r.get("item_id") is not None:
            try:
                r["item_id"] = int(r["item_id"])
            except (TypeError, ValueError):
                r["item_id"] = None
    return rows


def kitchen_portion_analytics_by_item(
    *,
    date_from: date,
    date_to: date,
    shop_id: Optional[int] = None,
    limit: int = 30,
):
    """Total portions sold per item (POS modes that deduct kitchen portions: kitchen, both)."""
    ensure_shop_pos_sales_inventory_mode_column()
    init_shop_pos_sale_items_table()
    dt_start = datetime.combine(date_from, datetime.min.time())
    dt_end_excl = datetime.combine(date_to + timedelta(days=1), datetime.min.time())
    where = ["s.created_at >= %s", "s.created_at < %s", "s.inventory_mode IN ('kitchen', 'both')"]
    params: list = [dt_start, dt_end_excl]
    if shop_id is not None and int(shop_id) > 0:
        where.append("s.shop_id = %s")
        params.append(int(shop_id))
    sql = f"""
    SELECT
        spi.item_id,
        MAX(spi.item_name) AS item_name,
        COALESCE(SUM(spi.qty), 0) AS total_portions
    FROM shop_pos_sale_items spi
    INNER JOIN shop_pos_sales s ON s.id = spi.sale_id AND s.shop_id = spi.shop_id
    WHERE {' AND '.join(where)}
    GROUP BY spi.item_id
    ORDER BY total_portions DESC
    LIMIT %s
    """
    params.append(int(limit))
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
    except pymysql.Error:
        return []
    for r in rows:
        r["total_portions"] = int(r.get("total_portions") or 0)
    return rows


def kitchen_portion_analytics_by_day(
    *,
    date_from: date,
    date_to: date,
    shop_id: Optional[int] = None,
):
    """Total portions sold per calendar day (kitchen + both POS modes)."""
    ensure_shop_pos_sales_inventory_mode_column()
    init_shop_pos_sale_items_table()
    dt_start = datetime.combine(date_from, datetime.min.time())
    dt_end_excl = datetime.combine(date_to + timedelta(days=1), datetime.min.time())
    where = ["s.created_at >= %s", "s.created_at < %s", "s.inventory_mode IN ('kitchen', 'both')"]
    params: list = [dt_start, dt_end_excl]
    if shop_id is not None and int(shop_id) > 0:
        where.append("s.shop_id = %s")
        params.append(int(shop_id))
    sql = f"""
    SELECT
        DATE(s.created_at) AS day,
        COALESCE(SUM(spi.qty), 0) AS total_portions
    FROM shop_pos_sale_items spi
    INNER JOIN shop_pos_sales s ON s.id = spi.sale_id AND s.shop_id = spi.shop_id
    WHERE {' AND '.join(where)}
    GROUP BY DATE(s.created_at)
    ORDER BY day ASC
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
    except pymysql.Error:
        return []
    out = []
    for r in rows:
        d = r.get("day")
        if hasattr(d, "isoformat"):
            ds = d.isoformat()
        else:
            ds = str(d)[:10]
        out.append({"day": ds, "total_portions": int(r.get("total_portions") or 0)})
    return out


def ensure_shop_credit_payments_schema() -> bool:
    """Ensure columns/tables exist for credit payment tracking (safe to call often)."""
    ok = True
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS shop_credit_payments (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    shop_id INT NOT NULL,
                    customer_name VARCHAR(190) NOT NULL,
                    customer_phone VARCHAR(40) NOT NULL,
                    amount DECIMAL(12,2) NOT NULL DEFAULT 0.00,
                    note VARCHAR(255) NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    KEY idx_shop_credit_payments_shop_created (shop_id, created_at),
                    KEY idx_shop_credit_payments_customer (shop_id, customer_phone, customer_name)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
                """
            )
    except pymysql.Error:
        ok = False

    try:
        with get_cursor(commit=True) as cur:
            if not column_exists("shop_pos_sales", "credit_paid_amount"):
                cur.execute(
                    "ALTER TABLE shop_pos_sales ADD COLUMN credit_paid_amount DECIMAL(12,2) NOT NULL DEFAULT 0.00"
                )
            if not column_exists("shop_pos_sales", "credit_status"):
                cur.execute(
                    "ALTER TABLE shop_pos_sales ADD COLUMN credit_status ENUM('not_paid','partially_paid','paid') NULL"
                )
            if not column_exists("shop_pos_sales", "credit_due_date"):
                cur.execute(
                    "ALTER TABLE shop_pos_sales ADD COLUMN credit_due_date DATE NULL"
                )
            if not column_exists("shop_pos_sales", "payment_method"):
                cur.execute(
                    "ALTER TABLE shop_pos_sales ADD COLUMN payment_method ENUM('cash','mpesa','both','credit') NULL"
                )
            if not column_exists("shop_pos_sales", "cash_amount"):
                cur.execute(
                    "ALTER TABLE shop_pos_sales ADD COLUMN cash_amount DECIMAL(12,2) NOT NULL DEFAULT 0.00"
                )
            if not column_exists("shop_pos_sales", "mpesa_amount"):
                cur.execute(
                    "ALTER TABLE shop_pos_sales ADD COLUMN mpesa_amount DECIMAL(12,2) NOT NULL DEFAULT 0.00"
                )
    except pymysql.Error:
        ok = False
    return ok


def list_shop_credit_customers_with_balance(
    shop_id: int,
    limit: int = 2000,
    analytics_filter: Optional[dict] = None,
    customer_q: Optional[str] = None,
):
    """List customers with credit balances for a shop (WALK IN included)."""
    ensure_shop_credit_payments_schema()
    where_parts = ["shop_id=%s", "sale_type='credit'"]
    params: list[Any] = [int(shop_id)]

    if analytics_filter:
        rw, rp = _analytics_where_clause(analytics_filter, "")
        where_parts.append(f"({rw})")
        params.extend(rp)

    cq = (customer_q or "").strip()
    if cq:
        like = f"%{cq}%"
        where_parts.append(
            "(COALESCE(NULLIF(customer_name, ''), 'WALK IN') LIKE %s OR COALESCE(NULLIF(customer_phone, ''), '-') LIKE %s)"
        )
        params.extend([like, like])

    where_sql = " AND ".join(where_parts)
    sql = f"""
    SELECT
        COALESCE(NULLIF(customer_name, ''), 'WALK IN') AS customer_name,
        COALESCE(NULLIF(customer_phone, ''), '-') AS customer_phone,
        COUNT(*) AS tx_count,
        COALESCE(SUM(total_amount), 0) AS credit_total,
        COALESCE(SUM(credit_paid_amount), 0) AS paid_total,
        COALESCE(SUM(GREATEST(total_amount - credit_paid_amount, 0)), 0) AS balance
    FROM shop_pos_sales
    WHERE {where_sql}
    GROUP BY customer_name, customer_phone
    HAVING balance > 0.0001
    ORDER BY balance DESC, tx_count DESC, customer_name ASC
    LIMIT %s
    """
    params.append(int(limit))
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
        return [
            {
                "customer_name": r.get("customer_name") or "WALK IN",
                "customer_phone": r.get("customer_phone") or "-",
                "tx_count": int(r.get("tx_count") or 0),
                "credit_total": float(r.get("credit_total") or 0),
                "paid_total": float(r.get("paid_total") or 0),
                "balance": float(r.get("balance") or 0),
            }
            for r in rows
        ]
    except pymysql.Error:
        return []


def get_shop_customer_credit_transactions(shop_id: int, customer_name: str, customer_phone: str, limit: int = 3000):
    """Credit transactions for one customer in a shop."""
    ensure_shop_credit_payments_schema()
    identity_where, identity_params = _customer_identity_where_sql(customer_name, customer_phone)
    sql = f"""
    SELECT
        id,
        sale_type,
        total_amount,
        credit_paid_amount,
        COALESCE(credit_status, 'not_paid') AS credit_status,
        item_count,
        COALESCE(NULLIF(employee_name, ''), 'Unknown') AS employee_name,
        employee_code,
        created_at
    FROM shop_pos_sales
    WHERE shop_id=%s
      AND sale_type='credit'
      AND {identity_where}
    ORDER BY created_at ASC, id ASC
    LIMIT %s
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (int(shop_id), *identity_params, int(limit)))
            rows = cur.fetchall() or []
        out = []
        for r in rows:
            total = float(r.get("total_amount") or 0)
            paid = float(r.get("credit_paid_amount") or 0)
            remaining = max(total - paid, 0.0)
            status = (r.get("credit_status") or "not_paid").strip().lower()
            if remaining <= 0.0001:
                status = "paid"
            elif paid > 0:
                status = "partially_paid"
            else:
                status = "not_paid"
            out.append(
                {
                    "id": int(r.get("id") or 0),
                    "total_amount": total,
                    "paid_amount": paid,
                    "remaining_amount": remaining,
                    "credit_status": status,
                    "item_count": int(r.get("item_count") or 0),
                    "employee_name": r.get("employee_name") or "Unknown",
                    "employee_code": (r.get("employee_code") or "").strip(),
                    "created_at": r.get("created_at"),
                }
            )
        return out
    except pymysql.Error:
        return []


def get_shop_customer_credit_payments(
    shop_id: int, customer_name: str, customer_phone: str, limit: int = 5000
):
    """Payment receipts recorded against a customer's credit at one shop."""
    ensure_shop_credit_payments_schema()
    identity_where, identity_params = _customer_identity_where_sql(customer_name, customer_phone)
    sql = f"""
    SELECT id, amount, note, created_at
    FROM shop_credit_payments
    WHERE shop_id=%s
      AND {identity_where}
    ORDER BY created_at ASC, id ASC
    LIMIT %s
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (int(shop_id), *identity_params, int(limit)))
            rows = cur.fetchall() or []
        return [
            {
                "id": int(r.get("id") or 0),
                "amount": float(r.get("amount") or 0),
                "note": (r.get("note") or "").strip(),
                "created_at": r.get("created_at"),
            }
            for r in rows
        ]
    except pymysql.Error:
        return []


def apply_shop_credit_payment_fifo(
    shop_id: int,
    customer_name: str,
    customer_phone: str,
    amount: float,
    note: Optional[str] = None,
):
    """Apply a payment FIFO across oldest outstanding credit sales."""
    ensure_shop_credit_payments_schema()
    try:
        pay_amt = float(amount)
    except Exception:
        return {"ok": False, "error": "Invalid amount."}
    if pay_amt <= 0:
        return {"ok": False, "error": "Amount must be greater than 0."}

    n = (customer_name or "").strip() or "WALK IN"
    p = normalize_customer_phone(customer_phone)

    identity_where, identity_params = _customer_identity_where_sql(n, p)
    select_sql = f"""
    SELECT id, total_amount, credit_paid_amount
    FROM shop_pos_sales
    WHERE shop_id=%s
      AND sale_type='credit'
      AND {identity_where}
      AND (total_amount - credit_paid_amount) > 0.0001
    ORDER BY created_at ASC, id ASC
    FOR UPDATE
    """
    update_sql = """
    UPDATE shop_pos_sales
    SET credit_paid_amount=%s,
        credit_status=%s
    WHERE id=%s AND shop_id=%s
    """
    insert_payment_sql = """
    INSERT INTO shop_credit_payments (shop_id, customer_name, customer_phone, amount, note)
    VALUES (%s, %s, %s, %s, %s)
    """
    allocated = []
    remaining_payment = pay_amt
    try:
        with get_cursor(commit=True) as cur:
            # record payment
            cur.execute(insert_payment_sql, (int(shop_id), n, p, pay_amt, (note or None)))

            cur.execute(select_sql, (int(shop_id), *identity_params))
            rows = cur.fetchall() or []
            for r in rows:
                if remaining_payment <= 0.0001:
                    break
                sale_id = int(r.get("id") or 0)
                total = float(r.get("total_amount") or 0)
                paid = float(r.get("credit_paid_amount") or 0)
                due = max(total - paid, 0.0)
                if due <= 0.0001:
                    continue
                apply_amt = min(due, remaining_payment)
                new_paid = paid + apply_amt
                new_due = max(total - new_paid, 0.0)
                if new_due <= 0.0001:
                    status = "paid"
                elif new_paid > 0:
                    status = "partially_paid"
                else:
                    status = "not_paid"
                cur.execute(update_sql, (new_paid, status, sale_id, int(shop_id)))
                allocated.append({"sale_id": sale_id, "applied": float(apply_amt)})
                remaining_payment -= apply_amt
    except pymysql.Error:
        return {"ok": False, "error": "Could not apply payment. Check database connection."}
    return {"ok": True, "allocated": allocated, "unused": float(max(remaining_payment, 0.0))}


def apply_company_credit_payment_fifo(
    customer_name: str,
    customer_phone: str,
    amount: float,
    payment_method: Optional[str] = None,
):
    """Apply one payment FIFO across oldest outstanding credit sales across all shops."""
    ensure_shop_credit_payments_schema()
    try:
        pay_amt = float(amount)
    except Exception:
        return {"ok": False, "error": "Invalid amount."}
    if pay_amt <= 0:
        return {"ok": False, "error": "Amount must be greater than 0."}

    n = (customer_name or "").strip() or "WALK IN"
    p = normalize_customer_phone(customer_phone)

    identity_where, identity_params = _customer_identity_where_sql(n, p, name_col="customer_name", phone_col="customer_phone")
    select_sql = f"""
    SELECT id, shop_id, total_amount, credit_paid_amount
    FROM shop_pos_sales
    WHERE sale_type='credit'
      AND {identity_where}
      AND (total_amount - credit_paid_amount) > 0.0001
    ORDER BY created_at ASC, id ASC
    FOR UPDATE
    """
    update_sql = """
    UPDATE shop_pos_sales
    SET credit_paid_amount=%s,
        credit_status=%s
    WHERE id=%s AND shop_id=%s
    """
    insert_payment_sql = """
    INSERT INTO shop_credit_payments (shop_id, customer_name, customer_phone, amount, note)
    VALUES (%s, %s, %s, %s, %s)
    """

    method = (payment_method or "").strip().lower()
    if method not in ("cash", "mpesa", "bank", "other"):
        method = "other"

    allocated = []
    remaining_payment = pay_amt
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(select_sql, tuple(identity_params))
            rows = cur.fetchall() or []
            for r in rows:
                if remaining_payment <= 0.0001:
                    break
                sale_id = int(r.get("id") or 0)
                shop_id = int(r.get("shop_id") or 0)
                total = float(r.get("total_amount") or 0)
                paid = float(r.get("credit_paid_amount") or 0)
                due = max(total - paid, 0.0)
                if due <= 0.0001:
                    continue

                apply_amt = min(due, remaining_payment)
                new_paid = paid + apply_amt
                new_due = max(total - new_paid, 0.0)
                if new_due <= 0.0001:
                    status = "paid"
                elif new_paid > 0:
                    status = "partially_paid"
                else:
                    status = "not_paid"

                cur.execute(update_sql, (new_paid, status, sale_id, shop_id))
                pay_note = f"[Company FIFO] method={method}"
                cur.execute(insert_payment_sql, (shop_id, n, p, apply_amt, pay_note))
                allocated.append({"shop_id": shop_id, "sale_id": sale_id, "applied": float(apply_amt)})
                remaining_payment -= apply_amt
    except pymysql.Error:
        return {"ok": False, "error": "Could not apply payment. Check database connection."}
    return {"ok": True, "allocated": allocated, "unused": float(max(remaining_payment, 0.0))}


def list_all_shops_credit_customers_with_balance(
    limit: int = 4000,
    analytics_filter: Optional[dict] = None,
    shop_id: Optional[int] = None,
    customer_q: Optional[str] = None,
):
    """All shops: customers with outstanding credit balances (aggregated over matching credit sales).

    When ``analytics_filter`` is set, only ``shop_pos_sales`` rows whose ``created_at`` falls in that
    range are included. When omitted, all credit sales are included (lifetime balances).
    """
    ensure_shop_credit_payments_schema()
    where_parts = ["sps.sale_type = 'credit'"]
    params: list[Any] = []

    if analytics_filter:
        rw, rp = _analytics_where_clause(analytics_filter, "sps")
        where_parts.append(f"({rw})")
        params.extend(rp)

    if shop_id is not None:
        where_parts.append("sps.shop_id = %s")
        params.append(int(shop_id))

    cq = (customer_q or "").strip()
    if cq:
        like = f"%{cq}%"
        where_parts.append(
            "(COALESCE(NULLIF(sps.customer_name, ''), 'WALK IN') LIKE %s OR COALESCE(NULLIF(sps.customer_phone, ''), '-') LIKE %s)"
        )
        params.extend([like, like])

    where_sql = " AND ".join(where_parts)

    sql = f"""
    SELECT
        sps.shop_id,
        sh.shop_name,
        sh.shop_code,
        COALESCE(NULLIF(sps.customer_name, ''), 'WALK IN') AS customer_name,
        COALESCE(NULLIF(sps.customer_phone, ''), '-') AS customer_phone,
        COUNT(*) AS tx_count,
        COALESCE(SUM(sps.total_amount), 0) AS credit_total,
        COALESCE(SUM(sps.credit_paid_amount), 0) AS paid_total,
        COALESCE(SUM(GREATEST(sps.total_amount - sps.credit_paid_amount, 0)), 0) AS balance
    FROM shop_pos_sales sps
    LEFT JOIN shops sh ON sh.id = sps.shop_id
    WHERE {where_sql}
    GROUP BY
        sps.shop_id,
        sh.shop_name,
        sh.shop_code,
        COALESCE(NULLIF(sps.customer_name, ''), 'WALK IN'),
        COALESCE(NULLIF(sps.customer_phone, ''), '-')
    HAVING balance > 0.0001
    ORDER BY balance DESC, tx_count DESC, sps.shop_id ASC, customer_name ASC
    LIMIT %s
    """
    params.append(int(limit))
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
        return [
            {
                "shop_id": int(r.get("shop_id") or 0),
                "shop_name": r.get("shop_name") or "Shop",
                "shop_code": r.get("shop_code") or "",
                "customer_name": r.get("customer_name") or "WALK IN",
                "customer_phone": r.get("customer_phone") or "-",
                "tx_count": int(r.get("tx_count") or 0),
                "credit_total": float(r.get("credit_total") or 0),
                "paid_total": float(r.get("paid_total") or 0),
                "balance": float(r.get("balance") or 0),
            }
            for r in rows
        ]
    except pymysql.Error:
        return []


def list_all_shops_credit_sales(
    limit: int = 5000,
    analytics_filter: Optional[dict] = None,
    analytics_scope: str = "general",
    shop_id: Optional[int] = None,
    customer_q: Optional[str] = None,
):
    """All shops: list credit sales (paid, partially paid, and unpaid)."""
    ensure_shop_credit_payments_schema()
    where_parts = ["sps.sale_type = 'credit'"]
    params: list[Any] = []

    if analytics_filter:
        rw, rp = _analytics_where_clause(analytics_filter, "sps")
        where_parts.append(f"({rw})")
        params.extend(rp)

    sw, sp = _analytics_receipt_scope_clause(analytics_scope, "sps")
    where_parts.append(f"({sw})")
    params.extend(sp)

    if shop_id is not None:
        where_parts.append("sps.shop_id = %s")
        params.append(int(shop_id))

    cq = (customer_q or "").strip()
    if cq:
        like = f"%{cq}%"
        where_parts.append(
            "(COALESCE(NULLIF(sps.customer_name, ''), 'WALK IN') LIKE %s OR COALESCE(NULLIF(sps.customer_phone, ''), '-') LIKE %s)"
        )
        params.extend([like, like])

    where_sql = " AND ".join(where_parts)
    sql = f"""
    SELECT
        sps.id,
        sps.shop_id,
        sh.shop_name,
        sh.shop_code,
        COALESCE(NULLIF(sps.customer_name, ''), 'WALK IN') AS customer_name,
        COALESCE(NULLIF(sps.customer_phone, ''), '-') AS customer_phone,
        sps.total_amount,
        sps.credit_paid_amount,
        COALESCE(sps.credit_status, 'not_paid') AS credit_status,
        sps.credit_due_date,
        sps.item_count,
        COALESCE(NULLIF(sps.employee_name, ''), 'Unknown') AS employee_name,
        COALESCE(NULLIF(sps.employee_code, ''), '-') AS employee_code,
        sps.created_at
    FROM shop_pos_sales sps
    LEFT JOIN shops sh ON sh.id = sps.shop_id
    WHERE {where_sql}
    ORDER BY sps.created_at DESC, sps.id DESC
    LIMIT %s
    """
    params.append(int(limit))
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
        out = []
        for r in rows:
            total = float(r.get("total_amount") or 0)
            paid = float(r.get("credit_paid_amount") or 0)
            remaining = max(total - paid, 0.0)
            status = (r.get("credit_status") or "not_paid").strip().lower()
            if remaining <= 0.0001:
                status = "paid"
            elif paid > 0:
                status = "partially_paid"
            else:
                status = "not_paid"
            out.append(
                {
                    "id": int(r.get("id") or 0),
                    "shop_id": int(r.get("shop_id") or 0),
                    "shop_name": r.get("shop_name") or "Shop",
                    "shop_code": r.get("shop_code") or "",
                    "customer_name": r.get("customer_name") or "WALK IN",
                    "customer_phone": r.get("customer_phone") or "-",
                    "total_amount": total,
                    "paid_amount": paid,
                    "remaining_amount": remaining,
                    "credit_status": status,
                    "credit_due_date": r.get("credit_due_date"),
                    "item_count": int(r.get("item_count") or 0),
                    "employee_name": r.get("employee_name") or "Unknown",
                    "employee_code": r.get("employee_code") or "-",
                    "created_at": r.get("created_at"),
                }
            )
        return out
    except pymysql.Error:
        return []


def list_all_shops_credit_sales_with_payments_audit(
    limit: int = 5000,
    analytics_filter: Optional[dict] = None,
    shop_id: Optional[int] = None,
    customer_q: Optional[str] = None,
    payment_scope: str = "all",
):
    """Credit sales where at least some payment was applied (credit_paid_amount > 0).

    payment_scope:
      - ``all``: any sale with a payment recorded toward it (partial or fully paid).
      - ``partial``: still outstanding but some amount paid.
      - ``paid``: fully settled (zero balance).

    Rows are ordered with partially paid first, then fully paid; within each group by newest sale.
    """
    ensure_shop_credit_payments_schema()
    scope = (payment_scope or "all").strip().lower()
    if scope not in ("all", "partial", "paid"):
        scope = "all"

    where_parts = [
        "sps.sale_type = 'credit'",
        "COALESCE(sps.credit_paid_amount, 0) > 0.0001",
    ]
    if scope == "partial":
        where_parts.append("(sps.total_amount - COALESCE(sps.credit_paid_amount, 0)) > 0.0001")
    elif scope == "paid":
        where_parts.append("(sps.total_amount - COALESCE(sps.credit_paid_amount, 0)) <= 0.0001")

    params: list[Any] = []

    if analytics_filter:
        rw, rp = _analytics_where_clause(analytics_filter, "sps")
        where_parts.append(f"({rw})")
        params.extend(rp)

    if shop_id is not None:
        where_parts.append("sps.shop_id = %s")
        params.append(int(shop_id))

    cq = (customer_q or "").strip()
    if cq:
        like = f"%{cq}%"
        where_parts.append(
            "(COALESCE(NULLIF(sps.customer_name, ''), 'WALK IN') LIKE %s OR COALESCE(NULLIF(sps.customer_phone, ''), '-') LIKE %s)"
        )
        params.extend([like, like])

    where_sql = " AND ".join(where_parts)
    sql = f"""
    SELECT
        sps.id,
        sps.shop_id,
        sh.shop_name,
        sh.shop_code,
        COALESCE(NULLIF(sps.customer_name, ''), 'WALK IN') AS customer_name,
        COALESCE(NULLIF(sps.customer_phone, ''), '-') AS customer_phone,
        sps.total_amount,
        sps.credit_paid_amount,
        COALESCE(sps.credit_status, 'not_paid') AS credit_status,
        sps.credit_due_date,
        sps.item_count,
        COALESCE(NULLIF(sps.employee_name, ''), 'Unknown') AS employee_name,
        COALESCE(NULLIF(sps.employee_code, ''), '-') AS employee_code,
        sps.created_at
    FROM shop_pos_sales sps
    LEFT JOIN shops sh ON sh.id = sps.shop_id
    WHERE {where_sql}
    ORDER BY
        CASE WHEN (sps.total_amount - COALESCE(sps.credit_paid_amount, 0)) > 0.0001 THEN 0 ELSE 1 END ASC,
        sps.created_at DESC,
        sps.id DESC
    LIMIT %s
    """
    params.append(int(limit))
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
        out = []
        for r in rows:
            total = float(r.get("total_amount") or 0)
            paid = float(r.get("credit_paid_amount") or 0)
            remaining = max(total - paid, 0.0)
            status = (r.get("credit_status") or "not_paid").strip().lower()
            if remaining <= 0.0001:
                status = "paid"
            elif paid > 0:
                status = "partially_paid"
            else:
                status = "not_paid"
            out.append(
                {
                    "id": int(r.get("id") or 0),
                    "shop_id": int(r.get("shop_id") or 0),
                    "shop_name": r.get("shop_name") or "Shop",
                    "shop_code": r.get("shop_code") or "",
                    "customer_name": r.get("customer_name") or "WALK IN",
                    "customer_phone": r.get("customer_phone") or "-",
                    "total_amount": total,
                    "paid_amount": paid,
                    "remaining_amount": remaining,
                    "credit_status": status,
                    "credit_due_date": r.get("credit_due_date"),
                    "item_count": int(r.get("item_count") or 0),
                    "employee_name": r.get("employee_name") or "Unknown",
                    "employee_code": r.get("employee_code") or "-",
                    "created_at": r.get("created_at"),
                }
            )
        return out
    except pymysql.Error:
        return []


def list_all_shops_company_credit_payment_receipts(
    limit: int = 5000,
    analytics_filter: Optional[dict] = None,
    shop_id: Optional[int] = None,
    customer_q: Optional[str] = None,
):
    """All shops: rows from shop_credit_payments (inbound payments toward credit balances)."""
    ensure_shop_credit_payments_schema()
    where_parts: list[str] = ["1=1"]
    params: list[Any] = []

    if analytics_filter:
        rw, rp = _analytics_where_clause(analytics_filter, "scp")
        where_parts.append(f"({rw})")
        params.extend(rp)

    if shop_id is not None:
        where_parts.append("scp.shop_id = %s")
        params.append(int(shop_id))

    cq = (customer_q or "").strip()
    if cq:
        like = f"%{cq}%"
        where_parts.append(
            "(COALESCE(NULLIF(scp.customer_name, ''), 'WALK IN') LIKE %s OR COALESCE(NULLIF(scp.customer_phone, ''), '-') LIKE %s)"
        )
        params.extend([like, like])

    where_sql = " AND ".join(where_parts)
    sql = f"""
    SELECT
        scp.id,
        scp.shop_id,
        sh.shop_name,
        sh.shop_code,
        COALESCE(NULLIF(scp.customer_name, ''), 'WALK IN') AS customer_name,
        COALESCE(NULLIF(scp.customer_phone, ''), '-') AS customer_phone,
        scp.amount,
        scp.note,
        scp.created_at
    FROM shop_credit_payments scp
    LEFT JOIN shops sh ON sh.id = scp.shop_id
    WHERE {where_sql}
    ORDER BY scp.created_at DESC, scp.id DESC
    LIMIT %s
    """
    params.append(int(limit))
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
        out = []
        for r in rows:
            raw_note = (r.get("note") or "").strip()
            note_lower = raw_note.lower()
            method = "other"
            marker = "method="
            if marker in note_lower:
                parsed = note_lower.split(marker, 1)[1].split()[0].strip(" ,.;:|")
                if parsed in ("cash", "mpesa", "bank", "other"):
                    method = parsed
            elif "mpesa" in note_lower:
                method = "mpesa"
            elif "cash" in note_lower:
                method = "cash"
            elif "bank" in note_lower:
                method = "bank"
            out.append(
                {
                    "id": int(r.get("id") or 0),
                    "shop_id": int(r.get("shop_id") or 0),
                    "shop_name": r.get("shop_name") or "Shop",
                    "shop_code": r.get("shop_code") or "",
                    "customer_name": r.get("customer_name") or "WALK IN",
                    "customer_phone": r.get("customer_phone") or "-",
                    "amount": float(r.get("amount") or 0),
                    "note": raw_note,
                    "payment_method": method,
                    "created_at": r.get("created_at"),
                }
            )
        return out
    except pymysql.Error:
        return []


def list_all_shops_credit_due_reminders(
    limit: int = 3000,
    days_ahead: int = 90,
    shop_id: Optional[int] = None,
    customer_q: Optional[str] = None,
):
    """Outstanding credit sales with a due date on or before today + days_ahead (includes overdue)."""
    ensure_shop_credit_payments_schema()
    try:
        horizon = max(1, min(int(days_ahead), 730))
    except Exception:
        horizon = 90

    where_parts = [
        "sps.sale_type = 'credit'",
        "sps.credit_due_date IS NOT NULL",
        "(sps.total_amount - COALESCE(sps.credit_paid_amount, 0)) > 0.0001",
        "sps.credit_due_date <= DATE_ADD(CURDATE(), INTERVAL %s DAY)",
    ]
    params: list[Any] = [horizon]

    if shop_id is not None:
        where_parts.append("sps.shop_id = %s")
        params.append(int(shop_id))

    cq = (customer_q or "").strip()
    if cq:
        like = f"%{cq}%"
        where_parts.append(
            "(COALESCE(NULLIF(sps.customer_name, ''), 'WALK IN') LIKE %s OR COALESCE(NULLIF(sps.customer_phone, ''), '-') LIKE %s)"
        )
        params.extend([like, like])

    where_sql = " AND ".join(where_parts)
    sql = f"""
    SELECT
        sps.id,
        sps.shop_id,
        sh.shop_name,
        sh.shop_code,
        COALESCE(NULLIF(sps.customer_name, ''), 'WALK IN') AS customer_name,
        COALESCE(NULLIF(sps.customer_phone, ''), '-') AS customer_phone,
        sps.total_amount,
        sps.credit_paid_amount,
        sps.credit_due_date,
        sps.item_count,
        sps.created_at
    FROM shop_pos_sales sps
    LEFT JOIN shops sh ON sh.id = sps.shop_id
    WHERE {where_sql}
    ORDER BY sps.credit_due_date ASC, sps.shop_id ASC, sps.id ASC
    LIMIT %s
    """
    params.append(int(limit))
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
        today = date.today()
        out = []
        for r in rows:
            total = float(r.get("total_amount") or 0)
            paid = float(r.get("credit_paid_amount") or 0)
            remaining = max(total - paid, 0.0)
            due = r.get("credit_due_date")
            overdue = False
            if due is not None:
                try:
                    if isinstance(due, datetime):
                        dcmp = due.date()
                    elif isinstance(due, date):
                        dcmp = due
                    else:
                        dcmp = date.fromisoformat(str(due)[:10])
                    overdue = dcmp < today
                except Exception:
                    overdue = False
            out.append(
                {
                    "id": int(r.get("id") or 0),
                    "shop_id": int(r.get("shop_id") or 0),
                    "shop_name": r.get("shop_name") or "Shop",
                    "shop_code": r.get("shop_code") or "",
                    "customer_name": r.get("customer_name") or "WALK IN",
                    "customer_phone": r.get("customer_phone") or "-",
                    "total_amount": total,
                    "paid_amount": paid,
                    "remaining_amount": remaining,
                    "credit_due_date": due,
                    "overdue": overdue,
                    "item_count": int(r.get("item_count") or 0),
                    "created_at": r.get("created_at"),
                }
            )
        return out
    except pymysql.Error:
        return []


def list_company_credit_customers(
    limit: int = 5000,
    analytics_filter: Optional[dict] = None,
    shop_id: Optional[int] = None,
    customer_q: Optional[str] = None,
):
    """Company-level credit view: aggregate all matching credit sales by customer."""
    ensure_shop_credit_payments_schema()
    where_parts = ["sps.sale_type = 'credit'"]
    params: list[Any] = []

    if analytics_filter:
        rw, rp = _analytics_where_clause(analytics_filter, "sps")
        where_parts.append(f"({rw})")
        params.extend(rp)

    if shop_id is not None:
        where_parts.append("sps.shop_id = %s")
        params.append(int(shop_id))

    cq = (customer_q or "").strip()
    if cq:
        like = f"%{cq}%"
        where_parts.append(
            "(COALESCE(NULLIF(sps.customer_name, ''), 'WALK IN') LIKE %s OR COALESCE(NULLIF(sps.customer_phone, ''), '-') LIKE %s)"
        )
        params.extend([like, like])

    where_sql = " AND ".join(where_parts)
    sql = f"""
    SELECT
        COALESCE(NULLIF(sps.customer_name, ''), 'WALK IN') AS customer_name,
        COALESCE(NULLIF(sps.customer_phone, ''), '-') AS customer_phone,
        COUNT(*) AS tx_count,
        COALESCE(SUM(sps.total_amount), 0) AS credit_total,
        COALESCE(SUM(sps.credit_paid_amount), 0) AS paid_total,
        COALESCE(SUM(GREATEST(sps.total_amount - sps.credit_paid_amount, 0)), 0) AS balance,
        MAX(sps.created_at) AS last_credit_at
    FROM shop_pos_sales sps
    WHERE {where_sql}
    GROUP BY
        COALESCE(NULLIF(sps.customer_name, ''), 'WALK IN'),
        COALESCE(NULLIF(sps.customer_phone, ''), '-')
    ORDER BY balance DESC, credit_total DESC, customer_name ASC
    LIMIT %s
    """
    params.append(int(limit))
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
        return [
            {
                "customer_name": r.get("customer_name") or "WALK IN",
                "customer_phone": r.get("customer_phone") or "-",
                "tx_count": int(r.get("tx_count") or 0),
                "total_amount": float(r.get("credit_total") or 0),
                "paid_amount": float(r.get("paid_total") or 0),
                "remaining_amount": float(r.get("balance") or 0),
                "created_at": r.get("last_credit_at"),
            }
            for r in rows
        ]
    except pymysql.Error:
        return []


def get_company_customer_credit_transactions(customer_name: str, customer_phone: str, limit: int = 5000):
    """Company-level credit transactions for one customer across all shops."""
    ensure_shop_credit_payments_schema()
    n = (customer_name or "").strip() or "WALK IN"
    p = (customer_phone or "").strip() or "-"
    sql = """
    SELECT
        sps.id,
        sps.shop_id,
        sh.shop_name,
        sh.shop_code,
        sps.total_amount,
        sps.credit_paid_amount,
        COALESCE(sps.credit_status, 'not_paid') AS credit_status,
        sps.item_count,
        COALESCE(NULLIF(sps.employee_name, ''), 'Unknown') AS employee_name,
        sps.employee_code,
        sps.created_at
    FROM shop_pos_sales sps
    LEFT JOIN shops sh ON sh.id = sps.shop_id
    WHERE sps.sale_type='credit'
      AND COALESCE(NULLIF(sps.customer_name, ''), 'WALK IN')=%s
      AND COALESCE(NULLIF(sps.customer_phone, ''), '-')=%s
    ORDER BY sps.created_at DESC, sps.id DESC
    LIMIT %s
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (n, p, int(limit)))
            rows = cur.fetchall() or []
        out = []
        for r in rows:
            total = float(r.get("total_amount") or 0)
            paid = float(r.get("credit_paid_amount") or 0)
            remaining = max(total - paid, 0.0)
            status = (r.get("credit_status") or "not_paid").strip().lower()
            if remaining <= 0.0001:
                status = "paid"
            elif paid > 0:
                status = "partially_paid"
            else:
                status = "not_paid"
            out.append(
                {
                    "id": int(r.get("id") or 0),
                    "shop_id": int(r.get("shop_id") or 0),
                    "shop_name": r.get("shop_name") or "Shop",
                    "shop_code": r.get("shop_code") or "",
                    "total_amount": total,
                    "paid_amount": paid,
                    "remaining_amount": remaining,
                    "credit_status": status,
                    "item_count": int(r.get("item_count") or 0),
                    "employee_name": r.get("employee_name") or "Unknown",
                    "employee_code": (r.get("employee_code") or "").strip(),
                    "created_at": r.get("created_at"),
                }
            )
        return out
    except pymysql.Error:
        return []


def get_company_customer_credit_payments(customer_name: str, customer_phone: str, limit: int = 5000):
    """Company-level payment transactions for one customer across all shops."""
    ensure_shop_credit_payments_schema()
    n = (customer_name or "").strip() or "WALK IN"
    p = (customer_phone or "").strip() or "-"
    sql = """
    SELECT
        scp.id,
        scp.shop_id,
        sh.shop_name,
        sh.shop_code,
        scp.amount,
        scp.note,
        scp.created_at
    FROM shop_credit_payments scp
    LEFT JOIN shops sh ON sh.id = scp.shop_id
    WHERE COALESCE(NULLIF(scp.customer_name, ''), 'WALK IN')=%s
      AND COALESCE(NULLIF(scp.customer_phone, ''), '-')=%s
    ORDER BY scp.created_at DESC, scp.id DESC
    LIMIT %s
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (n, p, int(limit)))
            rows = cur.fetchall() or []
        out = []
        for r in rows:
            raw_note = (r.get("note") or "").strip()
            note_lower = raw_note.lower()
            method = "other"
            marker = "method="
            if marker in note_lower:
                parsed = note_lower.split(marker, 1)[1].split()[0].strip(" ,.;:|")
                if parsed in ("cash", "mpesa", "bank", "other"):
                    method = parsed
            elif "mpesa" in note_lower:
                method = "mpesa"
            elif "cash" in note_lower:
                method = "cash"
            elif "bank" in note_lower:
                method = "bank"
            out.append(
                {
                    "id": int(r.get("id") or 0),
                    "shop_id": int(r.get("shop_id") or 0),
                    "shop_name": r.get("shop_name") or "Shop",
                    "shop_code": r.get("shop_code") or "",
                    "amount": float(r.get("amount") or 0),
                    "payment_method": method,
                    "note": raw_note,
                    "created_at": r.get("created_at"),
                }
            )
        return out
    except pymysql.Error:
        return []


def get_company_customer_credit_items(customer_name: str, customer_phone: str, limit: int = 20000):
    """All item lines bought on customer credit sales across all shops."""
    ensure_shop_credit_payments_schema()
    n = (customer_name or "").strip() or "WALK IN"
    p = (customer_phone or "").strip() or "-"
    sql = """
    SELECT
        sps.id AS sale_id,
        sps.shop_id,
        sh.shop_name,
        sh.shop_code,
        sps.created_at,
        spi.item_id,
        spi.item_name,
        spi.qty,
        spi.unit_price,
        spi.line_total
    FROM shop_pos_sales sps
    JOIN shop_pos_sale_items spi ON spi.sale_id = sps.id AND spi.shop_id = sps.shop_id
    LEFT JOIN shops sh ON sh.id = sps.shop_id
    WHERE sps.sale_type='credit'
      AND COALESCE(NULLIF(sps.customer_name, ''), 'WALK IN')=%s
      AND COALESCE(NULLIF(sps.customer_phone, ''), '-')=%s
    ORDER BY sps.created_at DESC, sps.id DESC, spi.id ASC
    LIMIT %s
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (n, p, int(limit)))
            rows = cur.fetchall() or []
        return [
            {
                "sale_id": int(r.get("sale_id") or 0),
                "shop_id": int(r.get("shop_id") or 0),
                "shop_name": r.get("shop_name") or "Shop",
                "shop_code": r.get("shop_code") or "",
                "created_at": r.get("created_at"),
                "item_id": r.get("item_id"),
                "item_name": r.get("item_name") or "-",
                "qty": int(r.get("qty") or 0),
                "unit_price": float(r.get("unit_price") or 0),
                "line_total": float(r.get("line_total") or 0),
            }
            for r in rows
        ]
    except pymysql.Error:
        return []


def get_shop_credit_sale_detail(shop_id: int, sale_id: int):
    """Return one credit sale (header + items) for a shop."""
    ensure_shop_credit_payments_schema()
    sale_sql = """
    SELECT
        sps.id,
        sps.shop_id,
        sh.shop_name,
        sh.shop_code,
        sps.sale_type,
        sps.total_amount,
        sps.credit_paid_amount,
        COALESCE(sps.credit_status, 'not_paid') AS credit_status,
        sps.item_count,
        COALESCE(NULLIF(sps.customer_name, ''), 'WALK IN') AS customer_name,
        COALESCE(NULLIF(sps.customer_phone, ''), '-') AS customer_phone,
        COALESCE(NULLIF(sps.employee_name, ''), 'Unknown') AS employee_name,
        COALESCE(NULLIF(sps.employee_code, ''), '-') AS employee_code,
        sps.created_at
    FROM shop_pos_sales sps
    LEFT JOIN shops sh ON sh.id = sps.shop_id
    WHERE sps.shop_id=%s AND sps.id=%s AND sps.sale_type='credit'
    LIMIT 1
    """
    items_sql = """
    SELECT
        item_id,
        item_name,
        qty,
        unit_price,
        line_total
    FROM shop_pos_sale_items
    WHERE shop_id=%s AND sale_id=%s
    ORDER BY id ASC
    """
    try:
        with get_cursor() as cur:
            cur.execute(sale_sql, (int(shop_id), int(sale_id)))
            sale = cur.fetchone()
            if not sale:
                return None
            cur.execute(items_sql, (int(shop_id), int(sale_id)))
            items = cur.fetchall() or []
        total = float(sale.get("total_amount") or 0)
        paid = float(sale.get("credit_paid_amount") or 0)
        remaining = max(total - paid, 0.0)
        status = (sale.get("credit_status") or "not_paid").strip().lower()
        if remaining <= 0.0001:
            status = "paid"
        elif paid > 0:
            status = "partially_paid"
        else:
            status = "not_paid"
        return {
            "sale": {
                "id": int(sale.get("id") or 0),
                "shop_id": int(sale.get("shop_id") or 0),
                "shop_name": sale.get("shop_name") or "Shop",
                "shop_code": sale.get("shop_code") or "",
                "total_amount": total,
                "paid_amount": paid,
                "remaining_amount": remaining,
                "credit_status": status,
                "item_count": int(sale.get("item_count") or 0),
                "customer_name": sale.get("customer_name") or "WALK IN",
                "customer_phone": sale.get("customer_phone") or "-",
                "employee_name": sale.get("employee_name") or "Unknown",
                "employee_code": sale.get("employee_code") or "-",
                "created_at": sale.get("created_at"),
            },
            "items": [
                {
                    "item_id": r.get("item_id"),
                    "item_name": r.get("item_name") or "Item",
                    "qty": int(r.get("qty") or 0),
                    "unit_price": float(r.get("unit_price") or 0),
                    "line_total": float(r.get("line_total") or 0),
                }
                for r in items
            ],
        }
    except pymysql.Error:
        return None


def get_shop_pos_sale_detail(shop_id: int, sale_id: int):
    """Return one POS sale (sale/credit) with line items for receipt popup."""
    ensure_shop_pos_sales_receipt_columns()
    ensure_shop_pos_sales_mpesa_receipt_column()
    sale_sql = """
    SELECT
        sps.id,
        sps.shop_id,
        sh.shop_name,
        sh.shop_code,
        sps.sale_type,
        sps.payment_method,
        sps.total_amount,
        sps.cash_amount,
        sps.mpesa_amount,
        sps.mpesa_receipt_number,
        sps.credit_paid_amount,
        COALESCE(sps.credit_status, 'not_paid') AS credit_status,
        sps.item_count,
        COALESCE(NULLIF(sps.customer_name, ''), 'WALK IN') AS customer_name,
        COALESCE(NULLIF(sps.customer_phone, ''), '-') AS customer_phone,
        COALESCE(NULLIF(sps.employee_name, ''), 'Unknown') AS employee_name,
        COALESCE(NULLIF(sps.employee_code, ''), '-') AS employee_code,
        COALESCE(NULLIF(sps.receipt_number, ''), CONCAT('#', sps.id)) AS receipt_number,
        COALESCE(sps.receipt_mark_status, 'pending') AS receipt_mark_status,
        sps.created_at
    FROM shop_pos_sales sps
    LEFT JOIN shops sh ON sh.id = sps.shop_id
    WHERE sps.shop_id=%s AND sps.id=%s
    LIMIT 1
    """
    items_sql = """
    SELECT
        id AS line_id,
        item_id,
        item_name,
        qty,
        unit_price,
        line_total
    FROM shop_pos_sale_items
    WHERE shop_id=%s AND sale_id=%s
    ORDER BY id ASC
    """
    try:
        with get_cursor() as cur:
            cur.execute(sale_sql, (int(shop_id), int(sale_id)))
            sale = cur.fetchone()
            if not sale:
                return None
            cur.execute(items_sql, (int(shop_id), int(sale_id)))
            items = cur.fetchall() or []
        return {
            "sale": {
                "id": int(sale.get("id") or 0),
                "shop_id": int(sale.get("shop_id") or 0),
                "shop_name": sale.get("shop_name") or "Shop",
                "shop_code": sale.get("shop_code") or "",
                "sale_type": (sale.get("sale_type") or "sale").strip().lower(),
                "payment_method": sale.get("payment_method") or "",
                "total_amount": float(sale.get("total_amount") or 0),
                "cash_amount": float(sale.get("cash_amount") or 0),
                "mpesa_amount": float(sale.get("mpesa_amount") or 0),
                "mpesa_receipt_number": (sale.get("mpesa_receipt_number") or "").strip(),
                "credit_paid_amount": float(sale.get("credit_paid_amount") or 0),
                "credit_status": (sale.get("credit_status") or "not_paid").strip().lower(),
                "item_count": int(sale.get("item_count") or 0),
                "customer_name": sale.get("customer_name") or "WALK IN",
                "customer_phone": sale.get("customer_phone") or "-",
                "employee_name": sale.get("employee_name") or "Unknown",
                "employee_code": sale.get("employee_code") or "-",
                "receipt_number": sale.get("receipt_number") or f"#{int(sale.get('id') or 0)}",
                "receipt_mark_status": (sale.get("receipt_mark_status") or "pending").strip().lower(),
                "created_at": sale.get("created_at"),
            },
            "items": [
                {
                    "line_id": int(r.get("line_id") or 0),
                    "item_id": r.get("item_id"),
                    "item_name": r.get("item_name") or "Item",
                    "qty": round(float(r.get("qty") or 0), STOCK_QTY_DECIMAL_PLACES),
                    "unit_price": float(r.get("unit_price") or 0),
                    "line_total": float(r.get("line_total") or 0),
                }
                for r in items
            ],
        }
    except pymysql.Error:
        return None


def _normalize_pos_sale_line_returns(
    line_ids: list | None,
    line_returns: list | None,
) -> dict[int, Optional[float]]:
    """Map sale line id -> qty to return (None = full remaining qty on that line)."""
    out: dict[int, Optional[float]] = {}
    if isinstance(line_returns, list):
        for raw in line_returns:
            if not isinstance(raw, dict):
                continue
            try:
                lid = int(raw.get("line_id") or raw.get("id") or 0)
            except (TypeError, ValueError):
                continue
            if lid <= 0 or lid in out:
                continue
            rqty = normalize_stock_move_qty(raw.get("qty")) if raw.get("qty") is not None else None
            if raw.get("qty") is not None and rqty is None:
                continue
            out[lid] = rqty
            if len(out) >= 500:
                break
    elif isinstance(line_ids, list):
        for raw in line_ids:
            try:
                lid = int(raw)
            except (TypeError, ValueError):
                continue
            if lid <= 0 or lid in out:
                continue
            out[lid] = None
            if len(out) >= 500:
                break
    return out


def return_shop_pos_sale_lines(
    *,
    shop_id: int,
    sale_id: int,
    line_ids: list[int] | None = None,
    line_returns: list[dict] | None = None,
) -> tuple[bool, Optional[str], dict]:
    """Return selected receipt lines (full or partial qty per selected line)."""
    ensure_shop_pos_sales_receipt_columns()
    return_map = _normalize_pos_sale_line_returns(line_ids, line_returns)
    if not return_map:
        return False, "Select at least one item to return.", {}

    try:
        with get_cursor(commit=True) as cur:
            cur.execute(
                """
                SELECT id, sale_type, total_amount, item_count, credit_paid_amount, COALESCE(inventory_mode, 'shop') AS inventory_mode
                FROM shop_pos_sales
                WHERE id=%s AND shop_id=%s
                LIMIT 1
                FOR UPDATE
                """,
                (int(sale_id), int(shop_id)),
            )
            sale = cur.fetchone()
            if not sale:
                return False, "Receipt not found.", {}
            mode = (sale.get("inventory_mode") or "shop").strip().lower()
            if mode in ("kitchen", "both"):
                ensure_shop_kitchen_portions_schema()

            refunded_amount = 0.0
            returned_qty = 0.0
            returned_lines = 0

            for lid, requested_qty in return_map.items():
                cur.execute(
                    """
                    SELECT id, item_id, qty, unit_price, line_total
                    FROM shop_pos_sale_items
                    WHERE id=%s AND sale_id=%s AND shop_id=%s
                    LIMIT 1
                    FOR UPDATE
                    """,
                    (int(lid), int(sale_id), int(shop_id)),
                )
                ln = cur.fetchone() or {}
                qty = round(float(ln.get("qty") or 0), STOCK_QTY_DECIMAL_PLACES)
                if qty <= 0:
                    continue
                if requested_qty is not None:
                    return_qty = round(min(requested_qty, qty), STOCK_QTY_DECIMAL_PLACES)
                    if return_qty <= 0:
                        continue
                else:
                    return_qty = qty
                item_id = ln.get("item_id")
                try:
                    item_id = int(item_id) if item_id is not None else None
                except (TypeError, ValueError):
                    item_id = None
                line_total = float(ln.get("line_total") or 0)
                unit_price = float(ln.get("unit_price") or 0)
                if return_qty + 1e-9 >= qty:
                    refund = max(0.0, line_total)
                    new_qty = 0.0
                    new_line_total = 0.0
                else:
                    refund = round(line_total * return_qty / qty, 2) if qty > 0 else round(unit_price * return_qty, 2)
                    new_qty = round(qty - return_qty, STOCK_QTY_DECIMAL_PLACES)
                    new_line_total = max(0.0, round(line_total - refund, 2))

                cur.execute(
                    """
                    UPDATE shop_pos_sale_items
                    SET qty=%s, line_total=%s
                    WHERE id=%s AND sale_id=%s AND shop_id=%s
                    """,
                    (new_qty, new_line_total, int(lid), int(sale_id), int(shop_id)),
                )
                refunded_amount += max(0.0, refund)
                returned_qty += return_qty
                returned_lines += 1

                if item_id is None:
                    continue
                cur.execute(
                    """
                    SELECT COALESCE(stock_update_enabled,0) AS sue
                    FROM shop_items
                    WHERE shop_id=%s AND item_id=%s
                    FOR UPDATE
                    """,
                    (int(shop_id), int(item_id)),
                )
                si = cur.fetchone() or {}
                if int(si.get("sue") or 0) != 1:
                    continue
                stock_qty = return_qty
                if mode == "shop":
                    cur.execute(
                        """
                        UPDATE shop_items
                        SET shop_stock_qty = COALESCE(shop_stock_qty,0) + %s
                        WHERE shop_id=%s AND item_id=%s
                        """,
                        (stock_qty, int(shop_id), int(item_id)),
                    )
                elif mode in ("kitchen", "both"):
                    cur.execute(
                        """
                        INSERT INTO shop_kitchen_portions (shop_id, item_id, portions_remaining)
                        VALUES (%s, %s, %s)
                        ON DUPLICATE KEY UPDATE portions_remaining = portions_remaining + VALUES(portions_remaining)
                        """,
                        (int(shop_id), int(item_id), int(stock_qty)),
                    )

            if returned_lines <= 0:
                return False, "Selected items are already returned.", {}

            prev_total = float(sale.get("total_amount") or 0)
            prev_count = round(float(sale.get("item_count") or 0), STOCK_QTY_DECIMAL_PLACES)
            new_total = max(0.0, round(prev_total - refunded_amount, 2))
            new_count = max(0.0, round(prev_count - returned_qty, STOCK_QTY_DECIMAL_PLACES))
            cur.execute(
                """
                UPDATE shop_pos_sales
                SET total_amount=%s, item_count=%s
                WHERE id=%s AND shop_id=%s
                """,
                (new_total, new_count, int(sale_id), int(shop_id)),
            )

            if (sale.get("sale_type") or "").strip().lower() == "credit":
                paid = float(sale.get("credit_paid_amount") or 0)
                if paid > new_total:
                    paid = new_total
                if new_total <= 0.0001:
                    cstat = "paid"
                elif paid <= 0.0001:
                    cstat = "not_paid"
                elif paid + 0.0001 >= new_total:
                    cstat = "paid"
                else:
                    cstat = "partially_paid"
                cur.execute(
                    """
                    UPDATE shop_pos_sales
                    SET credit_paid_amount=%s, credit_status=%s
                    WHERE id=%s AND shop_id=%s
                    """,
                    (round(paid, 2), cstat, int(sale_id), int(shop_id)),
                )

            cur.execute(
                """
                SELECT COUNT(*) AS remaining_lines
                FROM shop_pos_sale_items
                WHERE sale_id=%s AND shop_id=%s AND qty > 0
                """,
                (int(sale_id), int(shop_id)),
            )
            rem = int((cur.fetchone() or {}).get("remaining_lines") or 0)
            new_mark = "returned" if rem <= 0 else "partial_return"
            cur.execute(
                """
                UPDATE shop_pos_sales
                SET receipt_mark_status=%s
                WHERE id=%s AND shop_id=%s
                """,
                (new_mark, int(sale_id), int(shop_id)),
            )

            return True, None, {
                "returned_lines": returned_lines,
                "returned_qty": returned_qty,
                "refunded_amount": round(refunded_amount, 2),
                "new_total_amount": new_total,
                "new_mark_status": new_mark,
            }
    except pymysql.Error:
        return False, None, {}


_ENSURED_POS_SALE_ITEMS_PORTION_COST = False


def ensure_shop_pos_sale_items_portion_unit_cost_column() -> bool:
    """Snapshot est. production price on kitchen/both sale lines for accurate period COGS."""
    global _ENSURED_POS_SALE_ITEMS_PORTION_COST
    if _ENSURED_POS_SALE_ITEMS_PORTION_COST:
        return True
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS shop_pos_sale_items (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    sale_id BIGINT NOT NULL,
                    shop_id INT NOT NULL,
                    item_id INT NULL,
                    item_name VARCHAR(200) NOT NULL,
                    qty INT NOT NULL DEFAULT 0,
                    unit_price DECIMAL(12,2) NOT NULL DEFAULT 0.00,
                    line_total DECIMAL(12,2) NOT NULL DEFAULT 0.00,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    KEY idx_shop_pos_sale_items_sale (sale_id),
                    KEY idx_shop_pos_sale_items_shop_created (shop_id, created_at),
                    KEY idx_shop_pos_sale_items_item (item_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            if not column_exists("shop_pos_sale_items", "portion_unit_cost"):
                cur.execute(
                    """
                    ALTER TABLE shop_pos_sale_items
                    ADD COLUMN portion_unit_cost DECIMAL(14,2) NULL DEFAULT NULL
                    COMMENT 'Est. production price at checkout (kitchen/both)'
                    AFTER line_total
                    """
                )
        _ENSURED_POS_SALE_ITEMS_PORTION_COST = True
        return True
    except pymysql.Error as e:
        logger.warning("Could not add shop_pos_sale_items.portion_unit_cost: %s", e)
        return False


def init_shop_pos_sale_items_table():
    """Persist POS checkout line items for item analytics."""
    sql = """
    CREATE TABLE IF NOT EXISTS shop_pos_sale_items (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        sale_id BIGINT NOT NULL,
        shop_id INT NOT NULL,
        item_id INT NULL,
        item_name VARCHAR(200) NOT NULL,
        qty INT NOT NULL DEFAULT 0,
        unit_price DECIMAL(12,2) NOT NULL DEFAULT 0.00,
        line_total DECIMAL(12,2) NOT NULL DEFAULT 0.00,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        KEY idx_shop_pos_sale_items_sale (sale_id),
        KEY idx_shop_pos_sale_items_shop_created (shop_id, created_at),
        KEY idx_shop_pos_sale_items_item (item_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql)
        logger.info("Table shop_pos_sale_items is ready.")
        return True
    except pymysql.Error as e:
        logger.warning("Could not init shop_pos_sale_items: %s", e)
        return False


def init_shop_pos_quotations_table():
    """POS quotations (leads): printed like a receipt but no sale row or stock movement."""
    sql = """
    CREATE TABLE IF NOT EXISTS shop_pos_quotations (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        shop_id INT NULL,
        quote_basis ENUM('sale', 'credit') NOT NULL DEFAULT 'sale',
        quote_channel ENUM('walkin', 'online') NOT NULL DEFAULT 'walkin',
        customer_name VARCHAR(190) NULL,
        customer_phone VARCHAR(40) NULL,
        total_amount DECIMAL(12,2) NOT NULL DEFAULT 0.00,
        item_count INT NOT NULL DEFAULT 0,
        lines_json LONGTEXT NOT NULL,
        employee_id INT NULL,
        employee_code CHAR(6) NULL,
        employee_name VARCHAR(190) NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        KEY idx_shop_pos_q_shop_created (shop_id, created_at),
        KEY idx_shop_pos_q_customer (shop_id, customer_phone, customer_name),
        KEY idx_shop_pos_q_channel_created (quote_channel, created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql)
            if table_exists("shop_pos_quotations"):
                if not column_exists("shop_pos_quotations", "quote_channel"):
                    try:
                        cur.execute(
                            """
                            ALTER TABLE shop_pos_quotations
                            ADD COLUMN quote_channel ENUM('walkin', 'online') NOT NULL DEFAULT 'walkin'
                            AFTER quote_basis
                            """
                        )
                        logger.info("Added shop_pos_quotations.quote_channel.")
                    except pymysql.Error as e:
                        logger.warning("Could not add quote_channel: %s", e)
                try:
                    cur.execute(
                        "ALTER TABLE shop_pos_quotations MODIFY shop_id INT NULL"
                    )
                except pymysql.Error as e:
                    logger.warning("Could not relax shop_pos_quotations.shop_id: %s", e)
                if not column_exists("shop_pos_quotations", "customer_notes"):
                    try:
                        cur.execute(
                            """
                            ALTER TABLE shop_pos_quotations
                            ADD COLUMN customer_notes VARCHAR(500) NULL
                            AFTER customer_phone
                            """
                        )
                        logger.info("Added shop_pos_quotations.customer_notes.")
                    except pymysql.Error as e:
                        logger.warning("Could not add customer_notes: %s", e)
        logger.info("Table shop_pos_quotations is ready.")
        return True
    except pymysql.Error as e:
        logger.warning("Could not init shop_pos_quotations: %s", e)
        return False


def init_pos_held_orders_table() -> bool:
    """Withhold-POS held orders (running tabs): cart snapshot + committed qty map for incremental stock deduction."""
    sql = """
    CREATE TABLE IF NOT EXISTS pos_held_orders (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        shop_id INT NOT NULL,
        created_by_employee_id INT NULL,
        employee_code CHAR(6) NULL,
        employee_name VARCHAR(190) NULL,
        customer_name VARCHAR(190) NULL,
        customer_phone VARCHAR(40) NULL,
        label VARCHAR(120) NOT NULL DEFAULT '',
        status ENUM('open', 'finalized', 'voided') NOT NULL DEFAULT 'open',
        inventory_mode VARCHAR(16) NOT NULL DEFAULT 'shop',
        cart_json LONGTEXT NOT NULL,
        committed_json LONGTEXT NOT NULL,
        total_amount DECIMAL(12,2) NOT NULL DEFAULT 0.00,
        item_count DECIMAL(12,4) NOT NULL DEFAULT 0,
        saves_count INT NOT NULL DEFAULT 0,
        last_save_at DATETIME NULL,
        finalized_sale_id BIGINT NULL,
        finalized_at DATETIME NULL,
        voided_at DATETIME NULL,
        voided_by_employee_id INT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        KEY idx_pho_shop_status (shop_id, status),
        KEY idx_pho_emp (created_by_employee_id),
        KEY idx_pho_shop_last_save (shop_id, last_save_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql)
        logger.info("Table pos_held_orders is ready.")
        return True
    except pymysql.Error as e:
        logger.warning("Could not init pos_held_orders: %s", e)
        return False


def _pho_parse_lines(raw_lines) -> list:
    """Normalize a held-order cart payload into the same shape `create_shop_pos_sale` expects."""
    parsed: list = []
    for ln in (raw_lines or []):
        if not isinstance(ln, dict):
            continue
        item_id_ln = ln.get("id")
        try:
            item_id_ln = int(item_id_ln) if item_id_ln is not None else None
        except Exception:
            item_id_ln = None
        try:
            qf = float(ln.get("qty") or 0)
        except Exception:
            qf = 0.0
        nm = (ln.get("name") or "").strip()
        if not nm:
            if not (item_id_ln and abs(qf) < 1e-12):
                continue
            nm = "Item"
        if qf < 0:
            continue
        if qf == 0 and item_id_ln is None:
            continue
        try:
            unit_price = float(ln.get("price") or 0)
        except Exception:
            unit_price = 0.0
        try:
            line_total = float(ln.get("total") or 0)
        except Exception:
            line_total = unit_price * qf
        parsed.append(
            {
                "name": nm[:200],
                "qty": qf,
                "unit_price": unit_price,
                "line_total": line_total,
                "item_id": item_id_ln,
            }
        )
    return parsed


def _pho_committed_totals(parsed: list, mode: str) -> Dict[int, float]:
    """Sum committed qty per item_id (only items with a real item_id contribute to stock movement)."""
    out: Dict[int, float] = {}
    use_int = mode in ("kitchen", "both")
    for p in parsed:
        iid = p.get("item_id")
        if iid is None:
            continue
        q = float(p.get("qty") or 0)
        if use_int:
            qr = round(q, STOCK_QTY_DECIMAL_PLACES)
            if abs(qr - int(qr)) > 1e-9:
                raise ValueError("Kitchen inventory uses whole portions only; adjust quantities.")
            q = int(qr)
        out[int(iid)] = round(float(out.get(int(iid), 0)) + float(q), STOCK_QTY_DECIMAL_PLACES)
    return out


_HOLD_REDUCTION_APPROVER_ROLES = frozenset(
    {"manager", "admin", "super_admin", "company_manager", "it_support"}
)


def _pho_validate_hold_reduction_approver(shop_id: int, code: str) -> Tuple[Optional[dict], Optional[str]]:
    """POS held-order qty decrease: require active manager / admin / company manager / IT / super admin code."""
    code = (code or "").strip()
    if not code:
        return None, "A manager, admin, company manager, IT support, or super admin must enter their 6-digit code to approve reducing committed quantities."
    if not re.fullmatch(r"\d{6}", code):
        return None, "Approver code must be exactly 6 digits."
    row = get_employee_by_code(code)
    if not row:
        return None, "Approval code not recognised."
    if (row.get("status") or "").lower() != "active":
        return None, "That approver account is not active."
    role_key = (row.get("role") or "employee").lower()
    if role_key not in _HOLD_REDUCTION_APPROVER_ROLES:
        return None, "Approver must be a manager, admin, company manager, IT support, or super admin."
    if not employee_may_use_shop_branch(dict(row), int(shop_id)):
        return None, "That approver is not allowed for this shop."
    return dict(row), None


def pos_held_order_save(
    *,
    shop_id: int,
    hold_id: Optional[int],
    lines: list,
    inventory_mode: str = "shop",
    label: Optional[str] = None,
    customer_name: Optional[str] = None,
    customer_phone: Optional[str] = None,
    total_amount: Optional[float] = None,
    item_count: Optional[float] = None,
    employee_id: Optional[int] = None,
    employee_code: Optional[str] = None,
    employee_name: Optional[str] = None,
    reduction_approver_code: Optional[str] = None,
) -> Tuple[bool, Optional[str], Optional[int], list, Dict[str, float], list]:
    """
    Create or update a held order; deduct stock / portions for positive *delta* lines only.

    Decreasing committed quantities is allowed when ``reduction_approver_code`` validates to a
    manager, admin, company manager, IT support, or super admin for this shop; stock / kitchen portions are
    restored accordingly.

    Returns ``(ok, err, hold_id, delta_lines, committed_after, reduction_lines)``.
    """
    mode = (inventory_mode or "shop").strip().lower()
    if mode not in ("shop", "kitchen", "none", "both"):
        mode = "shop"
    if mode in ("kitchen", "both"):
        ensure_shop_kitchen_portions_schema()

    try:
        parsed = _pho_parse_lines(lines)
    except ValueError as e:
        return False, str(e), None, [], {}, []
    if not parsed:
        return False, "Add at least one item before saving (holding) the order.", None, [], {}, []

    try:
        committed_after_map = _pho_committed_totals(parsed, mode)
    except ValueError as e:
        return False, str(e), None, [], {}, []

    try:
        total_amount_v = float(total_amount or 0)
    except Exception:
        total_amount_v = 0.0
    if total_amount_v < 0:
        total_amount_v = 0.0
    try:
        item_count_v = float(item_count or 0)
    except Exception:
        item_count_v = 0.0
    if item_count_v < 0:
        item_count_v = 0.0

    label_v = (label or "").strip()[:120]
    customer_name_v = (customer_name or "").strip()[:190] or None
    customer_phone_v = (customer_phone or "").strip()[:40] or None
    emp_code_v = (employee_code or "").strip()[:6] or None
    emp_name_v = (employee_name or "").strip()[:190] or None

    cart_json_str = json.dumps([{
        "id": p.get("item_id"),
        "name": p.get("name"),
        "qty": p.get("qty"),
        "price": p.get("unit_price"),
        "total": p.get("line_total"),
    } for p in parsed], separators=(",", ":"))
    committed_json_str = json.dumps({str(k): v for k, v in committed_after_map.items()}, separators=(",", ":"))

    stock_tx_sql = """
    INSERT INTO shop_stock_transactions
        (shop_id, item_id, direction, source, qty, shop_stock_before, shop_stock_after,
         company_stock_before, company_stock_after,
         reason, refunded, refund_amount, payment_status, note, created_by_employee_id)
    VALUES (%s,%s,'out','manual',%s,%s,%s,NULL,NULL,'POS_HOLD',0,NULL,'paid',%s,%s)
    """

    stock_tx_sql_in = """
    INSERT INTO shop_stock_transactions
        (shop_id, item_id, direction, source, qty, shop_stock_before, shop_stock_after,
         company_stock_before, company_stock_after,
         reason, refunded, refund_amount, payment_status, note, created_by_employee_id)
    VALUES (%s,%s,'in','manual',%s,%s,%s,NULL,NULL,'POS_HOLD_RET',0,NULL,'paid',%s,%s)
    """

    try:
        with get_cursor(commit=True) as cur:
            cur.execute("SELECT id FROM shops WHERE id=%s LIMIT 1 FOR UPDATE", (int(shop_id),))
            if not cur.fetchone():
                return False, "Shop not found.", None, [], {}, []

            committed_before_map: Dict[int, float] = {}
            existing_hold_id: Optional[int] = None
            saves_count_before = 0
            row = None
            if hold_id is not None:
                try:
                    hid = int(hold_id)
                except Exception:
                    hid = 0
                if hid > 0:
                    cur.execute(
                        """
                        SELECT id, status, committed_json, saves_count, inventory_mode,
                               created_by_employee_id, employee_code
                        FROM pos_held_orders
                        WHERE id=%s AND shop_id=%s
                        FOR UPDATE
                        """,
                        (hid, int(shop_id)),
                    )
                    row = cur.fetchone()
                    if not row:
                        return False, "Held order not found.", None, [], {}, []
                    if (row.get("status") or "open") != "open":
                        return False, "Held order is no longer open.", None, [], {}, []
                    # Only the cashier who created this held order is allowed to update it.
                    # Validate by employee id (strong identity), falling back to the stored
                    # employee_code only when the id wasn't recorded on creation.
                    creator_id_raw = row.get("created_by_employee_id")
                    creator_code_raw = (row.get("employee_code") or "").strip()
                    try:
                        creator_id = int(creator_id_raw) if creator_id_raw is not None else 0
                    except (TypeError, ValueError):
                        creator_id = 0
                    try:
                        incoming_id = int(employee_id) if employee_id is not None else 0
                    except (TypeError, ValueError):
                        incoming_id = 0
                    incoming_code = (employee_code or "").strip()
                    matched = False
                    if creator_id > 0 and incoming_id > 0:
                        matched = creator_id == incoming_id
                    elif creator_code_raw and incoming_code:
                        matched = creator_code_raw == incoming_code
                    if not matched:
                        return (
                            False,
                            "Not your order — only the cashier who created it can update it.",
                            None,
                            [],
                            {},
                            [],
                        )
                    try:
                        committed_before_map = {
                            int(k): float(v) for k, v in (json.loads(row.get("committed_json") or "{}") or {}).items()
                        }
                    except (json.JSONDecodeError, TypeError, ValueError):
                        committed_before_map = {}
                    existing_hold_id = int(row.get("id") or 0)
                    saves_count_before = int(row.get("saves_count") or 0)
                    # Inventory mode is locked to the first save's mode so stock arithmetic stays consistent.
                    stored_mode = (row.get("inventory_mode") or mode).strip().lower()
                    if stored_mode in ("shop", "kitchen", "none", "both"):
                        mode = stored_mode

            name_by_id: Dict[int, str] = {}
            for p in parsed:
                iid_nm = p.get("item_id")
                if iid_nm is None:
                    continue
                try:
                    iid_i = int(iid_nm)
                except (TypeError, ValueError):
                    continue
                name_by_id[iid_i] = str(p.get("name") or "Item")[:200]

            reduction_lines: list = []
            for iid_b, before_v in committed_before_map.items():
                try:
                    ik = int(iid_b)
                except (TypeError, ValueError):
                    continue
                before_qty = float(before_v)
                after_qty = float(committed_after_map.get(ik, 0.0))
                delta = round(after_qty - before_qty, STOCK_QTY_DECIMAL_PLACES)
                if delta >= -1e-12:
                    continue
                rq = abs(delta)
                reduction_lines.append(
                    {
                        "id": ik,
                        "name": name_by_id.get(ik, "Item"),
                        "qty": int(rq) if mode in ("kitchen", "both") else rq,
                        "price": 0.0,
                        "total": 0.0,
                    }
                )

            approver_row: Optional[dict] = None
            if reduction_lines:
                approver_row, appr_err = _pho_validate_hold_reduction_approver(
                    int(shop_id), reduction_approver_code or ""
                )
                if appr_err:
                    return False, appr_err, None, [], {}, []

            delta_lines: list = []
            seen_for_delta: set = set()
            for p in parsed:
                iid = p.get("item_id")
                if iid is None:
                    continue
                if int(iid) in seen_for_delta:
                    continue
                seen_for_delta.add(int(iid))
                before_qty = float(committed_before_map.get(int(iid), 0.0))
                after_qty = float(committed_after_map.get(int(iid), 0.0))
                delta = round(after_qty - before_qty, STOCK_QTY_DECIMAL_PLACES)
                if delta <= 0:
                    continue
                delta_lines.append(
                    {
                        "id": int(iid),
                        "name": p.get("name"),
                        "qty": int(delta) if mode in ("kitchen", "both") else delta,
                        "price": float(p.get("unit_price") or 0),
                        "total": round(float(p.get("unit_price") or 0) * float(delta), 2),
                    }
                )

            if mode == "shop":
                for iid in sorted(d["id"] for d in delta_lines):
                    cur.execute(
                        """
                        SELECT shop_stock_qty, stock_update_enabled
                        FROM shop_items
                        WHERE shop_id=%s AND item_id=%s
                        FOR UPDATE
                        """,
                        (int(shop_id), int(iid)),
                    )
                    si = cur.fetchone()
                    if not si:
                        return False, "POS item is not linked to this shop.", None, [], {}, []
                    if int(si.get("stock_update_enabled") or 0) != 1:
                        continue
                    need = next((d["qty"] for d in delta_lines if d["id"] == iid), 0)
                    before = round(float(si.get("shop_stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
                    if before < float(need):
                        return False, "Not enough stock at the shop for one or more new items. Adjust quantities or stock.", None, [], {}, []
            elif mode in ("kitchen", "both"):
                for iid in sorted(d["id"] for d in delta_lines):
                    cur.execute(
                        """
                        SELECT COALESCE(stock_update_enabled,0) AS sue
                        FROM shop_items
                        WHERE shop_id=%s AND item_id=%s
                        FOR UPDATE
                        """,
                        (int(shop_id), int(iid)),
                    )
                    si = cur.fetchone()
                    if not si:
                        return False, "POS item is not linked to this shop.", None, [], {}, []
                    if int(si.get("sue") or 0) != 1:
                        continue
                    cur.execute(
                        """
                        SELECT portions_remaining
                        FROM shop_kitchen_portions
                        WHERE shop_id=%s AND item_id=%s
                        FOR UPDATE
                        """,
                        (int(shop_id), int(iid)),
                    )
                    kr = cur.fetchone()
                    rem = int(kr.get("portions_remaining") or 0) if kr else 0
                    need = int(next((d["qty"] for d in delta_lines if d["id"] == iid), 0))
                    if rem < need:
                        return False, "Not enough kitchen portions for one or more new items. Adjust quantities or kitchen portions.", None, [], {}, []

            if existing_hold_id is None:
                cur.execute(
                    """
                    INSERT INTO pos_held_orders
                        (shop_id, created_by_employee_id, employee_code, employee_name,
                         customer_name, customer_phone, label, status, inventory_mode,
                         cart_json, committed_json, total_amount, item_count, saves_count, last_save_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, 'open', %s, %s, %s, %s, %s, 1, NOW())
                    """,
                    (
                        int(shop_id),
                        int(employee_id) if employee_id is not None else None,
                        emp_code_v,
                        emp_name_v,
                        customer_name_v,
                        customer_phone_v,
                        label_v,
                        mode,
                        cart_json_str,
                        committed_json_str,
                        round(total_amount_v, 2),
                        round(item_count_v, STOCK_QTY_DECIMAL_PLACES),
                    ),
                )
                existing_hold_id = int(cur.lastrowid or 0)
            else:
                cur.execute(
                    """
                    UPDATE pos_held_orders
                    SET cart_json=%s,
                        committed_json=%s,
                        total_amount=%s,
                        item_count=%s,
                        customer_name=COALESCE(%s, customer_name),
                        customer_phone=COALESCE(%s, customer_phone),
                        label=CASE WHEN %s = '' THEN label ELSE %s END,
                        saves_count=%s,
                        last_save_at=NOW()
                    WHERE id=%s AND shop_id=%s
                    """,
                    (
                        cart_json_str,
                        committed_json_str,
                        round(total_amount_v, 2),
                        round(item_count_v, STOCK_QTY_DECIMAL_PLACES),
                        customer_name_v,
                        customer_phone_v,
                        label_v,
                        label_v,
                        saves_count_before + 1,
                        int(existing_hold_id),
                        int(shop_id),
                    ),
                )

            note_base = "POS HOLD #%s" % (existing_hold_id,)
            if mode == "shop":
                for d in delta_lines:
                    iid = int(d["id"])
                    qshop = round(float(d["qty"]), STOCK_QTY_DECIMAL_PLACES)
                    if qshop <= 0:
                        continue
                    cur.execute(
                        """
                        SELECT shop_stock_qty, stock_update_enabled
                        FROM shop_items
                        WHERE shop_id=%s AND item_id=%s
                        FOR UPDATE
                        """,
                        (int(shop_id), iid),
                    )
                    si = cur.fetchone()
                    if not si or int(si.get("stock_update_enabled") or 0) != 1:
                        continue
                    shop_before = round(float(si.get("shop_stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
                    if shop_before < qshop:
                        return False, "Not enough stock at the shop for one or more new items. Adjust quantities or stock.", None, [], {}, []
                    shop_after = round(shop_before - qshop, STOCK_QTY_DECIMAL_PLACES)
                    cur.execute(
                        "UPDATE shop_items SET shop_stock_qty=%s WHERE shop_id=%s AND item_id=%s",
                        (shop_after, int(shop_id), iid),
                    )
                    note = note_base + " · " + (str(d.get("name") or "")[:160])
                    cur.execute(
                        stock_tx_sql,
                        (
                            int(shop_id),
                            iid,
                            qshop,
                            shop_before,
                            shop_after,
                            note,
                            int(employee_id) if employee_id is not None else None,
                        ),
                    )
                    _shop_fifo_after_stock_out(
                        cur, int(shop_id), iid, qshop, int(cur.lastrowid or 0)
                    )
            elif mode in ("kitchen", "both"):
                for d in delta_lines:
                    iid = int(d["id"])
                    qk = int(d["qty"])
                    if qk <= 0:
                        continue
                    cur.execute(
                        """
                        SELECT COALESCE(stock_update_enabled,0) AS sue
                        FROM shop_items
                        WHERE shop_id=%s AND item_id=%s
                        FOR UPDATE
                        """,
                        (int(shop_id), iid),
                    )
                    si = cur.fetchone()
                    if not si or int(si.get("sue") or 0) != 1:
                        continue
                    cur.execute(
                        """
                        UPDATE shop_kitchen_portions
                        SET portions_remaining = portions_remaining - %s
                        WHERE shop_id=%s AND item_id=%s AND portions_remaining >= %s
                        """,
                        (qk, int(shop_id), iid, qk),
                    )
                    if int(cur.rowcount or 0) < 1:
                        return False, "Not enough kitchen portions for one or more new items. Adjust quantities or kitchen portions.", None, [], {}, []

            appr_note_uid = int(approver_row["id"]) if approver_row else (
                int(employee_id) if employee_id is not None else None
            )
            if reduction_lines and mode == "shop":
                for d in reduction_lines:
                    iid = int(d["id"])
                    qret = round(float(d["qty"]), STOCK_QTY_DECIMAL_PLACES)
                    if qret <= 0:
                        continue
                    cur.execute(
                        """
                        SELECT shop_stock_qty, stock_update_enabled
                        FROM shop_items
                        WHERE shop_id=%s AND item_id=%s
                        FOR UPDATE
                        """,
                        (int(shop_id), iid),
                    )
                    si = cur.fetchone()
                    if not si or int(si.get("stock_update_enabled") or 0) != 1:
                        continue
                    shop_before = round(float(si.get("shop_stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
                    shop_after = round(shop_before + qret, STOCK_QTY_DECIMAL_PLACES)
                    cur.execute(
                        "UPDATE shop_items SET shop_stock_qty=%s WHERE shop_id=%s AND item_id=%s",
                        (shop_after, int(shop_id), iid),
                    )
                    note = note_base + " return · " + (str(d.get("name") or "")[:140])
                    cur.execute(
                        stock_tx_sql_in,
                        (
                            int(shop_id),
                            iid,
                            qret,
                            shop_before,
                            shop_after,
                            note,
                            appr_note_uid,
                        ),
                    )
                    _shop_fifo_after_stock_restore(
                        cur, int(shop_id), iid, qret, int(cur.lastrowid or 0)
                    )
            elif reduction_lines and mode in ("kitchen", "both"):
                for d in reduction_lines:
                    iid = int(d["id"])
                    qk = int(d["qty"])
                    if qk <= 0:
                        continue
                    cur.execute(
                        """
                        SELECT COALESCE(stock_update_enabled,0) AS sue
                        FROM shop_items
                        WHERE shop_id=%s AND item_id=%s
                        FOR UPDATE
                        """,
                        (int(shop_id), iid),
                    )
                    si = cur.fetchone()
                    if not si or int(si.get("sue") or 0) != 1:
                        continue
                    cur.execute(
                        """
                        INSERT INTO shop_kitchen_portions (shop_id, item_id, portions_remaining)
                        VALUES (%s, %s, %s)
                        ON DUPLICATE KEY UPDATE portions_remaining = portions_remaining + VALUES(portions_remaining)
                        """,
                        (int(shop_id), iid, qk),
                    )

            return True, None, int(existing_hold_id), delta_lines, {str(k): v for k, v in committed_after_map.items()}, reduction_lines
    except pymysql.Error as e:
        logger.warning("pos_held_order_save error: %s", e)
        return False, "Could not save the held order.", None, [], {}, []
    except ValueError as e:
        return False, str(e) or "Could not save the held order.", None, [], {}, []


def pos_held_order_get(shop_id: int, hold_id: int) -> Optional[dict]:
    """Fetch one held order plus its parsed cart_json. Returns None if not found."""
    sql = """
    SELECT id, shop_id, created_by_employee_id, employee_code, employee_name,
           customer_name, customer_phone, label, status, inventory_mode,
           cart_json, committed_json, total_amount, item_count, saves_count,
           last_save_at, finalized_sale_id, finalized_at, voided_at, created_at, updated_at
    FROM pos_held_orders
    WHERE id=%s AND shop_id=%s
    LIMIT 1
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (int(hold_id), int(shop_id)))
            row = cur.fetchone()
    except pymysql.Error:
        return None
    if not row:
        return None
    try:
        row["cart_lines"] = json.loads(row.get("cart_json") or "[]") or []
    except (json.JSONDecodeError, TypeError):
        row["cart_lines"] = []
    if not isinstance(row.get("cart_lines"), list):
        row["cart_lines"] = []
    try:
        row["committed_map"] = json.loads(row.get("committed_json") or "{}") or {}
    except (json.JSONDecodeError, TypeError):
        row["committed_map"] = {}
    if not isinstance(row.get("committed_map"), dict):
        row["committed_map"] = {}
    return row


def pos_held_order_list_open(
    shop_id: int,
    limit: int = 200,
    *,
    employee_id: Optional[int] = None,
    employee_code: Optional[str] = None,
) -> list:
    """List open held orders for a shop (most recently saved first).

    Optional ``employee_id`` / ``employee_code`` filters restrict the result to held orders
    created by that specific cashier — the POS uses this so a cashier only sees their own
    in-flight tabs once they've authenticated with a 6-digit code in the held-orders modal.
    """
    try:
        lim = max(1, min(int(limit or 200), 500))
    except Exception:
        lim = 200
    conditions = ["shop_id=%s", "status='open'"]
    params: list = [int(shop_id)]
    if employee_id is not None:
        try:
            eid = int(employee_id)
        except (TypeError, ValueError):
            eid = 0
        if eid > 0:
            conditions.append("(created_by_employee_id=%s OR employee_code=%s)")
            params.append(eid)
            params.append((employee_code or "").strip()[:6])
    elif employee_code:
        code = (employee_code or "").strip()[:6]
        if code:
            conditions.append("employee_code=%s")
            params.append(code)
    sql = (
        "SELECT id, customer_name, customer_phone, label, total_amount, item_count, saves_count,\n"
        "       last_save_at, employee_code, employee_name, inventory_mode, created_at\n"
        "FROM pos_held_orders\n"
        "WHERE " + " AND ".join(conditions) + "\n"
        "ORDER BY COALESCE(last_save_at, created_at) DESC, id DESC\n"
        "LIMIT %s"
    )
    params.append(int(lim))
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(params))
            return list(cur.fetchall() or [])
    except pymysql.Error:
        return []


def list_all_pos_held_orders_register_rows(
    analytics_filter: Optional[dict] = None,
    *,
    shop_id: Optional[int] = None,
    limit: int = 8000,
) -> list:
    """All branches: held-order tabs for IT register (open / finalized / voided)."""
    init_pos_held_orders_table()
    af = analytics_filter if isinstance(analytics_filter, dict) else {}
    rng_sql, rng_params = _analytics_where_clause(af, "h")
    params: list = []
    where_parts = [rng_sql]
    params.extend(rng_params)
    if shop_id is not None and int(shop_id) > 0:
        where_parts.append("h.shop_id = %s")
        params.append(int(shop_id))
    where_sql = " AND ".join(where_parts)
    lim = max(1, min(int(limit or 8000), 30000))
    sql = f"""
    SELECT
        h.id AS hold_id,
        h.shop_id,
        COALESCE(sh.shop_name, '') AS shop_name,
        COALESCE(sh.shop_code, '') AS shop_code,
        h.status,
        h.customer_name,
        h.customer_phone,
        h.label,
        h.total_amount,
        h.item_count,
        h.saves_count,
        h.last_save_at,
        h.created_at,
        h.updated_at,
        h.finalized_sale_id,
        h.finalized_at,
        h.voided_at,
        h.employee_code,
        h.employee_name,
        h.inventory_mode,
        COALESCE(s.receipt_number, '') AS finalized_receipt_number
    FROM pos_held_orders h
    LEFT JOIN shops sh ON sh.id = h.shop_id
    LEFT JOIN shop_pos_sales s
        ON s.id = h.finalized_sale_id AND s.shop_id = h.shop_id
    WHERE {where_sql}
    ORDER BY COALESCE(h.last_save_at, h.updated_at, h.created_at) DESC, h.id DESC
    LIMIT %s
    """
    params.append(lim)
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
    except pymysql.Error:
        return []

    out: list = []
    for r in rows:
        rr = dict(r)
        try:
            rr["hold_id"] = int(rr.get("hold_id") or 0)
        except (TypeError, ValueError):
            rr["hold_id"] = 0
        try:
            rr["shop_id"] = int(rr.get("shop_id") or 0)
        except (TypeError, ValueError):
            rr["shop_id"] = 0
        st = str(rr.get("status") or "open").strip().lower()
        if st == "finalized":
            rr["register_status"] = "completed"
        elif st == "voided":
            rr["register_status"] = "returned"
        else:
            rr["register_status"] = "pending"
        for k in ("total_amount", "item_count"):
            try:
                rr[k] = float(rr.get(k) or 0)
            except (TypeError, ValueError):
                rr[k] = 0.0
        try:
            rr["saves_count"] = int(rr.get("saves_count") or 0)
        except (TypeError, ValueError):
            rr["saves_count"] = 0
        try:
            rr["finalized_sale_id"] = int(rr["finalized_sale_id"]) if rr.get("finalized_sale_id") is not None else None
        except (TypeError, ValueError):
            rr["finalized_sale_id"] = None
        for dk in ("last_save_at", "created_at", "updated_at", "finalized_at", "voided_at"):
            cat = rr.get(dk)
            if hasattr(cat, "isoformat"):
                rr[dk + "_iso"] = cat.isoformat(timespec="seconds")
            elif cat is None:
                rr[dk + "_iso"] = ""
            else:
                rr[dk + "_iso"] = str(cat)
        out.append(rr)
    return out


def pos_held_order_void(
    shop_id: int,
    hold_id: int,
    *,
    employee_id: Optional[int] = None,
) -> Tuple[bool, Optional[str]]:
    """Void an open held order. Disallowed once any qty has been committed (stock already deducted)."""
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(
                """
                SELECT id, status, committed_json
                FROM pos_held_orders
                WHERE id=%s AND shop_id=%s
                FOR UPDATE
                """,
                (int(hold_id), int(shop_id)),
            )
            row = cur.fetchone()
            if not row:
                return False, "Held order not found."
            if (row.get("status") or "open") != "open":
                return False, "Held order is no longer open."
            try:
                committed = json.loads(row.get("committed_json") or "{}") or {}
            except (json.JSONDecodeError, TypeError):
                committed = {}
            if isinstance(committed, dict) and any(float(v or 0) > 0 for v in committed.values()):
                return False, "This held order already has committed items; finalize the sale instead of voiding."
            cur.execute(
                """
                UPDATE pos_held_orders
                SET status='voided', voided_at=NOW(), voided_by_employee_id=%s
                WHERE id=%s AND shop_id=%s
                """,
                (int(employee_id) if employee_id is not None else None, int(hold_id), int(shop_id)),
            )
        return True, None
    except pymysql.Error as e:
        logger.warning("pos_held_order_void error: %s", e)
        return False, "Could not void the held order."


def pos_held_order_mark_finalized(
    shop_id: int,
    hold_id: int,
    finalized_sale_id: Optional[int],
) -> bool:
    """Mark a held order as finalized once its sale receipt has been recorded."""
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(
                """
                UPDATE pos_held_orders
                SET status='finalized', finalized_sale_id=%s, finalized_at=NOW()
                WHERE id=%s AND shop_id=%s AND status='open'
                """,
                (
                    int(finalized_sale_id) if finalized_sale_id is not None else None,
                    int(hold_id),
                    int(shop_id),
                ),
            )
            return int(cur.rowcount or 0) > 0
    except pymysql.Error as e:
        logger.warning("pos_held_order_mark_finalized error: %s", e)
        return False


_ENSURED_SHOP_KITCHEN_PORTIONS_PRICE = False


def ensure_shop_kitchen_portions_production_price_column() -> bool:
    """Add estimated production price per item (used as kitchen portion COGS in shop reports)."""
    global _ENSURED_SHOP_KITCHEN_PORTIONS_PRICE
    if _ENSURED_SHOP_KITCHEN_PORTIONS_PRICE:
        return True
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS shop_kitchen_portions (
                    shop_id INT NOT NULL,
                    item_id INT NOT NULL,
                    portions_remaining INT NOT NULL DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    PRIMARY KEY (shop_id, item_id),
                    KEY idx_shop_kitchen_shop (shop_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            if not column_exists("shop_kitchen_portions", "estimated_production_price"):
                cur.execute(
                    """
                    ALTER TABLE shop_kitchen_portions
                    ADD COLUMN estimated_production_price DECIMAL(14,2) NOT NULL DEFAULT 0
                    COMMENT 'Est. cost per portion for kitchen COGS in reports'
                    AFTER portions_remaining
                    """
                )
        _ENSURED_SHOP_KITCHEN_PORTIONS_PRICE = True
        return True
    except pymysql.Error as e:
        logger.warning("Could not add shop_kitchen_portions.estimated_production_price: %s", e)
        return False


def ensure_shop_kitchen_portions_schema() -> bool:
    """Per-shop kitchen portion counts for POS when inventory mode is kitchen (not shop stock)."""
    sql = """
    CREATE TABLE IF NOT EXISTS shop_kitchen_portions (
        shop_id INT NOT NULL,
        item_id INT NOT NULL,
        portions_remaining INT NOT NULL DEFAULT 0,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (shop_id, item_id),
        KEY idx_shop_kitchen_shop (shop_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql)
        logger.info("Table shop_kitchen_portions is ready.")
        return True
    except pymysql.Error as e:
        logger.warning("Could not init shop_kitchen_portions: %s", e)
        return False


def list_shop_kitchen_portion_editor_rows(
    shop_id: int, limit: int = 5000, *, only_displayed_on_pos: bool = True
):
    """Items for this shop with current kitchen portion counts.

    When only_displayed_on_pos is True (default), only rows with shop_items.displayed = 1 (POS-visible).
    When False, every item linked to the shop in shop_items is included.
    """
    ensure_shop_kitchen_portions_production_price_column()
    displayed_clause = " AND si.displayed = 1" if only_displayed_on_pos else ""
    sql = f"""
    SELECT
        i.id,
        i.category,
        i.name,
        COALESCE(skp.portions_remaining, 0) AS portions_remaining,
        COALESCE(skp.estimated_production_price, 0) AS estimated_production_price
    FROM shop_items si
    INNER JOIN items i ON i.id = si.item_id AND i.status = 'active'
    LEFT JOIN shop_kitchen_portions skp
        ON skp.shop_id = si.shop_id AND skp.item_id = i.id
    WHERE si.shop_id = %s{displayed_clause}
    ORDER BY i.category ASC, i.name ASC
    LIMIT %s
    """
    with get_cursor() as cur:
        cur.execute(sql, (int(shop_id), int(limit)))
        rows = cur.fetchall() or []
    for r in rows:
        r["portions_remaining"] = int(r.get("portions_remaining") or 0)
        r["id"] = int(r.get("id") or 0)
        r["estimated_production_price"] = round(float(r.get("estimated_production_price") or 0), 2)
    return rows


def upsert_shop_kitchen_portion_qty(
    shop_id: int,
    item_id: int,
    portions: int,
    *,
    estimated_production_price: Optional[float] = None,
) -> bool:
    """Set remaining kitchen portions (and optionally est. production price) for an item at a shop."""
    ensure_shop_kitchen_portions_production_price_column()
    q = max(0, min(99999999, int(portions or 0)))
    try:
        with get_cursor(commit=True) as cur:
            if estimated_production_price is None:
                cur.execute(
                    """
                    INSERT INTO shop_kitchen_portions (shop_id, item_id, portions_remaining)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE portions_remaining = VALUES(portions_remaining)
                    """,
                    (int(shop_id), int(item_id), q),
                )
            else:
                price = max(0.0, round(float(estimated_production_price or 0), 2))
                cur.execute(
                    """
                    INSERT INTO shop_kitchen_portions
                        (shop_id, item_id, portions_remaining, estimated_production_price)
                    VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        portions_remaining = VALUES(portions_remaining),
                        estimated_production_price = VALUES(estimated_production_price)
                    """,
                    (int(shop_id), int(item_id), q, price),
                )
        return True
    except pymysql.Error:
        return False


def _shop_kitchen_portion_cogs_report_filters(
    shop_id: int,
    analytics_filter: dict,
    analytics_scope: str = "general",
    *,
    allow_credit_sale: bool = True,
) -> Optional[tuple[str, list, str]]:
    """Shared WHERE clause for kitchen/both portion COGS (est. production price × qty sold)."""
    try:
        sid = int(shop_id)
    except Exception:
        return None
    if sid <= 0:
        return None
    ensure_shop_kitchen_portions_production_price_column()
    ensure_shop_pos_sale_items_portion_unit_cost_column()
    ensure_shop_pos_sales_inventory_mode_column()
    init_shop_pos_sale_items_table()
    af = analytics_filter or {}
    scope_where, scope_params = _analytics_receipt_scope_clause(analytics_scope, "s")
    sale_range_where, sale_range_params = _analytics_where_clause(af, "s")
    sale_where = f"s.shop_id=%s AND {sale_range_where} AND {scope_where}"
    sale_params = [sid] + list(sale_range_params) + list(scope_params)
    credit_clause = "" if allow_credit_sale else " AND s.sale_type <> 'credit'"
    return sale_where, sale_params, credit_clause


_KITCHEN_PORTION_SALE_NAME_JOIN = """
LEFT JOIN (
    SELECT MIN(i.id) AS id, LOWER(TRIM(i.name)) AS nm
    FROM items i
    WHERE i.status = 'active' AND COALESCE(i.stock_update_enabled, 0) = 1
    GROUP BY LOWER(TRIM(i.name))
) im ON (spi.item_id IS NULL OR spi.item_id = 0)
    AND im.nm = LOWER(TRIM(COALESCE(spi.item_name, '')))
    AND LENGTH(TRIM(COALESCE(spi.item_name, ''))) > 0
"""


def _kitchen_portion_unit_cost_expr() -> str:
    """Sale-time snapshot first, then current kitchen portion price (by id or matched name)."""
    if _ENSURED_POS_SALE_ITEMS_PORTION_COST or column_exists(
        "shop_pos_sale_items", "portion_unit_cost"
    ):
        return (
            "COALESCE(spi.portion_unit_cost, skp.estimated_production_price, "
            "skp_n.estimated_production_price, 0)"
        )
    return "COALESCE(skp.estimated_production_price, skp_n.estimated_production_price, 0)"


def _kitchen_portion_cogs_from_joins() -> str:
    cost = _kitchen_portion_unit_cost_expr()
    return f"""
    FROM shop_pos_sale_items spi
    INNER JOIN shop_pos_sales s ON s.id = spi.sale_id AND s.shop_id = spi.shop_id
    LEFT JOIN shop_kitchen_portions skp
        ON skp.shop_id = s.shop_id
        AND spi.item_id IS NOT NULL AND spi.item_id > 0
        AND skp.item_id = spi.item_id
    {_KITCHEN_PORTION_SALE_NAME_JOIN}
    LEFT JOIN shop_kitchen_portions skp_n
        ON skp_n.shop_id = s.shop_id AND skp_n.item_id = im.id
    """


def sum_shop_kitchen_portion_cogs_for_report(
    shop_id: int,
    analytics_filter: dict,
    analytics_scope: str = "general",
    *,
    allow_credit_sale: bool = True,
) -> float:
    """Kitchen/both POS sales COGS: portions sold × est. production price (snapshot at sale)."""
    filt = _shop_kitchen_portion_cogs_report_filters(
        shop_id, analytics_filter, analytics_scope, allow_credit_sale=allow_credit_sale
    )
    if not filt:
        return 0.0
    sale_where, sale_params, credit_clause = filt
    cost = _kitchen_portion_unit_cost_expr()
    sql = f"""
    SELECT COALESCE(SUM(spi.qty * ({cost})), 0) AS portion_cogs
    {_kitchen_portion_cogs_from_joins()}
    WHERE {sale_where}
      AND LOWER(COALESCE(s.inventory_mode, 'shop')) IN ('kitchen', 'both')
      {credit_clause}
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(sale_params))
            row = cur.fetchone() or {}
        return round(float(row.get("portion_cogs") or 0), 2)
    except pymysql.Error:
        return 0.0


def kitchen_portion_cogs_by_item_for_report(
    shop_id: int,
    analytics_filter: dict,
    analytics_scope: str = "general",
    *,
    allow_credit_sale: bool = True,
) -> dict:
    """Per-item portion COGS for kitchen/both POS sales (includes name-matched lines)."""
    filt = _shop_kitchen_portion_cogs_report_filters(
        shop_id, analytics_filter, analytics_scope, allow_credit_sale=allow_credit_sale
    )
    if not filt:
        return {}
    sale_where, sale_params, credit_clause = filt
    cost = _kitchen_portion_unit_cost_expr()
    sql = f"""
    SELECT
        COALESCE(NULLIF(spi.item_id, 0), im.id) AS item_id,
        MAX(COALESCE(spi.item_name, '')) AS item_name,
        COALESCE(SUM(spi.qty * ({cost})), 0) AS portion_cogs
    {_kitchen_portion_cogs_from_joins()}
    WHERE {sale_where}
      AND LOWER(COALESCE(s.inventory_mode, 'shop')) IN ('kitchen', 'both')
      {credit_clause}
    GROUP BY COALESCE(NULLIF(spi.item_id, 0), im.id), LOWER(TRIM(COALESCE(spi.item_name, '')))
    """
    out: dict = {}
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(sale_params))
            for r in cur.fetchall() or []:
                try:
                    iid = int(r.get("item_id") or 0)
                except Exception:
                    iid = 0
                cogs = round(float(r.get("portion_cogs") or 0), 2)
                if iid > 0:
                    out[iid] = round(float(out.get(iid) or 0) + cogs, 2)
                else:
                    nm = (r.get("item_name") or "").strip().lower()
                    if nm:
                        out[f"name:{nm}"] = cogs
    except pymysql.Error:
        return {}
    return out


def kitchen_portion_cost_warnings_for_report(
    shop_id: int,
    analytics_filter: dict,
    analytics_scope: str = "general",
    *,
    allow_credit_sale: bool = True,
) -> list:
    """Items with kitchen/both sales but zero est. production price (missing buying price)."""
    filt = _shop_kitchen_portion_cogs_report_filters(
        shop_id, analytics_filter, analytics_scope, allow_credit_sale=allow_credit_sale
    )
    if not filt:
        return []
    sale_where, sale_params, credit_clause = filt
    cost = _kitchen_portion_unit_cost_expr()
    sql = f"""
    SELECT
        COALESCE(NULLIF(spi.item_id, 0), im.id) AS item_id,
        MAX(COALESCE(spi.item_name, i2.name, '')) AS item_name,
        COALESCE(SUM(spi.qty), 0) AS portions_sold,
        COALESCE(SUM(spi.qty * ({cost})), 0) AS portion_cogs
    {_kitchen_portion_cogs_from_joins()}
    LEFT JOIN items i2 ON i2.id = COALESCE(NULLIF(spi.item_id, 0), im.id)
    WHERE {sale_where}
      AND LOWER(COALESCE(s.inventory_mode, 'shop')) IN ('kitchen', 'both')
      {credit_clause}
    GROUP BY COALESCE(NULLIF(spi.item_id, 0), im.id), LOWER(TRIM(COALESCE(spi.item_name, '')))
    HAVING portions_sold > 0 AND portion_cogs <= 0
    ORDER BY portions_sold DESC, item_name ASC
    LIMIT 50
    """
    warnings: list = []
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(sale_params))
            for r in cur.fetchall() or []:
                warnings.append(
                    {
                        "item_id": int(r.get("item_id") or 0) or None,
                        "name": (r.get("item_name") or "").strip() or "Unknown item",
                        "portions_sold": int(r.get("portions_sold") or 0),
                    }
                )
    except pymysql.Error:
        return []
    return warnings


def _apply_kitchen_portion_item_stock_costs(items: list, portion_cogs_by_item: dict) -> None:
    """Replace FIFO sale costs with est. production price × portions sold per item."""
    for item in items or []:
        if not isinstance(item, dict):
            continue
        try:
            iid = int(item.get("item_id") or 0)
        except Exception:
            iid = 0
        item_total = 0.0
        if iid > 0:
            item_total = round(float(portion_cogs_by_item.get(iid) or 0), 2)
        if item_total <= 0:
            nm = (item.get("name") or "").strip().lower()
            if nm:
                item_total = round(float(portion_cogs_by_item.get(f"name:{nm}") or 0), 2)
        sale_qty_i = int(item.get("sale_qty") or 0)
        credit_qty_i = int(item.get("credit_qty") or 0)
        sold_qty_total = sale_qty_i + credit_qty_i
        if sold_qty_total > 0 and item_total > 0:
            sale_stock_cost = round(item_total * sale_qty_i / sold_qty_total, 2)
            credit_stock_cost_full = round(item_total - sale_stock_cost, 2)
        else:
            sale_stock_cost = 0.0
            credit_stock_cost_full = 0.0
        credit_amount_f = round(float(item.get("credit_amount") or 0), 2)
        credit_paid_f = round(float(item.get("credit_paid_amount") or 0), 2)
        item["sale_stock_cost"] = sale_stock_cost
        item["credit_stock_cost_full"] = credit_stock_cost_full
        item["credit_stock_cost"] = _credit_stock_cost_for_payment(
            credit_stock_cost_full, credit_amount_f, credit_paid_f
        )
        item["stock_cost_sold"] = item_total


def _kitchen_portion_price_at_checkout(cur, shop_id: int, item_id, item_name: str = "") -> float:
    """Est. production price snapshot for a kitchen/both sale line."""
    iid = 0
    try:
        if item_id is not None:
            iid = int(item_id)
    except (TypeError, ValueError):
        iid = 0
    try:
        if iid > 0:
            cur.execute(
                """
                SELECT COALESCE(estimated_production_price, 0)
                FROM shop_kitchen_portions
                WHERE shop_id=%s AND item_id=%s
                LIMIT 1
                """,
                (int(shop_id), iid),
            )
        else:
            nm = (item_name or "").strip()
            if not nm:
                return 0.0
            cur.execute(
                """
                SELECT COALESCE(skp.estimated_production_price, 0)
                FROM items i
                INNER JOIN shop_items si ON si.item_id = i.id AND si.shop_id = %s
                LEFT JOIN shop_kitchen_portions skp
                    ON skp.shop_id = si.shop_id AND skp.item_id = i.id
                WHERE i.status = 'active'
                  AND LOWER(TRIM(i.name)) = LOWER(TRIM(%s))
                LIMIT 1
                """,
                (int(shop_id), nm[:200]),
            )
        row = cur.fetchone() or {}
        return round(max(0.0, float(row.get("estimated_production_price") or 0)), 2)
    except pymysql.Error:
        return 0.0


def add_shop_kitchen_portions(shop_id: int, item_id: int, delta: int) -> Tuple[bool, Optional[int]]:
    """Atomically add ``delta`` portions to ``shop_kitchen_portions.portions_remaining``.

    ``delta`` must be a positive integer (this is a refill operation; sales are deducted
    via the sale-recording path). Returns ``(ok, new_portions_remaining)``.
    """
    try:
        d = int(delta or 0)
    except (TypeError, ValueError):
        return False, None
    if d <= 0:
        return False, None
    if d > 99999999:
        d = 99999999
    ensure_shop_kitchen_portions_schema()
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(
                """
                INSERT INTO shop_kitchen_portions (shop_id, item_id, portions_remaining)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    portions_remaining = LEAST(99999999, portions_remaining + VALUES(portions_remaining))
                """,
                (int(shop_id), int(item_id), d),
            )
            cur.execute(
                "SELECT portions_remaining FROM shop_kitchen_portions WHERE shop_id=%s AND item_id=%s LIMIT 1",
                (int(shop_id), int(item_id)),
            )
            row = cur.fetchone()
        new_q = int((row or {}).get("portions_remaining") or 0) if row else None
        return True, new_q
    except pymysql.Error:
        return False, None


def create_shop_pos_quotation(
    *,
    shop_id: Optional[int],
    quote_basis: str,
    quote_channel: str = "walkin",
    total_amount: float,
    item_count: int,
    customer_name: Optional[str] = None,
    customer_phone: Optional[str] = None,
    customer_notes: Optional[str] = None,
    employee_id: Optional[int] = None,
    employee_code: Optional[str] = None,
    employee_name: Optional[str] = None,
    lines: Optional[list] = None,
    client_txn_id: Optional[str] = None,
) -> Tuple[Optional[int], Optional[str]]:
    basis = (quote_basis or "").strip().lower()
    if basis not in ("sale", "credit"):
        return None, "Invalid quotation type."
    channel = (quote_channel or "walkin").strip().lower()
    if channel not in ("walkin", "online"):
        return None, "Invalid quote channel."
    if channel == "walkin" and shop_id is None:
        return None, "Shop is required for walk-in quotations."
    try:
        amount = float(total_amount)
    except Exception:
        return None, "Invalid total."
    if amount < 0:
        return None, "Invalid total."
    try:
        count = int(item_count)
    except Exception:
        count = 0
    if count < 0:
        count = 0

    serializable = []
    for ln in lines or []:
        if not isinstance(ln, dict):
            continue
        nm = (ln.get("name") or "").strip()
        if not nm:
            continue
        try:
            qty = int(ln.get("qty") or 0)
        except Exception:
            qty = 0
        if qty <= 0:
            continue
        try:
            unit_price = float(ln.get("price") or 0)
        except Exception:
            unit_price = 0.0
        try:
            line_total = float(ln.get("total") or 0)
        except Exception:
            line_total = unit_price * qty
        item_id = ln.get("id")
        try:
            item_id = int(item_id) if item_id is not None else None
        except Exception:
            item_id = None
        serializable.append(
            {
                "id": item_id,
                "name": nm[:200],
                "qty": qty,
                "price": unit_price,
                "total": line_total,
            }
        )

    try:
        blob = json.dumps(serializable, separators=(",", ":"), ensure_ascii=False)
    except (TypeError, ValueError):
        blob = "[]"

    ensure_shop_pos_quotations_client_txn_column()
    client_txn = (client_txn_id or "").strip()[:64] or None
    notes_val = (customer_notes or "").strip()[:500] or None
    has_notes_col = column_exists("shop_pos_quotations", "customer_notes")

    sql = """
    INSERT INTO shop_pos_quotations
        (shop_id, quote_basis, quote_channel, total_amount, item_count, customer_name, customer_phone,
         {notes_col}lines_json, employee_id, employee_code, employee_name, client_txn_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s, {notes_val} %s, %s, %s, %s, %s)
    """.format(
        notes_col="customer_notes, " if has_notes_col else "",
        notes_val="%s, " if has_notes_col else "",
    )
    try:
        with get_cursor(commit=True) as cur:
            if client_txn and shop_id is not None:
                cur.execute(
                    """
                    SELECT id FROM shop_pos_quotations
                    WHERE shop_id=%s AND client_txn_id=%s
                    LIMIT 1
                    """,
                    (int(shop_id), client_txn),
                )
                existing = cur.fetchone()
                if existing:
                    return int(existing.get("id") or 0), None
            params = [
                int(shop_id) if shop_id is not None else None,
                basis,
                channel,
                amount,
                count,
                (customer_name or "").strip()[:190] or None,
                (customer_phone or "").strip()[:40] or None,
            ]
            if has_notes_col:
                params.append(notes_val)
            params.extend(
                [
                    blob,
                    int(employee_id) if employee_id is not None else None,
                    (employee_code or "").strip()[:6] or None,
                    (employee_name or "").strip()[:190] or None,
                    client_txn,
                ]
            )
            cur.execute(sql, tuple(params))
            qid = int(cur.lastrowid or 0)
            return (qid if qid else None), None
    except pymysql.Error as e:
        logger.warning("Could not insert shop_pos_quotation: %s", e)
        return None, None


def list_shop_pos_quotations(
    shop_id: int,
    limit: int = 500,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
):
    clauses = ["shop_id=%s"]
    params: list = [int(shop_id)]
    if date_from:
        clauses.append("DATE(created_at) >= %s")
        params.append(str(date_from)[:10])
    if date_to:
        clauses.append("DATE(created_at) <= %s")
        params.append(str(date_to)[:10])
    where_sql = " AND ".join(clauses)
    sql = f"""
    SELECT id, shop_id, quote_basis, quote_channel, customer_name, customer_phone, customer_notes, total_amount, item_count,
           lines_json, employee_id, employee_code, employee_name, created_at
    FROM shop_pos_quotations
    WHERE {where_sql}
    ORDER BY created_at DESC, id DESC
    LIMIT %s
    """
    params.append(int(limit))
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
    except pymysql.Error:
        return []
    out = []
    for r in rows:
        rr = dict(r)
        raw = rr.get("lines_json") or "[]"
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        try:
            rr["lines"] = json.loads(raw) if raw else []
        except (TypeError, ValueError):
            rr["lines"] = []
        rr.pop("lines_json", None)
        out.append(rr)
    return out


def get_shop_pos_quotation_by_id(quote_id: int, shop_id: Optional[int] = None) -> Optional[dict]:
    """Load one POS quotation row for public share pages."""
    try:
        qid = int(quote_id)
    except (TypeError, ValueError):
        return None
    if qid <= 0:
        return None
    clauses = ["id=%s"]
    params: list = [qid]
    if shop_id is not None:
        try:
            sid = int(shop_id)
        except (TypeError, ValueError):
            return None
        if sid > 0:
            clauses.append("shop_id=%s")
            params.append(sid)
    sql = f"""
    SELECT id, shop_id, quote_basis, quote_channel, customer_name, customer_phone, customer_notes,
           total_amount, item_count, lines_json, employee_id, employee_code, employee_name, created_at
    FROM shop_pos_quotations
    WHERE {" AND ".join(clauses)}
    LIMIT 1
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(params))
            row = cur.fetchone()
    except pymysql.Error:
        return None
    if not row:
        return None
    rr = dict(row)
    raw = rr.get("lines_json") or "[]"
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    try:
        rr["lines"] = json.loads(raw) if raw else []
    except (TypeError, ValueError):
        rr["lines"] = []
    rr.pop("lines_json", None)
    return rr


def list_all_pos_quotations_for_it(
    limit: int = 2000,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
):
    clauses = ["1=1"]
    params: list = []
    if date_from:
        clauses.append("DATE(q.created_at) >= %s")
        params.append(str(date_from)[:10])
    if date_to:
        clauses.append("DATE(q.created_at) <= %s")
        params.append(str(date_to)[:10])
    where_sql = " AND ".join(clauses)
    sql = f"""
    SELECT q.id, q.shop_id, q.quote_basis, q.quote_channel, q.customer_name, q.customer_phone, q.customer_notes,
           q.total_amount, q.item_count, q.lines_json, q.employee_id, q.employee_code, q.employee_name, q.created_at,
           s.shop_name, s.shop_code
    FROM shop_pos_quotations q
    LEFT JOIN shops s ON s.id = q.shop_id
    WHERE {where_sql}
    ORDER BY q.created_at DESC, q.id DESC
    LIMIT %s
    """
    params.append(int(limit))
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
    except pymysql.Error:
        return []
    out = []
    for r in rows:
        rr = dict(r)
        raw = rr.get("lines_json") or "[]"
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        try:
            rr["lines"] = json.loads(raw) if raw else []
        except (TypeError, ValueError):
            rr["lines"] = []
        rr.pop("lines_json", None)
        out.append(rr)
    return out


def create_shop_pos_sale(
    *,
    shop_id: int,
    sale_type: str,
    total_amount: float,
    item_count: int,
    customer_name: Optional[str] = None,
    customer_phone: Optional[str] = None,
    employee_id: Optional[int] = None,
    employee_code: Optional[str] = None,
    employee_name: Optional[str] = None,
    credit_due_date: Optional[str] = None,
    payment_method: Optional[str] = None,
    cash_amount: Optional[float] = None,
    mpesa_amount: Optional[float] = None,
    lines: Optional[list] = None,
    inventory_mode: str = "shop",
    client_txn_id: Optional[str] = None,
    skip_stock_deduction: bool = False,
    mpesa_receipt_number: Optional[str] = None,
) -> Tuple[bool, Optional[str], Optional[int], Optional[str]]:
    """
    Apply inventory for exactly one pathway per sale (no shelf + portion double-move).

    ``shop``: decrement ``shop_items.shop_stock_qty``. ``kitchen``: decrement kitchen portions only.
    ``both``: POS checkout uses kitchen portions only; shelf qty is adjusted elsewhere (Stock management).
    ``none``: no quantity movement.

    ``skip_stock_deduction=True`` records the sale receipt and line items but performs no stock
    pre-check or movement. This is used when finalizing a withhold-POS held order whose stock
    was already deducted incrementally via :func:`pos_held_order_save`. Default is False so
    existing Direct-POS callers retain their full inventory-validation behavior.
    """
    ensure_shop_credit_payments_schema()
    mode = (inventory_mode or "shop").strip().lower()
    if mode not in ("shop", "kitchen", "none", "both"):
        mode = "shop"
    if mode in ("kitchen", "both"):
        ensure_shop_kitchen_portions_schema()
    s_type = (sale_type or "").strip().lower()
    if s_type not in ("sale", "credit"):
        return False, None, None, None
    try:
        amount = float(total_amount)
    except Exception:
        return False, None, None, None
    if amount < 0:
        return False, None, None, None
    pay_method = (payment_method or "").strip().lower()
    if s_type == "credit":
        pay_method = "credit"
        cash_val = 0.0
        mpesa_val = 0.0
    else:
        if pay_method not in ("cash", "mpesa", "both"):
            return False, "Select a valid payment method.", None, None
        try:
            cash_val = float(cash_amount or 0)
        except Exception:
            cash_val = 0.0
        try:
            mpesa_val = float(mpesa_amount or 0)
        except Exception:
            mpesa_val = 0.0
        if cash_val < 0 or mpesa_val < 0:
            return False, "Payment amounts cannot be negative.", None, None
        if pay_method == "cash":
            cash_val = amount
            mpesa_val = 0.0
        elif pay_method == "mpesa":
            cash_val = 0.0
            mpesa_val = amount
        else:
            total_paid = round(cash_val + mpesa_val, 2)
            if abs(total_paid - round(amount, 2)) > 0.01:
                return False, "Cash + M-Pesa must match total amount.", None, None

    due_sql_val = None
    if s_type == "credit" and credit_due_date:
        raw_due = str(credit_due_date).strip()[:32]
        if raw_due:
            try:
                parts = raw_due.split("T", 1)[0].strip()
                y, m, d = (int(x) for x in parts.split("-", 2))
                due_sql_val = date(y, m, d)
            except Exception:
                return False, "Invalid credit payment date.", None, None

    parsed: list = []
    for ln in (lines or []):
        if not isinstance(ln, dict):
            continue
        nm = (ln.get("name") or "").strip()
        if not nm:
            continue
        if mode in ("kitchen", "both"):
            try:
                qf = float(ln.get("qty") or 0)
            except Exception:
                qf = 0.0
            if qf <= 0:
                continue
            qr = round(qf, STOCK_QTY_DECIMAL_PLACES)
            if abs(qr - int(qr)) > 1e-9:
                return False, "Kitchen inventory uses whole portions only; adjust quantities.", None, None
            qty = int(qr)
        elif mode == "shop":
            qty = normalize_stock_move_qty(ln.get("qty"))
            if qty is None:
                continue
        else:
            qty = normalize_stock_move_qty(ln.get("qty"))
            if qty is None:
                continue
        try:
            unit_price = float(ln.get("price") or 0)
        except Exception:
            unit_price = 0.0
        try:
            line_total = float(ln.get("total") or 0)
        except Exception:
            line_total = unit_price * float(qty)
        item_id_ln = ln.get("id")
        try:
            item_id_ln = int(item_id_ln) if item_id_ln is not None else None
        except Exception:
            item_id_ln = None
        parsed.append(
            {
                "name": nm[:200],
                "qty": qty,
                "unit_price": unit_price,
                "line_total": line_total,
                "item_id": item_id_ln,
            }
        )

    count = round(sum(float(p["qty"]) for p in parsed), STOCK_QTY_DECIMAL_PLACES)

    need = {}
    for p in parsed:
        iid = p["item_id"]
        if iid is None:
            continue
        qv = float(p["qty"])
        need[iid] = round(need.get(iid, 0.0) + qv, STOCK_QTY_DECIMAL_PLACES)

    ensure_shop_pos_sales_inventory_mode_column()
    ensure_shop_pos_sales_receipt_columns()
    ensure_shop_pos_sales_client_txn_column()
    ensure_shop_pos_sales_mpesa_receipt_column()
    ensure_shop_pos_sale_items_portion_unit_cost_column()
    client_txn = (client_txn_id or "").strip()[:64] or None
    mpesa_ref = (mpesa_receipt_number or "").strip()[:32] or None
    sale_sql = """
    INSERT INTO shop_pos_sales
        (shop_id, sale_type, payment_method, cash_amount, mpesa_amount, mpesa_receipt_number, total_amount, item_count, customer_name, customer_phone, employee_id, employee_code, employee_name, receipt_number, receipt_scope_key, receipt_sequence, client_txn_id, credit_due_date, inventory_mode)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    line_sql = """
    INSERT INTO shop_pos_sale_items
        (sale_id, shop_id, item_id, item_name, qty, unit_price, line_total, portion_unit_cost)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    stock_tx_sql = """
    INSERT INTO shop_stock_transactions
        (shop_id, item_id, direction, source, qty, shop_stock_before, shop_stock_after,
         company_stock_before, company_stock_after,
         reason, refunded, refund_amount, payment_status, note, created_by_employee_id)
    VALUES (%s,%s,'out','manual',%s,%s,%s,NULL,NULL,'POS',0,NULL,'paid',%s,%s)
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(
                "SELECT id, receipt_settings_json FROM shops WHERE id=%s LIMIT 1 FOR UPDATE",
                (int(shop_id),),
            )
            shop_row = cur.fetchone()
            if not shop_row:
                return False, "Shop not found.", None, None
            fmt, prefix, start = _effective_receipt_number_settings(shop_row.get("receipt_settings_json"))
            scope_key = _receipt_scope_key(fmt, datetime.now())
            if client_txn:
                cur.execute(
                    """
                    SELECT id, receipt_number
                    FROM shop_pos_sales
                    WHERE shop_id=%s AND client_txn_id=%s
                    LIMIT 1
                    """,
                    (int(shop_id), client_txn),
                )
                existing = cur.fetchone()
                if existing:
                    return True, None, int(existing.get("id") or 0), (existing.get("receipt_number") or "")
            cur.execute(
                """
                SELECT COALESCE(MAX(receipt_sequence), 0) AS mx
                FROM shop_pos_sales
                WHERE shop_id=%s AND receipt_scope_key=%s
                """,
                (int(shop_id), scope_key),
            )
            mx_row = cur.fetchone() or {}
            mx = int(mx_row.get("mx") or 0)
            next_seq = max(start - 1, mx) + 1
            receipt_number = f"{prefix}-{next_seq}"

            # Exactly one inventory pathway per sale — never validate shelf and portions together.
            # Skipped when stock has already been moved upstream (e.g. withhold-POS held-order finalize).
            if skip_stock_deduction:
                pass
            elif mode == "shop":
                for iid in sorted(need.keys()):
                    cur.execute(
                        """
                        SELECT shop_stock_qty, stock_update_enabled
                        FROM shop_items
                        WHERE shop_id=%s AND item_id=%s
                        FOR UPDATE
                        """,
                        (int(shop_id), int(iid)),
                    )
                    si = cur.fetchone()
                    if not si:
                        raise ValueError("POS item is not linked to this shop.")
                    if int(si.get("stock_update_enabled") or 0) != 1:
                        continue
                    before = round(float(si.get("shop_stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
                    if before < need[iid]:
                        raise ValueError(
                            "Not enough stock at the shop for one or more items. Adjust quantities or stock."
                        )
            elif mode in ("kitchen", "both"):
                # Mode "both": POS checkout consumes kitchen portions only; shelf qty is separate (Stock management).
                for iid in sorted(need.keys()):
                    cur.execute(
                        """
                        SELECT stock_update_enabled
                        FROM shop_items
                        WHERE shop_id=%s AND item_id=%s
                        FOR UPDATE
                        """,
                        (int(shop_id), int(iid)),
                    )
                    si = cur.fetchone()
                    if not si:
                        raise ValueError("POS item is not linked to this shop.")
                    if int(si.get("stock_update_enabled") or 0) != 1:
                        continue
                    cur.execute(
                        """
                        SELECT portions_remaining
                        FROM shop_kitchen_portions
                        WHERE shop_id=%s AND item_id=%s
                        FOR UPDATE
                        """,
                        (int(shop_id), int(iid)),
                    )
                    kr = cur.fetchone()
                    rem = int(kr.get("portions_remaining") or 0) if kr else 0
                    if rem < int(need[iid]):
                        raise ValueError(
                            "Not enough kitchen portions for one or more items. Set portions on the shop dashboard or reduce quantities."
                        )

            cur.execute(
                sale_sql,
                (
                    int(shop_id),
                    s_type,
                    pay_method or None,
                    cash_val,
                    mpesa_val,
                    mpesa_ref,
                    amount,
                    count,
                    (customer_name or "").strip()[:190] or None,
                    _stored_customer_phone(customer_phone),
                    int(employee_id) if employee_id is not None else None,
                    (employee_code or "").strip()[:6] or None,
                    (employee_name or "").strip()[:190] or None,
                    receipt_number,
                    scope_key,
                    next_seq,
                    client_txn,
                    due_sql_val,
                    mode,
                ),
            )
            sale_id = int(cur.lastrowid or 0)
            note_base = "POS %s #%s" % (s_type, sale_id)

            for p in parsed:
                portion_uc = None
                if mode in ("kitchen", "both"):
                    portion_uc = _kitchen_portion_price_at_checkout(
                        cur, int(shop_id), p.get("item_id"), p.get("name") or ""
                    )
                cur.execute(
                    line_sql,
                    (
                        sale_id,
                        int(shop_id),
                        p["item_id"],
                        p["name"],
                        p["qty"],
                        p["unit_price"],
                        p["line_total"],
                        portion_uc,
                    ),
                )

                iid = p["item_id"]
                if iid is None:
                    continue
                q = float(p["qty"])
                # Skip movement when stock has already been moved upstream (held-order finalize).
                if skip_stock_deduction:
                    continue
                # Mutually exclusive: shop shelf decrement OR kitchen portions — never both on one sale.
                if mode == "shop":
                    cur.execute(
                        """
                        SELECT shop_stock_qty, stock_update_enabled
                        FROM shop_items
                        WHERE shop_id=%s AND item_id=%s
                        FOR UPDATE
                        """,
                        (int(shop_id), int(iid)),
                    )
                    si = cur.fetchone()
                    if not si or int(si.get("stock_update_enabled") or 0) != 1:
                        continue
                    shop_before = round(float(si.get("shop_stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
                    qshop = round(q, STOCK_QTY_DECIMAL_PLACES)
                    if shop_before < qshop:
                        raise ValueError(
                            "Not enough stock at the shop for one or more items. Adjust quantities or stock."
                        )
                    shop_after = round(shop_before - qshop, STOCK_QTY_DECIMAL_PLACES)
                    cur.execute(
                        "UPDATE shop_items SET shop_stock_qty=%s WHERE shop_id=%s AND item_id=%s",
                        (shop_after, int(shop_id), int(iid)),
                    )
                    note = note_base + " · " + (p["name"][:160] if p.get("name") else "")
                    cur.execute(
                        stock_tx_sql,
                        (
                            int(shop_id),
                            int(iid),
                            qshop,
                            shop_before,
                            shop_after,
                            note,
                            int(employee_id) if employee_id is not None else None,
                        ),
                    )
                    _shop_fifo_after_stock_out(
                        cur, int(shop_id), int(iid), qshop, int(cur.lastrowid or 0)
                    )
                elif mode in ("kitchen", "both"):
                    qk = int(q)
                    cur.execute(
                        """
                        SELECT COALESCE(stock_update_enabled,0) AS sue
                        FROM shop_items
                        WHERE shop_id=%s AND item_id=%s
                        FOR UPDATE
                        """,
                        (int(shop_id), int(iid)),
                    )
                    ks = cur.fetchone() or {}
                    if int(ks.get("sue") or 0) != 1:
                        continue
                    cur.execute(
                        """
                        UPDATE shop_kitchen_portions
                        SET portions_remaining = portions_remaining - %s
                        WHERE shop_id=%s AND item_id=%s AND portions_remaining >= %s
                        """,
                        (qk, int(shop_id), int(iid), qk),
                    )
                    if int(cur.rowcount or 0) < 1:
                        raise ValueError(
                            "Not enough kitchen portions for one or more items. Adjust quantities or kitchen portions."
                        )

            return True, None, sale_id, receipt_number
    except ValueError as e:
        return False, str(e) or "Could not complete sale.", None, None
    except pymysql.Error:
        return False, None, None, None


def get_shop_pos_sale_by_client_txn(shop_id: int, client_txn_id: str) -> Optional[dict]:
    """Returns existing sale row for idempotent replay lookups."""
    txid = (client_txn_id or "").strip()[:64]
    if not txid:
        return None
    ensure_shop_pos_sales_client_txn_column()
    try:
        with get_cursor() as cur:
            cur.execute(
                """
                SELECT id, receipt_number, sale_type, payment_method, total_amount, item_count
                FROM shop_pos_sales
                WHERE shop_id=%s AND client_txn_id=%s
                LIMIT 1
                """,
                (int(shop_id), txid),
            )
            return cur.fetchone()
    except pymysql.Error:
        return None


def get_shop_revenue_analytics(
    shop_id: int, analytics_filter: dict, analytics_scope: str = "general"
):
    """Return revenue aggregates for sale and credit based on selected period."""
    range_where, range_params = _analytics_where_clause(analytics_filter, "")
    scope_where, scope_params = _analytics_receipt_scope_clause(analytics_scope, "")
    where_sql = f"shop_id=%s AND {range_where} AND {scope_where}"
    params = [int(shop_id)] + list(range_params) + list(scope_params)
    by_type_sql = f"""
    SELECT sale_type, COUNT(*) AS tx_count, COALESCE(SUM(total_amount), 0) AS total_amount
    FROM shop_pos_sales
    WHERE {where_sql}
    GROUP BY sale_type
    """
    by_day_sql = f"""
    SELECT
        DATE(created_at) AS day,
        COALESCE(SUM(CASE WHEN sale_type='sale' THEN total_amount ELSE 0 END), 0) AS sale_amount,
        COALESCE(SUM(CASE WHEN sale_type='credit' THEN total_amount ELSE 0 END), 0) AS credit_amount,
        COUNT(*) AS tx_count
    FROM shop_pos_sales
    WHERE {where_sql}
    GROUP BY DATE(created_at)
    ORDER BY day DESC
    LIMIT 31
    """
    out = {
        "sale": {"amount": 0.0, "count": 0},
        "credit": {"amount": 0.0, "count": 0},
        "total_amount": 0.0,
        "total_count": 0,
        "daily": [],
    }
    try:
        with get_cursor() as cur:
            cur.execute(by_type_sql, tuple(params))
            rows = cur.fetchall() or []
            for r in rows:
                k = (r.get("sale_type") or "").strip().lower()
                if k not in ("sale", "credit"):
                    continue
                amt = float(r.get("total_amount") or 0)
                cnt = int(r.get("tx_count") or 0)
                out[k] = {"amount": amt, "count": cnt}
                out["total_amount"] += amt
                out["total_count"] += cnt

            cur.execute(by_day_sql, tuple(params))
            drows = cur.fetchall() or []
            out["daily"] = [
                {
                    "day": str(r.get("day") or ""),
                    "sale_amount": float(r.get("sale_amount") or 0),
                    "credit_amount": float(r.get("credit_amount") or 0),
                    "total_amount": float(r.get("sale_amount") or 0) + float(r.get("credit_amount") or 0),
                    "tx_count": int(r.get("tx_count") or 0),
                }
                for r in drows
            ]
    except pymysql.Error:
        return out
    return out


def _receipt_mark_status_sort_sql(alias: str = "s") -> str:
    """SQL expression: pending → partial_return → returned → cancelled → confirmed."""
    col = f"{alias}.receipt_mark_status" if alias else "receipt_mark_status"
    return f"""CASE COALESCE({col}, 'pending')
        WHEN 'pending' THEN 1
        WHEN 'partial_return' THEN 2
        WHEN 'returned' THEN 3
        WHEN 'cancelled' THEN 4
        WHEN 'confirmed' THEN 5
        ELSE 6
    END"""


def list_shop_pos_sales_receipt_rows(
    shop_id: int,
    analytics_filter: Optional[dict],
    *,
    sale_id_search: Optional[str] = None,
    limit: int = 5000,
) -> list:
    """POS checkout rows for receipt register UI (sale + credit), scoped by date range."""
    ensure_shop_pos_sales_inventory_mode_column()
    ensure_shop_pos_sales_receipt_columns()
    af = analytics_filter if isinstance(analytics_filter, dict) else {}
    rng_sql, rng_params = _analytics_where_clause(af, "s")
    params: list = [int(shop_id)]
    where_parts = ["s.shop_id = %s", rng_sql]
    params.extend(rng_params)

    raw_q = str(sale_id_search or "").strip()
    if raw_q:
        digits = re.sub(r"\D", "", raw_q)
        needle = digits if digits else re.sub(r"\s+", "", raw_q)
        amount_needle = raw_q.replace(",", "").strip()
        q_parts = []
        q_params: list = []
        if needle:
            q_parts.append("(CAST(s.id AS CHAR) LIKE %s OR COALESCE(s.receipt_number, '') LIKE %s)")
            like = "%" + needle + "%"
            q_params.extend([like, like])
        if amount_needle:
            q_parts.append("CAST(s.total_amount AS CHAR) LIKE %s")
            q_params.append("%" + amount_needle + "%")
        if q_parts:
            where_parts.append("(" + " OR ".join(q_parts) + ")")
            params.extend(q_params)

    where_sql = " AND ".join(where_parts)
    lim = max(1, min(int(limit or 5000), 20000))

    sql = f"""
    SELECT
        s.id AS sale_id,
        s.shop_id,
        s.sale_type,
        s.payment_method,
        s.total_amount,
        s.cash_amount,
        s.mpesa_amount,
        s.credit_paid_amount,
        s.credit_status,
        s.item_count,
        s.customer_name,
        s.customer_phone,
        s.employee_id,
        s.employee_code,
        s.employee_name,
        s.receipt_number,
        s.receipt_mark_status,
        COALESCE(s.reprint_count, 0) AS reprint_count,
        s.created_at,
        s.inventory_mode
    FROM shop_pos_sales s
    WHERE {where_sql}
    ORDER BY {_receipt_mark_status_sort_sql("s")}, s.created_at DESC, s.id DESC
    LIMIT %s
    """
    params.append(lim)
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
    except pymysql.Error:
        return []

    out = []
    for r in rows:
        rr = dict(r)
        rr["sale_id"] = int(rr.get("sale_id") or 0)
        for k in (
            "total_amount",
            "cash_amount",
            "mpesa_amount",
            "credit_paid_amount",
        ):
            try:
                rr[k] = float(rr.get(k) or 0)
            except (TypeError, ValueError):
                rr[k] = 0.0
        try:
            rr["item_count"] = int(rr.get("item_count") or 0)
        except (TypeError, ValueError):
            rr["item_count"] = 0
        try:
            rr["reprint_count"] = int(rr.get("reprint_count") or 0)
        except (TypeError, ValueError):
            rr["reprint_count"] = 0
        cat = rr.get("created_at")
        if hasattr(cat, "isoformat"):
            rr["created_at_iso"] = cat.isoformat(timespec="seconds")
        elif cat is None:
            rr["created_at_iso"] = ""
        else:
            rr["created_at_iso"] = str(cat)
        out.append(rr)
    return out


def record_shop_pos_sale_reprint(*, shop_id: int, sale_id: int) -> tuple[bool, Optional[str], int]:
    """Increment persisted reprint counter for a POS receipt."""
    ensure_shop_pos_sales_receipt_columns()
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(
                """
                UPDATE shop_pos_sales
                SET reprint_count = COALESCE(reprint_count, 0) + 1
                WHERE id=%s AND shop_id=%s
                """,
                (int(sale_id), int(shop_id)),
            )
            if int(cur.rowcount or 0) <= 0:
                return False, "Receipt not found.", 0
            cur.execute(
                """
                SELECT COALESCE(reprint_count, 0) AS reprint_count
                FROM shop_pos_sales
                WHERE id=%s AND shop_id=%s
                LIMIT 1
                """,
                (int(sale_id), int(shop_id)),
            )
            row = cur.fetchone() or {}
            return True, None, int(row.get("reprint_count") or 0)
    except pymysql.Error:
        return False, None, 0


def list_all_shops_pos_sales_receipt_rows(
    analytics_filter: Optional[dict],
    *,
    shop_id: Optional[int] = None,
    sale_id_search: Optional[str] = None,
    limit: int = 8000,
) -> list:
    """All shops: POS rows for HQ receipt register (optional shop filter)."""
    ensure_shop_pos_sales_inventory_mode_column()
    ensure_shop_pos_sales_receipt_columns()
    af = analytics_filter if isinstance(analytics_filter, dict) else {}
    rng_sql, rng_params = _analytics_where_clause(af, "s")
    params: list = []
    where_parts = [rng_sql]
    params.extend(rng_params)

    if shop_id is not None and int(shop_id) > 0:
        where_parts.append("s.shop_id = %s")
        params.append(int(shop_id))

    raw_q = str(sale_id_search or "").strip()
    if raw_q:
        digits = re.sub(r"\D", "", raw_q)
        needle = digits if digits else re.sub(r"\s+", "", raw_q)
        amount_needle = raw_q.replace(",", "").strip()
        q_parts = []
        q_params: list = []
        if needle:
            q_parts.append("(CAST(s.id AS CHAR) LIKE %s OR COALESCE(s.receipt_number, '') LIKE %s)")
            like = "%" + needle + "%"
            q_params.extend([like, like])
        if amount_needle:
            q_parts.append("CAST(s.total_amount AS CHAR) LIKE %s")
            q_params.append("%" + amount_needle + "%")
        if q_parts:
            where_parts.append("(" + " OR ".join(q_parts) + ")")
            params.extend(q_params)

    where_sql = " AND ".join(where_parts)
    lim = max(1, min(int(limit or 8000), 30000))

    sql = f"""
    SELECT
        s.id AS sale_id,
        s.shop_id,
        COALESCE(sh.shop_name, '') AS shop_name,
        COALESCE(sh.shop_code, '') AS shop_code,
        s.sale_type,
        s.payment_method,
        s.total_amount,
        s.cash_amount,
        s.mpesa_amount,
        s.credit_paid_amount,
        s.credit_status,
        s.item_count,
        s.customer_name,
        s.customer_phone,
        s.employee_id,
        s.employee_code,
        s.employee_name,
        s.receipt_number,
        s.receipt_mark_status,
        COALESCE(s.reprint_count, 0) AS reprint_count,
        s.created_at,
        s.inventory_mode
    FROM shop_pos_sales s
    LEFT JOIN shops sh ON sh.id = s.shop_id
    WHERE {where_sql}
    ORDER BY {_receipt_mark_status_sort_sql("s")}, s.created_at DESC, s.id DESC
    LIMIT %s
    """
    params.append(lim)
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
    except pymysql.Error:
        return []

    out = []
    for r in rows:
        rr = dict(r)
        rr["sale_id"] = int(rr.get("sale_id") or 0)
        for k in (
            "total_amount",
            "cash_amount",
            "mpesa_amount",
            "credit_paid_amount",
        ):
            try:
                rr[k] = float(rr.get(k) or 0)
            except (TypeError, ValueError):
                rr[k] = 0.0
        try:
            rr["item_count"] = int(rr.get("item_count") or 0)
        except (TypeError, ValueError):
            rr["item_count"] = 0
        try:
            rr["reprint_count"] = int(rr.get("reprint_count") or 0)
        except (TypeError, ValueError):
            rr["reprint_count"] = 0
        cat = rr.get("created_at")
        if hasattr(cat, "isoformat"):
            rr["created_at_iso"] = cat.isoformat(timespec="seconds")
        elif cat is None:
            rr["created_at_iso"] = ""
        else:
            rr["created_at_iso"] = str(cat)
        out.append(rr)
    return out


def _parse_credit_payment_note_method(note: Optional[str]) -> str:
    """Infer payment method from shop_credit_payments.note (cash, mpesa, bank, other)."""
    raw_note = (note or "").strip()
    note_lower = raw_note.lower()
    marker = "method="
    if marker in note_lower:
        parsed = note_lower.split(marker, 1)[1].split()[0].strip(" ,.;:|")
        if parsed in ("cash", "mpesa", "bank", "other"):
            return parsed
    if "mpesa" in note_lower:
        return "mpesa"
    if "cash" in note_lower:
        return "cash"
    if "bank" in note_lower:
        return "bank"
    return "other"


def _extract_mpesa_reference_from_note(note: Optional[str]) -> str:
    """Extract Safaricom M-Pesa receipt code from payment note text."""
    raw = (note or "").strip()
    if not raw:
        return ""
    m = re.search(
        r"(?:m-?pesa\s*ref(?:erence)?\s*[:#]\s*)([A-Z0-9]{6,14})",
        raw,
        re.IGNORECASE,
    )
    if m:
        return m.group(1).strip().upper()
    return ""


def _income_row_created_iso(created_at) -> str:
    if hasattr(created_at, "isoformat"):
        return created_at.isoformat(timespec="seconds")
    if created_at is None:
        return ""
    return str(created_at)


def list_all_shops_income_payments(
    analytics_filter: Optional[dict],
    *,
    shop_id: Optional[int] = None,
    payment_method: Optional[str] = None,
    limit: int = 8000,
) -> dict:
    """Cash and M-Pesa income across all shops: POS sales + credit settlements."""
    ensure_shop_credit_payments_schema()
    ensure_shop_pos_sales_receipt_columns()
    ensure_shop_pos_sales_mpesa_receipt_column()
    af = analytics_filter if isinstance(analytics_filter, dict) else {}
    rng_sql, rng_params = _analytics_where_clause(af, "s")
    params: list = []
    where_parts = [
        "s.sale_type = 'sale'",
        "s.payment_method IN ('cash', 'mpesa', 'both')",
        rng_sql,
    ]
    params.extend(rng_params)
    if shop_id is not None and int(shop_id) > 0:
        where_parts.append("s.shop_id = %s")
        params.append(int(shop_id))
    where_sql = " AND ".join(where_parts)
    lim = max(1, min(int(limit or 8000), 30000))

    pos_sql = f"""
    SELECT
        s.id AS row_id,
        s.shop_id,
        COALESCE(sh.shop_name, '') AS shop_name,
        COALESCE(sh.shop_code, '') AS shop_code,
        s.payment_method,
        s.cash_amount,
        s.mpesa_amount,
        s.mpesa_receipt_number,
        s.total_amount,
        s.receipt_number,
        s.customer_name,
        s.employee_name,
        s.created_at
    FROM shop_pos_sales s
    LEFT JOIN shops sh ON sh.id = s.shop_id
    WHERE {where_sql}
    ORDER BY s.created_at DESC, s.id DESC
    LIMIT %s
    """
    pos_params = list(params) + [lim]
    rows: list = []
    try:
        with get_cursor() as cur:
            cur.execute(pos_sql, tuple(pos_params))
            for r in cur.fetchall() or []:
                pm = (r.get("payment_method") or "").strip().lower()
                cash_amt = float(r.get("cash_amount") or 0)
                mpesa_amt = float(r.get("mpesa_amount") or 0)
                total_amt = float(r.get("total_amount") or 0)
                mpesa_ref = (r.get("mpesa_receipt_number") or "").strip()
                rows.append(
                    {
                        "income_source": "pos_sale",
                        "row_id": int(r.get("row_id") or 0),
                        "shop_id": int(r.get("shop_id") or 0),
                        "shop_name": r.get("shop_name") or "",
                        "shop_code": r.get("shop_code") or "",
                        "payment_method": pm,
                        "cash_amount": cash_amt,
                        "mpesa_amount": mpesa_amt,
                        "total_amount": total_amt,
                        "mpesa_reference": mpesa_ref,
                        "reference": (r.get("receipt_number") or "").strip()
                        or f"Sale #{int(r.get('row_id') or 0)}",
                        "customer_name": (r.get("customer_name") or "").strip() or "—",
                        "employee_name": (r.get("employee_name") or "").strip() or "—",
                        "created_at": r.get("created_at"),
                        "created_at_iso": _income_row_created_iso(r.get("created_at")),
                    }
                )
    except pymysql.Error:
        rows = []

    credit_where = ["1=1"]
    credit_params: list = []
    if af:
        rw, rp = _analytics_where_clause(af, "scp")
        credit_where.append(f"({rw})")
        credit_params.extend(rp)
    if shop_id is not None and int(shop_id) > 0:
        credit_where.append("scp.shop_id = %s")
        credit_params.append(int(shop_id))
    credit_sql = f"""
    SELECT
        scp.id AS row_id,
        scp.shop_id,
        COALESCE(sh.shop_name, '') AS shop_name,
        COALESCE(sh.shop_code, '') AS shop_code,
        scp.amount,
        scp.note,
        COALESCE(NULLIF(scp.customer_name, ''), 'WALK IN') AS customer_name,
        scp.created_at
    FROM shop_credit_payments scp
    LEFT JOIN shops sh ON sh.id = scp.shop_id
    WHERE {' AND '.join(credit_where)}
    ORDER BY scp.created_at DESC, scp.id DESC
    LIMIT %s
    """
    credit_params.append(lim)
    try:
        with get_cursor() as cur:
            cur.execute(credit_sql, tuple(credit_params))
            for r in cur.fetchall() or []:
                method = _parse_credit_payment_note_method(r.get("note"))
                if method not in ("cash", "mpesa"):
                    continue
                amt = float(r.get("amount") or 0)
                if amt <= 0:
                    continue
                cash_amt = amt if method == "cash" else 0.0
                mpesa_amt = amt if method == "mpesa" else 0.0
                mpesa_ref = _extract_mpesa_reference_from_note(r.get("note"))
                rows.append(
                    {
                        "income_source": "credit_payment",
                        "row_id": int(r.get("row_id") or 0),
                        "shop_id": int(r.get("shop_id") or 0),
                        "shop_name": r.get("shop_name") or "",
                        "shop_code": r.get("shop_code") or "",
                        "payment_method": method,
                        "cash_amount": cash_amt,
                        "mpesa_amount": mpesa_amt,
                        "total_amount": amt,
                        "mpesa_reference": mpesa_ref,
                        "reference": f"Credit payment #{int(r.get('row_id') or 0)}",
                        "customer_name": r.get("customer_name") or "WALK IN",
                        "employee_name": "—",
                        "created_at": r.get("created_at"),
                        "created_at_iso": _income_row_created_iso(r.get("created_at")),
                    }
                )
    except pymysql.Error:
        pass

    pm_filter = (payment_method or "").strip().lower()
    if pm_filter == "cash":
        rows = [
            r
            for r in rows
            if float(r.get("cash_amount") or 0) > 0
            and (r.get("payment_method") or "") in ("cash", "both")
        ]
    elif pm_filter == "mpesa":
        rows = [
            r
            for r in rows
            if float(r.get("mpesa_amount") or 0) > 0
            and (r.get("payment_method") or "") in ("mpesa", "both")
        ]

    def _sort_key(row: dict):
        cat = row.get("created_at")
        if hasattr(cat, "timestamp"):
            return (cat.timestamp(), int(row.get("row_id") or 0))
        return (0.0, int(row.get("row_id") or 0))

    rows.sort(key=_sort_key, reverse=True)
    rows = rows[:lim]

    total_cash = sum(float(r.get("cash_amount") or 0) for r in rows)
    total_mpesa = sum(float(r.get("mpesa_amount") or 0) for r in rows)
    return {
        "rows": rows,
        "total_cash": total_cash,
        "total_mpesa": total_mpesa,
        "total_income": total_cash + total_mpesa,
        "count": len(rows),
    }


def bulk_mark_shop_pos_receipts(
    *,
    sale_ids: list[int],
    mark_status: str,
    shop_id: Optional[int] = None,
) -> int:
    """Mark multiple receipts with one status. Returns affected rows."""
    ensure_shop_pos_sales_receipt_columns()
    status = (mark_status or "").strip().lower()
    if status not in ("confirmed", "cancelled", "returned"):
        return 0
    if status == "returned":
        ensure_shop_kitchen_portions_schema()
    uniq_ids: list[int] = []
    for raw in sale_ids or []:
        try:
            v = int(raw)
        except (TypeError, ValueError):
            continue
        if v <= 0 or v in uniq_ids:
            continue
        uniq_ids.append(v)
        if len(uniq_ids) >= 1000:
            break
    if not uniq_ids:
        return 0

    def _restock_for_return(cur, sid: int, sid_shop: int, inventory_mode: str) -> None:
        cur.execute(
            """
            SELECT item_id, qty
            FROM shop_pos_sale_items
            WHERE sale_id=%s AND shop_id=%s
            """,
            (int(sid), int(sid_shop)),
        )
        lines = cur.fetchall() or []
        needed: dict[int, float] = {}
        for ln in lines:
            try:
                iid = int(ln.get("item_id") or 0)
            except (TypeError, ValueError):
                iid = 0
            try:
                qty = round(float(ln.get("qty") or 0), STOCK_QTY_DECIMAL_PLACES)
            except (TypeError, ValueError):
                qty = 0.0
            if iid <= 0 or qty <= 0:
                continue
            needed[iid] = round(needed.get(iid, 0.0) + qty, STOCK_QTY_DECIMAL_PLACES)
        if not needed:
            return
        mode = (inventory_mode or "shop").strip().lower()
        for iid, qty in needed.items():
            cur.execute(
                """
                SELECT COALESCE(stock_update_enabled,0) AS sue, COALESCE(shop_stock_qty,0) AS stock_qty
                FROM shop_items
                WHERE shop_id=%s AND item_id=%s
                FOR UPDATE
                """,
                (int(sid_shop), int(iid)),
            )
            si = cur.fetchone() or {}
            if int(si.get("sue") or 0) != 1:
                continue
            if mode == "shop":
                cur.execute(
                    """
                    UPDATE shop_items
                    SET shop_stock_qty = COALESCE(shop_stock_qty,0) + %s
                    WHERE shop_id=%s AND item_id=%s
                    """,
                    (qty, int(sid_shop), int(iid)),
                )
            elif mode in ("kitchen", "both"):
                cur.execute(
                    """
                    INSERT INTO shop_kitchen_portions (shop_id, item_id, portions_remaining)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE portions_remaining = portions_remaining + VALUES(portions_remaining)
                    """,
                    (int(sid_shop), int(iid), int(qty)),
                )

    try:
        with get_cursor(commit=True) as cur:
            placeholders = ", ".join(["%s"] * len(uniq_ids))
            sel_sql = f"""
            SELECT id, shop_id, COALESCE(inventory_mode, 'shop') AS inventory_mode,
                   COALESCE(receipt_mark_status, 'pending') AS receipt_mark_status,
                   COALESCE(receipt_return_restocked, 0) AS receipt_return_restocked
            FROM shop_pos_sales
            WHERE id IN ({placeholders})
            """
            sel_params: list = list(uniq_ids)
            if shop_id is not None and int(shop_id) > 0:
                sel_sql += " AND shop_id=%s"
                sel_params.append(int(shop_id))
            cur.execute(sel_sql, tuple(sel_params))
            rows = cur.fetchall() or []
            if not rows:
                return 0

            affected = 0
            for r in rows:
                sid = int(r.get("id") or 0)
                sid_shop = int(r.get("shop_id") or 0)
                prev_status = (r.get("receipt_mark_status") or "pending").strip().lower()
                restocked = int(r.get("receipt_return_restocked") or 0) == 1
                inventory_mode = (r.get("inventory_mode") or "shop").strip().lower()
                if sid <= 0 or sid_shop <= 0:
                    continue
                if status == "returned" and not restocked:
                    _restock_for_return(cur, sid, sid_shop, inventory_mode)
                    cur.execute(
                        """
                        UPDATE shop_pos_sales
                        SET receipt_mark_status=%s, receipt_return_restocked=1
                        WHERE id=%s AND shop_id=%s
                        """,
                        (status, sid, sid_shop),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE shop_pos_sales
                        SET receipt_mark_status=%s
                        WHERE id=%s AND shop_id=%s
                        """,
                        (status, sid, sid_shop),
                    )
                if int(cur.rowcount or 0) > 0 or prev_status != status:
                    affected += 1
            return affected
    except pymysql.Error:
        return 0


def get_it_support_revenue_analytics(
    analytics_filter: dict,
    analytics_scope: str = "general",
    shop_id: Optional[int] = None,
    *,
    transactions_limit: int = 150,
    transactions_offset: int = 0,
    include_transactions: bool = True,
):
    """Revenue analytics across all shops for IT support/super admin.

    Loads aggregates and shop/day breakdowns always. Transaction rows are paginated
    (default 150) so bulk periods stay fast; use transactions_offset for "load more".
    """
    try:
        tx_limit = max(1, min(500, int(transactions_limit)))
    except (TypeError, ValueError):
        tx_limit = 150
    try:
        tx_offset = max(0, int(transactions_offset))
    except (TypeError, ValueError):
        tx_offset = 0

    range_where, range_params = _analytics_where_clause(analytics_filter, "sps")
    scope_where, scope_params = _analytics_receipt_scope_clause(analytics_scope, "sps")
    shop_clause, shop_params = _analytics_shop_filter_clause("sps", shop_id)
    shop_row_filter, shop_row_params = _analytics_shops_row_filter(shop_id, "s")
    where_sql = f"{range_where} AND {scope_where}{shop_clause}"
    params = list(range_params) + list(scope_params) + list(shop_params)
    totals_sql = f"""
    SELECT
        COUNT(*) AS tx_count,
        COALESCE(SUM(CASE WHEN sps.sale_type='sale' THEN sps.total_amount ELSE 0 END), 0) AS sale_amount,
        COALESCE(SUM(CASE WHEN sps.sale_type='credit' THEN sps.total_amount ELSE 0 END), 0) AS credit_amount,
        COALESCE(SUM(sps.cash_amount), 0) AS cash_paid_total,
        COALESCE(SUM(sps.mpesa_amount), 0) AS mpesa_paid_total
    FROM shop_pos_sales sps
    WHERE {where_sql}
    """
    by_shop_sql = f"""
    SELECT
        s.id AS shop_id,
        s.shop_name,
        s.shop_code,
        COUNT(sps.id) AS tx_count,
        COALESCE(SUM(CASE WHEN sps.sale_type='sale' THEN sps.total_amount ELSE 0 END), 0) AS sale_amount,
        COALESCE(SUM(CASE WHEN sps.sale_type='credit' THEN sps.total_amount ELSE 0 END), 0) AS credit_amount,
        COALESCE(SUM(sps.cash_amount), 0) AS cash_paid,
        COALESCE(SUM(sps.mpesa_amount), 0) AS mpesa_paid,
        COALESCE(SUM(CASE WHEN sps.sale_type IN ('sale','credit') THEN sps.total_amount ELSE 0 END), 0) AS total_amount
    FROM shops s
    LEFT JOIN shop_pos_sales sps ON sps.shop_id = s.id AND ({where_sql})
    {shop_row_filter}
    GROUP BY s.id, s.shop_name, s.shop_code
    ORDER BY total_amount DESC, s.shop_name ASC
    LIMIT 500
    """
    by_day_sql = f"""
    SELECT
        DATE(sps.created_at) AS day,
        COUNT(*) AS tx_count,
        COALESCE(SUM(CASE WHEN sps.sale_type='sale' THEN sps.total_amount ELSE 0 END), 0) AS sale_amount,
        COALESCE(SUM(CASE WHEN sps.sale_type='credit' THEN sps.total_amount ELSE 0 END), 0) AS credit_amount
    FROM shop_pos_sales sps
    WHERE {where_sql}
    GROUP BY DATE(sps.created_at)
    ORDER BY day DESC
    LIMIT 31
    """
    transactions_sql = f"""
    SELECT
        sps.id,
        sps.shop_id,
        s.shop_name,
        s.shop_code,
        sps.sale_type,
        sps.payment_method,
        sps.cash_amount,
        sps.mpesa_amount,
        sps.total_amount,
        sps.item_count,
        sps.customer_name,
        sps.customer_phone,
        sps.employee_name,
        sps.employee_code,
        sps.created_at
    FROM shop_pos_sales sps
    LEFT JOIN shops s ON s.id = sps.shop_id
    WHERE {where_sql}
    ORDER BY sps.created_at DESC, sps.id DESC
    LIMIT %s OFFSET %s
    """
    out = {
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
            "limit": tx_limit,
            "offset": tx_offset,
            "loaded_count": 0,
            "total_count": 0,
            "has_more": False,
        },
    }
    try:
        with get_cursor() as cur:
            cur.execute(totals_sql, tuple(params))
            t = cur.fetchone() or {}
            out["tx_count"] = int(t.get("tx_count") or 0)
            out["sale_amount"] = float(t.get("sale_amount") or 0)
            out["credit_amount"] = float(t.get("credit_amount") or 0)
            out["total_amount"] = out["sale_amount"] + out["credit_amount"]
            out["cash_paid_total"] = float(t.get("cash_paid_total") or 0)
            out["mpesa_paid_total"] = float(t.get("mpesa_paid_total") or 0)
            out["transactions_meta"]["total_count"] = out["tx_count"]

            cur.execute(by_shop_sql, tuple(params + shop_row_params))
            expense_by_shop = _expenditure_total_by_shop_map(analytics_filter, shop_id)
            out["shops"] = [
                {
                    "shop_id": r.get("shop_id"),
                    "shop_name": r.get("shop_name") or "Shop",
                    "shop_code": r.get("shop_code") or "",
                    "tx_count": int(r.get("tx_count") or 0),
                    "sale_amount": float(r.get("sale_amount") or 0),
                    "credit_amount": float(r.get("credit_amount") or 0),
                    "cash_paid": float(r.get("cash_paid") or 0),
                    "mpesa_paid": float(r.get("mpesa_paid") or 0),
                    "expense_amount": float(
                        expense_by_shop.get(int(r.get("shop_id") or 0), 0.0)
                    ),
                    "total_amount": float(r.get("sale_amount") or 0)
                    + float(r.get("credit_amount") or 0),
                }
                for r in (cur.fetchall() or [])
            ]

            cur.execute(by_day_sql, tuple(params))
            out["daily"] = [
                {
                    "day": str(r.get("day") or ""),
                    "tx_count": int(r.get("tx_count") or 0),
                    "sale_amount": float(r.get("sale_amount") or 0),
                    "credit_amount": float(r.get("credit_amount") or 0),
                    "total_amount": float(r.get("sale_amount") or 0)
                    + float(r.get("credit_amount") or 0),
                }
                for r in (cur.fetchall() or [])
            ]

            if include_transactions:
                cur.execute(transactions_sql, tuple(params + [tx_limit, tx_offset]))
                out["transactions"] = [
                    {
                        "id": int(r.get("id") or 0),
                        "shop_id": r.get("shop_id"),
                        "shop_name": r.get("shop_name") or "Shop",
                        "shop_code": r.get("shop_code") or "",
                        "sale_type": (r.get("sale_type") or "").strip().lower(),
                        "payment_method": (r.get("payment_method") or "").strip().lower(),
                        "cash_amount": float(r.get("cash_amount") or 0),
                        "mpesa_amount": float(r.get("mpesa_amount") or 0),
                        "total_amount": float(r.get("total_amount") or 0),
                        "item_count": int(r.get("item_count") or 0),
                        "customer_name": (r.get("customer_name") or "").strip(),
                        "customer_phone": (r.get("customer_phone") or "").strip(),
                        "employee_name": (r.get("employee_name") or "").strip(),
                        "employee_code": (r.get("employee_code") or "").strip(),
                        "created_at": r.get("created_at"),
                    }
                    for r in (cur.fetchall() or [])
                ]
            loaded = len(out["transactions"])
            out["transactions_meta"] = {
                "limit": tx_limit,
                "offset": tx_offset,
                "loaded_count": tx_offset + loaded,
                "total_count": out["tx_count"],
                "has_more": (tx_offset + loaded) < out["tx_count"],
            }
    except pymysql.Error:
        return out
    return out


def get_it_support_item_analytics(
    analytics_filter: dict,
    analytics_scope: str = "general",
    top_items_limit: int = 100,
    shop_id: Optional[int] = None,
    *,
    lines_limit: int = 150,
    lines_offset: int = 0,
    include_lines: bool = True,
):
    """Item analytics across all shops for IT support/super admin.

    ``top_items_limit`` caps the grouped SKU list (defaults to 100 for dashboards).
    Line rows are paginated (default 150) for fast bulk periods.
    """
    try:
        lines_lim = max(1, min(500, int(lines_limit)))
    except (TypeError, ValueError):
        lines_lim = 150
    try:
        lines_off = max(0, int(lines_offset))
    except (TypeError, ValueError):
        lines_off = 0
    try:
        tl_raw = int(top_items_limit) if top_items_limit is not None else 100
    except (TypeError, ValueError):
        tl_raw = 100
    no_limit = tl_raw <= 0
    lim = max(1, min(50000, tl_raw)) if not no_limit else 0
    range_where, range_params = _analytics_where_clause(analytics_filter, "s")
    scope_where, scope_params = _analytics_receipt_scope_clause(analytics_scope, "s")
    shop_clause, shop_params = _analytics_shop_filter_clause("s", shop_id)
    shop_row_filter, shop_row_params = _analytics_shops_row_filter(shop_id, "sh")
    where_sql = f"{range_where} AND {scope_where}{shop_clause}"
    params = list(range_params) + list(scope_params) + list(shop_params)

    # Two aggregations merged in Python: avoids a single GROUP BY + LEFT JOIN shape that can
    # fail or mis-aggregate under strict SQL modes; matches lines with catalog id directly,
    # then adds lines with NULL item_id resolved by normalized item name.
    join_name_catalog = """
    INNER JOIN (
        SELECT MIN(i.id) AS id, LOWER(TRIM(i.name)) AS nm
        FROM items i
        WHERE i.status = 'active' AND COALESCE(i.stock_update_enabled, 0) = 1
        GROUP BY LOWER(TRIM(i.name))
    ) im ON im.nm = LOWER(TRIM(COALESCE(si.item_name, '')))
        AND LENGTH(TRIM(COALESCE(si.item_name, ''))) > 0
    """
    top_by_id_sql = f"""
    SELECT
        si.item_id AS item_id,
        MAX(COALESCE(si.item_name, '')) AS item_name,
        COALESCE(SUM(si.qty), 0) AS qty_sold,
        COALESCE(SUM(si.line_total), 0) AS revenue,
        COUNT(*) AS line_count
    FROM shop_pos_sale_items si
    JOIN shop_pos_sales s ON s.id = si.sale_id
    WHERE {where_sql} AND si.item_id IS NOT NULL AND si.item_id > 0
    GROUP BY si.item_id
    """
    top_by_resolved_name_sql = f"""
    SELECT
        im.id AS item_id,
        MAX(COALESCE(si.item_name, '')) AS item_name,
        COALESCE(SUM(si.qty), 0) AS qty_sold,
        COALESCE(SUM(si.line_total), 0) AS revenue,
        COUNT(*) AS line_count
    FROM shop_pos_sale_items si
    JOIN shop_pos_sales s ON s.id = si.sale_id
    {join_name_catalog}
    WHERE {where_sql} AND (si.item_id IS NULL OR si.item_id = 0)
    GROUP BY im.id
    """
    totals_sql = f"""
    SELECT
        COALESCE(SUM(si.qty), 0) AS total_qty,
        COALESCE(SUM(si.line_total), 0) AS total_revenue,
        COUNT(*) AS line_count,
        COUNT(DISTINCT COALESCE(si.item_id, CONCAT('n:', si.item_name))) AS distinct_items
    FROM shop_pos_sale_items si
    JOIN shop_pos_sales s ON s.id = si.sale_id
    WHERE {where_sql}
    """
    by_shop_sql = f"""
    SELECT
        sh.id AS shop_id,
        sh.shop_name,
        sh.shop_code,
        COALESCE(SUM(si.qty), 0) AS total_qty,
        COALESCE(SUM(si.line_total), 0) AS total_revenue,
        COUNT(*) AS line_count
    FROM shops sh
    LEFT JOIN shop_pos_sales s ON s.shop_id = sh.id AND ({where_sql})
    LEFT JOIN shop_pos_sale_items si ON si.sale_id = s.id
    {shop_row_filter}
    GROUP BY sh.id, sh.shop_name, sh.shop_code
    ORDER BY total_revenue DESC, sh.shop_name ASC
    LIMIT 500
    """
    lines_sql = f"""
    SELECT
        si.id,
        si.sale_id,
        si.shop_id,
        sh.shop_name,
        sh.shop_code,
        si.item_id,
        si.item_name,
        si.qty,
        si.unit_price,
        si.line_total,
        s.sale_type,
        s.employee_name,
        s.employee_code,
        s.customer_name,
        s.customer_phone,
        s.created_at
    FROM shop_pos_sale_items si
    JOIN shop_pos_sales s ON s.id = si.sale_id
    LEFT JOIN shops sh ON sh.id = si.shop_id
    WHERE {where_sql}
    ORDER BY s.created_at DESC, si.id DESC
    LIMIT %s OFFSET %s
    """

    out = {
        "total_qty": 0,
        "total_revenue": 0.0,
        "line_count": 0,
        "distinct_items": 0,
        "top_items": [],
        "shops": [],
        "lines": [],
        "lines_meta": {
            "limit": lines_lim,
            "offset": lines_off,
            "loaded_count": 0,
            "total_count": 0,
            "has_more": False,
        },
    }
    try:
        with get_cursor() as cur:
            cur.execute(totals_sql, tuple(params))
            t = cur.fetchone() or {}
            out["total_qty"] = int(t.get("total_qty") or 0)
            out["total_revenue"] = float(t.get("total_revenue") or 0)
            out["line_count"] = int(t.get("line_count") or 0)
            out["distinct_items"] = int(t.get("distinct_items") or 0)
            out["lines_meta"]["total_count"] = out["line_count"]

            merged: Dict[int, Dict[str, Any]] = {}

            def _merge_top_row(row: dict) -> None:
                try:
                    iid = int(row.get("item_id") or 0)
                except Exception:
                    return
                if iid <= 0:
                    return
                qty = int(row.get("qty_sold") or 0)
                rev = float(row.get("revenue") or 0)
                lc = int(row.get("line_count") or 0)
                nm = (row.get("item_name") or "").strip() or "Item"
                if iid not in merged:
                    merged[iid] = {
                        "item_id": iid,
                        "item_name": nm,
                        "qty_sold": 0,
                        "revenue": 0.0,
                        "line_count": 0,
                    }
                merged[iid]["qty_sold"] += qty
                merged[iid]["revenue"] += rev
                merged[iid]["line_count"] += lc
                if nm and nm != "Item":
                    merged[iid]["item_name"] = nm

            cur.execute(top_by_id_sql, tuple(params))
            for rr in cur.fetchall() or []:
                _merge_top_row(rr)
            cur.execute(top_by_resolved_name_sql, tuple(params))
            for rr in cur.fetchall() or []:
                _merge_top_row(rr)
            rows_sorted = sorted(
                merged.values(),
                key=lambda x: (-int(x.get("qty_sold") or 0), -float(x.get("revenue") or 0.0)),
            )
            if not no_limit:
                rows_sorted = rows_sorted[:lim]
            out["top_items"] = rows_sorted

            cur.execute(by_shop_sql, tuple(params + shop_row_params))
            out["shops"] = [
                {
                    "shop_id": r.get("shop_id"),
                    "shop_name": r.get("shop_name") or "Shop",
                    "shop_code": r.get("shop_code") or "",
                    "total_qty": int(r.get("total_qty") or 0),
                    "total_revenue": float(r.get("total_revenue") or 0),
                    "line_count": int(r.get("line_count") or 0),
                }
                for r in (cur.fetchall() or [])
            ]

            if include_lines:
                cur.execute(lines_sql, tuple(params + [lines_lim, lines_off]))
                out["lines"] = [
                    {
                        "id": int(r.get("id") or 0),
                        "sale_id": r.get("sale_id"),
                        "shop_id": r.get("shop_id"),
                        "shop_name": r.get("shop_name") or "Shop",
                        "shop_code": r.get("shop_code") or "",
                        "item_id": r.get("item_id"),
                        "item_name": r.get("item_name") or "Item",
                        "qty": int(r.get("qty") or 0),
                        "unit_price": float(r.get("unit_price") or 0),
                        "line_total": float(r.get("line_total") or 0),
                        "sale_type": (r.get("sale_type") or "").strip().lower(),
                        "employee_name": (r.get("employee_name") or "").strip(),
                        "employee_code": (r.get("employee_code") or "").strip(),
                        "customer_name": (r.get("customer_name") or "").strip(),
                        "customer_phone": (r.get("customer_phone") or "").strip(),
                        "created_at": r.get("created_at"),
                    }
                    for r in (cur.fetchall() or [])
                ]
            loaded_lines = len(out["lines"])
            out["lines_meta"] = {
                "limit": lines_lim,
                "offset": lines_off,
                "loaded_count": lines_off + loaded_lines,
                "total_count": out["line_count"],
                "has_more": (lines_off + loaded_lines) < out["line_count"],
            }
    except pymysql.Error:
        return out
    return out


def get_it_support_item_detail_analytics(item_id: int, analytics_filter: dict) -> Optional[dict]:
    """Full POS + stock-in analytics for one catalog item (all shops, scoped by ``analytics_filter``)."""
    try:
        iid = int(item_id)
    except Exception:
        return None
    if iid <= 0:
        return None
    row = get_item_by_id(iid)
    if not row:
        return None

    item_out = {
        "id": int(row.get("id") or 0),
        "category": (row.get("category") or "").strip(),
        "name": (row.get("name") or "").strip(),
        "description": (row.get("description") or "").strip(),
        "price": float(row.get("price") or 0),
        "selling_price": float(row.get("selling_price") if row.get("selling_price") is not None else row.get("price") or 0),
        "stock_qty": round(float(row.get("stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES),
        "image_path": row.get("image_path"),
        "status": (row.get("status") or "").strip(),
    }

    st_where, st_params = _analytics_where_clause(analytics_filter, "s")
    sale_where = f"si.item_id = %s AND ({st_where})"
    sale_params = [iid] + list(st_params)

    out: Dict[str, Any] = {
        "item": item_out,
        "sales": {
            "total_qty": 0,
            "total_revenue": 0.0,
            "line_count": 0,
            "sale_count": 0,
            "revenue_sale": 0.0,
            "revenue_credit": 0.0,
            "avg_unit_price": 0.0,
        },
        "by_shop": [],
        "by_day": [],
        "by_sale_type": [],
        "by_employee": [],
        "stock_in": {
            "company_qty": 0,
            "company_buy_value": 0.0,
            "shop_qty": 0,
            "shop_buy_value": 0.0,
            "total_qty_in": 0,
            "total_buy_value": 0.0,
            "avg_buying_price": 0.0,
        },
        "margin_estimate": None,
        "lines": [],
    }

    try:
        with get_cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    COALESCE(SUM(si.qty), 0) AS total_qty,
                    COALESCE(SUM(si.line_total), 0) AS total_revenue,
                    COUNT(*) AS line_count,
                    COUNT(DISTINCT s.id) AS sale_count,
                    COALESCE(SUM(CASE WHEN s.sale_type = 'sale' THEN si.line_total ELSE 0 END), 0) AS revenue_sale,
                    COALESCE(SUM(CASE WHEN s.sale_type = 'credit' THEN si.line_total ELSE 0 END), 0) AS revenue_credit
                FROM shop_pos_sale_items si
                JOIN shop_pos_sales s ON s.id = si.sale_id
                WHERE {sale_where}
                """,
                tuple(sale_params),
            )
            t = cur.fetchone() or {}
            tq = int(t.get("total_qty") or 0)
            tr = float(t.get("total_revenue") or 0)
            out["sales"]["total_qty"] = tq
            out["sales"]["total_revenue"] = tr
            out["sales"]["line_count"] = int(t.get("line_count") or 0)
            out["sales"]["sale_count"] = int(t.get("sale_count") or 0)
            out["sales"]["revenue_sale"] = float(t.get("revenue_sale") or 0)
            out["sales"]["revenue_credit"] = float(t.get("revenue_credit") or 0)
            out["sales"]["avg_unit_price"] = round(tr / tq, 4) if tq > 0 else 0.0

            cur.execute(
                f"""
                SELECT
                    sh.id AS shop_id,
                    sh.shop_name,
                    sh.shop_code,
                    COALESCE(SUM(si.qty), 0) AS qty_sold,
                    COALESCE(SUM(si.line_total), 0) AS revenue,
                    COUNT(*) AS line_count
                FROM shop_pos_sale_items si
                JOIN shop_pos_sales s ON s.id = si.sale_id
                LEFT JOIN shops sh ON sh.id = si.shop_id
                WHERE {sale_where}
                GROUP BY sh.id, sh.shop_name, sh.shop_code
                ORDER BY revenue DESC, qty_sold DESC
                LIMIT 200
                """,
                tuple(sale_params),
            )
            out["by_shop"] = [
                {
                    "shop_id": int(r.get("shop_id") or 0),
                    "shop_name": r.get("shop_name") or "Shop",
                    "shop_code": r.get("shop_code") or "",
                    "qty_sold": int(r.get("qty_sold") or 0),
                    "revenue": float(r.get("revenue") or 0),
                    "line_count": int(r.get("line_count") or 0),
                }
                for r in (cur.fetchall() or [])
            ]

            cur.execute(
                f"""
                SELECT
                    DATE(s.created_at) AS day,
                    COALESCE(SUM(si.qty), 0) AS qty_sold,
                    COALESCE(SUM(si.line_total), 0) AS revenue
                FROM shop_pos_sale_items si
                JOIN shop_pos_sales s ON s.id = si.sale_id
                WHERE {sale_where}
                GROUP BY DATE(s.created_at)
                ORDER BY day DESC
                LIMIT 120
                """,
                tuple(sale_params),
            )
            out["by_day"] = [
                {
                    "day": str(r.get("day") or ""),
                    "qty_sold": int(r.get("qty_sold") or 0),
                    "revenue": float(r.get("revenue") or 0),
                }
                for r in (cur.fetchall() or [])
            ]

            cur.execute(
                f"""
                SELECT
                    s.sale_type,
                    COALESCE(SUM(si.qty), 0) AS qty_sold,
                    COALESCE(SUM(si.line_total), 0) AS revenue
                FROM shop_pos_sale_items si
                JOIN shop_pos_sales s ON s.id = si.sale_id
                WHERE {sale_where}
                GROUP BY s.sale_type
                """,
                tuple(sale_params),
            )
            out["by_sale_type"] = [
                {
                    "sale_type": (r.get("sale_type") or "").strip().lower(),
                    "qty_sold": int(r.get("qty_sold") or 0),
                    "revenue": float(r.get("revenue") or 0),
                }
                for r in (cur.fetchall() or [])
            ]

            cur.execute(
                f"""
                SELECT
                    COALESCE(NULLIF(TRIM(s.employee_name), ''), 'Unknown') AS employee_name,
                    COALESCE(TRIM(s.employee_code), '') AS employee_code,
                    COALESCE(SUM(si.qty), 0) AS qty_sold,
                    COALESCE(SUM(si.line_total), 0) AS revenue,
                    COUNT(*) AS line_count
                FROM shop_pos_sale_items si
                JOIN shop_pos_sales s ON s.id = si.sale_id
                WHERE {sale_where}
                GROUP BY s.employee_id, s.employee_code, s.employee_name
                ORDER BY revenue DESC, qty_sold DESC
                LIMIT 50
                """,
                tuple(sale_params),
            )
            out["by_employee"] = [
                {
                    "employee_name": (r.get("employee_name") or "").strip() or "Unknown",
                    "employee_code": (r.get("employee_code") or "").strip(),
                    "qty_sold": int(r.get("qty_sold") or 0),
                    "revenue": float(r.get("revenue") or 0),
                    "line_count": int(r.get("line_count") or 0),
                }
                for r in (cur.fetchall() or [])
            ]

            cur.execute(
                f"""
                SELECT
                    si.id,
                    si.sale_id,
                    si.shop_id,
                    sh.shop_name,
                    si.qty,
                    si.unit_price,
                    si.line_total,
                    s.sale_type,
                    s.customer_name,
                    s.customer_phone,
                    s.employee_name,
                    s.created_at
                FROM shop_pos_sale_items si
                JOIN shop_pos_sales s ON s.id = si.sale_id
                LEFT JOIN shops sh ON sh.id = si.shop_id
                WHERE {sale_where}
                ORDER BY s.created_at DESC, si.id DESC
                LIMIT 200
                """,
                tuple(sale_params),
            )
            out["lines"] = [
                {
                    "id": int(r.get("id") or 0),
                    "sale_id": r.get("sale_id"),
                    "shop_name": r.get("shop_name") or "Shop",
                    "qty": int(r.get("qty") or 0),
                    "unit_price": float(r.get("unit_price") or 0),
                    "line_total": float(r.get("line_total") or 0),
                    "sale_type": (r.get("sale_type") or "").strip().lower(),
                    "customer_name": (r.get("customer_name") or "").strip(),
                    "customer_phone": (r.get("customer_phone") or "").strip(),
                    "employee_name": (r.get("employee_name") or "").strip(),
                    "created_at": r.get("created_at"),
                }
                for r in (cur.fetchall() or [])
            ]

            sst_where, sst_params = _analytics_where_clause(analytics_filter, "sst")
            cur.execute(
                f"""
                SELECT
                    COALESCE(SUM(sst.qty), 0) AS qty_in,
                    COALESCE(SUM(COALESCE(sst.buying_price, 0) * sst.qty), 0) AS buy_value
                FROM shop_stock_transactions sst
                WHERE sst.item_id = %s AND sst.direction = 'in' AND ({sst_where})
                """,
                (iid,) + tuple(sst_params),
            )
            sr = cur.fetchone() or {}
            sq = int(sr.get("qty_in") or 0)
            sv = float(sr.get("buy_value") or 0)

            st_in_where, st_in_params = _analytics_where_clause(analytics_filter, "st")
            cur.execute(
                f"""
                SELECT
                    COALESCE(SUM(st.qty), 0) AS qty_in,
                    COALESCE(SUM(COALESCE(st.buying_price, 0) * st.qty), 0) AS buy_value
                FROM stock_transactions st
                WHERE st.item_id = %s AND st.direction = 'in' AND ({st_in_where})
                """,
                (iid,) + tuple(st_in_params),
            )
            cr = cur.fetchone() or {}
            cq = int(cr.get("qty_in") or 0)
            cv = float(cr.get("buy_value") or 0)

            out["stock_in"]["shop_qty"] = sq
            out["stock_in"]["shop_buy_value"] = sv
            out["stock_in"]["company_qty"] = cq
            out["stock_in"]["company_buy_value"] = cv
            tin = sq + cq
            tval = sv + cv
            out["stock_in"]["total_qty_in"] = tin
            out["stock_in"]["total_buy_value"] = round(tval, 2)
            out["stock_in"]["avg_buying_price"] = round(tval / tin, 4) if tin > 0 else 0.0

            avg_buy = out["stock_in"]["avg_buying_price"]
            if tq > 0 and avg_buy > 0:
                est_cogs = avg_buy * tq
                out["margin_estimate"] = {
                    "estimated_cogs": round(est_cogs, 2),
                    "estimated_gross_margin": round(tr - est_cogs, 2),
                    "note": "COGS uses weighted average buying price from stock-ins in this period × quantity sold (approximate).",
                }
    except pymysql.Error:
        return None
    return out


def get_it_support_period_analytics(
    analytics_filter: dict, analytics_scope: str = "general", shop_id: Optional[int] = None
):
    """Period sales analytics across all shops for IT support/super admin."""
    range_where, range_params = _analytics_where_clause(analytics_filter, "sps")
    scope_where, scope_params = _analytics_receipt_scope_clause(analytics_scope, "sps")
    shop_clause, shop_params = _analytics_shop_filter_clause("sps", shop_id)
    shop_row_filter, shop_row_params = _analytics_shops_row_filter(shop_id, "sh")
    where_sql = f"{range_where} AND {scope_where}{shop_clause}"
    params = list(range_params) + list(scope_params) + list(shop_params)

    totals_sql = f"""
    SELECT
        COUNT(*) AS tx_count,
        COALESCE(SUM(sps.total_amount), 0) AS total_revenue
    FROM shop_pos_sales sps
    WHERE {where_sql}
    """
    daily_sql = f"""
    SELECT
        DATE(sps.created_at) AS day,
        COUNT(*) AS tx_count,
        COALESCE(SUM(CASE WHEN sps.sale_type='sale' THEN sps.total_amount ELSE 0 END), 0) AS sale_amount,
        COALESCE(SUM(CASE WHEN sps.sale_type='credit' THEN sps.total_amount ELSE 0 END), 0) AS credit_amount,
        COALESCE(SUM(sps.total_amount), 0) AS revenue
    FROM shop_pos_sales sps
    WHERE {where_sql}
    GROUP BY DATE(sps.created_at)
    ORDER BY day DESC
    LIMIT 90
    """
    hourly_sql = f"""
    SELECT
        HOUR(sps.created_at) AS hour_of_day,
        COUNT(*) AS tx_count,
        COALESCE(SUM(CASE WHEN sps.sale_type='sale' THEN sps.total_amount ELSE 0 END), 0) AS sale_amount,
        COALESCE(SUM(CASE WHEN sps.sale_type='credit' THEN sps.total_amount ELSE 0 END), 0) AS credit_amount,
        COALESCE(SUM(sps.total_amount), 0) AS revenue
    FROM shop_pos_sales sps
    WHERE {where_sql}
    GROUP BY HOUR(sps.created_at)
    ORDER BY hour_of_day ASC
    """
    employee_sql = f"""
    SELECT
        COALESCE(NULLIF(sps.employee_name, ''), 'Unknown') AS employee_name,
        sps.employee_code,
        sps.employee_id,
        COUNT(*) AS tx_count,
        COALESCE(SUM(sps.total_amount), 0) AS revenue
    FROM shop_pos_sales sps
    WHERE {where_sql}
    GROUP BY sps.employee_id, sps.employee_code, sps.employee_name
    ORDER BY revenue DESC, tx_count DESC
    LIMIT 200
    """
    shop_sql = f"""
    SELECT
        sh.id AS shop_id,
        sh.shop_name,
        sh.shop_code,
        COUNT(sps.id) AS tx_count,
        COALESCE(SUM(sps.total_amount), 0) AS revenue
    FROM shops sh
    LEFT JOIN shop_pos_sales sps ON sps.shop_id = sh.id AND ({where_sql})
    {shop_row_filter}
    GROUP BY sh.id, sh.shop_name, sh.shop_code
    ORDER BY revenue DESC, tx_count DESC, sh.shop_name ASC
    LIMIT 500
    """

    out = {
        "total_tx_count": 0,
        "total_revenue": 0.0,
        "daily": [],
        "hourly": [],
        "employees": [],
        "shops": [],
        "peak_day": None,
        "peak_hour": None,
    }
    try:
        with get_cursor() as cur:
            cur.execute(totals_sql, tuple(params))
            t = cur.fetchone() or {}
            out["total_tx_count"] = int(t.get("tx_count") or 0)
            out["total_revenue"] = float(t.get("total_revenue") or 0)

            cur.execute(daily_sql, tuple(params))
            out["daily"] = [
                {
                    "day": str(r.get("day") or ""),
                    "tx_count": int(r.get("tx_count") or 0),
                    "sale_amount": float(r.get("sale_amount") or 0),
                    "credit_amount": float(r.get("credit_amount") or 0),
                    "revenue": float(r.get("revenue") or 0),
                }
                for r in (cur.fetchall() or [])
            ]
            if out["daily"]:
                out["peak_day"] = max(out["daily"], key=lambda x: (x["revenue"], x["tx_count"]))

            cur.execute(hourly_sql, tuple(params))
            out["hourly"] = [
                {
                    "hour": f"{int(r.get('hour_of_day') or 0):02d}:00",
                    "tx_count": int(r.get("tx_count") or 0),
                    "sale_amount": float(r.get("sale_amount") or 0),
                    "credit_amount": float(r.get("credit_amount") or 0),
                    "revenue": float(r.get("revenue") or 0),
                }
                for r in (cur.fetchall() or [])
            ]
            if out["hourly"]:
                out["peak_hour"] = max(out["hourly"], key=lambda x: (x["revenue"], x["tx_count"]))

            cur.execute(employee_sql, tuple(params))
            out["employees"] = [
                {
                    "employee_id": r.get("employee_id"),
                    "employee_name": r.get("employee_name") or "Unknown",
                    "employee_code": r.get("employee_code") or "",
                    "tx_count": int(r.get("tx_count") or 0),
                    "revenue": float(r.get("revenue") or 0),
                }
                for r in (cur.fetchall() or [])
            ]

            cur.execute(shop_sql, tuple(params + shop_row_params))
            out["shops"] = [
                {
                    "shop_id": r.get("shop_id"),
                    "shop_name": r.get("shop_name") or "Shop",
                    "shop_code": r.get("shop_code") or "",
                    "tx_count": int(r.get("tx_count") or 0),
                    "revenue": float(r.get("revenue") or 0),
                }
                for r in (cur.fetchall() or [])
            ]
    except pymysql.Error:
        return out
    return out


def get_it_support_employee_analytics(
    analytics_filter: dict, analytics_scope: str = "general", shop_id: Optional[int] = None
):
    """Employee sales analytics across all shops for IT support/super admin."""
    range_where, range_params = _analytics_where_clause(analytics_filter, "sps")
    scope_where, scope_params = _analytics_receipt_scope_clause(analytics_scope, "sps")
    shop_clause, shop_params = _analytics_shop_filter_clause("sps", shop_id)
    where_sql = f"{range_where} AND {scope_where}{shop_clause}"
    params = list(range_params) + list(scope_params) + list(shop_params)

    totals_sql = f"""
    SELECT
        COUNT(*) AS tx_count,
        COALESCE(SUM(sps.total_amount), 0) AS total_revenue,
        COUNT(DISTINCT COALESCE(sps.employee_id, CONCAT('code:', COALESCE(sps.employee_code, '')), COALESCE(sps.employee_name, 'UNKNOWN'))) AS distinct_employees
    FROM shop_pos_sales sps
    WHERE {where_sql}
    """
    employees_sql = f"""
    SELECT
        sps.employee_id,
        COALESCE(NULLIF(sps.employee_name, ''), 'Unknown') AS employee_name,
        sps.employee_code,
        COUNT(*) AS tx_count,
        COALESCE(SUM(sps.total_amount), 0) AS revenue,
        COALESCE(SUM(CASE WHEN sps.sale_type='sale' THEN sps.total_amount ELSE 0 END), 0) AS sale_amount,
        COALESCE(SUM(CASE WHEN sps.sale_type='credit' THEN sps.total_amount ELSE 0 END), 0) AS credit_amount
    FROM shop_pos_sales sps
    WHERE {where_sql}
    GROUP BY sps.employee_id, sps.employee_code, sps.employee_name
    ORDER BY revenue DESC, tx_count DESC
    LIMIT 500
    """
    by_shop_sql = f"""
    SELECT
        COALESCE(NULLIF(sps.employee_name, ''), 'Unknown') AS employee_name,
        sps.employee_code,
        sh.id AS shop_id,
        sh.shop_name,
        sh.shop_code,
        COUNT(*) AS tx_count,
        COALESCE(SUM(sps.total_amount), 0) AS revenue
    FROM shop_pos_sales sps
    LEFT JOIN shops sh ON sh.id = sps.shop_id
    WHERE {where_sql}
    GROUP BY sps.employee_name, sps.employee_code, sh.id, sh.shop_name, sh.shop_code
    ORDER BY revenue DESC, tx_count DESC
    LIMIT 1000
    """
    transactions_by_employee_sql = f"""
    SELECT
        sps.employee_id,
        COALESCE(NULLIF(sps.employee_name, ''), 'Unknown') AS employee_name,
        sps.employee_code,
        COUNT(*) AS tx_count,
        COALESCE(SUM(sps.item_count), 0) AS item_count,
        COALESCE(SUM(CASE WHEN sps.sale_type='sale' THEN sps.total_amount ELSE 0 END), 0) AS sale_amount,
        COALESCE(SUM(CASE WHEN sps.sale_type='credit' THEN sps.total_amount ELSE 0 END), 0) AS credit_amount,
        COALESCE(SUM(sps.total_amount), 0) AS total_amount
    FROM shop_pos_sales sps
    WHERE {where_sql}
    GROUP BY sps.employee_id, sps.employee_code, sps.employee_name
    ORDER BY COALESCE(SUM(sps.total_amount), 0) DESC, COUNT(*) DESC, COALESCE(NULLIF(sps.employee_name, ''), 'Unknown') ASC
    LIMIT 500
    """

    out = {
        "total_tx_count": 0,
        "total_revenue": 0.0,
        "distinct_employees": 0,
        "employees": [],
        "employee_shop_rows": [],
        "transactions": [],
        "top_employee": None,
        "least_employee": None,
    }
    try:
        with get_cursor() as cur:
            cur.execute(totals_sql, tuple(params))
            t = cur.fetchone() or {}
            out["total_tx_count"] = int(t.get("tx_count") or 0)
            out["total_revenue"] = float(t.get("total_revenue") or 0)
            out["distinct_employees"] = int(t.get("distinct_employees") or 0)

            cur.execute(employees_sql, tuple(params))
            out["employees"] = [
                {
                    "employee_id": r.get("employee_id"),
                    "employee_name": r.get("employee_name") or "Unknown",
                    "employee_code": r.get("employee_code") or "",
                    "tx_count": int(r.get("tx_count") or 0),
                    "revenue": float(r.get("revenue") or 0),
                    "sale_amount": float(r.get("sale_amount") or 0),
                    "credit_amount": float(r.get("credit_amount") or 0),
                }
                for r in (cur.fetchall() or [])
            ]
            if out["employees"]:
                out["top_employee"] = out["employees"][0]
                out["least_employee"] = sorted(
                    out["employees"], key=lambda x: (x["revenue"], x["tx_count"])
                )[0]

            cur.execute(by_shop_sql, tuple(params))
            out["employee_shop_rows"] = [
                {
                    "employee_name": r.get("employee_name") or "Unknown",
                    "employee_code": r.get("employee_code") or "",
                    "shop_id": r.get("shop_id"),
                    "shop_name": r.get("shop_name") or "Shop",
                    "shop_code": r.get("shop_code") or "",
                    "tx_count": int(r.get("tx_count") or 0),
                    "revenue": float(r.get("revenue") or 0),
                }
                for r in (cur.fetchall() or [])
            ]

            try:
                cur.execute(transactions_by_employee_sql, tuple(params))
                out["transactions"] = []
                for r in cur.fetchall() or []:
                    en = r.get("employee_name")
                    ec = r.get("employee_code")
                    out["transactions"].append(
                        {
                            "employee_id": r.get("employee_id"),
                            "employee_name": (str(en).strip() if en is not None else "") or "Unknown",
                            "employee_code": str(ec).strip() if ec is not None else "",
                            "tx_count": int(r.get("tx_count") or 0),
                            "item_count": float(r.get("item_count") or 0),
                            "sale_amount": float(r.get("sale_amount") or 0),
                            "credit_amount": float(r.get("credit_amount") or 0),
                            "total_amount": float(r.get("total_amount") or 0),
                        }
                    )
            except pymysql.Error as e:
                logger.warning("get_it_support_employee_analytics transactions aggregate skipped: %s", e)
                out["transactions"] = []
    except pymysql.Error:
        return out
    return out


def get_it_support_sales_analytics(
    analytics_filter: dict, analytics_scope: str = "general", shop_id: Optional[int] = None
):
    """Sales-only analytics across all shops (excludes credit)."""
    range_where, range_params = _analytics_where_clause(analytics_filter, "sps")
    scope_where, scope_params = _analytics_receipt_scope_clause(analytics_scope, "sps")
    shop_clause, shop_params = _analytics_shop_filter_clause("sps", shop_id)
    shop_row_filter, shop_row_params = _analytics_shops_row_filter(shop_id, "sh")
    where_sql = f"sps.sale_type='sale' AND {range_where} AND {scope_where}{shop_clause}"
    params = list(range_params) + list(scope_params) + list(shop_params)

    totals_sql = f"""
    SELECT
        COUNT(*) AS tx_count,
        COALESCE(SUM(sps.total_amount), 0) AS total_revenue
    FROM shop_pos_sales sps
    WHERE {where_sql}
    """
    daily_sql = f"""
    SELECT
        DATE(sps.created_at) AS day,
        COUNT(*) AS tx_count,
        COALESCE(SUM(sps.total_amount), 0) AS revenue
    FROM shop_pos_sales sps
    WHERE {where_sql}
    GROUP BY DATE(sps.created_at)
    ORDER BY day DESC
    LIMIT 90
    """
    hourly_sql = f"""
    SELECT
        HOUR(sps.created_at) AS hour_of_day,
        COUNT(*) AS tx_count,
        COALESCE(SUM(sps.total_amount), 0) AS revenue
    FROM shop_pos_sales sps
    WHERE {where_sql}
    GROUP BY HOUR(sps.created_at)
    ORDER BY hour_of_day ASC
    """
    by_shop_sql = f"""
    SELECT
        sh.id AS shop_id,
        sh.shop_name,
        sh.shop_code,
        COUNT(sps.id) AS tx_count,
        COALESCE(SUM(sps.total_amount), 0) AS revenue
    FROM shops sh
    LEFT JOIN shop_pos_sales sps ON sps.shop_id = sh.id AND ({where_sql})
    {shop_row_filter}
    GROUP BY sh.id, sh.shop_name, sh.shop_code
    ORDER BY revenue DESC, tx_count DESC, sh.shop_name ASC
    LIMIT 500
    """
    by_employee_sql = f"""
    SELECT
        sps.employee_id,
        COALESCE(NULLIF(sps.employee_name, ''), 'Unknown') AS employee_name,
        sps.employee_code,
        COUNT(*) AS tx_count,
        COALESCE(SUM(sps.total_amount), 0) AS revenue
    FROM shop_pos_sales sps
    WHERE {where_sql}
    GROUP BY sps.employee_id, sps.employee_code, sps.employee_name
    ORDER BY revenue DESC, tx_count DESC
    LIMIT 500
    """
    transactions_by_shop_sql = f"""
    SELECT
        sps.shop_id,
        COALESCE(sh.shop_name, 'Unknown') AS shop_name,
        COALESCE(sh.shop_code, '') AS shop_code,
        COUNT(*) AS tx_count,
        COALESCE(SUM(sps.total_amount), 0) AS total_amount,
        COALESCE(SUM(sps.item_count), 0) AS item_count
    FROM shop_pos_sales sps
    LEFT JOIN shops sh ON sh.id = sps.shop_id
    WHERE {where_sql}
    GROUP BY sps.shop_id, sh.shop_name, sh.shop_code
    ORDER BY SUM(sps.total_amount) DESC, COUNT(*) DESC, sh.shop_name ASC
    LIMIT 500
    """

    out = {
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
    try:
        with get_cursor() as cur:
            cur.execute(totals_sql, tuple(params))
            t = cur.fetchone() or {}
            out["total_tx_count"] = int(t.get("tx_count") or 0)
            out["total_revenue"] = float(t.get("total_revenue") or 0)

            cur.execute(daily_sql, tuple(params))
            out["daily"] = [
                {
                    "day": str(r.get("day") or ""),
                    "tx_count": int(r.get("tx_count") or 0),
                    "revenue": float(r.get("revenue") or 0),
                }
                for r in (cur.fetchall() or [])
            ]
            if out["daily"]:
                out["peak_day"] = max(out["daily"], key=lambda x: (x["revenue"], x["tx_count"]))

            cur.execute(hourly_sql, tuple(params))
            out["hourly"] = [
                {
                    "hour": f"{int(r.get('hour_of_day') or 0):02d}:00",
                    "tx_count": int(r.get("tx_count") or 0),
                    "revenue": float(r.get("revenue") or 0),
                }
                for r in (cur.fetchall() or [])
            ]
            if out["hourly"]:
                out["peak_hour"] = max(out["hourly"], key=lambda x: (x["revenue"], x["tx_count"]))

            cur.execute(by_shop_sql, tuple(params + shop_row_params))
            out["shops"] = [
                {
                    "shop_id": r.get("shop_id"),
                    "shop_name": r.get("shop_name") or "Shop",
                    "shop_code": r.get("shop_code") or "",
                    "tx_count": int(r.get("tx_count") or 0),
                    "revenue": float(r.get("revenue") or 0),
                }
                for r in (cur.fetchall() or [])
            ]

            cur.execute(by_employee_sql, tuple(params))
            out["employees"] = [
                {
                    "employee_id": r.get("employee_id"),
                    "employee_name": r.get("employee_name") or "Unknown",
                    "employee_code": r.get("employee_code") or "",
                    "tx_count": int(r.get("tx_count") or 0),
                    "revenue": float(r.get("revenue") or 0),
                }
                for r in (cur.fetchall() or [])
            ]

            cur.execute(transactions_by_shop_sql, tuple(params))
            out["transactions"] = [
                {
                    "shop_id": r.get("shop_id"),
                    "shop_name": r.get("shop_name") or "Shop",
                    "shop_code": r.get("shop_code") or "",
                    "tx_count": int(r.get("tx_count") or 0),
                    "total_amount": float(r.get("total_amount") or 0),
                    "item_count": float(r.get("item_count") or 0),
                }
                for r in (cur.fetchall() or [])
            ]
    except pymysql.Error:
        return out
    return out


def get_it_support_credit_analytics(
    analytics_filter: dict, analytics_scope: str = "general", shop_id: Optional[int] = None
):
    """Credit-only analytics across all shops (excludes cash sales)."""
    range_where, range_params = _analytics_where_clause(analytics_filter, "sps")
    scope_where, scope_params = _analytics_receipt_scope_clause(analytics_scope, "sps")
    shop_clause, shop_params = _analytics_shop_filter_clause("sps", shop_id)
    shop_row_filter, shop_row_params = _analytics_shops_row_filter(shop_id, "sh")
    where_sql = f"sps.sale_type='credit' AND {range_where} AND {scope_where}{shop_clause}"
    params = list(range_params) + list(scope_params) + list(shop_params)

    totals_sql = f"""
    SELECT
        COUNT(*) AS tx_count,
        COALESCE(SUM(sps.total_amount), 0) AS total_revenue
    FROM shop_pos_sales sps
    WHERE {where_sql}
    """
    daily_sql = f"""
    SELECT
        DATE(sps.created_at) AS day,
        COUNT(*) AS tx_count,
        COALESCE(SUM(sps.total_amount), 0) AS revenue
    FROM shop_pos_sales sps
    WHERE {where_sql}
    GROUP BY DATE(sps.created_at)
    ORDER BY day DESC
    LIMIT 90
    """
    hourly_sql = f"""
    SELECT
        HOUR(sps.created_at) AS hour_of_day,
        COUNT(*) AS tx_count,
        COALESCE(SUM(sps.total_amount), 0) AS revenue
    FROM shop_pos_sales sps
    WHERE {where_sql}
    GROUP BY HOUR(sps.created_at)
    ORDER BY hour_of_day ASC
    """
    by_shop_sql = f"""
    SELECT
        sh.id AS shop_id,
        sh.shop_name,
        sh.shop_code,
        COUNT(sps.id) AS tx_count,
        COALESCE(SUM(sps.total_amount), 0) AS revenue
    FROM shops sh
    LEFT JOIN shop_pos_sales sps ON sps.shop_id = sh.id AND ({where_sql})
    {shop_row_filter}
    GROUP BY sh.id, sh.shop_name, sh.shop_code
    ORDER BY revenue DESC, tx_count DESC, sh.shop_name ASC
    LIMIT 500
    """
    by_employee_sql = f"""
    SELECT
        sps.employee_id,
        COALESCE(NULLIF(sps.employee_name, ''), 'Unknown') AS employee_name,
        sps.employee_code,
        COUNT(*) AS tx_count,
        COALESCE(SUM(sps.total_amount), 0) AS revenue
    FROM shop_pos_sales sps
    WHERE {where_sql}
    GROUP BY sps.employee_id, sps.employee_code, sps.employee_name
    ORDER BY revenue DESC, tx_count DESC
    LIMIT 500
    """
    by_customer_sql = f"""
    SELECT
        COALESCE(NULLIF(sps.customer_name, ''), 'Unknown customer') AS customer_name,
        COALESCE(NULLIF(sps.customer_phone, ''), '-') AS customer_phone,
        COUNT(*) AS tx_count,
        COALESCE(SUM(sps.total_amount), 0) AS revenue
    FROM shop_pos_sales sps
    WHERE {where_sql}
    GROUP BY sps.customer_name, sps.customer_phone
    ORDER BY revenue DESC, tx_count DESC
    LIMIT 500
    """
    transactions_by_shop_sql = f"""
    SELECT
        sps.shop_id,
        COALESCE(sh.shop_name, 'Unknown') AS shop_name,
        COALESCE(sh.shop_code, '') AS shop_code,
        COUNT(*) AS tx_count,
        COALESCE(SUM(sps.total_amount), 0) AS total_amount,
        COALESCE(SUM(sps.item_count), 0) AS item_count
    FROM shop_pos_sales sps
    LEFT JOIN shops sh ON sh.id = sps.shop_id
    WHERE {where_sql}
    GROUP BY sps.shop_id, sh.shop_name, sh.shop_code
    ORDER BY SUM(sps.total_amount) DESC, COUNT(*) DESC, sh.shop_name ASC
    LIMIT 500
    """

    out = {
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
    try:
        with get_cursor() as cur:
            cur.execute(totals_sql, tuple(params))
            t = cur.fetchone() or {}
            out["total_tx_count"] = int(t.get("tx_count") or 0)
            out["total_revenue"] = float(t.get("total_revenue") or 0)

            cur.execute(daily_sql, tuple(params))
            out["daily"] = [
                {
                    "day": str(r.get("day") or ""),
                    "tx_count": int(r.get("tx_count") or 0),
                    "revenue": float(r.get("revenue") or 0),
                }
                for r in (cur.fetchall() or [])
            ]
            if out["daily"]:
                out["peak_day"] = max(out["daily"], key=lambda x: (x["revenue"], x["tx_count"]))

            cur.execute(hourly_sql, tuple(params))
            out["hourly"] = [
                {
                    "hour": f"{int(r.get('hour_of_day') or 0):02d}:00",
                    "tx_count": int(r.get("tx_count") or 0),
                    "revenue": float(r.get("revenue") or 0),
                }
                for r in (cur.fetchall() or [])
            ]
            if out["hourly"]:
                out["peak_hour"] = max(out["hourly"], key=lambda x: (x["revenue"], x["tx_count"]))

            cur.execute(by_shop_sql, tuple(params + shop_row_params))
            out["shops"] = [
                {
                    "shop_id": r.get("shop_id"),
                    "shop_name": r.get("shop_name") or "Shop",
                    "shop_code": r.get("shop_code") or "",
                    "tx_count": int(r.get("tx_count") or 0),
                    "revenue": float(r.get("revenue") or 0),
                }
                for r in (cur.fetchall() or [])
            ]

            cur.execute(by_employee_sql, tuple(params))
            out["employees"] = [
                {
                    "employee_id": r.get("employee_id"),
                    "employee_name": r.get("employee_name") or "Unknown",
                    "employee_code": r.get("employee_code") or "",
                    "tx_count": int(r.get("tx_count") or 0),
                    "revenue": float(r.get("revenue") or 0),
                }
                for r in (cur.fetchall() or [])
            ]

            cur.execute(by_customer_sql, tuple(params))
            out["customers"] = [
                {
                    "customer_name": r.get("customer_name") or "Unknown customer",
                    "customer_phone": r.get("customer_phone") or "-",
                    "tx_count": int(r.get("tx_count") or 0),
                    "revenue": float(r.get("revenue") or 0),
                }
                for r in (cur.fetchall() or [])
            ]

            cur.execute(transactions_by_shop_sql, tuple(params))
            out["transactions"] = [
                {
                    "shop_id": r.get("shop_id"),
                    "shop_name": r.get("shop_name") or "Shop",
                    "shop_code": r.get("shop_code") or "",
                    "tx_count": int(r.get("tx_count") or 0),
                    "total_amount": float(r.get("total_amount") or 0),
                    "item_count": float(r.get("item_count") or 0),
                }
                for r in (cur.fetchall() or [])
            ]
    except pymysql.Error:
        return out
    return out


def get_it_support_customer_analytics(
    analytics_filter: dict,
    analytics_scope: str = "general",
    shop_id: Optional[int] = None,
    *,
    customers_limit: int = 150,
    customers_offset: int = 0,
    include_customers: bool = True,
):
    """Customer analytics across all shops within selected period."""
    try:
        cust_lim = max(1, min(500, int(customers_limit)))
    except (TypeError, ValueError):
        cust_lim = 150
    try:
        cust_off = max(0, int(customers_offset))
    except (TypeError, ValueError):
        cust_off = 0
    range_where, range_params = _analytics_where_clause(analytics_filter, "sps")
    scope_where, scope_params = _analytics_receipt_scope_clause(analytics_scope, "sps")
    shop_clause, shop_params = _analytics_shop_filter_clause("sps", shop_id)
    where_sql = f"{range_where} AND {scope_where}{shop_clause}"
    params = list(range_params) + list(scope_params) + list(shop_params)
    totals_sql = f"""
    SELECT
        COUNT(*) AS tx_count,
        COALESCE(SUM(sps.total_amount), 0) AS total_amount,
        COUNT(DISTINCT CONCAT(
            COALESCE(NULLIF(sps.customer_name, ''), 'WALK IN'),
            '|||',
            COALESCE(NULLIF(sps.customer_phone, ''), '-')
        )) AS distinct_customers
    FROM shop_pos_sales sps
    WHERE {where_sql}
    """
    customers_sql = f"""
    SELECT
        COALESCE(NULLIF(sps.customer_name, ''), 'WALK IN') AS customer_name,
        COALESCE(NULLIF(sps.customer_phone, ''), '-') AS customer_phone,
        COUNT(*) AS tx_count,
        MAX(sps.created_at) AS last_tx_at,
        COALESCE(SUM(sps.total_amount), 0) AS total_amount,
        COALESCE(SUM(CASE WHEN sps.sale_type='sale' THEN sps.total_amount ELSE 0 END), 0) AS sale_amount,
        COALESCE(SUM(CASE WHEN sps.sale_type='credit' THEN sps.total_amount ELSE 0 END), 0) AS credit_amount
    FROM shop_pos_sales sps
    WHERE {where_sql}
    GROUP BY customer_name, customer_phone
    ORDER BY last_tx_at DESC, tx_count DESC, total_amount DESC, customer_name ASC
    LIMIT %s OFFSET %s
    """
    out = {
        "total_tx_count": 0,
        "total_amount": 0.0,
        "distinct_customers": 0,
        "customers": [],
        "customers_meta": {
            "limit": cust_lim,
            "offset": cust_off,
            "loaded_count": 0,
            "total_count": 0,
            "has_more": False,
        },
    }
    try:
        with get_cursor() as cur:
            cur.execute(totals_sql, tuple(params))
            t = cur.fetchone() or {}
            out["total_tx_count"] = int(t.get("tx_count") or 0)
            out["total_amount"] = float(t.get("total_amount") or 0)
            out["distinct_customers"] = int(t.get("distinct_customers") or 0)
            out["customers_meta"]["total_count"] = out["distinct_customers"]

            if include_customers:
                cur.execute(customers_sql, tuple(params + [cust_lim, cust_off]))
                out["customers"] = [
                    {
                        "customer_name": r.get("customer_name") or "WALK IN",
                        "customer_phone": r.get("customer_phone") or "-",
                        "tx_count": int(r.get("tx_count") or 0),
                        "last_tx_at": r.get("last_tx_at"),
                        "total_amount": float(r.get("total_amount") or 0),
                        "sale_amount": float(r.get("sale_amount") or 0),
                        "credit_amount": float(r.get("credit_amount") or 0),
                    }
                    for r in (cur.fetchall() or [])
                ]
            loaded = len(out["customers"])
            out["customers_meta"] = {
                "limit": cust_lim,
                "offset": cust_off,
                "loaded_count": cust_off + loaded,
                "total_count": out["distinct_customers"],
                "has_more": (cust_off + loaded) < out["distinct_customers"],
            }
    except pymysql.Error:
        return out
    return out


def get_it_support_customer_transactions(
    customer_name: str,
    customer_phone: str,
    limit: int = 3000,
    analytics_filter: Optional[dict] = None,
    shop_id: Optional[int] = None,
    analytics_scope: str = "general",
):
    """All transactions for one customer identity (including WALK IN placeholder)."""
    n = (customer_name or "").strip() or "WALK IN"
    p = normalize_customer_phone(customer_phone)
    range_where, range_params = _analytics_where_clause(analytics_filter or {}, "sps")
    scope_where, scope_params = _analytics_receipt_scope_clause(analytics_scope, "sps")
    shop_scope = " AND sps.shop_id=%s" if shop_id else ""
    identity_where, identity_params = _customer_identity_where_sql(
        n, p, name_col="sps.customer_name", phone_col="sps.customer_phone"
    )
    sql = f"""
    SELECT
        sps.id,
        sps.shop_id,
        sh.shop_name,
        sh.shop_code,
        sps.sale_type,
        sps.total_amount,
        sps.item_count,
        COALESCE(NULLIF(sps.customer_name, ''), 'WALK IN') AS customer_name,
        COALESCE(NULLIF(sps.customer_phone, ''), '-') AS customer_phone,
        COALESCE(NULLIF(sps.employee_name, ''), 'Unknown') AS employee_name,
        sps.employee_code,
        sps.created_at
    FROM shop_pos_sales sps
    LEFT JOIN shops sh ON sh.id = sps.shop_id
    WHERE {identity_where}
      AND {range_where}
      AND {scope_where}
      {shop_scope}
    ORDER BY sps.created_at DESC, sps.id DESC
    LIMIT %s
    """
    try:
        with get_cursor() as cur:
            args = [*identity_params, *range_params, *scope_params]
            if shop_id:
                args.append(int(shop_id))
            args.append(int(limit))
            cur.execute(sql, tuple(args))
            rows = cur.fetchall() or []
        return [
            {
                "id": int(r.get("id") or 0),
                "shop_id": r.get("shop_id"),
                "shop_name": r.get("shop_name") or "Shop",
                "shop_code": r.get("shop_code") or "",
                "sale_type": (r.get("sale_type") or "").strip().lower(),
                "total_amount": float(r.get("total_amount") or 0),
                "item_count": int(r.get("item_count") or 0),
                "customer_name": r.get("customer_name") or "WALK IN",
                "customer_phone": r.get("customer_phone") or "-",
                "employee_name": r.get("employee_name") or "Unknown",
                "employee_code": (r.get("employee_code") or "").strip(),
                "created_at": r.get("created_at"),
            }
            for r in rows
        ]
    except pymysql.Error:
        return []


def get_it_support_customer_transaction_items(
    customer_name: str,
    customer_phone: str,
    limit: int = 5000,
    analytics_filter: Optional[dict] = None,
    shop_id: Optional[int] = None,
    analytics_scope: str = "general",
):
    """Item-level transactions for one customer identity."""
    n = (customer_name or "").strip() or "WALK IN"
    p = normalize_customer_phone(customer_phone)
    range_where, range_params = _analytics_where_clause(analytics_filter or {}, "sps")
    scope_where, scope_params = _analytics_receipt_scope_clause(analytics_scope, "sps")
    shop_scope = " AND sps.shop_id=%s" if shop_id else ""
    identity_where, identity_params = _customer_identity_where_sql(
        n, p, name_col="sps.customer_name", phone_col="sps.customer_phone"
    )
    sql = f"""
    SELECT
        sps.id AS sale_id,
        sps.shop_id,
        sh.shop_name,
        sh.shop_code,
        sps.sale_type,
        COALESCE(NULLIF(sps.employee_name, ''), 'Unknown') AS employee_name,
        COALESCE(NULLIF(sps.employee_code, ''), '-') AS employee_code,
        sps.created_at,
        COALESCE(NULLIF(si.item_name, ''), 'Item') AS item_name,
        COALESCE(si.qty, 0) AS qty,
        COALESCE(si.line_total, 0) AS amount
    FROM shop_pos_sales sps
    JOIN shop_pos_sale_items si ON si.sale_id = sps.id
    LEFT JOIN shops sh ON sh.id = sps.shop_id
    WHERE {identity_where}
      AND {range_where}
      AND {scope_where}
      {shop_scope}
    ORDER BY sps.created_at DESC, sps.id DESC, si.id DESC
    LIMIT %s
    """
    try:
        with get_cursor() as cur:
            args = [*identity_params, *range_params, *scope_params]
            if shop_id:
                args.append(int(shop_id))
            args.append(int(limit))
            cur.execute(sql, tuple(args))
            rows = cur.fetchall() or []
        return [
            {
                "sale_id": int(r.get("sale_id") or 0),
                "shop_id": r.get("shop_id"),
                "shop_name": r.get("shop_name") or "Shop",
                "shop_code": r.get("shop_code") or "",
                "sale_type": (r.get("sale_type") or "").strip().lower(),
                "employee_name": r.get("employee_name") or "Unknown",
                "employee_code": (r.get("employee_code") or "").strip(),
                "created_at": r.get("created_at"),
                "item_name": r.get("item_name") or "Item",
                "qty": int(r.get("qty") or 0),
                "amount": float(r.get("amount") or 0),
            }
            for r in rows
        ]
    except pymysql.Error:
        return []


def get_it_support_customer_detail_analytics(
    customer_name: str,
    customer_phone: str,
    analytics_filter: dict,
    shop_id: Optional[int] = None,
    analytics_scope: str = "general",
):
    """Detailed analytics for one customer identity with date filters."""
    n = (customer_name or "").strip() or "WALK IN"
    p = (customer_phone or "").strip() or "-"
    range_where, range_params = _analytics_where_clause(analytics_filter, "sps")
    scope_where, scope_params = _analytics_receipt_scope_clause(analytics_scope, "sps")
    where_sql = (
        "COALESCE(NULLIF(sps.customer_name, ''), 'WALK IN')=%s "
        "AND COALESCE(NULLIF(sps.customer_phone, ''), '-')=%s "
        f"AND {range_where} "
        f"AND {scope_where}"
    )
    params = [n, p] + list(range_params) + list(scope_params)
    if shop_id:
        where_sql += " AND sps.shop_id=%s"
        params.append(int(shop_id))

    totals_sql = f"""
    SELECT
        COUNT(*) AS tx_count,
        COALESCE(SUM(sps.total_amount), 0) AS total_amount,
        COALESCE(SUM(CASE WHEN sps.sale_type='sale' THEN sps.total_amount ELSE 0 END), 0) AS sale_amount,
        COALESCE(SUM(CASE WHEN sps.sale_type='credit' THEN sps.total_amount ELSE 0 END), 0) AS credit_amount,
        COALESCE(SUM(CASE WHEN sps.sale_type='sale' THEN 1 ELSE 0 END), 0) AS sale_tx_count,
        COALESCE(SUM(CASE WHEN sps.sale_type='credit' THEN 1 ELSE 0 END), 0) AS credit_tx_count,
        COALESCE(AVG(sps.total_amount), 0) AS avg_ticket
    FROM shop_pos_sales sps
    WHERE {where_sql}
    """
    items_totals_sql = f"""
    SELECT
        COALESCE(SUM(si.qty), 0) AS total_item_qty,
        COUNT(DISTINCT COALESCE(si.item_id, CONCAT('n:', si.item_name))) AS distinct_items
    FROM shop_pos_sale_items si
    JOIN shop_pos_sales sps ON sps.id = si.sale_id
    WHERE {where_sql}
    """
    top_items_sql = f"""
    SELECT
        si.item_id,
        si.item_name,
        COALESCE(SUM(si.qty), 0) AS qty_sold,
        COALESCE(SUM(si.line_total), 0) AS revenue,
        COUNT(*) AS line_count
    FROM shop_pos_sale_items si
    JOIN shop_pos_sales sps ON sps.id = si.sale_id
    WHERE {where_sql}
    GROUP BY si.item_id, si.item_name
    ORDER BY qty_sold DESC, revenue DESC
    LIMIT 20
    """
    daily_sql = f"""
    SELECT
        DATE(sps.created_at) AS day,
        COUNT(*) AS tx_count,
        COALESCE(SUM(sps.total_amount), 0) AS amount
    FROM shop_pos_sales sps
    WHERE {where_sql}
    GROUP BY DATE(sps.created_at)
    ORDER BY day DESC
    LIMIT 90
    """
    hourly_sql = f"""
    SELECT
        HOUR(sps.created_at) AS hour_of_day,
        COUNT(*) AS tx_count,
        COALESCE(SUM(sps.total_amount), 0) AS amount
    FROM shop_pos_sales sps
    WHERE {where_sql}
    GROUP BY HOUR(sps.created_at)
    ORDER BY hour_of_day ASC
    """
    # Split sale vs credit for visual charts.
    daily_split_sql = f"""
    SELECT
        DATE(sps.created_at) AS day,
        COALESCE(SUM(CASE WHEN sps.sale_type='sale' THEN sps.total_amount ELSE 0 END), 0) AS sale_amount,
        COALESCE(SUM(CASE WHEN sps.sale_type='credit' THEN sps.total_amount ELSE 0 END), 0) AS credit_amount,
        COUNT(CASE WHEN sps.sale_type='sale' THEN 1 END) AS sale_tx_count,
        COUNT(CASE WHEN sps.sale_type='credit' THEN 1 END) AS credit_tx_count
    FROM shop_pos_sales sps
    WHERE {where_sql}
    GROUP BY DATE(sps.created_at)
    ORDER BY day ASC
    LIMIT 90
    """
    hourly_split_sql = f"""
    SELECT
        HOUR(sps.created_at) AS hour_of_day,
        COALESCE(SUM(CASE WHEN sps.sale_type='sale' THEN sps.total_amount ELSE 0 END), 0) AS sale_amount,
        COALESCE(SUM(CASE WHEN sps.sale_type='credit' THEN sps.total_amount ELSE 0 END), 0) AS credit_amount,
        COUNT(CASE WHEN sps.sale_type='sale' THEN 1 END) AS sale_tx_count,
        COUNT(CASE WHEN sps.sale_type='credit' THEN 1 END) AS credit_tx_count
    FROM shop_pos_sales sps
    WHERE {where_sql}
    GROUP BY HOUR(sps.created_at)
    ORDER BY hour_of_day ASC
    """
    shops_sql = f"""
    SELECT
        s.id AS shop_id,
        s.shop_name,
        s.shop_code,
        COUNT(*) AS tx_count,
        COALESCE(SUM(sps.total_amount), 0) AS amount
    FROM shop_pos_sales sps
    LEFT JOIN shops s ON s.id = sps.shop_id
    WHERE {where_sql}
    GROUP BY s.id, s.shop_name, s.shop_code
    ORDER BY amount DESC, tx_count DESC
    LIMIT 200
    """
    employees_sql = f"""
    SELECT
        sps.employee_id,
        COALESCE(NULLIF(sps.employee_name, ''), 'Unknown') AS employee_name,
        COALESCE(NULLIF(sps.employee_code, ''), '-') AS employee_code,
        COUNT(*) AS tx_count,
        COALESCE(SUM(sps.total_amount), 0) AS amount,
        COALESCE(SUM(CASE WHEN sps.sale_type='sale' THEN sps.total_amount ELSE 0 END), 0) AS sale_amount,
        COALESCE(SUM(CASE WHEN sps.sale_type='credit' THEN sps.total_amount ELSE 0 END), 0) AS credit_amount
    FROM shop_pos_sales sps
    WHERE {where_sql}
    GROUP BY sps.employee_id, employee_name, employee_code
    ORDER BY amount DESC, tx_count DESC
    LIMIT 400
    """
    out = {
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
    try:
        with get_cursor() as cur:
            cur.execute(totals_sql, tuple(params))
            t = cur.fetchone() or {}
            out["total_amount"] = float(t.get("total_amount") or 0)
            out["total_tx_count"] = int(t.get("tx_count") or 0)
            out["sale_amount"] = float(t.get("sale_amount") or 0)
            out["sale_tx_count"] = int(t.get("sale_tx_count") or 0)
            out["credit_amount"] = float(t.get("credit_amount") or 0)
            out["credit_tx_count"] = int(t.get("credit_tx_count") or 0)
            out["avg_ticket"] = float(t.get("avg_ticket") or 0)

            cur.execute(items_totals_sql, tuple(params))
            it = cur.fetchone() or {}
            out["total_item_qty"] = int(it.get("total_item_qty") or 0)
            out["distinct_items"] = int(it.get("distinct_items") or 0)

            cur.execute(top_items_sql, tuple(params))
            out["top_items"] = [
                {
                    "item_id": r.get("item_id"),
                    "item_name": r.get("item_name") or "Item",
                    "qty_sold": int(r.get("qty_sold") or 0),
                    "revenue": float(r.get("revenue") or 0),
                    "line_count": int(r.get("line_count") or 0),
                }
                for r in (cur.fetchall() or [])
            ]

            cur.execute(daily_sql, tuple(params))
            out["daily"] = [
                {
                    "day": str(r.get("day") or ""),
                    "tx_count": int(r.get("tx_count") or 0),
                    "amount": float(r.get("amount") or 0),
                }
                for r in (cur.fetchall() or [])
            ]

            cur.execute(hourly_sql, tuple(params))
            out["hourly"] = [
                {
                    "hour": f"{int(r.get('hour_of_day') or 0):02d}:00",
                    "tx_count": int(r.get("tx_count") or 0),
                    "amount": float(r.get("amount") or 0),
                }
                for r in (cur.fetchall() or [])
            ]

            # Sale vs credit split
            cur.execute(daily_split_sql, tuple(params))
            dailySplitRows = cur.fetchall() or []
            out["daily_sale"] = [
                {
                    "day": str(r.get("day") or ""),
                    "tx_count": int(r.get("sale_tx_count") or 0),
                    "amount": float(r.get("sale_amount") or 0),
                }
                for r in dailySplitRows
            ]
            out["daily_credit"] = [
                {
                    "day": str(r.get("day") or ""),
                    "tx_count": int(r.get("credit_tx_count") or 0),
                    "amount": float(r.get("credit_amount") or 0),
                }
                for r in dailySplitRows
            ]

            cur.execute(hourly_split_sql, tuple(params))
            hourlySplitRows = cur.fetchall() or []
            out["hourly_sale"] = [
                {
                    "hour": f"{int(r.get('hour_of_day') or 0):02d}:00",
                    "tx_count": int(r.get("sale_tx_count") or 0),
                    "amount": float(r.get("sale_amount") or 0),
                }
                for r in hourlySplitRows
            ]
            out["hourly_credit"] = [
                {
                    "hour": f"{int(r.get('hour_of_day') or 0):02d}:00",
                    "tx_count": int(r.get("credit_tx_count") or 0),
                    "amount": float(r.get("credit_amount") or 0),
                }
                for r in hourlySplitRows
            ]

            cur.execute(shops_sql, tuple(params))
            out["shops"] = [
                {
                    "shop_id": r.get("shop_id"),
                    "shop_name": r.get("shop_name") or "Shop",
                    "shop_code": r.get("shop_code") or "",
                    "tx_count": int(r.get("tx_count") or 0),
                    "amount": float(r.get("amount") or 0),
                }
                for r in (cur.fetchall() or [])
            ]

            cur.execute(employees_sql, tuple(params))
            out["employees"] = [
                {
                    "employee_id": r.get("employee_id"),
                    "employee_name": r.get("employee_name") or "Unknown",
                    "employee_code": r.get("employee_code") or "-",
                    "tx_count": int(r.get("tx_count") or 0),
                    "amount": float(r.get("amount") or 0),
                    "sale_amount": float(r.get("sale_amount") or 0),
                    "credit_amount": float(r.get("credit_amount") or 0),
                }
                for r in (cur.fetchall() or [])
            ]
    except pymysql.Error:
        return out
    return out


def get_shop_customer_analytics(
    shop_id: int, analytics_filter: dict, analytics_scope: str = "general"
):
    """Customer analytics scoped to one shop only."""
    range_where, range_params = _analytics_where_clause(analytics_filter, "sps")
    scope_where, scope_params = _analytics_receipt_scope_clause(analytics_scope, "sps")
    where_sql = f"sps.shop_id=%s AND {range_where} AND {scope_where}"
    params = [int(shop_id)] + list(range_params) + list(scope_params)
    totals_sql = f"""
    SELECT
        COUNT(*) AS tx_count,
        COALESCE(SUM(sps.total_amount), 0) AS total_amount,
        COUNT(DISTINCT CONCAT(
            COALESCE(NULLIF(sps.customer_name, ''), 'WALK IN'),
            '|||',
            COALESCE(NULLIF(sps.customer_phone, ''), '-')
        )) AS distinct_customers
    FROM shop_pos_sales sps
    WHERE {where_sql}
    """
    customers_sql = f"""
    SELECT
        COALESCE(NULLIF(sps.customer_name, ''), 'WALK IN') AS customer_name,
        COALESCE(NULLIF(sps.customer_phone, ''), '-') AS customer_phone,
        COUNT(*) AS tx_count,
        COALESCE(SUM(sps.total_amount), 0) AS total_amount,
        COALESCE(SUM(CASE WHEN sps.sale_type='sale' THEN sps.total_amount ELSE 0 END), 0) AS sale_amount,
        COALESCE(SUM(CASE WHEN sps.sale_type='credit' THEN sps.total_amount ELSE 0 END), 0) AS credit_amount
    FROM shop_pos_sales sps
    WHERE {where_sql}
    GROUP BY customer_name, customer_phone
    ORDER BY total_amount DESC, tx_count DESC, customer_name ASC
    LIMIT 1500
    """
    out = {
        "total_tx_count": 0,
        "total_amount": 0.0,
        "distinct_customers": 0,
        "customers": [],
    }
    try:
        with get_cursor() as cur:
            cur.execute(totals_sql, tuple(params))
            t = cur.fetchone() or {}
            out["total_tx_count"] = int(t.get("tx_count") or 0)
            out["total_amount"] = float(t.get("total_amount") or 0)
            out["distinct_customers"] = int(t.get("distinct_customers") or 0)

            cur.execute(customers_sql, tuple(params))
            out["customers"] = [
                {
                    "customer_name": r.get("customer_name") or "WALK IN",
                    "customer_phone": r.get("customer_phone") or "-",
                    "tx_count": int(r.get("tx_count") or 0),
                    "total_amount": float(r.get("total_amount") or 0),
                    "sale_amount": float(r.get("sale_amount") or 0),
                    "credit_amount": float(r.get("credit_amount") or 0),
                }
                for r in (cur.fetchall() or [])
            ]
    except pymysql.Error:
        return out
    return out


def get_shop_item_analytics(
    shop_id: int,
    analytics_filter: dict,
    analytics_scope: str = "general",
    top_items_limit: int = 0,
):
    """Return item analytics: totals, sold items, and peak day/hour sold.

    ``top_items_limit`` caps the grouped item list (0 = no limit, show all sold items).
    """
    try:
        tl_raw = int(top_items_limit) if top_items_limit is not None else 0
    except (TypeError, ValueError):
        tl_raw = 0
    no_limit = tl_raw <= 0
    lim = max(1, min(50000, tl_raw)) if not no_limit else 0
    sid = int(shop_id)
    inv_mode = resolve_shop_pos_inventory_mode(sid)
    range_where, range_params = _analytics_where_clause(analytics_filter, "s")
    scope_where, scope_params = _analytics_receipt_scope_clause(analytics_scope, "s")
    where_sql = f"s.shop_id=%s AND {range_where} AND {scope_where}"
    params = [sid] + list(range_params) + list(scope_params)
    if inv_mode in ("kitchen", "both"):
        ensure_shop_pos_sale_items_portion_unit_cost_column()
        cost = _kitchen_portion_unit_cost_expr()
        portion_cost_select = f"""
        , COALESCE(SUM(
            CASE
                WHEN LOWER(COALESCE(s.inventory_mode, 'shop')) IN ('kitchen', 'both')
                THEN si.qty * ({cost})
                ELSE 0
            END
        ), 0) AS portion_cost
        """
        top_items_from = f"""
    FROM shop_pos_sale_items si
    JOIN shop_pos_sales s ON s.id = si.sale_id
    LEFT JOIN shop_kitchen_portions skp
        ON skp.shop_id = s.shop_id
        AND si.item_id IS NOT NULL AND si.item_id > 0
        AND skp.item_id = si.item_id
    {_KITCHEN_PORTION_SALE_NAME_JOIN}
    LEFT JOIN shop_kitchen_portions skp_n
        ON skp_n.shop_id = s.shop_id AND skp_n.item_id = im.id
        """
    else:
        portion_cost_select = ", 0 AS portion_cost"
        top_items_from = """
    FROM shop_pos_sale_items si
    JOIN shop_pos_sales s ON s.id = si.sale_id
        """
    top_items_sql = f"""
    SELECT
        si.item_id,
        si.item_name,
        COALESCE(SUM(si.qty), 0) AS qty_sold,
        COALESCE(SUM(si.line_total), 0) AS revenue,
        COUNT(*) AS sale_lines
        {portion_cost_select}
    {top_items_from}
    WHERE {where_sql}
    GROUP BY si.item_id, si.item_name
    ORDER BY qty_sold DESC, revenue DESC
    """
    if not no_limit:
        top_items_sql += f"\n    LIMIT {lim}"
    totals_sql = f"""
    SELECT
        COALESCE(SUM(si.qty), 0) AS total_qty,
        COALESCE(SUM(si.line_total), 0) AS total_revenue,
        COUNT(*) AS line_count,
        COUNT(DISTINCT COALESCE(si.item_id, CONCAT('n:', si.item_name))) AS distinct_items
    FROM shop_pos_sale_items si
    JOIN shop_pos_sales s ON s.id = si.sale_id
    WHERE {where_sql}
    """
    peak_day_sql = f"""
    SELECT DATE(s.created_at) AS day, COALESCE(SUM(si.qty), 0) AS qty_sold
    FROM shop_pos_sale_items si
    JOIN shop_pos_sales s ON s.id = si.sale_id
    WHERE {where_sql}
    GROUP BY DATE(s.created_at)
    ORDER BY qty_sold DESC, day ASC
    LIMIT 1
    """
    peak_hour_sql = f"""
    SELECT HOUR(s.created_at) AS hour_of_day, COALESCE(SUM(si.qty), 0) AS qty_sold
    FROM shop_pos_sale_items si
    JOIN shop_pos_sales s ON s.id = si.sale_id
    WHERE {where_sql}
    GROUP BY HOUR(s.created_at)
    ORDER BY qty_sold DESC, hour_of_day ASC
    LIMIT 1
    """
    out = {
        "total_qty": 0,
        "total_revenue": 0.0,
        "line_count": 0,
        "distinct_items": 0,
        "top_items": [],
        "peak_day": None,
        "peak_hour": None,
        "pos_inventory_mode": inv_mode,
    }
    try:
        with get_cursor() as cur:
            cur.execute(totals_sql, tuple(params))
            t = cur.fetchone() or {}
            out["total_qty"] = int(t.get("total_qty") or 0)
            out["total_revenue"] = float(t.get("total_revenue") or 0)
            out["line_count"] = int(t.get("line_count") or 0)
            out["distinct_items"] = int(t.get("distinct_items") or 0)

            cur.execute(top_items_sql, tuple(params))
            rows = cur.fetchall() or []
            out["top_items"] = [
                {
                    "item_id": r.get("item_id"),
                    "item_name": r.get("item_name") or "Item",
                    "qty_sold": int(r.get("qty_sold") or 0),
                    "revenue": float(r.get("revenue") or 0),
                    "sale_lines": int(r.get("sale_lines") or 0),
                    "portion_cost": round(float(r.get("portion_cost") or 0), 2),
                }
                for r in rows
            ]

            cur.execute(peak_day_sql, tuple(params))
            pd = cur.fetchone() or None
            if pd:
                out["peak_day"] = {"day": str(pd.get("day") or ""), "qty_sold": int(pd.get("qty_sold") or 0)}

            cur.execute(peak_hour_sql, tuple(params))
            ph = cur.fetchone() or None
            if ph:
                hr = ph.get("hour_of_day")
                try:
                    h = int(hr)
                    hour_label = f"{h:02d}:00"
                except Exception:
                    hour_label = str(hr or "")
                out["peak_hour"] = {"hour": hour_label, "qty_sold": int(ph.get("qty_sold") or 0)}
    except pymysql.Error:
        return out
    return out


def get_shop_item_sales_totals_by_item(shop_id: int, analytics_filter: dict) -> Dict[int, Dict[str, Any]]:
    """Per-item POS sales aggregates for the analytics period (one shop)."""
    range_where, range_params = _analytics_where_clause(analytics_filter, "s")
    where_sql = f"s.shop_id=%s AND si.item_id IS NOT NULL AND {range_where}"
    params = [int(shop_id)] + list(range_params)
    sql = f"""
    SELECT
        si.item_id,
        COALESCE(SUM(si.qty), 0) AS qty_sold,
        COALESCE(SUM(si.line_total), 0) AS revenue,
        COUNT(*) AS sale_lines
    FROM shop_pos_sale_items si
    JOIN shop_pos_sales s ON s.id = si.sale_id
    WHERE {where_sql}
    GROUP BY si.item_id
    """
    out: Dict[int, Dict[str, Any]] = {}
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(params))
            for r in (cur.fetchall() or []):
                try:
                    iid = int(r.get("item_id") or 0)
                except Exception:
                    continue
                if iid <= 0:
                    continue
                out[iid] = {
                    "qty_sold": int(r.get("qty_sold") or 0),
                    "revenue": float(r.get("revenue") or 0),
                    "sale_lines": int(r.get("sale_lines") or 0),
                }
    except pymysql.Error:
        return {}
    return out


def get_shop_avg_buying_price_by_item(shop_id: int) -> Dict[int, float]:
    """Weighted average buying price per item from stock-in rows at this shop."""
    sql = """
    SELECT
        item_id,
        SUM(COALESCE(buying_price, 0) * qty) AS buy_value,
        SUM(qty) AS buy_qty
    FROM shop_stock_transactions
    WHERE shop_id = %s AND direction = 'in' AND buying_price IS NOT NULL
    GROUP BY item_id
    """
    out: Dict[int, float] = {}
    try:
        with get_cursor() as cur:
            cur.execute(sql, (int(shop_id),))
            for rr in (cur.fetchall() or []):
                try:
                    iid = int(rr.get("item_id") or 0)
                    bq = int(rr.get("buy_qty") or 0)
                    bv = float(rr.get("buy_value") or 0)
                except Exception:
                    continue
                if iid > 0 and bq > 0:
                    out[iid] = bv / bq
    except pymysql.Error:
        return {}
    return out


def get_shop_period_analytics(
    shop_id: int, analytics_filter: dict, analytics_scope: str = "general"
):
    """Detailed period analytics: day/hour trends + employee performance."""
    range_where, range_params = _analytics_where_clause(analytics_filter, "")
    scope_where, scope_params = _analytics_receipt_scope_clause(analytics_scope, "")
    where_sql = f"shop_id=%s AND {range_where} AND {scope_where}"
    params = [int(shop_id)] + list(range_params) + list(scope_params)
    daily_sql = f"""
    SELECT
        DATE(created_at) AS day,
        COUNT(*) AS tx_count,
        COALESCE(SUM(total_amount), 0) AS revenue
    FROM shop_pos_sales
    WHERE {where_sql}
    GROUP BY DATE(created_at)
    ORDER BY day DESC
    LIMIT 60
    """
    hourly_sql = f"""
    SELECT
        HOUR(created_at) AS hour_of_day,
        COUNT(*) AS tx_count,
        COALESCE(SUM(total_amount), 0) AS revenue
    FROM shop_pos_sales
    WHERE {where_sql}
    GROUP BY HOUR(created_at)
    ORDER BY hour_of_day ASC
    """
    employee_sql = f"""
    SELECT
        COALESCE(NULLIF(employee_name, ''), 'Unknown') AS employee_name,
        employee_code,
        employee_id,
        COUNT(*) AS tx_count,
        COALESCE(SUM(total_amount), 0) AS revenue
    FROM shop_pos_sales
    WHERE {where_sql}
    GROUP BY employee_id, employee_code, employee_name
    ORDER BY revenue DESC, tx_count DESC
    """
    totals_sql = f"""
    SELECT COUNT(*) AS tx_count, COALESCE(SUM(total_amount), 0) AS revenue
    FROM shop_pos_sales
    WHERE {where_sql}
    """

    out = {
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
    try:
        with get_cursor() as cur:
            cur.execute(totals_sql, tuple(params))
            totals = cur.fetchone() or {}
            out["total_tx_count"] = int(totals.get("tx_count") or 0)
            out["total_revenue"] = float(totals.get("revenue") or 0)

            cur.execute(daily_sql, tuple(params))
            daily = cur.fetchall() or []
            out["daily"] = [
                {
                    "day": str(r.get("day") or ""),
                    "tx_count": int(r.get("tx_count") or 0),
                    "revenue": float(r.get("revenue") or 0),
                }
                for r in daily
            ]
            if out["daily"]:
                out["peak_day"] = max(out["daily"], key=lambda x: (x["revenue"], x["tx_count"]))

            cur.execute(hourly_sql, tuple(params))
            hourly = cur.fetchall() or []
            out["hourly"] = [
                {
                    "hour": f"{int(r.get('hour_of_day') or 0):02d}:00",
                    "tx_count": int(r.get("tx_count") or 0),
                    "revenue": float(r.get("revenue") or 0),
                }
                for r in hourly
            ]
            if out["hourly"]:
                out["peak_hour"] = max(out["hourly"], key=lambda x: (x["revenue"], x["tx_count"]))

            cur.execute(employee_sql, tuple(params))
            emps = cur.fetchall() or []
            out["employees"] = [
                {
                    "employee_id": r.get("employee_id"),
                    "employee_name": r.get("employee_name") or "Unknown",
                    "employee_code": r.get("employee_code") or "",
                    "tx_count": int(r.get("tx_count") or 0),
                    "revenue": float(r.get("revenue") or 0),
                }
                for r in emps
            ]
            if out["employees"]:
                out["top_employee"] = out["employees"][0]
                out["least_employee"] = sorted(
                    out["employees"], key=lambda x: (x["revenue"], x["tx_count"])
                )[0]
    except pymysql.Error:
        return out
    return out


def get_shop_sales_analytics(
    shop_id: int, analytics_filter: dict, analytics_scope: str = "general"
):
    """Sales-only analytics (excludes credit transactions)."""
    range_where, range_params = _analytics_where_clause(analytics_filter, "")
    scope_where, scope_params = _analytics_receipt_scope_clause(analytics_scope, "")
    where_sql = f"shop_id=%s AND sale_type='sale' AND {range_where} AND {scope_where}"
    params = [int(shop_id)] + list(range_params) + list(scope_params)

    totals_sql = f"""
    SELECT COUNT(*) AS tx_count, COALESCE(SUM(total_amount), 0) AS revenue
    FROM shop_pos_sales
    WHERE {where_sql}
    """
    daily_sql = f"""
    SELECT DATE(created_at) AS day, COUNT(*) AS tx_count, COALESCE(SUM(total_amount), 0) AS revenue
    FROM shop_pos_sales
    WHERE {where_sql}
    GROUP BY DATE(created_at)
    ORDER BY day DESC
    LIMIT 60
    """
    hourly_sql = f"""
    SELECT HOUR(created_at) AS hour_of_day, COUNT(*) AS tx_count, COALESCE(SUM(total_amount), 0) AS revenue
    FROM shop_pos_sales
    WHERE {where_sql}
    GROUP BY HOUR(created_at)
    ORDER BY hour_of_day ASC
    """
    employee_sql = f"""
    SELECT
        COALESCE(NULLIF(employee_name, ''), 'Unknown') AS employee_name,
        employee_code,
        employee_id,
        COUNT(*) AS tx_count,
        COALESCE(SUM(total_amount), 0) AS revenue
    FROM shop_pos_sales
    WHERE {where_sql}
    GROUP BY employee_id, employee_code, employee_name
    ORDER BY revenue DESC, tx_count DESC
    """
    out = {
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
    try:
        with get_cursor() as cur:
            cur.execute(totals_sql, tuple(params))
            totals = cur.fetchone() or {}
            out["total_tx_count"] = int(totals.get("tx_count") or 0)
            out["total_revenue"] = float(totals.get("revenue") or 0)

            cur.execute(daily_sql, tuple(params))
            daily = cur.fetchall() or []
            out["daily"] = [
                {"day": str(r.get("day") or ""), "tx_count": int(r.get("tx_count") or 0), "revenue": float(r.get("revenue") or 0)}
                for r in daily
            ]
            if out["daily"]:
                out["peak_day"] = max(out["daily"], key=lambda x: (x["revenue"], x["tx_count"]))

            cur.execute(hourly_sql, tuple(params))
            hourly = cur.fetchall() or []
            out["hourly"] = [
                {
                    "hour": f"{int(r.get('hour_of_day') or 0):02d}:00",
                    "tx_count": int(r.get("tx_count") or 0),
                    "revenue": float(r.get("revenue") or 0),
                }
                for r in hourly
            ]
            if out["hourly"]:
                out["peak_hour"] = max(out["hourly"], key=lambda x: (x["revenue"], x["tx_count"]))

            cur.execute(employee_sql, tuple(params))
            emps = cur.fetchall() or []
            out["employees"] = [
                {
                    "employee_id": r.get("employee_id"),
                    "employee_name": r.get("employee_name") or "Unknown",
                    "employee_code": r.get("employee_code") or "",
                    "tx_count": int(r.get("tx_count") or 0),
                    "revenue": float(r.get("revenue") or 0),
                }
                for r in emps
            ]
            if out["employees"]:
                out["top_employee"] = out["employees"][0]
                out["least_employee"] = sorted(out["employees"], key=lambda x: (x["revenue"], x["tx_count"]))[0]
    except pymysql.Error:
        return out
    return out


def get_shop_credit_analytics(
    shop_id: int, analytics_filter: dict, analytics_scope: str = "general"
):
    """Credit-only analytics (excludes direct sales transactions)."""
    range_where, range_params = _analytics_where_clause(analytics_filter, "")
    scope_where, scope_params = _analytics_receipt_scope_clause(analytics_scope, "")
    where_sql = f"shop_id=%s AND sale_type='credit' AND {range_where} AND {scope_where}"
    params = [int(shop_id)] + list(range_params) + list(scope_params)

    totals_sql = f"""
    SELECT COUNT(*) AS tx_count, COALESCE(SUM(total_amount), 0) AS revenue
    FROM shop_pos_sales
    WHERE {where_sql}
    """
    daily_sql = f"""
    SELECT DATE(created_at) AS day, COUNT(*) AS tx_count, COALESCE(SUM(total_amount), 0) AS revenue
    FROM shop_pos_sales
    WHERE {where_sql}
    GROUP BY DATE(created_at)
    ORDER BY day DESC
    LIMIT 60
    """
    hourly_sql = f"""
    SELECT HOUR(created_at) AS hour_of_day, COUNT(*) AS tx_count, COALESCE(SUM(total_amount), 0) AS revenue
    FROM shop_pos_sales
    WHERE {where_sql}
    GROUP BY HOUR(created_at)
    ORDER BY hour_of_day ASC
    """
    employee_sql = f"""
    SELECT
        COALESCE(NULLIF(employee_name, ''), 'Unknown') AS employee_name,
        employee_code,
        employee_id,
        COUNT(*) AS tx_count,
        COALESCE(SUM(total_amount), 0) AS revenue
    FROM shop_pos_sales
    WHERE {where_sql}
    GROUP BY employee_id, employee_code, employee_name
    ORDER BY revenue DESC, tx_count DESC
    """
    customer_sql = f"""
    SELECT
        COALESCE(NULLIF(customer_name, ''), 'Unknown customer') AS customer_name,
        COALESCE(NULLIF(customer_phone, ''), '-') AS customer_phone,
        COUNT(*) AS tx_count,
        COALESCE(SUM(total_amount), 0) AS credit_total
    FROM shop_pos_sales
    WHERE {where_sql}
    GROUP BY customer_name, customer_phone
    ORDER BY credit_total DESC, tx_count DESC
    LIMIT 200
    """
    customer_sales_sql = f"""
    SELECT
        s.id AS sale_id,
        COALESCE(NULLIF(s.customer_name, ''), 'Unknown customer') AS customer_name,
        COALESCE(NULLIF(s.customer_phone, ''), '-') AS customer_phone,
        s.total_amount,
        s.created_at,
        COALESCE(NULLIF(s.employee_name, ''), 'Unknown') AS employee_name,
        s.employee_code
    FROM shop_pos_sales s
    WHERE {where_sql}
    ORDER BY s.created_at DESC
    LIMIT 1500
    """
    sale_items_sql = """
    SELECT sale_id, item_name, qty, unit_price, line_total
    FROM shop_pos_sale_items
    WHERE sale_id IN ({placeholders})
    ORDER BY sale_id ASC, id ASC
    """
    out = {
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
    try:
        with get_cursor() as cur:
            cur.execute(totals_sql, tuple(params))
            totals = cur.fetchone() or {}
            out["total_tx_count"] = int(totals.get("tx_count") or 0)
            out["total_revenue"] = float(totals.get("revenue") or 0)

            cur.execute(daily_sql, tuple(params))
            daily = cur.fetchall() or []
            out["daily"] = [
                {"day": str(r.get("day") or ""), "tx_count": int(r.get("tx_count") or 0), "revenue": float(r.get("revenue") or 0)}
                for r in daily
            ]
            if out["daily"]:
                out["peak_day"] = max(out["daily"], key=lambda x: (x["revenue"], x["tx_count"]))

            cur.execute(hourly_sql, tuple(params))
            hourly = cur.fetchall() or []
            out["hourly"] = [
                {
                    "hour": f"{int(r.get('hour_of_day') or 0):02d}:00",
                    "tx_count": int(r.get("tx_count") or 0),
                    "revenue": float(r.get("revenue") or 0),
                }
                for r in hourly
            ]
            if out["hourly"]:
                out["peak_hour"] = max(out["hourly"], key=lambda x: (x["revenue"], x["tx_count"]))

            cur.execute(employee_sql, tuple(params))
            emps = cur.fetchall() or []
            out["employees"] = [
                {
                    "employee_id": r.get("employee_id"),
                    "employee_name": r.get("employee_name") or "Unknown",
                    "employee_code": r.get("employee_code") or "",
                    "tx_count": int(r.get("tx_count") or 0),
                    "revenue": float(r.get("revenue") or 0),
                }
                for r in emps
            ]
            if out["employees"]:
                out["top_employee"] = out["employees"][0]
                out["least_employee"] = sorted(out["employees"], key=lambda x: (x["revenue"], x["tx_count"]))[0]

            cur.execute(customer_sql, tuple(params))
            crows = cur.fetchall() or []
            out["customers"] = [
                {
                    "customer_name": r.get("customer_name") or "Unknown customer",
                    "customer_phone": r.get("customer_phone") or "-",
                    "tx_count": int(r.get("tx_count") or 0),
                    "credit_total": float(r.get("credit_total") or 0),
                    "amount_due": float(r.get("credit_total") or 0),
                }
                for r in crows
            ]
            if out["customers"]:
                out["top_customer"] = out["customers"][0]

            cur.execute(customer_sales_sql, tuple(params))
            srows = cur.fetchall() or []
            sale_ids = [int(r.get("sale_id")) for r in srows if r.get("sale_id") is not None]
            items_by_sale = {}
            if sale_ids:
                placeholders = ", ".join(["%s"] * len(sale_ids))
                cur.execute(sale_items_sql.format(placeholders=placeholders), tuple(sale_ids))
                irows = cur.fetchall() or []
                for ir in irows:
                    sid = int(ir.get("sale_id"))
                    items_by_sale.setdefault(sid, []).append(
                        {
                            "item_name": ir.get("item_name") or "Item",
                            "qty": int(ir.get("qty") or 0),
                            "unit_price": float(ir.get("unit_price") or 0),
                            "line_total": float(ir.get("line_total") or 0),
                        }
                    )

            details = {}
            for sr in srows:
                c_name = sr.get("customer_name") or "Unknown customer"
                c_phone = sr.get("customer_phone") or "-"
                key = f"{c_name}|||{c_phone}"
                sid = int(sr.get("sale_id") or 0)
                details.setdefault(key, []).append(
                    {
                        "sale_id": sid,
                        "created_at": (
                            sr.get("created_at").isoformat(sep=" ", timespec="seconds")
                            if hasattr(sr.get("created_at"), "isoformat")
                            else str(sr.get("created_at") or "")
                        ),
                        "employee_name": sr.get("employee_name") or "Unknown",
                        "employee_code": sr.get("employee_code") or "",
                        "total_amount": float(sr.get("total_amount") or 0),
                        "items": items_by_sale.get(sid, []),
                    }
                )
            out["customer_details"] = details
    except pymysql.Error:
        return out
    return out


def _canonical_kenya_phone(phone: str) -> str:
    """Prefer 254… storage for Kenyan mobiles when recognizable."""
    p = _normalize_phone(phone or "")
    digits = re.sub(r"\D", "", p)
    if not digits:
        return (phone or "").strip() or "-"
    if digits == "-":
        return "-"
    if len(digits) == 10 and digits.startswith(("07", "01")):
        return "254" + digits[1:]
    if len(digits) == 12 and digits.startswith("254"):
        return digits
    if len(digits) == 9 and digits[0] in ("7", "1"):
        return "254" + digits
    return digits


def _is_meaningful_customer_phone(phone: str) -> bool:
    p = (phone or "").strip()
    if not p or p == "-":
        return False
    digits = re.sub(r"\D", "", p)
    return len(digits) >= 9


def _customer_phone_lookup_keys(phone: str) -> list[str]:
    """Possible stored customer_phone values for the same Kenyan subscriber."""
    p = (phone or "").strip() or "-"
    if p == "-" or not _is_meaningful_customer_phone(p):
        return ["-"]
    keys = list(_seller_phone_lookup_keys(p))
    canon = _canonical_kenya_phone(p)
    if canon and canon not in keys:
        keys.insert(0, canon)
    return keys


def _customer_identity_where_sql(
    customer_name: str,
    customer_phone: str,
    *,
    name_col: str = "customer_name",
    phone_col: str = "customer_phone",
) -> tuple[str, list]:
    """Match walk-in by name+phone; otherwise all rows for the same phone (any format)."""
    n = (customer_name or "").strip() or "WALK IN"
    p = (customer_phone or "").strip() or "-"
    if _is_meaningful_customer_phone(p):
        keys = _customer_phone_lookup_keys(p)
        placeholders = ",".join(["%s"] * len(keys))
        return (
            f"COALESCE(NULLIF({phone_col}, ''), '-') IN ({placeholders})",
            list(keys),
        )
    return (
        f"COALESCE(NULLIF({name_col}, ''), 'WALK IN')=%s AND COALESCE(NULLIF({phone_col}, ''), '-')=%s",
        [n, p],
    )


def normalize_customer_phone(phone: str) -> str:
    """Public helper — canonical 254… for Kenyan mobiles, else stripped input."""
    raw = (phone or "").strip()
    if not raw or raw == "-":
        return "-"
    canon = _canonical_kenya_phone(raw)
    return canon if canon else raw


def normalize_supplier_phone(phone: str) -> str:
    """Canonical 254… for supplier/seller phones."""
    raw = (phone or "").strip()
    if not raw or raw == "-":
        return ""
    canon = _canonical_kenya_phone(raw)
    return canon if canon and canon != "-" else raw


def _stored_seller_phone(phone: Optional[str]) -> Optional[str]:
    raw = (phone or "").strip()
    if not raw or raw == "-":
        return None
    canon = normalize_supplier_phone(raw)
    return (canon or raw)[:40]


def _stored_customer_phone(phone: Optional[str]) -> Optional[str]:
    raw = (phone or "").strip()
    if not raw or raw == "-":
        return None
    return normalize_customer_phone(raw)[:40]


def search_shop_customers_for_pos(shop_id: int, query: str, limit: int = 10):
    """Substring name search for POS customer autofill suggestions."""
    q = (query or "").strip()
    if len(q) < 2:
        return []
    try:
        lim = max(1, min(int(limit), 25))
    except Exception:
        lim = 10
    like = f"%{q}%"
    sql = """
    SELECT id, customer_name, phone
    FROM shop_customers
    WHERE shop_id=%s
      AND TRIM(customer_name) <> ''
      AND customer_name LIKE %s
    ORDER BY updated_at DESC, customer_name ASC
    LIMIT %s
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (int(shop_id), like, lim))
            rows = cur.fetchall() or []
        return [
            {
                "id": r.get("id"),
                "customer_name": (r.get("customer_name") or "").strip(),
                "phone": (r.get("phone") or "").strip(),
            }
            for r in rows
            if (r.get("customer_name") or "").strip()
        ]
    except pymysql.Error:
        return []


def get_shop_customer_by_phone(shop_id: int, phone: str):
    sql = """
    SELECT id, shop_id, customer_name, phone, created_at, updated_at
    FROM shop_customers
    WHERE shop_id=%s AND phone=%s
    LIMIT 1
    """
    try:
        with get_cursor() as cur:
            for key in _seller_phone_lookup_keys(phone):
                cur.execute(sql, (int(shop_id), key))
                row = cur.fetchone()
                if row:
                    return row
            return None
    except pymysql.Error:
        return None


def upsert_shop_customer(shop_id: int, customer_name: str, phone: str) -> bool:
    name = (customer_name or "").strip()
    ph = normalize_customer_phone((phone or "").strip())
    if not name or not ph or ph == "-":
        return False
    sql = """
    INSERT INTO shop_customers (shop_id, customer_name, phone)
    VALUES (%s, %s, %s)
    ON DUPLICATE KEY UPDATE
        customer_name=VALUES(customer_name),
        updated_at=CURRENT_TIMESTAMP
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql, (int(shop_id), name[:190], ph[:40]))
            return True
    except pymysql.Error:
        return False


def get_shop_printer_settings(shop_id: int):
    sql = """
    SELECT shop_id, printer_type, device_label, config_json, updated_at
    FROM shop_printer_settings
    WHERE shop_id=%s
    LIMIT 1
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (int(shop_id),))
            return cur.fetchone()
    except pymysql.Error:
        return None


def upsert_shop_printer_settings(
    shop_id: int,
    *,
    printer_type: str,
    device_label: Optional[str],
    config_json: str,
) -> bool:
    if printer_type not in ("bluetooth", "network", "usb"):
        return False
    sql = """
    INSERT INTO shop_printer_settings (shop_id, printer_type, device_label, config_json)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        printer_type=VALUES(printer_type),
        device_label=VALUES(device_label),
        config_json=VALUES(config_json),
        updated_at=CURRENT_TIMESTAMP
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(
                sql,
                (int(shop_id), printer_type, (device_label or None)[:255], config_json),
            )
            return True
    except pymysql.Error:
        return False


def delete_shop_printer_settings(shop_id: int) -> bool:
    sql = "DELETE FROM shop_printer_settings WHERE shop_id=%s"
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql, (int(shop_id),))
            return True
    except pymysql.Error:
        return False


def init_shop_print_agent_jobs_table():
    """Queued ESC/POS payloads for on-prem print agents (cloud → LAN bridge)."""
    sql = """
    CREATE TABLE IF NOT EXISTS shop_print_agent_jobs (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        shop_id INT NOT NULL,
        payload MEDIUMBLOB NOT NULL,
        status ENUM('pending', 'delivered', 'failed') NOT NULL DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        delivered_at TIMESTAMP NULL DEFAULT NULL,
        error_message VARCHAR(500) NULL,
        KEY idx_spaj_shop_status (shop_id, status, id),
        KEY idx_spaj_created (created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql)
        logger.info("Table shop_print_agent_jobs is ready.")
        return True
    except pymysql.Error as e:
        logger.warning("Could not init shop_print_agent_jobs: %s", e)
        return False


def enqueue_shop_print_agent_job(shop_id: int, payload: bytes) -> Optional[int]:
    if not payload or len(payload) > 16_000_000:
        return None
    sql = """
    INSERT INTO shop_print_agent_jobs (shop_id, payload, status)
    VALUES (%s, %s, 'pending')
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql, (int(shop_id), payload))
            return int(cur.lastrowid)
    except pymysql.Error:
        return None


def list_pending_shop_print_agent_jobs(shop_id: int, limit: int = 20):
    sql = """
    SELECT id, shop_id, payload, created_at
    FROM shop_print_agent_jobs
    WHERE shop_id=%s AND status='pending'
    ORDER BY id ASC
    LIMIT %s
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (int(shop_id), int(limit)))
            return cur.fetchall() or []
    except pymysql.Error:
        return []


def ack_shop_print_agent_job(shop_id: int, job_id: int, *, failed: bool = False, error_message: Optional[str] = None) -> bool:
    if failed:
        sql = """
        UPDATE shop_print_agent_jobs
        SET status='failed', delivered_at=CURRENT_TIMESTAMP, error_message=%s
        WHERE shop_id=%s AND id=%s AND status='pending'
        """
        try:
            with get_cursor(commit=True) as cur:
                cur.execute(sql, ((error_message or "print failed")[:500], int(shop_id), int(job_id)))
                return cur.rowcount > 0
        except pymysql.Error:
            return False
    sql = """
    UPDATE shop_print_agent_jobs
    SET status='delivered', delivered_at=CURRENT_TIMESTAMP, error_message=NULL
    WHERE shop_id=%s AND id=%s AND status='pending'
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql, (int(shop_id), int(job_id)))
            return cur.rowcount > 0
    except pymysql.Error:
        return False


def init_shop_stock_transactions_table():
    sql = """
    CREATE TABLE IF NOT EXISTS shop_stock_transactions (
        id INT AUTO_INCREMENT PRIMARY KEY,
        shop_id INT NOT NULL,
        item_id INT NOT NULL,
        direction ENUM('in', 'out') NOT NULL DEFAULT 'in',
        source ENUM('company', 'manual') NOT NULL,
        qty INT NOT NULL,
        shop_stock_before INT NOT NULL,
        shop_stock_after INT NOT NULL,
        company_stock_before INT NULL,
        company_stock_after INT NULL,
        buying_price DECIMAL(12,2) NULL,
        place_brought_from VARCHAR(255) NULL,
        seller_phone VARCHAR(40) NULL,
        reason VARCHAR(50) NULL,
        refunded TINYINT(1) NOT NULL DEFAULT 0,
        refund_amount DECIMAL(12,2) NULL,
        payment_status ENUM('pending_payment','partially_paid','paid') NOT NULL DEFAULT 'pending_payment',
        amount_paid DECIMAL(12,2) NOT NULL DEFAULT 0.00,
        note VARCHAR(255) NULL,
        created_by_employee_id INT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        KEY idx_sst_shop (shop_id),
        KEY idx_sst_item (item_id),
        KEY idx_sst_created (created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql)
            if not column_exists("shop_stock_transactions", "direction"):
                cur.execute(
                    "ALTER TABLE shop_stock_transactions ADD COLUMN direction ENUM('in','out') NOT NULL DEFAULT 'in' AFTER item_id"
                )
            if not column_exists("shop_stock_transactions", "reason"):
                cur.execute("ALTER TABLE shop_stock_transactions ADD COLUMN reason VARCHAR(50) NULL AFTER place_brought_from")
            if not column_exists("shop_stock_transactions", "seller_phone"):
                cur.execute("ALTER TABLE shop_stock_transactions ADD COLUMN seller_phone VARCHAR(40) NULL AFTER place_brought_from")
            if not column_exists("shop_stock_transactions", "refunded"):
                cur.execute("ALTER TABLE shop_stock_transactions ADD COLUMN refunded TINYINT(1) NOT NULL DEFAULT 0 AFTER reason")
            if not column_exists("shop_stock_transactions", "refund_amount"):
                cur.execute("ALTER TABLE shop_stock_transactions ADD COLUMN refund_amount DECIMAL(12,2) NULL AFTER refunded")
            if not column_exists("shop_stock_transactions", "payment_status"):
                cur.execute(
                    "ALTER TABLE shop_stock_transactions ADD COLUMN payment_status ENUM('pending_payment','partially_paid','paid') NOT NULL DEFAULT 'pending_payment' AFTER refund_amount"
                )
            if not column_exists("shop_stock_transactions", "amount_paid"):
                cur.execute(
                    "ALTER TABLE shop_stock_transactions ADD COLUMN amount_paid DECIMAL(12,2) NOT NULL DEFAULT 0.00 AFTER payment_status"
                )
        logger.info("Table shop_stock_transactions is ready.")
        return True
    except pymysql.Error as e:
        logger.warning("Could not init shop_stock_transactions: %s", e)
        return False


_shop_fifo_schema_ready = False


def ensure_shop_stock_fifo_schema() -> bool:
    """Cost layers + FIFO consumption audit for shop inventory COGS."""
    global _shop_fifo_schema_ready
    if _shop_fifo_schema_ready:
        return True
    init_shop_stock_transactions_table()
    layers_sql = """
    CREATE TABLE IF NOT EXISTS shop_stock_cost_layers (
        id INT AUTO_INCREMENT PRIMARY KEY,
        shop_id INT NOT NULL,
        item_id INT NOT NULL,
        source_transaction_id INT NULL,
        qty_remaining DECIMAL(18,4) NOT NULL,
        unit_cost DECIMAL(12,2) NOT NULL DEFAULT 0.00,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        KEY idx_sscl_shop_item (shop_id, item_id, created_at, id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    cons_sql = """
    CREATE TABLE IF NOT EXISTS shop_stock_fifo_consumptions (
        id INT AUTO_INCREMENT PRIMARY KEY,
        shop_id INT NOT NULL,
        item_id INT NOT NULL,
        layer_id INT NOT NULL,
        out_transaction_id INT NOT NULL,
        qty DECIMAL(18,4) NOT NULL,
        unit_cost DECIMAL(12,2) NOT NULL DEFAULT 0.00,
        line_cost DECIMAL(12,2) NOT NULL DEFAULT 0.00,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        KEY idx_ssfc_out (out_transaction_id),
        KEY idx_ssfc_shop_item (shop_id, item_id, created_at, id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(layers_sql)
            cur.execute(cons_sql)
            if not column_exists("shop_stock_transactions", "cost_total"):
                cur.execute(
                    "ALTER TABLE shop_stock_transactions ADD COLUMN cost_total DECIMAL(12,2) NULL AFTER buying_price"
                )
        _shop_fifo_schema_ready = True
        return True
    except pymysql.Error as e:
        logger.warning("Could not ensure shop stock FIFO schema: %s", e)
        return False


def _shop_fifo_default_unit_cost(cur, shop_id: int, item_id: int) -> float:
    cur.execute(
        """
        SELECT buying_price
        FROM shop_stock_transactions
        WHERE shop_id=%s AND item_id=%s AND direction='in'
          AND buying_price IS NOT NULL AND buying_price > 0
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (int(shop_id), int(item_id)),
    )
    row = cur.fetchone() or {}
    try:
        return round(float(row.get("buying_price") or 0), 2)
    except Exception:
        return 0.0


def _shop_fifo_add_layer(
    cur,
    shop_id: int,
    item_id: int,
    qty,
    unit_cost,
    source_transaction_id: Optional[int] = None,
) -> Optional[int]:
    q = round(float(qty), STOCK_QTY_DECIMAL_PLACES)
    if q <= 0:
        return None
    uc = round(float(unit_cost or 0), 2)
    cur.execute(
        """
        INSERT INTO shop_stock_cost_layers
            (shop_id, item_id, source_transaction_id, qty_remaining, unit_cost)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (int(shop_id), int(item_id), source_transaction_id, q, uc),
    )
    return int(cur.lastrowid or 0) or None


def _shop_fifo_consume(cur, shop_id: int, item_id: int, qty) -> dict:
    need = round(float(qty), STOCK_QTY_DECIMAL_PLACES)
    if need <= 0:
        return {"total_cost": 0.0, "avg_unit_cost": 0.0, "consumptions": []}
    cur.execute(
        """
        SELECT id, qty_remaining, unit_cost
        FROM shop_stock_cost_layers
        WHERE shop_id=%s AND item_id=%s AND qty_remaining > 0
        ORDER BY created_at ASC, id ASC
        FOR UPDATE
        """,
        (int(shop_id), int(item_id)),
    )
    layers = cur.fetchall() or []
    remaining = need
    total_cost = 0.0
    consumptions: list = []
    for layer in layers:
        if remaining <= 1e-12:
            break
        lid = int(layer["id"])
        avail = round(float(layer.get("qty_remaining") or 0), STOCK_QTY_DECIMAL_PLACES)
        if avail <= 0:
            continue
        take = min(remaining, avail)
        unit = round(float(layer.get("unit_cost") or 0), 2)
        line_cost = round(take * unit, 2)
        new_rem = round(avail - take, STOCK_QTY_DECIMAL_PLACES)
        cur.execute(
            "UPDATE shop_stock_cost_layers SET qty_remaining=%s WHERE id=%s",
            (new_rem, lid),
        )
        total_cost += line_cost
        consumptions.append(
            {"layer_id": lid, "qty": take, "unit_cost": unit, "line_cost": line_cost}
        )
        remaining = round(remaining - take, STOCK_QTY_DECIMAL_PLACES)
    if remaining > 1e-12:
        uc = _shop_fifo_default_unit_cost(cur, int(shop_id), int(item_id))
        total_cost += round(remaining * uc, 2)
    total_cost = round(total_cost, 2)
    avg = round(total_cost / need, 2) if need > 0 else 0.0
    return {"total_cost": total_cost, "avg_unit_cost": avg, "consumptions": consumptions}


def _shop_fifo_record_consumptions(
    cur, shop_id: int, item_id: int, out_transaction_id: int, consumptions: list
) -> None:
    for c in consumptions or []:
        cur.execute(
            """
            INSERT INTO shop_stock_fifo_consumptions
                (shop_id, item_id, layer_id, out_transaction_id, qty, unit_cost, line_cost)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                int(shop_id),
                int(item_id),
                int(c["layer_id"]),
                int(out_transaction_id),
                round(float(c["qty"]), STOCK_QTY_DECIMAL_PLACES),
                round(float(c["unit_cost"]), 2),
                round(float(c["line_cost"]), 2),
            ),
        )


def _shop_fifo_restore_qty(cur, shop_id: int, item_id: int, qty) -> float:
    """Reverse recent FIFO consumptions (e.g. held-order qty reduction)."""
    need = round(float(qty), STOCK_QTY_DECIMAL_PLACES)
    if need <= 0:
        return 0.0
    cur.execute(
        """
        SELECT c.id, c.layer_id, c.qty, c.unit_cost, c.line_cost
        FROM shop_stock_fifo_consumptions c
        WHERE c.shop_id=%s AND c.item_id=%s AND c.qty > 0
        ORDER BY c.created_at DESC, c.id DESC
        FOR UPDATE
        """,
        (int(shop_id), int(item_id)),
    )
    rows = cur.fetchall() or []
    remaining = need
    total_cost = 0.0
    for row in rows:
        if remaining <= 1e-12:
            break
        avail = round(float(row.get("qty") or 0), STOCK_QTY_DECIMAL_PLACES)
        if avail <= 0:
            continue
        take = min(remaining, avail)
        unit = round(float(row.get("unit_cost") or 0), 2)
        line_cost = round(take * unit, 2)
        lid = int(row["layer_id"])
        cur.execute(
            """
            UPDATE shop_stock_cost_layers
            SET qty_remaining = qty_remaining + %s
            WHERE id=%s
            """,
            (take, lid),
        )
        new_cq = round(avail - take, STOCK_QTY_DECIMAL_PLACES)
        cid = int(row["id"])
        if new_cq <= 1e-12:
            cur.execute("DELETE FROM shop_stock_fifo_consumptions WHERE id=%s", (cid,))
        else:
            cur.execute(
                """
                UPDATE shop_stock_fifo_consumptions
                SET qty=%s, line_cost=%s
                WHERE id=%s
                """,
                (new_cq, round(new_cq * unit, 2), cid),
            )
        total_cost += line_cost
        remaining = round(remaining - take, STOCK_QTY_DECIMAL_PLACES)
    if remaining > 1e-12:
        uc = _shop_fifo_default_unit_cost(cur, int(shop_id), int(item_id))
        _shop_fifo_add_layer(cur, int(shop_id), int(item_id), remaining, uc, None)
        total_cost += round(remaining * uc, 2)
    return round(total_cost, 2)


def _shop_fifo_backfill_shop_layers(cur, shop_id: Optional[int] = None) -> None:
    params: list = []
    shop_clause = ""
    if shop_id is not None:
        shop_clause = " AND si.shop_id=%s"
        params.append(int(shop_id))
    cur.execute(
        f"""
        SELECT si.shop_id, si.item_id, si.shop_stock_qty,
               COALESCE(SUM(l.qty_remaining), 0) AS layer_qty
        FROM shop_items si
        LEFT JOIN shop_stock_cost_layers l
          ON l.shop_id = si.shop_id AND l.item_id = si.item_id
        WHERE si.shop_stock_qty > 0{shop_clause}
        GROUP BY si.shop_id, si.item_id, si.shop_stock_qty
        HAVING si.shop_stock_qty > COALESCE(SUM(l.qty_remaining), 0) + 0.0001
        """,
        tuple(params),
    )
    for row in cur.fetchall() or []:
        sid = int(row.get("shop_id") or 0)
        iid = int(row.get("item_id") or 0)
        if sid <= 0 or iid <= 0:
            continue
        shop_qty = round(float(row.get("shop_stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
        layer_qty = round(float(row.get("layer_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
        diff = round(shop_qty - layer_qty, STOCK_QTY_DECIMAL_PLACES)
        if diff <= 0:
            continue
        uc = _shop_fifo_default_unit_cost(cur, sid, iid)
        _shop_fifo_add_layer(cur, sid, iid, diff, uc, None)


def _shop_fifo_after_stock_out(cur, shop_id: int, item_id: int, qty, out_tx_id: int) -> dict:
    if not ensure_shop_stock_fifo_schema() or not column_exists("shop_stock_transactions", "cost_total"):
        return {"total_cost": 0.0, "avg_unit_cost": 0.0}
    info = _shop_fifo_consume(cur, int(shop_id), int(item_id), qty)
    if int(out_tx_id or 0) > 0:
        _shop_fifo_record_consumptions(
            cur, int(shop_id), int(item_id), int(out_tx_id), info.get("consumptions") or []
        )
        cur.execute(
            """
            UPDATE shop_stock_transactions
            SET cost_total=%s, buying_price=%s
            WHERE id=%s
            """,
            (
                round(float(info.get("total_cost") or 0), 2),
                round(float(info.get("avg_unit_cost") or 0), 2),
                int(out_tx_id),
            ),
        )
    return info


def _shop_fifo_after_stock_in(
    cur, shop_id: int, item_id: int, qty, unit_cost, in_tx_id: int
) -> None:
    if not ensure_shop_stock_fifo_schema():
        return
    uc = round(float(unit_cost or 0), 2)
    q = round(float(qty), STOCK_QTY_DECIMAL_PLACES)
    if q <= 0:
        return
    _shop_fifo_add_layer(cur, int(shop_id), int(item_id), q, uc, int(in_tx_id) if in_tx_id else None)
    if column_exists("shop_stock_transactions", "cost_total") and int(in_tx_id or 0) > 0:
        cur.execute(
            """
            UPDATE shop_stock_transactions
            SET cost_total=%s, buying_price=%s
            WHERE id=%s
            """,
            (round(q * uc, 2), uc, int(in_tx_id)),
        )


def _shop_fifo_after_stock_restore(cur, shop_id: int, item_id: int, qty, in_tx_id: int) -> None:
    if not ensure_shop_stock_fifo_schema():
        return
    restored_cost = _shop_fifo_restore_qty(cur, int(shop_id), int(item_id), qty)
    if column_exists("shop_stock_transactions", "cost_total") and int(in_tx_id or 0) > 0:
        cur.execute(
            "UPDATE shop_stock_transactions SET cost_total=%s WHERE id=%s",
            (round(restored_cost, 2), int(in_tx_id)),
        )


def backfill_shop_stock_fifo_layers(shop_id: Optional[int] = None) -> bool:
    """Seed cost layers for on-hand stock that predates FIFO tracking."""
    if not ensure_shop_stock_fifo_schema():
        return False
    try:
        with get_cursor(commit=True) as cur:
            _shop_fifo_backfill_shop_layers(cur, shop_id=shop_id)
        return True
    except pymysql.Error as e:
        logger.warning("backfill_shop_stock_fifo_layers failed: %s", e)
        return False


def _shop_fifo_replay_unit_cost_for_in(tx: dict, last_unit_cost: float) -> float:
    """Resolve purchase unit cost for a stock-in row during historical replay."""
    try:
        bp = float(tx.get("buying_price") or 0)
    except Exception:
        bp = 0.0
    if bp > 0:
        return round(bp, 2)
    try:
        ct = float(tx.get("cost_total") or 0)
        q = round(float(tx.get("qty") or 0), STOCK_QTY_DECIMAL_PLACES)
    except Exception:
        ct = 0.0
        q = 0.0
    if ct > 0 and q > 0:
        return round(ct / q, 2)
    return round(float(last_unit_cost or 0), 2)


def _shop_fifo_replay_item_pair(cur, shop_id: int, item_id: int) -> dict:
    """Replay FIFO for one shop/item from all stock transactions (oldest first)."""
    cur.execute(
        """
        SELECT id, direction, qty, buying_price, cost_total, reason
        FROM shop_stock_transactions
        WHERE shop_id=%s AND item_id=%s
        ORDER BY created_at ASC, id ASC
        """,
        (int(shop_id), int(item_id)),
    )
    rows = cur.fetchall() or []
    stats = {
        "transactions": len(rows),
        "ins": 0,
        "outs": 0,
        "outs_costed": 0,
        "out_cost_total": 0.0,
    }
    if not rows:
        return stats
    last_unit_cost = 0.0
    for tx in rows:
        tx_id = int(tx.get("id") or 0)
        direction = (tx.get("direction") or "").strip().lower()
        reason = (tx.get("reason") or "").strip().upper()
        qty = round(float(tx.get("qty") or 0), STOCK_QTY_DECIMAL_PLACES)
        if qty <= 0 or tx_id <= 0:
            continue
        if direction == "in":
            stats["ins"] += 1
            if reason == "POS_HOLD_RET":
                restored = _shop_fifo_restore_qty(cur, int(shop_id), int(item_id), qty)
                if column_exists("shop_stock_transactions", "cost_total"):
                    cur.execute(
                        "UPDATE shop_stock_transactions SET cost_total=%s WHERE id=%s",
                        (round(restored, 2), tx_id),
                    )
                continue
            unit_cost = _shop_fifo_replay_unit_cost_for_in(tx, last_unit_cost)
            if unit_cost > 0:
                last_unit_cost = unit_cost
            _shop_fifo_add_layer(cur, int(shop_id), int(item_id), qty, unit_cost, tx_id)
            if column_exists("shop_stock_transactions", "cost_total"):
                cur.execute(
                    """
                    UPDATE shop_stock_transactions
                    SET cost_total=%s, buying_price=%s
                    WHERE id=%s
                    """,
                    (round(qty * unit_cost, 2), unit_cost, tx_id),
                )
        elif direction == "out":
            stats["outs"] += 1
            info = _shop_fifo_consume(cur, int(shop_id), int(item_id), qty)
            _shop_fifo_record_consumptions(
                cur,
                int(shop_id),
                int(item_id),
                tx_id,
                info.get("consumptions") or [],
            )
            total_cost = round(float(info.get("total_cost") or 0), 2)
            avg_unit = round(float(info.get("avg_unit_cost") or 0), 2)
            if column_exists("shop_stock_transactions", "cost_total"):
                cur.execute(
                    """
                    UPDATE shop_stock_transactions
                    SET cost_total=%s, buying_price=%s
                    WHERE id=%s
                    """,
                    (total_cost, avg_unit, tx_id),
                )
            stats["outs_costed"] += 1
            stats["out_cost_total"] = round(stats["out_cost_total"] + total_cost, 2)
    return stats


def backfill_shop_stock_fifo_historical_cogs(
    shop_id: Optional[int] = None,
    *,
    clear_existing: bool = True,
) -> dict:
    """
    One-time replay of all shop stock movements to assign FIFO ``cost_total`` on
    historical outs (POS, held orders, manual stock out, transfers, etc.).
    """
    out = {
        "ok": False,
        "shop_id": int(shop_id) if shop_id is not None else None,
        "pairs": 0,
        "transactions": 0,
        "outs": 0,
        "outs_costed": 0,
        "out_cost_total": 0.0,
        "error": None,
    }
    if not ensure_shop_stock_fifo_schema():
        out["error"] = "FIFO schema not ready"
        return out
    try:
        with get_cursor(commit=True) as cur:
            if clear_existing:
                if shop_id is not None:
                    cur.execute(
                        "DELETE FROM shop_stock_fifo_consumptions WHERE shop_id=%s",
                        (int(shop_id),),
                    )
                    cur.execute(
                        "DELETE FROM shop_stock_cost_layers WHERE shop_id=%s",
                        (int(shop_id),),
                    )
                else:
                    cur.execute("DELETE FROM shop_stock_fifo_consumptions")
                    cur.execute("DELETE FROM shop_stock_cost_layers")

            pair_sql = """
            SELECT DISTINCT shop_id, item_id
            FROM shop_stock_transactions
            """
            pair_params: list = []
            if shop_id is not None:
                pair_sql += " WHERE shop_id=%s"
                pair_params.append(int(shop_id))
            pair_sql += " ORDER BY shop_id ASC, item_id ASC"
            cur.execute(pair_sql, tuple(pair_params))
            pairs = cur.fetchall() or []

            for pair in pairs:
                sid = int(pair.get("shop_id") or 0)
                iid = int(pair.get("item_id") or 0)
                if sid <= 0 or iid <= 0:
                    continue
                st = _shop_fifo_replay_item_pair(cur, sid, iid)
                out["pairs"] += 1
                out["transactions"] += int(st.get("transactions") or 0)
                out["outs"] += int(st.get("outs") or 0)
                out["outs_costed"] += int(st.get("outs_costed") or 0)
                out["out_cost_total"] = round(
                    float(out["out_cost_total"]) + float(st.get("out_cost_total") or 0), 2
                )
        out["ok"] = True
        logger.info(
            "Historical FIFO COGS backfill complete shop_id=%s pairs=%s outs=%s cost=%.2f",
            shop_id,
            out["pairs"],
            out["outs_costed"],
            out["out_cost_total"],
        )
    except pymysql.Error as e:
        out["error"] = str(e)
        logger.warning("backfill_shop_stock_fifo_historical_cogs failed: %s", e)
    return out


def ensure_shop_stock_fifo_historical_cogs_backfill() -> bool:
    """Run historical FIFO COGS replay once per deployment (site setting gate)."""
    settings = get_site_settings(["shop_fifo_historical_cogs_v1"])
    if (settings.get("shop_fifo_historical_cogs_v1") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "done",
    ):
        return True
    result = backfill_shop_stock_fifo_historical_cogs(clear_existing=True)
    if not result.get("ok"):
        return False
    set_site_settings({"shop_fifo_historical_cogs_v1": "done"})
    return True


def ensure_shop_items_for_shop(shop_id: int):
    """Seed shop_items for every catalog item; defaults follow company active + stock flags."""
    sid = int(shop_id)
    probe_sql = """
    SELECT 1 FROM items i
    WHERE NOT EXISTS (
        SELECT 1 FROM shop_items si
        WHERE si.shop_id = %s AND si.item_id = i.id
      )
    LIMIT 1
    """
    with get_cursor() as cur:
        cur.execute(probe_sql, (sid,))
        if not cur.fetchone():
            return
    sql = """
    INSERT INTO shop_items (shop_id, item_id, shop_stock_qty, stock_update_enabled, displayed)
    SELECT
        %s AS shop_id,
        i.id AS item_id,
        0 AS shop_stock_qty,
        CASE WHEN i.status='active' AND i.stock_update_enabled=1 THEN 1 ELSE 0 END,
        CASE WHEN i.status='active' THEN 1 ELSE 0 END
    FROM items i
    WHERE NOT EXISTS (
        SELECT 1 FROM shop_items si
        WHERE si.shop_id=%s AND si.item_id=i.id
      )
    """
    with get_cursor(commit=True) as cur:
        cur.execute(sql, (sid, sid))


def seed_shop_items_for_company_item(item_id: int, origin_shop_id: Optional[int] = None) -> None:
    """
    Create shop_items rows for a catalog item on every branch.
    When ``origin_shop_id`` is set (branch-registered item), only that shop is displayed;
    all other branches start hidden with stock updates off.
    """
    init_shop_items_table()
    sql_item = "SELECT status, stock_update_enabled FROM items WHERE id=%s LIMIT 1"
    with get_cursor() as cur:
        cur.execute(sql_item, (int(item_id),))
        row = cur.fetchone()
    if not row:
        return
    active = (row.get("status") or "") == "active"
    comp_stock = int(row.get("stock_update_enabled") or 0) == 1
    origin = int(origin_shop_id) if origin_shop_id is not None else None

    shops = list_shops(limit=5000) or []
    insert_sql = """
    INSERT INTO shop_items (shop_id, item_id, shop_stock_qty, stock_update_enabled, displayed)
    VALUES (%s, %s, 0, %s, %s)
    ON DUPLICATE KEY UPDATE
        stock_update_enabled = VALUES(stock_update_enabled),
        displayed = VALUES(displayed)
    """
    with get_cursor(commit=True) as cur:
        for s in shops:
            try:
                sid = int(s.get("id") or 0)
            except (TypeError, ValueError):
                continue
            if sid <= 0:
                continue
            if origin is not None:
                if sid == origin:
                    displayed = 1 if active else 0
                    stock_upd = 1 if active and comp_stock else 0
                else:
                    displayed = 0
                    stock_upd = 0
            else:
                displayed = 1 if active else 0
                stock_upd = 1 if active and comp_stock else 0
            cur.execute(insert_sql, (sid, int(item_id), stock_upd, displayed))


def list_shop_items(shop_id: int, limit: int = 500):
    has_company_levels = column_exists("items", "low_stock_threshold") and column_exists("items", "reorder_level")
    has_shop_levels = column_exists("shop_items", "low_stock_threshold") and column_exists("shop_items", "reorder_level")
    has_shop_selling_price = column_exists("shop_items", "selling_price")
    company_cols = ", i.low_stock_threshold, i.reorder_level" if has_company_levels else ""
    shop_cols = ", si.low_stock_threshold AS shop_low_stock_threshold, si.reorder_level AS shop_reorder_level" if has_shop_levels else ""
    selling_price_col = (
        "COALESCE(si.selling_price, i.selling_price, i.price) AS selling_price"
        if has_shop_selling_price
        else "COALESCE(i.selling_price, i.price) AS selling_price"
    )
    sql = """
    SELECT
        i.id,
        i.category,
        i.name,
        i.description,
        i.price,
        {selling_price_col},
        i.image_path,
        si.shop_stock_qty,
        si.stock_update_enabled AS shop_item_stock_updates,
        si.displayed,
        i.status,
        i.stock_update_enabled AS company_stock_update_enabled,
        i.created_at
        {company_cols}
        {shop_cols}
    FROM items i
    JOIN shop_items si ON si.item_id = i.id AND si.shop_id=%s
    ORDER BY i.category ASC, i.name ASC
    LIMIT %s
    """.format(company_cols=company_cols, shop_cols=shop_cols, selling_price_col=selling_price_col)
    with get_cursor() as cur:
        cur.execute(sql, (int(shop_id), int(limit)))
        rows = cur.fetchall() or []
    # Normalize fields used by templates.
    for r in rows:
        try:
            r["selling_price"] = float(
                r.get("selling_price") if r.get("selling_price") is not None else r.get("price") or 0
            )
        except (TypeError, ValueError):
            r["selling_price"] = float(r.get("price") or 0)
        r["shop_stock_qty"] = round(float(r.get("shop_stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
        # Explicit aliased cols avoid DictCursor overriding duplicate logical column names from JOINs.
        shop_s = r.get("shop_item_stock_updates")
        if shop_s is None:
            shop_s = r.get("stock_update_enabled")
        r["stock_update_enabled"] = int(shop_s or 0)
        r["displayed"] = int(r.get("displayed") or 0)
        r["company_stock_update_enabled"] = int(r.get("company_stock_update_enabled") or 0)
        r["low_stock_threshold"] = int(r.get("low_stock_threshold") or 0) if has_company_levels else 0
        r["reorder_level"] = int(r.get("reorder_level") or 0) if has_company_levels else 0
        r["shop_low_stock_threshold"] = int(r.get("shop_low_stock_threshold") or 0) if has_shop_levels else 0
        r["shop_reorder_level"] = int(r.get("shop_reorder_level") or 0) if has_shop_levels else 0
    return rows


def list_shop_pos_items(shop_id: int, limit: int = 2000):
    """
    Items for Shop POS: only active system items that are marked displayed for this shop.
    """
    has_shop_selling_price = column_exists("shop_items", "selling_price")
    selling_price_col = (
        "COALESCE(si.selling_price, i.selling_price, i.price) AS selling_price"
        if has_shop_selling_price
        else "COALESCE(i.selling_price, i.price) AS selling_price"
    )
    sql = """
    SELECT
        i.id,
        i.category,
        i.name,
        i.description,
        i.price AS original_selling_price,
        {selling_price_col},
        i.image_path,
        si.shop_stock_qty,
        si.stock_update_enabled AS shop_pos_inventory_toggle,
        si.displayed,
        COALESCE(skp.portions_remaining, 0) AS kitchen_portions
    FROM items i
    JOIN shop_items si ON si.item_id = i.id AND si.shop_id=%s
    LEFT JOIN shop_kitchen_portions skp
        ON skp.shop_id = si.shop_id AND skp.item_id = i.id
    WHERE i.status='active' AND si.displayed=1
    ORDER BY i.category ASC, i.name ASC
    LIMIT %s
    """.format(selling_price_col=selling_price_col)
    with get_cursor() as cur:
        cur.execute(sql, (int(shop_id), int(limit)))
        rows = cur.fetchall() or []
    for r in rows:
        r["shop_stock_qty"] = round(float(r.get("shop_stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
        r["stock_update_enabled"] = int(r.get("shop_pos_inventory_toggle") or r.get("stock_update_enabled") or 0)
        r["kitchen_portions"] = int(r.get("kitchen_portions") or 0)
        r["displayed"] = int(r.get("displayed") or 0)
        try:
            orig = float(r.get("original_selling_price") or 0)
        except (TypeError, ValueError):
            orig = 0.0
        try:
            sell = float(r.get("selling_price") if r.get("selling_price") is not None else orig)
        except (TypeError, ValueError):
            sell = orig
        r["price"] = sell
        r["selling_price"] = sell
        r["original_selling_price"] = orig
    return rows


def toggle_shop_item_displayed(shop_id: int, item_id: int) -> bool:
    """Shop may hide/show on POS only while the company item is active; turning on is blocked if suspended."""
    sel = """
    SELECT si.displayed, i.status
    FROM shop_items si
    INNER JOIN items i ON i.id = si.item_id
    WHERE si.shop_id=%s AND si.item_id=%s
    LIMIT 1
    """
    with get_cursor() as cur:
        cur.execute(sel, (int(shop_id), int(item_id)))
        row = cur.fetchone()
    if not row:
        return False
    cur_d = int(row.get("displayed") or 0)
    new_d = 1 - cur_d
    if new_d == 1 and (row.get("status") or "") != "active":
        return False
    upd = "UPDATE shop_items SET displayed=%s WHERE shop_id=%s AND item_id=%s"
    ver = "SELECT displayed FROM shop_items WHERE shop_id=%s AND item_id=%s LIMIT 1"
    with get_cursor(commit=True) as cur:
        cur.execute(upd, (new_d, int(shop_id), int(item_id)))
        cur.execute(ver, (int(shop_id), int(item_id)))
        vr = cur.fetchone() or {}
    return int(vr.get("displayed") or 0) == new_d


def set_all_shop_items_displayed(shop_id: int, displayed: bool) -> int:
    """Turn display on/off for every shop item row (on requires company item active)."""
    sid = int(shop_id)
    with get_cursor(commit=True) as cur:
        if displayed:
            cur.execute(
                """
                UPDATE shop_items si
                INNER JOIN items i ON i.id = si.item_id
                SET si.displayed = 1
                WHERE si.shop_id = %s AND i.status = 'active'
                """,
                (sid,),
            )
        else:
            cur.execute("UPDATE shop_items SET displayed=0 WHERE shop_id=%s", (sid,))
        return int(cur.rowcount or 0)


def set_all_shop_items_stock_update_enabled(shop_id: int, enabled: bool) -> int:
    """Turn shop stock/POS updates on/off for every item (on requires company active + master on)."""
    sid = int(shop_id)
    with get_cursor(commit=True) as cur:
        if enabled:
            cur.execute(
                """
                UPDATE shop_items si
                INNER JOIN items i ON i.id = si.item_id
                SET si.stock_update_enabled = 1
                WHERE si.shop_id = %s
                  AND i.status = 'active'
                  AND i.stock_update_enabled = 1
                """,
                (sid,),
            )
        else:
            cur.execute(
                "UPDATE shop_items SET stock_update_enabled=0 WHERE shop_id=%s",
                (sid,),
            )
        return int(cur.rowcount or 0)


def toggle_shop_item_stock_update_enabled(shop_id: int, item_id: int) -> bool:
    """Shop stock updates may be enabled only when the company item is active and company stock updates are on."""
    sel = """
    SELECT
      si.stock_update_enabled AS shop_si_stock_upd,
      i.status AS company_item_status,
      i.stock_update_enabled AS company_stock_upd
    FROM shop_items si
    INNER JOIN items i ON i.id = si.item_id
    WHERE si.shop_id=%s AND si.item_id=%s
    LIMIT 1
    """
    with get_cursor() as cur:
        cur.execute(sel, (int(shop_id), int(item_id)))
        row = cur.fetchone()
    if not row:
        return False
    cur_s = int(row.get("shop_si_stock_upd") or row.get("stock_update_enabled") or 0)
    new_s = 1 - cur_s
    if new_s == 1:
        if (
            (row.get("company_item_status") or "") != "active"
            or int(row.get("company_stock_upd") or 0) != 1
        ):
            return False
    upd = "UPDATE shop_items SET stock_update_enabled=%s WHERE shop_id=%s AND item_id=%s"
    ver = "SELECT COALESCE(stock_update_enabled,0) AS si_stock_save FROM shop_items WHERE shop_id=%s AND item_id=%s LIMIT 1"
    with get_cursor(commit=True) as cur:
        cur.execute(upd, (new_s, int(shop_id), int(item_id)))
        cur.execute(ver, (int(shop_id), int(item_id)))
        vr = cur.fetchone() or {}
    return int(vr.get("si_stock_save") or 0) == new_s


def set_shop_item_stock_alert_levels(shop_id: int, item_id: int, low_stock_threshold: int, reorder_level: int) -> bool:
    """Save per-shop override thresholds; 0 means fallback to company defaults."""
    if not column_exists("shop_items", "low_stock_threshold") or not column_exists("shop_items", "reorder_level"):
        return False
    lo = max(0, min(999999, int(low_stock_threshold or 0)))
    rl = max(0, min(999999, int(reorder_level or 0)))
    sql = """
    UPDATE shop_items
    SET low_stock_threshold=%s, reorder_level=%s
    WHERE shop_id=%s AND item_id=%s
    """
    with get_cursor(commit=True) as cur:
        cur.execute(sql, (lo, rl, int(shop_id), int(item_id)))
        return cur.rowcount > 0


def get_shop_item_stock_movement_summary(shop_id: int, lookback_days: int = 30) -> Dict[int, Dict[str, Any]]:
    """
    Per-item movement summary used for reorder suggestions.
    Includes POS-driven outs and manual stock outs/ins from shop_stock_transactions.
    """
    days = max(7, min(120, int(lookback_days or 30)))
    out_sql = f"""
    SELECT
        sst.item_id,
        SUM(CASE WHEN sst.direction='out' THEN sst.qty ELSE 0 END) AS out_qty,
        SUM(CASE WHEN sst.direction='in' THEN sst.qty ELSE 0 END) AS in_qty,
        COUNT(DISTINCT CASE WHEN sst.direction='out' THEN DATE(sst.created_at) END) AS out_days
    FROM shop_stock_transactions sst
    WHERE sst.shop_id=%s
      AND sst.created_at >= (NOW() - INTERVAL {days} DAY)
    GROUP BY sst.item_id
    """
    with get_cursor() as cur:
        cur.execute(out_sql, (int(shop_id),))
        rows = cur.fetchall() or []
    out: Dict[int, Dict[str, Any]] = {}
    for r in rows:
        try:
            iid = int(r.get("item_id") or 0)
        except Exception:
            continue
        if iid <= 0:
            continue
        out[iid] = {
            "out_qty": round(float(r.get("out_qty") or 0), STOCK_QTY_DECIMAL_PLACES),
            "in_qty": round(float(r.get("in_qty") or 0), STOCK_QTY_DECIMAL_PLACES),
            "out_days": int(r.get("out_days") or 0),
            "lookback_days": days,
        }
    return out


def list_shop_stock_manage_items(shop_id: int, limit: int = 500):
    """Rows keyed by ``items.id`` / ``shop_items.item_id`` (matches manual stock-in / POS shelf flows).

    In POS inventory mode ``both`` (kitchen + shelf), only items flagged ``store_stock_registered``
    are listed — ``store_stock_items`` SKUs are a separate catalog and cannot be saved via
    ``shop_manual_stock_in``, which always targets ``shop_items``.
    """
    inv_mode = resolve_shop_pos_inventory_mode(int(shop_id))
    if inv_mode == "kitchen":
        return []
    if inv_mode == "both" and not column_exists("shop_items", "store_stock_registered"):
        return []
    reg_filter = ""
    shop_tracking_filter = ""
    if inv_mode == "both" and column_exists("shop_items", "store_stock_registered"):
        reg_filter = " AND COALESCE(si.store_stock_registered,0) = 1 "
    elif inv_mode in ("shop", "none"):
        shop_tracking_filter = " AND COALESCE(si.stock_update_enabled,0) = 1 "
    reg_col = ""
    if column_exists("shop_items", "store_stock_registered"):
        reg_col = ", COALESCE(si.store_stock_registered,0) AS store_stock_registered"
    else:
        reg_col = ", 0 AS store_stock_registered"
    sql = f"""
    SELECT
        i.id,
        i.category,
        i.name,
        i.image_path,
        i.stock_qty AS company_stock_qty,
        si.shop_stock_qty,
        si.stock_update_enabled,
        si.displayed
        {reg_col},
        (
            SELECT sst.buying_price
            FROM shop_stock_transactions sst
            WHERE sst.shop_id = si.shop_id
              AND sst.item_id = i.id
              AND sst.direction = 'in'
              AND sst.buying_price IS NOT NULL
            ORDER BY sst.id DESC
            LIMIT 1
        ) AS last_buying_price
    FROM items i
    JOIN shop_items si ON si.item_id=i.id AND si.shop_id=%s
    WHERE i.status='active'
    {reg_filter}{shop_tracking_filter}
    ORDER BY i.category ASC, i.name ASC
    LIMIT %s
    """
    with get_cursor() as cur:
        cur.execute(sql, (int(shop_id), int(limit)))
        rows = cur.fetchall() or []
    for r in rows:
        r["company_stock_qty"] = round(float(r.get("company_stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
        r["shop_stock_qty"] = round(float(r.get("shop_stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
        r["stock_update_enabled"] = int(r.get("stock_update_enabled") or 0)
        r["displayed"] = int(r.get("displayed") or 0)
        r["store_stock_registered"] = int(r.get("store_stock_registered") or 0)
        lp = r.get("last_buying_price")
        r["last_buying_price"] = float(lp) if lp is not None else None
    return rows


def list_shop_store_stock_registration_candidates(shop_id: int, limit: int = 500):
    """Active items on the shop not yet registered for shelf stock (dual kitchen + store mode)."""
    if resolve_shop_pos_inventory_mode(int(shop_id)) != "both":
        return []
    if not column_exists("shop_items", "store_stock_registered"):
        return []
    sql = """
    SELECT
        i.id,
        i.category,
        i.name,
        i.image_path,
        si.shop_stock_qty
    FROM items i
    JOIN shop_items si ON si.item_id = i.id AND si.shop_id = %s
    WHERE i.status = 'active'
      AND COALESCE(si.store_stock_registered, 0) = 0
    ORDER BY i.category ASC, i.name ASC
    LIMIT %s
    """
    with get_cursor() as cur:
        cur.execute(sql, (int(shop_id), int(limit)))
        rows = cur.fetchall() or []
    for r in rows:
        r["shop_stock_qty"] = round(float(r.get("shop_stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
    return rows


def set_shop_store_stock_registered(shop_id: int, item_id: int, enabled: bool) -> bool:
    if resolve_shop_pos_inventory_mode(int(shop_id)) != "both":
        return False
    if not column_exists("shop_items", "store_stock_registered"):
        return False
    sql = """
    UPDATE shop_items si
    INNER JOIN items i ON i.id = si.item_id
    SET si.store_stock_registered = %s
    WHERE si.shop_id = %s AND si.item_id = %s AND i.status = 'active'
    """
    val = 1 if enabled else 0
    with get_cursor(commit=True) as cur:
        cur.execute(sql, (val, int(shop_id), int(item_id)))
        return cur.rowcount > 0


def get_request_source_stock_snapshot(
    *,
    requesting_shop_id: int,
    batch_request_source: str,
    limit: int = 500,
) -> Tuple[str, dict]:
    """
    Label + map item_id -> stock qty at the chosen request source (company or another shop).
    Used for shop stock management UI when switching "request from".
    """
    batch_request_source = (batch_request_source or "company").strip().lower()
    requesting_shop_id = int(requesting_shop_id)
    limit = max(1, min(int(limit), 2000))
    rows = list_shop_stock_manage_items(shop_id=requesting_shop_id, limit=limit)
    item_ids = [int(r.get("id") or 0) for r in rows if int(r.get("id") or 0) > 0]
    if not item_ids:
        return "Company", {}
    if resolve_shop_pos_inventory_mode(int(requesting_shop_id)) == "both":
        return "Shelf stock", {iid: 0 for iid in item_ids}

    source_type = "company"
    source_shop_id: Optional[int] = None
    if batch_request_source.startswith("shop:"):
        source_type = "shop"
        try:
            source_shop_id = int(batch_request_source.split(":", 1)[1])
        except Exception:
            source_shop_id = None
        if not source_shop_id or source_shop_id == requesting_shop_id:
            return "Company", {
                iid: round(
                    float(next((r.get("company_stock_qty") for r in rows if int(r.get("id") or 0) == iid), 0) or 0),
                    STOCK_QTY_DECIMAL_PLACES,
                )
                for iid in item_ids
            }

    out: dict[int, float] = {}
    if source_type == "company":
        label = "Company warehouse"
        try:
            with get_cursor() as cur:
                placeholders = ",".join(["%s"] * len(item_ids))
                cur.execute(
                    f"SELECT id, stock_qty FROM items WHERE id IN ({placeholders})",
                    tuple(item_ids),
                )
                pending_map = _pending_outbound_qty_map_for_source(
                    cur, item_ids=item_ids, source_type="company", source_shop_id=None
                )
                for r in cur.fetchall() or []:
                    iid = int(r["id"])
                    physical = round(float(r.get("stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
                    out[iid] = _available_qty_after_pending(physical, pending_map.get(iid, 0.0))
        except pymysql.Error:
            out = {iid: 0.0 for iid in item_ids}
        for iid in item_ids:
            out.setdefault(iid, 0.0)
        return label, out

    label = f"Shop #{int(source_shop_id)}"
    try:
        with get_cursor() as cur:
            cur.execute(
                "SELECT shop_name FROM shops WHERE id=%s LIMIT 1",
                (int(source_shop_id),),
            )
            srow = cur.fetchone()
            if srow and (srow.get("shop_name") or "").strip():
                label = (srow.get("shop_name") or "").strip()
    except pymysql.Error:
        pass

    try:
        with get_cursor() as cur:
            placeholders = ",".join(["%s"] * len(item_ids))
            cur.execute(
                f"""
                SELECT item_id, COALESCE(shop_stock_qty, 0) AS q
                FROM shop_items
                WHERE shop_id=%s AND item_id IN ({placeholders})
                """,
                tuple([int(source_shop_id)] + item_ids),
            )
            pending_map = _pending_outbound_qty_map_for_source(
                cur,
                item_ids=item_ids,
                source_type="shop",
                source_shop_id=int(source_shop_id),
            )
            for r in cur.fetchall() or []:
                iid = int(r["item_id"])
                physical = round(float(r.get("q") or 0), STOCK_QTY_DECIMAL_PLACES)
                out[iid] = _available_qty_after_pending(physical, pending_map.get(iid, 0.0))
    except pymysql.Error:
        out = {}
    for iid in item_ids:
        out.setdefault(iid, 0.0)
    return label, out


def list_shop_stock_transactions(shop_id: int, item_id: Optional[int] = None, limit: int = 200):
    if item_id:
        sql = """
        SELECT sst.id, sst.direction, sst.source, sst.qty, sst.shop_stock_before, sst.shop_stock_after,
               sst.company_stock_before, sst.company_stock_after,
               sst.buying_price, sst.place_brought_from, sst.seller_phone, sst.reason, sst.refunded, sst.refund_amount,
               sst.payment_status, sst.amount_paid, sst.note, sst.created_at,
               i.name AS item_name
        FROM shop_stock_transactions sst
        LEFT JOIN items i ON i.id = sst.item_id
        WHERE sst.shop_id=%s AND sst.item_id=%s
        ORDER BY sst.created_at DESC
        LIMIT %s
        """
        params = (int(shop_id), int(item_id), int(limit))
    else:
        sql = """
        SELECT sst.id, sst.direction, sst.source, sst.qty, sst.shop_stock_before, sst.shop_stock_after,
               sst.company_stock_before, sst.company_stock_after,
               sst.buying_price, sst.place_brought_from, sst.seller_phone, sst.reason, sst.refunded, sst.refund_amount,
               sst.payment_status, sst.amount_paid, sst.note, sst.created_at,
               i.name AS item_name
        FROM shop_stock_transactions sst
        LEFT JOIN items i ON i.id = sst.item_id
        WHERE sst.shop_id=%s
        ORDER BY sst.created_at DESC
        LIMIT %s
        """
        params = (int(shop_id), int(limit))
    with get_cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall() or []


def get_shop_stock_in_receipt_row(shop_id: int, tx_id: int):
    """Single stock-in transaction row enriched for receipt printing."""
    sql = """
    SELECT
        sst.id,
        sst.shop_id,
        sst.item_id,
        sst.source,
        sst.direction,
        sst.qty,
        sst.buying_price,
        sst.place_brought_from,
        sst.seller_phone,
        sst.payment_status,
        sst.amount_paid,
        sst.note,
        sst.created_at,
        sh.shop_name,
        sh.shop_code,
        sh.shop_location,
        i.name AS item_name,
        i.category AS item_category,
        COALESCE(e.full_name, 'UNKNOWN') AS served_by
    FROM shop_stock_transactions sst
    JOIN shops sh ON sh.id = sst.shop_id
    JOIN items i ON i.id = sst.item_id
    LEFT JOIN employees e ON e.id = sst.created_by_employee_id
    WHERE sst.shop_id=%s AND sst.id=%s AND sst.direction='in'
    LIMIT 1
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (int(shop_id), int(tx_id)))
            row = cur.fetchone() or None
    except pymysql.Error:
        return None
    if not row:
        return None
    r = dict(row)
    r["qty"] = int(r.get("qty") or 0)
    r["buying_price"] = float(r.get("buying_price") or 0.0)
    r["amount_paid"] = float(r.get("amount_paid") or 0.0)
    r["total_cost"] = float(r["qty"] * r["buying_price"])
    r["place_brought_from"] = (r.get("place_brought_from") or "").strip() or "-"
    r["seller_phone"] = (r.get("seller_phone") or "").strip() or "-"
    r["served_by"] = (r.get("served_by") or "").strip() or "UNKNOWN"
    r["note"] = (r.get("note") or "").strip()
    r["payment_status"] = (r.get("payment_status") or "pending_payment").strip().lower()
    return r


def get_shop_operational_expense_receipt_row(shop_id: int, expense_id: int):
    """Single operational expense row enriched for thermal receipt printing."""
    sql = """
    SELECT
        soe.id,
        soe.shop_id,
        soe.qty,
        soe.unit_price,
        soe.total_amount,
        soe.note,
        soe.created_at,
        soe.category_name,
        soe.expense_name,
        soe.supplier_name,
        soe.seller_phone,
        sh.shop_name,
        sh.shop_code,
        sh.shop_location,
        COALESCE(e.full_name, 'UNKNOWN') AS served_by
    FROM shop_operational_expenses soe
    JOIN shops sh ON sh.id = soe.shop_id
    LEFT JOIN employees e ON e.id = soe.created_by_employee_id
    WHERE soe.shop_id=%s AND soe.id=%s
    LIMIT 1
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (int(shop_id), int(expense_id)))
            row = cur.fetchone() or None
    except pymysql.Error:
        return None
    if not row:
        return None
    r = dict(row)
    try:
        qty = float(r.get("qty") or 0)
    except Exception:
        qty = 0.0
    unit = float(r.get("unit_price") or 0.0)
    total = float(r.get("total_amount") or 0.0)
    if total <= 0 and qty and unit:
        total = round(qty * unit, 2)
    r["qty"] = qty
    r["buying_price"] = unit
    r["total_cost"] = total
    r["item_name"] = (r.get("expense_name") or "").strip() or "Expense"
    r["category_name"] = (r.get("category_name") or "").strip() or "—"
    r["place_brought_from"] = (r.get("supplier_name") or "").strip() or "-"
    r["seller_phone"] = (r.get("seller_phone") or "").strip() or "-"
    r["served_by"] = (r.get("served_by") or "").strip() or "UNKNOWN"
    r["note"] = (r.get("note") or "").strip()
    return r


_TRANSFER_DELIVERED_BY_NOTE_RE = re.compile(r"\s·\sDelivered by:\s*(.*)$", re.IGNORECASE)


def _parse_delivered_by_from_transfer_note(note: Optional[str]) -> str:
    if not note:
        return ""
    m = _TRANSFER_DELIVERED_BY_NOTE_RE.search(str(note))
    return (m.group(1).strip() if m else "")


def _transfer_note_with_delivered_by(base_note: Optional[str], delivered_by: Optional[str]) -> Optional[str]:
    """Append POS delivery-note signer; empty string leaves a blank line on the printed receipt."""
    if delivered_by is None:
        return (base_note or "").strip() or None
    base = (base_note or "").strip()
    name = str(delivered_by).strip()
    suffix = f" · Delivered by: {name}"
    combined = (base + suffix).strip() if base else suffix.strip()
    return combined or None


def get_shop_stock_transfer_out_receipt_row(*, shop_id: int, stock_request_id: int):
    """Transfer-out slip for a source shop after approving an incoming stock request."""
    shop_id = int(shop_id)
    stock_request_id = int(stock_request_id)
    note_like = f"%request #{stock_request_id}%"
    approved_note_like = f"%Approved request #{stock_request_id}%"
    has_sr_col = column_exists("shop_stock_transactions", "stock_request_id")
    if has_sr_col:
        sql = """
        SELECT
            sst.id, sst.shop_id, sst.item_id, sst.source, sst.direction, sst.qty, sst.note, sst.created_at,
            sh.shop_name, sh.shop_code, sh.shop_location,
            i.name AS item_name, i.category AS item_category,
            COALESCE(e.full_name, 'UNKNOWN') AS served_by,
            rq.shop_name AS transfer_to_shop_name,
            sr.id AS stock_request_id
        FROM shop_stock_transactions sst
        JOIN shops sh ON sh.id = sst.shop_id
        JOIN items i ON i.id = sst.item_id
        LEFT JOIN employees e ON e.id = sst.created_by_employee_id
        LEFT JOIN shop_stock_requests sr ON sr.id = sst.stock_request_id
        LEFT JOIN shops rq ON rq.id = sr.requesting_shop_id
        WHERE sst.shop_id=%s
          AND sst.direction='out'
          AND sst.source='transfer'
          AND (sst.stock_request_id=%s OR sst.note LIKE %s OR sst.note LIKE %s)
        ORDER BY sst.id DESC
        LIMIT 1
        """
        params = (shop_id, stock_request_id, note_like, approved_note_like)
    else:
        sql = """
        SELECT
            sst.id, sst.shop_id, sst.item_id, sst.source, sst.direction, sst.qty, sst.note, sst.created_at,
            sh.shop_name, sh.shop_code, sh.shop_location,
            i.name AS item_name, i.category AS item_category,
            COALESCE(e.full_name, 'UNKNOWN') AS served_by,
            rq.shop_name AS transfer_to_shop_name,
            sr.id AS stock_request_id
        FROM shop_stock_transactions sst
        JOIN shops sh ON sh.id = sst.shop_id
        JOIN items i ON i.id = sst.item_id
        LEFT JOIN employees e ON e.id = sst.created_by_employee_id
        LEFT JOIN shop_stock_requests sr ON sr.id=%s AND sr.source_shop_id = sst.shop_id
        LEFT JOIN shops rq ON rq.id = sr.requesting_shop_id
        WHERE sst.shop_id=%s
          AND sst.direction='out'
          AND sst.source='transfer'
          AND (sr.id IS NOT NULL OR sst.note LIKE %s OR sst.note LIKE %s)
        ORDER BY sst.id DESC
        LIMIT 1
        """
        params = (stock_request_id, shop_id, note_like, approved_note_like)
    try:
        with get_cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone() or None
    except pymysql.Error:
        return None
    if not row:
        return None
    r = dict(row)
    qty = round(float(r.get("qty") or 0), STOCK_QTY_DECIMAL_PLACES)
    r["qty"] = qty
    r["buying_price"] = 0.0
    r["amount_paid"] = 0.0
    r["total_cost"] = 0.0
    dest = (r.get("transfer_to_shop_name") or "").strip() or "Receiving shop"
    r["place_brought_from"] = dest
    r["seller_phone"] = "TRANSFER OUT"
    r["served_by"] = (r.get("served_by") or "").strip() or "UNKNOWN"
    r["payment_status"] = "paid"
    r["transfer_request_id"] = int(r.get("stock_request_id") or stock_request_id or 0)
    r["delivered_by"] = _parse_delivered_by_from_transfer_note(r.get("note") or "")
    return r


def get_latest_shop_manual_stock_in_tx_id(shop_id: int, item_id: int, created_by_employee_id: Optional[int] = None) -> Optional[int]:
    """Best-effort latest manual stock-in tx id for recent redirect/receipt links."""
    params = [int(shop_id), int(item_id)]
    where_emp = ""
    if created_by_employee_id:
        where_emp = " AND created_by_employee_id=%s"
        params.append(int(created_by_employee_id))
    sql = f"""
    SELECT id
    FROM shop_stock_transactions
    WHERE shop_id=%s
      AND item_id=%s
      AND direction='in'
      AND source='manual'
      {where_emp}
    ORDER BY id DESC
    LIMIT 1
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(params))
            row = cur.fetchone() or {}
            tid = int(row.get("id") or 0)
            return tid if tid > 0 else None
    except pymysql.Error:
        return None


def list_manual_shop_stock_ins(limit: int = 1500) -> list:
    """Manual stock-in rows across all shops, newest first."""
    sql = """
    SELECT
        sst.id,
        sst.created_at,
        sst.shop_id,
        sh.shop_name,
        sst.item_id,
        i.name AS item_name,
        sst.qty,
        sst.buying_price,
        sst.place_brought_from,
        sst.payment_status,
        sst.amount_paid,
        COALESCE(e.full_name, 'UNKNOWN') AS created_by
    FROM shop_stock_transactions sst
    JOIN shops sh ON sh.id = sst.shop_id
    JOIN items i ON i.id = sst.item_id
    LEFT JOIN employees e ON e.id = sst.created_by_employee_id
    WHERE sst.direction='in' AND sst.source='manual'
    ORDER BY sst.created_at DESC
    LIMIT %s
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (int(limit),))
            rows = cur.fetchall() or []
    except pymysql.Error:
        return []
    out = []
    for r in rows:
        rr = dict(r)
        rr["qty"] = int(rr.get("qty") or 0)
        rr["buying_price"] = float(rr.get("buying_price") or 0.0)
        rr["total_cost"] = float(rr["qty"] * rr["buying_price"])
        rr["amount_paid"] = float(rr.get("amount_paid") or 0.0)
        rr["payment_status"] = (rr.get("payment_status") or "pending_payment").strip().lower()
        rr["place_brought_from"] = (rr.get("place_brought_from") or "").strip() or "-"
        out.append(rr)
    return out


def update_shop_manual_stock_in_payment(
    tx_id: int,
    amount_paid: Optional[float] = None,
    *,
    additional_payment: Optional[float] = None,
) -> Optional[dict]:
    """Update amount paid + payment status for one manual stock-in transaction.

    Pass either ``amount_paid`` (absolute new total) or ``additional_payment`` (added to
    the current stored amount) — not both; ``additional_payment`` wins when provided.
    """
    with get_cursor(commit=True) as cur:
        cur.execute(
            """
            SELECT id, qty, buying_price, COALESCE(amount_paid, 0) AS amount_paid
            FROM shop_stock_transactions
            WHERE id=%s AND direction='in' AND source='manual'
            FOR UPDATE
            """,
            (int(tx_id),),
        )
        row = cur.fetchone() or {}
        if not row:
            return None
        qty = int(row.get("qty") or 0)
        buying_price = float(row.get("buying_price") or 0.0)
        total_cost = float(qty * buying_price)
        current_paid = float(row.get("amount_paid") or 0.0)

        if additional_payment is not None:
            try:
                add = float(additional_payment)
            except Exception:
                return None
            if add < 0:
                return None
            new_paid = round(current_paid + add, 2)
        else:
            try:
                new_paid = round(float(amount_paid or 0), 2)
            except Exception:
                return None
            if new_paid < 0:
                return None

        if total_cost <= 0:
            status = "paid"
        elif new_paid <= 0:
            status = "pending_payment"
        elif new_paid < total_cost:
            status = "partially_paid"
        else:
            status = "paid"
        cur.execute(
            """
            UPDATE shop_stock_transactions
            SET amount_paid=%s, payment_status=%s
            WHERE id=%s
            """,
            (new_paid, status, int(tx_id)),
        )
        return {
            "id": int(tx_id),
            "qty": qty,
            "buying_price": buying_price,
            "total_cost": total_cost,
            "amount_paid": new_paid,
            "payment_status": status,
        }


def update_company_stock_in_payment(
    tx_id: int,
    amount_paid: Optional[float] = None,
    *,
    additional_payment: Optional[float] = None,
) -> Optional[dict]:
    """Update ``amount_paid`` / ``payment_status`` on a company ``stock_transactions`` stock-in row."""
    with get_cursor(commit=True) as cur:
        cur.execute(
            """
            SELECT id, qty, buying_price, COALESCE(amount_paid, 0) AS amount_paid
            FROM stock_transactions
            WHERE id=%s AND direction='in'
            FOR UPDATE
            """,
            (int(tx_id),),
        )
        row = cur.fetchone() or {}
        if not row:
            return None
        qty = int(row.get("qty") or 0)
        buying_price = float(row.get("buying_price") or 0.0)
        total_cost = float(qty * buying_price)
        current_paid = float(row.get("amount_paid") or 0.0)

        if additional_payment is not None:
            try:
                add = float(additional_payment)
            except Exception:
                return None
            if add < 0:
                return None
            new_paid = round(current_paid + add, 2)
        else:
            try:
                new_paid = round(float(amount_paid or 0), 2)
            except Exception:
                return None
            if new_paid < 0:
                return None

        if total_cost <= 0:
            status = "paid"
        elif new_paid <= 0:
            status = "pending_payment"
        elif new_paid < total_cost:
            status = "partially_paid"
        else:
            status = "paid"
        cur.execute(
            """
            UPDATE stock_transactions
            SET amount_paid=%s, payment_status=%s
            WHERE id=%s
            """,
            (new_paid, status, int(tx_id)),
        )
        return {
            "id": int(tx_id),
            "qty": qty,
            "buying_price": buying_price,
            "total_cost": total_cost,
            "amount_paid": new_paid,
            "payment_status": status,
        }


def apply_supplier_payment_fifo_from_tx(tx_id: int, additional_payment: float) -> Optional[dict]:
    """Apply payment to selected supplier tx, then spill excess to next supplier tx rows."""
    try:
        pay_amt = float(additional_payment)
    except Exception:
        return None
    if pay_amt <= 0:
        return None

    with get_cursor(commit=True) as cur:
        cur.execute(
            """
            SELECT
              id,
              created_at,
              COALESCE(NULLIF(place_brought_from, ''), '-') AS seller_name,
              COALESCE(NULLIF(seller_phone, ''), '-') AS seller_phone
            FROM shop_stock_transactions
            WHERE id=%s
              AND direction='in'
              AND source='manual'
            LIMIT 1
            FOR UPDATE
            """,
            (int(tx_id),),
        )
        base = cur.fetchone() or {}
        if not base:
            return None

        seller_name = (base.get("seller_name") or "-").strip() or "-"
        seller_phone = (base.get("seller_phone") or "-").strip() or "-"
        base_created_at = base.get("created_at")
        base_id = int(base.get("id") or 0)

        cur.execute(
            """
            SELECT
              id,
              qty,
              buying_price,
              COALESCE(amount_paid, 0) AS amount_paid,
              created_at
            FROM shop_stock_transactions
            WHERE direction='in'
              AND source='manual'
              AND COALESCE(NULLIF(place_brought_from, ''), '-')=%s
              AND COALESCE(NULLIF(seller_phone, ''), '-')=%s
              AND (
                created_at > %s
                OR (created_at = %s AND id >= %s)
              )
            ORDER BY created_at ASC, id ASC
            FOR UPDATE
            """,
            (seller_name, seller_phone, base_created_at, base_created_at, base_id),
        )
        rows = cur.fetchall() or []
        if not rows:
            return None

        allocated = []
        remaining = pay_amt
        for r in rows:
            if remaining <= 0.0001:
                break
            rid = int(r.get("id") or 0)
            qty = int(r.get("qty") or 0)
            buying_price = float(r.get("buying_price") or 0.0)
            total_cost = float(qty * buying_price)
            old_paid = float(r.get("amount_paid") or 0.0)
            due = max(total_cost - old_paid, 0.0)
            if due <= 0.0001:
                continue

            apply_amt = min(due, remaining)
            new_paid = round(old_paid + apply_amt, 2)
            if total_cost <= 0:
                status = "paid"
            elif new_paid <= 0:
                status = "pending_payment"
            elif new_paid < total_cost:
                status = "partially_paid"
            else:
                status = "paid"

            cur.execute(
                """
                UPDATE shop_stock_transactions
                SET amount_paid=%s, payment_status=%s
                WHERE id=%s
                """,
                (new_paid, status, rid),
            )
            allocated.append({"tx_id": rid, "applied": float(apply_amt)})
            remaining -= apply_amt

        return {
            "base_tx_id": base_id,
            "seller_name": seller_name,
            "seller_phone": seller_phone,
            "allocated": allocated,
            "unused": float(max(remaining, 0.0)),
        }


def apply_supplier_payment_fifo_company_from_tx(tx_id: int, additional_payment: float) -> Optional[dict]:
    """FIFO apply payment across company warehouse stock-ins for the same seller (``stock_transactions``)."""
    try:
        pay_amt = float(additional_payment)
    except Exception:
        return None
    if pay_amt <= 0:
        return None

    with get_cursor(commit=True) as cur:
        cur.execute(
            """
            SELECT
              id,
              created_at,
              COALESCE(NULLIF(place_brought_from, ''), '-') AS seller_name,
              COALESCE(NULLIF(seller_phone, ''), '-') AS seller_phone
            FROM stock_transactions
            WHERE id=%s
              AND direction='in'
            LIMIT 1
            FOR UPDATE
            """,
            (int(tx_id),),
        )
        base = cur.fetchone() or {}
        if not base:
            return None

        seller_name = (base.get("seller_name") or "-").strip() or "-"
        seller_phone = (base.get("seller_phone") or "-").strip() or "-"
        base_created_at = base.get("created_at")
        base_id = int(base.get("id") or 0)

        cur.execute(
            """
            SELECT
              id,
              qty,
              buying_price,
              COALESCE(amount_paid, 0) AS amount_paid,
              created_at
            FROM stock_transactions
            WHERE direction='in'
              AND COALESCE(NULLIF(place_brought_from, ''), '-')=%s
              AND COALESCE(NULLIF(seller_phone, ''), '-')=%s
              AND (
                created_at > %s
                OR (created_at = %s AND id >= %s)
              )
            ORDER BY created_at ASC, id ASC
            FOR UPDATE
            """,
            (seller_name, seller_phone, base_created_at, base_created_at, base_id),
        )
        rows = cur.fetchall() or []
        if not rows:
            return None

        allocated = []
        remaining = pay_amt
        for r in rows:
            if remaining <= 0.0001:
                break
            rid = int(r.get("id") or 0)
            qty = int(r.get("qty") or 0)
            buying_price = float(r.get("buying_price") or 0.0)
            total_cost = float(qty * buying_price)
            old_paid = float(r.get("amount_paid") or 0.0)
            due = max(total_cost - old_paid, 0.0)
            if due <= 0.0001:
                continue

            apply_amt = min(due, remaining)
            new_paid = round(old_paid + apply_amt, 2)
            if total_cost <= 0:
                status = "paid"
            elif new_paid <= 0:
                status = "pending_payment"
            elif new_paid < total_cost:
                status = "partially_paid"
            else:
                status = "paid"

            cur.execute(
                """
                UPDATE stock_transactions
                SET amount_paid=%s, payment_status=%s
                WHERE id=%s
                """,
                (new_paid, status, rid),
            )
            allocated.append({"tx_id": rid, "applied": float(apply_amt)})
            remaining -= apply_amt

        return {
            "base_tx_id": base_id,
            "seller_name": seller_name,
            "seller_phone": seller_phone,
            "allocated": allocated,
            "unused": float(max(remaining, 0.0)),
        }


def apply_supplier_payment_fifo_for_seller(
    seller_name: str,
    seller_phone: str,
    additional_payment: float,
    *,
    shop_id: Optional[int] = None,
) -> Optional[dict]:
    """Apply payment FIFO from oldest stock-in across company warehouse and shop manual receipts."""
    try:
        pay_amt = float(additional_payment)
    except Exception:
        return None
    if pay_amt <= 0:
        return None

    sn = (seller_name or "-").strip() or "-"
    sp = (seller_phone or "-").strip() or "-"

    with get_cursor(commit=True) as cur:
        merged: list[dict] = []

        if shop_id is None:
            cur.execute(
                """
                SELECT
                  id,
                  qty,
                  buying_price,
                  COALESCE(amount_paid, 0) AS amount_paid,
                  created_at,
                  'company' AS tx_scope
                FROM stock_transactions
                WHERE direction='in'
                  AND COALESCE(NULLIF(place_brought_from, ''), '-')=%s
                  AND COALESCE(NULLIF(seller_phone, ''), '-')=%s
                ORDER BY created_at ASC, id ASC
                FOR UPDATE
                """,
                (sn, sp),
            )
            merged.extend(cur.fetchall() or [])

        shop_sql = """
            SELECT
              sst.id,
              sst.qty,
              sst.buying_price,
              COALESCE(sst.amount_paid, 0) AS amount_paid,
              sst.created_at,
              'shop' AS tx_scope
            FROM shop_stock_transactions sst
            WHERE sst.direction='in'
              AND sst.source='manual'
              AND COALESCE(NULLIF(sst.place_brought_from, ''), '-')=%s
              AND COALESCE(NULLIF(sst.seller_phone, ''), '-')=%s
        """
        shop_params: list[Any] = [sn, sp]
        if shop_id is not None:
            shop_sql += " AND sst.shop_id=%s"
            shop_params.append(int(shop_id))
        shop_sql += " ORDER BY sst.created_at ASC, sst.id ASC FOR UPDATE"
        cur.execute(shop_sql, tuple(shop_params))
        merged.extend(cur.fetchall() or [])

        merged.sort(key=lambda r: (r.get("created_at"), int(r.get("id") or 0)))
        if not merged:
            return None

        allocated = []
        remaining = pay_amt
        for r in merged:
            if remaining <= 0.0001:
                break
            rid = int(r.get("id") or 0)
            scope = (r.get("tx_scope") or "shop").strip().lower()
            if scope not in ("company", "shop"):
                scope = "shop"
            qty = int(r.get("qty") or 0)
            buying_price = float(r.get("buying_price") or 0.0)
            total_cost = float(qty * buying_price)
            old_paid = float(r.get("amount_paid") or 0.0)
            due = max(total_cost - old_paid, 0.0)
            if due <= 0.0001:
                continue

            apply_amt = min(due, remaining)
            new_paid = round(old_paid + apply_amt, 2)
            if total_cost <= 0:
                status = "paid"
            elif new_paid <= 0:
                status = "pending_payment"
            elif new_paid < total_cost:
                status = "partially_paid"
            else:
                status = "paid"

            if scope == "company":
                cur.execute(
                    """
                    UPDATE stock_transactions
                    SET amount_paid=%s, payment_status=%s
                    WHERE id=%s
                    """,
                    (new_paid, status, rid),
                )
            else:
                cur.execute(
                    """
                    UPDATE shop_stock_transactions
                    SET amount_paid=%s, payment_status=%s
                    WHERE id=%s
                    """,
                    (new_paid, status, rid),
                )
            allocated.append(
                {"tx_id": rid, "tx_scope": scope, "applied": float(apply_amt)}
            )
            remaining -= apply_amt

        if not allocated:
            return None

        return {
            "seller_name": sn,
            "seller_phone": sp,
            "allocated": allocated,
            "unused": float(max(remaining, 0.0)),
        }


def list_shop_stock_audit_rows(
    shop_id: int,
    limit: int = 1000,
    *,
    analytics_filter: Optional[dict] = None,
    direction: Optional[str] = None,
    source: Optional[str] = None,
    item_id: Optional[int] = None,
    search: Optional[str] = None,
):
    """Shop stock transaction audit lines with optional date range and facet filters."""
    af = analytics_filter or {}
    if (af.get("mode") or "").strip().lower() == "all":
        range_where, range_params = "1=1", []
    else:
        range_where, range_params = _analytics_where_clause(af, "sst")
    where_parts = ["sst.shop_id=%s", f"({range_where})"]
    params: list[Any] = [int(shop_id)] + list(range_params)

    d = (direction or "").strip().lower()
    if d in ("in", "out"):
        where_parts.append("sst.direction=%s")
        params.append(d)
    s = (source or "").strip().lower()
    if s in ("company", "manual", "transfer"):
        where_parts.append("sst.source=%s")
        params.append(s)
    try:
        iid = int(item_id) if item_id is not None else 0
    except Exception:
        iid = 0
    if iid > 0:
        where_parts.append("sst.item_id=%s")
        params.append(iid)
    q = (search or "").strip()
    if q:
        like = f"%{q}%"
        where_parts.append(
            "(COALESCE(i.name,'') LIKE %s OR COALESCE(i.category,'') LIKE %s OR CAST(sst.item_id AS CHAR) LIKE %s)"
        )
        params.extend([like, like, like])

    where_sql = " AND ".join(where_parts)
    sql = f"""
    SELECT
        sst.id,
        sst.item_id,
        COALESCE(NULLIF(TRIM(i.category), ''), '—') AS category,
        COALESCE(NULLIF(TRIM(i.name), ''), CONCAT('Item #', sst.item_id)) AS name,
        sst.direction,
        sst.source,
        sst.qty,
        sst.shop_stock_before,
        sst.shop_stock_after,
        sst.company_stock_before,
        sst.company_stock_after,
        sst.buying_price,
        sst.place_brought_from,
        sst.reason,
        sst.refunded,
        sst.refund_amount,
        sst.note,
        sst.created_at,
        COALESCE(NULLIF(TRIM(e.full_name), ''), '') AS moved_by
    FROM shop_stock_transactions sst
    LEFT JOIN items i ON i.id = sst.item_id
    LEFT JOIN employees e ON e.id = sst.created_by_employee_id
    WHERE {where_sql}
    ORDER BY sst.created_at DESC
    LIMIT %s
    """
    params.append(int(limit))
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(params))
            return cur.fetchall() or []
    except pymysql.Error:
        return []


def _format_shop_stock_out_reason_label(raw: Optional[str]) -> Optional[str]:
    s = (raw or "").strip()
    if not s:
        return None
    key = s.lower().replace(" ", "_")
    labels = {
        "pos": "POS sale",
        "pos_hold": "POS hold",
        "pos_hold_ret": "POS hold return",
        "return": "Return",
        "waste": "Waste",
        "display": "Display",
    }
    if key in labels:
        return labels[key]
    return s.replace("_", " ").title()


def get_shop_stock_analytics(shop_id: int, analytics_filter: dict):
    """Return stock movement analytics for a specific shop and date filter."""
    range_where, range_params = _analytics_where_clause(analytics_filter, "sst")
    where_sql = f"sst.shop_id=%s AND {range_where}"
    params = [int(shop_id)] + list(range_params)

    totals_sql = f"""
    SELECT
        COUNT(*) AS tx_count,
        COALESCE(SUM(CASE WHEN sst.direction='in' THEN sst.qty ELSE 0 END), 0) AS qty_in,
        COALESCE(SUM(CASE WHEN sst.direction='out' THEN sst.qty ELSE 0 END), 0) AS qty_out,
        COUNT(DISTINCT sst.item_id) AS distinct_items
    FROM shop_stock_transactions sst
    WHERE {where_sql}
    """
    top_in_sql = f"""
    SELECT
        sst.item_id,
        i.name,
        COALESCE(SUM(sst.qty), 0) AS qty,
        SUM(CASE WHEN sst.buying_price IS NOT NULL THEN sst.qty * sst.buying_price ELSE 0 END)
            / NULLIF(SUM(CASE WHEN sst.buying_price IS NOT NULL THEN sst.qty ELSE 0 END), 0) AS avg_buying_price
    FROM shop_stock_transactions sst
    JOIN items i ON i.id = sst.item_id
    WHERE {where_sql} AND sst.direction='in'
    GROUP BY sst.item_id, i.name
    ORDER BY qty DESC, i.name ASC
    """
    top_in_supplier_sql = f"""
    SELECT
        sst.item_id,
        TRIM(sst.place_brought_from) AS supplier,
        SUM(sst.qty) AS s_qty,
        SUM(sst.qty * sst.buying_price) / NULLIF(SUM(sst.qty), 0) AS w_avg
    FROM shop_stock_transactions sst
    WHERE {where_sql} AND sst.direction='in'
      AND sst.buying_price IS NOT NULL
      AND TRIM(COALESCE(sst.place_brought_from, '')) != ''
    GROUP BY sst.item_id, TRIM(sst.place_brought_from)
    """
    top_out_sql = f"""
    SELECT
        sst.item_id,
        i.name,
        COALESCE(SUM(sst.qty), 0) AS qty
    FROM shop_stock_transactions sst
    JOIN items i ON i.id = sst.item_id
    WHERE {where_sql} AND sst.direction='out'
    GROUP BY sst.item_id, i.name
    ORDER BY qty DESC, i.name ASC
    """
    top_out_reason_sql = f"""
    SELECT
        sst.item_id,
        TRIM(COALESCE(sst.reason, '')) AS reason,
        SUM(sst.qty) AS r_qty,
        COUNT(*) AS r_tx
    FROM shop_stock_transactions sst
    WHERE {where_sql} AND sst.direction='out'
      AND TRIM(COALESCE(sst.reason, '')) != ''
    GROUP BY sst.item_id, TRIM(COALESCE(sst.reason, ''))
    """
    by_day_sql = f"""
    SELECT
        DATE(sst.created_at) AS day,
        COUNT(*) AS tx_count,
        COALESCE(SUM(CASE WHEN sst.direction='in' THEN sst.qty ELSE 0 END), 0) AS qty_in,
        COALESCE(SUM(CASE WHEN sst.direction='out' THEN sst.qty ELSE 0 END), 0) AS qty_out
    FROM shop_stock_transactions sst
    WHERE {where_sql}
    GROUP BY DATE(sst.created_at)
    ORDER BY day DESC
    LIMIT 31
    """
    by_source_sql = f"""
    SELECT
        sst.source,
        sst.direction,
        COUNT(*) AS tx_count,
        COALESCE(SUM(sst.qty), 0) AS qty
    FROM shop_stock_transactions sst
    WHERE {where_sql}
    GROUP BY sst.source, sst.direction
    ORDER BY sst.source ASC, sst.direction ASC
    """

    out = {
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
        with get_cursor() as cur:
            cur.execute(totals_sql, tuple(params))
            t = cur.fetchone() or {}
            out["tx_count"] = int(t.get("tx_count") or 0)
            out["qty_in"] = int(t.get("qty_in") or 0)
            out["qty_out"] = int(t.get("qty_out") or 0)
            out["net_qty"] = out["qty_in"] - out["qty_out"]
            out["distinct_items"] = int(t.get("distinct_items") or 0)

            best_supplier_by_item: Dict[int, str] = {}
            cur.execute(top_in_supplier_sql, tuple(params))
            by_item_suppliers: Dict[int, list] = {}
            for sr in cur.fetchall() or []:
                try:
                    iid = int(sr.get("item_id") or 0)
                except Exception:
                    continue
                if iid <= 0:
                    continue
                by_item_suppliers.setdefault(iid, []).append(sr)
            for iid, rows in by_item_suppliers.items():
                best_sup = None
                best_avg = None
                best_qty = -1
                for sr in rows:
                    try:
                        wa = float(sr.get("w_avg") or 0)
                        sq = int(sr.get("s_qty") or 0)
                    except Exception:
                        continue
                    name = (sr.get("supplier") or "").strip() or "—"
                    if (
                        best_sup is None
                        or wa < (best_avg or 0) - 1e-9
                        or (abs(wa - (best_avg or 0)) < 1e-9 and sq > best_qty)
                    ):
                        best_sup = name
                        best_avg = wa
                        best_qty = sq
                if best_sup:
                    best_supplier_by_item[iid] = best_sup

            cur.execute(top_in_sql, tuple(params))
            top_in_rows = []
            for r in cur.fetchall() or []:
                try:
                    iid = int(r.get("item_id") or 0)
                except Exception:
                    iid = 0
                avg_raw = r.get("avg_buying_price")
                avg_buy = None
                if avg_raw is not None:
                    try:
                        avg_buy = round(float(avg_raw), 2)
                    except Exception:
                        avg_buy = None
                top_in_rows.append(
                    {
                        "item_id": r.get("item_id"),
                        "name": r.get("name") or "Item",
                        "qty": int(r.get("qty") or 0),
                        "avg_buying_price": avg_buy,
                        "best_supplier": best_supplier_by_item.get(iid),
                    }
                )
            out["top_in_items"] = top_in_rows

            main_reason_by_item: Dict[int, str] = {}
            cur.execute(top_out_reason_sql, tuple(params))
            by_item_reasons: Dict[int, list] = {}
            for rr in cur.fetchall() or []:
                try:
                    iid = int(rr.get("item_id") or 0)
                except Exception:
                    continue
                if iid <= 0:
                    continue
                by_item_reasons.setdefault(iid, []).append(rr)
            for iid, rows in by_item_reasons.items():
                main_reason = None
                main_qty = -1
                main_tx = -1
                for rr in rows:
                    try:
                        rq = int(rr.get("r_qty") or 0)
                        rtx = int(rr.get("r_tx") or 0)
                    except Exception:
                        continue
                    raw_reason = (rr.get("reason") or "").strip()
                    label = _format_shop_stock_out_reason_label(raw_reason)
                    if not label:
                        continue
                    if main_reason is None or rq > main_qty or (rq == main_qty and rtx > main_tx):
                        main_reason = label
                        main_qty = rq
                        main_tx = rtx
                if main_reason:
                    main_reason_by_item[iid] = main_reason

            cur.execute(top_out_sql, tuple(params))
            top_out_rows = []
            for r in cur.fetchall() or []:
                try:
                    iid = int(r.get("item_id") or 0)
                except Exception:
                    iid = 0
                top_out_rows.append(
                    {
                        "item_id": r.get("item_id"),
                        "name": r.get("name") or "Item",
                        "qty": int(r.get("qty") or 0),
                        "main_out_reason": main_reason_by_item.get(iid),
                    }
                )
            out["top_out_items"] = top_out_rows

            cur.execute(by_day_sql, tuple(params))
            out["daily"] = [
                {
                    "day": str(r.get("day") or ""),
                    "tx_count": int(r.get("tx_count") or 0),
                    "qty_in": int(r.get("qty_in") or 0),
                    "qty_out": int(r.get("qty_out") or 0),
                    "net_qty": int(r.get("qty_in") or 0) - int(r.get("qty_out") or 0),
                }
                for r in (cur.fetchall() or [])
            ]

            cur.execute(by_source_sql, tuple(params))
            out["source_rows"] = [
                {
                    "source": (r.get("source") or "").upper(),
                    "direction": (r.get("direction") or "").upper(),
                    "tx_count": int(r.get("tx_count") or 0),
                    "qty": int(r.get("qty") or 0),
                }
                for r in (cur.fetchall() or [])
            ]
    except pymysql.Error:
        return out
    return out


def get_company_stock_analytics(analytics_filter: dict):
    """Company-wide stock movement analytics (all shops) for a date filter."""
    range_where, range_params = _analytics_where_clause(analytics_filter, "sst")
    where_sql = f"{range_where}"
    params = list(range_params)

    totals_sql = f"""
    SELECT
        COUNT(*) AS tx_count,
        COALESCE(SUM(CASE WHEN sst.direction='in' THEN sst.qty ELSE 0 END), 0) AS qty_in,
        COALESCE(SUM(CASE WHEN sst.direction='out' THEN sst.qty ELSE 0 END), 0) AS qty_out,
        COUNT(DISTINCT sst.item_id) AS distinct_items,
        COUNT(DISTINCT sst.shop_id) AS distinct_shops
    FROM shop_stock_transactions sst
    WHERE {where_sql}
    """
    top_in_sql = f"""
    SELECT
        sst.item_id,
        i.category,
        i.name,
        COALESCE(SUM(sst.qty), 0) AS qty
    FROM shop_stock_transactions sst
    JOIN items i ON i.id = sst.item_id
    WHERE {where_sql} AND sst.direction='in'
    GROUP BY sst.item_id, i.category, i.name
    ORDER BY qty DESC, i.name ASC
    LIMIT 10
    """
    top_out_sql = f"""
    SELECT
        sst.item_id,
        i.category,
        i.name,
        COALESCE(SUM(sst.qty), 0) AS qty
    FROM shop_stock_transactions sst
    JOIN items i ON i.id = sst.item_id
    WHERE {where_sql} AND sst.direction='out'
    GROUP BY sst.item_id, i.category, i.name
    ORDER BY qty DESC, i.name ASC
    LIMIT 10
    """
    by_day_sql = f"""
    SELECT
        DATE(sst.created_at) AS day,
        COUNT(*) AS tx_count,
        COALESCE(SUM(CASE WHEN sst.direction='in' THEN sst.qty ELSE 0 END), 0) AS qty_in,
        COALESCE(SUM(CASE WHEN sst.direction='out' THEN sst.qty ELSE 0 END), 0) AS qty_out
    FROM shop_stock_transactions sst
    WHERE {where_sql}
    GROUP BY DATE(sst.created_at)
    ORDER BY day DESC
    LIMIT 31
    """
    top_shops_sql = f"""
    SELECT
        sst.shop_id,
        s.shop_name,
        s.shop_code,
        COUNT(*) AS tx_count,
        COALESCE(SUM(CASE WHEN sst.direction='in' THEN sst.qty ELSE 0 END), 0) AS qty_in,
        COALESCE(SUM(CASE WHEN sst.direction='out' THEN sst.qty ELSE 0 END), 0) AS qty_out
    FROM shop_stock_transactions sst
    JOIN shops s ON s.id = sst.shop_id
    WHERE {where_sql}
    GROUP BY sst.shop_id, s.shop_name, s.shop_code
    ORDER BY (qty_in + qty_out) DESC, s.shop_name ASC
    LIMIT 12
    """

    out = {
        "tx_count": 0,
        "qty_in": 0,
        "qty_out": 0,
        "net_qty": 0,
        "distinct_items": 0,
        "distinct_shops": 0,
        "top_in_items": [],
        "top_out_items": [],
        "daily": [],
        "top_shops": [],
    }
    try:
        with get_cursor() as cur:
            cur.execute(totals_sql, tuple(params))
            t = cur.fetchone() or {}
            out["tx_count"] = int(t.get("tx_count") or 0)
            out["qty_in"] = int(t.get("qty_in") or 0)
            out["qty_out"] = int(t.get("qty_out") or 0)
            out["net_qty"] = out["qty_in"] - out["qty_out"]
            out["distinct_items"] = int(t.get("distinct_items") or 0)
            out["distinct_shops"] = int(t.get("distinct_shops") or 0)

            cur.execute(top_in_sql, tuple(params))
            out["top_in_items"] = [
                {
                    "item_id": r.get("item_id"),
                    "category": r.get("category") or "",
                    "name": r.get("name") or "Item",
                    "qty": int(r.get("qty") or 0),
                }
                for r in (cur.fetchall() or [])
            ]

            cur.execute(top_out_sql, tuple(params))
            out["top_out_items"] = [
                {
                    "item_id": r.get("item_id"),
                    "category": r.get("category") or "",
                    "name": r.get("name") or "Item",
                    "qty": int(r.get("qty") or 0),
                }
                for r in (cur.fetchall() or [])
            ]

            cur.execute(by_day_sql, tuple(params))
            out["daily"] = [
                {
                    "day": str(r.get("day") or ""),
                    "tx_count": int(r.get("tx_count") or 0),
                    "qty_in": int(r.get("qty_in") or 0),
                    "qty_out": int(r.get("qty_out") or 0),
                    "net_qty": int(r.get("qty_in") or 0) - int(r.get("qty_out") or 0),
                }
                for r in (cur.fetchall() or [])
            ]

            cur.execute(top_shops_sql, tuple(params))
            out["top_shops"] = [
                {
                    "shop_id": r.get("shop_id"),
                    "shop_name": r.get("shop_name") or "",
                    "shop_code": r.get("shop_code") or "",
                    "tx_count": int(r.get("tx_count") or 0),
                    "qty_in": int(r.get("qty_in") or 0),
                    "qty_out": int(r.get("qty_out") or 0),
                    "net_qty": int(r.get("qty_in") or 0) - int(r.get("qty_out") or 0),
                }
                for r in (cur.fetchall() or [])
            ]
    except pymysql.Error:
        return out
    return out


def get_company_stock_status(
    *,
    limit_items: int = 1000,
    inventory_mode: Optional[str] = None,
    only_active: bool = False,
):
    """
    Return company stock matrix:
      - company_stock: items.stock_qty
      - per shop stock: shop_items.shop_stock_qty
    Result is (shops, rows) where rows is list of dicts with keys:
      id, category, name, company_stock_qty, total_stock_qty, per_shop (dict shop_id->qty)
    """
    m = (inventory_mode or "").strip().lower()
    shops = []
    try:
        shops = list_shops(limit=500) or []
    except Exception:
        shops = []
    active_clause = " AND i.status = 'active' " if only_active else ""
    # Shelf / company matrix always uses catalog ``items`` + ``shop_items`` (correct ids for
    # company ↔ shop movements). "Both" mode counts only rows flagged ``store_stock_registered``.
    shop_join_extra = ""
    items_where_extra = ""
    if m == "both" and column_exists("shop_items", "store_stock_registered"):
        shop_join_extra = " AND COALESCE(si.store_stock_registered,0) = 1 "
        items_where_extra = """
    AND EXISTS (
      SELECT 1 FROM shop_items sreg
      WHERE sreg.item_id = i.id AND COALESCE(sreg.store_stock_registered,0) = 1
      LIMIT 1
    )"""
    shop_ids = []
    for s in shops:
        try:
            shop_ids.append(int(s.get("id")))
        except Exception:
            continue

    # Build a pivot query: one column per shop_id.
    pivot_bits = []
    for sid in shop_ids:
        pivot_bits.append(
            f"COALESCE(SUM(CASE WHEN si.shop_id={sid} THEN si.shop_stock_qty ELSE 0 END),0) AS shop_{sid}"
        )
    pivot_sql = ",\n        ".join(pivot_bits) if pivot_bits else "0 AS shop_0"

    mode_filter = ""
    sql = f"""
    SELECT
        i.id,
        i.category,
        i.name,
        COALESCE(i.stock_qty, 0) AS company_stock_qty,
        {pivot_sql}
    FROM items i
    LEFT JOIN shop_items si ON si.item_id = i.id{shop_join_extra}
    WHERE 1=1{active_clause}{items_where_extra}
    {mode_filter}
    GROUP BY i.id, i.category, i.name, i.stock_qty
    ORDER BY i.name ASC
    LIMIT %s
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (int(limit_items),))
            raw = cur.fetchall() or []
    except pymysql.Error:
        return shops, []

    rows = []
    for r in raw:
        rr = dict(r)
        per_shop = {}
        total = 0
        try:
            total += int(rr.get("company_stock_qty") or 0)
        except Exception:
            pass
        for sid in shop_ids:
            k = f"shop_{sid}"
            try:
                q = int(rr.get(k) or 0)
            except Exception:
                q = 0
            per_shop[str(sid)] = q
            total += q
            rr.pop(k, None)
        rr["per_shop"] = per_shop
        rr["total_stock_qty"] = total
        rows.append(rr)
    return shops, rows


def get_shop_stock_period_item_balances(shop_id: int, analytics_filter: dict) -> dict:
    """
    Per-item stock reconciliation for one shop over an analytics period.

    Returns {item_id: {opening_stock, stock_in, stock_out, stock_sold, closing_stock}}.
    """
    try:
        sid = int(shop_id)
    except Exception:
        return {}
    if sid <= 0:
        return {}

    af = analytics_filter or {}
    range_start = (af.get("range_start") or "").strip()
    range_end = (af.get("range_end_exclusive") or "").strip()

    sst_where, sst_params = _analytics_where_clause(af, "sst")
    sst_where = f"({sst_where}) AND sst.shop_id=%s"
    sst_params = list(sst_params) + [sid]

    sale_range_where, sale_range_params = _analytics_where_clause(af, "s")
    scope_where, scope_params = _analytics_receipt_scope_clause("general", "s")
    sale_where = f"s.shop_id=%s AND {sale_range_where} AND {scope_where}"
    sale_params = [sid] + list(sale_range_params) + list(scope_params)

    movement_sql = f"""
    SELECT
        sst.item_id,
        COALESCE(SUM(CASE WHEN sst.direction = 'in' THEN sst.qty ELSE 0 END), 0) AS stock_in,
        COALESCE(SUM(CASE WHEN sst.direction = 'out' THEN sst.qty ELSE 0 END), 0) AS stock_out
    FROM shop_stock_transactions sst
    WHERE {sst_where}
    GROUP BY sst.item_id
    """

    join_name_catalog = """
    INNER JOIN (
        SELECT MIN(i.id) AS id, LOWER(TRIM(i.name)) AS nm
        FROM items i
        WHERE i.status = 'active' AND COALESCE(i.stock_update_enabled, 0) = 1
        GROUP BY LOWER(TRIM(i.name))
    ) im ON im.nm = LOWER(TRIM(COALESCE(si.item_name, '')))
        AND LENGTH(TRIM(COALESCE(si.item_name, ''))) > 0
    """
    sales_by_id_sql = f"""
    SELECT
        si.item_id AS item_id,
        COALESCE(SUM(si.qty), 0) AS stock_sold
    FROM shop_pos_sale_items si
    JOIN shop_pos_sales s ON s.id = si.sale_id
    WHERE {sale_where} AND si.item_id IS NOT NULL AND si.item_id > 0
    GROUP BY si.item_id
    """
    sales_by_name_sql = f"""
    SELECT
        im.id AS item_id,
        COALESCE(SUM(si.qty), 0) AS stock_sold
    FROM shop_pos_sale_items si
    JOIN shop_pos_sales s ON s.id = si.sale_id
    {join_name_catalog}
    WHERE {sale_where} AND (si.item_id IS NULL OR si.item_id = 0)
    GROUP BY im.id
    """

    stock_at_sql = """
    SELECT sst.item_id, CAST(sst.shop_stock_after AS SIGNED) AS qty
    FROM shop_stock_transactions sst
    INNER JOIN (
        SELECT item_id, MAX(id) AS mid
        FROM shop_stock_transactions
        WHERE shop_id = %s AND created_at < %s
        GROUP BY item_id
    ) z ON z.mid = sst.id
    """
    current_qty_sql = """
    SELECT si.item_id, COALESCE(si.shop_stock_qty, 0) AS qty
    FROM shop_items si
    JOIN items i ON i.id = si.item_id
    WHERE si.shop_id = %s AND i.status = 'active'
    """

    movement_map: dict = {}
    sales_map: dict = {}
    opening_map: dict = {}
    closing_map: dict = {}
    current_qty: dict = {}

    try:
        with get_cursor() as cur:
            cur.execute(movement_sql, tuple(sst_params))
            for r in cur.fetchall() or []:
                try:
                    iid = int(r.get("item_id") or 0)
                except Exception:
                    continue
                if iid > 0:
                    movement_map[iid] = {
                        "stock_in": int(r.get("stock_in") or 0),
                        "stock_out": int(r.get("stock_out") or 0),
                    }

            cur.execute(sales_by_id_sql, tuple(sale_params))
            for r in cur.fetchall() or []:
                try:
                    iid = int(r.get("item_id") or 0)
                except Exception:
                    continue
                if iid > 0:
                    sales_map[iid] = int(r.get("stock_sold") or 0)

            cur.execute(sales_by_name_sql, tuple(sale_params))
            for r in cur.fetchall() or []:
                try:
                    iid = int(r.get("item_id") or 0)
                except Exception:
                    continue
                if iid > 0:
                    sales_map[iid] = int(sales_map.get(iid, 0)) + int(r.get("stock_sold") or 0)

            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", range_start):
                cur.execute(stock_at_sql, (sid, range_start))
                for r in cur.fetchall() or []:
                    try:
                        iid = int(r.get("item_id") or 0)
                    except Exception:
                        continue
                    if iid > 0:
                        opening_map[iid] = int(r.get("qty") or 0)

            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", range_end):
                cur.execute(stock_at_sql, (sid, range_end))
                for r in cur.fetchall() or []:
                    try:
                        iid = int(r.get("item_id") or 0)
                    except Exception:
                        continue
                    if iid > 0:
                        closing_map[iid] = int(r.get("qty") or 0)

            cur.execute(current_qty_sql, (sid,))
            for r in cur.fetchall() or []:
                try:
                    iid = int(r.get("item_id") or 0)
                except Exception:
                    continue
                if iid > 0:
                    current_qty[iid] = int(r.get("qty") or 0)
    except pymysql.Error:
        return {}

    out: dict = {}
    all_ids = set(current_qty) | set(movement_map) | set(sales_map) | set(opening_map) | set(closing_map)
    for iid in all_ids:
        mv = movement_map.get(iid) or {"stock_in": 0, "stock_out": 0}
        stock_in = int(mv["stock_in"])
        stock_out = int(mv["stock_out"])
        stock_sold = int(sales_map.get(iid, 0))
        shop_qty = int(current_qty.get(iid, 0))
        closing_stock = int(closing_map.get(iid, shop_qty))
        if iid in opening_map:
            opening_stock = int(opening_map[iid])
        else:
            opening_stock = closing_stock - stock_in + stock_out
        out[iid] = {
            "opening_stock": opening_stock,
            "stock_in": stock_in,
            "stock_out": stock_out,
            "stock_sold": stock_sold,
            "closing_stock": closing_stock,
        }
    return out


def get_it_stock_period_item_balances(
    analytics_filter: dict, shop_id: Optional[int] = None
) -> dict:
    """
    Per-item period balances for IT stock reports.

    - shop_id set: that shop only (same as get_shop_stock_period_item_balances).
    - shop_id None: company warehouse + all shops combined per item.
    """
    if shop_id is not None:
        try:
            sid = int(shop_id)
        except Exception:
            return {}
        if sid > 0:
            return get_shop_stock_period_item_balances(sid, analytics_filter)
        return {}

    af = analytics_filter or {}
    range_start = (af.get("range_start") or "").strip()
    range_end = (af.get("range_end_exclusive") or "").strip()

    st_where, st_params = _analytics_where_clause(af, "st")
    sst_where, sst_params = _analytics_where_clause(af, "sst")

    movement_sql = f"""
    SELECT
        mv.item_id,
        COALESCE(SUM(CASE WHEN mv.direction = 'in' THEN mv.qty ELSE 0 END), 0) AS stock_in,
        COALESCE(SUM(CASE WHEN mv.direction = 'out' THEN mv.qty ELSE 0 END), 0) AS stock_out
    FROM (
        SELECT st.item_id, st.direction, st.qty
        FROM stock_transactions st
        WHERE {st_where}
        UNION ALL
        SELECT sst.item_id, sst.direction, sst.qty
        FROM shop_stock_transactions sst
        WHERE {sst_where}
    ) mv
    GROUP BY mv.item_id
    """

    sale_range_where, sale_range_params = _analytics_where_clause(af, "s")
    scope_where, scope_params = _analytics_receipt_scope_clause("general", "s")
    sale_where = f"{sale_range_where} AND {scope_where}"
    sale_params = list(sale_range_params) + list(scope_params)

    join_name_catalog = """
    INNER JOIN (
        SELECT MIN(i.id) AS id, LOWER(TRIM(i.name)) AS nm
        FROM items i
        WHERE i.status = 'active' AND COALESCE(i.stock_update_enabled, 0) = 1
        GROUP BY LOWER(TRIM(i.name))
    ) im ON im.nm = LOWER(TRIM(COALESCE(si.item_name, '')))
        AND LENGTH(TRIM(COALESCE(si.item_name, ''))) > 0
    """
    sales_by_id_sql = f"""
    SELECT
        si.item_id AS item_id,
        COALESCE(SUM(si.qty), 0) AS stock_sold
    FROM shop_pos_sale_items si
    JOIN shop_pos_sales s ON s.id = si.sale_id
    WHERE {sale_where} AND si.item_id IS NOT NULL AND si.item_id > 0
    GROUP BY si.item_id
    """
    sales_by_name_sql = f"""
    SELECT
        im.id AS item_id,
        COALESCE(SUM(si.qty), 0) AS stock_sold
    FROM shop_pos_sale_items si
    JOIN shop_pos_sales s ON s.id = si.sale_id
    {join_name_catalog}
    WHERE {sale_where} AND (si.item_id IS NULL OR si.item_id = 0)
    GROUP BY im.id
    """

    stock_at_company_sql = """
    SELECT st.item_id, CAST(st.stock_after AS SIGNED) AS qty
    FROM stock_transactions st
    INNER JOIN (
        SELECT item_id, MAX(id) AS mid
        FROM stock_transactions
        WHERE created_at < %s
        GROUP BY item_id
    ) z ON z.mid = st.id
    """
    stock_at_shops_sql = """
    SELECT sst.item_id, COALESCE(SUM(CAST(sst.shop_stock_after AS SIGNED)), 0) AS qty
    FROM shop_stock_transactions sst
    INNER JOIN (
        SELECT shop_id, item_id, MAX(id) AS mid
        FROM shop_stock_transactions
        WHERE created_at < %s
        GROUP BY shop_id, item_id
    ) z ON z.mid = sst.id
    GROUP BY sst.item_id
    """
    current_qty_sql = """
    SELECT
        i.id AS item_id,
        COALESCE(i.stock_qty, 0) + COALESCE(SUM(si.shop_stock_qty), 0) AS qty
    FROM items i
    LEFT JOIN shop_items si ON si.item_id = i.id
    WHERE i.status = 'active'
    GROUP BY i.id, i.stock_qty
    """

    movement_map: dict = {}
    sales_map: dict = {}
    opening_map: dict = {}
    closing_map: dict = {}
    current_qty: dict = {}

    try:
        with get_cursor() as cur:
            cur.execute(movement_sql, tuple(list(st_params) + list(sst_params)))
            for r in cur.fetchall() or []:
                try:
                    iid = int(r.get("item_id") or 0)
                except Exception:
                    continue
                if iid > 0:
                    movement_map[iid] = {
                        "stock_in": int(r.get("stock_in") or 0),
                        "stock_out": int(r.get("stock_out") or 0),
                    }

            cur.execute(sales_by_id_sql, tuple(sale_params))
            for r in cur.fetchall() or []:
                try:
                    iid = int(r.get("item_id") or 0)
                except Exception:
                    continue
                if iid > 0:
                    sales_map[iid] = int(r.get("stock_sold") or 0)

            cur.execute(sales_by_name_sql, tuple(sale_params))
            for r in cur.fetchall() or []:
                try:
                    iid = int(r.get("item_id") or 0)
                except Exception:
                    continue
                if iid > 0:
                    sales_map[iid] = int(sales_map.get(iid, 0)) + int(r.get("stock_sold") or 0)

            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", range_start):
                cur.execute(stock_at_company_sql, (range_start,))
                for r in cur.fetchall() or []:
                    try:
                        iid = int(r.get("item_id") or 0)
                    except Exception:
                        continue
                    if iid > 0:
                        opening_map[iid] = int(opening_map.get(iid, 0)) + int(r.get("qty") or 0)
                cur.execute(stock_at_shops_sql, (range_start,))
                for r in cur.fetchall() or []:
                    try:
                        iid = int(r.get("item_id") or 0)
                    except Exception:
                        continue
                    if iid > 0:
                        opening_map[iid] = int(opening_map.get(iid, 0)) + int(r.get("qty") or 0)

            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", range_end):
                cur.execute(stock_at_company_sql, (range_end,))
                for r in cur.fetchall() or []:
                    try:
                        iid = int(r.get("item_id") or 0)
                    except Exception:
                        continue
                    if iid > 0:
                        closing_map[iid] = int(closing_map.get(iid, 0)) + int(r.get("qty") or 0)
                cur.execute(stock_at_shops_sql, (range_end,))
                for r in cur.fetchall() or []:
                    try:
                        iid = int(r.get("item_id") or 0)
                    except Exception:
                        continue
                    if iid > 0:
                        closing_map[iid] = int(closing_map.get(iid, 0)) + int(r.get("qty") or 0)

            cur.execute(current_qty_sql)
            for r in cur.fetchall() or []:
                try:
                    iid = int(r.get("item_id") or 0)
                except Exception:
                    continue
                if iid > 0:
                    current_qty[iid] = int(r.get("qty") or 0)
    except pymysql.Error:
        return {}

    out: dict = {}
    all_ids = set(current_qty) | set(movement_map) | set(sales_map) | set(opening_map) | set(closing_map)
    for iid in all_ids:
        mv = movement_map.get(iid) or {"stock_in": 0, "stock_out": 0}
        stock_in = int(mv["stock_in"])
        stock_out = int(mv["stock_out"])
        stock_sold = int(sales_map.get(iid, 0))
        total_qty = int(current_qty.get(iid, 0))
        closing_stock = int(closing_map.get(iid, total_qty))
        if iid in opening_map:
            opening_stock = int(opening_map[iid])
        else:
            opening_stock = closing_stock - stock_in + stock_out
        out[iid] = {
            "opening_stock": opening_stock,
            "stock_in": stock_in,
            "stock_out": stock_out,
            "stock_sold": stock_sold,
            "closing_stock": closing_stock,
        }
    return out


def get_shop_daily_stock_count(shop_id: int, report_date) -> list:
    """
    Per-item daily stock reconciliation for one shop (calendar day).

    - opening: stock at start of day (derived from end-of-day stock minus net change that day)
    - added: supplier manual stock-in (source=manual, direction=in)
    - moved: internal movements (company + transfer), net in minus out
    - sold: shop outs via manual source (POS sales, waste, returns, etc.)
    - remaining: stock at end of day (last transaction shop_stock_after before next day,
      or current shop stock when no history exists)
    """
    try:
        sid = int(shop_id)
    except Exception:
        return []
    if isinstance(report_date, str):
        raw = (report_date or "").strip()[:10]
        try:
            d = datetime.strptime(raw, "%Y-%m-%d").date()
        except Exception:
            return []
    elif isinstance(report_date, date):
        d = report_date
    else:
        return []

    day_start = datetime.combine(d, time.min)
    day_end = day_start + timedelta(days=1)

    sql = """
    SELECT
      si.item_id,
      i.name AS item_name,
      i.category,
      COALESCE(c.closing_qty, COALESCE(si.shop_stock_qty, 0)) AS closing_qty,
      COALESCE(a.added_stock, 0) AS added_stock,
      COALESCE(a.moved_stock, 0) AS moved_stock,
      COALESCE(a.sold_stock, 0) AS sold_stock,
      COALESCE(a.net_day, 0) AS net_day
    FROM shop_items si
    JOIN items i ON i.id = si.item_id
    LEFT JOIN (
      SELECT sst.item_id, sst.shop_stock_after AS closing_qty
      FROM shop_stock_transactions sst
      INNER JOIN (
        SELECT item_id, MAX(id) AS mid
        FROM shop_stock_transactions
        WHERE shop_id = %s AND created_at < %s
        GROUP BY item_id
      ) z ON z.mid = sst.id
    ) c ON c.item_id = si.item_id
    LEFT JOIN (
      SELECT
        item_id,
        SUM(CASE WHEN direction = 'in' AND source = 'manual' THEN qty ELSE 0 END) AS added_stock,
        SUM(
          CASE
            WHEN source IN ('company', 'transfer') THEN IF(direction = 'in', qty, -qty)
            ELSE 0
          END
        ) AS moved_stock,
        SUM(CASE WHEN direction = 'out' AND source = 'manual' THEN qty ELSE 0 END) AS sold_stock,
        SUM(IF(direction = 'in', qty, -qty)) AS net_day
      FROM shop_stock_transactions
      WHERE shop_id = %s AND created_at >= %s AND created_at < %s
      GROUP BY item_id
    ) a ON a.item_id = si.item_id
    WHERE si.shop_id = %s AND si.stock_update_enabled = 1 AND i.status = 'active'
    ORDER BY COALESCE(NULLIF(TRIM(i.category), ''), 'Uncategorized') ASC, i.name ASC
    """
    params = (sid, day_end, sid, day_start, day_end, sid)
    try:
        with get_cursor() as cur:
            cur.execute(sql, params)
            raw = cur.fetchall() or []
    except pymysql.Error:
        return []

    out = []
    for r in raw:
        closing = int(r.get("closing_qty") or 0)
        net_day = int(r.get("net_day") or 0)
        added = int(r.get("added_stock") or 0)
        moved = int(r.get("moved_stock") or 0)
        sold = int(r.get("sold_stock") or 0)
        opening = closing - net_day
        rr = {
            "item_id": int(r.get("item_id") or 0),
            "name": (r.get("item_name") or "").strip() or "Item",
            "category": (r.get("category") or "").strip(),
            "opening_stock": opening,
            "added_stock": added,
            "moved_stock": moved,
            "sold_stock": sold,
            "remaining_stock": closing,
        }
        out.append(rr)
    return out


def list_shop_stock_count_sheet_items(shop_id: int) -> list:
    """Item rows for a printable blank day stock count sheet (tracked items at one shop)."""
    try:
        sid = int(shop_id)
    except Exception:
        return []
    sql = """
    SELECT si.item_id, i.name AS item_name
    FROM shop_items si
    JOIN items i ON i.id = si.item_id
    WHERE si.shop_id = %s AND si.stock_update_enabled = 1 AND i.status = 'active'
    ORDER BY COALESCE(NULLIF(TRIM(i.category), ''), 'Uncategorized') ASC, i.name ASC
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (sid,))
            raw = cur.fetchall() or []
    except pymysql.Error:
        return []
    out = []
    for r in raw:
        out.append(
            {
                "item_id": int(r.get("item_id") or 0),
                "name": (r.get("item_name") or "").strip() or "Item",
            }
        )
    return out


def get_company_stock_movement_analytics(analytics_filter: dict, shop_id: Optional[int] = None):
    """
    Stock movement analytics across company + shops, with optional shop-only filter.
    - Company movements come from stock_transactions.
    - Shop movements come from shop_stock_transactions.
    """
    st_where, st_params = _analytics_where_clause(analytics_filter, "st")
    sst_where, sst_params = _analytics_where_clause(analytics_filter, "sst")
    union_sql = f"""
    (
      SELECT NULL AS shop_id, 'company' AS scope, st.item_id, st.direction, st.qty, st.created_at
      FROM stock_transactions st
      WHERE {st_where}
      UNION ALL
      SELECT sst.shop_id AS shop_id, 'shop' AS scope, sst.item_id, sst.direction, sst.qty, sst.created_at
      FROM shop_stock_transactions sst
      WHERE {sst_where}
    )
    """
    base_params = list(st_params) + list(sst_params)
    mv_where = "1=1"
    mv_params: list = []
    if shop_id is not None:
        mv_where = "mv.scope='shop' AND mv.shop_id=%s"
        mv_params.append(int(shop_id))

    totals_sql = f"""
    SELECT
      COUNT(*) AS tx_count,
      COALESCE(SUM(CASE WHEN mv.direction='in' THEN mv.qty ELSE 0 END), 0) AS qty_in,
      COALESCE(SUM(CASE WHEN mv.direction='out' THEN mv.qty ELSE 0 END), 0) AS qty_out,
      COUNT(DISTINCT mv.item_id) AS distinct_items,
      COUNT(DISTINCT CASE WHEN mv.scope='shop' THEN mv.shop_id END) AS distinct_shops
    FROM {union_sql} mv
    WHERE {mv_where}
    """
    top_items_sql = f"""
    SELECT
      mv.item_id,
      i.category,
      i.name,
      COALESCE(SUM(CASE WHEN mv.direction='in' THEN mv.qty ELSE 0 END), 0) AS qty_in,
      COALESCE(SUM(CASE WHEN mv.direction='out' THEN mv.qty ELSE 0 END), 0) AS qty_out
    FROM {union_sql} mv
    JOIN items i ON i.id = mv.item_id
    WHERE {mv_where}
    GROUP BY mv.item_id, i.category, i.name
    ORDER BY (qty_in + qty_out) DESC, i.name ASC
    LIMIT 20
    """
    daily_sql = f"""
    SELECT
      DATE(mv.created_at) AS day,
      COUNT(*) AS tx_count,
      COALESCE(SUM(CASE WHEN mv.direction='in' THEN mv.qty ELSE 0 END), 0) AS qty_in,
      COALESCE(SUM(CASE WHEN mv.direction='out' THEN mv.qty ELSE 0 END), 0) AS qty_out
    FROM {union_sql} mv
    WHERE {mv_where}
    GROUP BY DATE(mv.created_at)
    ORDER BY day DESC
    LIMIT 31
    """
    by_shop_sql = f"""
    SELECT
      mv.shop_id,
      s.shop_name,
      s.shop_code,
      COUNT(*) AS tx_count,
      COALESCE(SUM(CASE WHEN mv.direction='in' THEN mv.qty ELSE 0 END), 0) AS qty_in,
      COALESCE(SUM(CASE WHEN mv.direction='out' THEN mv.qty ELSE 0 END), 0) AS qty_out
    FROM {union_sql} mv
    JOIN shops s ON s.id = mv.shop_id
    WHERE mv.scope='shop' AND {mv_where}
    GROUP BY mv.shop_id, s.shop_name, s.shop_code
    ORDER BY (qty_in + qty_out) DESC, s.shop_name ASC
    LIMIT 50
    """
    by_scope_sql = f"""
    SELECT
      mv.scope,
      mv.direction,
      COUNT(*) AS tx_count,
      COALESCE(SUM(mv.qty), 0) AS qty
    FROM {union_sql} mv
    WHERE {mv_where}
    GROUP BY mv.scope, mv.direction
    ORDER BY mv.scope ASC, mv.direction ASC
    """

    out = {
        "tx_count": 0,
        "qty_in": 0,
        "qty_out": 0,
        "net_qty": 0,
        "distinct_items": 0,
        "distinct_shops": 0,
        "top_items": [],
        "daily": [],
        "by_shop": [],
        "scope_rows": [],
    }
    params = tuple(base_params + mv_params)
    try:
        with get_cursor() as cur:
            cur.execute(totals_sql, params)
            t = cur.fetchone() or {}
            out["tx_count"] = int(t.get("tx_count") or 0)
            out["qty_in"] = int(t.get("qty_in") or 0)
            out["qty_out"] = int(t.get("qty_out") or 0)
            out["net_qty"] = out["qty_in"] - out["qty_out"]
            out["distinct_items"] = int(t.get("distinct_items") or 0)
            out["distinct_shops"] = int(t.get("distinct_shops") or 0)

            cur.execute(top_items_sql, params)
            out["top_items"] = [
                {
                    "item_id": r.get("item_id"),
                    "category": r.get("category") or "",
                    "name": r.get("name") or "Item",
                    "qty_in": int(r.get("qty_in") or 0),
                    "qty_out": int(r.get("qty_out") or 0),
                    "total": int(r.get("qty_in") or 0) + int(r.get("qty_out") or 0),
                }
                for r in (cur.fetchall() or [])
            ]

            cur.execute(daily_sql, params)
            out["daily"] = [
                {
                    "day": str(r.get("day") or ""),
                    "tx_count": int(r.get("tx_count") or 0),
                    "qty_in": int(r.get("qty_in") or 0),
                    "qty_out": int(r.get("qty_out") or 0),
                    "net_qty": int(r.get("qty_in") or 0) - int(r.get("qty_out") or 0),
                }
                for r in (cur.fetchall() or [])
            ]

            cur.execute(by_shop_sql, params)
            out["by_shop"] = [
                {
                    "shop_id": r.get("shop_id"),
                    "shop_name": r.get("shop_name") or "",
                    "shop_code": r.get("shop_code") or "",
                    "tx_count": int(r.get("tx_count") or 0),
                    "qty_in": int(r.get("qty_in") or 0),
                    "qty_out": int(r.get("qty_out") or 0),
                    "net_qty": int(r.get("qty_in") or 0) - int(r.get("qty_out") or 0),
                }
                for r in (cur.fetchall() or [])
            ]

            cur.execute(by_scope_sql, params)
            out["scope_rows"] = [
                {
                    "scope": (r.get("scope") or "").upper(),
                    "direction": (r.get("direction") or "").upper(),
                    "tx_count": int(r.get("tx_count") or 0),
                    "qty": int(r.get("qty") or 0),
                }
                for r in (cur.fetchall() or [])
            ]
    except pymysql.Error:
        return out
    return out


def _report_catalog_by_item_id(rows: list) -> dict:
    out: dict = {}
    for r in rows or []:
        try:
            iid = int(r.get("item_id") or 0)
        except Exception:
            continue
        if iid > 0:
            out[iid] = r
    return out


def _report_fetch_item_meta_by_ids(item_ids: list[int]) -> dict:
    """Name/category for report rows not in the active catalog snapshot."""
    ids = sorted({int(x) for x in (item_ids or []) if int(x) > 0})
    if not ids:
        return {}
    placeholders = ",".join(["%s"] * len(ids))
    sql = f"""
    SELECT id AS item_id, category, name
    FROM items
    WHERE id IN ({placeholders})
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(ids))
            return _report_catalog_by_item_id(cur.fetchall() or [])
    except pymysql.Error:
        return {}


def _report_item_had_period_activity(
    stock_in: int, stock_out: int, stock_sold: int, revenue: float
) -> bool:
    return bool(stock_in or stock_out or stock_sold or abs(float(revenue or 0)) > 1e-9)


def get_company_report(
    analytics_filter: dict,
    analytics_scope: str = "general",
    shop_id: Optional[int] = None,
):
    """Company-wide period report: revenue, expenditure, and per-item stock + sales."""
    af = analytics_filter or {}
    shop_filter: Optional[int] = None
    if shop_id is not None:
        try:
            sid = int(shop_id)
        except Exception:
            sid = 0
        if sid > 0:
            shop_filter = sid

    range_end = (af.get("range_end_exclusive") or "").strip()
    st_where, st_params = _analytics_where_clause(af, "st")
    sst_where, sst_params = _analytics_where_clause(af, "sst")
    scope_where, scope_params = _analytics_receipt_scope_clause(analytics_scope, "s")
    sale_range_where, sale_range_params = _analytics_where_clause(af, "s")
    if shop_filter is not None:
        sale_where = f"s.shop_id=%s AND {sale_range_where} AND {scope_where}"
        sale_params = [shop_filter] + list(sale_range_params) + list(scope_params)
        sst_where = f"({sst_where}) AND sst.shop_id=%s"
        sst_params = list(sst_params) + [shop_filter]
        st_where = f"({st_where}) AND 1=0"
    else:
        sale_where = f"{sale_range_where} AND {scope_where}"
        sale_params = list(sale_range_params) + list(scope_params)

    movement_sql = f"""
    SELECT
        mv.item_id,
        COALESCE(SUM(CASE WHEN mv.direction = 'in' THEN mv.qty ELSE 0 END), 0) AS stock_in,
        COALESCE(SUM(CASE WHEN mv.direction = 'out' THEN mv.qty ELSE 0 END), 0) AS stock_out
    FROM (
        SELECT st.item_id, st.direction, st.qty
        FROM stock_transactions st
        WHERE {st_where}
        UNION ALL
        SELECT sst.item_id, sst.direction, sst.qty
        FROM shop_stock_transactions sst
        WHERE {sst_where}
    ) mv
    GROUP BY mv.item_id
    """

    expenditure_sql = f"""
    SELECT COALESCE(SUM(x.qty * COALESCE(x.buying_price, 0)), 0) AS total_expenditure
    FROM (
        SELECT st.qty, st.buying_price
        FROM stock_transactions st
        WHERE {st_where} AND st.direction = 'in'
        UNION ALL
        SELECT sst.qty, sst.buying_price
        FROM shop_stock_transactions sst
        WHERE {sst_where} AND sst.direction = 'in' AND sst.source = 'manual'
    ) x
    """

    sales_by_id_sql = f"""
    SELECT
        si.item_id AS item_id,
        COALESCE(SUM(si.qty), 0) AS stock_sold,
        COALESCE(SUM(si.line_total), 0) AS revenue
    FROM shop_pos_sale_items si
    JOIN shop_pos_sales s ON s.id = si.sale_id
    WHERE {sale_where} AND si.item_id IS NOT NULL AND si.item_id > 0
    GROUP BY si.item_id
    """

    join_name_catalog = """
    INNER JOIN (
        SELECT MIN(i.id) AS id, LOWER(TRIM(i.name)) AS nm
        FROM items i
        WHERE i.status = 'active' AND COALESCE(i.stock_update_enabled, 0) = 1
        GROUP BY LOWER(TRIM(i.name))
    ) im ON im.nm = LOWER(TRIM(COALESCE(si.item_name, '')))
        AND LENGTH(TRIM(COALESCE(si.item_name, ''))) > 0
    """
    sales_by_name_sql = f"""
    SELECT
        im.id AS item_id,
        COALESCE(SUM(si.qty), 0) AS stock_sold,
        COALESCE(SUM(si.line_total), 0) AS revenue
    FROM shop_pos_sale_items si
    JOIN shop_pos_sales s ON s.id = si.sale_id
    {join_name_catalog}
    WHERE {sale_where} AND (si.item_id IS NULL OR si.item_id = 0)
    GROUP BY im.id
    """

    revenue_totals_sql = f"""
    SELECT
        COALESCE(SUM(CASE WHEN s.sale_type IN ('sale', 'credit') THEN s.total_amount ELSE 0 END), 0) AS total_revenue,
        COALESCE(SUM(CASE WHEN s.sale_type = 'sale' THEN s.total_amount ELSE 0 END), 0) AS sale_revenue,
        COALESCE(SUM(CASE WHEN s.sale_type = 'credit' THEN s.total_amount ELSE 0 END), 0) AS credit_revenue,
        COALESCE(SUM(CASE WHEN s.sale_type = 'credit' THEN COALESCE(s.credit_paid_amount, 0) ELSE 0 END), 0) AS paid_credit,
        COALESCE(SUM(s.cash_amount), 0) AS cash_revenue,
        COALESCE(SUM(s.mpesa_amount), 0) AS mpesa_revenue
    FROM shop_pos_sales s
    WHERE {sale_where}
    """

    ending_company_sql = """
    SELECT st.item_id, CAST(st.stock_after AS SIGNED) AS qty
    FROM stock_transactions st
    INNER JOIN (
        SELECT item_id, MAX(id) AS mid
        FROM stock_transactions
        WHERE created_at < %s
        GROUP BY item_id
    ) z ON z.mid = st.id
    """
    ending_shop_sql = """
    SELECT sst.item_id, COALESCE(SUM(CAST(sst.shop_stock_after AS SIGNED)), 0) AS qty
    FROM shop_stock_transactions sst
    INNER JOIN (
        SELECT shop_id, item_id, MAX(id) AS mid
        FROM shop_stock_transactions
        WHERE created_at < %s
        GROUP BY shop_id, item_id
    ) z ON z.mid = sst.id
    GROUP BY sst.item_id
    """
    ending_shop_params: list = [range_end]
    if shop_filter is not None:
        ending_shop_sql = """
        SELECT sst.item_id, CAST(sst.shop_stock_after AS SIGNED) AS qty
        FROM shop_stock_transactions sst
        INNER JOIN (
            SELECT item_id, MAX(id) AS mid
            FROM shop_stock_transactions
            WHERE shop_id = %s AND created_at < %s
            GROUP BY item_id
        ) z ON z.mid = sst.id
        """
        ending_shop_params = [shop_filter, range_end]

    if shop_filter is not None:
        current_stock_sql = """
        SELECT
            si.item_id,
            i.category,
            i.name,
            0 AS company_stock_qty,
            COALESCE(si.shop_stock_qty, 0) AS shop_stock_qty
        FROM shop_items si
        JOIN items i ON i.id = si.item_id
        WHERE si.shop_id = %s AND i.status = 'active'
        ORDER BY COALESCE(NULLIF(TRIM(i.category), ''), 'Uncategorized') ASC, i.name ASC
        """
    else:
        current_stock_sql = """
        SELECT
            i.id AS item_id,
            i.category,
            i.name,
            COALESCE(i.stock_qty, 0) AS company_stock_qty,
            COALESCE(SUM(si.shop_stock_qty), 0) AS shop_stock_qty
        FROM items i
        LEFT JOIN shop_items si ON si.item_id = i.id
        WHERE i.status = 'active'
        GROUP BY i.id, i.category, i.name, i.stock_qty
        ORDER BY COALESCE(NULLIF(TRIM(i.category), ''), 'Uncategorized') ASC, i.name ASC
        """

    out = {
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
        "shop_filter_id": shop_filter,
        "items": [],
    }
    movement_map: dict = {}
    sales_map: dict = {}
    ending_map: dict = {}
    catalog_rows: list = []

    try:
        with get_cursor() as cur:
            cur.execute(revenue_totals_sql, tuple(sale_params))
            rt = cur.fetchone() or {}
            out["total_revenue"] = float(rt.get("total_revenue") or 0)
            out["sale_revenue"] = float(rt.get("sale_revenue") or 0)
            out["credit_revenue"] = float(rt.get("credit_revenue") or 0)
            out["paid_credit"] = round(float(rt.get("paid_credit") or 0), 2)
            out["cash_revenue"] = float(rt.get("cash_revenue") or 0)
            out["mpesa_revenue"] = float(rt.get("mpesa_revenue") or 0)
            out["unpaid_credit"] = round(
                max(0.0, float(out.get("credit_revenue") or 0) - float(out.get("paid_credit") or 0)),
                2,
            )
            out["collected_revenue"] = round(
                float(out.get("sale_revenue") or 0) + float(out.get("paid_credit") or 0),
                2,
            )

            cur.execute(movement_sql, tuple(st_params + sst_params))
            for r in cur.fetchall() or []:
                try:
                    iid = int(r.get("item_id") or 0)
                except Exception:
                    continue
                if iid > 0:
                    movement_map[iid] = {
                        "stock_in": int(r.get("stock_in") or 0),
                        "stock_out": int(r.get("stock_out") or 0),
                    }

            cur.execute(sales_by_id_sql, tuple(sale_params))
            for r in cur.fetchall() or []:
                try:
                    iid = int(r.get("item_id") or 0)
                except Exception:
                    continue
                if iid <= 0:
                    continue
                sales_map[iid] = {
                    "stock_sold": int(r.get("stock_sold") or 0),
                    "revenue": float(r.get("revenue") or 0),
                }

            cur.execute(sales_by_name_sql, tuple(sale_params))
            for r in cur.fetchall() or []:
                try:
                    iid = int(r.get("item_id") or 0)
                except Exception:
                    continue
                if iid <= 0:
                    continue
                prev = sales_map.get(iid) or {"stock_sold": 0, "revenue": 0.0}
                sales_map[iid] = {
                    "stock_sold": int(prev["stock_sold"]) + int(r.get("stock_sold") or 0),
                    "revenue": float(prev["revenue"]) + float(r.get("revenue") or 0),
                }

            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", range_end):
                if shop_filter is None:
                    cur.execute(ending_company_sql, (range_end,))
                    for r in cur.fetchall() or []:
                        try:
                            iid = int(r.get("item_id") or 0)
                        except Exception:
                            continue
                        if iid > 0:
                            ending_map.setdefault(iid, {"company": 0, "shop": 0})
                            ending_map[iid]["company"] = int(r.get("qty") or 0)
                cur.execute(ending_shop_sql, tuple(ending_shop_params))
                for r in cur.fetchall() or []:
                    try:
                        iid = int(r.get("item_id") or 0)
                    except Exception:
                        continue
                    if iid <= 0:
                        continue
                    if shop_filter is not None:
                        ending_map[iid] = int(r.get("qty") or 0)
                    else:
                        ending_map.setdefault(iid, {"company": 0, "shop": 0})
                        ending_map[iid]["shop"] = int(r.get("qty") or 0)

            if shop_filter is not None:
                cur.execute(current_stock_sql, (shop_filter,))
            else:
                cur.execute(current_stock_sql)
            catalog_rows = cur.fetchall() or []
    except pymysql.Error:
        return out

    catalog_by_id = _report_catalog_by_item_id(catalog_rows)
    affected_ids = set(movement_map.keys()) | set(sales_map.keys())
    missing_meta = _report_fetch_item_meta_by_ids(
        [iid for iid in affected_ids if iid not in catalog_by_id]
    )
    items_out = []
    for iid in affected_ids:
        r = catalog_by_id.get(iid) or missing_meta.get(iid) or {}
        mv = movement_map.get(iid) or {"stock_in": 0, "stock_out": 0}
        sl = sales_map.get(iid) or {"stock_sold": 0, "revenue": 0.0}
        stock_in = int(mv["stock_in"])
        stock_out = int(mv["stock_out"])
        stock_sold = int(sl["stock_sold"])
        revenue = float(sl["revenue"])
        if not _report_item_had_period_activity(stock_in, stock_out, stock_sold, revenue):
            continue
        company_qty = int(r.get("company_stock_qty") or 0)
        shop_qty = int(r.get("shop_stock_qty") or 0)
        if shop_filter is not None:
            ending_stock = ending_map.get(iid, shop_qty)
            if not isinstance(ending_stock, int):
                ending_stock = int(ending_stock or shop_qty)
        else:
            current_total = company_qty + shop_qty
            end_parts = ending_map.get(iid)
            if end_parts:
                ending_stock = int(end_parts.get("company") or 0) + int(end_parts.get("shop") or 0)
            else:
                ending_stock = current_total
        starting_stock = ending_stock - stock_in + stock_out
        items_out.append(
            {
                "item_id": iid,
                "category": (r.get("category") or "").strip(),
                "name": (r.get("name") or "").strip() or f"Item #{iid}",
                "starting_stock": starting_stock,
                "ending_stock": ending_stock,
                "stock_in": stock_in,
                "stock_out": stock_out,
                "stock_sold": stock_sold,
                "revenue": round(revenue, 2),
            }
        )

    items_out.sort(key=lambda x: (-float(x.get("revenue") or 0), x.get("name") or ""))
    out["items"] = items_out
    openings = list_shop_day_openings_for_report(af, shop_id=shop_filter)
    out["shop_openings"] = openings
    out["opening_cash_total"] = round(
        sum(float(o.get("opening_cash") or 0) for o in openings), 2
    )
    out["opening_mpesa_total"] = round(
        sum(float(o.get("opening_mpesa") or 0) for o in openings), 2
    )
    out["expenditure_rows"] = list_company_expenditure_for_report(af, shop_id=shop_filter)
    exp_totals = _sum_shop_expenditure_totals(out["expenditure_rows"])
    out["total_expenditure"] = exp_totals["total_expenditure"]
    out["paid_expenditure"] = exp_totals["paid_expenditure"]
    out["balance_expenditure"] = exp_totals["balance_expenditure"]
    _aggregate_company_financials_from_shops(out, af, analytics_scope, shop_id=shop_filter)
    if shop_filter is not None:
        out["pos_allow_credit_sale"] = resolve_shop_pos_allow_credit_sale(shop_filter)
        out["pos_inventory_mode"] = resolve_shop_pos_inventory_mode(shop_filter)
    else:
        out["pos_allow_credit_sale"] = True
        out["pos_inventory_mode"] = "shop"
    return _enrich_period_report_financials(out)


def get_shop_report(shop_id: int, analytics_filter: dict, analytics_scope: str = "general"):
    """Single-shop period report: revenue, expenditure, and per-item stock + sales."""
    try:
        sid = int(shop_id)
    except Exception:
        return {
            "total_revenue": 0.0,
            "sale_revenue": 0.0,
            "credit_revenue": 0.0,
            "cash_revenue": 0.0,
            "mpesa_revenue": 0.0,
            "total_expenditure": 0.0,
            "net_profit": 0.0,
            "items": [],
        }
    if sid <= 0:
        return {
            "total_revenue": 0.0,
            "sale_revenue": 0.0,
            "credit_revenue": 0.0,
            "cash_revenue": 0.0,
            "mpesa_revenue": 0.0,
            "total_expenditure": 0.0,
            "net_profit": 0.0,
            "items": [],
        }

    af = analytics_filter or {}
    range_end = (af.get("range_end_exclusive") or "").strip()
    sst_where, sst_params = _analytics_where_clause(af, "sst")
    sst_where = f"({sst_where}) AND sst.shop_id=%s"
    sst_params = list(sst_params) + [sid]
    scope_where, scope_params = _analytics_receipt_scope_clause(analytics_scope, "s")
    sale_range_where, sale_range_params = _analytics_where_clause(af, "s")
    sale_where = f"s.shop_id=%s AND {sale_range_where} AND {scope_where}"
    sale_params = [sid] + list(sale_range_params) + list(scope_params)

    movement_sql = f"""
    SELECT
        sst.item_id,
        COALESCE(SUM(CASE WHEN sst.direction = 'in' THEN sst.qty ELSE 0 END), 0) AS stock_in,
        COALESCE(SUM(CASE WHEN sst.direction = 'out' THEN sst.qty ELSE 0 END), 0) AS stock_out
    FROM shop_stock_transactions sst
    WHERE {sst_where}
    GROUP BY sst.item_id
    """

    cost_map: dict = {}
    fifo_stock_out_cost = 0.0
    if column_exists("shop_stock_transactions", "cost_total"):
        cost_sql = f"""
        SELECT
            sst.item_id,
            COALESCE(SUM(
                CASE
                    WHEN sst.direction = 'out'
                     AND UPPER(COALESCE(sst.reason, '')) IN ('POS', 'POS_HOLD')
                    THEN COALESCE(sst.cost_total, 0)
                    ELSE 0
                END
            ), 0) AS pos_out_cost,
            COALESCE(SUM(
                CASE
                    WHEN sst.direction = 'in'
                     AND UPPER(COALESCE(sst.reason, '')) = 'POS_HOLD_RET'
                    THEN COALESCE(sst.cost_total, 0)
                    ELSE 0
                END
            ), 0) AS hold_ret_cost,
            COALESCE(SUM(
                CASE
                    WHEN sst.direction = 'out'
                     AND sst.source = 'manual'
                     AND UPPER(COALESCE(sst.reason, '')) IN ('RETURN', 'WASTE', 'DISPLAY')
                    THEN COALESCE(sst.cost_total, 0)
                    ELSE 0
                END
            ), 0) AS manual_out_cost
        FROM shop_stock_transactions sst
        WHERE {sst_where}
        GROUP BY sst.item_id
        """

    expenditure_sql = None

    sales_by_id_sql = f"""
    SELECT
        si.item_id AS item_id,
        COALESCE(SUM(si.qty), 0) AS stock_sold,
        COALESCE(SUM(si.line_total), 0) AS revenue,
        COALESCE(SUM(CASE WHEN s.sale_type = 'sale' THEN si.qty ELSE 0 END), 0) AS sale_qty,
        COALESCE(SUM(CASE WHEN s.sale_type = 'credit' THEN si.qty ELSE 0 END), 0) AS credit_qty,
        COALESCE(SUM(CASE WHEN s.sale_type = 'sale' THEN si.line_total ELSE 0 END), 0) AS sale_amount,
        COALESCE(SUM(CASE WHEN s.sale_type = 'credit' THEN si.line_total ELSE 0 END), 0) AS credit_amount,
        COALESCE(SUM(
            CASE
                WHEN s.sale_type = 'credit' AND COALESCE(s.total_amount, 0) > 0 THEN
                    si.line_total * COALESCE(s.credit_paid_amount, 0) / s.total_amount
                ELSE 0
            END
        ), 0) AS credit_paid_amount
    FROM shop_pos_sale_items si
    JOIN shop_pos_sales s ON s.id = si.sale_id
    WHERE {sale_where} AND si.item_id IS NOT NULL AND si.item_id > 0
    GROUP BY si.item_id
    """

    join_name_catalog = """
    INNER JOIN (
        SELECT MIN(i.id) AS id, LOWER(TRIM(i.name)) AS nm
        FROM items i
        WHERE i.status = 'active' AND COALESCE(i.stock_update_enabled, 0) = 1
        GROUP BY LOWER(TRIM(i.name))
    ) im ON im.nm = LOWER(TRIM(COALESCE(si.item_name, '')))
        AND LENGTH(TRIM(COALESCE(si.item_name, ''))) > 0
    """
    sales_by_name_sql = f"""
    SELECT
        im.id AS item_id,
        COALESCE(SUM(si.qty), 0) AS stock_sold,
        COALESCE(SUM(si.line_total), 0) AS revenue,
        COALESCE(SUM(CASE WHEN s.sale_type = 'sale' THEN si.qty ELSE 0 END), 0) AS sale_qty,
        COALESCE(SUM(CASE WHEN s.sale_type = 'credit' THEN si.qty ELSE 0 END), 0) AS credit_qty,
        COALESCE(SUM(CASE WHEN s.sale_type = 'sale' THEN si.line_total ELSE 0 END), 0) AS sale_amount,
        COALESCE(SUM(CASE WHEN s.sale_type = 'credit' THEN si.line_total ELSE 0 END), 0) AS credit_amount,
        COALESCE(SUM(
            CASE
                WHEN s.sale_type = 'credit' AND COALESCE(s.total_amount, 0) > 0 THEN
                    si.line_total * COALESCE(s.credit_paid_amount, 0) / s.total_amount
                ELSE 0
            END
        ), 0) AS credit_paid_amount
    FROM shop_pos_sale_items si
    JOIN shop_pos_sales s ON s.id = si.sale_id
    {join_name_catalog}
    WHERE {sale_where} AND (si.item_id IS NULL OR si.item_id = 0)
    GROUP BY im.id
    """

    revenue_totals_sql = f"""
    SELECT
        COALESCE(SUM(CASE WHEN s.sale_type IN ('sale', 'credit') THEN s.total_amount ELSE 0 END), 0) AS total_revenue,
        COALESCE(SUM(CASE WHEN s.sale_type = 'sale' THEN s.total_amount ELSE 0 END), 0) AS sale_revenue,
        COALESCE(SUM(CASE WHEN s.sale_type = 'credit' THEN s.total_amount ELSE 0 END), 0) AS credit_revenue,
        COALESCE(SUM(CASE WHEN s.sale_type = 'credit' THEN COALESCE(s.credit_paid_amount, 0) ELSE 0 END), 0) AS paid_credit,
        COALESCE(SUM(s.cash_amount), 0) AS cash_revenue,
        COALESCE(SUM(s.mpesa_amount), 0) AS mpesa_revenue
    FROM shop_pos_sales s
    WHERE {sale_where}
    """

    ending_shop_sql = """
    SELECT sst.item_id, CAST(sst.shop_stock_after AS SIGNED) AS qty
    FROM shop_stock_transactions sst
    INNER JOIN (
        SELECT item_id, MAX(id) AS mid
        FROM shop_stock_transactions
        WHERE shop_id = %s AND created_at < %s
        GROUP BY item_id
    ) z ON z.mid = sst.id
    """
    current_stock_sql = """
    SELECT
        si.item_id,
        i.category,
        i.name,
        COALESCE(si.shop_stock_qty, 0) AS shop_stock_qty
    FROM shop_items si
    JOIN items i ON i.id = si.item_id
    WHERE si.shop_id = %s AND i.status = 'active'
    ORDER BY COALESCE(NULLIF(TRIM(i.category), ''), 'Uncategorized') ASC, i.name ASC
    """

    out = {
        "total_revenue": 0.0,
        "sale_revenue": 0.0,
        "credit_revenue": 0.0,
        "paid_credit": 0.0,
        "unpaid_credit": 0.0,
        "cash_revenue": 0.0,
        "mpesa_revenue": 0.0,
        "total_expenditure": 0.0,
        "paid_expenditure": 0.0,
        "balance_expenditure": 0.0,
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
    movement_map: dict = {}
    sales_map: dict = {}
    ending_map: dict = {}
    catalog_rows: list = []

    try:
        with get_cursor() as cur:
            cur.execute(revenue_totals_sql, tuple(sale_params))
            rt = cur.fetchone() or {}
            out["total_revenue"] = float(rt.get("total_revenue") or 0)
            out["sale_revenue"] = float(rt.get("sale_revenue") or 0)
            out["credit_revenue"] = float(rt.get("credit_revenue") or 0)
            out["paid_credit"] = round(float(rt.get("paid_credit") or 0), 2)
            out["cash_revenue"] = float(rt.get("cash_revenue") or 0)
            out["mpesa_revenue"] = float(rt.get("mpesa_revenue") or 0)

            cur.execute(movement_sql, tuple(sst_params))
            for r in cur.fetchall() or []:
                try:
                    iid = int(r.get("item_id") or 0)
                except Exception:
                    continue
                if iid > 0:
                    movement_map[iid] = {
                        "stock_in": int(r.get("stock_in") or 0),
                        "stock_out": int(r.get("stock_out") or 0),
                    }

            if column_exists("shop_stock_transactions", "cost_total"):
                cur.execute(cost_sql, tuple(sst_params))
                for r in cur.fetchall() or []:
                    try:
                        iid = int(r.get("item_id") or 0)
                    except Exception:
                        continue
                    if iid <= 0:
                        continue
                    pos_out = round(float(r.get("pos_out_cost") or 0), 2)
                    hold_ret = round(float(r.get("hold_ret_cost") or 0), 2)
                    manual_out = round(float(r.get("manual_out_cost") or 0), 2)
                    sold_cost = round(max(0.0, pos_out - hold_ret), 2)
                    cost_map[iid] = {
                        "sold_cost": sold_cost,
                        "manual_out_cost": manual_out,
                        "total_cost": round(sold_cost + manual_out, 2),
                    }
                    fifo_stock_out_cost += manual_out

            cur.execute(sales_by_id_sql, tuple(sale_params))
            for r in cur.fetchall() or []:
                try:
                    iid = int(r.get("item_id") or 0)
                except Exception:
                    continue
                if iid <= 0:
                    continue
                sales_map[iid] = {
                    "stock_sold": int(r.get("stock_sold") or 0),
                    "revenue": float(r.get("revenue") or 0),
                    "sale_qty": int(r.get("sale_qty") or 0),
                    "credit_qty": int(r.get("credit_qty") or 0),
                    "sale_amount": float(r.get("sale_amount") or 0),
                    "credit_amount": float(r.get("credit_amount") or 0),
                    "credit_paid_amount": float(r.get("credit_paid_amount") or 0),
                }

            cur.execute(sales_by_name_sql, tuple(sale_params))
            for r in cur.fetchall() or []:
                try:
                    iid = int(r.get("item_id") or 0)
                except Exception:
                    continue
                if iid <= 0:
                    continue
                prev = sales_map.get(iid) or {
                    "stock_sold": 0,
                    "revenue": 0.0,
                    "sale_qty": 0,
                    "credit_qty": 0,
                    "sale_amount": 0.0,
                    "credit_amount": 0.0,
                    "credit_paid_amount": 0.0,
                }
                sales_map[iid] = {
                    "stock_sold": int(prev["stock_sold"]) + int(r.get("stock_sold") or 0),
                    "revenue": float(prev["revenue"]) + float(r.get("revenue") or 0),
                    "sale_qty": int(prev["sale_qty"]) + int(r.get("sale_qty") or 0),
                    "credit_qty": int(prev["credit_qty"]) + int(r.get("credit_qty") or 0),
                    "sale_amount": float(prev["sale_amount"]) + float(r.get("sale_amount") or 0),
                    "credit_amount": float(prev["credit_amount"]) + float(r.get("credit_amount") or 0),
                    "credit_paid_amount": float(prev["credit_paid_amount"])
                    + float(r.get("credit_paid_amount") or 0),
                }

            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", range_end):
                cur.execute(ending_shop_sql, (sid, range_end))
                for r in cur.fetchall() or []:
                    try:
                        iid = int(r.get("item_id") or 0)
                    except Exception:
                        continue
                    if iid > 0:
                        ending_map[iid] = int(r.get("qty") or 0)

            cur.execute(current_stock_sql, (sid,))
            catalog_rows = cur.fetchall() or []
    except pymysql.Error:
        return out

    out["expenditure_rows"] = list_shop_expenditure_for_report(sid, af)
    exp_totals = _sum_shop_expenditure_totals(out["expenditure_rows"])
    out["total_expenditure"] = exp_totals["total_expenditure"]
    out["paid_expenditure"] = exp_totals["paid_expenditure"]
    out["balance_expenditure"] = exp_totals["balance_expenditure"]
    credit_rev = float(out.get("credit_revenue") or 0)
    paid_cr = float(out.get("paid_credit") or 0)
    out["unpaid_credit"] = round(max(0.0, credit_rev - paid_cr), 2)
    out["collected_revenue"] = round(float(out.get("sale_revenue") or 0) + paid_cr, 2)
    _, accrual_net_collected = _shop_report_accrual_net_profit_pair(out)
    out["net_profit"] = accrual_net_collected
    catalog_by_id = _report_catalog_by_item_id(catalog_rows)
    affected_ids = set(movement_map.keys()) | set(sales_map.keys())
    missing_meta = _report_fetch_item_meta_by_ids(
        [iid for iid in affected_ids if iid not in catalog_by_id]
    )
    items_out = []
    for iid in affected_ids:
        r = catalog_by_id.get(iid) or missing_meta.get(iid) or {}
        mv = movement_map.get(iid) or {"stock_in": 0, "stock_out": 0}
        sl = sales_map.get(iid) or {
            "stock_sold": 0,
            "revenue": 0.0,
            "sale_qty": 0,
            "credit_qty": 0,
            "sale_amount": 0.0,
            "credit_amount": 0.0,
            "credit_paid_amount": 0.0,
        }
        stock_in = int(mv["stock_in"])
        stock_out = int(mv["stock_out"])
        stock_sold = int(sl["stock_sold"])
        revenue = float(sl["revenue"])
        if not _report_item_had_period_activity(stock_in, stock_out, stock_sold, revenue):
            continue
        shop_qty = int(r.get("shop_stock_qty") or 0)
        ending_stock = ending_map.get(iid, shop_qty)
        starting_stock = ending_stock - stock_in + stock_out
        cost_row = cost_map.get(iid) or {}
        item_sold_cost = round(float(cost_row.get("sold_cost") or 0), 2)
        item_manual_out_cost = round(float(cost_row.get("manual_out_cost") or 0), 2)
        item_total_cost = round(float(cost_row.get("total_cost") or 0), 2)
        sale_qty_i = int(sl.get("sale_qty") or 0)
        credit_qty_i = int(sl.get("credit_qty") or 0)
        sold_qty_total = sale_qty_i + credit_qty_i
        if sold_qty_total > 0 and item_sold_cost > 0:
            sale_stock_cost = round(item_sold_cost * sale_qty_i / sold_qty_total, 2)
            credit_stock_cost_full = round(item_sold_cost - sale_stock_cost, 2)
        else:
            sale_stock_cost = 0.0
            credit_stock_cost_full = 0.0
        credit_amount_f = round(float(sl.get("credit_amount") or 0), 2)
        credit_paid_f = round(float(sl.get("credit_paid_amount") or 0), 2)
        credit_stock_cost = _credit_stock_cost_for_payment(
            credit_stock_cost_full, credit_amount_f, credit_paid_f
        )
        items_out.append(
            {
                "item_id": iid,
                "category": (r.get("category") or "").strip(),
                "name": (r.get("name") or "").strip() or f"Item #{iid}",
                "starting_stock": starting_stock,
                "ending_stock": ending_stock,
                "stock_in": stock_in,
                "stock_out": stock_out,
                "stock_sold": stock_sold,
                "revenue": round(revenue, 2),
                "sale_qty": sale_qty_i,
                "credit_qty": credit_qty_i,
                "sale_amount": round(float(sl.get("sale_amount") or 0), 2),
                "credit_amount": round(float(sl.get("credit_amount") or 0), 2),
                "credit_paid_amount": round(float(sl.get("credit_paid_amount") or 0), 2),
                "stock_cost_sold": item_sold_cost,
                "stock_cost_stock_out": item_manual_out_cost,
                "stock_cost_total": item_total_cost,
                "sale_stock_cost": sale_stock_cost,
                "credit_stock_cost": credit_stock_cost,
                "credit_stock_cost_full": credit_stock_cost_full,
            }
        )

    items_out.sort(key=lambda x: (-float(x.get("revenue") or 0), x.get("name") or ""))
    out["items"] = items_out
    _apply_shop_report_accrual_summary(out, stock_cost_stock_out=fifo_stock_out_cost)
    out["accrual_cogs_shop_mode_sales"] = round(
        float(out.get("accrual_cogs_sale") or 0) + float(out.get("accrual_cogs_credit") or 0),
        2,
    )
    openings = list_shop_day_openings_for_report(af, shop_id=sid)
    out["shop_openings"] = openings
    out["opening_cash_total"] = round(
        sum(float(o.get("opening_cash") or 0) for o in openings), 2
    )
    out["opening_mpesa_total"] = round(
        sum(float(o.get("opening_mpesa") or 0) for o in openings), 2
    )
    out["pos_allow_credit_sale"] = resolve_shop_pos_allow_credit_sale(sid)
    out["pos_inventory_mode"] = resolve_shop_pos_inventory_mode(sid)
    inv_mode = (out.get("pos_inventory_mode") or "shop").strip().lower()
    allow_credit = bool(out.get("pos_allow_credit_sale", True))
    if inv_mode in ("kitchen", "both"):
        out["accrual_cogs_kitchen_portions"] = sum_shop_kitchen_portion_cogs_for_report(
            sid,
            af,
            analytics_scope,
            allow_credit_sale=allow_credit,
        )
        portion_by_item = kitchen_portion_cogs_by_item_for_report(
            sid,
            af,
            analytics_scope,
            allow_credit_sale=allow_credit,
        )
        _apply_kitchen_portion_item_stock_costs(out.get("items") or [], portion_by_item)
        out["kitchen_portion_cost_warnings"] = kitchen_portion_cost_warnings_for_report(
            sid,
            af,
            analytics_scope,
            allow_credit_sale=allow_credit,
        )
    else:
        out["accrual_cogs_kitchen_portions"] = 0.0
        out["kitchen_portion_cost_warnings"] = []
    return _enrich_period_report_financials(out)


def list_company_stock_movements(
    analytics_filter: dict,
    shop_id: Optional[int] = None,
    employee_id: Optional[int] = None,
    supplier_search: Optional[str] = None,
    moved_by_contains: Optional[str] = None,
    sort_payment_status_groups: bool = False,
    limit: int = 1000,
):
    """Detailed stock movement log across company + shops."""
    st_where, st_params = _analytics_where_clause(analytics_filter, "st")
    sst_where, sst_params = _analytics_where_clause(analytics_filter, "sst")
    if employee_id is not None:
        st_where = f"({st_where}) AND st.created_by_employee_id=%s"
        st_params = list(st_params) + [int(employee_id)]
        sst_where = f"({sst_where}) AND sst.created_by_employee_id=%s"
        sst_params = list(sst_params) + [int(employee_id)]

    sup = (supplier_search or "").strip()
    if sup:
        like = f"%{sup}%"
        st_where = f"({st_where}) AND (COALESCE(st.place_brought_from,'') LIKE %s OR COALESCE(st.seller_phone,'') LIKE %s)"
        st_params = list(st_params) + [like, like]
        sst_where = f"({sst_where}) AND (COALESCE(sst.place_brought_from,'') LIKE %s OR COALESCE(sst.seller_phone,'') LIKE %s)"
        sst_params = list(sst_params) + [like, like]

    mb = (moved_by_contains or "").strip()
    if mb:
        like_mb = f"%{mb}%"
        st_where = f"({st_where}) AND COALESCE(e.full_name,'') LIKE %s"
        st_params = list(st_params) + [like_mb]
        sst_where = f"({sst_where}) AND COALESCE(e.full_name,'') LIKE %s"
        sst_params = list(sst_params) + [like_mb]

    if shop_id is not None:
        st_where = f"({st_where}) AND 1=0"
        sst_where = f"({sst_where}) AND sst.shop_id=%s"
        sst_params = list(sst_params) + [int(shop_id)]

    # Company stock movements.
    company_sql = f"""
    SELECT
      st.id AS tx_id,
      'company' AS movement_scope,
      st.created_at,
      st.item_id,
      i.name AS item_name,
      st.direction,
      'company' AS source,
      st.qty,
      CASE WHEN st.direction='in' THEN 'OUTSIDE' ELSE 'COMPANY' END AS from_where,
      CASE WHEN st.direction='in' THEN 'COMPANY' ELSE 'OUTSIDE' END AS to_where,
      st.buying_price AS buying_price,
      st.place_brought_from AS place_brought_from,
      st.seller_phone AS seller_phone,
      NULL AS payment_status,
      0.00 AS amount_paid,
      COALESCE(e.full_name, 'UNKNOWN') AS moved_by,
      NULL AS shop_id,
      'Company' AS shop_name,
      st.created_by_employee_id AS created_by_employee_id
    FROM stock_transactions st
    JOIN items i ON i.id = st.item_id
    LEFT JOIN employees e ON e.id = st.created_by_employee_id
    WHERE {st_where}
    """

    # Shop stock movements with explicit from/to labels.
    shop_sql = f"""
    SELECT
      sst.id AS tx_id,
      'shop' AS movement_scope,
      sst.created_at,
      sst.item_id,
      i.name AS item_name,
      sst.direction,
      sst.source AS source,
      sst.qty,
      CASE
        WHEN sst.source='manual' AND sst.direction='in' THEN 'OUTSIDE'
        WHEN sst.source='company' AND sst.direction='in' THEN 'COMPANY'
        WHEN sst.source='company' AND sst.direction='out' THEN sh.shop_name
        WHEN sst.source='transfer' AND sst.direction='out' THEN sh.shop_name
        WHEN sst.source='transfer' AND sst.direction='in' THEN 'TRANSFER IN'
        ELSE sh.shop_name
      END AS from_where,
      CASE
        WHEN sst.source='company' AND sst.direction='in' THEN sh.shop_name
        WHEN sst.source='company' AND sst.direction='out' THEN 'COMPANY'
        WHEN sst.source='transfer' AND sst.direction='out' THEN 'TRANSFER OUT'
        WHEN sst.source='transfer' AND sst.direction='in' THEN sh.shop_name
        WHEN sst.direction='out' AND UPPER(COALESCE(sst.reason,''))='POS' THEN 'CLIENT'
        WHEN sst.direction='out' THEN 'OUTSIDE'
        ELSE sh.shop_name
      END AS to_where,
      sst.buying_price AS buying_price,
      sst.place_brought_from AS place_brought_from,
      sst.seller_phone AS seller_phone,
      sst.payment_status AS payment_status,
      sst.amount_paid AS amount_paid,
      COALESCE(e.full_name, 'UNKNOWN') AS moved_by,
      sst.shop_id AS shop_id,
      sh.shop_name AS shop_name,
      sst.created_by_employee_id AS created_by_employee_id
    FROM shop_stock_transactions sst
    JOIN items i ON i.id = sst.item_id
    JOIN shops sh ON sh.id = sst.shop_id
    LEFT JOIN employees e ON e.id = sst.created_by_employee_id
    WHERE {sst_where}
    """

    params = list(st_params) + list(sst_params)
    if sort_payment_status_groups:
        order_sql = """
    ORDER BY
      CASE LOWER(COALESCE(mv.payment_status, ''))
        WHEN 'partially_paid' THEN 0
        WHEN 'pending_payment' THEN 1
        WHEN 'paid' THEN 2
        ELSE 3
      END ASC,
      mv.created_at DESC
    """
    else:
        order_sql = "ORDER BY mv.created_at DESC"

    sql = f"""
    SELECT * FROM (
      {company_sql}
      UNION ALL
      {shop_sql}
    ) mv
    {order_sql}
    LIMIT %s
    """
    params.append(int(limit))
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
    except pymysql.Error:
        return []
    out = []
    for r in rows:
        rr = dict(r)
        rr["tx_id"] = int(rr.get("tx_id") or 0)
        rr["qty"] = int(rr.get("qty") or 0)
        rr["item_name"] = rr.get("item_name") or "Item"
        rr["movement_scope"] = (rr.get("movement_scope") or "").strip().lower()
        rr["source"] = (rr.get("source") or "").strip().lower()
        rr["from_where"] = rr.get("from_where") or "UNKNOWN"
        rr["to_where"] = rr.get("to_where") or "UNKNOWN"
        rr["buying_price"] = float(rr.get("buying_price") or 0.0)
        rr["direction"] = (rr.get("direction") or "").strip().lower()
        rr["place_brought_from"] = rr.get("place_brought_from") or "-"
        rr["seller_phone"] = (rr.get("seller_phone") or "").strip()
        rr["payment_status"] = (rr.get("payment_status") or "").strip().lower()
        rr["amount_paid"] = float(rr.get("amount_paid") or 0.0)
        rr["total_cost"] = float(rr["qty"] * rr["buying_price"])
        rr["moved_by"] = rr.get("moved_by") or "UNKNOWN"
        sid = rr.get("shop_id")
        rr["shop_id"] = int(sid) if sid is not None else None
        rr["shop_name"] = (rr.get("shop_name") or "Company").strip() or "Company"
        eid = rr.get("created_by_employee_id")
        rr["created_by_employee_id"] = int(eid) if eid is not None else None
        rr["is_external_stock_in"] = (
            rr.get("movement_scope") == "company" and rr.get("direction") == "in"
        ) or (
            rr.get("movement_scope") == "shop"
            and rr.get("direction") == "in"
            and rr.get("source") == "manual"
        )
        out.append(rr)
    return out


def _supplier_stock_in_payment_snapshot(
    *,
    total_cost: float,
    amount_paid: float,
    payment_status: str,
) -> dict:
    """Align supplier stock-in paid/balance fields with payment_status for reporting."""
    total = round(max(0.0, float(total_cost or 0)), 2)
    paid = round(max(0.0, float(amount_paid or 0)), 2)
    status = (payment_status or "pending_payment").strip().lower()
    if status not in {"pending_payment", "partially_paid", "paid"}:
        status = "pending_payment"

    if status == "paid" and paid + 0.009 < total:
        paid = total
    elif paid + 0.009 >= total and total > 0:
        paid = total
        status = "paid"
    elif paid > 0.009 and paid + 0.009 < total:
        status = "partially_paid"
    elif paid <= 0.009 and status not in ("paid", "partially_paid"):
        status = "pending_payment"

    balance = round(max(total - paid, 0.0), 2)
    return {
        "amount_paid": paid,
        "balance": balance,
        "payment_status": status,
    }


def list_company_supplier_stock_ins(
    analytics_filter: Optional[dict] = None,
    shop_id: Optional[int] = None,
    supplier_search: Optional[str] = None,
    moved_by_contains: Optional[str] = None,
    limit: int = 5000,
):
    """Supplier stock-ins: company warehouse (``stock_transactions`` in) plus shop manual receipts."""
    af = analytics_filter or {}

    st_where, st_params = _analytics_where_clause(af, "st")
    st_where = f"({st_where}) AND st.direction='in'"

    sst_where, sst_params = _analytics_where_clause(af, "sst")
    sst_where = f"({sst_where}) AND sst.source='manual' AND sst.direction='in'"

    sup = (supplier_search or "").strip()
    if sup:
        like = f"%{sup}%"
        st_where = (
            f"({st_where}) AND "
            "(COALESCE(st.place_brought_from,'') LIKE %s OR COALESCE(st.seller_phone,'') LIKE %s)"
        )
        st_params = list(st_params) + [like, like]
        sst_where = (
            f"({sst_where}) AND "
            "(COALESCE(sst.place_brought_from,'') LIKE %s OR COALESCE(sst.seller_phone,'') LIKE %s)"
        )
        sst_params = list(sst_params) + [like, like]

    mb = (moved_by_contains or "").strip()
    if mb:
        like_mb = f"%{mb}%"
        st_where = f"({st_where}) AND COALESCE(e_st.full_name,'') LIKE %s"
        st_params = list(st_params) + [like_mb]
        sst_where = f"({sst_where}) AND COALESCE(e_sst.full_name,'') LIKE %s"
        sst_params = list(sst_params) + [like_mb]

    if shop_id is not None:
        st_where = f"({st_where}) AND 1=0"
        sst_where = f"({sst_where}) AND sst.shop_id=%s"
        sst_params = list(sst_params) + [int(shop_id)]

    company_sql = f"""
    SELECT
      st.id AS tx_id,
      st.created_at,
      0 AS shop_id,
      'Company' AS shop_name,
      i.id AS item_id,
      i.name AS item_name,
      st.qty,
      st.buying_price,
      COALESCE(NULLIF(st.place_brought_from, ''), '-') AS seller_name,
      COALESCE(NULLIF(st.seller_phone, ''), '-') AS seller_phone,
      COALESCE(st.payment_status, 'pending_payment') AS payment_status,
      COALESCE(st.amount_paid, 0) AS amount_paid,
      COALESCE(e_st.full_name, 'UNKNOWN') AS moved_by,
      COALESCE(st.note, '') AS note,
      'company' AS tx_scope
    FROM stock_transactions st
    JOIN items i ON i.id = st.item_id
    LEFT JOIN employees e_st ON e_st.id = st.created_by_employee_id
    WHERE {st_where}
    """

    shop_sql = f"""
    SELECT
      sst.id AS tx_id,
      sst.created_at,
      sst.shop_id,
      sh.shop_name,
      i.id AS item_id,
      i.name AS item_name,
      sst.qty,
      sst.buying_price,
      COALESCE(NULLIF(sst.place_brought_from, ''), '-') AS seller_name,
      COALESCE(NULLIF(sst.seller_phone, ''), '-') AS seller_phone,
      COALESCE(sst.payment_status, 'pending_payment') AS payment_status,
      COALESCE(sst.amount_paid, 0) AS amount_paid,
      COALESCE(e_sst.full_name, 'UNKNOWN') AS moved_by,
      COALESCE(sst.note, '') AS note,
      'shop' AS tx_scope
    FROM shop_stock_transactions sst
    JOIN items i ON i.id = sst.item_id
    JOIN shops sh ON sh.id = sst.shop_id
    LEFT JOIN employees e_sst ON e_sst.id = sst.created_by_employee_id
    WHERE {sst_where}
    """

    sql = f"""
    SELECT * FROM (
      {company_sql}
      UNION ALL
      {shop_sql}
    ) u
    ORDER BY u.created_at DESC, u.tx_id DESC
    LIMIT %s
    """
    params = list(st_params) + list(sst_params) + [int(limit)]
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
    except pymysql.Error:
        return []

    out = []
    for r in rows:
        qty = int(r.get("qty") or 0)
        buying_price = float(r.get("buying_price") or 0)
        total_cost = max(0.0, float(qty * buying_price))
        pay = _supplier_stock_in_payment_snapshot(
            total_cost=total_cost,
            amount_paid=float(r.get("amount_paid") or 0),
            payment_status=(r.get("payment_status") or "pending_payment"),
        )
        scope = (r.get("tx_scope") or "shop").strip().lower()
        if scope not in ("company", "shop"):
            scope = "shop"
        out.append(
            {
                "tx_id": int(r.get("tx_id") or 0),
                "tx_scope": scope,
                "created_at": r.get("created_at"),
                "shop_id": int(r.get("shop_id") or 0),
                "shop_name": (r.get("shop_name") or "Shop").strip() or "Shop",
                "item_id": int(r.get("item_id") or 0),
                "item_name": (r.get("item_name") or "Item").strip() or "Item",
                "qty": qty,
                "buying_price": buying_price,
                "total_cost": total_cost,
                "seller_name": (r.get("seller_name") or "-").strip() or "-",
                "seller_phone": (r.get("seller_phone") or "-").strip() or "-",
                "payment_status": pay["payment_status"],
                "amount_paid": pay["amount_paid"],
                "balance": pay["balance"],
                "moved_by": (r.get("moved_by") or "UNKNOWN").strip() or "UNKNOWN",
                "note": (r.get("note") or "").strip(),
            }
        )
    return out


def get_shop_manual_stock_in_transaction(tx_id: int):
    """Return one manual shop stock-in transaction with item/shop context."""
    sql = """
    SELECT
      sst.id AS tx_id,
      sst.created_at,
      sst.shop_id,
      sh.shop_name,
      i.id AS item_id,
      i.name AS item_name,
      sst.qty,
      sst.buying_price,
      COALESCE(NULLIF(sst.place_brought_from, ''), '-') AS seller_name,
      COALESCE(NULLIF(sst.seller_phone, ''), '-') AS seller_phone,
      COALESCE(sst.payment_status, 'pending_payment') AS payment_status,
      COALESCE(sst.amount_paid, 0) AS amount_paid,
      COALESCE(e.full_name, 'UNKNOWN') AS moved_by,
      COALESCE(sst.note, '') AS note
    FROM shop_stock_transactions sst
    JOIN items i ON i.id = sst.item_id
    JOIN shops sh ON sh.id = sst.shop_id
    LEFT JOIN employees e ON e.id = sst.created_by_employee_id
    WHERE sst.id=%s
      AND sst.source='manual'
      AND sst.direction='in'
    LIMIT 1
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (int(tx_id),))
            r = cur.fetchone()
        if not r:
            return None
        qty = round(float(r.get("qty") or 0), STOCK_QTY_DECIMAL_PLACES)
        buying_price = float(r.get("buying_price") or 0)
        total_cost = max(0.0, float(qty * buying_price))
        pay = _supplier_stock_in_payment_snapshot(
            total_cost=total_cost,
            amount_paid=float(r.get("amount_paid") or 0),
            payment_status=(r.get("payment_status") or "pending_payment"),
        )
        return {
            "tx_id": int(r.get("tx_id") or 0),
            "tx_scope": "shop",
            "created_at": r.get("created_at"),
            "shop_id": int(r.get("shop_id") or 0),
            "shop_name": (r.get("shop_name") or "Shop").strip() or "Shop",
            "item_id": int(r.get("item_id") or 0),
            "item_name": (r.get("item_name") or "Item").strip() or "Item",
            "qty": qty,
            "buying_price": buying_price,
            "total_cost": total_cost,
            "seller_name": (r.get("seller_name") or "-").strip() or "-",
            "seller_phone": (r.get("seller_phone") or "-").strip() or "-",
            "payment_status": pay["payment_status"],
            "amount_paid": pay["amount_paid"],
            "balance": pay["balance"],
            "moved_by": (r.get("moved_by") or "UNKNOWN").strip() or "UNKNOWN",
            "note": (r.get("note") or "").strip(),
        }
    except pymysql.Error:
        return None


def get_company_stock_in_transaction(tx_id: int):
    """Return one company warehouse stock-in (``stock_transactions`` direction in)."""
    sql = """
    SELECT
      st.id AS tx_id,
      st.created_at,
      0 AS shop_id,
      'Company' AS shop_name,
      i.id AS item_id,
      i.name AS item_name,
      st.qty,
      st.buying_price,
      COALESCE(NULLIF(st.place_brought_from, ''), '-') AS seller_name,
      COALESCE(NULLIF(st.seller_phone, ''), '-') AS seller_phone,
      COALESCE(st.payment_status, 'pending_payment') AS payment_status,
      COALESCE(st.amount_paid, 0) AS amount_paid,
      COALESCE(e.full_name, 'UNKNOWN') AS moved_by,
      COALESCE(st.note, '') AS note
    FROM stock_transactions st
    JOIN items i ON i.id = st.item_id
    LEFT JOIN employees e ON e.id = st.created_by_employee_id
    WHERE st.id=%s
      AND st.direction='in'
    LIMIT 1
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (int(tx_id),))
            r = cur.fetchone()
        if not r:
            return None
        qty = round(float(r.get("qty") or 0), STOCK_QTY_DECIMAL_PLACES)
        buying_price = float(r.get("buying_price") or 0)
        total_cost = max(0.0, float(qty * buying_price))
        pay = _supplier_stock_in_payment_snapshot(
            total_cost=total_cost,
            amount_paid=float(r.get("amount_paid") or 0),
            payment_status=(r.get("payment_status") or "pending_payment"),
        )
        return {
            "tx_id": int(r.get("tx_id") or 0),
            "tx_scope": "company",
            "created_at": r.get("created_at"),
            "shop_id": 0,
            "shop_name": "Company",
            "item_id": int(r.get("item_id") or 0),
            "item_name": (r.get("item_name") or "Item").strip() or "Item",
            "qty": qty,
            "buying_price": buying_price,
            "total_cost": total_cost,
            "seller_name": (r.get("seller_name") or "-").strip() or "-",
            "seller_phone": (r.get("seller_phone") or "-").strip() or "-",
            "payment_status": pay["payment_status"],
            "amount_paid": pay["amount_paid"],
            "balance": pay["balance"],
            "moved_by": (r.get("moved_by") or "UNKNOWN").strip() or "UNKNOWN",
            "note": (r.get("note") or "").strip(),
        }
    except pymysql.Error:
        return None


def _shop_stock_in_fifo_layers_for_tx(cur, tx_id: int) -> list:
    """FIFO cost layers tied to a manual stock-in transaction."""
    if not ensure_shop_stock_fifo_schema():
        return []
    cur.execute(
        """
        SELECT id, qty_remaining, unit_cost
        FROM shop_stock_cost_layers
        WHERE source_transaction_id=%s
        FOR UPDATE
        """,
        (int(tx_id),),
    )
    return cur.fetchall() or []


def _shop_stock_in_fifo_remaining_for_tx(cur, tx_id: int) -> float:
    layers = _shop_stock_in_fifo_layers_for_tx(cur, tx_id)
    return round(
        sum(round(float(r.get("qty_remaining") or 0), STOCK_QTY_DECIMAL_PLACES) for r in layers),
        STOCK_QTY_DECIMAL_PLACES,
    )


def _shop_stock_in_fifo_consumed(cur, tx_id: int, tx_qty) -> bool:
    """True when FIFO shows part of this stock-in layer was already consumed."""
    q = round(float(tx_qty or 0), STOCK_QTY_DECIMAL_PLACES)
    if q <= 0:
        return False
    rem = _shop_stock_in_fifo_remaining_for_tx(cur, tx_id)
    if rem <= 0:
        return False
    return rem + 1e-9 < q


def _shop_stock_in_payment_fields(qty, buying_price, amount_paid) -> dict:
    total_cost = max(0.0, round(float(qty or 0) * float(buying_price or 0), 2))
    try:
        paid = round(float(amount_paid or 0), 2)
    except Exception:
        paid = 0.0
    if paid < 0:
        paid = 0.0
    if total_cost <= 0:
        status = "paid"
        paid = 0.0
    elif paid <= 0:
        status = "pending_payment"
    elif paid + 0.009 < total_cost:
        status = "partially_paid"
    else:
        status = "paid"
        paid = total_cost
    return {
        "total_cost": total_cost,
        "amount_paid": paid,
        "payment_status": status,
    }


def _delete_shop_stock_in_fifo_layers(cur, tx_id: int) -> None:
    if not ensure_shop_stock_fifo_schema():
        return
    layers = _shop_stock_in_fifo_layers_for_tx(cur, tx_id)
    for layer in layers:
        lid = int(layer.get("id") or 0)
        if lid <= 0:
            continue
        cur.execute(
            "DELETE FROM shop_stock_fifo_consumptions WHERE layer_id=%s",
            (lid,),
        )
    cur.execute(
        "DELETE FROM shop_stock_cost_layers WHERE source_transaction_id=%s",
        (int(tx_id),),
    )


def delete_shop_manual_stock_in(tx_id: int) -> Tuple[bool, str]:
    """Remove a manual shop stock-in and reverse its quantity from shop stock."""
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(
                """
                SELECT id, shop_id, item_id, qty, source, direction
                FROM shop_stock_transactions
                WHERE id=%s
                FOR UPDATE
                """,
                (int(tx_id),),
            )
            tx = cur.fetchone()
            if not tx:
                return False, "Transaction not found."
            if (tx.get("source") or "").strip().lower() != "manual":
                return False, "Only manual stock-in supplies can be deleted."
            if (tx.get("direction") or "").strip().lower() != "in":
                return False, "Only stock-in supplies can be deleted."

            qty = round(float(tx.get("qty") or 0), STOCK_QTY_DECIMAL_PLACES)
            if qty <= 0:
                return False, "Invalid transaction quantity."
            if _shop_stock_in_fifo_consumed(cur, int(tx_id), qty):
                return (
                    False,
                    "Cannot delete: part of this supply has already been used in sales or stock outs.",
                )

            shop_id = int(tx.get("shop_id") or 0)
            item_id = int(tx.get("item_id") or 0)
            if shop_id <= 0 or item_id <= 0:
                return False, "Invalid shop or item on this transaction."

            cur.execute(
                f"SELECT {_shop_items_physical_select_sql()} FROM shop_items WHERE shop_id=%s AND item_id=%s FOR UPDATE",
                (shop_id, item_id),
            )
            si = cur.fetchone()
            if not si:
                return False, "Shop item not found."
            shop_now = round(float(si.get("shop_stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
            if shop_now + 1e-9 < qty:
                return False, "Cannot delete: insufficient shop stock on hand."

            shop_after = round(shop_now - qty, STOCK_QTY_DECIMAL_PLACES)
            cur.execute(
                "UPDATE shop_items SET shop_stock_qty=%s WHERE shop_id=%s AND item_id=%s",
                (shop_after, shop_id, item_id),
            )
            _delete_shop_stock_in_fifo_layers(cur, int(tx_id))
            cur.execute("DELETE FROM shop_stock_transactions WHERE id=%s", (int(tx_id),))
        return True, "Supply deleted."
    except pymysql.Error as e:
        logger.warning("delete_shop_manual_stock_in failed: %s", e)
        return False, "Could not delete supply."


def delete_company_stock_in(tx_id: int) -> Tuple[bool, str]:
    """Remove a company warehouse stock-in and reverse its quantity."""
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(
                """
                SELECT id, item_id, qty, direction
                FROM stock_transactions
                WHERE id=%s
                FOR UPDATE
                """,
                (int(tx_id),),
            )
            tx = cur.fetchone()
            if not tx:
                return False, "Transaction not found."
            if (tx.get("direction") or "").strip().lower() != "in":
                return False, "Only stock-in supplies can be deleted."

            qty = round(float(tx.get("qty") or 0), STOCK_QTY_DECIMAL_PLACES)
            if qty <= 0:
                return False, "Invalid transaction quantity."
            item_id = int(tx.get("item_id") or 0)
            if item_id <= 0:
                return False, "Invalid item on this transaction."

            cur.execute(
                "SELECT stock_qty FROM items WHERE id=%s FOR UPDATE",
                (item_id,),
            )
            item = cur.fetchone()
            if not item:
                return False, "Item not found."
            before = round(float(item.get("stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
            if before + 1e-9 < qty:
                return False, "Cannot delete: insufficient company stock on hand."

            after = round(before - qty, STOCK_QTY_DECIMAL_PLACES)
            cur.execute("UPDATE items SET stock_qty=%s WHERE id=%s", (after, item_id))
            cur.execute("DELETE FROM stock_transactions WHERE id=%s", (int(tx_id),))
        return True, "Supply deleted."
    except pymysql.Error as e:
        logger.warning("delete_company_stock_in failed: %s", e)
        return False, "Could not delete supply."


def update_shop_manual_stock_in(
    tx_id: int,
    *,
    qty,
    buying_price,
    note: Optional[str] = None,
    place_brought_from: Optional[str] = None,
    seller_phone: Optional[str] = None,
    amount_paid: Optional[float] = None,
) -> Tuple[bool, str, Optional[dict]]:
    """Edit a manual shop stock-in (qty, cost, supplier, note, payment)."""
    n = normalize_stock_move_qty(qty)
    if n is None:
        return False, "Invalid quantity.", None
    try:
        bp = float(buying_price if buying_price is not None else 0)
    except Exception:
        return False, "Invalid unit price.", None
    if not math.isfinite(bp) or bp < 0:
        return False, "Invalid unit price.", None

    place_clean = (place_brought_from or "").strip() or None
    phone_raw = (seller_phone or "").strip()
    if place_clean or phone_raw:
        if not place_clean:
            return False, "Seller name is required when a phone is provided.", None
        seller_phone_norm = _stored_seller_phone(seller_phone)
        if not seller_phone_norm:
            return False, "Seller phone must have at least 7 digits.", None
        place_clean = place_clean.upper()
    else:
        place_clean = None
        seller_phone_norm = None

    try:
        with get_cursor(commit=True) as cur:
            cur.execute(
                """
                SELECT id, shop_id, item_id, qty, buying_price, source, direction,
                       COALESCE(amount_paid, 0) AS amount_paid
                FROM shop_stock_transactions
                WHERE id=%s
                FOR UPDATE
                """,
                (int(tx_id),),
            )
            tx = cur.fetchone()
            if not tx:
                return False, "Transaction not found.", None
            if (tx.get("source") or "").strip().lower() != "manual":
                return False, "Only manual stock-in supplies can be edited.", None
            if (tx.get("direction") or "").strip().lower() != "in":
                return False, "Only stock-in supplies can be edited.", None

            old_qty = round(float(tx.get("qty") or 0), STOCK_QTY_DECIMAL_PLACES)
            delta = round(n - old_qty, STOCK_QTY_DECIMAL_PLACES)
            shop_id = int(tx.get("shop_id") or 0)
            item_id = int(tx.get("item_id") or 0)
            if shop_id <= 0 or item_id <= 0:
                return False, "Invalid shop or item on this transaction.", None

            if delta < 0 and _shop_stock_in_fifo_consumed(cur, int(tx_id), old_qty):
                rem = _shop_stock_in_fifo_remaining_for_tx(cur, int(tx_id))
                if rem + delta < -1e-9:
                    return (
                        False,
                        "Cannot reduce quantity: part of this supply has already been used.",
                        None,
                    )

            cur.execute(
                f"SELECT {_shop_items_physical_select_sql()} FROM shop_items WHERE shop_id=%s AND item_id=%s FOR UPDATE",
                (shop_id, item_id),
            )
            si = cur.fetchone()
            if not si:
                return False, "Shop item not found.", None
            shop_now = round(float(si.get("shop_stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
            if delta < 0 and shop_now + delta < -1e-9:
                return False, "Cannot reduce quantity: insufficient shop stock on hand.", None

            shop_after = round(shop_now + delta, STOCK_QTY_DECIMAL_PLACES)
            if delta != 0:
                cur.execute(
                    "UPDATE shop_items SET shop_stock_qty=%s WHERE shop_id=%s AND item_id=%s",
                    (shop_after, shop_id, item_id),
                )
                layers = _shop_stock_in_fifo_layers_for_tx(cur, int(tx_id))
                if layers:
                    primary = layers[0]
                    lid = int(primary.get("id") or 0)
                    rem = round(float(primary.get("qty_remaining") or 0), STOCK_QTY_DECIMAL_PLACES)
                    new_rem = round(rem + delta, STOCK_QTY_DECIMAL_PLACES)
                    if new_rem < -1e-9:
                        return (
                            False,
                            "Cannot reduce quantity: part of this supply has already been used.",
                            None,
                        )
                    if lid > 0:
                        cur.execute(
                            "UPDATE shop_stock_cost_layers SET qty_remaining=%s WHERE id=%s",
                            (max(0.0, new_rem), lid),
                        )
                elif delta > 0 and ensure_shop_stock_fifo_schema():
                    _shop_fifo_add_layer(cur, shop_id, item_id, delta, bp, int(tx_id))

            paid_raw = amount_paid if amount_paid is not None else float(tx.get("amount_paid") or 0)
            pay = _shop_stock_in_payment_fields(n, bp, paid_raw)
            cost_total = pay["total_cost"]
            cur.execute(
                """
                UPDATE shop_stock_transactions
                SET qty=%s, buying_price=%s, place_brought_from=%s, seller_phone=%s,
                    note=%s, amount_paid=%s, payment_status=%s, shop_stock_after=%s
                WHERE id=%s
                """,
                (
                    n,
                    bp,
                    place_clean,
                    seller_phone_norm,
                    (note or "").strip() or None,
                    pay["amount_paid"],
                    pay["payment_status"],
                    shop_after,
                    int(tx_id),
                ),
            )
            if column_exists("shop_stock_transactions", "cost_total"):
                cur.execute(
                    "UPDATE shop_stock_transactions SET cost_total=%s WHERE id=%s",
                    (cost_total, int(tx_id)),
                )
            layers = _shop_stock_in_fifo_layers_for_tx(cur, int(tx_id))
            if layers:
                lid = int(layers[0].get("id") or 0)
                if lid > 0:
                    cur.execute(
                        "UPDATE shop_stock_cost_layers SET unit_cost=%s WHERE id=%s",
                        (round(bp, 2), lid),
                    )
                    if column_exists("shop_stock_transactions", "cost_total"):
                        rem = round(float(layers[0].get("qty_remaining") or 0), STOCK_QTY_DECIMAL_PLACES)
                        cur.execute(
                            "UPDATE shop_stock_transactions SET cost_total=%s WHERE id=%s",
                            (round(rem * bp, 2), int(tx_id)),
                        )

            return True, "Supply updated.", {
                "id": int(tx_id),
                "qty": n,
                "buying_price": bp,
                "total_cost": cost_total,
                "amount_paid": pay["amount_paid"],
                "payment_status": pay["payment_status"],
            }
    except pymysql.Error as e:
        logger.warning("update_shop_manual_stock_in failed: %s", e)
        return False, "Could not update supply.", None


def update_company_stock_in(
    tx_id: int,
    *,
    qty,
    buying_price,
    note: Optional[str] = None,
    place_brought_from: Optional[str] = None,
    seller_phone: Optional[str] = None,
    amount_paid: Optional[float] = None,
) -> Tuple[bool, str, Optional[dict]]:
    """Edit a company warehouse stock-in."""
    n = normalize_stock_move_qty(qty)
    if n is None:
        return False, "Invalid quantity.", None
    try:
        bp = float(buying_price if buying_price is not None else 0)
    except Exception:
        return False, "Invalid unit price.", None
    if not math.isfinite(bp) or bp < 0:
        return False, "Invalid unit price.", None

    place_clean = (place_brought_from or "").strip() or None
    phone_raw = (seller_phone or "").strip()
    if place_clean or phone_raw:
        if not place_clean:
            return False, "Seller name is required when a phone is provided.", None
        seller_phone_norm = _stored_seller_phone(seller_phone)
        if not seller_phone_norm:
            return False, "Seller phone must have at least 7 digits.", None
        place_clean = place_clean.upper()
    else:
        place_clean = None
        seller_phone_norm = None

    try:
        with get_cursor(commit=True) as cur:
            cur.execute(
                """
                SELECT id, item_id, qty, buying_price, direction,
                       COALESCE(amount_paid, 0) AS amount_paid, stock_before, stock_after
                FROM stock_transactions
                WHERE id=%s
                FOR UPDATE
                """,
                (int(tx_id),),
            )
            tx = cur.fetchone()
            if not tx:
                return False, "Transaction not found.", None
            if (tx.get("direction") or "").strip().lower() != "in":
                return False, "Only stock-in supplies can be edited.", None

            old_qty = round(float(tx.get("qty") or 0), STOCK_QTY_DECIMAL_PLACES)
            delta = round(n - old_qty, STOCK_QTY_DECIMAL_PLACES)
            item_id = int(tx.get("item_id") or 0)
            if item_id <= 0:
                return False, "Invalid item on this transaction.", None

            cur.execute("SELECT stock_qty FROM items WHERE id=%s FOR UPDATE", (item_id,))
            item = cur.fetchone()
            if not item:
                return False, "Item not found.", None
            stock_now = round(float(item.get("stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
            if delta < 0 and stock_now + delta < -1e-9:
                return False, "Cannot reduce quantity: insufficient company stock on hand.", None

            stock_after = round(stock_now + delta, STOCK_QTY_DECIMAL_PLACES)
            if delta != 0:
                cur.execute(
                    "UPDATE items SET stock_qty=%s WHERE id=%s",
                    (stock_after, item_id),
                )

            paid_raw = amount_paid if amount_paid is not None else float(tx.get("amount_paid") or 0)
            pay = _shop_stock_in_payment_fields(n, bp, paid_raw)
            cur.execute(
                """
                UPDATE stock_transactions
                SET qty=%s, buying_price=%s, place_brought_from=%s, seller_phone=%s,
                    note=%s, amount_paid=%s, payment_status=%s, stock_after=%s
                WHERE id=%s
                """,
                (
                    n,
                    bp,
                    place_clean,
                    seller_phone_norm,
                    (note or "").strip() or None,
                    pay["amount_paid"],
                    pay["payment_status"],
                    stock_after,
                    int(tx_id),
                ),
            )
            return True, "Supply updated.", {
                "id": int(tx_id),
                "qty": n,
                "buying_price": bp,
                "total_cost": pay["total_cost"],
                "amount_paid": pay["amount_paid"],
                "payment_status": pay["payment_status"],
            }
    except pymysql.Error as e:
        logger.warning("update_company_stock_in failed: %s", e)
        return False, "Could not update supply.", None


def get_shop_stock_qty_map_for_item(item_id: int) -> dict:
    """Return {shop_id: shop_stock_qty} for one item across all shops."""
    try:
        with get_cursor() as cur:
            cur.execute(
                """
                SELECT shop_id, shop_stock_qty
                FROM shop_items
                WHERE item_id=%s
                """,
                (int(item_id),),
            )
            rows = cur.fetchall() or []
        out = {}
        for r in rows:
            sid = int(r.get("shop_id") or 0)
            if sid > 0:
                out[sid] = round(float(r.get("shop_stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
        return out
    except pymysql.Error:
        return {}


def shop_request_stock_from_company(
    *,
    shop_id: int,
    item_id: int,
    qty,
    note: Optional[str] = None,
    created_by_employee_id: Optional[int] = None,
    stock_request_id: Optional[int] = None,
    cur=None,
) -> bool:
    n = normalize_stock_move_qty(qty)
    if n is None:
        return False

    def _run(cur):
        # Lock company item row.
        cur.execute(
            "SELECT stock_qty, status FROM items WHERE id=%s FOR UPDATE",
            (int(item_id),),
        )
        item = cur.fetchone()
        if not item or item.get("status") != "active":
            return False
        company_before = round(float(item.get("stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
        if company_before < n:
            return False

        # Lock shop item row.
        cur.execute(
            f"SELECT {_shop_items_physical_select_sql()} FROM shop_items WHERE shop_id=%s AND item_id=%s FOR UPDATE",
            (int(shop_id), int(item_id)),
        )
        si = cur.fetchone()
        if not si or not _shop_item_physical_stock_tracking_ok(si, shop_id=int(shop_id)):
            return False

        shop_before = round(float(si.get("shop_stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
        shop_after = round(shop_before + n, STOCK_QTY_DECIMAL_PLACES)
        company_after = round(company_before - n, STOCK_QTY_DECIMAL_PLACES)

        cur.execute("UPDATE items SET stock_qty=%s WHERE id=%s", (company_after, int(item_id)))
        cur.execute(
            "UPDATE shop_items SET shop_stock_qty=%s WHERE shop_id=%s AND item_id=%s",
            (shop_after, int(shop_id), int(item_id)),
        )
        has_sr = column_exists("shop_stock_transactions", "stock_request_id") and stock_request_id
        if has_sr:
            cur.execute(
                """
                INSERT INTO shop_stock_transactions
                    (shop_id, item_id, direction, source, qty, shop_stock_before, shop_stock_after,
                     company_stock_before, company_stock_after, payment_status, note, created_by_employee_id, stock_request_id)
                VALUES (%s,%s,'in','company',%s,%s,%s,%s,%s,'paid',%s,%s,%s)
                """,
                (
                    int(shop_id),
                    int(item_id),
                    n,
                    shop_before,
                    shop_after,
                    company_before,
                    company_after,
                    note or None,
                    created_by_employee_id,
                    int(stock_request_id),
                ),
            )
        else:
            cur.execute(
                """
                INSERT INTO shop_stock_transactions
                    (shop_id, item_id, direction, source, qty, shop_stock_before, shop_stock_after,
                     company_stock_before, company_stock_after, payment_status, note, created_by_employee_id)
                VALUES (%s,%s,'in','company',%s,%s,%s,%s,%s,'paid',%s,%s)
                """,
                (
                    int(shop_id),
                    int(item_id),
                    n,
                    shop_before,
                    shop_after,
                    company_before,
                    company_after,
                    note or None,
                    created_by_employee_id,
                ),
            )
        in_tx_id = int(cur.lastrowid or 0)
        unit_cost = _shop_fifo_default_unit_cost(cur, int(shop_id), int(item_id))
        if in_tx_id > 0:
            _shop_fifo_after_stock_in(cur, int(shop_id), int(item_id), n, unit_cost, in_tx_id)
        return True

    if cur is not None:
        return _run(cur)
    with get_cursor(commit=True) as cur:
        return _run(cur)


def shop_return_stock_to_company(
    *,
    shop_id: int,
    item_id: int,
    qty,
    reason: Optional[str] = None,
    refunded: bool = False,
    refund_amount: Optional[float] = None,
    note: Optional[str] = None,
    created_by_employee_id: Optional[int] = None,
    stock_request_id: Optional[int] = None,
    cur=None,
) -> bool:
    n = normalize_stock_move_qty(qty)
    if n is None:
        return False
    reason = (reason or "").strip().lower() or None
    if reason not in (None, "return", "waste"):
        return False
    refunded = bool(refunded)
    if refunded:
        try:
            refund_amount = float(refund_amount) if refund_amount is not None and str(refund_amount).strip() != "" else None
        except Exception:
            return False
        if refund_amount is None or refund_amount < 0:
            return False
    else:
        refund_amount = None

    def _run(cur):
        # Lock company item row.
        cur.execute(
            "SELECT stock_qty, status FROM items WHERE id=%s FOR UPDATE",
            (int(item_id),),
        )
        item = cur.fetchone()
        if not item or item.get("status") != "active":
            return False
        company_before = round(float(item.get("stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)

        # Lock shop item row.
        cur.execute(
            f"SELECT {_shop_items_physical_select_sql()} FROM shop_items WHERE shop_id=%s AND item_id=%s FOR UPDATE",
            (int(shop_id), int(item_id)),
        )
        si = cur.fetchone()
        if not si or not _shop_item_physical_stock_tracking_ok(si, shop_id=int(shop_id)):
            return False

        shop_before = round(float(si.get("shop_stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
        if shop_before < n:
            return False
        shop_after = round(shop_before - n, STOCK_QTY_DECIMAL_PLACES)
        company_after = round(company_before + n, STOCK_QTY_DECIMAL_PLACES)

        cur.execute("UPDATE items SET stock_qty=%s WHERE id=%s", (company_after, int(item_id)))
        cur.execute(
            "UPDATE shop_items SET shop_stock_qty=%s WHERE shop_id=%s AND item_id=%s",
            (shop_after, int(shop_id), int(item_id)),
        )
        has_sr = column_exists("shop_stock_transactions", "stock_request_id") and stock_request_id
        if has_sr:
            cur.execute(
                """
                INSERT INTO shop_stock_transactions
                    (shop_id, item_id, direction, source, qty, shop_stock_before, shop_stock_after,
                     company_stock_before, company_stock_after, reason, refunded, refund_amount, payment_status, note, created_by_employee_id, stock_request_id)
                VALUES (%s,%s,'out','company',%s,%s,%s,%s,%s,%s,%s,%s,'paid',%s,%s,%s)
                """,
                (
                    int(shop_id),
                    int(item_id),
                    n,
                    shop_before,
                    shop_after,
                    company_before,
                    company_after,
                    reason,
                    1 if refunded else 0,
                    refund_amount,
                    note or None,
                    created_by_employee_id,
                    int(stock_request_id),
                ),
            )
        else:
            cur.execute(
                """
                INSERT INTO shop_stock_transactions
                    (shop_id, item_id, direction, source, qty, shop_stock_before, shop_stock_after,
                     company_stock_before, company_stock_after, reason, refunded, refund_amount, payment_status, note, created_by_employee_id)
                VALUES (%s,%s,'out','company',%s,%s,%s,%s,%s,%s,%s,%s,'paid',%s,%s)
                """,
                (
                    int(shop_id),
                    int(item_id),
                    n,
                    shop_before,
                    shop_after,
                    company_before,
                    company_after,
                    reason,
                    1 if refunded else 0,
                    refund_amount,
                    note or None,
                    created_by_employee_id,
                ),
            )
        out_tx_id = int(cur.lastrowid or 0)
        if out_tx_id > 0:
            _shop_fifo_after_stock_out(cur, int(shop_id), int(item_id), n, out_tx_id)
        return True

    if cur is not None:
        return _run(cur)
    with get_cursor(commit=True) as cur:
        return _run(cur)


def shop_manual_stock_in(
    *,
    shop_id: int,
    item_id: int,
    qty,
    buying_price: float,
    place_brought_from: Optional[str] = None,
    seller_phone: Optional[str] = None,
    payment_status: str = "pending_payment",
    note: Optional[str] = None,
    created_by_employee_id: Optional[int] = None,
) -> bool:
    n = normalize_stock_move_qty(qty)
    if n is None:
        return False
    if buying_price is None:
        buying_price = 0.0
    elif isinstance(buying_price, str):
        s = buying_price.strip().replace("\u00a0", "").replace(" ", "")
        if not s:
            buying_price = 0.0
        else:
            if "," in s and "." not in s:
                s = s.replace(",", ".")
            try:
                buying_price = float(s)
            except ValueError:
                return False
            if not math.isfinite(buying_price):
                return False
    else:
        try:
            buying_price = float(buying_price)
        except (TypeError, ValueError):
            return False
        if not math.isfinite(buying_price):
            return False
    if buying_price < 0:
        return False
    place_clean = (place_brought_from or "").strip() or None
    phone_raw = (seller_phone or "").strip()
    if place_clean or phone_raw:
        if not place_clean:
            return False
        seller_phone_norm = _stored_seller_phone(seller_phone)
        if not seller_phone_norm:
            return False
        place_clean = place_clean.upper()
    else:
        place_clean = None
        seller_phone_norm = None
    payment_status = (payment_status or "pending_payment").strip().lower()
    if payment_status not in {"pending_payment", "partially_paid", "paid"}:
        payment_status = "pending_payment"
    line_total = max(0.0, round(float(n) * buying_price, 2))
    if payment_status == "paid":
        amount_paid = line_total
    else:
        amount_paid = 0.0

    with get_cursor(commit=True) as cur:
        cur.execute(
            f"SELECT {_shop_items_physical_select_sql()} FROM shop_items WHERE shop_id=%s AND item_id=%s FOR UPDATE",
            (int(shop_id), int(item_id)),
        )
        si = cur.fetchone()
        if not si or not _shop_item_physical_stock_tracking_ok(si, shop_id=int(shop_id)):
            return False
        shop_before = round(float(si.get("shop_stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
        shop_after = round(shop_before + n, STOCK_QTY_DECIMAL_PLACES)

        cur.execute(
            "UPDATE shop_items SET shop_stock_qty=%s WHERE shop_id=%s AND item_id=%s",
            (shop_after, int(shop_id), int(item_id)),
        )
        cur.execute(
            """
            INSERT INTO shop_stock_transactions
                (shop_id, item_id, direction, source, qty, shop_stock_before, shop_stock_after,
                 buying_price, place_brought_from, seller_phone, payment_status, amount_paid, note, created_by_employee_id)
            VALUES (%s,%s,'in','manual',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                int(shop_id),
                int(item_id),
                n,
                shop_before,
                shop_after,
                buying_price,
                place_clean,
                seller_phone_norm,
                payment_status,
                amount_paid,
                note or None,
                created_by_employee_id,
            ),
        )
        _shop_fifo_after_stock_in(
            cur, int(shop_id), int(item_id), n, buying_price, int(cur.lastrowid or 0)
        )
        return True


def shop_manual_stock_out(
    *,
    shop_id: int,
    item_id: int,
    qty,
    reason: str,
    refunded: bool,
    refund_amount: Optional[float] = None,
    note: Optional[str] = None,
    created_by_employee_id: Optional[int] = None,
) -> bool:
    n = normalize_stock_move_qty(qty)
    if n is None:
        return False

    reason = (reason or "").strip().lower()
    allowed = {"return", "waste", "display"}
    if reason not in allowed:
        return False

    if refunded:
        try:
            refund_amount = float(refund_amount or 0)
            if refund_amount < 0:
                return False
        except Exception:
            return False
    else:
        refund_amount = None

    with get_cursor(commit=True) as cur:
        cur.execute(
            f"SELECT {_shop_items_physical_select_sql()} FROM shop_items WHERE shop_id=%s AND item_id=%s FOR UPDATE",
            (int(shop_id), int(item_id)),
        )
        si = cur.fetchone()
        if not si or not _shop_item_physical_stock_tracking_ok(si, shop_id=int(shop_id)):
            return False
        shop_before = round(float(si.get("shop_stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
        if shop_before < n:
            return False
        shop_after = round(shop_before - n, STOCK_QTY_DECIMAL_PLACES)

        cur.execute(
            "UPDATE shop_items SET shop_stock_qty=%s WHERE shop_id=%s AND item_id=%s",
            (shop_after, int(shop_id), int(item_id)),
        )
        cur.execute(
            """
            INSERT INTO shop_stock_transactions
                (shop_id, item_id, direction, source, qty, shop_stock_before, shop_stock_after,
                 reason, refunded, refund_amount, payment_status, note, created_by_employee_id)
                VALUES (%s,%s,'out','manual',%s,%s,%s,%s,%s,%s,'paid',%s,%s)
            """,
            (
                int(shop_id),
                int(item_id),
                n,
                shop_before,
                shop_after,
                reason.upper(),
                1 if refunded else 0,
                refund_amount if refund_amount is not None else None,
                note or None,
                created_by_employee_id,
            ),
        )
        _shop_fifo_after_stock_out(
            cur, int(shop_id), int(item_id), n, int(cur.lastrowid or 0)
        )
        return True


def shop_transfer_stock_between_shops(
    *,
    from_shop_id: int,
    to_shop_id: int,
    item_id: int,
    qty,
    note: Optional[str] = None,
    created_by_employee_id: Optional[int] = None,
    stock_request_id: Optional[int] = None,
    cur=None,
) -> bool:
    """Move stock for one item from one shop to another (atomic)."""
    n = normalize_stock_move_qty(qty)
    if n is None:
        return False
    if int(from_shop_id) == int(to_shop_id):
        return False

    def _run(cur):
        # Lock source + destination rows.
        cur.execute(
            f"SELECT {_shop_items_physical_select_sql()} FROM shop_items WHERE shop_id=%s AND item_id=%s FOR UPDATE",
            (int(from_shop_id), int(item_id)),
        )
        src = cur.fetchone()
        if not src or not _shop_item_physical_stock_tracking_ok(src, shop_id=int(from_shop_id)):
            return False
        src_before = round(float(src.get("shop_stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
        if src_before < n:
            return False

        cur.execute(
            f"SELECT {_shop_items_physical_select_sql()} FROM shop_items WHERE shop_id=%s AND item_id=%s FOR UPDATE",
            (int(to_shop_id), int(item_id)),
        )
        dst = cur.fetchone()
        if not dst or not _shop_item_physical_stock_tracking_ok(dst, shop_id=int(to_shop_id)):
            return False
        dst_before = round(float(dst.get("shop_stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)

        src_after = round(src_before - n, STOCK_QTY_DECIMAL_PLACES)
        dst_after = round(dst_before + n, STOCK_QTY_DECIMAL_PLACES)

        cur.execute(
            "UPDATE shop_items SET shop_stock_qty=%s WHERE shop_id=%s AND item_id=%s",
            (src_after, int(from_shop_id), int(item_id)),
        )
        cur.execute(
            "UPDATE shop_items SET shop_stock_qty=%s WHERE shop_id=%s AND item_id=%s",
            (dst_after, int(to_shop_id), int(item_id)),
        )

        has_sr = column_exists("shop_stock_transactions", "stock_request_id") and stock_request_id
        # Record OUT transaction for source shop.
        if has_sr:
            cur.execute(
                """
                INSERT INTO shop_stock_transactions
                    (shop_id, item_id, direction, source, qty, shop_stock_before, shop_stock_after,
                     reason, payment_status, note, created_by_employee_id, stock_request_id)
                VALUES (%s,%s,'out','transfer',%s,%s,%s,%s,'paid',%s,%s,%s)
                """,
                (
                    int(from_shop_id),
                    int(item_id),
                    n,
                    src_before,
                    src_after,
                    "TRANSFER",
                    note or None,
                    created_by_employee_id,
                    int(stock_request_id),
                ),
            )
        else:
            cur.execute(
                """
                INSERT INTO shop_stock_transactions
                    (shop_id, item_id, direction, source, qty, shop_stock_before, shop_stock_after,
                     reason, payment_status, note, created_by_employee_id)
                VALUES (%s,%s,'out','transfer',%s,%s,%s,%s,'paid',%s,%s)
                """,
                (
                    int(from_shop_id),
                    int(item_id),
                    n,
                    src_before,
                    src_after,
                    "TRANSFER",
                    note or None,
                    created_by_employee_id,
                ),
            )
        xfer_cost = _shop_fifo_after_stock_out(
            cur, int(from_shop_id), int(item_id), n, int(cur.lastrowid or 0)
        )
        xfer_unit = round(float(xfer_cost.get("avg_unit_cost") or 0), 2)
        # Record IN transaction for destination shop.
        if has_sr:
            cur.execute(
                """
                INSERT INTO shop_stock_transactions
                    (shop_id, item_id, direction, source, qty, shop_stock_before, shop_stock_after,
                     reason, payment_status, note, created_by_employee_id, stock_request_id)
                VALUES (%s,%s,'in','transfer',%s,%s,%s,%s,'paid',%s,%s,%s)
                """,
                (
                    int(to_shop_id),
                    int(item_id),
                    n,
                    dst_before,
                    dst_after,
                    "TRANSFER",
                    note or None,
                    created_by_employee_id,
                    int(stock_request_id),
                ),
            )
        else:
            cur.execute(
                """
                INSERT INTO shop_stock_transactions
                    (shop_id, item_id, direction, source, qty, shop_stock_before, shop_stock_after,
                     reason, payment_status, note, created_by_employee_id)
                VALUES (%s,%s,'in','transfer',%s,%s,%s,%s,'paid',%s,%s)
                """,
                (
                    int(to_shop_id),
                    int(item_id),
                    n,
                    dst_before,
                    dst_after,
                    "TRANSFER",
                    note or None,
                    created_by_employee_id,
                ),
            )
        in_tx_id = int(cur.lastrowid or 0)
        if in_tx_id > 0:
            _shop_fifo_after_stock_in(
                cur, int(to_shop_id), int(item_id), n, xfer_unit, in_tx_id
            )
        return True

    if cur is not None:
        return _run(cur)
    with get_cursor(commit=True) as cur:
        return _run(cur)


def get_site_settings(keys: Optional[list[str]] = None) -> dict:
    """Fetch settings as a dict. If keys is provided, fetch only those keys."""
    if keys:
        placeholders = ", ".join(["%s"] * len(keys))
        sql = f"SELECT `k`, `v` FROM site_settings WHERE `k` IN ({placeholders})"
        params = tuple(keys)
    else:
        sql = "SELECT `k`, `v` FROM site_settings"
        params = ()
    out: dict = {}
    try:
        with get_cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall() or []
        for r in rows:
            out[r["k"]] = r["v"]
    except pymysql.Error:
        return {}
    return out


def set_site_settings(values: dict) -> bool:
    """Upsert multiple settings."""
    if not values:
        return True
    sql = """
    INSERT INTO site_settings (`k`, `v`)
    VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE `v` = VALUES(`v`)
    """
    try:
        with get_cursor(commit=True) as cur:
            for k, v in values.items():
                cur.execute(sql, (str(k), None if v is None else str(v)))
        return True
    except pymysql.Error:
        return False


def _merge_printing_inventory_flags(merged: Dict[str, Any]) -> None:
    """Match app `_enforce_exclusive_pos_inventory` (dual vs kitchen-only vs shop-only)."""
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


        merged["pos_shop_stock_sale"] = True


def _merged_shop_printing_settings(shop_id: int) -> Dict[str, Any]:
    """Site printing defaults with optional per-shop override (same merge as POS checkout)."""
    defaults: Dict[str, Any] = {
        "pos_inventory_use_both": False,
        "pos_kitchen_portions": False,
        "pos_shop_stock_sale": True,
        "pos_allow_credit_sale": True,
    }
    try:
        sid = int(shop_id)
    except (TypeError, ValueError):
        return dict(defaults)
    if sid <= 0:
        return dict(defaults)
    try:
        raw = (get_site_settings(["printing_settings_json"]).get("printing_settings_json") or "").strip() or "{}"
        data = json.loads(raw)
        if not isinstance(data, dict):
            data = {}
        merged: Dict[str, Any] = {**defaults, **data}
        for k in ("pos_inventory_use_both", "pos_kitchen_portions", "pos_shop_stock_sale", "pos_allow_credit_sale"):
            if k in merged:
                merged[k] = merged[k] in (True, "true", "1", 1, "True")
        shop_row = get_shop_by_id(sid)
        ovr = shop_row.get("printing_settings_json") if shop_row else None
        if ovr and str(ovr).strip() and str(ovr).strip() != "{}":
            try:
                sdata = json.loads(ovr)
            except (json.JSONDecodeError, TypeError):
                sdata = {}
            if isinstance(sdata, dict):
                merged = {**merged, **sdata}
                for kk in ("pos_inventory_use_both", "pos_kitchen_portions", "pos_shop_stock_sale", "pos_allow_credit_sale"):
                    if kk in merged:
                        merged[kk] = merged[kk] in (True, "true", "1", 1, "True")
        _merge_printing_inventory_flags(merged)
        return merged
    except Exception:
        return dict(defaults)


def resolve_shop_pos_allow_credit_sale(shop_id: int) -> bool:
    """Whether credit sales are enabled for this shop's POS."""
    return bool(_merged_shop_printing_settings(shop_id).get("pos_allow_credit_sale"))


def resolve_shop_pos_inventory_mode(shop_id: int) -> str:
    """
    Effective POS inventory mode for a branch (site printing defaults + optional shop override).
    Values: kitchen, shop, both, none — same rules as the Flask app.
    """
    try:
        sid = int(shop_id)
    except (TypeError, ValueError):
        return "none"
    if sid <= 0:
        return "none"
    merged = _merged_shop_printing_settings(sid)
    k_on = bool(merged.get("pos_kitchen_portions"))
    s_on = bool(merged.get("pos_shop_stock_sale"))
    if k_on and s_on:
        return "both"
    if k_on:
        return "kitchen"
    if s_on:
        return "shop"
    return "none"


def _shop_item_physical_stock_tracking_ok(si_row: Optional[dict], *, shop_id: int) -> bool:
    """Shelf / manual stock ops: in 'both' mode only registered items; else stock_update_enabled."""
    if not si_row:
        return False
    mode = resolve_shop_pos_inventory_mode(int(shop_id))
    if mode == "kitchen":
        return False
    if mode == "both":
        if not column_exists("shop_items", "store_stock_registered"):
            return False
        return int(si_row.get("store_stock_registered") or 0) == 1
    return int(si_row.get("stock_update_enabled") or 0) == 1


def _shop_items_physical_select_sql() -> str:
    cols = "shop_stock_qty, stock_update_enabled"
    if column_exists("shop_items", "store_stock_registered"):
        return cols + ", COALESCE(store_stock_registered,0) AS store_stock_registered"
    return cols + ", 0 AS store_stock_registered"


HR_EMPLOYEE_SHOP_LINK_MODE_KEY = "hr_employee_shop_link_mode"
_hr_shop_link_mode_cache: Optional[tuple[float, str]] = None
_HR_SHOP_LINK_MODE_TTL_SEC = 120.0


def get_hr_employee_shop_link_mode() -> str:
    """Returns ``single`` (one branch per employee) or ``multi`` (POS access to selected branches)."""
    global _hr_shop_link_mode_cache
    import time

    now = time.monotonic()
    if _hr_shop_link_mode_cache is not None:
        cached_at, cached_val = _hr_shop_link_mode_cache
        if now - cached_at < _HR_SHOP_LINK_MODE_TTL_SEC:
            return cached_val
    raw = (get_site_settings([HR_EMPLOYEE_SHOP_LINK_MODE_KEY]) or {}).get(HR_EMPLOYEE_SHOP_LINK_MODE_KEY) or ""
    raw = str(raw).strip().lower()
    val = "multi" if raw == "multi" else "single"
    _hr_shop_link_mode_cache = (now, val)
    return val


def set_hr_employee_shop_link_mode(mode: str) -> bool:
    global _hr_shop_link_mode_cache
    m = "multi" if str(mode or "").strip().lower() == "multi" else "single"
    ok = set_site_settings({HR_EMPLOYEE_SHOP_LINK_MODE_KEY: m})
    if ok:
        _hr_shop_link_mode_cache = None
    return ok


# Pending requests older than this are eligible for automatic expiry (see expire_old_pending_stock_requests).
STOCK_REQUEST_PENDING_EXPIRY_DAYS = 30


def ensure_shop_stock_request_audit_schema() -> bool:
    """Migrations: expired status, audit event log, optional link from shop_stock_transactions."""
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(
                """
                SELECT COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'shop_stock_requests' AND COLUMN_NAME = 'status'
                LIMIT 1
                """
            )
            row = cur.fetchone() or {}
            ct = ""
            if isinstance(row, dict):
                ct = (row.get("COLUMN_TYPE") or row.get("column_type") or "") or ""
            if ct and "expired" not in ct.lower():
                cur.execute(
                    "ALTER TABLE shop_stock_requests MODIFY COLUMN status "
                    "ENUM('pending','approved','rejected','expired') NOT NULL DEFAULT 'pending'"
                )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS shop_stock_request_events (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    request_id INT NOT NULL,
                    event_type VARCHAR(32) NOT NULL,
                    actor_employee_id INT NULL,
                    actor_shop_id INT NULL,
                    payload_json JSON NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_ssre_request (request_id),
                    INDEX idx_ssre_created (created_at),
                    INDEX idx_ssre_type (event_type),
                    CONSTRAINT fk_ssre_request FOREIGN KEY (request_id) REFERENCES shop_stock_requests(id) ON DELETE CASCADE,
                    CONSTRAINT fk_ssre_actor_emp FOREIGN KEY (actor_employee_id) REFERENCES employees(id) ON DELETE SET NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            if not column_exists("shop_stock_transactions", "stock_request_id"):
                cur.execute(
                    "ALTER TABLE shop_stock_transactions ADD COLUMN stock_request_id INT NULL AFTER created_by_employee_id"
                )
                cur.execute("ALTER TABLE shop_stock_transactions ADD INDEX idx_sst_stock_request_id (stock_request_id)")
                try:
                    cur.execute(
                        "ALTER TABLE shop_stock_transactions ADD CONSTRAINT fk_sst_stock_request "
                        "FOREIGN KEY (stock_request_id) REFERENCES shop_stock_requests(id) ON DELETE SET NULL"
                    )
                except pymysql.Error:
                    pass
        return True
    except pymysql.Error as e:
        logger.warning("ensure_shop_stock_request_audit_schema: %s", e)
        return False


def _insert_stock_request_event_row(
    cur,
    *,
    request_id: int,
    event_type: str,
    actor_employee_id: Optional[int],
    actor_shop_id: Optional[int],
    payload: Optional[dict] = None,
) -> None:
    if not table_exists("shop_stock_request_events"):
        return
    payload_s = None
    if payload is not None:
        try:
            payload_s = json.dumps(payload, ensure_ascii=False, default=str)
            if len(payload_s) > 8000:
                payload_s = payload_s[:8000]
        except Exception:
            payload_s = None
    cur.execute(
        """
        INSERT INTO shop_stock_request_events (request_id, event_type, actor_employee_id, actor_shop_id, payload_json)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (
            int(request_id),
            (event_type or "")[:32],
            int(actor_employee_id) if actor_employee_id else None,
            int(actor_shop_id) if actor_shop_id else None,
            payload_s,
        ),
    )


def init_shop_stock_requests_table() -> bool:
    sql = """
    CREATE TABLE IF NOT EXISTS shop_stock_requests (
        id INT AUTO_INCREMENT PRIMARY KEY,
        requesting_shop_id INT NOT NULL,
        source_type ENUM('company','shop') NOT NULL,
        source_shop_id INT NULL,
        item_id INT NOT NULL,
        qty INT NOT NULL,
        status ENUM('pending','approved','rejected') NOT NULL DEFAULT 'pending',
        note VARCHAR(255) NULL,
        requested_by_employee_id INT NULL,
        reviewed_by_employee_id INT NULL,
        review_note VARCHAR(255) NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        reviewed_at TIMESTAMP NULL DEFAULT NULL,
        INDEX idx_shop_stock_requests_requesting_shop (requesting_shop_id),
        INDEX idx_shop_stock_requests_source_shop (source_shop_id),
        INDEX idx_shop_stock_requests_item (item_id),
        INDEX idx_shop_stock_requests_status (status),
        CONSTRAINT fk_ssr_requesting_shop FOREIGN KEY (requesting_shop_id) REFERENCES shops(id) ON DELETE CASCADE,
        CONSTRAINT fk_ssr_source_shop FOREIGN KEY (source_shop_id) REFERENCES shops(id) ON DELETE SET NULL,
        CONSTRAINT fk_ssr_item FOREIGN KEY (item_id) REFERENCES items(id) ON DELETE CASCADE,
        CONSTRAINT fk_ssr_requested_by FOREIGN KEY (requested_by_employee_id) REFERENCES employees(id) ON DELETE SET NULL,
        CONSTRAINT fk_ssr_reviewed_by FOREIGN KEY (reviewed_by_employee_id) REFERENCES employees(id) ON DELETE SET NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql)
            if not column_exists("shop_stock_requests", "request_type"):
                cur.execute(
                    "ALTER TABLE shop_stock_requests ADD COLUMN request_type ENUM('stock_in','return_to_company') NOT NULL DEFAULT 'stock_in' AFTER requesting_shop_id"
                )
            return True
    except pymysql.Error:
        return False


def init_notifications_table() -> bool:
    sql = """
    CREATE TABLE IF NOT EXISTS app_notifications (
        id INT AUTO_INCREMENT PRIMARY KEY,
        title VARCHAR(180) NOT NULL,
        message VARCHAR(500) NOT NULL,
        employee_id INT NULL,
        shop_id INT NULL,
        audience_role ENUM('all','admin_only') NOT NULL DEFAULT 'all',
        link_url VARCHAR(500) NULL,
        dedupe_key VARCHAR(96) NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_app_notifications_employee (employee_id),
        INDEX idx_app_notifications_shop (shop_id),
        INDEX idx_app_notifications_created (created_at),
        CONSTRAINT fk_app_notifications_employee FOREIGN KEY (employee_id) REFERENCES employees(id) ON DELETE CASCADE,
        CONSTRAINT fk_app_notifications_shop FOREIGN KEY (shop_id) REFERENCES shops(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql)
            if not column_exists("app_notifications", "dedupe_key"):
                cur.execute(
                    "ALTER TABLE app_notifications ADD COLUMN dedupe_key VARCHAR(96) NULL AFTER link_url"
                )
            cur.execute(
                """
                SELECT 1 FROM INFORMATION_SCHEMA.STATISTICS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'app_notifications'
                  AND INDEX_NAME = 'uq_app_notifications_dedupe_key'
                LIMIT 1
                """
            )
            if not cur.fetchone():
                cur.execute(
                    "CREATE UNIQUE INDEX uq_app_notifications_dedupe_key ON app_notifications (dedupe_key)"
                )
            return True
    except pymysql.Error:
        return False


def _insert_app_notification(
    cur,
    *,
    title: str,
    message: str,
    employee_id: Optional[int],
    shop_id: Optional[int],
    audience_role: str,
    link_url: Optional[str],
    dedupe_key: Optional[str],
) -> None:
    """Insert or upsert one notification row (same cursor = same transaction)."""
    t = (title or "").strip()[:180] or "Notification"
    m = (message or "").strip()[:500] or ""
    eid = int(employee_id) if employee_id else None
    sid = int(shop_id) if shop_id else None
    lu = (link_url or "").strip()[:500] or None
    dk = (dedupe_key or "").strip()[:96] or None
    if dk:
        cur.execute(
            """
            INSERT INTO app_notifications (title, message, employee_id, shop_id, audience_role, link_url, dedupe_key)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                title = VALUES(title),
                message = VALUES(message),
                link_url = VALUES(link_url),
                employee_id = VALUES(employee_id),
                shop_id = VALUES(shop_id),
                audience_role = VALUES(audience_role)
            """,
            (t, m, eid, sid, audience_role, lu, dk),
        )
    else:
        cur.execute(
            """
            INSERT INTO app_notifications (title, message, employee_id, shop_id, audience_role, link_url, dedupe_key)
            VALUES (%s, %s, %s, %s, %s, %s, NULL)
            """,
            (t, m, eid, sid, audience_role, lu),
        )


def create_notification(
    *,
    title: str,
    message: str,
    employee_id: Optional[int] = None,
    shop_id: Optional[int] = None,
    audience_role: str = "all",
    link_url: Optional[str] = None,
    dedupe_key: Optional[str] = None,
) -> bool:
    audience_role = (audience_role or "all").strip().lower()
    if audience_role not in ("all", "admin_only"):
        audience_role = "all"
    try:
        with get_cursor(commit=True) as cur:
            _insert_app_notification(
                cur,
                title=title,
                message=message,
                employee_id=employee_id,
                shop_id=shop_id,
                audience_role=audience_role,
                link_url=link_url,
                dedupe_key=dedupe_key,
            )
        return True
    except pymysql.Error:
        return False


def list_notifications_for_session(
    *,
    employee_id: Optional[int],
    shop_id: Optional[int],
    role_key: str,
    limit: int = 100,
    outcomes_only: bool = False,
):
    role_key = (role_key or "").strip().lower()
    is_admin = role_key in ("it_support", "super_admin", "company_manager")
    limit = max(1, min(int(limit), 500))
    outcome_sql = (
        " AND (dedupe_key LIKE 'sr:rev:%' OR dedupe_key LIKE 'sr:exp:%')"
        if outcomes_only
        else ""
    )
    try:
        with get_cursor() as cur:
            if is_admin:
                cur.execute(
                    f"""
                    SELECT id, title, message, employee_id, shop_id, audience_role, link_url, created_at
                    FROM app_notifications
                    WHERE 1=1{outcome_sql}
                    ORDER BY created_at DESC, id DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
            else:
                # Shop/employee users must not see admin_only rows (IT / super_admin alerts).
                conds = []
                params = []
                if employee_id:
                    conds.append("employee_id=%s")
                    params.append(int(employee_id))
                if shop_id:
                    conds.append("shop_id=%s")
                    params.append(int(shop_id))
                conds.append("(employee_id IS NULL AND shop_id IS NULL AND audience_role='all')")
                where_sql = " OR ".join(conds)
                cur.execute(
                    f"""
                    SELECT id, title, message, employee_id, shop_id, audience_role, link_url, created_at
                    FROM app_notifications
                    WHERE ({where_sql}) AND audience_role <> 'admin_only'{outcome_sql}
                    ORDER BY created_at DESC, id DESC
                    LIMIT %s
                    """,
                    tuple(params + [limit]),
                )
            return cur.fetchall() or []
    except pymysql.Error:
        return []


def count_notifications_for_session(*, employee_id: Optional[int], shop_id: Optional[int], role_key: str) -> int:
    role_key = (role_key or "").strip().lower()
    is_admin = role_key in ("it_support", "super_admin", "company_manager")
    try:
        with get_cursor() as cur:
            if is_admin:
                cur.execute("SELECT COUNT(*) AS c FROM app_notifications")
                row = cur.fetchone() or {}
                return int(row.get("c") or 0)
            conds = []
            params = []
            if employee_id:
                conds.append("employee_id=%s")
                params.append(int(employee_id))
            if shop_id:
                conds.append("shop_id=%s")
                params.append(int(shop_id))
            conds.append("(employee_id IS NULL AND shop_id IS NULL AND audience_role='all')")
            where_sql = " OR ".join(conds)
            cur.execute(
                f"SELECT COUNT(*) AS c FROM app_notifications WHERE ({where_sql}) AND audience_role <> 'admin_only'",
                tuple(params),
            )
            row = cur.fetchone() or {}
            return int(row.get("c") or 0)
    except pymysql.Error:
        return 0


def _stock_request_has_request_type_col() -> bool:
    return column_exists("shop_stock_requests", "request_type")


def _admin_stock_approval_queue_sql(*, alias: str = "r") -> str:
    """Pending rows company admins should action (company source or returns), not inter-shop."""
    a = (alias or "r").strip() or "r"
    if _stock_request_has_request_type_col():
        return (
            f" AND (COALESCE({a}.request_type, 'stock_in') = 'return_to_company'"
            f" OR {a}.source_type = 'company')"
        )
    return f" AND {a}.source_type = 'company'"


def _sum_pending_outbound_qty_for_source(
    cur,
    *,
    item_id: int,
    source_type: str,
    source_shop_id: Optional[int],
    exclude_request_id: Optional[int] = None,
) -> float:
    """Sum qty reserved by pending stock_in requests drawing from company or a source shop."""
    source_type = (source_type or "").strip().lower()
    item_id = int(item_id)
    if source_type == "company":
        sql = """
            SELECT COALESCE(SUM(qty), 0) AS s
            FROM shop_stock_requests
            WHERE status = 'pending' AND source_type = 'company' AND item_id = %s
        """
        params: list = [item_id]
        if _stock_request_has_request_type_col():
            sql += " AND COALESCE(request_type, 'stock_in') = 'stock_in'"
    elif source_type == "shop":
        sql = """
            SELECT COALESCE(SUM(qty), 0) AS s
            FROM shop_stock_requests
            WHERE status = 'pending' AND source_type = 'shop' AND source_shop_id = %s AND item_id = %s
        """
        params = [int(source_shop_id or 0), item_id]
        if _stock_request_has_request_type_col():
            sql += " AND COALESCE(request_type, 'stock_in') = 'stock_in'"
    else:
        return 0.0
    if exclude_request_id:
        sql += " AND id <> %s"
        params.append(int(exclude_request_id))
    cur.execute(sql, tuple(params))
    row = cur.fetchone() or {}
    return round(float(row.get("s") or 0), STOCK_QTY_DECIMAL_PLACES)


def _sum_pending_return_qty_from_shop(
    cur,
    *,
    shop_id: int,
    item_id: int,
    exclude_request_id: Optional[int] = None,
) -> float:
    """Sum qty reserved by pending return_to_company requests from a shop."""
    if not _stock_request_has_request_type_col():
        return 0.0
    shop_id = int(shop_id)
    item_id = int(item_id)
    sql = """
        SELECT COALESCE(SUM(qty), 0) AS s
        FROM shop_stock_requests
        WHERE status = 'pending'
          AND COALESCE(request_type, 'stock_in') = 'return_to_company'
          AND requesting_shop_id = %s
          AND item_id = %s
    """
    params: list = [shop_id, item_id]
    if exclude_request_id:
        sql += " AND id <> %s"
        params.append(int(exclude_request_id))
    cur.execute(sql, tuple(params))
    row = cur.fetchone() or {}
    return round(float(row.get("s") or 0), STOCK_QTY_DECIMAL_PLACES)


def _pending_outbound_qty_map_for_source(
    cur,
    *,
    item_ids: list,
    source_type: str,
    source_shop_id: Optional[int],
) -> dict:
    """Map item_id -> reserved pending outbound qty for a batch of items."""
    ids = [int(i) for i in (item_ids or []) if int(i or 0) > 0]
    if not ids:
        return {}
    source_type = (source_type or "").strip().lower()
    placeholders = ",".join(["%s"] * len(ids))
    if source_type == "company":
        sql = f"""
            SELECT item_id, COALESCE(SUM(qty), 0) AS s
            FROM shop_stock_requests
            WHERE status = 'pending' AND source_type = 'company' AND item_id IN ({placeholders})
        """
        params: list = list(ids)
        if _stock_request_has_request_type_col():
            sql += " AND COALESCE(request_type, 'stock_in') = 'stock_in'"
        sql += " GROUP BY item_id"
    elif source_type == "shop":
        sql = f"""
            SELECT item_id, COALESCE(SUM(qty), 0) AS s
            FROM shop_stock_requests
            WHERE status = 'pending' AND source_type = 'shop' AND source_shop_id = %s
              AND item_id IN ({placeholders})
        """
        params = [int(source_shop_id or 0)] + ids
        if _stock_request_has_request_type_col():
            sql += " AND COALESCE(request_type, 'stock_in') = 'stock_in'"
        sql += " GROUP BY item_id"
    else:
        return {}
    cur.execute(sql, tuple(params))
    out: dict = {}
    for r in cur.fetchall() or []:
        out[int(r["item_id"])] = round(float(r.get("s") or 0), STOCK_QTY_DECIMAL_PLACES)
    return out


def _available_qty_after_pending(
    physical_qty: float,
    pending_reserved: float,
) -> float:
    return round(
        max(0.0, round(float(physical_qty or 0), STOCK_QTY_DECIMAL_PLACES) - round(float(pending_reserved or 0), STOCK_QTY_DECIMAL_PLACES)),
        STOCK_QTY_DECIMAL_PLACES,
    )


def _find_pending_stock_request_duplicate(
    cur,
    *,
    requesting_shop_id: int,
    request_type: str,
    source_type: str,
    source_shop_id: Optional[int],
    item_id: int,
) -> Optional[dict]:
    """Return pending duplicate row if the same shop already has an open request for this line."""
    requesting_shop_id = int(requesting_shop_id)
    item_id = int(item_id)
    request_type = (request_type or "stock_in").strip().lower()
    source_type = (source_type or "").strip().lower()
    has_rt = _stock_request_has_request_type_col()
    if has_rt:
        cur.execute(
            """
            SELECT id, qty, note
            FROM shop_stock_requests
            WHERE status = 'pending'
              AND requesting_shop_id = %s
              AND request_type = %s
              AND source_type = %s
              AND item_id = %s
              AND (source_shop_id <=> %s)
            ORDER BY id DESC
            LIMIT 1
            """,
            (
                requesting_shop_id,
                request_type,
                source_type,
                item_id,
                int(source_shop_id) if source_shop_id else None,
            ),
        )
    else:
        if request_type == "return_to_company":
            return None
        cur.execute(
            """
            SELECT id, qty, note
            FROM shop_stock_requests
            WHERE status = 'pending'
              AND requesting_shop_id = %s
              AND source_type = %s
              AND item_id = %s
              AND (source_shop_id <=> %s)
            ORDER BY id DESC
            LIMIT 1
            """,
            (
                requesting_shop_id,
                source_type,
                item_id,
                int(source_shop_id) if source_shop_id else None,
            ),
        )
    row = cur.fetchone()
    return dict(row) if row else None


def list_shop_stock_request_alerts_for_shop(*, shop_id: int, limit: int = 20):
    """Outcome alerts for a shop that submitted stock requests (approved or cancelled)."""
    shop_id = int(shop_id)
    limit = max(1, min(int(limit), 50))
    try:
        with get_cursor() as cur:
            cur.execute(
                """
                SELECT id, title, message, link_url, created_at, dedupe_key
                FROM app_notifications
                WHERE shop_id = %s
                  AND dedupe_key LIKE 'sr:rev:%:rq'
                ORDER BY id DESC
                LIMIT %s
                """,
                (shop_id, limit),
            )
            return cur.fetchall() or []
    except pymysql.Error:
        return []


def create_shop_stock_request(
    *,
    requesting_shop_id: int,
    request_type: str = "stock_in",
    source_type: str,
    source_shop_id: Optional[int],
    item_id: int,
    qty,
    note: Optional[str] = None,
    requested_by_employee_id: Optional[int] = None,
) -> Tuple[Optional[int], str]:
    """
    Create or merge into a pending stock request.
    Returns (request_id, status_tag) where status_tag is '' (new), 'merged', or an error code.
    """
    request_type = (request_type or "stock_in").strip().lower()
    if request_type not in ("stock_in", "return_to_company"):
        return None, "invalid"
    source_type = (source_type or "").strip().lower()
    if source_type not in ("company", "shop"):
        return None, "invalid"
    qty = normalize_stock_move_qty(qty)
    if qty is None:
        return None, "invalid_qty"
    requesting_shop_id = int(requesting_shop_id)
    item_id = int(item_id)
    if request_type == "return_to_company":
        source_type = "company"
        source_shop_id = None
    elif source_type == "company":
        source_shop_id = None
    else:
        if not source_shop_id:
            return None, "invalid"
        source_shop_id = int(source_shop_id)
        if source_shop_id == requesting_shop_id:
            return None, "invalid"
    note_clean = (note or "").strip()[:255] or None
    try:
        with get_cursor(commit=True) as cur:
            cur.execute("SELECT id FROM shops WHERE id=%s LIMIT 1", (requesting_shop_id,))
            if not cur.fetchone():
                return None, "invalid"

            duplicate = _find_pending_stock_request_duplicate(
                cur,
                requesting_shop_id=requesting_shop_id,
                request_type=request_type,
                source_type=source_type,
                source_shop_id=source_shop_id,
                item_id=item_id,
            )
            eff_qty = qty
            merge_rid: Optional[int] = None
            if duplicate:
                merge_rid = int(duplicate.get("id") or 0) or None
                if not merge_rid:
                    return None, "invalid"
                eff_qty = round(
                    float(duplicate.get("qty") or 0) + float(qty),
                    STOCK_QTY_DECIMAL_PLACES,
                )

            if request_type == "return_to_company":
                cur.execute(
                    f"SELECT {_shop_items_physical_select_sql()} FROM shop_items WHERE shop_id=%s AND item_id=%s LIMIT 1",
                    (requesting_shop_id, item_id),
                )
                src = cur.fetchone()
                if not src or not _shop_item_physical_stock_tracking_ok(src, shop_id=requesting_shop_id):
                    return None, "unavailable"
                physical = round(float(src.get("shop_stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
                pending = _sum_pending_return_qty_from_shop(
                    cur,
                    shop_id=requesting_shop_id,
                    item_id=item_id,
                    exclude_request_id=merge_rid,
                )
                if _available_qty_after_pending(physical, pending) < eff_qty:
                    return None, "insufficient_stock"
            elif source_type == "shop":
                cur.execute("SELECT id FROM shops WHERE id=%s LIMIT 1", (source_shop_id,))
                if not cur.fetchone():
                    return None, "invalid"
                cur.execute(
                    f"SELECT {_shop_items_physical_select_sql()} FROM shop_items WHERE shop_id=%s AND item_id=%s LIMIT 1",
                    (source_shop_id, item_id),
                )
                src = cur.fetchone()
                if not src or not _shop_item_physical_stock_tracking_ok(src, shop_id=int(source_shop_id or 0)):
                    return None, "unavailable"
                physical = round(float(src.get("shop_stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
                pending = _sum_pending_outbound_qty_for_source(
                    cur,
                    item_id=item_id,
                    source_type="shop",
                    source_shop_id=source_shop_id,
                    exclude_request_id=merge_rid,
                )
                if _available_qty_after_pending(physical, pending) < eff_qty:
                    return None, "insufficient_stock"
            else:
                cur.execute(
                    f"SELECT {_shop_items_physical_select_sql()} FROM shop_items WHERE shop_id=%s AND item_id=%s LIMIT 1",
                    (requesting_shop_id, item_id),
                )
                dst_si = cur.fetchone()
                if not dst_si or not _shop_item_physical_stock_tracking_ok(dst_si, shop_id=requesting_shop_id):
                    return None, "unavailable"
                cur.execute("SELECT stock_qty, status FROM items WHERE id=%s LIMIT 1", (item_id,))
                item = cur.fetchone()
                if not item or item.get("status") != "active":
                    return None, "unavailable"
                physical = round(float(item.get("stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
                pending = _sum_pending_outbound_qty_for_source(
                    cur,
                    item_id=item_id,
                    source_type="company",
                    source_shop_id=None,
                    exclude_request_id=merge_rid,
                )
                if _available_qty_after_pending(physical, pending) < eff_qty:
                    return None, "insufficient_stock"

            if merge_rid:
                merged_note = note_clean
                prev_note = (duplicate.get("note") or "").strip() if duplicate else ""
                if note_clean and prev_note and note_clean != prev_note:
                    merged_note = (prev_note + "; " + note_clean)[:255]
                elif prev_note and not note_clean:
                    merged_note = prev_note[:255] or None
                cur.execute(
                    "UPDATE shop_stock_requests SET qty=%s, note=%s WHERE id=%s AND status='pending'",
                    (eff_qty, merged_note, merge_rid),
                )
                if not cur.rowcount:
                    return None, "invalid"
                _insert_stock_request_event_row(
                    cur,
                    request_id=merge_rid,
                    event_type="merged",
                    actor_employee_id=int(requested_by_employee_id) if requested_by_employee_id else None,
                    actor_shop_id=requesting_shop_id,
                    payload={
                        "added_qty": qty,
                        "new_qty": eff_qty,
                        "request_type": request_type,
                        "source_type": source_type,
                        "source_shop_id": source_shop_id,
                        "item_id": item_id,
                    },
                )
                return merge_rid, "merged"

            has_request_type = column_exists("shop_stock_requests", "request_type")
            if has_request_type:
                cur.execute(
                    """
                    INSERT INTO shop_stock_requests
                        (requesting_shop_id, request_type, source_type, source_shop_id, item_id, qty, note, requested_by_employee_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        requesting_shop_id,
                        request_type,
                        source_type,
                        source_shop_id,
                        item_id,
                        qty,
                        note_clean,
                        int(requested_by_employee_id) if requested_by_employee_id else None,
                    ),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO shop_stock_requests
                        (requesting_shop_id, source_type, source_shop_id, item_id, qty, note, requested_by_employee_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        requesting_shop_id,
                        source_type,
                        source_shop_id,
                        item_id,
                        qty,
                        note_clean,
                        int(requested_by_employee_id) if requested_by_employee_id else None,
                    ),
                )
            rid = int(cur.lastrowid or 0) or None
            if rid:
                _insert_stock_request_event_row(
                    cur,
                    request_id=rid,
                    event_type="created",
                    actor_employee_id=int(requested_by_employee_id) if requested_by_employee_id else None,
                    actor_shop_id=requesting_shop_id,
                    payload={
                        "request_type": request_type,
                        "source_type": source_type,
                        "source_shop_id": source_shop_id,
                        "item_id": item_id,
                        "qty": qty,
                    },
                )
            return rid, ""
    except pymysql.Error:
        return None, "error"


def cancel_stock_request_by_requester(
    *,
    request_id: int,
    requesting_shop_id: int,
    employee_id: Optional[int] = None,
) -> Tuple[bool, str]:
    """Allow the requesting shop to withdraw a pending request."""
    request_id = int(request_id)
    requesting_shop_id = int(requesting_shop_id)
    if request_id <= 0 or requesting_shop_id <= 0:
        return False, "Invalid request."
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(
                """
                SELECT id, requesting_shop_id, status
                FROM shop_stock_requests
                WHERE id=%s
                FOR UPDATE
                """,
                (request_id,),
            )
            row = cur.fetchone()
            if not row:
                return False, "Request not found."
            if int(row.get("requesting_shop_id") or 0) != requesting_shop_id:
                return False, "You can only cancel requests submitted by your shop."
            if (row.get("status") or "").lower() != "pending":
                return False, "This request is no longer pending."
            cur.execute(
                """
                UPDATE shop_stock_requests
                SET status='rejected', reviewed_by_employee_id=%s, review_note=%s, reviewed_at=NOW()
                WHERE id=%s AND status='pending'
                """,
                (
                    int(employee_id) if employee_id else None,
                    "Cancelled by requester",
                    request_id,
                ),
            )
            if not cur.rowcount:
                return False, "Could not cancel this request."
            _insert_stock_request_event_row(
                cur,
                request_id=request_id,
                event_type="cancelled",
                actor_employee_id=int(employee_id) if employee_id else None,
                actor_shop_id=requesting_shop_id,
                payload={"reason": "cancelled_by_requester"},
            )
            return True, ""
    except pymysql.Error:
        return False, "Something went wrong. Please try again."


def list_stock_requests_for_session(
    *,
    role_key: str,
    viewer_shop_id: Optional[int],
    limit: int = 200,
    status: Optional[str] = None,
    approval_queue_only: bool = False,
):
    role_key = (role_key or "").strip().lower()
    is_admin = role_key in ("it_support", "super_admin", "company_manager")
    limit = max(1, min(int(limit), 1000))
    status_filter = (status or "").strip().lower() or None
    try:
        with get_cursor() as cur:
            req_type_col = "r.request_type" if column_exists("shop_stock_requests", "request_type") else "'stock_in'"
            status_sql = " AND r.status = %s" if status_filter else ""
            status_params: tuple = (status_filter,) if status_filter else ()
            if approval_queue_only and not is_admin:
                approval_sql = (
                    " AND r.source_type = 'shop'"
                    " AND r.source_shop_id = %s"
                )
                if column_exists("shop_stock_requests", "request_type"):
                    approval_sql += " AND COALESCE(r.request_type, 'stock_in') <> 'return_to_company'"
                approval_params = (int(viewer_shop_id or 0),)
            elif approval_queue_only and is_admin:
                approval_sql = _admin_stock_approval_queue_sql(alias="r")
                approval_params = ()
            else:
                approval_sql = ""
                approval_params = ()
            if is_admin:
                cur.execute(
                    """
                    SELECT r.id, r.requesting_shop_id, """ + req_type_col + """ AS request_type, r.source_type, r.source_shop_id, r.item_id, r.qty, r.status,
                           r.note, r.requested_by_employee_id, r.reviewed_by_employee_id, r.review_note,
                           r.created_at, r.reviewed_at,
                           rq.shop_name AS requesting_shop_name,
                           ss.shop_name AS source_shop_name,
                           i.name AS item_name
                    FROM shop_stock_requests r
                    JOIN shops rq ON rq.id = r.requesting_shop_id
                    LEFT JOIN shops ss ON ss.id = r.source_shop_id
                    JOIN items i ON i.id = r.item_id
                    WHERE 1=1""" + status_sql + approval_sql + """
                    ORDER BY r.created_at DESC, r.id DESC
                    LIMIT %s
                    """,
                    status_params + approval_params + (limit,),
                )
            else:
                scope_sql = (
                    approval_sql
                    if approval_queue_only
                    else " AND (r.requesting_shop_id=%s OR r.source_shop_id=%s)"
                )
                scope_params = (
                    approval_params
                    if approval_queue_only
                    else (int(viewer_shop_id or 0), int(viewer_shop_id or 0))
                )
                cur.execute(
                    """
                    SELECT r.id, r.requesting_shop_id, """ + req_type_col + """ AS request_type, r.source_type, r.source_shop_id, r.item_id, r.qty, r.status,
                           r.note, r.requested_by_employee_id, r.reviewed_by_employee_id, r.review_note,
                           r.created_at, r.reviewed_at,
                           rq.shop_name AS requesting_shop_name,
                           ss.shop_name AS source_shop_name,
                           i.name AS item_name
                    FROM shop_stock_requests r
                    JOIN shops rq ON rq.id = r.requesting_shop_id
                    LEFT JOIN shops ss ON ss.id = r.source_shop_id
                    JOIN items i ON i.id = r.item_id
                    WHERE 1=1""" + scope_sql + status_sql + """
                    ORDER BY r.created_at DESC, r.id DESC
                    LIMIT %s
                    """,
                    scope_params + status_params + (limit,),
                )
            return cur.fetchall() or []
    except pymysql.Error:
        return []


def count_pending_stock_requests_for_session(*, role_key: str, viewer_shop_id: Optional[int]) -> int:
    """Pending stock requests this user can approve (matches Requests awaiting approval)."""
    role_key = (role_key or "").strip().lower()
    is_admin = role_key in ("it_support", "super_admin", "company_manager")
    try:
        with get_cursor() as cur:
            if is_admin:
                cur.execute(
                    """
                    SELECT COUNT(*) AS c
                    FROM shop_stock_requests r
                    WHERE r.status = 'pending'
                    """ + _admin_stock_approval_queue_sql(alias="r"),
                )
            else:
                return_sql = (
                    " AND COALESCE(r.request_type, 'stock_in') <> 'return_to_company'"
                    if column_exists("shop_stock_requests", "request_type")
                    else ""
                )
                cur.execute(
                    f"""
                    SELECT COUNT(*) AS c
                    FROM shop_stock_requests r
                    WHERE r.status = 'pending'
                      AND r.source_type = 'shop'
                      AND r.source_shop_id = %s{return_sql}
                    """,
                    (int(viewer_shop_id or 0),),
                )
            row = cur.fetchone() or {}
            return int(row.get("c") or 0)
    except pymysql.Error:
        return 0


def list_incoming_stock_requests_for_shop(*, source_shop_id: int, limit: int = 300, status: Optional[str] = None):
    """Incoming shop-to-shop stock requests where this shop is the source (all statuses for notifications page)."""
    source_shop_id = int(source_shop_id)
    limit = max(1, min(int(limit), 500))
    status_filter = (status or "").strip().lower() or None
    req_type_col = "r.request_type" if column_exists("shop_stock_requests", "request_type") else "'stock_in'"
    status_sql = " AND r.status = %s" if status_filter else ""
    status_params: tuple = (status_filter,) if status_filter else ()
    try:
        with get_cursor() as cur:
            cur.execute(
                """
                SELECT r.id, r.requesting_shop_id, """ + req_type_col + """ AS request_type, r.source_type, r.source_shop_id,
                       r.item_id, r.qty, r.status, r.note, r.requested_by_employee_id,
                       r.reviewed_by_employee_id, r.review_note, r.created_at, r.reviewed_at,
                       rq.shop_name AS requesting_shop_name,
                       ss.shop_name AS source_shop_name,
                       i.name AS item_name,
                       req_emp.full_name AS requested_by_name,
                       rev_emp.full_name AS reviewed_by_name,
                       COALESCE((
                         SELECT si.shop_stock_qty FROM shop_items si
                         WHERE si.shop_id = r.source_shop_id AND si.item_id = r.item_id
                         LIMIT 1
                       ), 0) AS source_shop_stock_qty
                FROM shop_stock_requests r
                JOIN shops rq ON rq.id = r.requesting_shop_id
                LEFT JOIN shops ss ON ss.id = r.source_shop_id
                JOIN items i ON i.id = r.item_id
                LEFT JOIN employees req_emp ON req_emp.id = r.requested_by_employee_id
                LEFT JOIN employees rev_emp ON rev_emp.id = r.reviewed_by_employee_id
                WHERE r.source_type = 'shop'
                  AND r.source_shop_id = %s""" + status_sql + """
                ORDER BY r.created_at DESC, r.id DESC
                LIMIT %s
                """,
                (source_shop_id,) + status_params + (limit,),
            )
            return cur.fetchall() or []
    except pymysql.Error:
        return []


def list_incoming_pending_stock_requests_for_shop(*, source_shop_id: int, limit: int = 30):
    """Pending stock-in requests where another shop asked to receive stock from this shop (POS popup)."""
    source_shop_id = int(source_shop_id)
    limit = max(1, min(int(limit), 100))
    req_type_col = "r.request_type" if column_exists("shop_stock_requests", "request_type") else "'stock_in'"
    try:
        with get_cursor() as cur:
            cur.execute(
                """
                SELECT r.id, r.requesting_shop_id, """ + req_type_col + """ AS request_type, r.source_type, r.source_shop_id,
                       r.item_id, r.qty, r.status, r.note, r.created_at,
                       rq.shop_name AS requesting_shop_name,
                       i.name AS item_name,
                       COALESCE((
                         SELECT si.shop_stock_qty FROM shop_items si
                         WHERE si.shop_id = r.source_shop_id AND si.item_id = r.item_id
                         LIMIT 1
                       ), 0) AS source_shop_stock_qty
                FROM shop_stock_requests r
                JOIN shops rq ON rq.id = r.requesting_shop_id
                JOIN items i ON i.id = r.item_id
                WHERE r.status = 'pending'
                  AND r.source_type = 'shop'
                  AND r.source_shop_id = %s
                ORDER BY r.created_at ASC, r.id ASC
                LIMIT %s
                """,
                (source_shop_id, limit),
            )
            return cur.fetchall() or []
    except pymysql.Error:
        return []


def _can_review_request(row: dict, *, approver_role: str, approver_shop_id: Optional[int]) -> bool:
    approver_role = (approver_role or "").strip().lower()
    if approver_role in ("it_support", "super_admin", "company_manager"):
        return True
    if (row.get("request_type") or "").lower() == "return_to_company":
        return False
    if (row.get("source_type") or "").lower() == "shop":
        try:
            return int(approver_shop_id or 0) == int(row.get("source_shop_id") or 0)
        except Exception:
            return False
    return False


def _can_fulfill_move_qty_for_request_row(cur, req: dict, move_qty) -> bool:
    """Same rules as can_fulfill_stock_request but for an approved quantity (same cursor/transaction)."""
    mq = normalize_stock_move_qty(move_qty)
    if mq is None:
        return False
    request_id = int(req.get("id") or 0) or None
    request_type = (req.get("request_type") or "stock_in").lower()
    source_type = (req.get("source_type") or "").lower()
    item_id = int(req.get("item_id") or 0)
    if request_type == "return_to_company":
        rq_shop = int(req.get("requesting_shop_id") or 0)
        cur.execute(
            f"SELECT {_shop_items_physical_select_sql()} FROM shop_items WHERE shop_id=%s AND item_id=%s LIMIT 1",
            (rq_shop, item_id),
        )
        row = cur.fetchone()
        if not row or not _shop_item_physical_stock_tracking_ok(row, shop_id=rq_shop):
            return False
        physical = round(float(row.get("shop_stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
        pending = _sum_pending_return_qty_from_shop(
            cur, shop_id=rq_shop, item_id=item_id, exclude_request_id=request_id
        )
        return _available_qty_after_pending(physical, pending) >= mq
    if source_type == "company":
        cur.execute("SELECT stock_qty, status FROM items WHERE id=%s LIMIT 1", (item_id,))
        row = cur.fetchone()
        if not row or row.get("status") != "active":
            return False
        physical = round(float(row.get("stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
        pending = _sum_pending_outbound_qty_for_source(
            cur,
            item_id=item_id,
            source_type="company",
            source_shop_id=None,
            exclude_request_id=request_id,
        )
        if _available_qty_after_pending(physical, pending) < mq:
            return False
        rq_shop = int(req.get("requesting_shop_id") or 0)
        cur.execute(
            f"SELECT {_shop_items_physical_select_sql()} FROM shop_items WHERE shop_id=%s AND item_id=%s LIMIT 1",
            (rq_shop, item_id),
        )
        dst = cur.fetchone()
        return bool(dst) and _shop_item_physical_stock_tracking_ok(dst, shop_id=rq_shop)
    if source_type == "shop":
        src_shop = int(req.get("source_shop_id") or 0)
        cur.execute(
            f"SELECT {_shop_items_physical_select_sql()} FROM shop_items WHERE shop_id=%s AND item_id=%s LIMIT 1",
            (src_shop, item_id),
        )
        row = cur.fetchone()
        if not row or not _shop_item_physical_stock_tracking_ok(row, shop_id=src_shop):
            return False
        physical = round(float(row.get("shop_stock_qty") or 0), STOCK_QTY_DECIMAL_PLACES)
        pending = _sum_pending_outbound_qty_for_source(
            cur,
            item_id=item_id,
            source_type="shop",
            source_shop_id=src_shop,
            exclude_request_id=request_id,
        )
        return _available_qty_after_pending(physical, pending) >= mq
    return False


def review_stock_request(
    *,
    request_id: int,
    approve: bool,
    approver_employee_id: Optional[int],
    approver_role: str,
    approver_shop_id: Optional[int],
    review_note: Optional[str] = None,
    fulfill_qty=None,
    delivered_by: Optional[str] = None,
) -> Tuple[bool, str]:
    """Approve or reject a pending stock request. Returns (success, error_message). error_message is empty on success."""
    request_id = int(request_id)
    if request_id <= 0:
        return False, "Invalid request."
    try:
        with get_cursor(commit=True) as cur:
            req_type_col = "request_type" if column_exists("shop_stock_requests", "request_type") else "'stock_in' AS request_type"
            cur.execute(
                """
                SELECT r.id, r.requesting_shop_id, """ + req_type_col + """, r.source_type, r.source_shop_id, r.item_id, r.qty, r.status, r.note, r.requested_by_employee_id,
                       rq.shop_name AS requesting_shop_name,
                       ss.shop_name AS source_shop_name,
                       i.name AS item_name
                FROM shop_stock_requests r
                JOIN shops rq ON rq.id = r.requesting_shop_id
                LEFT JOIN shops ss ON ss.id = r.source_shop_id
                JOIN items i ON i.id = r.item_id
                WHERE r.id=%s
                FOR UPDATE
                """,
                (request_id,),
            )
            req = cur.fetchone()
            if not req or (req.get("status") or "").lower() != "pending":
                return False, "This request is no longer pending."
            if not _can_review_request(req, approver_role=approver_role, approver_shop_id=approver_shop_id):
                return False, "You are not allowed to act on this request."

            rqty = round(float(req["qty"] or 0), STOCK_QTY_DECIMAL_PLACES)
            eff_qty = rqty
            if approve:
                if fulfill_qty is not None:
                    eff_qty = normalize_stock_move_qty(fulfill_qty)
                    if eff_qty is None:
                        return False, "Enter a valid positive quantity."
                if eff_qty <= 0 or eff_qty > rqty:
                    return False, "Enter a quantity greater than zero and not more than the amount requested."

            user_note = (review_note or "").strip()
            final_review_note: Optional[str] = user_note[:255] if user_note else None
            if approve and eff_qty < rqty:
                extra = f"Approved qty {eff_qty} (requested {rqty})."
                if final_review_note:
                    final_review_note = (final_review_note + " " + extra)[:255]
                else:
                    final_review_note = extra[:255]

            if approve:
                if not _can_fulfill_move_qty_for_request_row(cur, req, eff_qty):
                    return False, "Not enough stock at the source to approve this quantity."
                ok = False
                if (req.get("request_type") or "").lower() == "return_to_company":
                    ok = shop_return_stock_to_company(
                        shop_id=int(req["requesting_shop_id"]),
                        item_id=int(req["item_id"]),
                        qty=eff_qty,
                        reason="return",
                        refunded=False,
                        refund_amount=None,
                        note=(req.get("note") or "").strip() or f"Approved return request #{request_id}",
                        created_by_employee_id=approver_employee_id,
                        stock_request_id=request_id,
                        cur=cur,
                    )
                elif (req.get("source_type") or "").lower() == "company":
                    ok = shop_request_stock_from_company(
                        shop_id=int(req["requesting_shop_id"]),
                        item_id=int(req["item_id"]),
                        qty=eff_qty,
                        note=(req.get("note") or "").strip() or f"Approved request #{request_id}",
                        created_by_employee_id=approver_employee_id,
                        stock_request_id=request_id,
                        cur=cur,
                    )
                else:
                    xfer_note = (req.get("note") or "").strip() or f"Approved request #{request_id}"
                    if delivered_by is not None:
                        xfer_note = _transfer_note_with_delivered_by(xfer_note, delivered_by)
                    ok = shop_transfer_stock_between_shops(
                        from_shop_id=int(req["source_shop_id"]),
                        to_shop_id=int(req["requesting_shop_id"]),
                        item_id=int(req["item_id"]),
                        qty=eff_qty,
                        note=xfer_note,
                        created_by_employee_id=approver_employee_id,
                        stock_request_id=request_id,
                        cur=cur,
                    )
                if not ok:
                    return (
                        False,
                        "Stock could not be updated. Check stock levels and that tracking is enabled for this item.",
                    )
                new_status = "approved"
            else:
                new_status = "rejected"

            cur.execute(
                """
                UPDATE shop_stock_requests
                SET status=%s, reviewed_by_employee_id=%s, review_note=%s, reviewed_at=NOW()
                WHERE id=%s
                """,
                (
                    new_status,
                    int(approver_employee_id) if approver_employee_id else None,
                    final_review_note,
                    request_id,
                ),
            )
            ev_payload: dict = {
                "review_note": final_review_note,
                "new_status": new_status,
                "requested_qty": rqty,
            }
            if approve:
                ev_payload["fulfilled_qty"] = eff_qty
            _insert_stock_request_event_row(
                cur,
                request_id=request_id,
                event_type="approved" if approve else "rejected",
                actor_employee_id=int(approver_employee_id) if approver_employee_id else None,
                actor_shop_id=int(approver_shop_id) if approver_shop_id else None,
                payload=ev_payload,
            )

            status_word = "approved" if approve else "cancelled"
            item_label = ((req.get("item_name") or "").strip() or f"Item #{int(req['item_id'])}")[:200]
            rq_shop = int(req["requesting_shop_id"])
            st = (req.get("source_type") or "").lower()
            src_sid = int(req["source_shop_id"] or 0)
            src_nm = ((req.get("source_shop_name") or "").strip() or (f"Shop #{src_sid}" if src_sid else "Company"))[:120]
            if approve:
                if st == "shop" and src_sid > 0:
                    requester_title = "Stock request approved"
                    to_requester = (
                        f"Request #{request_id}: {item_label} × {eff_qty} approved. "
                        f"Stock was transferred from {src_nm} to your shop — levels updated at both shops."
                    )
                elif st == "company":
                    requester_title = "Stock request approved"
                    to_requester = (
                        f"Request #{request_id}: {item_label} × {eff_qty} approved. "
                        "Stock was added to your shop from company inventory."
                    )
                else:
                    requester_title = "Stock request approved"
                    to_requester = f"Request #{request_id}: {item_label} × {eff_qty} was approved."
            else:
                requester_title = "Stock request cancelled"
                to_requester = (
                    f"Request #{request_id} cancelled: {item_label} × {rqty}. "
                    "The supplying party declined this request."
                )
            link_requester = f"/shops/{rq_shop}/notifications"
            _insert_app_notification(
                cur,
                title=requester_title[:180],
                message=to_requester[:500],
                employee_id=None,
                shop_id=rq_shop,
                audience_role="all",
                link_url=link_requester[:500],
                dedupe_key=f"sr:rev:{request_id}:rq",
            )
            if st == "shop" and src_sid > 0:
                rq_nm = ((req.get("requesting_shop_name") or "").strip() or f"Shop #{rq_shop}")[:120]
                if approve:
                    to_source = (
                        f"Request #{request_id}: you transferred {item_label} × {eff_qty} to {rq_nm}. "
                        "Stock levels were updated at both shops."
                    )[:500]
                    _insert_app_notification(
                        cur,
                        title="Stock transfer completed",
                        message=to_source,
                        employee_id=None,
                        shop_id=src_sid,
                        audience_role="all",
                        link_url=f"/shops/{src_sid}/notifications"[:500],
                        dedupe_key=f"sr:rev:{request_id}:src:{src_sid}",
                    )
            return True, ""
    except pymysql.Error as e:
        logger.warning("review_stock_request failed for request_id=%s: %s", request_id, e)
        return False, "Something went wrong. Please try again."


def can_fulfill_stock_request(request_id: int, *, move_qty=None) -> bool:
    """True when current source stock can satisfy this pending request (full qty, or move_qty if given)."""
    try:
        with get_cursor() as cur:
            req_type_col = "request_type" if column_exists("shop_stock_requests", "request_type") else "'stock_in' AS request_type"
            cur.execute(
                """
                SELECT id, requesting_shop_id, """ + req_type_col + """, source_type, source_shop_id, item_id, qty, status
                FROM shop_stock_requests
                WHERE id=%s
                LIMIT 1
                """,
                (int(request_id),),
            )
            req = cur.fetchone()
            if not req:
                return False
            if (req.get("status") or "").lower() != "pending":
                return False
            rqty = round(float(req.get("qty") or 0), STOCK_QTY_DECIMAL_PLACES)
            if rqty <= 0:
                return False
            if move_qty is not None:
                check_qty = normalize_stock_move_qty(move_qty)
                if check_qty is None or check_qty > rqty:
                    return False
            else:
                check_qty = rqty
            return _can_fulfill_move_qty_for_request_row(cur, req, check_qty)
    except pymysql.Error:
        return False


def expire_old_pending_stock_requests(*, days: Optional[int] = None) -> int:
    """Mark stale pending requests as expired and append audit events (and notify requesting shop)."""
    d = int(days) if days is not None else STOCK_REQUEST_PENDING_EXPIRY_DAYS
    d = max(1, min(d, 3650))
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(
                """
                SELECT r.id, r.requesting_shop_id, r.item_id, r.qty, i.name AS item_name
                FROM shop_stock_requests r
                JOIN items i ON i.id = r.item_id
                WHERE r.status = 'pending' AND r.created_at < DATE_SUB(NOW(), INTERVAL %s DAY)
                ORDER BY r.id ASC
                LIMIT 500
                """,
                (d,),
            )
            rows = cur.fetchall() or []
            n = 0
            for row in rows:
                rid = int(row.get("id") or 0)
                if rid <= 0:
                    continue
                rq_shop = int(row.get("requesting_shop_id") or 0)
                item_label = ((row.get("item_name") or "").strip() or f"Item #{int(row.get('item_id') or 0)}")[:200]
                cur.execute(
                    "UPDATE shop_stock_requests SET status='expired', reviewed_at=NOW() WHERE id=%s AND status='pending'",
                    (rid,),
                )
                if not cur.rowcount:
                    continue
                n += 1
                _insert_stock_request_event_row(
                    cur,
                    request_id=rid,
                    event_type="expired",
                    actor_employee_id=None,
                    actor_shop_id=None,
                    payload={"reason": "pending_timeout_days", "days": d},
                )
                if rq_shop > 0:
                    _insert_app_notification(
                        cur,
                        title="Stock request expired",
                        message=(
                            f"Request #{rid}: {item_label} × {int(row.get('qty') or 0)} was not approved in time and has expired."
                        )[:500],
                        employee_id=None,
                        shop_id=rq_shop,
                        audience_role="all",
                        link_url=f"/shops/{rq_shop}/notifications"[:500],
                        dedupe_key=f"sr:exp:{rid}",
                    )
            return n
    except pymysql.Error:
        return 0


def list_stock_requests_audit_rows(
    *,
    status: Optional[str] = None,
    shop_id: Optional[int] = None,
    limit: int = 500,
    offset: int = 0,
):
    """IT/super_admin: full request list with optional filters."""
    limit = max(1, min(int(limit), 5000))
    offset = max(0, min(int(offset), 100000))
    req_type_col = "r.request_type" if column_exists("shop_stock_requests", "request_type") else "'stock_in'"
    has_events = table_exists("shop_stock_request_events")
    ev_sql = (
        "(SELECT COUNT(*) FROM shop_stock_request_events e WHERE e.request_id = r.id) AS event_count"
        if has_events
        else "0 AS event_count"
    )
    try:
        with get_cursor() as cur:
            where = ["1=1"]
            params: list = []
            if status:
                st = (status or "").strip().lower()
                if st in ("pending", "approved", "rejected", "expired"):
                    where.append("r.status = %s")
                    params.append(st)
            if shop_id:
                sid = int(shop_id)
                where.append("(r.requesting_shop_id = %s OR r.source_shop_id = %s)")
                params.extend([sid, sid])
            where_sql = " AND ".join(where)
            sql = f"""
                SELECT r.id, r.requesting_shop_id, {req_type_col} AS request_type, r.source_type, r.source_shop_id,
                       r.item_id, r.qty, r.status, r.note, r.requested_by_employee_id, r.reviewed_by_employee_id,
                       r.review_note, r.created_at, r.reviewed_at,
                       rq.shop_name AS requesting_shop_name, ss.shop_name AS source_shop_name, i.name AS item_name,
                       {ev_sql}
                FROM shop_stock_requests r
                JOIN shops rq ON rq.id = r.requesting_shop_id
                LEFT JOIN shops ss ON ss.id = r.source_shop_id
                JOIN items i ON i.id = r.item_id
                WHERE {where_sql}
                ORDER BY r.created_at DESC, r.id DESC
                LIMIT %s OFFSET %s
            """
            params.extend([limit, offset])
            cur.execute(sql, tuple(params))
            return cur.fetchall() or []
    except pymysql.Error:
        return []


def list_stock_request_events_export(*, request_id: Optional[int] = None, limit: int = 5000):
    """Append-only event rows for CSV export."""
    if not table_exists("shop_stock_request_events"):
        return []
    limit = max(1, min(int(limit), 20000))
    try:
        with get_cursor() as cur:
            if request_id:
                cur.execute(
                    """
                    SELECT e.id, e.request_id, e.event_type, e.actor_employee_id, e.actor_shop_id,
                           e.payload_json, e.created_at
                    FROM shop_stock_request_events e
                    WHERE e.request_id = %s
                    ORDER BY e.id ASC
                    LIMIT %s
                    """,
                    (int(request_id), limit),
                )
            else:
                cur.execute(
                    """
                    SELECT e.id, e.request_id, e.event_type, e.actor_employee_id, e.actor_shop_id,
                           e.payload_json, e.created_at
                    FROM shop_stock_request_events e
                    ORDER BY e.id DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
            return cur.fetchall() or []
    except pymysql.Error:
        return []


_EXPECTED_SCHEMA_TABLES = (
    "contact_messages",
    "employees",
    "employee_password_resets",
    "employee_shop_access",
    "employee_payroll",
    "employee_payroll_advances",
    "site_settings",
    "items",
    "stock_transactions",
    "shops",
    "shop_items",
    "store_stock_items",
    "store_stock_transactions",
    "shop_kitchen_portions",
    "shop_stock_transactions",
    "shop_printer_settings",
    "shop_print_agent_jobs",
    "shop_customers",
    "public_customers",
    "sellers",
    "shop_pos_sales",
    "shop_pos_sale_items",
    "shop_pos_quotations",
    "shop_credit_payments",
    "shop_stock_requests",
    "shop_stock_request_events",
    "app_notifications",
    "pos_held_orders",
    "hr_activity_log",
    "shop_day_openings",
)


def verify_database_schema_integrity() -> bool:
    """After migrations, confirm every expected application table exists."""
    missing = [t for t in _EXPECTED_SCHEMA_TABLES if not table_exists(t)]
    if missing:
        logger.error("Schema verification failed: missing tables: %s", ", ".join(missing))
        return False
    logger.info("Database schema verification passed (%s tables).", len(_EXPECTED_SCHEMA_TABLES))
    return True


def init_schema() -> bool:
    """
    Ensure the MySQL database exists, create missing tables, add missing columns
    (incremental migrations), then verify all expected tables are present.
    """
    if not ensure_database_exists():
        return False
    ok_contact = init_contact_table()
    ok_employees = init_employees_table()
    ok_employee_password_resets = init_employee_password_resets_table()
    ok_employee_payroll = init_employee_payroll_table()
    ok_employee_payroll_advances = init_employee_payroll_advances_table()
    ok_settings = init_site_settings_table()
    ok_items = init_items_table()
    ok_stock = init_stock_transactions_table()
    ok_shops = init_shops_table()
    ok_employee_shop_access = init_employee_shop_access_table()
    ok_shop_items = init_shop_items_table()
    ok_store_stock_items = init_store_stock_items_table()
    ok_store_stock_transactions = init_store_stock_transactions_table()
    ok_shop_kitchen_portions = ensure_shop_kitchen_portions_schema()
    ok_shop_stock = init_shop_stock_transactions_table()
    ok_shop_fifo = ensure_shop_stock_fifo_schema()
    ok_shop_printer = init_shop_printer_settings_table()
    ok_shop_print_agent = init_shop_print_agent_jobs_table()
    ok_shop_customers = init_shop_customers_table()
    ok_public_customers = init_public_customers_table()
    ok_sellers = init_sellers_table()
    ok_shop_pos_sales = init_shop_pos_sales_table()
    ok_shop_pos_sale_items = init_shop_pos_sale_items_table()
    ok_shop_pos_quotations = init_shop_pos_quotations_table()
    ok_credit = ensure_shop_credit_payments_schema()
    ok_shop_stock_requests = init_shop_stock_requests_table()
    ok_shop_stock_request_audit = ensure_shop_stock_request_audit_schema()
    ok_notifications = init_notifications_table()
    ok_pos_held_orders = init_pos_held_orders_table()
    ok_hr_activity_log = init_hr_activity_log_table()
    ok_shop_day_openings = init_shop_day_openings_table()
    steps_ok = (
        ok_contact
        and ok_employees
        and ok_employee_password_resets
        and ok_employee_payroll
        and ok_employee_payroll_advances
        and ok_settings
        and ok_items
        and ok_stock
        and ok_shops
        and ok_employee_shop_access
        and ok_shop_items
        and ok_store_stock_items
        and ok_store_stock_transactions
        and ok_shop_kitchen_portions
        and ok_shop_stock
        and ok_shop_fifo
        and ok_shop_printer
        and ok_shop_print_agent
        and ok_shop_customers
        and ok_public_customers
        and ok_sellers
        and ok_shop_pos_sales
        and ok_shop_pos_sale_items
        and ok_shop_pos_quotations
        and ok_credit
        and ok_shop_stock_requests
        and ok_shop_stock_request_audit
        and ok_notifications
        and ok_pos_held_orders
        and ok_hr_activity_log
        and ok_shop_day_openings
    )
    if not steps_ok:
        logger.warning("Database schema initialization did not complete successfully.")
        return False
    ensure_stock_qty_decimal_schema()
    ensure_shop_stock_fifo_historical_cogs_backfill()
    if not verify_database_schema_integrity():
        return False
    logger.info("Database schema is up to date.")
    return True


def sync_database_schema() -> bool:
    """Apply create/migrate steps and verification; same as init_schema()."""
    return init_schema()


def employee_code_available(code: str) -> bool:
    sql = "SELECT 1 FROM employees WHERE employee_code = %s LIMIT 1"
    try:
        with get_cursor() as cur:
            cur.execute(sql, (code,))
            return cur.fetchone() is None
    except pymysql.Error:
        return False


def email_available(email: str) -> bool:
    sql = "SELECT 1 FROM employees WHERE LOWER(email) = LOWER(%s) LIMIT 1"
    try:
        with get_cursor() as cur:
            cur.execute(sql, (email.strip(),))
            return cur.fetchone() is None
    except pymysql.Error:
        return False


def get_employee_by_code(code: str):
    sql = """
    SELECT id, full_name, email, phone, employee_code, password_hash, status, role, shop_id, profile_image, created_at
    FROM employees WHERE employee_code = %s LIMIT 1
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (code,))
            return cur.fetchone()
    except pymysql.Error:
        return None


def get_active_employee_by_email(email: str):
    sql = """
    SELECT id, full_name, email, phone, employee_code, status
    FROM employees
    WHERE LOWER(email) = LOWER(%s) AND status = 'active'
    LIMIT 1
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, ((email or "").strip(),))
            return cur.fetchone()
    except pymysql.Error:
        return None


def get_active_employee_email_for_code(code: str) -> Optional[str]:
    row = get_employee_by_code((code or "").strip())
    if not row or (row.get("status") or "") != "active":
        return None
    return (row.get("email") or "").strip() or None


def resolve_active_employee_for_password_reset(email: str, employee_code: Optional[str] = None):
    """Match active employee by email; if code is given it must match the same account."""
    em = (email or "").strip()
    if not em or "@" not in em:
        return None
    row = get_active_employee_by_email(em)
    if not row:
        return None
    code = (employee_code or "").strip()
    if code:
        if not re.fullmatch(r"\d{6}", code):
            return None
        if (row.get("employee_code") or "").strip() != code:
            return None
    return row


def create_employee_password_reset(employee_id: int, email: str, code_hash: str, expires_at: datetime) -> bool:
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(
                "UPDATE employee_password_resets SET used_at = NOW() "
                "WHERE employee_id = %s AND used_at IS NULL",
                (int(employee_id),),
            )
            cur.execute(
                """
                INSERT INTO employee_password_resets (employee_id, email, code_hash, expires_at)
                VALUES (%s, %s, %s, %s)
                """,
                (int(employee_id), (email or "").strip(), code_hash, expires_at),
            )
        return True
    except pymysql.Error:
        return False


def recent_employee_password_reset_exists(employee_id: int, within_seconds: int = 60) -> bool:
    sql = """
    SELECT 1 FROM employee_password_resets
    WHERE employee_id = %s AND created_at >= (NOW() - INTERVAL %s SECOND)
    LIMIT 1
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (int(employee_id), int(within_seconds)))
            return cur.fetchone() is not None
    except pymysql.Error:
        return False


def verify_employee_password_reset_code(employee_id: int, code: str) -> Optional[int]:
    from werkzeug.security import check_password_hash

    raw = (code or "").strip()
    if not re.fullmatch(r"\d{6}", raw):
        return None
    sql = """
    SELECT id, code_hash
    FROM employee_password_resets
    WHERE employee_id = %s AND used_at IS NULL AND expires_at > NOW()
    ORDER BY id DESC
    LIMIT 5
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (int(employee_id),))
            rows = cur.fetchall() or []
        for row in rows:
            if check_password_hash(row.get("code_hash") or "", raw):
                return int(row["id"])
        return None
    except pymysql.Error:
        return None


def mark_employee_password_reset_used(reset_id: int) -> bool:
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(
                "UPDATE employee_password_resets SET used_at = NOW() WHERE id = %s",
                (int(reset_id),),
            )
        return True
    except pymysql.Error:
        return False


def update_employee_password_hash(employee_id: int, password_hash: str) -> bool:
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(
                "UPDATE employees SET password_hash = %s WHERE id = %s AND status = 'active'",
                (password_hash, int(employee_id)),
            )
            return cur.rowcount > 0
    except pymysql.Error:
        return False


def get_employee_by_code_for_pos_auth(code: str):
    """Like ``get_employee_by_code`` but includes branch access ids for one-shot POS authorization."""
    sql = """
    SELECT
        e.id,
        e.full_name,
        e.email,
        e.phone,
        e.employee_code,
        e.password_hash,
        e.status,
        e.role,
        e.shop_id,
        e.profile_image,
        e.created_at,
        (
            SELECT GROUP_CONCAT(esa.shop_id ORDER BY esa.shop_id SEPARATOR ',')
            FROM employee_shop_access esa
            WHERE esa.employee_id = e.id
        ) AS shop_access_ids_csv
    FROM employees e
    WHERE e.employee_code = %s
    LIMIT 1
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (code,))
            return cur.fetchone()
    except pymysql.Error:
        return None


def _parse_shop_access_ids_csv(csv: Optional[str]) -> list:
    if not csv:
        return []
    out: list = []
    for part in str(csv).split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(int(part))
        except (TypeError, ValueError):
            continue
    return out


def get_employee_by_id(emp_id: int):
    sql = """
    SELECT
        id, full_name, email, phone, employee_code, status, role, shop_id, profile_image,
        preferred_payment_method, payment_account_holder, payment_bank_or_provider,
        payment_account_number, created_at
    FROM employees WHERE id = %s LIMIT 1
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (emp_id,))
            return cur.fetchone()
    except pymysql.Error:
        return None


def get_employee_accessible_shop_ids(emp_id: int) -> list:
    sql = """
    SELECT shop_id FROM employee_shop_access
    WHERE employee_id = %s
    ORDER BY shop_id ASC
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (int(emp_id),))
            rows = cur.fetchall() or []
            return [int(r["shop_id"]) for r in rows if r.get("shop_id") is not None]
    except pymysql.Error:
        return []


def employee_may_use_shop_branch(
    employee_row: dict,
    branch_shop_id: int,
    *,
    link_mode: Optional[str] = None,
) -> bool:
    """True if employee may authorize POS at ``branch_shop_id`` (respects HR single/multi shop mode)."""
    role_key = (employee_row.get("role") or "employee").lower()
    if role_key in ("super_admin", "it_support", "company_manager"):
        return True
    try:
        sid = int(branch_shop_id)
    except (TypeError, ValueError):
        return False
    try:
        emp_shop_id = (
            int(employee_row["shop_id"])
            if employee_row.get("shop_id") is not None
            else None
        )
    except (TypeError, ValueError):
        emp_shop_id = None
    mode = link_mode if link_mode is not None else get_hr_employee_shop_link_mode()
    if mode != "multi":
        return emp_shop_id == sid
    if "shop_access_ids_csv" in employee_row:
        extra = _parse_shop_access_ids_csv(employee_row.get("shop_access_ids_csv"))
    else:
        try:
            eid = int(employee_row.get("id"))
        except (TypeError, ValueError):
            return False
        extra = get_employee_accessible_shop_ids(eid)
    if extra:
        return sid in extra
    return emp_shop_id == sid


def list_employees_for_pos_auth(limit: int = 5000):
    """Minimal employee rows for POS offline auth cache (avoids payroll/shop label subqueries)."""
    sql = """
    SELECT
        e.id,
        e.full_name,
        e.employee_code,
        e.status,
        e.role,
        e.shop_id,
        (
            SELECT GROUP_CONCAT(esa.shop_id ORDER BY esa.shop_id SEPARATOR ',')
            FROM employee_shop_access esa
            WHERE esa.employee_id = e.id
        ) AS shop_access_ids_csv
    FROM employees e
    WHERE e.status = 'active'
    ORDER BY e.id ASC
    LIMIT %s
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (int(limit),))
            return cur.fetchall() or []
    except pymysql.Error:
        return []


def _normalize_linked_shop_ids(raw_ids) -> list:
    """Unique positive ints, preserving first-seen order (first = primary branch on employee row)."""
    out: list = []
    seen: set = set()
    if not raw_ids:
        return out
    for x in raw_ids:
        try:
            i = int(x)
        except (TypeError, ValueError):
            continue
        if i <= 0 or i in seen:
            continue
        seen.add(i)
        out.append(i)
    return out


def email_taken_by_other(email: str, exclude_id: int) -> bool:
    """True if another employee already uses this email (case-insensitive)."""
    sql = "SELECT 1 FROM employees WHERE LOWER(email) = LOWER(%s) AND id != %s LIMIT 1"
    try:
        with get_cursor() as cur:
            cur.execute(sql, (email.strip(), exclude_id))
            return cur.fetchone() is not None
    except pymysql.Error:
        return True


def normalize_employee_payment_fields(
    preferred_payment_method: Optional[str],
    payment_account_holder: Optional[str],
    payment_bank_or_provider: Optional[str],
    payment_account_number: Optional[str],
) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """Return cleaned payment fields for storage (None = unset/cleared).

    Methods: ``mpesa``, ``bank``, ``cash``. Legacy values ``mobile_money`` /
    ``bank_transfer`` are mapped. For M-Pesa only the phone column is kept;
    for bank, name + bank + account; for cash, all account fields cleared.
    """

    def clip(val: Optional[str], n: int) -> Optional[str]:
        s = (val or "").strip()
        return s[:n] if s else None

    raw = (preferred_payment_method or "").strip().lower()
    legacy = {"mobile_money": "mpesa", "bank_transfer": "bank", "other": ""}
    raw = legacy.get(raw, raw)
    allowed = {"mpesa", "bank", "cash"}
    method = raw if raw in allowed else None

    if not method:
        return None, None, None, None

    ph = clip(payment_account_holder, 200)
    pb = clip(payment_bank_or_provider, 120)
    pn = clip(payment_account_number, 128)

    if method == "cash":
        return method, None, None, None
    if method == "mpesa":
        return method, None, None, pn
    # bank
    return method, ph, pb, pn


def update_employee_payout_details(
    emp_id: int,
    *,
    preferred_payment_method: str,
    payment_account_holder: Optional[str] = None,
    payment_bank_or_provider: Optional[str] = None,
    payment_account_number: Optional[str] = None,
) -> bool:
    """Update payout columns for an active employee. Uses ``normalize_employee_payment_fields``."""
    row = get_employee_by_id(emp_id)
    if not row or (row.get("status") or "") != "active":
        return False
    pm, ph, pb, pn = normalize_employee_payment_fields(
        preferred_payment_method,
        payment_account_holder,
        payment_bank_or_provider,
        payment_account_number,
    )
    if pm is None:
        return False
    sql = """
    UPDATE employees
    SET preferred_payment_method=%s, payment_account_holder=%s,
        payment_bank_or_provider=%s, payment_account_number=%s
    WHERE id=%s AND status='active'
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql, (pm, ph, pb, pn, int(emp_id)))
            return cur.rowcount > 0
    except pymysql.Error:
        return False


def employee_code_taken_by_other(code: str, exclude_id: int) -> bool:
    """True if another employee already uses this login code."""
    sql = "SELECT 1 FROM employees WHERE employee_code = %s AND id != %s LIMIT 1"
    try:
        with get_cursor() as cur:
            cur.execute(sql, ((code or "").strip(), int(exclude_id)))
            return cur.fetchone() is not None
    except pymysql.Error:
        return True


_PROFILE_UNSET = object()


def update_employee_profile(
    emp_id: int,
    full_name: str,
    email: str,
    phone: str,
    profile_image=_PROFILE_UNSET,
):
    """
    Update name, email, phone. If profile_image is not _PROFILE_UNSET, set column
    (pass None to clear the image).
    """
    sets = ["full_name = %s", "email = %s", "phone = %s"]
    params: list = [full_name.strip(), email.strip(), phone.strip()]
    if profile_image is not _PROFILE_UNSET:
        sets.append("profile_image = %s")
        params.append(profile_image)
    params.append(emp_id)
    sql = f"UPDATE employees SET {', '.join(sets)} WHERE id = %s"
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql, tuple(params))
        return True
    except pymysql.Error:
        return False


def create_employee_pending(
    full_name: str,
    email: str,
    phone: str,
    employee_code: str,
    password_hash: str,
    profile_image: Optional[str] = None,
):
    sql = """
    INSERT INTO employees (
        full_name, email, phone, employee_code, password_hash,
        status, role, profile_image
    )
    VALUES (%s, %s, %s, %s, %s, 'pending_approval', 'employee', %s)
    """
    with get_cursor(commit=True) as cur:
        cur.execute(
            sql,
            (full_name, email.strip(), phone, employee_code, password_hash, profile_image),
        )
        return cur.lastrowid


def list_employees(limit: int = 1000):
    sql = """
    SELECT
        e.id,
        e.full_name,
        e.email,
        e.phone,
        e.employee_code,
        e.status,
        e.role,
        e.shop_id,
        s.shop_name,
        (
            SELECT GROUP_CONCAT(
                CONCAT(
                    COALESCE(sn.shop_name, ''),
                    CASE
                        WHEN sn.shop_code IS NOT NULL AND CHAR_LENGTH(TRIM(sn.shop_code)) > 0
                            THEN CONCAT(' (', sn.shop_code, ')')
                        ELSE ''
                    END
                )
                ORDER BY sn.shop_name
                SEPARATOR ', '
            )
            FROM employee_shop_access esa
            INNER JOIN shops sn ON sn.id = esa.shop_id
            WHERE esa.employee_id = e.id
        ) AS shops_access_concat,
        (
            SELECT GROUP_CONCAT(esa.shop_id ORDER BY esa.shop_id SEPARATOR ',')
            FROM employee_shop_access esa
            WHERE esa.employee_id = e.id
        ) AS shop_access_ids_csv,
        e.preferred_payment_method,
        e.payment_account_holder,
        e.payment_bank_or_provider,
        e.payment_account_number,
        e.created_at
    FROM employees e
    LEFT JOIN shops s ON s.id = e.shop_id
    ORDER BY e.created_at DESC
    LIMIT %s
    """
    with get_cursor() as cur:
        cur.execute(sql, (int(limit),))
        return cur.fetchall() or []


def list_employees_payroll_eligible(limit: int = 2000):
    """Active employees only, for payroll registration dropdowns."""
    sql = """
    SELECT
        e.id,
        e.full_name,
        e.email,
        e.phone,
        e.employee_code,
        e.status,
        e.role,
        e.shop_id,
        s.shop_name,
        e.preferred_payment_method,
        e.payment_account_holder,
        e.payment_bank_or_provider,
        e.payment_account_number,
        e.created_at
    FROM employees e
    LEFT JOIN shops s ON s.id = e.shop_id
    WHERE e.status = 'active'
    ORDER BY e.full_name ASC
    LIMIT %s
    """
    with get_cursor() as cur:
        cur.execute(sql, (int(limit),))
        return cur.fetchall() or []


def register_employee_payroll(
    employee_id: int,
    gross_amount,
    pay_frequency: str,
    effective_from,
    notes: Optional[str] = None,
) -> Optional[int]:
    """
    Record a payroll line for an active employee. ``gross_amount`` may be str/Decimal/float.
    ``effective_from`` is a date or 'YYYY-MM-DD' string.
    Returns new row id or None if validation fails.
    """
    allowed_freq = {"monthly", "weekly", "biweekly", "daily"}
    freq = (pay_frequency or "monthly").strip().lower()
    if freq not in allowed_freq:
        return None
    try:
        eid = int(employee_id)
    except (TypeError, ValueError):
        return None
    if eid <= 0:
        return None
    row = get_employee_by_id(eid)
    if not row or (row.get("status") or "") != "active":
        return None
    try:
        amt = Decimal(str(gross_amount).strip().replace(",", ""))
    except (InvalidOperation, AttributeError, TypeError):
        return None
    if amt <= 0:
        return None
    if isinstance(effective_from, datetime):
        ef = effective_from.date()
    elif isinstance(effective_from, date):
        ef = effective_from
    else:
        raw = (str(effective_from) if effective_from is not None else "").strip()[:10]
        if len(raw) < 10:
            return None
        try:
            ef = datetime.strptime(raw, "%Y-%m-%d").date()
        except ValueError:
            return None
    note_clean = (notes or "").strip()
    if len(note_clean) > 500:
        note_clean = note_clean[:500]
    note_clean = note_clean or None
    sql = """
    INSERT INTO employee_payroll (employee_id, gross_amount, pay_frequency, effective_from, notes)
    VALUES (%s, %s, %s, %s, %s)
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql, (eid, amt, freq, ef, note_clean))
            return int(cur.lastrowid)
    except pymysql.Error:
        return None


def list_employee_payroll_recent(limit: int = 50):
    sql = """
    SELECT
        p.id,
        p.employee_id,
        p.gross_amount,
        p.pay_frequency,
        p.effective_from,
        p.notes,
        p.registered_at,
        e.full_name,
        e.employee_code,
        e.role
    FROM employee_payroll p
    INNER JOIN employees e ON e.id = p.employee_id
    ORDER BY p.registered_at DESC
    LIMIT %s
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (int(limit),))
            return cur.fetchall() or []
    except pymysql.Error:
        return []


def _payroll_period_totals_for_normalized_start(
    employee_id: int, pay_frequency: str, period_start_norm: date
) -> dict:
    """Gross, advances, and remaining for a pay period (``period_start`` already normalized)."""
    freq = (pay_frequency or "").strip().lower()
    pe = payroll_period_end(freq, period_start_norm)
    gross = get_employee_payroll_gross_for_period_end(employee_id, pe)
    prior = sum_payroll_advances_for_period(employee_id, freq, period_start_norm)
    gross_d = gross if gross is not None else Decimal("0")
    remaining = gross_d - prior
    return {
        "period_start": period_start_norm.isoformat(),
        "period_end": pe.isoformat(),
        "gross": str(gross_d) if gross is not None else None,
        "advances_recorded": str(prior),
        "remaining_before_new": str(remaining),
        "has_payroll_rate": gross is not None,
    }


def compute_payroll_period_balance(
    employee_id: int, pay_frequency: str, ref: Optional[date] = None
) -> Optional[dict]:
    """
    Current-period net room (gross minus advances booked for this period). Uses ``ref`` (default today)
    to determine which calendar pay window applies. No employee row check—caller supplies a valid id.
    """
    try:
        eid = int(employee_id)
    except (TypeError, ValueError):
        return None
    if eid <= 0:
        return None
    freq = (pay_frequency or "").strip().lower()
    allowed = {"monthly", "weekly", "biweekly", "daily"}
    if freq not in allowed:
        return None
    r = ref or date.today()
    ps = normalize_advance_period_start(freq, r)
    return _payroll_period_totals_for_normalized_start(eid, freq, ps)


def list_company_payroll_overview(
    pay_frequency_filter: Optional[str] = None, limit: int = 2000
):
    """
    Active employees with their latest payroll row and payout columns. Optionally restrict to employees
    whose current (latest) registration matches ``pay_frequency_filter``.
    """
    allowed = {"monthly", "weekly", "biweekly", "daily"}
    freq = (pay_frequency_filter or "").strip().lower()
    freq_clause = ""
    params: list = []
    if freq in allowed:
        freq_clause = " AND p.pay_frequency = %s "
        params.append(freq)
    sql = f"""
    SELECT
        e.id AS employee_id,
        e.full_name,
        e.email,
        e.phone,
        e.employee_code,
        e.role,
        e.shop_id,
        s.shop_name,
        e.preferred_payment_method,
        e.payment_account_holder,
        e.payment_bank_or_provider,
        e.payment_account_number,
        p.id AS payroll_row_id,
        p.gross_amount,
        p.pay_frequency,
        p.effective_from AS payroll_effective_from,
        p.notes AS payroll_notes,
        p.registered_at AS payroll_registered_at
    FROM employees e
    LEFT JOIN shops s ON s.id = e.shop_id
    LEFT JOIN employee_payroll p ON p.id = (
        SELECT p2.id FROM employee_payroll p2
        WHERE p2.employee_id = e.id
        ORDER BY p2.effective_from DESC, p2.id DESC
        LIMIT 1
    )
    WHERE e.status = 'active'
    {freq_clause}
    ORDER BY e.full_name ASC
    LIMIT %s
    """
    params.append(int(limit))
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(params))
            return cur.fetchall() or []
    except pymysql.Error:
        return []


def list_employee_payroll_all_history(
    pay_frequency_filter: Optional[str] = None, limit: int = 400
):
    """All payroll registration rows (including superseded), newest first."""
    allowed = {"monthly", "weekly", "biweekly", "daily"}
    freq = (pay_frequency_filter or "").strip().lower()
    freq_clause = ""
    params: list = []
    if freq in allowed:
        freq_clause = " AND p.pay_frequency = %s "
        params.append(freq)
    sql = f"""
    SELECT
        p.id,
        p.employee_id,
        p.gross_amount,
        p.pay_frequency,
        p.effective_from,
        p.notes,
        p.registered_at,
        e.full_name,
        e.employee_code,
        e.role,
        e.status AS employee_status
    FROM employee_payroll p
    INNER JOIN employees e ON e.id = p.employee_id
    WHERE 1=1
    {freq_clause}
    ORDER BY p.effective_from DESC, p.id DESC
    LIMIT %s
    """
    params.append(int(limit))
    try:
        with get_cursor() as cur:
            cur.execute(sql, tuple(params))
            return cur.fetchall() or []
    except pymysql.Error:
        return []


def payroll_period_end(pay_frequency: str, period_start: date) -> date:
    """Last calendar day included in the pay period starting ``period_start``."""
    freq = (pay_frequency or "").strip().lower()
    if freq == "monthly":
        _, last_day = calendar.monthrange(period_start.year, period_start.month)
        return date(period_start.year, period_start.month, last_day)
    if freq == "weekly":
        return period_start + timedelta(days=6)
    if freq == "biweekly":
        return period_start + timedelta(days=13)
    return period_start


def normalize_advance_period_start(pay_frequency: str, ref: date) -> date:
    """Canonical start date for grouping advances (month start, week Monday, biweek Monday, or same day)."""
    freq = (pay_frequency or "").strip().lower()
    if freq == "monthly":
        return date(ref.year, ref.month, 1)
    if freq == "weekly":
        return ref - timedelta(days=ref.weekday())
    if freq == "biweekly":
        monday = ref - timedelta(days=ref.weekday())
        iy, iw, _ = monday.isocalendar()
        pair_start_week = iw - 1 - ((iw - 1) % 2)
        return date.fromisocalendar(iy, pair_start_week + 1, 1)
    return ref


def get_employee_payroll_gross_for_period_end(employee_id: int, period_end: date) -> Optional[Decimal]:
    """
    Gross from the latest ``employee_payroll`` row for this employee with
    ``effective_from`` on or before ``period_end`` (pay rate in force for that period).
    """
    try:
        eid = int(employee_id)
    except (TypeError, ValueError):
        return None
    if eid <= 0:
        return None
    sql = """
    SELECT gross_amount FROM employee_payroll
    WHERE employee_id = %s AND effective_from <= %s
    ORDER BY effective_from DESC
    LIMIT 1
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (eid, period_end))
            row = cur.fetchone()
            if not row:
                return None
            return Decimal(str(row.get("gross_amount")))
    except pymysql.Error:
        return None


def sum_payroll_advances_for_period(
    employee_id: int, pay_frequency: str, period_start: date
) -> Decimal:
    """Total advances already recorded for this employee, frequency, and period."""
    try:
        eid = int(employee_id)
    except (TypeError, ValueError):
        return Decimal("0")
    if eid <= 0:
        return Decimal("0")
    freq = (pay_frequency or "").strip().lower()
    allowed = {"monthly", "weekly", "biweekly", "daily"}
    if freq not in allowed:
        return Decimal("0")
    sql = """
    SELECT COALESCE(SUM(amount), 0) AS s FROM employee_payroll_advances
    WHERE employee_id = %s AND pay_frequency = %s AND period_start = %s
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (eid, freq, period_start))
            row = cur.fetchone()
            if not row:
                return Decimal("0")
            return Decimal(str(row.get("s") or 0))
    except pymysql.Error:
        return Decimal("0")


def get_payroll_advance_period_summary(
    employee_id: int, pay_frequency: str, period_start: date
) -> Optional[dict]:
    """
    Dict with period_start, period_end, gross (payroll), advances_prior, new_balance room,
    or None if employee invalid.
    """
    row = get_employee_by_id(int(employee_id))
    if not row or (row.get("status") or "") != "active":
        return None
    freq = (pay_frequency or "").strip().lower()
    allowed = {"monthly", "weekly", "biweekly", "daily"}
    if freq not in allowed:
        return None
    if isinstance(period_start, datetime):
        ref = period_start.date()
    elif isinstance(period_start, date):
        ref = period_start
    else:
        return None
    ps = normalize_advance_period_start(freq, ref)
    return _payroll_period_totals_for_normalized_start(int(employee_id), freq, ps)


def register_payroll_advance(
    employee_id: int,
    amount,
    pay_frequency: str,
    period_start,
    notes: Optional[str] = None,
) -> Optional[int]:
    """Record an advance against a pay period. ``period_start`` is a date or YYYY-MM-DD string."""
    allowed_freq = {"monthly", "weekly", "biweekly", "daily"}
    freq = (pay_frequency or "").strip().lower()
    if freq not in allowed_freq:
        return None
    try:
        eid = int(employee_id)
    except (TypeError, ValueError):
        return None
    if eid <= 0:
        return None
    emp = get_employee_by_id(eid)
    if not emp or (emp.get("status") or "") != "active":
        return None
    try:
        amt = Decimal(str(amount).strip().replace(",", ""))
    except (InvalidOperation, AttributeError, TypeError):
        return None
    if amt <= 0:
        return None
    if isinstance(period_start, datetime):
        ref = period_start.date()
    elif isinstance(period_start, date):
        ref = period_start
    else:
        raw = (str(period_start) if period_start is not None else "").strip()[:10]
        if len(raw) < 10:
            return None
        try:
            ref = datetime.strptime(raw, "%Y-%m-%d").date()
        except ValueError:
            return None
    ps = normalize_advance_period_start(freq, ref)
    note_clean = (notes or "").strip()
    if len(note_clean) > 500:
        note_clean = note_clean[:500]
    note_clean = note_clean or None
    sql = """
    INSERT INTO employee_payroll_advances (employee_id, amount, pay_frequency, period_start, notes)
    VALUES (%s, %s, %s, %s, %s)
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql, (eid, amt, freq, ps, note_clean))
            return int(cur.lastrowid)
    except pymysql.Error:
        return None


def list_payroll_advances_recent(limit: int = 50):
    sql = """
    SELECT
        a.id,
        a.employee_id,
        a.amount,
        a.pay_frequency,
        a.period_start,
        a.notes,
        a.registered_at,
        e.full_name,
        e.employee_code
    FROM employee_payroll_advances a
    INNER JOIN employees e ON e.id = a.employee_id
    ORDER BY a.registered_at DESC
    LIMIT %s
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (int(limit),))
            return cur.fetchall() or []
    except pymysql.Error:
        return []


def approve_employee(
    emp_id: int,
    *,
    role: str,
    shop_id: Optional[int],
    linked_shop_ids: Optional[list] = None,
) -> bool:
    role = (role or "").strip().lower()
    allowed = {
        "super_admin",
        "it_support",
        "company_manager",
        "admin",
        "manager",
        "sales",
        "finance",
        "employee",
        "rider",
    }
    if role not in allowed:
        return False
    eid = int(emp_id)
    multi = get_hr_employee_shop_link_mode() == "multi"

    if role in {"super_admin", "it_support", "company_manager"}:
        sql = "UPDATE employees SET status='active', role=%s, shop_id=NULL WHERE id=%s"
        try:
            with get_cursor(commit=True) as cur:
                cur.execute(sql, (role, eid))
                ok = cur.rowcount > 0
                cur.execute("DELETE FROM employee_shop_access WHERE employee_id=%s", (eid,))
            return ok
        except pymysql.Error:
            return False

    if multi:
        access_list = _normalize_linked_shop_ids(
            linked_shop_ids if linked_shop_ids is not None else ([] if shop_id is None else [shop_id])
        )
        if not access_list:
            return False
        primary_shop = int(access_list[0])
    else:
        if shop_id is None:
            return False
        primary_shop = int(shop_id)
        if primary_shop <= 0:
            return False
        access_list = []

    sql = "UPDATE employees SET status='active', role=%s, shop_id=%s WHERE id=%s"
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql, (role, primary_shop, eid))
            if cur.rowcount <= 0:
                return False
            cur.execute("DELETE FROM employee_shop_access WHERE employee_id=%s", (eid,))
            if multi:
                for sid in access_list:
                    cur.execute(
                        """
                        INSERT INTO employee_shop_access (employee_id, shop_id)
                        VALUES (%s, %s)
                        """,
                        (eid, int(sid)),
                    )
        return True
    except pymysql.Error:
        return False


def update_employee_by_it_hr(
    emp_id: int,
    *,
    full_name: str,
    email: str,
    phone: str,
    role: str,
    shop_id: Optional[int],
    employee_code: str,
    linked_shop_ids: Optional[list] = None,
    password_hash: Optional[str] = None,
    preferred_payment_method: Optional[str] = None,
    payment_account_holder: Optional[str] = None,
    payment_bank_or_provider: Optional[str] = None,
    payment_account_number: Optional[str] = None,
) -> bool:
    """
    Update name, email, phone, role, shop, employee login code, optional password,
    and preferred payroll payment details for employees already approved (active or suspended).
    ``employee_code`` must be exactly 6 digits. ``password_hash`` = None keeps the current password.
    """
    row = get_employee_by_id(emp_id)
    if not row or (row.get("status") or "") not in ("active", "suspended"):
        return False

    role = (role or "").strip().lower()
    allowed = {
        "super_admin",
        "it_support",
        "company_manager",
        "admin",
        "manager",
        "sales",
        "finance",
        "employee",
        "rider",
    }
    if role not in allowed:
        return False

    multi = get_hr_employee_shop_link_mode() == "multi"
    eid_int = int(emp_id)
    prev_access_sorted = tuple(sorted(get_employee_accessible_shop_ids(eid_int)))

    junction_for_write: list = []
    if role in {"super_admin", "it_support", "company_manager"}:
        shop_id = None
    elif multi:
        junction_for_write = _normalize_linked_shop_ids(linked_shop_ids)
        if not junction_for_write:
            return False
        shop_id = int(junction_for_write[0])
    else:
        if shop_id is None:
            return False
        shop_id = int(shop_id)
        if shop_id <= 0:
            return False

    new_access_sorted = tuple(sorted(junction_for_write)) if junction_for_write else tuple()
    junction_changed = prev_access_sorted != new_access_sorted

    if email_taken_by_other(email.strip(), int(emp_id)):
        return False

    fn = (full_name or "").strip()
    if not fn:
        return False

    code = (employee_code or "").strip()
    if not re.fullmatch(r"\d{6}", code):
        return False
    prev_code = (row.get("employee_code") or "").strip()
    if code != prev_code and employee_code_taken_by_other(code, int(emp_id)):
        return False

    pm, ph, pb, pn = normalize_employee_payment_fields(
        preferred_payment_method,
        payment_account_holder,
        payment_bank_or_provider,
        payment_account_number,
    )
    sets = [
        "full_name=%s",
        "email=%s",
        "phone=%s",
        "role=%s",
        "shop_id=%s",
        "employee_code=%s",
        "preferred_payment_method=%s",
        "payment_account_holder=%s",
        "payment_bank_or_provider=%s",
        "payment_account_number=%s",
    ]
    params: list = [fn, email.strip(), (phone or "").strip(), role, shop_id, code, pm, ph, pb, pn]
    if password_hash is not None:
        sets.append("password_hash=%s")
        params.append(password_hash)
    params.append(int(emp_id))
    sql = f"""
    UPDATE employees
    SET {", ".join(sets)}
    WHERE id=%s AND status IN ('active','suspended')
    """
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql, tuple(params))
            rows_hit = cur.rowcount > 0
            cur.execute("DELETE FROM employee_shop_access WHERE employee_id=%s", (eid_int,))
            if junction_for_write:
                for sid in junction_for_write:
                    cur.execute(
                        """
                        INSERT INTO employee_shop_access (employee_id, shop_id)
                        VALUES (%s, %s)
                        """,
                        (eid_int, int(sid)),
                    )
        if rows_hit or junction_changed:
            return True
        row_after = get_employee_by_id(eid_int)
        if not row_after or (row_after.get("status") or "") not in ("active", "suspended"):
            return False
        if (row_after.get("full_name") or "").strip() != fn:
            return False
        if (row_after.get("email") or "").strip().lower() != email.strip().lower():
            return False
        if (row_after.get("phone") or "").strip() != (phone or "").strip():
            return False
        if (row_after.get("role") or "").strip().lower() != role:
            return False
        if (row_after.get("employee_code") or "").strip() != code:
            return False
        sid_db = row_after.get("shop_id")
        if shop_id is None:
            if sid_db is not None:
                return False
        else:
            try:
                if int(sid_db or 0) != int(shop_id):
                    return False
            except (TypeError, ValueError):
                return False
        ja = tuple(sorted(get_employee_accessible_shop_ids(eid_int)))
        if ja != new_access_sorted:
            return False
        return True
    except pymysql.Error:
        return False


def set_employee_suspended(emp_id: int, *, suspended: bool) -> bool:
    """Set status to suspended or active. Only for already-approved rows (active/suspended)."""
    row = get_employee_by_id(emp_id)
    if not row or (row.get("status") or "") not in ("active", "suspended"):
        return False
    new_status = "suspended" if suspended else "active"
    sql = (
        "UPDATE employees SET status=%s "
        "WHERE id=%s AND status IN ('active','suspended')"
    )
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql, (new_status, int(emp_id)))
            return cur.rowcount > 0
    except pymysql.Error:
        return False


def delete_employee_if_approved(emp_id: int) -> bool:
    """Hard-delete an employee (active, suspended, or pending approval)."""
    sql = (
        "DELETE FROM employees WHERE id=%s "
        "AND status IN ('active','suspended','pending_approval')"
    )
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql, (int(emp_id),))
            return cur.rowcount > 0
    except pymysql.Error:
        return False

