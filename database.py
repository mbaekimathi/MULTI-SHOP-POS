"""PyMySQL helpers for Richcom Technologies.

All MySQL access uses PyMySQL (`pymysql`). Credentials come from environment variables;
`.env` is loaded from the project root (same folder as this file) so `MYSQL_*` work even
if this module is imported before `app.py` runs.
"""

import logging
import os
import re
from contextlib import contextmanager
from pathlib import Path
from typing import Optional, Tuple

import pymysql
from pymysql.cursors import DictCursor
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

logger = logging.getLogger(__name__)

# Avoid spamming logs when MySQL rejects credentials (e.g. Flask debug reloader / repeated init).
_MYSQL_1045_LOGGED = False

# Set RICHCOM_HOSTED=1 (or true/yes) on the production server so MySQL uses hosted defaults below.
# Local/dev: omit it and keep root / richcom-style settings unless MYSQL_* env vars are set.


def is_hosted_deployment() -> bool:
    """True when app runs on hosted/production (set RICHCOM_HOSTED=1 on the server)."""
    v = (os.getenv("RICHCOM_HOSTED") or "").strip().lower()
    return v in ("1", "true", "yes")


def _safe_database_name(raw: Optional[str]) -> str:
    """Allow only safe MySQL identifier characters; default richcom."""
    name = (raw or "richcom").strip()
    if not name or not re.match(r"^[a-zA-Z0-9_-]+$", name):
        return "richcom"
    return name[:64]


def get_database_name() -> str:
    raw = os.getenv("MYSQL_DATABASE")
    if raw:
        return _safe_database_name(raw)
    if is_hosted_deployment():
        return _safe_database_name("twigabea_pos")
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
    return pymysql.connect(**_config())


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
            'super_admin', 'it_support', 'admin', 'manager',
            'sales', 'finance', 'employee', 'rider'
        ) NOT NULL DEFAULT 'employee',
        shop_id INT NULL,
        profile_image VARCHAR(500) DEFAULT NULL,
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
        logger.info("Table employees is ready.")
        return True
    except pymysql.Error as e:
        logger.warning("Could not init employees: %s", e)
        return False


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
        status, created_by_employee_id
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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


def get_item_by_id(item_id: int):
    sql = """
    SELECT id, category, name, description, price, selling_price, image_path, stock_qty, stock_update_enabled, status, created_at
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
    stock_qty: int,
):
    sql = """
    UPDATE items
    SET category=%s, name=%s, description=%s, price=%s, selling_price=%s, image_path=%s, stock_qty=%s
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
                int(stock_qty),
                int(item_id),
            ),
        )
        return True


def update_item_selling_price_for_shop(shop_id: int, item_id: int, selling_price) -> bool:
    """Update ``items.selling_price`` only when the item is linked to the shop (shop portal)."""
    try:
        sp = float(selling_price)
    except (TypeError, ValueError):
        return False
    if sp < 0:
        return False
    sql = """
    UPDATE items i
    INNER JOIN shop_items si ON si.item_id = i.id AND si.shop_id = %s
    SET i.selling_price = %s
    WHERE i.id = %s AND i.status = 'active'
    """
    with get_cursor(commit=True) as cur:
        cur.execute(sql, (int(shop_id), sp, int(item_id)))
        return cur.rowcount > 0


def toggle_item_status(item_id: int) -> bool:
    sql = """
    UPDATE items
    SET status = IF(status='active','suspended','active')
    WHERE id=%s
    """
    with get_cursor(commit=True) as cur:
        cur.execute(sql, (int(item_id),))
        return cur.rowcount > 0


def toggle_stock_update(item_id: int) -> bool:
    sql = """
    UPDATE items
    SET stock_update_enabled = IF(stock_update_enabled=1,0,1)
    WHERE id=%s
    """
    with get_cursor(commit=True) as cur:
        cur.execute(sql, (int(item_id),))
        return cur.rowcount > 0


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
        stock_out_reason VARCHAR(50) NULL,
        refunded TINYINT(1) NOT NULL DEFAULT 0,
        refund_amount DECIMAL(12,2) NULL,
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
            if not column_exists("stock_transactions", "stock_out_reason"):
                cur.execute("ALTER TABLE stock_transactions ADD COLUMN stock_out_reason VARCHAR(50) NULL AFTER place_brought_from")
            if not column_exists("stock_transactions", "refunded"):
                cur.execute("ALTER TABLE stock_transactions ADD COLUMN refunded TINYINT(1) NOT NULL DEFAULT 0 AFTER stock_out_reason")
            if not column_exists("stock_transactions", "refund_amount"):
                cur.execute("ALTER TABLE stock_transactions ADD COLUMN refund_amount DECIMAL(12,2) NULL AFTER refunded")
        logger.info("Table stock_transactions is ready.")
        return True
    except pymysql.Error as e:
        logger.warning("Could not init stock_transactions: %s", e)
        return False


def list_stock_manage_items(limit: int = 500):
    """Items eligible for stock management: active + stock updates enabled."""
    sql = """
    SELECT id, category, name, stock_qty, stock_update_enabled, status
    FROM items
    WHERE status='active' AND stock_update_enabled=1
    ORDER BY name ASC
    LIMIT %s
    """
    with get_cursor() as cur:
        cur.execute(sql, (int(limit),))
        return cur.fetchall() or []


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
            st.stock_out_reason,
            st.refunded,
            st.refund_amount,
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
            st.stock_out_reason,
            st.refunded,
            st.refund_amount,
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


def create_stock_transaction(
    *,
    item_id: int,
    direction: str,
    qty: int,
    buying_price: Optional[float] = None,
    place_brought_from: Optional[str] = None,
    stock_out_reason: Optional[str] = None,
    refunded: bool = False,
    refund_amount: Optional[float] = None,
    note: Optional[str] = None,
    created_by_employee_id: Optional[int] = None,
) -> bool:
    """
    Create a stock in/out transaction and update item stock atomically.
    Only works if item is active and stock_update_enabled=1.
    """
    if direction not in ("in", "out"):
        return False
    qty = int(qty)
    if qty <= 0:
        return False

    with get_cursor(commit=True) as cur:
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

        before = int(row.get("stock_qty") or 0)
        if direction == "in":
            after = before + qty
        else:
            after = before - qty
            if after < 0:
                return False

        cur.execute("UPDATE items SET stock_qty=%s WHERE id=%s", (after, int(item_id)))
        cur.execute(
            """
            INSERT INTO stock_transactions
                (
                    item_id, direction, qty, stock_before, stock_after,
                    buying_price, place_brought_from, stock_out_reason, refunded, refund_amount,
                    note, created_by_employee_id
                )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                int(item_id),
                direction,
                qty,
                before,
                after,
                buying_price if buying_price is not None else None,
                place_brought_from or None,
                stock_out_reason or None,
                1 if refunded else 0,
                refund_amount if refund_amount is not None else None,
                note or None,
                created_by_employee_id,
            ),
        )
        return True


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
        logger.info("Table shops is ready.")
        return True
    except pymysql.Error as e:
        logger.warning("Could not init shops: %s", e)
        return False


def create_shop(
    *,
    shop_name: str,
    shop_code: str,
    shop_password_hash: str,
    shop_location: str,
    created_by_employee_id: Optional[int] = None,
) -> int:
    sql = """
    INSERT INTO shops
        (shop_name, shop_code, shop_password_hash, shop_location, created_by_employee_id)
    VALUES (%s, %s, %s, %s, %s)
    """
    with get_cursor(commit=True) as cur:
        cur.execute(
            sql,
            (
                shop_name.strip().upper(),
                shop_code.strip().upper(),
                shop_password_hash,
                shop_location.strip().upper(),
                created_by_employee_id,
            ),
        )
        return int(cur.lastrowid)


def shop_code_available(shop_code: str) -> bool:
    sql = "SELECT 1 FROM shops WHERE shop_code = %s LIMIT 1"
    with get_cursor() as cur:
        cur.execute(sql, ((shop_code or "").strip().upper(),))
        return cur.fetchone() is None


def list_shops(limit: int = 500):
    sql = """
    SELECT id, shop_name, shop_code, shop_location, status, default_theme, font_family, primary_color, accent_color, shop_logo,
           printing_settings_json, receipt_settings_json, created_at
    FROM shops
    ORDER BY created_at DESC
    LIMIT %s
    """
    with get_cursor() as cur:
        cur.execute(sql, (int(limit),))
        return cur.fetchall() or []


def get_shop_by_id(shop_id: int):
    sql = """
    SELECT id, shop_name, shop_code, shop_password_hash, shop_location, status, default_theme, font_family, primary_color, accent_color, shop_logo,
           printing_settings_json, receipt_settings_json, created_at
    FROM shops
    WHERE id=%s
    LIMIT 1
    """
    with get_cursor() as cur:
        cur.execute(sql, (int(shop_id),))
        return cur.fetchone()


def get_shop_by_code(shop_code: str):
    sql = """
    SELECT id, shop_name, shop_code, shop_password_hash, shop_location, status, default_theme, font_family, primary_color, accent_color, shop_logo,
           printing_settings_json, receipt_settings_json, created_at
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
) -> bool:
    if default_theme not in ("dark", "light"):
        return False
    allowed_fonts = {"Plus Jakarta Sans", "Inter", "System UI"}
    if font_family not in allowed_fonts:
        return False
    if not re.match(r"^#[0-9a-fA-F]{6}$", (primary_color or "")) or not re.match(
        r"^#[0-9a-fA-F]{6}$", (accent_color or "")
    ):
        return False
    sql = """
    UPDATE shops
    SET default_theme=%s, font_family=%s, primary_color=%s, accent_color=%s,
        printing_settings_json=%s, receipt_settings_json=%s
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
                int(shop_id),
            ),
        )
        # MySQL reports 0 affected rows when values are unchanged; still a successful save.
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
        logger.info("Table shop_items is ready.")
        return True
    except pymysql.Error as e:
        logger.warning("Could not init shop_items: %s", e)
        return False


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


def init_shop_pos_sales_table():
    """Persist POS checkouts for analytics (sale vs credit revenue)."""
    sql = """
    CREATE TABLE IF NOT EXISTS shop_pos_sales (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        shop_id INT NOT NULL,
        sale_type ENUM('sale', 'credit') NOT NULL,
        total_amount DECIMAL(12,2) NOT NULL DEFAULT 0.00,
        credit_paid_amount DECIMAL(12,2) NOT NULL DEFAULT 0.00,
        credit_status ENUM('not_paid', 'partially_paid', 'paid') NULL,
        item_count INT NOT NULL DEFAULT 0,
        customer_name VARCHAR(190) NULL,
        customer_phone VARCHAR(40) NULL,
        employee_id INT NULL,
        employee_code CHAR(6) NULL,
        employee_name VARCHAR(190) NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        KEY idx_shop_pos_sales_shop_created (shop_id, created_at),
        KEY idx_shop_pos_sales_type_created (sale_type, created_at)
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
    except pymysql.Error:
        ok = False
    return ok


def list_shop_credit_customers_with_balance(shop_id: int, limit: int = 2000):
    """List customers with credit balances for a shop (WALK IN included)."""
    ensure_shop_credit_payments_schema()
    sql = """
    SELECT
        COALESCE(NULLIF(customer_name, ''), 'WALK IN') AS customer_name,
        COALESCE(NULLIF(customer_phone, ''), '-') AS customer_phone,
        COUNT(*) AS tx_count,
        COALESCE(SUM(total_amount), 0) AS credit_total,
        COALESCE(SUM(credit_paid_amount), 0) AS paid_total,
        COALESCE(SUM(GREATEST(total_amount - credit_paid_amount, 0)), 0) AS balance
    FROM shop_pos_sales
    WHERE shop_id=%s AND sale_type='credit'
    GROUP BY customer_name, customer_phone
    HAVING balance > 0.0001
    ORDER BY balance DESC, tx_count DESC, customer_name ASC
    LIMIT %s
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (int(shop_id), int(limit)))
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
    n = (customer_name or "").strip() or "WALK IN"
    p = (customer_phone or "").strip() or "-"
    sql = """
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
      AND COALESCE(NULLIF(customer_name, ''), 'WALK IN')=%s
      AND COALESCE(NULLIF(customer_phone, ''), '-')=%s
    ORDER BY created_at ASC, id ASC
    LIMIT %s
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (int(shop_id), n, p, int(limit)))
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
    p = (customer_phone or "").strip() or "-"

    select_sql = """
    SELECT id, total_amount, credit_paid_amount
    FROM shop_pos_sales
    WHERE shop_id=%s
      AND sale_type='credit'
      AND COALESCE(NULLIF(customer_name, ''), 'WALK IN')=%s
      AND COALESCE(NULLIF(customer_phone, ''), '-')=%s
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

            cur.execute(select_sql, (int(shop_id), n, p))
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


def list_all_shops_credit_customers_with_balance(limit: int = 4000):
    """All shops: customers with outstanding credit balances."""
    ensure_shop_credit_payments_schema()
    sql = """
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
    WHERE sps.sale_type='credit'
    GROUP BY sps.shop_id, sh.shop_name, sh.shop_code, customer_name, customer_phone
    HAVING balance > 0.0001
    ORDER BY balance DESC, tx_count DESC, shop_id ASC, customer_name ASC
    LIMIT %s
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (int(limit),))
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
    lines: Optional[list] = None,
) -> Tuple[bool, Optional[str]]:
    s_type = (sale_type or "").strip().lower()
    if s_type not in ("sale", "credit"):
        return False, None
    try:
        amount = float(total_amount)
    except Exception:
        return False, None
    if amount < 0:
        return False, None
    try:
        count = int(item_count)
    except Exception:
        return False, None
    if count < 0:
        count = 0

    sale_sql = """
    INSERT INTO shop_pos_sales
        (shop_id, sale_type, total_amount, item_count, customer_name, customer_phone, employee_id, employee_code, employee_name)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    line_sql = """
    INSERT INTO shop_pos_sale_items
        (sale_id, shop_id, item_id, item_name, qty, unit_price, line_total)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    stock_tx_sql = """
    INSERT INTO shop_stock_transactions
        (shop_id, item_id, direction, source, qty, shop_stock_before, shop_stock_after,
         company_stock_before, company_stock_after,
         reason, refunded, refund_amount, note, created_by_employee_id)
    VALUES (%s,%s,'out','manual',%s,%s,%s,NULL,NULL,'POS',0,NULL,%s,%s)
    """
    try:
        with get_cursor(commit=True) as cur:
            parsed = []
            for ln in (lines or []):
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
                parsed.append(
                    {
                        "name": nm[:200],
                        "qty": qty,
                        "unit_price": unit_price,
                        "line_total": line_total,
                        "item_id": item_id,
                    }
                )

            # When shop stock update is ON for a line item, ensure enough shop stock before recording the sale.
            need = {}
            for p in parsed:
                iid = p["item_id"]
                if iid is None:
                    continue
                need[iid] = need.get(iid, 0) + p["qty"]

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
                before = int(si.get("shop_stock_qty") or 0)
                if before < need[iid]:
                    raise ValueError(
                        "Not enough stock at the shop for one or more items. Adjust quantities or stock."
                    )

            cur.execute(
                sale_sql,
                (
                    int(shop_id),
                    s_type,
                    amount,
                    count,
                    (customer_name or "").strip()[:190] or None,
                    (customer_phone or "").strip()[:40] or None,
                    int(employee_id) if employee_id is not None else None,
                    (employee_code or "").strip()[:6] or None,
                    (employee_name or "").strip()[:190] or None,
                ),
            )
            sale_id = int(cur.lastrowid or 0)
            note_base = "POS %s #%s" % (s_type, sale_id)

            for p in parsed:
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
                    ),
                )

                iid = p["item_id"]
                if iid is None:
                    continue
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
                shop_before = int(si.get("shop_stock_qty") or 0)
                q = int(p["qty"])
                if shop_before < q:
                    raise ValueError(
                        "Not enough stock at the shop for one or more items. Adjust quantities or stock."
                    )
                shop_after = shop_before - q
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
                        q,
                        shop_before,
                        shop_after,
                        note,
                        int(employee_id) if employee_id is not None else None,
                    ),
                )

            return True, None
    except ValueError as e:
        return False, str(e) or "Could not complete sale."
    except pymysql.Error:
        return False, None


def get_shop_revenue_analytics(shop_id: int, analytics_filter: dict):
    """Return revenue aggregates for sale and credit based on selected period."""
    range_where, range_params = _analytics_where_clause(analytics_filter, "")
    where_sql = f"shop_id=%s AND {range_where}"
    params = [int(shop_id)] + list(range_params)
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


def get_it_support_revenue_analytics(analytics_filter: dict):
    """Revenue analytics across all shops for IT support/super admin."""
    range_where, range_params = _analytics_where_clause(analytics_filter, "sps")
    where_sql = f"{range_where}"
    params = list(range_params)
    totals_sql = f"""
    SELECT
        COUNT(*) AS tx_count,
        COALESCE(SUM(CASE WHEN sps.sale_type='sale' THEN sps.total_amount ELSE 0 END), 0) AS sale_amount,
        COALESCE(SUM(CASE WHEN sps.sale_type='credit' THEN sps.total_amount ELSE 0 END), 0) AS credit_amount
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
        COALESCE(SUM(CASE WHEN sps.sale_type IN ('sale','credit') THEN sps.total_amount ELSE 0 END), 0) AS total_amount
    FROM shops s
    LEFT JOIN shop_pos_sales sps ON sps.shop_id = s.id AND ({where_sql})
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
    LIMIT 2000
    """
    out = {
        "tx_count": 0,
        "sale_amount": 0.0,
        "credit_amount": 0.0,
        "total_amount": 0.0,
        "shops": [],
        "daily": [],
        "transactions": [],
    }
    try:
        with get_cursor() as cur:
            cur.execute(totals_sql, tuple(params))
            t = cur.fetchone() or {}
            out["tx_count"] = int(t.get("tx_count") or 0)
            out["sale_amount"] = float(t.get("sale_amount") or 0)
            out["credit_amount"] = float(t.get("credit_amount") or 0)
            out["total_amount"] = out["sale_amount"] + out["credit_amount"]

            cur.execute(by_shop_sql, tuple(params))
            out["shops"] = [
                {
                    "shop_id": r.get("shop_id"),
                    "shop_name": r.get("shop_name") or "Shop",
                    "shop_code": r.get("shop_code") or "",
                    "tx_count": int(r.get("tx_count") or 0),
                    "sale_amount": float(r.get("sale_amount") or 0),
                    "credit_amount": float(r.get("credit_amount") or 0),
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

            cur.execute(transactions_sql, tuple(params))
            out["transactions"] = [
                {
                    "id": int(r.get("id") or 0),
                    "shop_id": r.get("shop_id"),
                    "shop_name": r.get("shop_name") or "Shop",
                    "shop_code": r.get("shop_code") or "",
                    "sale_type": (r.get("sale_type") or "").strip().lower(),
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
    except pymysql.Error:
        return out
    return out


def get_it_support_item_analytics(analytics_filter: dict):
    """Item analytics across all shops for IT support/super admin."""
    range_where, range_params = _analytics_where_clause(analytics_filter, "s")
    where_sql = f"{range_where}"
    params = list(range_params)

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
    top_items_sql = f"""
    SELECT
        si.item_id,
        si.item_name,
        COALESCE(SUM(si.qty), 0) AS qty_sold,
        COALESCE(SUM(si.line_total), 0) AS revenue,
        COUNT(*) AS line_count
    FROM shop_pos_sale_items si
    JOIN shop_pos_sales s ON s.id = si.sale_id
    WHERE {where_sql}
    GROUP BY si.item_id, si.item_name
    ORDER BY qty_sold DESC, revenue DESC
    LIMIT 100
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
    LIMIT 3000
    """

    out = {
        "total_qty": 0,
        "total_revenue": 0.0,
        "line_count": 0,
        "distinct_items": 0,
        "top_items": [],
        "shops": [],
        "lines": [],
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

            cur.execute(by_shop_sql, tuple(params))
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

            cur.execute(lines_sql, tuple(params))
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
    except pymysql.Error:
        return out
    return out


def get_it_support_period_analytics(analytics_filter: dict):
    """Period sales analytics across all shops for IT support/super admin."""
    range_where, range_params = _analytics_where_clause(analytics_filter, "sps")
    where_sql = f"{range_where}"
    params = list(range_params)

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

            cur.execute(shop_sql, tuple(params))
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


def get_it_support_employee_analytics(analytics_filter: dict):
    """Employee sales analytics across all shops for IT support/super admin."""
    range_where, range_params = _analytics_where_clause(analytics_filter, "sps")
    where_sql = f"{range_where}"
    params = list(range_params)

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
    transactions_sql = f"""
    SELECT
        sps.id,
        sps.shop_id,
        sh.shop_name,
        sh.shop_code,
        sps.sale_type,
        sps.total_amount,
        sps.item_count,
        sps.customer_name,
        sps.customer_phone,
        sps.employee_id,
        COALESCE(NULLIF(sps.employee_name, ''), 'Unknown') AS employee_name,
        sps.employee_code,
        sps.created_at
    FROM shop_pos_sales sps
    LEFT JOIN shops sh ON sh.id = sps.shop_id
    WHERE {where_sql}
    ORDER BY sps.created_at DESC, sps.id DESC
    LIMIT 3000
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

            cur.execute(transactions_sql, tuple(params))
            out["transactions"] = [
                {
                    "id": int(r.get("id") or 0),
                    "shop_id": r.get("shop_id"),
                    "shop_name": r.get("shop_name") or "Shop",
                    "shop_code": r.get("shop_code") or "",
                    "sale_type": (r.get("sale_type") or "").strip().lower(),
                    "total_amount": float(r.get("total_amount") or 0),
                    "item_count": int(r.get("item_count") or 0),
                    "customer_name": (r.get("customer_name") or "").strip(),
                    "customer_phone": (r.get("customer_phone") or "").strip(),
                    "employee_id": r.get("employee_id"),
                    "employee_name": (r.get("employee_name") or "").strip(),
                    "employee_code": (r.get("employee_code") or "").strip(),
                    "created_at": r.get("created_at"),
                }
                for r in (cur.fetchall() or [])
            ]
    except pymysql.Error:
        return out
    return out


def get_it_support_sales_analytics(analytics_filter: dict):
    """Sales-only analytics across all shops (excludes credit)."""
    range_where, range_params = _analytics_where_clause(analytics_filter, "sps")
    where_sql = f"sps.sale_type='sale' AND {range_where}"
    params = list(range_params)

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
    transactions_sql = f"""
    SELECT
        sps.id,
        sps.shop_id,
        sh.shop_name,
        sh.shop_code,
        sps.total_amount,
        sps.item_count,
        sps.customer_name,
        sps.customer_phone,
        sps.employee_name,
        sps.employee_code,
        sps.created_at
    FROM shop_pos_sales sps
    LEFT JOIN shops sh ON sh.id = sps.shop_id
    WHERE {where_sql}
    ORDER BY sps.created_at DESC, sps.id DESC
    LIMIT 3000
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

            cur.execute(by_shop_sql, tuple(params))
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

            cur.execute(transactions_sql, tuple(params))
            out["transactions"] = [
                {
                    "id": int(r.get("id") or 0),
                    "shop_id": r.get("shop_id"),
                    "shop_name": r.get("shop_name") or "Shop",
                    "shop_code": r.get("shop_code") or "",
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
    except pymysql.Error:
        return out
    return out


def get_it_support_credit_analytics(analytics_filter: dict):
    """Credit-only analytics across all shops (excludes cash sales)."""
    range_where, range_params = _analytics_where_clause(analytics_filter, "sps")
    where_sql = f"sps.sale_type='credit' AND {range_where}"
    params = list(range_params)

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
    transactions_sql = f"""
    SELECT
        sps.id,
        sps.shop_id,
        sh.shop_name,
        sh.shop_code,
        sps.total_amount,
        sps.item_count,
        sps.customer_name,
        sps.customer_phone,
        sps.employee_name,
        sps.employee_code,
        sps.created_at
    FROM shop_pos_sales sps
    LEFT JOIN shops sh ON sh.id = sps.shop_id
    WHERE {where_sql}
    ORDER BY sps.created_at DESC, sps.id DESC
    LIMIT 3000
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

            cur.execute(by_shop_sql, tuple(params))
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

            cur.execute(transactions_sql, tuple(params))
            out["transactions"] = [
                {
                    "id": int(r.get("id") or 0),
                    "shop_id": r.get("shop_id"),
                    "shop_name": r.get("shop_name") or "Shop",
                    "shop_code": r.get("shop_code") or "",
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
    except pymysql.Error:
        return out
    return out


def get_it_support_customer_analytics(analytics_filter: dict):
    """Customer analytics across all shops within selected period."""
    range_where, range_params = _analytics_where_clause(analytics_filter, "sps")
    where_sql = f"{range_where}"
    params = list(range_params)
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
    LIMIT 2000
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


def get_it_support_customer_transactions(
    customer_name: str,
    customer_phone: str,
    limit: int = 3000,
    analytics_filter: Optional[dict] = None,
    shop_id: Optional[int] = None,
):
    """All transactions for one customer identity (including WALK IN placeholder)."""
    n = (customer_name or "").strip() or "WALK IN"
    p = (customer_phone or "").strip() or "-"
    range_where, range_params = _analytics_where_clause(analytics_filter or {}, "sps")
    shop_scope = " AND sps.shop_id=%s" if shop_id else ""
    sql = """
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
    WHERE COALESCE(NULLIF(sps.customer_name, ''), 'WALK IN')=%s
      AND COALESCE(NULLIF(sps.customer_phone, ''), '-')=%s
      AND {range_where}
      {shop_scope}
    ORDER BY sps.created_at DESC, sps.id DESC
    LIMIT %s
    """.format(range_where=range_where, shop_scope=shop_scope)
    try:
        with get_cursor() as cur:
            args = [n, p, *range_params]
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


def get_it_support_customer_detail_analytics(
    customer_name: str, customer_phone: str, analytics_filter: dict, shop_id: Optional[int] = None
):
    """Detailed analytics for one customer identity with date filters."""
    n = (customer_name or "").strip() or "WALK IN"
    p = (customer_phone or "").strip() or "-"
    range_where, range_params = _analytics_where_clause(analytics_filter, "sps")
    where_sql = (
        "COALESCE(NULLIF(sps.customer_name, ''), 'WALK IN')=%s "
        "AND COALESCE(NULLIF(sps.customer_phone, ''), '-')=%s "
        f"AND {range_where}"
    )
    params = [n, p] + list(range_params)
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


def get_shop_customer_analytics(shop_id: int, analytics_filter: dict):
    """Customer analytics scoped to one shop only."""
    range_where, range_params = _analytics_where_clause(analytics_filter, "sps")
    where_sql = f"sps.shop_id=%s AND {range_where}"
    params = [int(shop_id)] + list(range_params)
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


def get_shop_item_analytics(shop_id: int, analytics_filter: dict):
    """Return item analytics: totals, top items, and peak day/hour sold."""
    range_where, range_params = _analytics_where_clause(analytics_filter, "s")
    where_sql = f"s.shop_id=%s AND {range_where}"
    params = [int(shop_id)] + list(range_params)
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
    top_items_sql = f"""
    SELECT
        si.item_id,
        si.item_name,
        COALESCE(SUM(si.qty), 0) AS qty_sold,
        COALESCE(SUM(si.line_total), 0) AS revenue,
        COUNT(*) AS sale_lines
    FROM shop_pos_sale_items si
    JOIN shop_pos_sales s ON s.id = si.sale_id
    WHERE {where_sql}
    GROUP BY si.item_id, si.item_name
    ORDER BY qty_sold DESC, revenue DESC
    LIMIT 20
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


def get_shop_period_analytics(shop_id: int, analytics_filter: dict):
    """Detailed period analytics: day/hour trends + employee performance."""
    range_where, range_params = _analytics_where_clause(analytics_filter, "")
    where_sql = f"shop_id=%s AND {range_where}"
    params = [int(shop_id)] + list(range_params)
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


def get_shop_sales_analytics(shop_id: int, analytics_filter: dict):
    """Sales-only analytics (excludes credit transactions)."""
    range_where, range_params = _analytics_where_clause(analytics_filter, "")
    where_sql = f"shop_id=%s AND sale_type='sale' AND {range_where}"
    params = [int(shop_id)] + list(range_params)

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


def get_shop_credit_analytics(shop_id: int, analytics_filter: dict):
    """Credit-only analytics (excludes direct sales transactions)."""
    range_where, range_params = _analytics_where_clause(analytics_filter, "")
    where_sql = f"shop_id=%s AND sale_type='credit' AND {range_where}"
    params = [int(shop_id)] + list(range_params)

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


def get_shop_customer_by_phone(shop_id: int, phone: str):
    sql = """
    SELECT id, shop_id, customer_name, phone, created_at, updated_at
    FROM shop_customers
    WHERE shop_id=%s AND phone=%s
    LIMIT 1
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (int(shop_id), (phone or "").strip()))
            return cur.fetchone()
    except pymysql.Error:
        return None


def upsert_shop_customer(shop_id: int, customer_name: str, phone: str) -> bool:
    name = (customer_name or "").strip()
    ph = (phone or "").strip()
    if not name or not ph:
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
        reason VARCHAR(50) NULL,
        refunded TINYINT(1) NOT NULL DEFAULT 0,
        refund_amount DECIMAL(12,2) NULL,
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
            if not column_exists("shop_stock_transactions", "refunded"):
                cur.execute("ALTER TABLE shop_stock_transactions ADD COLUMN refunded TINYINT(1) NOT NULL DEFAULT 0 AFTER reason")
            if not column_exists("shop_stock_transactions", "refund_amount"):
                cur.execute("ALTER TABLE shop_stock_transactions ADD COLUMN refund_amount DECIMAL(12,2) NULL AFTER refunded")
        logger.info("Table shop_stock_transactions is ready.")
        return True
    except pymysql.Error as e:
        logger.warning("Could not init shop_stock_transactions: %s", e)
        return False


def ensure_shop_items_for_shop(shop_id: int):
    """Seed shop_items rows for all active items (displayed=1, stock_update_enabled=1)."""
    sql = """
    INSERT INTO shop_items (shop_id, item_id, shop_stock_qty, stock_update_enabled, displayed)
    SELECT
        %s AS shop_id,
        i.id AS item_id,
        0 AS shop_stock_qty,
        1 AS stock_update_enabled,
        1 AS displayed
    FROM items i
    WHERE i.status='active'
      AND NOT EXISTS (
        SELECT 1 FROM shop_items si
        WHERE si.shop_id=%s AND si.item_id=i.id
      )
    """
    with get_cursor(commit=True) as cur:
        cur.execute(sql, (int(shop_id), int(shop_id)))


def list_shop_items(shop_id: int, limit: int = 500):
    sql = """
    SELECT
        i.id,
        i.category,
        i.name,
        i.description,
        i.price,
        i.selling_price,
        i.image_path,
        si.shop_stock_qty,
        si.stock_update_enabled,
        si.displayed,
        i.status,
        i.created_at
    FROM items i
    JOIN shop_items si ON si.item_id = i.id AND si.shop_id=%s
    WHERE i.status='active'
    ORDER BY i.category ASC, i.name ASC
    LIMIT %s
    """
    with get_cursor() as cur:
        cur.execute(sql, (int(shop_id), int(limit)))
        rows = cur.fetchall() or []
    # Normalize fields used by templates.
    for r in rows:
        r["shop_stock_qty"] = int(r.get("shop_stock_qty") or 0)
        r["stock_update_enabled"] = int(r.get("stock_update_enabled") or 0)
        r["displayed"] = int(r.get("displayed") or 0)
    return rows


def list_shop_pos_items(shop_id: int, limit: int = 2000):
    """
    Items for Shop POS: only active system items that are marked displayed for this shop.
    """
    sql = """
    SELECT
        i.id,
        i.category,
        i.name,
        i.description,
        i.price AS original_selling_price,
        i.selling_price,
        i.image_path,
        si.shop_stock_qty,
        si.displayed
    FROM items i
    JOIN shop_items si ON si.item_id = i.id AND si.shop_id=%s
    WHERE i.status='active' AND si.displayed=1
    ORDER BY i.category ASC, i.name ASC
    LIMIT %s
    """
    with get_cursor() as cur:
        cur.execute(sql, (int(shop_id), int(limit)))
        rows = cur.fetchall() or []
    for r in rows:
        r["shop_stock_qty"] = int(r.get("shop_stock_qty") or 0)
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
        r["original_selling_price"] = orig
    return rows


def toggle_shop_item_displayed(shop_id: int, item_id: int) -> bool:
    sql = """
    UPDATE shop_items
    SET displayed = IF(displayed=1,0,1)
    WHERE shop_id=%s AND item_id=%s
    """
    with get_cursor(commit=True) as cur:
        cur.execute(sql, (int(shop_id), int(item_id)))
        return cur.rowcount > 0


def toggle_shop_item_stock_update_enabled(shop_id: int, item_id: int) -> bool:
    sql = """
    UPDATE shop_items
    SET stock_update_enabled = IF(stock_update_enabled=1,0,1)
    WHERE shop_id=%s AND item_id=%s
    """
    with get_cursor(commit=True) as cur:
        cur.execute(sql, (int(shop_id), int(item_id)))
        return cur.rowcount > 0


def list_shop_stock_manage_items(shop_id: int, limit: int = 500):
    sql = """
    SELECT
        i.id,
        i.category,
        i.name,
        i.image_path,
        i.stock_qty AS company_stock_qty,
        si.shop_stock_qty,
        si.stock_update_enabled,
        si.displayed
    FROM items i
    JOIN shop_items si ON si.item_id=i.id AND si.shop_id=%s
    WHERE i.status='active'
    ORDER BY i.category ASC, i.name ASC
    LIMIT %s
    """
    with get_cursor() as cur:
        cur.execute(sql, (int(shop_id), int(limit)))
        rows = cur.fetchall() or []
    for r in rows:
        r["company_stock_qty"] = int(r.get("company_stock_qty") or 0)
        r["shop_stock_qty"] = int(r.get("shop_stock_qty") or 0)
        r["stock_update_enabled"] = int(r.get("stock_update_enabled") or 0)
        r["displayed"] = int(r.get("displayed") or 0)
    return rows


def list_shop_stock_transactions(shop_id: int, item_id: Optional[int] = None, limit: int = 200):
    if item_id:
        sql = """
        SELECT id, direction, source, qty, shop_stock_before, shop_stock_after, company_stock_before, company_stock_after,
               buying_price, place_brought_from, reason, refunded, refund_amount, note, created_at
        FROM shop_stock_transactions
        WHERE shop_id=%s AND item_id=%s
        ORDER BY created_at DESC
        LIMIT %s
        """
        params = (int(shop_id), int(item_id), int(limit))
    else:
        sql = """
        SELECT id, direction, source, qty, shop_stock_before, shop_stock_after, company_stock_before, company_stock_after,
               buying_price, place_brought_from, reason, refunded, refund_amount, note, created_at
        FROM shop_stock_transactions
        WHERE shop_id=%s
        ORDER BY created_at DESC
        LIMIT %s
        """
        params = (int(shop_id), int(limit))
    with get_cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall() or []


def list_shop_stock_audit_rows(shop_id: int, limit: int = 1000):
    sql = """
    SELECT
        sst.id,
        sst.item_id,
        i.category,
        i.name,
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
        sst.created_at
    FROM shop_stock_transactions sst
    JOIN items i ON i.id = sst.item_id
    WHERE sst.shop_id=%s
    ORDER BY sst.created_at DESC
    LIMIT %s
    """
    with get_cursor() as cur:
        cur.execute(sql, (int(shop_id), int(limit)))
        return cur.fetchall() or []


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


def shop_request_stock_from_company(
    *,
    shop_id: int,
    item_id: int,
    qty: int,
    note: Optional[str] = None,
    created_by_employee_id: Optional[int] = None,
) -> bool:
    qty = int(qty)
    if qty <= 0:
        return False

    with get_cursor(commit=True) as cur:
        # Lock company item row.
        cur.execute(
            "SELECT stock_qty, status FROM items WHERE id=%s FOR UPDATE",
            (int(item_id),),
        )
        item = cur.fetchone()
        if not item or item.get("status") != "active":
            return False
        company_before = int(item.get("stock_qty") or 0)
        if company_before < qty:
            return False

        # Lock shop item row.
        cur.execute(
            "SELECT shop_stock_qty, stock_update_enabled FROM shop_items WHERE shop_id=%s AND item_id=%s FOR UPDATE",
            (int(shop_id), int(item_id)),
        )
        si = cur.fetchone()
        if not si or int(si.get("stock_update_enabled") or 0) != 1:
            return False

        shop_before = int(si.get("shop_stock_qty") or 0)
        shop_after = shop_before + qty
        company_after = company_before - qty

        cur.execute("UPDATE items SET stock_qty=%s WHERE id=%s", (company_after, int(item_id)))
        cur.execute(
            "UPDATE shop_items SET shop_stock_qty=%s WHERE shop_id=%s AND item_id=%s",
            (shop_after, int(shop_id), int(item_id)),
        )
        cur.execute(
            """
            INSERT INTO shop_stock_transactions
                (shop_id, item_id, direction, source, qty, shop_stock_before, shop_stock_after,
                 company_stock_before, company_stock_after, note, created_by_employee_id)
            VALUES (%s,%s,'in','company',%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                int(shop_id),
                int(item_id),
                qty,
                shop_before,
                shop_after,
                company_before,
                company_after,
                note or None,
                created_by_employee_id,
            ),
        )
        return True


def shop_return_stock_to_company(
    *,
    shop_id: int,
    item_id: int,
    qty: int,
    reason: Optional[str] = None,
    refunded: bool = False,
    refund_amount: Optional[float] = None,
    note: Optional[str] = None,
    created_by_employee_id: Optional[int] = None,
) -> bool:
    qty = int(qty)
    if qty <= 0:
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

    with get_cursor(commit=True) as cur:
        # Lock company item row.
        cur.execute(
            "SELECT stock_qty, status FROM items WHERE id=%s FOR UPDATE",
            (int(item_id),),
        )
        item = cur.fetchone()
        if not item or item.get("status") != "active":
            return False
        company_before = int(item.get("stock_qty") or 0)

        # Lock shop item row.
        cur.execute(
            "SELECT shop_stock_qty, stock_update_enabled FROM shop_items WHERE shop_id=%s AND item_id=%s FOR UPDATE",
            (int(shop_id), int(item_id)),
        )
        si = cur.fetchone()
        if not si or int(si.get("stock_update_enabled") or 0) != 1:
            return False

        shop_before = int(si.get("shop_stock_qty") or 0)
        if shop_before < qty:
            return False
        shop_after = shop_before - qty
        company_after = company_before + qty

        cur.execute("UPDATE items SET stock_qty=%s WHERE id=%s", (company_after, int(item_id)))
        cur.execute(
            "UPDATE shop_items SET shop_stock_qty=%s WHERE shop_id=%s AND item_id=%s",
            (shop_after, int(shop_id), int(item_id)),
        )
        cur.execute(
            """
            INSERT INTO shop_stock_transactions
                (shop_id, item_id, direction, source, qty, shop_stock_before, shop_stock_after,
                 company_stock_before, company_stock_after, reason, refunded, refund_amount, note, created_by_employee_id)
            VALUES (%s,%s,'out','company',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                int(shop_id),
                int(item_id),
                qty,
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
        return True


def shop_manual_stock_in(
    *,
    shop_id: int,
    item_id: int,
    qty: int,
    buying_price: float,
    place_brought_from: str,
    note: Optional[str] = None,
    created_by_employee_id: Optional[int] = None,
) -> bool:
    qty = int(qty)
    if qty <= 0:
        return False
    try:
        buying_price = float(buying_price)
        if buying_price < 0:
            return False
    except Exception:
        return False
    place_brought_from = (place_brought_from or "").strip()
    if not place_brought_from:
        return False

    with get_cursor(commit=True) as cur:
        cur.execute(
            "SELECT shop_stock_qty FROM shop_items WHERE shop_id=%s AND item_id=%s FOR UPDATE",
            (int(shop_id), int(item_id)),
        )
        si = cur.fetchone()
        if not si:
            return False
        shop_before = int(si.get("shop_stock_qty") or 0)
        shop_after = shop_before + qty

        cur.execute(
            "UPDATE shop_items SET shop_stock_qty=%s WHERE shop_id=%s AND item_id=%s",
            (shop_after, int(shop_id), int(item_id)),
        )
        cur.execute(
            """
            INSERT INTO shop_stock_transactions
                (shop_id, item_id, direction, source, qty, shop_stock_before, shop_stock_after,
                 buying_price, place_brought_from, note, created_by_employee_id)
            VALUES (%s,%s,'in','manual',%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                int(shop_id),
                int(item_id),
                qty,
                shop_before,
                shop_after,
                buying_price,
                place_brought_from.strip().upper(),
                note or None,
                created_by_employee_id,
            ),
        )
        return True


def shop_manual_stock_out(
    *,
    shop_id: int,
    item_id: int,
    qty: int,
    reason: str,
    refunded: bool,
    refund_amount: Optional[float] = None,
    note: Optional[str] = None,
    created_by_employee_id: Optional[int] = None,
) -> bool:
    qty = int(qty)
    if qty <= 0:
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
            "SELECT shop_stock_qty FROM shop_items WHERE shop_id=%s AND item_id=%s FOR UPDATE",
            (int(shop_id), int(item_id)),
        )
        si = cur.fetchone()
        if not si:
            return False
        shop_before = int(si.get("shop_stock_qty") or 0)
        if shop_before < qty:
            return False
        shop_after = shop_before - qty

        cur.execute(
            "UPDATE shop_items SET shop_stock_qty=%s WHERE shop_id=%s AND item_id=%s",
            (shop_after, int(shop_id), int(item_id)),
        )
        cur.execute(
            """
            INSERT INTO shop_stock_transactions
                (shop_id, item_id, direction, source, qty, shop_stock_before, shop_stock_after,
                 reason, refunded, refund_amount, note, created_by_employee_id)
            VALUES (%s,%s,'out','manual',%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                int(shop_id),
                int(item_id),
                qty,
                shop_before,
                shop_after,
                reason.upper(),
                1 if refunded else 0,
                refund_amount if refund_amount is not None else None,
                note or None,
                created_by_employee_id,
            ),
        )
        return True


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


_EXPECTED_SCHEMA_TABLES = (
    "contact_messages",
    "employees",
    "site_settings",
    "items",
    "stock_transactions",
    "shops",
    "shop_items",
    "shop_stock_transactions",
    "shop_printer_settings",
    "shop_print_agent_jobs",
    "shop_customers",
    "shop_pos_sales",
    "shop_pos_sale_items",
    "shop_credit_payments",
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
    ok_settings = init_site_settings_table()
    ok_items = init_items_table()
    ok_stock = init_stock_transactions_table()
    ok_shops = init_shops_table()
    ok_shop_items = init_shop_items_table()
    ok_shop_stock = init_shop_stock_transactions_table()
    ok_shop_printer = init_shop_printer_settings_table()
    ok_shop_print_agent = init_shop_print_agent_jobs_table()
    ok_shop_customers = init_shop_customers_table()
    ok_shop_pos_sales = init_shop_pos_sales_table()
    ok_shop_pos_sale_items = init_shop_pos_sale_items_table()
    ok_credit = ensure_shop_credit_payments_schema()
    steps_ok = (
        ok_contact
        and ok_employees
        and ok_settings
        and ok_items
        and ok_stock
        and ok_shops
        and ok_shop_items
        and ok_shop_stock
        and ok_shop_printer
        and ok_shop_print_agent
        and ok_shop_customers
        and ok_shop_pos_sales
        and ok_shop_pos_sale_items
        and ok_credit
    )
    if not steps_ok:
        logger.warning("Database schema initialization did not complete successfully.")
        return False
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


def get_employee_by_id(emp_id: int):
    sql = """
    SELECT id, full_name, email, phone, employee_code, status, role, shop_id, profile_image, created_at
    FROM employees WHERE id = %s LIMIT 1
    """
    try:
        with get_cursor() as cur:
            cur.execute(sql, (emp_id,))
            return cur.fetchone()
    except pymysql.Error:
        return None


def email_taken_by_other(email: str, exclude_id: int) -> bool:
    """True if another employee already uses this email (case-insensitive)."""
    sql = "SELECT 1 FROM employees WHERE LOWER(email) = LOWER(%s) AND id != %s LIMIT 1"
    try:
        with get_cursor() as cur:
            cur.execute(sql, (email.strip(), exclude_id))
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
        e.created_at
    FROM employees e
    LEFT JOIN shops s ON s.id = e.shop_id
    ORDER BY e.created_at DESC
    LIMIT %s
    """
    with get_cursor() as cur:
        cur.execute(sql, (int(limit),))
        return cur.fetchall() or []


def approve_employee(emp_id: int, *, role: str, shop_id: Optional[int]) -> bool:
    role = (role or "").strip().lower()
    allowed = {"super_admin", "it_support", "admin", "manager", "sales", "finance", "employee", "rider"}
    if role not in allowed:
        return False
    if role in {"super_admin", "it_support"}:
        shop_id = None
    else:
        if shop_id is None:
            return False
        shop_id = int(shop_id)
        if shop_id <= 0:
            return False

    sql = "UPDATE employees SET status='active', role=%s, shop_id=%s WHERE id=%s"
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql, (role, shop_id, int(emp_id)))
            return cur.rowcount > 0
    except pymysql.Error:
        return False
