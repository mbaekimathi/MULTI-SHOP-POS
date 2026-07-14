"""Auto-detect cPanel / Passenger hosting and apply safe MySQL / URL defaults.

Explicit environment variables always win. This module only fills gaps so a
hosted ``.env`` can stay minimal (usually just ``MYSQL_PASSWORD`` + secret).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional


def _truthy(raw: Optional[str]) -> bool:
    return (raw or "").strip().lower() in {"1", "true", "yes", "on"}


def _falsy(raw: Optional[str]) -> bool:
    return (raw or "").strip().lower() in {"0", "false", "no", "off"}


def cpanel_username() -> str:
    """Best-effort cPanel account name (e.g. ``kwetufar``)."""
    for key in ("CPANEL_USER", "CPANEL_USERNAME", "USER", "LOGNAME"):
        v = (os.getenv(key) or "").strip()
        if v and v not in {"root", "nobody", "www-data", "apache", "nginx", "http"}:
            return v
    home = (os.getenv("HOME") or "").strip()
    if home.startswith("/home/"):
        parts = Path(home).parts
        if len(parts) >= 3 and parts[1] == "home" and parts[2]:
            return parts[2]
    return ""


def _looks_like_windows_dev() -> bool:
    if os.name == "nt":
        return True
    home = (os.getenv("HOME") or "").replace("\\", "/")
    if "/Users/" in home or home.startswith("C:"):
        return True
    return False


def _has_cpanel_markers() -> bool:
    if Path("/usr/local/cpanel").exists():
        return True
    if Path("/var/cpanel").exists():
        return True
    for key in (
        "PASSENGER_BASE_URI",
        "PASSENGER_APP_ENV",
        "PASSENGER_SPAWN_WORK_DIR",
        "CPANEL_USER",
        "CPANEL_USERNAME",
    ):
        if (os.getenv(key) or "").strip():
            return True
    home = (os.getenv("HOME") or "").strip()
    if home.startswith("/home/") and (
        Path(home, "public_html").exists() or Path(home, "etc").exists()
    ):
        return True
    return False


def detect_hosted_deployment() -> bool:
    """True on production hosting; False for local Windows/macOS-style development."""
    raw = os.getenv("RICHCOM_HOSTED")
    if raw is not None and str(raw).strip() != "":
        if _falsy(raw):
            return False
        if _truthy(raw):
            return True
    if _looks_like_windows_dev():
        return False
    return _has_cpanel_markers()


def _project_slug() -> str:
    """Short slug from the app folder name (e.g. KARISMA → karisma)."""
    try:
        name = Path(__file__).resolve().parent.name
    except Exception:
        name = ""
    slug = "".join(ch.lower() if ch.isalnum() else "_" for ch in name).strip("_")
    while "__" in slug:
        slug = slug.replace("__", "_")
    return slug[:32] if slug else "pos"


def suggested_mysql_user(cpanel_user: str = "") -> str:
    user = (cpanel_user or cpanel_username()).strip()
    if not user:
        return "twigabea_pos"
    slug = _project_slug()
    # Prefer account_project when the folder is not a generic name.
    if slug and slug not in {"pos", "app", "public_html", "publichtml", "www", "htdocs"}:
        return f"{user}_{slug}"[:64]
    return f"{user}_pos"[:64]


def apply_hosted_env_defaults(*, env_file_keys: Optional[set] = None) -> dict:
    """Set missing env keys for hosted deployments. Returns what was applied.

    ``env_file_keys`` = keys explicitly set in the server ``.env`` (never overwritten).
    Placeholder values that only came from ``.env.example`` (e.g. root / multi_pos)
    are replaced with cPanel-derived identities.
    """
    applied: dict[str, str] = {}
    if not detect_hosted_deployment():
        return applied

    explicit = env_file_keys if env_file_keys is not None else set()

    def _set(key: str, value: str, *, replace_placeholders: tuple = ()) -> None:
        if key in explicit:
            return
        if not value:
            return
        current = (os.getenv(key) or "").strip()
        if current and current not in replace_placeholders:
            return
        os.environ[key] = value
        applied[key] = value

    _set("RICHCOM_HOSTED", "1")
    _set("MYSQL_HOST", "127.0.0.1", replace_placeholders=("localhost",))
    _set("MYSQL_PORT", "3306")
    _set("SESSION_COOKIE_SECURE", "1", replace_placeholders=("0", "false", "no", "off"))

    cpanel = cpanel_username()
    mysql_identity = suggested_mysql_user(cpanel)
    _set(
        "MYSQL_USER",
        mysql_identity,
        replace_placeholders=("root", "mysql", "admin"),
    )
    _set(
        "MYSQL_DATABASE",
        mysql_identity,
        replace_placeholders=("multi_pos", "richcom", "test", "mysql"),
    )

    _set("TRUST_PROXY_HEADERS", "1", replace_placeholders=("0", "false", "no", "off"))

    return applied
