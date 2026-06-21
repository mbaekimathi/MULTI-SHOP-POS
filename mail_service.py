"""Transactional email for employee signup and HR approval (stdlib SMTP)."""

from __future__ import annotations

import logging
import os
import re
import smtplib
import ssl
import threading
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Callable, Optional

logger = logging.getLogger(__name__)

_mail_executor_lock = threading.Lock()
_mail_executor: Optional[threading.Thread] = None


def _env_bool(key: str, default: bool = False) -> bool:
    raw = (os.getenv(key) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def smtp_config() -> dict:
    """SMTP settings from environment (see .env.example)."""
    port_raw = (os.getenv("SMTP_PORT") or "587").strip()
    try:
        port = int(port_raw)
    except ValueError:
        port = 587
    user = (os.getenv("SMTP_USER") or "").strip()
    password = os.getenv("SMTP_PASSWORD") or ""
    from_addr = (os.getenv("SMTP_FROM") or user or "").strip()
    return {
        "host": (os.getenv("SMTP_HOST") or "").strip(),
        "port": port,
        "user": user,
        "password": password,
        "from_addr": from_addr,
        "use_tls": _env_bool("SMTP_USE_TLS", True),
        "use_ssl": _env_bool("SMTP_USE_SSL", False),
    }


def is_mail_configured() -> bool:
    cfg = smtp_config()
    if not cfg["host"] or not cfg["from_addr"]:
        return False
    if _env_bool("SMTP_ALLOW_UNAUTH", False):
        return True
    return bool(cfg["user"] and cfg["password"])


def send_mail(
    to_address: str,
    subject: str,
    html_body: str,
    text_body: Optional[str] = None,
) -> tuple[bool, Optional[str]]:
    """Send one HTML email. Returns (ok, error_message)."""
    to_addr = (to_address or "").strip()
    if not to_addr or "@" not in to_addr:
        return False, "Invalid recipient email."

    cfg = smtp_config()
    if not cfg["host"]:
        return False, "SMTP_HOST is not configured."
    if not cfg["from_addr"]:
        return False, "SMTP_FROM or SMTP_USER is not configured."

    plain = (text_body or "").strip() or _html_to_plain(html_body)
    msg = MIMEMultipart("alternative")
    msg["Subject"] = (subject or "").strip() or "Notification"
    msg["From"] = cfg["from_addr"]
    msg["To"] = to_addr
    msg.attach(MIMEText(plain, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    try:
        if cfg["use_ssl"]:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(cfg["host"], cfg["port"], context=context, timeout=25) as smtp:
                if cfg["user"]:
                    smtp.login(cfg["user"], cfg["password"])
                smtp.sendmail(cfg["from_addr"], [to_addr], msg.as_string())
        else:
            with smtplib.SMTP(cfg["host"], cfg["port"], timeout=25) as smtp:
                smtp.ehlo()
                if cfg["use_tls"]:
                    context = ssl.create_default_context()
                    smtp.starttls(context=context)
                    smtp.ehlo()
                if cfg["user"]:
                    smtp.login(cfg["user"], cfg["password"])
                smtp.sendmail(cfg["from_addr"], [to_addr], msg.as_string())
        return True, None
    except Exception as exc:
        logger.exception("Failed to send email to %s", to_addr)
        return False, str(exc)


def _html_to_plain(html: str) -> str:
    text = re.sub(r"(?i)<br\s*/?>", "\n", html or "")
    text = re.sub(r"(?i)</p>", "\n\n", text)
    text = re.sub(r"<[^>]+>", "", text)
    return re.sub(r"\n{3,}", "\n\n", text).strip()


def _email_shell(*, company_name: str, title: str, body_html: str, footer_note: str) -> str:
    company = (company_name or "Point of Sale").strip()
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>{title}</title>
</head>
<body style="margin:0;padding:0;background:#f1f5f9;font-family:Segoe UI,system-ui,sans-serif;">
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background:#f1f5f9;padding:24px 12px;">
    <tr><td align="center">
      <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="max-width:520px;background:#ffffff;border-radius:16px;overflow:hidden;border:1px solid #e2e8f0;">
        <tr><td style="padding:28px 24px;background:linear-gradient(135deg,#6d28d9,#7c3aed);color:#fff;">
          <p style="margin:0 0 6px;font-size:11px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;opacity:.9;">{company}</p>
          <h1 style="margin:0;font-size:22px;line-height:1.25;font-weight:800;">{title}</h1>
        </td></tr>
        <tr><td style="padding:24px;color:#334155;font-size:15px;line-height:1.6;">
          {body_html}
        </td></tr>
        <tr><td style="padding:16px 24px 24px;border-top:1px solid #e2e8f0;color:#64748b;font-size:12px;line-height:1.5;">
          {footer_note}
        </td></tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""


def build_signup_pending_email(
    *,
    full_name: str,
    employee_code: str,
    company_name: str,
    login_url: str,
) -> tuple[str, str, str]:
    name = (full_name or "there").strip()
    code = (employee_code or "").strip()
    login = (login_url or "").strip()
    subject = f"Registration received — {company_name or 'Employee portal'}"
    body = f"""
      <p style="margin:0 0 16px;">Hello <strong>{name}</strong>,</p>
      <p style="margin:0 0 16px;">Thank you for registering. Your application was submitted successfully and is <strong>waiting for administrator approval</strong>.</p>
      <p style="margin:0 0 12px;">Your employee login code:</p>
      <p style="margin:0 0 20px;font-size:28px;font-weight:800;letter-spacing:.18em;color:#6d28d9;">{code}</p>
      <p style="margin:0 0 20px;">You will receive another email when your account is approved. After that, sign in with your code and password.</p>
      <p style="margin:0;"><a href="{login}" style="display:inline-block;background:#7c3aed;color:#fff;text-decoration:none;font-weight:700;padding:12px 20px;border-radius:10px;">Go to sign in</a></p>
    """
    html = _email_shell(
        company_name=company_name,
        title="Application submitted",
        body_html=body,
        footer_note="You cannot sign in until an administrator activates your account.",
    )
    text = (
        f"Hello {name},\n\n"
        f"Your registration at {company_name} was received and is pending approval.\n"
        f"Your employee code: {code}\n\n"
        f"You will receive another email when approved.\n"
        f"Sign in: {login}\n"
    )
    return subject, html, text


def build_approval_email(
    *,
    full_name: str,
    employee_code: str,
    role_label: str,
    company_name: str,
    login_url: str,
) -> tuple[str, str, str]:
    name = (full_name or "there").strip()
    code = (employee_code or "").strip()
    role = (role_label or "Employee").strip()
    login = (login_url or "").strip()
    subject = f"Account approved — {company_name or 'Employee portal'}"
    body = f"""
      <p style="margin:0 0 16px;">Hello <strong>{name}</strong>,</p>
      <p style="margin:0 0 16px;">Good news — your employee account has been <strong>approved</strong>. You can now sign in to the portal.</p>
      <table role="presentation" cellspacing="0" cellpadding="0" style="margin:0 0 20px;width:100%;">
        <tr><td style="padding:8px 0;color:#64748b;font-size:13px;">Role</td><td style="padding:8px 0;font-weight:700;color:#0f172a;">{role}</td></tr>
        <tr><td style="padding:8px 0;color:#64748b;font-size:13px;">Employee code</td><td style="padding:8px 0;font-weight:800;letter-spacing:.12em;color:#6d28d9;">{code}</td></tr>
      </table>
      <p style="margin:0;"><a href="{login}" style="display:inline-block;background:#059669;color:#fff;text-decoration:none;font-weight:700;padding:12px 20px;border-radius:10px;">Sign in now</a></p>
    """
    html = _email_shell(
        company_name=company_name,
        title="You're approved",
        body_html=body,
        footer_note="Use the employee code and password you chose at registration.",
    )
    text = (
        f"Hello {name},\n\n"
        f"Your account at {company_name} has been approved.\n"
        f"Role: {role}\n"
        f"Employee code: {code}\n\n"
        f"Sign in: {login}\n"
    )
    return subject, html, text


def build_password_reset_email(
    *,
    full_name: str,
    verification_code: str,
    company_name: str,
    login_url: str,
) -> tuple[str, str, str]:
    name = (full_name or "there").strip()
    code = (verification_code or "").strip()
    login = (login_url or "").strip()
    subject = f"Password reset code — {company_name or 'Employee portal'}"
    body = f"""
      <p style="margin:0 0 16px;">Hello <strong>{name}</strong>,</p>
      <p style="margin:0 0 16px;">Use this verification code to reset your employee portal password. It expires in <strong>15 minutes</strong>.</p>
      <p style="margin:0 0 20px;font-size:32px;font-weight:800;letter-spacing:.28em;color:#6d28d9;text-align:center;">{code}</p>
      <p style="margin:0 0 20px;">If you did not request this, you can ignore this email. Your password will stay the same.</p>
      <p style="margin:0;"><a href="{login}" style="display:inline-block;background:#7c3aed;color:#fff;text-decoration:none;font-weight:700;padding:12px 20px;border-radius:10px;">Go to sign in</a></p>
    """
    html = _email_shell(
        company_name=company_name,
        title="Reset your password",
        body_html=body,
        footer_note="Never share this code with anyone.",
    )
    text = (
        f"Hello {name},\n\n"
        f"Your password reset verification code: {code}\n"
        f"This code expires in 15 minutes.\n\n"
        f"Sign in: {login}\n"
    )
    return subject, html, text


def _run_in_background(fn: Callable, **kwargs) -> None:
    def worker() -> None:
        try:
            fn(**kwargs)
        except Exception:
            logger.exception("Background email task failed")

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()


def send_signup_pending_email(
    *,
    to_email: str,
    full_name: str,
    employee_code: str,
    company_name: str,
    login_url: str,
) -> tuple[bool, Optional[str]]:
    subject, html, text = build_signup_pending_email(
        full_name=full_name,
        employee_code=employee_code,
        company_name=company_name,
        login_url=login_url,
    )
    return send_mail(to_email, subject, html, text)


def send_approval_email(
    *,
    to_email: str,
    full_name: str,
    employee_code: str,
    role_label: str,
    company_name: str,
    login_url: str,
) -> tuple[bool, Optional[str]]:
    subject, html, text = build_approval_email(
        full_name=full_name,
        employee_code=employee_code,
        role_label=role_label,
        company_name=company_name,
        login_url=login_url,
    )
    return send_mail(to_email, subject, html, text)


def send_password_reset_email(
    *,
    to_email: str,
    full_name: str,
    verification_code: str,
    company_name: str,
    login_url: str,
) -> tuple[bool, Optional[str]]:
    subject, html, text = build_password_reset_email(
        full_name=full_name,
        verification_code=verification_code,
        company_name=company_name,
        login_url=login_url,
    )
    return send_mail(to_email, subject, html, text)


def queue_signup_pending_email(**kwargs) -> None:
    _run_in_background(send_signup_pending_email, **kwargs)


def queue_approval_email(**kwargs) -> None:
    _run_in_background(send_approval_email, **kwargs)


def queue_password_reset_email(**kwargs) -> None:
    _run_in_background(send_password_reset_email, **kwargs)
