"""Safaricom Daraja API helpers (OAuth + Lipa Na M-Pesa STK Push)."""

from __future__ import annotations

import base64
import json
import logging
import os
import re
import time
import urllib.error
import urllib.request
from datetime import datetime
from typing import Any, Dict, Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# Official Daraja 3.0 proxy hosts (developer.safaricom.co.ke dashboard → APIs)
_DARAJA_HOSTS = {
    "sandbox": "https://sandbox.safaricom.co.ke",
    "production": "https://api.safaricom.co.ke",
}

# Paths from Safaricom product list (OAuth, M-PESA EXPRESS, AccountBalance, etc.)
_DARAJA_PATHS = {
    "oauth": "/oauth/v1/generate?grant_type=client_credentials",
    "stk_push": "/mpesa/stkpush/v1/processrequest",
    "stk_query": "/mpesa/stkpushquery/v1/query",
    "account_balance": "/mpesa/accountbalance/v1/query",
    "transaction_status": "/mpesa/transactionstatus/v1/query",
    "b2c": "/mpesa/b2c/v1/paymentrequest",
    "b2b": "/mpesa/b2b/v1/paymentrequest",
    "reversal": "/mpesa/reversal/v1/request",
    "c2b_register_v1": "/mpesa/c2b/v1/registerurl",
    "c2b_register_v2": "/mpesa/c2b/v2/registerurl",
}

STK_QUERY_PENDING_CODE = "500.001.1001"

# Reuse OAuth tokens (~1h lifetime) to avoid hammering Daraja on STK status polls.
_OAUTH_TOKEN_CACHE: Dict[str, Dict[str, Any]] = {}

SANDBOX_SHORTCODE = "174379"
SANDBOX_TEST_MSISDN = "254708374149"
SANDBOX_DEFAULT_PASSKEY = (
    "bfb279f9aa9bdbcf158e97dd1a4d9433029cb0bf8ae345c70896136d5d6ebc2"
)


class DarajaApiError(Exception):
    def __init__(self, message: str, payload: Optional[dict] = None):
        super().__init__(message)
        self.payload = payload or {}


def daraja_settings_ready(settings: dict) -> bool:
    if not settings or not settings.get("daraja_enabled"):
        return False
    if not str(settings.get("daraja_consumer_key") or "").strip():
        return False
    if not str(settings.get("daraja_consumer_secret") or "").strip():
        return False
    if _daraja_environment(settings) == "sandbox":
        return True
    return bool(
        str(settings.get("daraja_passkey") or "").strip()
        and str(settings.get("daraja_shortcode") or "").strip()
    )


def normalize_msisdn(phone: str) -> str:
    digits = re.sub(r"\D", "", phone or "")
    if not digits:
        return ""
    if digits.startswith("254"):
        return digits
    if digits.startswith("0") and len(digits) >= 10:
        return "254" + digits[1:]
    if len(digits) == 9 and digits[0] in "17":
        return "254" + digits
    return digits


def extract_mpesa_payer_from_stk_metadata(
    metadata: Optional[dict], fallback_phone: str = ""
) -> tuple[str, str]:
    """Best-effort payer name and phone from STK CallbackMetadata (or merged status dict)."""
    meta = metadata if isinstance(metadata, dict) else {}
    phone_raw = str(
        meta.get("PhoneNumber")
        or meta.get("phone")
        or meta.get("MSISDN")
        or fallback_phone
        or ""
    ).strip()
    phone = normalize_msisdn(phone_raw) or phone_raw

    name_parts: list[str] = []
    for key in ("FirstName", "MiddleName", "LastName"):
        part = str(meta.get(key) or "").strip()
        if part:
            name_parts.append(part)
    name = " ".join(name_parts).strip()
    if not name:
        for key in ("CustomerName", "PayerName", "Name", "BillRefNumber"):
            candidate = str(meta.get(key) or "").strip()
            if len(candidate) >= 2 and not candidate.isdigit():
                name = candidate
                break
    return name, phone


def stk_timestamp() -> str:
    """Daraja password must use East Africa Time (Nairobi)."""
    try:
        from zoneinfo import ZoneInfo

        now = datetime.now(ZoneInfo("Africa/Nairobi"))
    except Exception:
        now = datetime.now()
    return now.strftime("%Y%m%d%H%M%S")


def stk_password(shortcode: str, passkey: str, timestamp: str) -> str:
    raw = f"{shortcode}{passkey}{timestamp}"
    return base64.b64encode(raw.encode("utf-8")).decode("ascii")


def _daraja_api_url(settings: dict, path_key: str) -> str:
    env = _daraja_environment(settings)
    host = _DARAJA_HOSTS.get(env) or _DARAJA_HOSTS["sandbox"]
    path = _DARAJA_PATHS.get(path_key) or ""
    return f"{host}{path}"


def daraja_api_endpoints(settings: dict) -> dict:
    """Resolved Daraja URLs for the active environment (for UI / logging)."""
    s = settings if isinstance(settings, dict) else {}
    return {
        "environment": _daraja_environment(s),
        "oauth": _daraja_api_url(s, "oauth"),
        "stk_push": _daraja_api_url(s, "stk_push"),
        "stk_query": _daraja_api_url(s, "stk_query"),
        "account_balance": _daraja_api_url(s, "account_balance"),
        "transaction_status": _daraja_api_url(s, "transaction_status"),
        "b2c": _daraja_api_url(s, "b2c"),
        "b2b": _daraja_api_url(s, "b2b"),
        "reversal": _daraja_api_url(s, "reversal"),
        "c2b_register_v1": _daraja_api_url(s, "c2b_register_v1"),
        "c2b_register_v2": _daraja_api_url(s, "c2b_register_v2"),
    }


def _oauth_url(settings: dict) -> str:
    return _daraja_api_url(settings, "oauth")


def _stk_url(settings: dict) -> str:
    return _daraja_api_url(settings, "stk_push")


def _stk_query_url(settings: dict) -> str:
    return _daraja_api_url(settings, "stk_query")


def _flatten_fault(parsed: dict) -> dict:
    """Unwrap Safaricom { fault: { faultstring, detail } } envelopes."""
    if not isinstance(parsed, dict):
        return {}
    fault = parsed.get("fault")
    if isinstance(fault, dict):
        merged = dict(parsed)
        for k, v in fault.items():
            if k not in merged or merged[k] in (None, ""):
                merged[k] = v
        detail = fault.get("detail")
        if isinstance(detail, dict):
            for k, v in detail.items():
                if k not in merged or merged[k] in (None, ""):
                    merged[k] = v
        return merged
    return parsed


def _is_blocked_html_response(raw: str) -> bool:
    low = (raw or "").lower()
    return (
        "incapsula" in low
        or "_incapsula_resource" in low
        or ("request unsuccessful" in low and "incident_id" in low)
    )


def _extract_incapsula_incident_id(raw: str) -> str:
    text = str(raw or "")
    for pattern in (
        r"incident[_\s-]*id[\"'\s:=]+([0-9]+)",
        r"incident_id=(\d+)",
        r"incident id[:\s]+([0-9]+)",
    ):
        match = re.search(pattern, text, re.I)
        if match:
            return match.group(1)
    return ""


def _incapsula_block_message(raw: str = "") -> str:
    incident = _extract_incapsula_incident_id(raw)
    message = (
        "Safaricom's firewall (Incapsula) blocked the Daraja API request from "
        "this network. Wait a few minutes and retry. If it keeps happening, try "
        "another network, disable VPN, or contact Safaricom Daraja support"
    )
    if incident:
        message += f" with incident ID {incident}"
    else:
        message += " with the incident ID from the error"
    return message + "."


def _extract_daraja_error(
    parsed: object,
    err_raw: str,
    exc: Exception,
    *,
    url: str = "",
) -> str:
    if _is_blocked_html_response(err_raw):
        return _incapsula_block_message(err_raw)
    parts = []
    if isinstance(parsed, dict):
        flat = _flatten_fault(parsed)
        for key in (
            "errorMessage",
            "error_description",
            "ResponseDescription",
            "error",
            "faultstring",
        ):
            val = flat.get(key)
            if val and str(val).strip():
                parts.append(str(val).strip())
        code = (
            flat.get("errorCode")
            or flat.get("error_code")
            or flat.get("errorcode")
        )
        if code and str(code).strip():
            parts.append(f"({code})")
    if err_raw and err_raw.strip():
        compact = err_raw.strip()[:500]
        if compact not in " ".join(parts):
            parts.append(compact)
    if not parts:
        url_low = (url or "").lower()
        if isinstance(exc, urllib.error.HTTPError):
            code = exc.code
            if code == 400 and "oauth" in url_low and not (err_raw or "").strip():
                parts.append(
                    "Daraja OAuth failed — consumer key or consumer secret is invalid. "
                    "Copy both from developer.safaricom.co.ke (Sandbox app) into "
                    "Company settings, and set Environment to Sandbox."
                )
            elif code in (400, 401, 403):
                parts.append(
                    f"Daraja API HTTP {code}. Check consumer key, secret, environment, "
                    "and callback URL in Company settings."
                )
            else:
                parts.append(f"Daraja API HTTP {code}.")
        else:
            raw = str(exc).strip()
            if raw.lower().startswith("http error"):
                parts.append(
                    "Daraja API request failed. Check consumer key, secret, and environment "
                    "in Company settings."
                )
            else:
                parts.append(raw or "Daraja API request failed.")
    return _humanize_daraja_error(" ".join(parts), parsed if isinstance(parsed, dict) else {})


def _humanize_daraja_error(message: str, payload: Optional[dict] = None) -> str:
    low = (message or "").lower()
    if "datastore error" in low or (
        "internal server error" in low and "fault" in json.dumps(payload or {}).lower()
    ):
        return (
            "Safaricom Daraja sandbox returned a temporary error (Datastore Error). "
            "Wait 1–2 minutes and try again. If it keeps failing, check: Environment "
            "= Sandbox, shortcode 174379, Lipa Na M-Pesa passkey from the same Sandbox "
            f"app, customer phone {SANDBOX_TEST_MSISDN} (0708374149), and matching "
            "consumer key/secret. Wrong passkey or production credentials often cause this."
        )
    if "invalid access token" in low or "invalid authentication" in low:
        return (
            "Daraja authentication failed — consumer key/secret may not match the "
            "selected environment (sandbox vs production)."
        )
    if "invalid callbackurl" in low or "400.002.02" in low:
        return (
            "Safaricom rejected the STK callback URL (400.002.02). It must be a "
            "public HTTPS URL — not localhost or 127.0.0.1. On localhost, set "
            "Company settings → STK callback URL — hosted to your live server "
            "(payment status is then checked directly with Safaricom), or use ngrok "
            "in the local dev callback field."
        )
    if "incapsula" in low or "_incapsula" in low:
        return _incapsula_block_message()
    if "1037" in low or "ds timeout" in low or "cannot be reached" in low:
        return (
            "M-Pesa could not reach the phone (DS timeout). The customer did not get "
            "or complete the prompt in time. Check: Safaricom number, phone on with "
            "signal, SIM updated (*234*1*6#). Sandbox testing: use 0708374149 only. "
            "Then tap Try STK Push again."
        )
    return message


def humanize_stk_result(
    result_code: object, result_desc: str = "", *, environment: str = ""
) -> str:
    """Plain-language STK Push result for POS staff."""
    desc = str(result_desc or "").strip()
    low = desc.lower()
    try:
        code = int(result_code)
    except (TypeError, ValueError):
        code = None

    if code == 0:
        return desc or "Payment completed."
    if code == 1032 or "cancelled by user" in low:
        return "Customer cancelled the M-Pesa prompt. You can send STK Push again."
    if code == 1037 or "ds timeout" in low or "cannot be reached" in low:
        env = str(environment or "").strip().lower()
        sandbox_hint = (
            f" Sandbox: customer phone must be {SANDBOX_TEST_MSISDN} (0708374149)."
            if env == "sandbox"
            else ""
        )
        return (
            "M-Pesa could not reach the phone in time (DS timeout). "
            "Phone off, no signal, wrong network, or prompt not opened."
            + sandbox_hint
            + " Try STK Push again."
        )
    if code == 1 or "insufficient" in low:
        return "Insufficient M-Pesa balance."
    if code == 1001 or "transaction in progress" in low:
        return "Customer has another M-Pesa transaction in progress. Wait and retry."
    if code == 1025:
        return "Safaricom could not send the prompt. Check the phone number and retry."
    if desc:
        return _humanize_daraja_error(desc)
    if code is not None:
        return f"M-Pesa payment failed (code {code})."
    return "M-Pesa payment failed."


def _is_incapsula_block(exc: DarajaApiError) -> bool:
    payload = exc.payload or {}
    if payload.get("blocked"):
        return True
    return "incapsula" in str(exc).lower()


def _is_transient_daraja_fault(exc: DarajaApiError) -> bool:
    msg = str(exc).lower()
    payload = exc.payload or {}
    flat = _flatten_fault(payload) if isinstance(payload, dict) else {}
    fault = str(flat.get("faultstring") or "").lower()
    return (
        "datastore error" in msg
        or "datastore error" in fault
        or ("internal server error" in msg and "datastore" in fault)
        or _is_incapsula_block(exc)
    )


def _daraja_request_with_retries(
    method: str,
    url: str,
    headers: Dict[str, str],
    body: Optional[dict] = None,
    *,
    retries: int = 4,
) -> dict:
    last_exc: Optional[DarajaApiError] = None
    for attempt in range(max(1, retries)):
        try:
            return _http_json(method, url, headers, body)
        except DarajaApiError as exc:
            last_exc = exc
            if attempt + 1 >= retries or not _is_transient_daraja_fault(exc):
                raise
            wait_s = min(12.0, 2.0 * (attempt + 1))
            logger.info(
                "Daraja transient error (%s), retrying (%s/%s) in %.1fs",
                "Incapsula" if _is_incapsula_block(exc) else "fault",
                attempt + 1,
                retries,
                wait_s,
            )
            time.sleep(wait_s)
    if last_exc:
        raise last_exc
    raise DarajaApiError("Daraja API request failed.")


def _stk_push_request(
    settings: dict,
    headers: Dict[str, str],
    body: dict,
    *,
    retries: int = 4,
) -> dict:
    return _daraja_request_with_retries(
        "POST",
        _stk_url(settings),
        headers,
        body,
        retries=retries,
    )


def _http_json(
    method: str,
    url: str,
    headers: Dict[str, str],
    body: Optional[dict] = None,
    timeout: int = 45,
) -> dict:
    payload = None
    if body is not None:
        payload = json.dumps(body).encode("utf-8")
    merged_headers = {
        "User-Agent": "RichcomPOS/1.0 (+https://developer.safaricom.co.ke)",
        "Accept": "application/json",
        **(headers or {}),
    }
    if body is not None and "Content-Type" not in merged_headers:
        merged_headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=payload, headers=merged_headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            if not raw:
                return {}
            if _is_blocked_html_response(raw):
                message = _incapsula_block_message(raw)
                logger.warning("Daraja blocked by Incapsula/WAF on %s", url)
                raise DarajaApiError(message, {"blocked": True, "html": raw[:500]})
            try:
                data = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise DarajaApiError(
                    "Daraja API returned an unexpected response (not JSON). "
                    "Check network/VPN or retry in a few minutes.",
                    {"raw": raw[:500]},
                ) from exc
            return data if isinstance(data, dict) else {}
    except urllib.error.HTTPError as exc:
        err_raw = exc.read().decode("utf-8", errors="replace")
        if _is_blocked_html_response(err_raw):
            message = _incapsula_block_message(err_raw)
            logger.warning("Daraja blocked by Incapsula/WAF HTTP %s %s", exc.code, url)
            raise DarajaApiError(message, {"blocked": True, "html": err_raw[:500]}) from exc
        try:
            parsed = json.loads(err_raw) if err_raw else {}
        except json.JSONDecodeError:
            parsed = {"error": err_raw or str(exc)}
        message = _extract_daraja_error(parsed, err_raw, exc, url=url)
        logger.warning("Daraja HTTP %s %s: %s", exc.code, url, message)
        raise DarajaApiError(message, parsed if isinstance(parsed, dict) else {}) from exc
    except urllib.error.URLError as exc:
        raise DarajaApiError(f"Could not reach Safaricom Daraja API: {exc}") from exc


def _daraja_environment(settings: dict) -> str:
    env = str(settings.get("daraja_environment") or "sandbox").strip().lower()
    return env if env in ("sandbox", "production") else "sandbox"


def _callback_hostname(url: str) -> str:
    try:
        return (urlparse(url).hostname or "").strip().lower()
    except Exception:
        return ""


def _is_local_callback_host(url: str) -> bool:
    host = _callback_hostname(url)
    if not host:
        return False
    if host in ("localhost", "127.0.0.1", "::1", "0.0.0.0"):
        return True
    if host.endswith(".local"):
        return True
    if re.match(r"^192\.168\.\d{1,3}\.\d{1,3}$", host):
        return True
    if re.match(r"^10\.\d{1,3}\.\d{1,3}\.\d{1,3}$", host):
        return True
    if re.match(r"^172\.(1[6-9]|2\d|3[0-1])\.\d{1,3}\.\d{1,3}$", host):
        return True
    return False


def _is_tunnel_public_host(host: str) -> bool:
    """Hosts commonly used for dev tunnels (ngrok, etc.) — always HTTPS for Daraja."""
    h = (host or "").strip().lower()
    if not h:
        return False
    markers = (
        "ngrok",
        "ngrok-free.app",
        "ngrok.io",
        "loca.lt",
        "localtunnel",
        "trycloudflare.com",
        "serveo.net",
    )
    return any(m in h for m in markers)


def _normalize_callback_https(url: str) -> str:
    """Safaricom requires HTTPS; ngrok often reaches Flask as http:// behind the proxy."""
    u = (url or "").strip().rstrip("/")
    if not u or u.lower().startswith("https://"):
        return u
    if not u.lower().startswith("http://"):
        return u
    if _is_local_callback_host(u):
        return u
    host = _callback_hostname(u)
    if _is_tunnel_public_host(host) or host:
        return "https://" + u[7:]
    return u


def _is_placeholder_callback_url(url: str) -> bool:
    low = (url or "").strip().lower()
    if not low:
        return False
    host = _callback_hostname(url)
    if host in ("your-domain.com", "example.com", "example.org", "localhost"):
        return True
    markers = (
        "your-domain",
        "your-ngrok",
        "your-subdomain",
        "ngrok-url",
        "placeholder",
        "replace-me",
    )
    return any(marker in low for marker in markers)


def _try_local_ngrok_callback_url() -> str:
    """Use ngrok agent API when tunnel is running on this machine (localhost dev)."""
    try:
        req = urllib.request.Request(
            "http://127.0.0.1:4040/api/tunnels",
            headers={"Accept": "application/json"},
            method="GET",
        )
        with urllib.request.urlopen(req, timeout=2) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
        data = json.loads(raw) if raw else {}
        for tunnel in data.get("tunnels") or []:
            if not isinstance(tunnel, dict):
                continue
            public = str(tunnel.get("public_url") or "").strip().rstrip("/")
            if public.lower().startswith("https://"):
                return f"{public}/api/daraja/mpesa-callback"
    except Exception:
        pass
    return ""


def resolve_callback_url(settings: dict, request_url: str = "") -> str:
    """
    Pick STK callback URL for local dev vs hosted deployment.

    Safaricom rejects localhost/LAN URLs (400.002.02). For local dev without a
    tunnel, set daraja_callback_url to your live HTTPS server; the app polls
    Safaricom's STK Query API for payment status. Use daraja_callback_url_local
    only when you need callbacks (e.g. M-Pesa receipt ref) on this machine.

    Priority:
      1. DARAJA_CALLBACK_URL env (override)
      2. Local request → daraja_callback_url_local, else daraja_callback_url
      3. Hosted request → daraja_callback_url, else public HTTPS request URL
      4. Fallback daraja_callback_url_local when nothing else applies
    """
    env_override = (os.getenv("DARAJA_CALLBACK_URL") or "").strip().rstrip("/")
    if env_override:
        return _normalize_callback_https(env_override)

    hosted = str(settings.get("daraja_callback_url") or "").strip().rstrip("/")
    local = str(settings.get("daraja_callback_url_local") or "").strip().rstrip("/")
    local_env = (os.getenv("DARAJA_CALLBACK_URL_LOCAL") or "").strip().rstrip("/")
    if local_env:
        local = local_env
    if _is_placeholder_callback_url(local):
        local = ""
    if _is_placeholder_callback_url(hosted):
        hosted = ""
    public_base = str(settings.get("public_app_url") or "").strip().rstrip("/")
    if public_base and not public_base.lower().startswith(("http://", "https://")):
        public_base = "https://" + public_base.lstrip("/")
    if not hosted and public_base:
        hosted = f"{public_base.rstrip('/')}/api/daraja/mpesa-callback"

    req = str(request_url or "").strip().rstrip("/")
    is_local_req = _is_local_callback_host(req) if req else False
    req_is_public = bool(req and not _is_local_callback_host(req))
    auto_ngrok = _try_local_ngrok_callback_url()

    if is_local_req:
        # Prefer hosted HTTPS even on localhost — STK status is polled via STK Query API.
        # Auto-ngrok often breaks when ngrok free blocks Safaricom (DS timeout / no callback).
        if hosted:
            return _normalize_callback_https(hosted)
        if local:
            return _normalize_callback_https(local)
        if auto_ngrok:
            logger.info("Daraja STK using auto-detected ngrok callback: %s", auto_ngrok)
            return auto_ngrok
        return ""

    if hosted:
        return _normalize_callback_https(hosted)
    if req_is_public:
        if not req.lower().startswith("https://") and auto_ngrok:
            logger.info(
                "Daraja STK using ngrok callback (request URL was HTTP): %s", auto_ngrok
            )
            return auto_ngrok
        return _normalize_callback_https(req)
    if local:
        return _normalize_callback_https(local)
    if auto_ngrok:
        logger.info("Daraja STK fallback to auto-detected ngrok callback: %s", auto_ngrok)
        return auto_ngrok
    return ""


def _callback_base_origin(url: str) -> str:
    """Scheme + host from a full callback URL."""
    u = (url or "").strip()
    if not u:
        return ""
    parsed = urlparse(u)
    if not parsed.netloc:
        return ""
    scheme = (parsed.scheme or "https").strip().lower()
    return f"{scheme}://{parsed.netloc}"


def _is_ngrok_free_host(url: str) -> bool:
    host = _callback_hostname(url).lower()
    return host.endswith(".ngrok-free.app") or host.endswith(".ngrok-free.dev")


def resolve_balance_callback_urls(
    settings: dict, request_url: str = ""
) -> tuple[str, str]:
    """
    Public HTTPS Result/Timeout URLs for Account Balance API.

    Unlike STK (which can poll Safaricom directly), balance *requires* a working
    inbound callback. Prefer the hosted HTTPS URL over auto-detected ngrok,
    because ngrok free often blocks Safaricom's servers.
    """
    env_override = (os.getenv("DARAJA_CALLBACK_URL") or "").strip().rstrip("/")
    hosted = str(settings.get("daraja_callback_url") or "").strip().rstrip("/")
    local = str(settings.get("daraja_callback_url_local") or "").strip().rstrip("/")
    if _is_placeholder_callback_url(hosted):
        hosted = ""
    if _is_placeholder_callback_url(local):
        local = ""
    public_base = str(settings.get("public_app_url") or "").strip().rstrip("/")
    if public_base and not public_base.lower().startswith(("http://", "https://")):
        public_base = "https://" + public_base.lstrip("/")

    bases: list[str] = []
    if env_override:
        bases.append(_callback_base_origin(env_override))
    if hosted:
        bases.append(_callback_base_origin(hosted))
    if public_base:
        bases.append(public_base.rstrip("/"))
    if local:
        bases.append(_callback_base_origin(local))

    auto_ngrok = _try_local_ngrok_callback_url()
    if auto_ngrok:
        bases.append(_callback_base_origin(auto_ngrok))

    req = str(request_url or "").strip().rstrip("/")
    if req and not _is_local_callback_host(req):
        bases.append(_callback_base_origin(req))

    stk_fallback = resolve_callback_url(settings, request_url)
    if stk_fallback:
        bases.append(_callback_base_origin(stk_fallback))

    seen: set[str] = set()
    for base in bases:
        if not base or base in seen:
            continue
        seen.add(base)
        if _is_local_callback_host(base + "/"):
            continue
        return (
            f"{base}/api/daraja/account-balance-result",
            f"{base}/api/daraja/account-balance-timeout",
        )
    return "", ""


def balance_callback_ngrok_warning(result_url: str) -> str:
    """User-facing hint when ngrok free may block Safaricom callbacks."""
    if not _is_ngrok_free_host(result_url or ""):
        return ""
    return (
        "Using ngrok free — Safaricom often cannot reach it. Set "
        "'STK callback URL — hosted' to your live HTTPS server for balance checks."
    )


def validate_callback_url(url: str, settings: Optional[dict] = None) -> None:
    settings = settings or {}
    u = (url or "").strip()
    if not u:
        raise DarajaApiError(
            "STK callback URL is missing. Safaricom cannot use localhost. Set "
            "'STK callback URL — hosted' to your live HTTPS server "
            "(works from localhost without ngrok), or set 'local dev' to an ngrok URL."
        )
    low = u.lower()
    if not low.startswith(("http://", "https://")):
        raise DarajaApiError("Callback URL must start with http:// or https://.")

    env = _daraja_environment(settings)
    is_local = _is_local_callback_host(u)
    host = _callback_hostname(u)

    if _is_placeholder_callback_url(u):
        raise DarajaApiError(
            "STK callback URL is still a placeholder (e.g. your-domain.com or "
            "YOUR-NGROK-URL). Paste your real ngrok HTTPS URL from the ngrok terminal, "
            "ending with /api/daraja/mpesa-callback — or leave local dev blank if ngrok "
            "is running (the app can auto-detect it)."
        )

    if is_local:
        raise DarajaApiError(
            "Safaricom rejects localhost/LAN callback URLs (400.002.02). "
            "Set 'STK callback URL — hosted' to your live HTTPS server, or use "
            "ngrok in 'local dev' for callbacks on this machine."
        )

    if env == "production" and not low.startswith("https://"):
        raise DarajaApiError(
            "Production callback URL must use HTTPS. "
            "Set the hosted callback URL in Company settings."
        )

    if not low.startswith("https://"):
        raise DarajaApiError(
            "Callback URL must use HTTPS. Use an ngrok or hosted public URL."
        )


def _sandbox_passkey(settings: dict) -> str:
    pk = str(settings.get("daraja_passkey") or "").strip()
    if pk:
        return pk
    if _daraja_environment(settings) == "sandbox":
        return SANDBOX_DEFAULT_PASSKEY
    return ""


def _sandbox_shortcode(settings: dict) -> str:
    sc = str(settings.get("daraja_shortcode") or "").strip()
    if sc:
        return sc
    if _daraja_environment(settings) == "sandbox":
        return SANDBOX_SHORTCODE
    return ""


def _oauth_cache_key(settings: dict) -> str:
    key = str(settings.get("daraja_consumer_key") or "").strip()
    return f"{_daraja_environment(settings)}:{key}"


def _clear_oauth_cache(settings: dict) -> None:
    _OAUTH_TOKEN_CACHE.pop(_oauth_cache_key(settings), None)


def get_access_token(settings: dict) -> str:
    key = str(settings.get("daraja_consumer_key") or "").strip()
    secret = str(settings.get("daraja_consumer_secret") or "").strip()
    if not key or not secret:
        raise DarajaApiError("Daraja consumer key and secret are required.")

    cache_key = _oauth_cache_key(settings)
    cached = _OAUTH_TOKEN_CACHE.get(cache_key)
    now = time.time()
    if cached and float(cached.get("expires_at") or 0) > now + 30:
        return str(cached.get("token") or "")

    auth = base64.b64encode(f"{key}:{secret}".encode("utf-8")).decode("ascii")
    try:
        data = _daraja_request_with_retries(
            "GET",
            _oauth_url(settings),
            {"Authorization": f"Basic {auth}"},
            retries=4,
        )
    except DarajaApiError:
        _clear_oauth_cache(settings)
        raise
    token = str(data.get("access_token") or "").strip()
    if not token:
        _clear_oauth_cache(settings)
        raise DarajaApiError(
            "Daraja OAuth failed — check consumer key and secret match your Daraja app.",
            data,
        )
    try:
        expires_in = int(data.get("expires_in") or 3599)
    except (TypeError, ValueError):
        expires_in = 3599
    _OAUTH_TOKEN_CACHE[cache_key] = {
        "token": token,
        "expires_at": now + max(120, expires_in - 120),
    }
    return token


def _stk_result_desc_is_pending(desc: str) -> bool:
    low = str(desc or "").strip().lower()
    if not low:
        return False
    return (
        "under processing" in low
        or "still being processed" in low
        or "still under processing" in low
        or ("still" in low and "process" in low)
    )


def _stk_query_payload_pending(data: dict) -> bool:
    if not isinstance(data, dict):
        return False
    if data.get("pending") is True:
        return True
    rc = data.get("result_code", data.get("ResultCode"))
    if rc is None or str(rc).strip() == "":
        return True
    return _stk_result_desc_is_pending(
        str(data.get("result_desc") or data.get("ResultDesc") or "")
    )


def _stk_query_still_pending(exc: DarajaApiError) -> bool:
    msg = str(exc).lower()
    payload = exc.payload or {}
    flat = _flatten_fault(payload) if isinstance(payload, dict) else {}
    code = str(
        flat.get("errorCode") or flat.get("error_code") or flat.get("errorcode") or ""
    )
    return STK_QUERY_PENDING_CODE in code or STK_QUERY_PENDING_CODE in msg or (
        "still being processed" in msg or "under processing" in msg
    )


def query_stk_push(settings: dict, checkout_request_id: str) -> Dict[str, Any]:
    """
    Poll Safaricom for STK Push result (outbound from your server).

    Use when callbacks cannot reach localhost — no ngrok required for payment
    confirmation. M-Pesa receipt numbers still come from the callback only.
    """
    if not daraja_settings_ready(settings):
        raise DarajaApiError(
            "Daraja API is not fully configured. Set credentials in Company settings."
        )

    cid = str(checkout_request_id or "").strip()
    if not cid:
        raise DarajaApiError("Checkout request ID is required.")

    shortcode = _sandbox_shortcode(settings)
    passkey = _sandbox_passkey(settings)
    if not shortcode:
        raise DarajaApiError("Business short code is required.")
    if not passkey:
        raise DarajaApiError(
            "Lipa Na M-Pesa passkey is required. Copy it from your Daraja app settings."
        )

    timestamp = stk_timestamp()
    token = get_access_token(settings)
    body = {
        "BusinessShortCode": shortcode,
        "Password": stk_password(shortcode, passkey, timestamp),
        "Timestamp": timestamp,
        "CheckoutRequestID": cid,
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    try:
        data = _daraja_request_with_retries(
            "POST",
            _stk_query_url(settings),
            headers,
            body,
            retries=3,
        )
    except DarajaApiError as exc:
        if _stk_query_still_pending(exc):
            return {
                "CheckoutRequestID": cid,
                "pending": True,
                "ResultCode": None,
                "ResultDesc": "Payment is still being processed on the phone.",
            }
        raise

    response_code = str(data.get("ResponseCode") or "").strip()
    if response_code and response_code != "0":
        raise DarajaApiError(
            str(
                data.get("ResponseDescription")
                or data.get("errorMessage")
                or "STK status query was rejected."
            ),
            data,
        )

    result_code = data.get("ResultCode")
    result_desc = str(data.get("ResultDesc") or "").strip()
    if _stk_query_payload_pending(
        {
            "ResultCode": result_code,
            "ResultDesc": result_desc,
        }
    ):
        return {
            **data,
            "CheckoutRequestID": cid,
            "pending": True,
            "ResultDesc": result_desc or "Waiting for customer to complete M-Pesa on phone.",
        }

    return {
        **data,
        "CheckoutRequestID": cid,
        "pending": False,
        "completed": True,
        "result_code": result_code,
        "result_desc": result_desc,
    }


def initiate_stk_push(
    settings: dict,
    *,
    phone: str,
    amount: float,
    callback_url: str,
    account_reference: str = "",
    transaction_desc: str = "POS payment",
) -> Dict[str, Any]:
    if not daraja_settings_ready(settings):
        raise DarajaApiError(
            "Daraja API is not fully configured. Set credentials in Company settings."
        )

    callback_url = resolve_callback_url(settings, callback_url)
    validate_callback_url(callback_url, settings)

    msisdn = normalize_msisdn(phone)
    if len(msisdn) < 12:
        raise DarajaApiError("Enter a valid customer phone number for STK Push.")

    try:
        amount_int = int(round(float(amount)))
    except (TypeError, ValueError):
        raise DarajaApiError("Invalid M-Pesa amount.") from None
    if amount_int < 1:
        raise DarajaApiError("M-Pesa amount must be at least 1.")

    env = _daraja_environment(settings)
    shortcode = _sandbox_shortcode(settings)
    passkey = _sandbox_passkey(settings)
    if not shortcode:
        raise DarajaApiError("Business short code is required.")
    if not passkey:
        raise DarajaApiError(
            "Lipa Na M-Pesa passkey is required. Copy it from your Daraja app settings."
        )

    if env == "sandbox" and msisdn != SANDBOX_TEST_MSISDN:
        logger.info(
            "Daraja sandbox STK using customer phone %s (official test MSISDN is %s)",
            msisdn,
            SANDBOX_TEST_MSISDN,
        )

    tx_type = str(settings.get("daraja_transaction_type") or "CustomerBuyGoodsOnline").strip()
    if tx_type not in ("CustomerBuyGoodsOnline", "CustomerPayBillOnline"):
        tx_type = "CustomerBuyGoodsOnline"

    timestamp = stk_timestamp()
    token = get_access_token(settings)
    body = {
        "BusinessShortCode": shortcode,
        "Password": stk_password(shortcode, passkey, timestamp),
        "Timestamp": timestamp,
        "TransactionType": tx_type,
        "Amount": amount_int,
        "PartyA": msisdn,
        "PartyB": shortcode,
        "PhoneNumber": msisdn,
        "CallBackURL": callback_url,
        "AccountReference": (account_reference or "POS")[:12],
        "TransactionDesc": (transaction_desc or "POS payment")[:13],
    }
    logger.info(
        "Daraja STK push env=%s shortcode=%s phone=%s amount=%s callback=%s",
        env,
        shortcode,
        msisdn,
        amount_int,
        callback_url,
    )
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    data = _stk_push_request(settings, headers, body, retries=4)
    response_code = str(data.get("ResponseCode") or "").strip()
    if response_code and response_code != "0":
        raise DarajaApiError(
            str(
                data.get("ResponseDescription")
                or data.get("errorMessage")
                or "STK Push was rejected."
            ),
            data,
        )
    return data


def daraja_balance_settings_ready(settings: dict) -> bool:
    """True when live M-Pesa account balance can be requested."""
    if not daraja_settings_ready(settings):
        return False
    if not str(settings.get("daraja_initiator_name") or "").strip():
        return False
    if not str(settings.get("daraja_security_credential") or "").strip():
        return False
    return bool(_sandbox_shortcode(settings))


def daraja_account_type_label(settings: dict) -> str:
    tx = str(settings.get("daraja_transaction_type") or "CustomerBuyGoodsOnline").strip()
    if tx == "CustomerPayBillOnline":
        return "Pay Bill"
    return "Till (Buy Goods)"


def _account_balance_url(settings: dict) -> str:
    return _daraja_api_url(settings, "account_balance")


def _transaction_status_url(settings: dict) -> str:
    return _daraja_api_url(settings, "transaction_status")


def initiate_transaction_status_query(
    settings: dict,
    *,
    transaction_id: str,
    result_url: str,
    timeout_url: str,
) -> Dict[str, Any]:
    """Query Daraja TransactionStatus for a prior request (e.g. AccountBalance)."""
    if not daraja_balance_settings_ready(settings):
        raise DarajaApiError(
            "Transaction status needs Daraja short code, initiator, and security credential."
        )
    tx_id = str(transaction_id or "").strip()
    if not tx_id:
        raise DarajaApiError("Transaction / conversation ID is required.")
    result_url = str(result_url or "").strip()
    timeout_url = str(timeout_url or "").strip()
    if not result_url or not timeout_url:
        raise DarajaApiError("Transaction status callback URLs are required.")
    for url in (result_url, timeout_url):
        validate_callback_url(url, settings)

    shortcode = _sandbox_shortcode(settings)
    initiator = str(settings.get("daraja_initiator_name") or "").strip()
    credential = str(settings.get("daraja_security_credential") or "").strip()
    token = get_access_token(settings)
    body = {
        "Initiator": initiator,
        "SecurityCredential": credential,
        "CommandID": "TransactionStatusQuery",
        "PartyA": shortcode,
        "IdentifierType": "4",
        "Remarks": "Company account balance status",
        "QueueTimeOutURL": timeout_url,
        "ResultURL": result_url,
        "TransactionID": tx_id,
        "Occasion": "",
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    data = _daraja_request_with_retries(
        "POST",
        _transaction_status_url(settings),
        headers,
        body,
        retries=2,
    )
    response_code = str(data.get("ResponseCode") or "").strip()
    if response_code and response_code != "0":
        raise DarajaApiError(
            str(
                data.get("ResponseDescription")
                or data.get("errorMessage")
                or "Transaction status request was rejected."
            ),
            data,
        )
    return data


def parse_account_balance_callback(data: dict) -> dict:
    """Extract balance fields from Daraja AccountBalance ResultURL payload."""
    root = data if isinstance(data, dict) else {}
    result = root.get("Result") if isinstance(root.get("Result"), dict) else root
    result_code = result.get("ResultCode")
    result_desc = str(result.get("ResultDesc") or "").strip()
    try:
        code_int = int(result_code)
    except (TypeError, ValueError):
        code_int = -1

    params_raw = (result.get("ResultParameters") or {}).get("ResultParameter")
    params: list = []
    if isinstance(params_raw, list):
        params = [p for p in params_raw if isinstance(p, dict)]
    elif isinstance(params_raw, dict):
        params = [params_raw]

    balance = None
    currency = "KES"
    account_label = ""
    for item in params:
        key = str(item.get("Key") or "").strip()
        val = str(item.get("Value") or "").strip()
        if not key or not val:
            continue
        if key == "Currency":
            currency = val or currency
        if key == "DebitPartyAffectedAccountBalance" and "|" in val:
            parts = [p.strip() for p in val.split("|")]
            if parts:
                account_label = parts[0]
            if len(parts) >= 3:
                try:
                    balance = float(parts[2])
                except (TypeError, ValueError):
                    pass
        if balance is None and key in (
            "DebitAccountBalance",
            "InitiatorAccountCurrentBalance",
            "AccountBalance",
            "UtilityAccountActive",
        ):
            if "|" in val:
                parts = [p.strip() for p in val.split("|")]
                if len(parts) >= 3:
                    try:
                        balance = float(parts[2])
                    except (TypeError, ValueError):
                        pass
            if balance is None:
                match = re.search(r"BasicAmount=([0-9.]+)", val)
                if match:
                    try:
                        balance = float(match.group(1))
                    except (TypeError, ValueError):
                        pass
            if balance is None:
                try:
                    balance = float(val)
                except (TypeError, ValueError):
                    pass

    return {
        "result_code": code_int,
        "result_desc": result_desc,
        "balance": balance,
        "currency": currency,
        "account_label": account_label,
        "conversation_id": str(
            result.get("ConversationID") or root.get("ConversationID") or ""
        ).strip(),
        "originator_conversation_id": str(
            result.get("OriginatorConversationID")
            or root.get("OriginatorConversationID")
            or ""
        ).strip(),
        "completed": True,
        "pending": False,
        "timed_out": False,
    }


def initiate_account_balance_query(
    settings: dict,
    *,
    result_url: str,
    timeout_url: str,
) -> Dict[str, Any]:
    """Request M-Pesa account balance (result arrives asynchronously on ResultURL)."""
    if not daraja_balance_settings_ready(settings):
        raise DarajaApiError(
            "M-Pesa balance lookup needs Daraja enabled with short code, initiator name, "
            "and security credential in Company settings."
        )

    result_url = str(result_url or "").strip()
    timeout_url = str(timeout_url or "").strip()
    if not result_url or not timeout_url:
        raise DarajaApiError(
            "Balance callback URL is missing. Safaricom cannot use localhost. Set "
            "'STK callback URL — hosted' to your live HTTPS server, or 'local dev' to "
            "an ngrok URL (same as STK Push)."
        )
    for label, url in (("Result", result_url), ("Timeout", timeout_url)):
        low = url.lower()
        validate_callback_url(url, settings)
        if _is_placeholder_callback_url(url):
            raise DarajaApiError(
                f"Balance {label} URL is a placeholder. Set a public HTTPS Daraja callback URL "
                "in Company settings."
            )
        if _daraja_environment(settings) == "production" and not low.startswith("https://"):
            raise DarajaApiError(f"Production balance {label} URL must use HTTPS.")

    shortcode = _sandbox_shortcode(settings)
    initiator = str(settings.get("daraja_initiator_name") or "").strip()
    credential = str(settings.get("daraja_security_credential") or "").strip()
    token = get_access_token(settings)
    body = {
        "Initiator": initiator,
        "SecurityCredential": credential,
        "CommandID": "AccountBalance",
        "PartyA": shortcode,
        "IdentifierType": "4",
        "Remarks": "Company account balance",
        "QueueTimeOutURL": timeout_url,
        "ResultURL": result_url,
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    balance_url = _account_balance_url(settings)
    logger.info(
        "Daraja AccountBalance request env=%s url=%s shortcode=%s",
        _daraja_environment(settings),
        balance_url,
        shortcode,
    )
    data = _daraja_request_with_retries(
        "POST",
        balance_url,
        headers,
        body,
        retries=3,
    )
    response_code = str(data.get("ResponseCode") or "").strip()
    if response_code and response_code != "0":
        raise DarajaApiError(
            str(
                data.get("ResponseDescription")
                or data.get("errorMessage")
                or "Account balance request was rejected."
            ),
            data,
        )
    conversation_id = str(data.get("ConversationID") or "").strip()
    if not conversation_id:
        raise DarajaApiError(
            "Daraja did not return a conversation ID for the balance request.",
            data,
        )
    return {
        **data,
        "api_url": balance_url,
        "conversation_id": conversation_id,
        "originator_conversation_id": str(
            data.get("OriginatorConversationID") or ""
        ).strip(),
        "pending": True,
        "completed": False,
    }
