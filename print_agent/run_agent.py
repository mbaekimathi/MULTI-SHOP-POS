#!/usr/bin/env python3
"""
Richcom LAN print agent — polls the hosted app for queued ESC/POS jobs and prints
to a local thermal printer (same Wi‑Fi / LAN).

Environment (required):
  RICHCOM_BASE_URL   — e.g. https://your-domain.com  (no trailing slash)
  RICHCOM_SHOP_ID    — shop id (integer)
  RICHCOM_AGENT_TOKEN — secret from POS → Network → LAN print agent

Environment (printer target on this machine):
  PRINTER_HOST       — default 127.0.0.1 or the thermal printer IP on this LAN
  PRINTER_PORT       — default 9100

Optional:
  POLL_INTERVAL_SEC  — seconds between idle polls (default 2)
"""
from __future__ import annotations

import base64
import json
import os
import socket
import sys
import time
import urllib.error
import urllib.request


def getenv(name: str, default: str | None = None) -> str | None:
    v = (os.environ.get(name) or "").strip()
    return v if v else default


def poll_once(base: str, shop_id: int, token: str) -> list[dict]:
    url = f"{base}/shops/{shop_id}/shop-pos/print-agent/jobs"
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=120) as r:
        data = json.loads(r.read().decode("utf-8"))
    if not data.get("ok"):
        return []
    return list(data.get("jobs") or [])


def ack_job(base: str, shop_id: int, token: str, job_id: int, *, failed: bool, error: str = "") -> None:
    url = f"{base}/shops/{shop_id}/shop-pos/print-agent/jobs/{job_id}/ack"
    body = json.dumps({"failed": failed, "error": error}).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        method="POST",
    )
    urllib.request.urlopen(req, timeout=60).read()


def send_to_printer(host: str, port: int, raw: bytes) -> None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(30.0)
    sock.connect((host, port))
    sock.sendall(raw)
    sock.shutdown(socket.SHUT_WR)
    sock.close()


def main() -> int:
    base = (getenv("RICHCOM_BASE_URL") or "").rstrip("/")
    shop_raw = getenv("RICHCOM_SHOP_ID", "1")
    token = getenv("RICHCOM_AGENT_TOKEN", "")
    host = getenv("PRINTER_HOST", "127.0.0.1") or "127.0.0.1"
    port_s = getenv("PRINTER_PORT", "9100") or "9100"
    interval = float(getenv("POLL_INTERVAL_SEC", "2") or "2")

    if not base:
        print("Set RICHCOM_BASE_URL (e.g. https://app.example.com)", file=sys.stderr)
        return 1
    if not token:
        print("Set RICHCOM_AGENT_TOKEN from POS printer settings.", file=sys.stderr)
        return 1
    try:
        shop_id = int(shop_raw)
    except ValueError:
        print("RICHCOM_SHOP_ID must be an integer.", file=sys.stderr)
        return 1
    try:
        port = int(port_s)
    except ValueError:
        print("PRINTER_PORT must be an integer.", file=sys.stderr)
        return 1

    print(f"Polling {base} shop={shop_id} → printer {host}:{port}", flush=True)

    while True:
        try:
            jobs = poll_once(base, shop_id, token)
        except urllib.error.HTTPError as e:
            print("poll HTTP", e.code, e.reason, file=sys.stderr, flush=True)
            time.sleep(interval)
            continue
        except Exception as e:
            print("poll error:", e, file=sys.stderr, flush=True)
            time.sleep(interval)
            continue

        for job in jobs:
            jid = int(job.get("id") or 0)
            b64 = (job.get("data_b64") or "").strip()
            if not jid or not b64:
                continue
            try:
                raw = base64.b64decode(b64, validate=True)
            except Exception as e:
                try:
                    ack_job(base, shop_id, token, jid, failed=True, error=f"base64: {e}")
                except Exception:
                    pass
                continue
            if not raw:
                try:
                    ack_job(base, shop_id, token, jid, failed=True, error="empty payload")
                except Exception:
                    pass
                continue
            try:
                send_to_printer(host, port, raw)
            except OSError as e:
                print("print failed:", e, file=sys.stderr, flush=True)
                try:
                    ack_job(base, shop_id, token, jid, failed=True, error=str(e))
                except Exception as ex:
                    print("ack failed:", ex, file=sys.stderr, flush=True)
                continue
            try:
                ack_job(base, shop_id, token, jid, failed=False)
            except Exception as ex:
                print("ack after print failed:", ex, file=sys.stderr, flush=True)

        time.sleep(interval)


if __name__ == "__main__":
    raise SystemExit(main())
