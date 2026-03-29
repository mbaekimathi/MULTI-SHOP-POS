# Richcom LAN print agent

Use this when the Richcom POS app is **hosted on the internet** but the thermal printer only exists on the **shop LAN**. Checkout queues ESC/POS bytes in MySQL; this process polls your site and prints to a **local** IP:port.

## 1. Enable in POS

1. Save a **network** printer (IP/host + port) as usual.
2. Open **Printer setup → Network** and scroll to **LAN print agent**.
3. Turn on **Queue prints for agent**, click **Save agent settings**.
4. Copy the shown `RICHCOM_AGENT_TOKEN` (or use **New token** if you lose it).

## 2. Run on a shop PC (same Wi‑Fi as the printer)

Install Python 3.9+ on the PC that can reach the printer (usually the printer’s own IP, e.g. `192.168.100.171:9100`).

```bash
cd print_agent
set RICHCOM_BASE_URL=https://your-production-domain.com
set RICHCOM_SHOP_ID=1
set RICHCOM_AGENT_TOKEN=paste-token-here
set PRINTER_HOST=192.168.100.171
set PRINTER_PORT=9100
python run_agent.py
```

- `PRINTER_HOST` / `PRINTER_PORT` are **on this machine’s network** (not the cloud). They can match what you saved in POS or another address if routing differs.
- Leave the process running (Windows Task Scheduler, `nssm`, or a systemd service on Linux).

## 3. Behaviour

- With **agent mode on**, the server **does not** open TCP to the printer; it only **enqueues** jobs.
- The agent **GET**s pending jobs (authenticated with the token), prints each payload, then **POST**s ack.
- With **agent mode off**, behaviour is unchanged: the server connects directly to the saved printer (works when the app and printer share a LAN).

## Security

Keep `RICHCOM_AGENT_TOKEN` secret. Use HTTPS for `RICHCOM_BASE_URL`. Rotate the token if it leaks (POS → **New token**).
