# PingGuard

Self-hosted uptime monitoring with multi-channel alerts.
Python + SQLite, one small service, nothing to install beyond three pip packages.

**Monitors:** website/API (status code + keyword), TCP port, ping, SSL certificate expiry, heartbeat (cron jobs push to a URL).
**Alerts:** email (SMTP), SMS (Twilio), WhatsApp (Twilio), Microsoft Teams, Slack, Discord, Telegram, push (ntfy, Pushover), generic webhook.
An alert fires after N consecutive failures (you choose N), and a recovery message with the downtime is sent when the monitor comes back.

## Install (Ubuntu/Debian)

Run from the unzipped `pingguard/` folder:

```bash
sudo apt install -y python3 python3-venv iputils-ping
sudo useradd --system --home /opt/pingguard --shell /usr/sbin/nologin pingguard
sudo mkdir -p /opt/pingguard/data
sudo cp -r app.py checks.py notifiers.py static requirements.txt /opt/pingguard/
sudo cp .env.example /opt/pingguard/.env
sudo python3 -m venv /opt/pingguard/venv
sudo /opt/pingguard/venv/bin/pip install -r /opt/pingguard/requirements.txt

sudo nano /opt/pingguard/.env          # set PINGGUARD_PASSWORD to a long random string
sudo chmod 600 /opt/pingguard/.env
sudo chown -R pingguard:pingguard /opt/pingguard

sudo cp pingguard.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now pingguard
journalctl -u pingguard -f
```

The dashboard listens on `127.0.0.1:8080`. Log in with `PINGGUARD_USER` / `PINGGUARD_PASSWORD`.
The service refuses to start if no password is set.

## Put HTTPS in front (recommended)

Basic auth sends the password with every request, so don't expose port 8080 directly. With Caddy:

```
monitor.example.com {
    reverse_proxy 127.0.0.1:8080
}
```

nginx works equally well. The public heartbeat URLs (`/hb/<token>`) go through the same proxy.

## Set up channels

Add them in the dashboard under **Alert channels**, then press **Send test**.

| Channel | What you need |
|---|---|
| Email | SMTP host, port, login, from/to addresses |
| SMS | Twilio Account SID, auth token, a Twilio number |
| WhatsApp | Same Twilio account plus a WhatsApp-enabled sender (the Twilio sandbox works for testing). Outside a 24-hour conversation window WhatsApp only delivers approved templates, so production alerts need an approved template or the recipient messaging your number first. |
| Teams | A Workflows webhook URL ("Post to a channel when a webhook request is received") |
| Slack / Discord | Incoming webhook URL |
| Telegram | Bot token from @BotFather and your chat ID |
| Push (ntfy) | A topic name; run your own ntfy server for fully self-hosted push |
| Push (Pushover) | User key and application token |
| Webhook | Any URL that accepts JSON, e.g. a bridge to MSG91, Mattermost or PagerDuty |

Channel secrets are stored in the SQLite file as plain text. Keep `data/` readable only by the `pingguard` user and back it up like any credential store.

## Heartbeat monitors (cron jobs, backups)

Create a heartbeat monitor, copy its URL, and append it to the job:

```bash
0 2 * * *  /usr/local/bin/backup.sh && curl -fsS https://monitor.example.com/hb/<token> > /dev/null
```

Set "Check every" to how often the job runs, and a grace period for slow runs.

## Notes

- Ping checks need `iputils-ping`; the service file grants `CAP_NET_RAW` so the unprivileged user can run it.
- Check history is kept for 30 days (`PINGGUARD_RETENTION_DAYS`).
- Everything the dashboard does is available under `/api/...` with the same basic auth, e.g. `GET /api/monitors`.
- It runs as a single process. A server watching itself can't report that the whole machine or its network is down, so run a second copy elsewhere, or send a heartbeat to an external service, to cover that case.
- To add a channel type, add an entry to `SCHEMA` and `SENDERS` in `notifiers.py`; the dashboard form builds itself from it.

## Reliability and development

The database defaults to `data/pingguard.db`. Existing installations using the old
`pingguard.db` location should set `PINGGUARD_DB` explicitly or move their database
while the service is stopped (including any SQLite WAL/SHM files).

Notification failures are retried up to three total attempts with 1s and 2s delays.
Retries are in memory; restarting the service loses pending deliveries. A provider
may accept a request before a timeout, so retries can occasionally duplicate alerts.
Provider exception details are omitted from logs and API responses to protect credentials.

Run regression tests with `python -m unittest discover -s tests -v` after installing
`requirements.txt`. Local startup: set `PINGGUARD_PASSWORD` in your environment and
run `python app.py`. The `.env` file is loaded by systemd, not by local Python startup.
