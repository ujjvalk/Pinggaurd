"""Notification channels.

To add a channel: add an entry to SCHEMA (the dashboard builds its form from it)
and a matching async sender in SENDERS.
"""
import asyncio
import smtplib
from email.message import EmailMessage

import httpx


def F(key, label, default="", secret=False, required=True):
    return {"key": key, "label": label, "default": default, "secret": secret, "required": required}


SCHEMA = {
    "email": {"label": "Email (SMTP)", "help": "Port 465 uses SSL; other ports use STARTTLS unless you set it to false.",
              "fields": [F("host", "SMTP host"), F("port", "Port", "587"), F("username", "Username", required=False),
                         F("password", "Password", secret=True, required=False), F("from", "From address"),
                         F("to", "Send to (comma-separated)"), F("starttls", "Use STARTTLS (true/false)", "true")]},
    "sms": {"label": "SMS (Twilio)", "help": "Phone numbers use international format, e.g. +919876543210.",
            "fields": [F("account_sid", "Account SID"), F("auth_token", "Auth token", secret=True),
                       F("from", "Twilio number"), F("to", "Send to (comma-separated)")]},
    "whatsapp": {"label": "WhatsApp (Twilio)",
                 "help": "Use your WhatsApp-enabled Twilio number (sandbox: +14155238886). Outside a 24-hour "
                         "conversation window, WhatsApp only delivers pre-approved templates.",
                 "fields": [F("account_sid", "Account SID"), F("auth_token", "Auth token", secret=True),
                            F("from", "WhatsApp sender number"), F("to", "Send to (comma-separated)")]},
    "teams": {"label": "Microsoft Teams",
              "help": "In Teams, create a Workflows webhook (\"Post to a channel when a webhook request is received\") "
                      "and paste its URL. Old Office 365 connector URLs are being retired.",
              "fields": [F("webhook_url", "Workflow webhook URL", secret=True)]},
    "slack": {"label": "Slack", "help": "Create an incoming webhook in your Slack app.",
              "fields": [F("webhook_url", "Webhook URL", secret=True)]},
    "discord": {"label": "Discord", "help": "Channel settings > Integrations > Webhooks.",
                "fields": [F("webhook_url", "Webhook URL", secret=True)]},
    "telegram": {"label": "Telegram", "help": "Create a bot with @BotFather, message it once, then use your chat ID.",
                 "fields": [F("bot_token", "Bot token", secret=True), F("chat_id", "Chat ID")]},
    "ntfy": {"label": "Push - ntfy", "help": "Install the ntfy app and subscribe to your topic. "
                                              "Point the server at your own ntfy instance for fully self-hosted push.",
             "fields": [F("server", "Server URL", "https://ntfy.sh"), F("topic", "Topic"),
                        F("token", "Access token", secret=True, required=False)]},
    "pushover": {"label": "Push - Pushover", "help": "Needs the Pushover app on your phone.",
                 "fields": [F("user_key", "User key"), F("api_token", "Application token", secret=True)]},
    "webhook": {"label": "Generic webhook (JSON)", "help": "POSTs the alert as JSON to your URL.",
                "fields": [F("url", "URL", secret=True)]},
}


def _split(value):
    return [p.strip() for p in str(value).split(",") if p.strip()]


def render(ev):
    icon, word = {"DOWN": ("🔴", "DOWN"), "UP": ("🟢", "RECOVERED"), "TEST": ("🔔", "TEST")}[ev["status"]]
    lines = [f"Target: {ev['target']}" if ev.get("target") else "",
             f"Reason: {ev['reason']}" if ev.get("reason") else "",
             f"Downtime: {ev['duration']}" if ev.get("duration") else "",
             f"Time: {ev['time']}"]
    return f"{icon} {word}: {ev['monitor']}", "\n".join(l for l in lines if l)


def _plain(ev):
    word = {"DOWN": "DOWN", "UP": "RECOVERED", "TEST": "TEST"}[ev["status"]]
    extra = f" - {ev['reason']}" if ev.get("reason") else ""
    return f"[PingGuard] {word}: {ev['monitor']}{extra}"[:300]


async def _post(url, **kw):
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(url, **kw)
    if not 200 <= r.status_code < 300:  # URLs and bodies may contain secrets
        raise RuntimeError(f"Notification provider returned HTTP {r.status_code}")


async def _email(c, ev):
    title, body = render(ev)

    def work():
        msg = EmailMessage()
        msg["Subject"], msg["From"], msg["To"] = title, c["from"], c["to"]
        msg.set_content(body)
        port = int(c.get("port") or 587)
        if port == 465:
            smtp = smtplib.SMTP_SSL(c["host"], port, timeout=20)
        else:
            smtp = smtplib.SMTP(c["host"], port, timeout=20)
            if str(c.get("starttls", "true")).lower() != "false":
                smtp.starttls()
        with smtp:
            if c.get("username"):
                smtp.login(c["username"], c.get("password", ""))
            smtp.send_message(msg)

    await asyncio.to_thread(work)


async def _twilio(c, text, prefix=""):
    sid = c["account_sid"]
    for to in _split(c["to"]):
        await _post(f"https://api.twilio.com/2010-04-01/Accounts/{sid}/Messages.json",
                    auth=(sid, c["auth_token"]),
                    data={"From": prefix + c["from"], "To": prefix + to, "Body": text})


async def _sms(c, ev):
    await _twilio(c, _plain(ev))


async def _whatsapp(c, ev):
    title, body = render(ev)
    await _twilio(c, f"{title}\n{body}"[:1500], prefix="whatsapp:")


async def _teams(c, ev):
    title, body = render(ev)
    card = {"$schema": "http://adaptivecards.io/schemas/adaptive-card.json", "type": "AdaptiveCard", "version": "1.4",
            "body": [{"type": "TextBlock", "text": title, "weight": "Bolder", "size": "Medium",
                      "color": "Good" if ev["status"] == "UP" else "Attention"},
                     {"type": "TextBlock", "text": body.replace("\n", "\n\n"), "wrap": True}]}
    await _post(c["webhook_url"], json={"type": "message", "attachments": [
        {"contentType": "application/vnd.microsoft.card.adaptive", "content": card}]})


async def _slack(c, ev):
    title, body = render(ev)
    await _post(c["webhook_url"], json={"text": f"*{title}*\n{body}"})


async def _discord(c, ev):
    title, body = render(ev)
    await _post(c["webhook_url"], json={"content": f"**{title}**\n{body}"[:1900]})


async def _telegram(c, ev):
    title, body = render(ev)
    await _post(f"https://api.telegram.org/bot{c['bot_token']}/sendMessage",
                json={"chat_id": c["chat_id"], "text": f"{title}\n{body}", "disable_web_page_preview": True})


async def _ntfy(c, ev):
    title, body = render(ev)
    down = ev["status"] == "DOWN"
    headers = {"Authorization": f"Bearer {c['token']}"} if c.get("token") else {}
    await _post((c.get("server") or "https://ntfy.sh").rstrip("/"), headers=headers, json={
        "topic": c["topic"], "title": title, "message": body, "priority": 5 if down else 3,
        "tags": ["rotating_light"] if down else ["white_check_mark"]})


async def _pushover(c, ev):
    title, body = render(ev)
    await _post("https://api.pushover.net/1/messages.json", data={
        "token": c["api_token"], "user": c["user_key"], "title": title, "message": body,
        "priority": 1 if ev["status"] == "DOWN" else 0})


async def _webhook(c, ev):
    title, body = render(ev)
    await _post(c["url"], json={**ev, "title": title, "text": body})


SENDERS = {"email": _email, "sms": _sms, "whatsapp": _whatsapp, "teams": _teams, "slack": _slack,
           "discord": _discord, "telegram": _telegram, "ntfy": _ntfy, "pushover": _pushover, "webhook": _webhook}


async def send(ctype, config, ev):
    await SENDERS[ctype](config, ev)
