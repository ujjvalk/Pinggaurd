"""Monitor checks. Each returns (ok, latency_ms | None, error_message)."""
import asyncio
import re
import ssl
import time
from urllib.parse import urlsplit

import httpx

TYPES = ("http", "tcp", "ping", "ssl", "heartbeat")
HOST_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")


def host_port(target, default=None):
    parsed = urlsplit("//" + target)
    try:
        port = parsed.port if parsed.port is not None else default
        host = parsed.hostname
    except ValueError as e:
        raise ValueError("Invalid host or port") from e
    if not host or parsed.username or parsed.password or parsed.path or parsed.query or parsed.fragment:
        raise ValueError("Target must look like host:port")
    if port is None or not 1 <= port <= 65535 or target.endswith(":"):
        raise ValueError("Port must be between 1 and 65535")
    return host, port


def validate(mtype, target, options):
    if mtype not in TYPES:
        raise ValueError(f"Type must be one of: {', '.join(TYPES)}")
    numeric = {"http": {"expected_status": (0, 599)}, "ssl": {"warn_days": (0, 3650)},
               "heartbeat": {"grace": (0, 604800)}}
    allowed = {"http": {"expected_status", "keyword", "verify_ssl"}, "ssl": {"warn_days"},
               "heartbeat": {"grace"}, "tcp": set(), "ping": set()}
    if options.keys() - allowed[mtype]:
        raise ValueError("Unknown options for monitor type")
    for key, (low, high) in numeric.get(mtype, {}).items():
        if key in options:
            value = options[key]
            if type(value) is not int or not low <= value <= high:
                raise ValueError(f"{key} must be an integer between {low} and {high}")
    if mtype == "http":
        if 0 < options.get("expected_status", 0) < 100:
            raise ValueError("expected_status must be 0 or between 100 and 599")
        if "verify_ssl" in options and type(options["verify_ssl"]) is not bool:
            raise ValueError("verify_ssl must be a boolean")
        if "keyword" in options and not isinstance(options["keyword"], str):
            raise ValueError("keyword must be a string")
        try:
            url = urlsplit(target)
            port = url.port
            if url.scheme not in ("http", "https") or not url.hostname or (port is not None and port < 1):
                raise ValueError()
        except ValueError as e:
            raise ValueError("Enter a valid HTTP or HTTPS URL") from e
    if mtype == "heartbeat":
        return
    if not target:
        raise ValueError("Target is required")
    if mtype == "ping" and not HOST_RE.fullmatch(target):
        raise ValueError("Enter a hostname or IPv4 address")
    if mtype == "tcp":
        host_port(target)
    if mtype == "ssl":
        host_port(target, 443)


async def check_http(m):
    o = m["options"]
    t0 = time.perf_counter()
    async with httpx.AsyncClient(
        timeout=m["timeout"], follow_redirects=True,
        verify=o.get("verify_ssl", True), headers={"User-Agent": "PingGuard/1.0"},
    ) as client:
        r = await client.get(m["target"])
    ms = (time.perf_counter() - t0) * 1000
    want = int(o.get("expected_status") or 0)
    if want and r.status_code != want:
        return False, ms, f"HTTP {r.status_code} (expected {want})"
    if not want and r.status_code >= 400:
        return False, ms, f"HTTP {r.status_code}"
    kw = o.get("keyword")
    if kw and kw not in r.text:
        return False, ms, f"Keyword '{kw}' not found in response"
    return True, ms, ""


async def check_tcp(m):
    host, port = host_port(m["target"])
    t0 = time.perf_counter()
    _, writer = await asyncio.wait_for(asyncio.open_connection(host, port), m["timeout"])
    ms = (time.perf_counter() - t0) * 1000
    writer.close()
    await writer.wait_closed()
    return True, ms, ""


async def check_ping(m):
    proc = await asyncio.create_subprocess_exec(
        "ping", "-c", "1", "-W", str(m["timeout"]), m["target"],
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        out, _ = await asyncio.wait_for(proc.communicate(), m["timeout"] + 3)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        if proc.returncode is None:
            proc.kill()
        await proc.communicate()
        raise
    if proc.returncode != 0:
        return False, None, "No ping reply"
    hit = re.search(r"time[=<]([\d.]+)", out.decode())
    return True, float(hit.group(1)) if hit else None, ""


async def check_ssl(m):
    host, port = host_port(m["target"], 443)
    t0 = time.perf_counter()
    _, writer = await asyncio.wait_for(asyncio.open_connection(
        host, port, ssl=ssl.create_default_context(), server_hostname=host), m["timeout"])
    ms = (time.perf_counter() - t0) * 1000
    cert = writer.get_extra_info("peercert")
    writer.close()
    await writer.wait_closed()
    days = (ssl.cert_time_to_seconds(cert["notAfter"]) - time.time()) / 86400
    if days < int(m["options"].get("warn_days", 14)):
        return False, ms, f"Certificate expires in {days:.0f} days"
    return True, ms, ""


def check_heartbeat(m):
    ref = m["last_ping"] or m["created"]
    grace = int(m["options"].get("grace", 60))
    silent = time.time() - ref
    if silent > m["interval"] + grace:
        return False, None, f"No heartbeat for {int(silent)}s (expected every {m['interval']}s)"
    return True, None, ""


CHECKS = {"http": check_http, "tcp": check_tcp, "ping": check_ping, "ssl": check_ssl}


async def run(m):
    try:
        if m["type"] == "heartbeat":
            return check_heartbeat(m)
        return await asyncio.wait_for(CHECKS[m["type"]](m), m["timeout"] + 5)
    except Exception as e:  # network errors, timeouts, bad certs...
        msg = f"{type(e).__name__}: {e}" if str(e) else type(e).__name__
        return False, None, msg[:300]
