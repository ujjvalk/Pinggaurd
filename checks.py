"""Monitor checks. Each returns (ok, latency_ms | None, error_message)."""
import asyncio
import re
import ssl
import time

import httpx

TYPES = ("http", "tcp", "ping", "ssl", "heartbeat")
HOST_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")


def host_port(target, default=None):
    if ":" in target:
        host, port = target.rsplit(":", 1)
        return host, int(port)
    if default is None:
        raise ValueError("Target must look like host:port")
    return target, default


def validate(mtype, target, options):
    if mtype not in TYPES:
        raise ValueError(f"Type must be one of: {', '.join(TYPES)}")
    if mtype == "heartbeat":
        return
    if not target:
        raise ValueError("Target is required")
    if mtype == "http" and not target.startswith(("http://", "https://")):
        raise ValueError("URL must start with http:// or https://")
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
    return True, ms, ""


async def check_ping(m):
    proc = await asyncio.create_subprocess_exec(
        "ping", "-c", "1", "-W", str(m["timeout"]), m["target"],
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        out, _ = await asyncio.wait_for(proc.communicate(), m["timeout"] + 3)
    except asyncio.TimeoutError:
        proc.kill()
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
