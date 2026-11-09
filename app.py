"""PingGuard - self-hosted uptime monitoring with multi-channel alerting."""
import asyncio
import json
import logging
import os
import secrets
import sqlite3
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import APIRouter, Depends, FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel, Field

import checks
import notifiers

BASE = Path(__file__).parent
DB_PATH = os.getenv("PINGGUARD_DB", str(BASE / "pingguard.db"))
ADMIN_USER = os.getenv("PINGGUARD_USER", "admin")
ADMIN_PASS = os.getenv("PINGGUARD_PASSWORD", "")
RETENTION_DAYS = int(os.getenv("PINGGUARD_RETENTION_DAYS", "30"))
MAX_CONCURRENT = int(os.getenv("PINGGUARD_MAX_CONCURRENT", "20"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("pingguard")

# ---------------------------------------------------------------- database
SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS channels(
  id INTEGER PRIMARY KEY, name TEXT NOT NULL, type TEXT NOT NULL, config TEXT NOT NULL, enabled INTEGER DEFAULT 1);
CREATE TABLE IF NOT EXISTS monitors(
  id INTEGER PRIMARY KEY, name TEXT NOT NULL, type TEXT NOT NULL, target TEXT NOT NULL DEFAULT '',
  options TEXT NOT NULL DEFAULT '{}', interval INTEGER NOT NULL DEFAULT 60, timeout INTEGER NOT NULL DEFAULT 10,
  threshold INTEGER NOT NULL DEFAULT 2, channel_ids TEXT NOT NULL DEFAULT '[]', enabled INTEGER NOT NULL DEFAULT 1,
  token TEXT, state TEXT NOT NULL DEFAULT 'pending', fail_count INTEGER NOT NULL DEFAULT 0,
  last_check REAL NOT NULL DEFAULT 0, last_latency REAL, last_error TEXT DEFAULT '', last_change REAL,
  last_ping REAL, created REAL);
CREATE TABLE IF NOT EXISTS checks(id INTEGER PRIMARY KEY, monitor_id INTEGER, ts REAL, ok INTEGER, latency REAL, error TEXT);
CREATE INDEX IF NOT EXISTS idx_checks ON checks(monitor_id, ts);
CREATE TABLE IF NOT EXISTS incidents(id INTEGER PRIMARY KEY, monitor_id INTEGER, started REAL, ended REAL, reason TEXT);
"""
os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)
db = sqlite3.connect(DB_PATH, check_same_thread=False, isolation_level=None)
db.row_factory = sqlite3.Row
db.executescript(SCHEMA)
lock = threading.Lock()


def q(sql, args=(), one=False):
    with lock:
        rows = db.execute(sql, args).fetchall()
    return (rows[0] if rows else None) if one else rows


def run_sql(sql, args=()):
    with lock:
        return db.execute(sql, args).lastrowid


# ---------------------------------------------------------------- alerting
def fmt_duration(sec):
    d, rem = divmod(int(sec), 86400)
    h, rem = divmod(rem, 3600)
    m, s = divmod(rem, 60)
    parts = [f"{d}d" if d else "", f"{h}h" if h else "", f"{m}m" if m else "", f"{s}s" if s or not (d or h or m) else ""]
    return " ".join(p for p in parts if p)


def make_event(status, name, target="", reason="", duration=None):
    return {"status": status, "monitor": name, "target": target, "reason": reason, "duration": duration,
            "time": time.strftime("%Y-%m-%d %H:%M:%S %Z")}


async def send_one(channel, ev):
    """Send one event through one channel. Returns an error string, or None on success."""
    try:
        await notifiers.send(channel["type"], json.loads(channel["config"]), ev)
        return None
    except Exception as e:
        err = (str(e) or type(e).__name__)[:300]
        log.warning("channel %s (%s) failed: %s", channel["name"], channel["type"], err)
        return err


async def dispatch(monitor, ev):
    ids = json.loads(monitor["channel_ids"])
    sql = "SELECT * FROM channels WHERE enabled=1"
    if ids:
        sql += f" AND id IN ({','.join('?' * len(ids))})"
    channels = q(sql, ids)
    if not channels:
        log.warning("monitor %s changed state but has no channels to notify", monitor["name"])
    await asyncio.gather(*(send_one(c, ev) for c in channels))


async def record(m, ok, latency, err):
    """Store a check result, update state, and alert on DOWN / recovery."""
    now = time.time()
    run_sql("INSERT INTO checks(monitor_id, ts, ok, latency, error) VALUES(?,?,?,?,?)",
            (m["id"], now, int(ok), latency, err))
    prev = m["state"]
    fails = 0 if ok else m["fail_count"] + 1
    state, event = prev, None
    if ok:
        state = "up"
        event = "UP" if prev == "down" else None
    elif fails >= m["threshold"] and prev != "down":
        state, event = "down", "DOWN"
    run_sql("UPDATE monitors SET state=?, fail_count=?, last_check=?, last_latency=?, last_error=?, "
            "last_change=CASE WHEN state!=? THEN ? ELSE last_change END WHERE id=?",
            (state, fails, now, latency, err, state, now, m["id"]))
    if event == "DOWN":
        run_sql("INSERT INTO incidents(monitor_id, started, reason) VALUES(?,?,?)", (m["id"], now, err))
        await dispatch(m, make_event("DOWN", m["name"], m["target"], err))
    elif event == "UP":
        inc = q("SELECT * FROM incidents WHERE monitor_id=? AND ended IS NULL ORDER BY id DESC LIMIT 1",
                (m["id"],), one=True)
        run_sql("UPDATE incidents SET ended=? WHERE monitor_id=? AND ended IS NULL", (now, m["id"]))
        dur = fmt_duration(now - inc["started"]) if inc else None
        await dispatch(m, make_event("UP", m["name"], m["target"], "Back to normal", dur))


async def check_one(row, sem, running):
    m = dict(row)
    m["options"] = json.loads(m["options"])
    try:
        async with sem:
            ok, latency, err = await checks.run(m)
        m["options"] = row["options"]
        await record(m, ok, latency, err)
    except Exception:
        log.exception("check failed for monitor %s", row["id"])
    finally:
        running.discard(row["id"])


async def scheduler():
    sem, running, tasks, last_prune = asyncio.Semaphore(MAX_CONCURRENT), set(), set(), 0
    while True:
        now = time.time()
        due = q("SELECT * FROM monitors WHERE enabled=1 AND last_check + "
                "CASE WHEN type='heartbeat' THEN MIN(interval, 30) ELSE interval END <= ?", (now,))
        for row in due:
            if row["id"] not in running:
                running.add(row["id"])
                t = asyncio.create_task(check_one(row, sem, running))
                tasks.add(t)
                t.add_done_callback(tasks.discard)
        if now - last_prune > 3600:
            run_sql("DELETE FROM checks WHERE ts < ?", (now - RETENTION_DAYS * 86400,))
            last_prune = now
        await asyncio.sleep(2)


@asynccontextmanager
async def lifespan(_):
    if not ADMIN_PASS:
        raise RuntimeError("Set PINGGUARD_PASSWORD before starting PingGuard")
    task = asyncio.create_task(scheduler())
    log.info("PingGuard started, database: %s", DB_PATH)
    yield
    task.cancel()


# ---------------------------------------------------------------- API
basic = HTTPBasic()


def auth(c: HTTPBasicCredentials = Depends(basic)):
    ok = secrets.compare_digest(c.username.encode(), ADMIN_USER.encode()) and \
        secrets.compare_digest(c.password.encode(), ADMIN_PASS.encode())
    if not ok:
        raise HTTPException(401, "Wrong username or password", headers={"WWW-Authenticate": "Basic"})


app = FastAPI(title="PingGuard", lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
api = APIRouter(prefix="/api", dependencies=[Depends(auth)])


class MonitorIn(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    type: str
    target: str = ""
    interval: int = Field(60, ge=10, le=86400)
    timeout: int = Field(10, ge=1, le=120)
    threshold: int = Field(2, ge=1, le=20)
    options: dict = {}
    channel_ids: list[int] = []


class ChannelIn(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    type: str
    config: dict[str, str] = {}


def monitor_out(r):
    d = dict(r)
    d["options"], d["channel_ids"] = json.loads(d["options"]), json.loads(d["channel_ids"])
    if d["type"] != "heartbeat":
        d.pop("token")
    stats = q("SELECT COUNT(*) n, SUM(ok) s FROM checks WHERE monitor_id=? AND ts>?",
              (d["id"], time.time() - 86400), one=True)
    d["uptime_24h"] = round(100 * stats["s"] / stats["n"], 2) if stats["n"] else None
    recent = q("SELECT ok FROM checks WHERE monitor_id=? ORDER BY ts DESC LIMIT 40", (d["id"],))
    d["recent"] = [r["ok"] for r in reversed(recent)]
    return d


def channel_out(r):
    fields = {f["key"]: f for f in notifiers.SCHEMA[r["type"]]["fields"]}
    cfg = {k: ("********" if fields.get(k, {}).get("secret") and v else v) for k, v in json.loads(r["config"]).items()}
    return {"id": r["id"], "name": r["name"], "type": r["type"], "enabled": bool(r["enabled"]), "config": cfg}


@api.get("/channel-types")
def channel_types():
    return notifiers.SCHEMA


@api.get("/channels")
def list_channels():
    return [channel_out(r) for r in q("SELECT * FROM channels ORDER BY id")]


@api.post("/channels")
def add_channel(b: ChannelIn):
    schema = notifiers.SCHEMA.get(b.type)
    if not schema:
        raise HTTPException(400, "Unknown channel type")
    cfg = {f["key"]: b.config.get(f["key"], f["default"]).strip() for f in schema["fields"]}
    missing = [f["label"] for f in schema["fields"] if f["required"] and not cfg[f["key"]]]
    if missing:
        raise HTTPException(400, "Missing: " + ", ".join(missing))
    return {"id": run_sql("INSERT INTO channels(name, type, config) VALUES(?,?,?)", (b.name, b.type, json.dumps(cfg)))}


@api.delete("/channels/{cid}")
def delete_channel(cid: int):
    run_sql("DELETE FROM channels WHERE id=?", (cid,))
    return {"ok": True}


@api.post("/channels/{cid}/test")
async def test_channel(cid: int):
    ch = q("SELECT * FROM channels WHERE id=?", (cid,), one=True)
    if not ch:
        raise HTTPException(404, "Channel not found")
    err = await send_one(ch, make_event("TEST", "Test alert", "", "This is a test notification from PingGuard"))
    if err:
        raise HTTPException(502, err)
    return {"ok": True}


@api.get("/monitors")
def list_monitors():
    return [monitor_out(r) for r in q("SELECT * FROM monitors ORDER BY id")]


@api.post("/monitors")
def add_monitor(b: MonitorIn):
    target = b.target.strip()
    try:
        checks.validate(b.type, target, b.options)
    except ValueError as e:
        raise HTTPException(400, str(e))
    token = secrets.token_urlsafe(16) if b.type == "heartbeat" else None
    mid = run_sql("INSERT INTO monitors(name,type,target,options,interval,timeout,threshold,channel_ids,token,created) "
                  "VALUES(?,?,?,?,?,?,?,?,?,?)",
                  (b.name, b.type, target, json.dumps(b.options), b.interval, b.timeout, b.threshold,
                   json.dumps(b.channel_ids), token, time.time()))
    return {"id": mid}


@api.post("/monitors/{mid}/toggle")
def toggle_monitor(mid: int):
    run_sql("UPDATE monitors SET enabled = 1 - enabled WHERE id=?", (mid,))
    return {"ok": True}


@api.delete("/monitors/{mid}")
def delete_monitor(mid: int):
    for table in ("checks", "incidents"):
        run_sql(f"DELETE FROM {table} WHERE monitor_id=?", (mid,))
    run_sql("DELETE FROM monitors WHERE id=?", (mid,))
    return {"ok": True}


@api.get("/monitors/{mid}/history")
def history(mid: int):
    return [dict(r) for r in q("SELECT ts, ok, latency, error FROM checks WHERE monitor_id=? "
                               "ORDER BY ts DESC LIMIT 200", (mid,))]


@api.get("/incidents")
def incidents():
    rows = q("SELECT i.*, m.name FROM incidents i JOIN monitors m ON m.id=i.monitor_id "
             "ORDER BY i.started DESC LIMIT 30")
    return [dict(r) for r in rows]


app.include_router(api)


@app.api_route("/hb/{token}", methods=["GET", "POST", "HEAD"])
def heartbeat(token: str):
    """Public endpoint for cron jobs: curl -fsS https://host/hb/<token>"""
    m = q("SELECT id FROM monitors WHERE token=? AND type='heartbeat'", (token,), one=True)
    if not m:
        raise HTTPException(404, "Unknown heartbeat token")
    run_sql("UPDATE monitors SET last_ping=? WHERE id=?", (time.time(), m["id"]))
    return {"ok": True}


@app.get("/", dependencies=[Depends(auth)])
def index():
    return FileResponse(BASE / "static" / "index.html")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=int(os.getenv("PORT", "8080")))
