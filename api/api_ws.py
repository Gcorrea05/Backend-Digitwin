# api/api_ws.py
import os
import re
import time
import asyncio
import math
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Any, Dict, Tuple

from dotenv import load_dotenv, find_dotenv
from fastapi import FastAPI, Request, HTTPException, Query, WebSocket, WebSocketDisconnect, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from mysql.connector.errors import PoolError

# =============================================================================
# .env
# =============================================================================
load_dotenv(find_dotenv())
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

# =============================================================================
# FastAPI + CORS
# =============================================================================
app = FastAPI(title="Festo DT API (WS-first)", version="4.0.0")

ALLOWED_ORIGINS = [
    o.strip()
    for o in os.getenv("ALLOWED_ORIGINS", "http://localhost:8080,http://127.0.0.1:8080").split(",")
    if o.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

@app.middleware("http")
async def ensure_cors_headers(request: Request, call_next):
    try:
        resp = await call_next(request)
    except Exception:
        resp = JSONResponse(status_code=500, content={"detail": "Internal Server Error"})
    origin = request.headers.get("origin")
    if origin and origin in ALLOWED_ORIGINS:
        resp.headers.setdefault("Access-Control-Allow-Origin", origin)
        resp.headers.setdefault("Vary", "Origin")
        resp.headers.setdefault("Access-Control-Allow-Credentials", "true")
        resp.headers.setdefault("Access-Control-Expose-Headers", "*")
    return resp

@app.options("/{full_path:path}")
def any_options(full_path: str, request: Request):
    origin = request.headers.get("origin")
    headers = {}
    if origin and origin in ALLOWED_ORIGINS:
        headers["Access-Control-Allow-Origin"] = origin
        headers["Vary"] = "Origin"
        headers["Access-Control-Allow-Credentials"] = "true"
        headers["Access-Control-Expose-Headers"] = "*"
    headers["Access-Control-Allow-Methods"] = request.headers.get("access-control-request-method", "*") or "*"
    req_headers = request.headers.get("access-control-request-headers")
    headers["Access-Control-Allow-Headers"] = req_headers or "*"
    return Response(status_code=204, headers=headers)

@app.exception_handler(PoolError)
async def pool_error_handler(_, __):
    return JSONResponse(status_code=503, content={"detail": "DB pool exhausted"})

# =============================================================================
# Imports locais (apenas o essencial)
# =============================================================================
from .database import get_db

# =============================================================================
# Config / Constantes
# =============================================================================
PROCESS_STARTED_MS = int(time.time() * 1000)

# Cadências (ENV sobrescreve)
LIVE_TICK_MS = int(os.getenv("LIVE_TICK_MS", "200"))          # Dashboard (200 ms)
MON_TICK_MS  = int(os.getenv("MON_TICK_MS",  "2000"))         # Monitoring (2 s)
SLOW_TICK_MS = int(os.getenv("SLOW_TICK_MS", "60000"))        # CPM (60 s)
HEARTBEAT_MS = int(os.getenv("WS_HEARTBEAT_MS", "10000"))     # Heartbeat (10 s)

# Tabelas
OPC_TABLE = os.getenv("OPC_TABLE", "opc_samples")
MPU_TABLE = os.getenv("MPU_TABLE", "mpu_samples")
_DURATION_RE = re.compile(r"^-(\d+)\s*([smhd])$", re.IGNORECASE)

MAX_LIMIT = 50000

# =============================================================================
# DB helpers (mínimos)
# =============================================================================
def get_db_safe():
    return get_db()

def fetch_one(q: str, params: Tuple[Any, ...] = ()):
    db = get_db_safe()
    try:
        db.execute(q, params)
        return db.fetchone()
    finally:
        db.close()

def fetch_all(q: str, params: Tuple[Any, ...] = ()):
    db = get_db_safe()
    try:
        db.execute(q, params)
        return db.fetchall()
    finally:
        db.close()

def col(row: Any, key_or_index: Any) -> Any:
    if row is None:
        return None
    if isinstance(row, dict):
        return row.get(key_or_index)
    if isinstance(key_or_index, int):
        return row[key_or_index]
    return None

def _parse_since_to_seconds(s: str, default_s: int = 7200) -> int:
    """Aceita -60m, -2h, -7200s (ou vazio) -> segundos (int, positivo)."""
    if not s:
        return default_s
    ss = s.strip()
    if ss.startswith("-"):
        ss = ss[1:]
    try:
        if ss.endswith("h"):
            return int(float(ss[:-1]) * 3600)
        if ss.endswith("m"):
            return int(float(ss[:-1]) * 60)
        if ss.endswith("s"):
            return int(float(ss[:-1]))
        # número puro (assume segundos)
        return int(float(ss))
    except Exception:
        return default_s

# =============================================================================
# Latch config (EXATAMENTE como seu modelo)
# =============================================================================
StableState = str  # "RECUADO" | "AVANÇADO"

@dataclass
class _LatchCfg:
    id: str
    expected_ms: int
    debounce_ms: int
    timeout_factor: float
    v_av: str
    v_rec: str
    s_adv: str
    s_rec: str

# Mantido 1:1 com seu arquivo
_CFG_A1 = _LatchCfg(
    id="A1", expected_ms=1500, debounce_ms=80, timeout_factor=1.5,
    v_av="V1_14", v_rec="V1_12", s_adv="Recuado_1S1", s_rec="Avancado_1S2",
)
_CFG_A2 = _LatchCfg(
    id="A2", expected_ms=500, debounce_ms=80, timeout_factor=1.5,
    v_av="V2_14", v_rec="V2_12", s_adv="Recuado_2S1", s_rec="Avancado_2S2",
)
_NAMES_LATCH = (
    _CFG_A1.v_av, _CFG_A1.v_rec, _CFG_A1.s_adv, _CFG_A1.s_rec,
    _CFG_A2.v_av, _CFG_A2.v_rec, _CFG_A2.s_adv, _CFG_A2.s_rec
)

# Mapa para resolver facetas -> nomes de sinais
_SMAP = {
    1: {"S1": _CFG_A1.s_adv, "S2": _CFG_A1.s_rec},
    2: {"S1": _CFG_A2.s_adv, "S2": _CFG_A2.s_rec},
}

def _resolve_signal_by_facet(act: str | int, facet: str) -> Optional[str]:
    """act pode ser 1/2 ou 'A1'/'A2'. facet é 'S1' ou 'S2'."""
    if isinstance(act, str):
        act = act.upper().replace("A", "")
    try:
        aid = int(act)
    except Exception:
        return None
    f = facet.upper()
    mp = _SMAP.get(aid)
    if not mp or f not in mp:
        return None
    return mp[f]

# =============================================================================
# Utils de tempo/ISO
# =============================================================================
def _coerce_to_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(value, tz=timezone.utc)
    elif isinstance(value, str):
        s = value.strip()
        try:
            s2 = s.replace("Z", "+00:00")
            dt = datetime.fromisoformat(s2)
        except Exception:
            dt = None
            for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
                try:
                    dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
                    break
                except Exception:
                    continue
            if dt is None:
                return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
    else:
        return None
    return dt.astimezone(timezone.utc)

def dt_to_iso_utc(value: Any) -> Optional[str]:
    dt = _coerce_to_datetime(value)
    if dt is None:
        return None
    return dt.isoformat().replace("+00:00", "Z")

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

# =============================================================================
# Leituras S1/S2 e derivados (mínimos para live/monitoring/CPM)
# =============================================================================
def _fetch_latest_rows(names: Tuple[str, ...]) -> Dict[str, Dict[str, Any]]:
    if not names:
        return {}
    placeholders = ", ".join(["%s"] * len(names))
    sql = f"""
        SELECT s.name, s.value_bool, s.ts_utc
        FROM {OPC_TABLE} s
        JOIN (
            SELECT name, MAX(ts_utc) AS ts_utc
            FROM {OPC_TABLE}
            WHERE name IN ({placeholders})
            GROUP BY name
        ) m ON m.name = s.name AND m.ts_utc = s.ts_utc
    """
    rows = fetch_all(sql, names)
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        n = col(r, "name") or col(r, 0)
        vb = col(r, "value_bool") if isinstance(r, dict) else r[1]
        ts = col(r, "ts_utc") if isinstance(r, dict) else r[2]
        out[str(n)] = {"value_bool": (None if vb is None else int(vb)), "ts_utc": ts}
    for n in names:
        out.setdefault(n, {"value_bool": None, "ts_utc": None})
    return out

def _infer_state_from_latest(cfg: _LatchCfg, latest: Dict[str, Dict[str, Any]]):
    now = datetime.now(timezone.utc)
    adv = latest.get(cfg.s_adv, {})
    rec = latest.get(cfg.s_rec, {})
    vav = latest.get(cfg.v_av, {})
    vrc = latest.get(cfg.v_rec, {})

    adv_v = int(adv.get("value_bool") or 0)
    rec_v = int(rec.get("value_bool") or 0)

    t_adv = _coerce_to_datetime(adv.get("ts_utc"))
    t_rec = _coerce_to_datetime(rec.get("ts_utc"))
    t_vav = _coerce_to_datetime(vav.get("ts_utc"))
    t_vrc = _coerce_to_datetime(vrc.get("ts_utc"))

    displayed = "RECUADO"
    if adv_v and rec_v:
        displayed = "RECUADO"  # conflito -> mantém RECUADO (regra simples)
    elif adv_v:
        displayed = "AVANÇADO"
    elif rec_v:
        displayed = "RECUADO"

    pending = None
    started_at = None

    last_cmd_ts = None
    last_cmd_kind = None  # "AV" | "REC"
    if t_vav and (not t_vrc or t_vav >= t_vrc):
        last_cmd_ts = t_vav; last_cmd_kind = "AV"
    elif t_vrc:
        last_cmd_ts = t_vrc; last_cmd_kind = "REC"

    if last_cmd_kind == "AV":
        if not adv_v or (t_adv and last_cmd_ts and last_cmd_ts > t_adv):
            pending = "AV"; started_at = last_cmd_ts or now
    elif last_cmd_kind == "REC":
        if not rec_v or (t_rec and last_cmd_ts and last_cmd_ts > t_rec):
            pending = "REC"; started_at = last_cmd_ts or now

    return {
        "state": displayed,
        "pending": pending,
        "started_at": (started_at.isoformat() if started_at else None),
    }

def _fetch_series(names: List[str], window_s: int) -> Dict[str, List[Tuple[datetime,int]]]:
    if not names:
        return {}
    placeholders = ", ".join(["%s"] * len(names))
    sql = f"""
        SELECT name, ts_utc, value_bool
        FROM {OPC_TABLE}
        WHERE name IN ({placeholders})
          AND ts_utc >= NOW(6) - INTERVAL %s SECOND
        ORDER BY ts_utc ASC
    """
    rows = fetch_all(sql, (*names, window_s))
    out: Dict[str, List[Tuple[datetime,int]]] = {str(n): [] for n in names}
    for r in rows:
        nm = str(col(r, "name") or r[0])
        ts_raw = col(r, "ts_utc") or r[1]
        vb_raw = col(r, "value_bool") if isinstance(r, dict) else r[2]
        dt = _coerce_to_datetime(ts_raw)
        if dt is None or vb_raw is None:
            continue
        try:
            v = 1 if int(vb_raw) else 0
        except Exception:
            v = 1 if bool(vb_raw) else 0
        out[nm].append((dt, v))
    return out

def _dedup(seq: List[Tuple[datetime,int]]) -> List[Tuple[datetime,int]]:
    if not seq:
        return []
    out = [seq[0]]
    for t, v in seq[1:]:
        if v != out[-1][1]:
            out.append((t, v))
    return out

def _derive_open_closed_from_S1S2(
    s1: List[Tuple[datetime,int]],
    s2: List[Tuple[datetime,int]],
) -> Tuple[List[Tuple[datetime,int]], List[Tuple[datetime,int]]]:
    if not s1 and not s2:
        return [], []
    times = sorted({t for t,_ in s1} | {t for t,_ in s2})
    if not times:
        return [], []

    def val_at(seq: List[Tuple[datetime,int]], t: datetime) -> int:
        if not seq:
            return 0
        lo, hi = 0, len(seq) - 1
        last = seq[0][1]
        while lo <= hi:
            mid = (lo + hi) // 2
            if seq[mid][0] <= t:
                last = seq[mid][1]
                lo = mid + 1
            else:
                hi = mid - 1
        return last

    opened: List[Tuple[datetime,int]] = []
    closed: List[Tuple[datetime,int]] = []
    prev_open: Optional[int] = None

    for t in times:
        v1 = val_at(s1, t)
        v2 = val_at(s2, t)
        if v1 == 0 and v2 == 1:
            cur_open, cur_close = 1, 0
            prev_open = 1
        elif v1 == 1 and v2 == 0:
            cur_open, cur_close = 0, 1
            prev_open = 0
        else:
            if prev_open is None:
                cur_open, cur_close = 0, 0
            else:
                cur_open = 1 if prev_open == 1 else 0
                cur_close = 0 if prev_open == 1 else 1
        opened.append((t, cur_open))
        closed.append((t, cur_close))
    return _dedup(opened), _dedup(closed)

def _rising_edges(seq: List[Tuple[datetime,int]]) -> List[datetime]:
    edges: List[datetime] = []
    for i in range(1, len(seq)):
        if seq[i-1][1] == 0 and seq[i][1] == 1:
            edges.append(seq[i][0])
    return edges

def _last_pulse_duration(seq: List[Tuple[datetime,int]]) -> Optional[float]:
    if not seq: return None
    last_on = None; last_pulse = None
    for i in range(1, len(seq)):
        if seq[i-1][1]==0 and seq[i][1]==1:
            last_on = seq[i][0]
        if seq[i-1][1]==1 and seq[i][1]==0 and last_on:
            last_pulse = (last_on, seq[i][0])
    if last_pulse:
        t_on, t_off = last_pulse
        return (t_off - t_on).total_seconds()
    if seq[-1][1]==1 and last_on:
        return (datetime.now(timezone.utc) - last_on).total_seconds()
    return None

# =============================================================================
# Payload builders (contratos WS)
# =============================================================================
def build_live_payload() -> dict:
    latest = _fetch_latest_rows(_NAMES_LATCH)
    a1 = _infer_state_from_latest(_CFG_A1, latest)
    a2 = _infer_state_from_latest(_CFG_A2, latest)
    items = [
        {"id": 1, "state": a1["state"]},
        {"id": 2, "state": a2["state"]},
    ]
    return {"type": "live", "ts": _now_iso(), "actuators": items}

def build_monitoring_payload() -> dict:
    # Timings pela última janela de 2s
    s_map = {1: {"S1": _CFG_A1.s_adv, "S2": _CFG_A1.s_rec},
             2: {"S1": _CFG_A2.s_adv, "S2": _CFG_A2.s_rec}}
    names = [v for m in s_map.values() for v in (m["S1"], m["S2"])]
    raw = _fetch_series(names, 2)
    compact = {k: _dedup(v) for k, v in raw.items()}
    timings = []
    for aid, m in s_map.items():
        s1 = compact.get(m["S1"], [])
        s2 = compact.get(m["S2"], [])
        opened, closed = _derive_open_closed_from_S1S2(s1, s2)
        t_abre  = _last_pulse_duration(opened)
        t_fecha = _last_pulse_duration(closed)
        t_ciclo = (t_abre or 0.0) + (t_fecha or 0.0) if (t_abre or t_fecha) else None
        timings.append({"actuator_id": aid, "last": {
            "dt_abre_s": t_abre, "dt_fecha_s": t_fecha, "dt_ciclo_s": t_ciclo
        }})

    # Vibração: RMS em 2s
    now = datetime.now(timezone.utc)
    start = now - timedelta(seconds=2)
    rows = fetch_all(
        f"SELECT mpu_id, ts_utc, ax_g, ay_g, az_g FROM {MPU_TABLE} WHERE ts_utc >= %s AND ts_utc < %s",
        (start, now),
    )
    by: Dict[int, Dict[str, List[float]]] = {}
    for r in rows:
        mid = int(col(r, "mpu_id") or col(r, 0))
        ax = col(r, "ax_g"); ay = col(r, "ay_g"); az = col(r, "az_g")
        d = by.setdefault(mid, {"ax": [], "ay": [], "az": []})
        d["ax"].append(float(ax or 0.0))
        d["ay"].append(float(ay or 0.0))
        d["az"].append(float(az or 0.0))
    vib_items = []
    for mid, d in by.items():
        rms_ax = (sum(v*v for v in d["ax"]) / max(1, len(d["ax"]))) ** 0.5
        rms_ay = (sum(v*v for v in d["ay"]) / max(1, len(d["ay"]))) ** 0.5
        rms_az = (sum(v*v for v in d["az"]) / max(1, len(d["az"]))) ** 0.5
        overall = float((rms_ax**2 + rms_ay**2 + rms_az**2) ** 0.5)
        vib_items.append({"mpu_id": mid, "overall": overall})

    return {
        "type": "monitoring",
        "ts": _now_iso(),
        "timings": timings,
        "vibration": {"window_s": 2, "items": vib_items},
    }

def build_cpm_payload(window_s: int = 60) -> dict:
    s_map = {1: {"S1": _CFG_A1.s_adv, "S2": _CFG_A1.s_rec},
             2: {"S1": _CFG_A2.s_adv, "S2": _CFG_A2.s_rec}}
    names = [v for m in s_map.values() for v in (m["S1"], m["S2"])]
    raw = _fetch_series(names, window_s)
    compact = {k: _dedup(v) for k, v in raw.items()}
    out = []
    for aid, m in s_map.items():
        s1 = compact.get(m["S1"], [])
        s2 = compact.get(m["S2"], [])
        opened, closed = _derive_open_closed_from_S1S2(s1, s2)
        e_open  = _rising_edges(opened)
        e_close = _rising_edges(closed)
        cycles = min(len(e_open), len(e_close))
        cpm = cycles * (60.0 / float(window_s)) if window_s > 0 else 0.0
        out.append({"id": aid, "cycles": cycles, "cpm": cpm, "window_s": window_s})
    return {"type": "cpm", "ts": _now_iso(), "window_s": window_s, "items": out}

def maybe_alerts_from_vibration(vib_items: List[dict], threshold: float = 0.5) -> List[dict]:
    alerts = []
    for it in vib_items:
        if float(it.get("overall", 0.0)) >= threshold:
            origin = "A1" if it.get("mpu_id") == 1 else "A2" if it.get("mpu_id") == 2 else f"MPU{it.get('mpu_id')}"
            alerts.append({
                "type": "alert", "ts": _now_iso(),
                "code": "LIMIT", "severity": 3, "origin": origin,
                "message": f"Vibration overall >= {threshold:g}",
            })
    return alerts

# =============================================================================
# Alerts cache (evita rebuild a cada request)
# =============================================================================
from hashlib import sha1

_LAST_ALERTS_PAYLOAD: dict | None = None
_LAST_ALERTS_ETAG: str | None = None
_LAST_ALERTS_TS: datetime | None = None          # quando OS ITEMS mudaram
_LAST_ALERTS_SIG: str | None = None             # assinatura (hash) só dos items

def _alerts_signature(items: list[dict]) -> str:
    """Hash estável SOMENTE do conteúdo (items), ignorando timestamps efêmeros."""
    # ordena chaves para ter determinismo
    # cuidado para não explodir com floats -> normaliza com repr
    norm = repr([{k: v for k, v in sorted(it.items())} for it in items])
    return sha1(norm.encode("utf-8")).hexdigest()

def _make_alerts_payload(items: list[dict], changed_at: datetime) -> tuple[dict, str]:
    """
    Monta payload + ETag:
    - ETag = hash(items) -> estável se conteúdo não mudou
    - ts = changed_at (ISO) -> quando o conteúdo mudou pela última vez
    """
    etag = _alerts_signature(items)
    payload = {"items": items, "ts": changed_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")}
    return payload, etag
def _make_alerts_payload_from_mon(mon: dict, limit: int = 5) -> tuple[dict, str]:
    items = maybe_alerts_from_vibration(mon.get("vibration", {}).get("items", []))
    items = items[: max(1, min(limit, 100))]
    payload = {"items": items, "ts": _now_iso()}
    body = (repr(items) + "|" + payload["ts"]).encode("utf-8")
    etag = sha1(body).hexdigest()
    return payload, etag

def _update_alerts_cache_from_mon(mon: dict) -> None:
    """
    Atualiza cache APENAS se a lista de alerts mudou.
    """
    global _LAST_ALERTS_PAYLOAD, _LAST_ALERTS_ETAG, _LAST_ALERTS_TS, _LAST_ALERTS_SIG

    items = maybe_alerts_from_vibration(mon.get("vibration", {}).get("items", []))
    items = items[:5]  # mesmo limit padrão do endpoint

    sig = _alerts_signature(items)
    if sig == _LAST_ALERTS_SIG and _LAST_ALERTS_PAYLOAD is not None:
        # nada mudou -> mantém ETag e Last-Modified/ts
        return

    changed_at = datetime.now(timezone.utc)
    payload, etag = _make_alerts_payload(items, changed_at)

    _LAST_ALERTS_SIG = sig
    _LAST_ALERTS_TS = changed_at
    _LAST_ALERTS_ETAG = etag
    _LAST_ALERTS_PAYLOAD = payload

def _get_cached_alerts(limit: int = 5) -> tuple[dict | None, str | None, datetime | None]:
    if _LAST_ALERTS_PAYLOAD is None:
        return None, None, None
    items = (_LAST_ALERTS_PAYLOAD.get("items") or [])[: max(1, min(limit, 100))]
    # IMPORTANTE: não atualizamos ts aqui — ele representa a ÚLTIMA mudança real
    payload = {"items": items, "ts": _LAST_ALERTS_PAYLOAD.get("ts")}
    return payload, _LAST_ALERTS_ETAG, _LAST_ALERTS_TS

# =============================================================================
# WS Groups / Broadcast
# =============================================================================
class WSGroup:
    def __init__(self, name: str):
        self.name = name
        self.clients: "set[WebSocket]" = set()
        self._lock = asyncio.Lock()
        self.last_hb = 0

    async def connect(self, ws: WebSocket):
        await ws.accept()
        async with self._lock:
            self.clients.add(ws)

    async def disconnect(self, ws: WebSocket):
        async with self._lock:
            self.clients.discard(ws)

    async def broadcast_json(self, payload: dict):
        dead = []
        for ws in list(self.clients):
            try:
                await ws.send_json(payload)
            except Exception:
                dead.append(ws)
        if dead:
            async with self._lock:
                for ws in dead:
                    self.clients.discard(ws)

    async def heartbeat_if_due(self):
        now = time.time() * 1000
        if now - self.last_hb >= HEARTBEAT_MS:
            await self.broadcast_json({"type": "hb", "ts": _now_iso(), "channel": self.name})
            self.last_hb = now

WS_LIVE = WSGroup("live")
WS_MON  = WSGroup("monitoring")
WS_SLOW = WSGroup("slow")

_bg_tasks: "list[asyncio.Task]" = []

# =============================================================================
# Producers
# =============================================================================
async def live_producer_loop():
    tick = max(0.02, LIVE_TICK_MS / 1000.0)
    while True:
        try:
            await WS_LIVE.broadcast_json(build_live_payload())
            await WS_LIVE.heartbeat_if_due()
        except Exception as e:
            await WS_LIVE.broadcast_json({"type": "error", "channel": "live", "detail": str(e)[:180], "ts": _now_iso()})
        await asyncio.sleep(tick)

async def monitoring_producer_loop():
    tick = max(0.2, MON_TICK_MS / 1000.0)
    while True:
        try:
            mon = build_monitoring_payload()
            _update_alerts_cache_from_mon(mon)  # atualiza cache de alerts

            await WS_MON.broadcast_json(mon)
            # Alerts simples derivados de vibração (push)
            for a in maybe_alerts_from_vibration(mon.get("vibration", {}).get("items", [])):
                await WS_SLOW.broadcast_json(a)
            await WS_MON.heartbeat_if_due()
        except Exception as e:
            await WS_MON.broadcast_json({"type": "error", "channel": "monitoring", "detail": str(e)[:180], "ts": _now_iso()})
        await asyncio.sleep(tick)

async def slow_producer_loop():
    tick = max(1.0, SLOW_TICK_MS / 1000.0)
    while True:
        try:
            await WS_SLOW.broadcast_json(build_cpm_payload(window_s=int(tick)))
            await WS_SLOW.heartbeat_if_due()
        except Exception as e:
            await WS_SLOW.broadcast_json({"type": "error", "channel": "slow", "detail": str(e)[:180], "ts": _now_iso()})
        await asyncio.sleep(tick)

# =============================================================================
# Startup / Shutdown
# =============================================================================
@app.on_event("startup")
async def _startup_ws_tasks():
    for fn in (live_producer_loop, monitoring_producer_loop, slow_producer_loop):
        _bg_tasks.append(asyncio.create_task(fn()))

@app.on_event("shutdown")
async def _shutdown_ws_tasks():
    for t in _bg_tasks:
        t.cancel()
    await asyncio.gather(*_bg_tasks, return_exceptions=True)

# =============================================================================
# Endpoints WebSocket
# =============================================================================
@app.websocket("/ws/live")
async def ws_live(ws: WebSocket):
    await WS_LIVE.connect(ws)
    try:
        while True:
            try:
                await ws.receive_text()  # ignoramos; serve só p/ detectar disconnect
            except WebSocketDisconnect:
                break
            except Exception:
                await asyncio.sleep(1.0)
    finally:
        await WS_LIVE.disconnect(ws)

@app.websocket("/ws/monitoring")
async def ws_monitoring(ws: WebSocket):
    await WS_MON.connect(ws)
    try:
        while True:
            try:
                await ws.receive_text()
            except WebSocketDisconnect:
                break
            except Exception:
                await asyncio.sleep(1.0)
    finally:
        await WS_MON.disconnect(ws)

@app.websocket("/ws/slow")
async def ws_slow(ws: WebSocket):
    await WS_SLOW.connect(ws)
    try:
        while True:
            try:
                await ws.receive_text()
            except WebSocketDisconnect:
                break
            except Exception:
                await asyncio.sleep(1.0)
    finally:
        await WS_SLOW.disconnect(ws)

# =============================================================================
# Snapshots HTTP (fallback)
# =============================================================================
@app.get("/api/live/snapshot")
def http_snapshot_live():
    return JSONResponse(build_live_payload(), headers={"Cache-Control":"no-store","Pragma":"no-cache"})

@app.get("/api/monitoring/snapshot")
def http_snapshot_monitoring():
    return JSONResponse(build_monitoring_payload(), headers={"Cache-Control":"no-store","Pragma":"no-cache"})

@app.get("/api/slow/snapshot")
def http_snapshot_slow():
    return JSONResponse(build_cpm_payload(), headers={"Cache-Control":"no-store","Pragma":"no-cache"})

# =============================================================================
# Helpers de janela/series para OPC
# =============================================================================
def _since_to_window_seconds(since: str, default_sec: int = 7200) -> int:
    """
    Converte 'since' em janela (segundos) para a query baseada em NOW() - INTERVAL.
    Aceita:
      - padrões relativos: '-10s', '-120m', '-2h', '-1d'
      - ISO/Datetime (interpreta como ts inicial -> janela = agora - ts_inicial)
      - vazio/None -> default_sec
    """
    if not since:
        return default_sec
    s = str(since).strip()
    m = _DURATION_RE.match(s)
    if m:
        val = int(m.group(1))
        unit = m.group(2).lower()
        mult = {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]
        return max(1, val * mult)
    # tenta ISO: janela = now - since
    dt = _coerce_to_datetime(s)
    if dt:
        now = datetime.now(timezone.utc)
        delta = (now - dt).total_seconds()
        if math.isfinite(delta) and delta > 0:
            return int(delta)
    return default_sec

def _rows_for_names(names: List[str], window_s: int) -> Dict[str, List[Dict[str, Any]]]:
    """Usa _fetch_series e entrega points [{ts, value}] por nome."""
    raw = _fetch_series(names, window_s)
    out: Dict[str, List[Dict[str, Any]]] = {}
    for nm, series in raw.items():
        compact = _dedup(series)
        out[nm] = [{"ts": dt_to_iso_utc(t), "value": int(v)} for (t, v) in compact if dt_to_iso_utc(t)]
    return out

# =============================================================================
# OPC Endpoints
# =============================================================================
@app.get("/api/opc/history/name")
def http_opc_history_by_name(
    name: str = Query(...),
    since: str = Query("-120m"),
    limit: int = Query(200),
    asc: bool = Query(True),
):
    return http_opc_by_name(name=name, since=since, limit=limit, asc=asc)

@app.get("/api/opc/by-name")
def http_opc_by_name(
    name: str = Query(...),
    since: str = Query("-120m"),
    limit: int = Query(200),
    asc: bool = Query(True),
):
    window_s = _parse_since_to_seconds(since, 7200)
    safe_limit = max(1, min(int(limit), MAX_LIMIT))
    series = _fetch_series([name], window_s).get(name, [])
    if not asc:
        series = list(reversed(series))
    if safe_limit and len(series) > safe_limit:
        series = series[-safe_limit:] if asc else series[:safe_limit]

    return JSONResponse({
        "name": name,
        "points": [{"ts": dt_to_iso_utc(t), "value": v} for t, v in series],
        "window_s": window_s,
        "count": len(series),
    }, headers={"Cache-Control":"no-store","Pragma":"no-cache"})

@app.get("/api/opc/query")
def http_opc_query(
    act: str | int = Query(..., description="1/2 ou A1/A2"),
    facet: str = Query("S1", pattern="^(?i:S1|S2)$"),
    since: str = Query("-120m"),
    asc: bool = Query(True),
):
    name = _resolve_signal_by_facet(act, facet)
    if not name:
        raise HTTPException(status_code=422, detail="act/facet inválidos")
    return http_opc_by_name(name=name, since=since, limit=MAX_LIMIT, asc=asc)

@app.get("/api/opc/by-facet")
def http_opc_by_facet(
    act: str | int = Query(...),
    facet: str = Query(...),
    since: str = Query("-120m"),
    asc: bool = Query(True),
):
    name = _resolve_signal_by_facet(act, facet)
    if not name:
        raise HTTPException(status_code=422, detail="act/facet inválidos")
    return http_opc_by_name(name=name, since=since, limit=MAX_LIMIT, asc=asc)

@app.get("/api/opc/history/facet")
def http_opc_history_facet(
    act: str | int = Query(...),
    facet: str = Query(...),
    since: str = Query("-120m"),
    asc: bool = Query(True),
):
    return http_opc_by_facet(act=act, facet=facet, since=since, asc=asc)

# ---------- Aliases SEM /api (compat com legado) ----------
@app.get("/opc/history/name")
def compat_opc_history_name_legacy(**kwargs): return http_opc_history_by_name(**kwargs)

@app.get("/opc/by-name")
def compat_opc_by_name_legacy(**kwargs): return http_opc_by_name(**kwargs)

@app.get("/opc/query")
def compat_opc_query_legacy(**kwargs): return http_opc_query(**kwargs)

@app.get("/opc/by-facet")
def compat_opc_by_facet_legacy(**kwargs): return http_opc_by_facet(**kwargs)

@app.get("/opc/history/facet")
def compat_opc_history_facet_legacy(**kwargs): return http_opc_history_facet(**kwargs)

# ---------- Compat POST (aceita JSON no body) ----------
@app.post("/api/opc/history/name")
def http_opc_history_by_name_post(body: dict = Body(...)):
    return http_opc_by_name_post(body)

@app.post("/api/opc/by-name")
def http_opc_by_name_post(body: dict = Body(...)):
    return http_opc_by_name(
        name=str(body.get("name")),
        since=str(body.get("since", "-120m")),
        limit=int(body.get("limit", 200)),
        asc=bool(body.get("asc", True)),
    )

# Alias adicional que seu front parece chamar:
@app.get("/api/opc/history")
def http_opc_history_alias(name: str = Query(...), since: str = "-120m", limit: int = 200, asc: bool = True):
    return http_opc_history_by_name(name=name, since=since, limit=limit, asc=asc)

@app.post("/api/opc/history")
def http_opc_history_alias_post(body: dict = Body(...)):
    return http_opc_history_by_name(
        name=str(body.get("name")),
        since=str(body.get("since", "-120m")),
        limit=int(body.get("limit", 200)),
        asc=bool(body.get("asc", True)),
    )

# =============================================================================
# Endpoints compat do live (HTTP)
# =============================================================================
@app.get("/api/live/actuators/cpm")
def compat_actuators_cpm(window_s: int = Query(60, ge=10, le=600)):
    payload = build_cpm_payload(window_s=window_s)
    items = payload.get("items", [])
    return JSONResponse(
        {
            "ts": payload.get("ts"),
            "actuators": [
                {
                    "id": int(it.get("id")),
                    "window_s": int(it.get("window_s", window_s)),
                    "cycles": int(it.get("cycles", 0)),
                    "cpm": float(it.get("cpm", 0.0)),
                }
                for it in items
            ],
        },
        headers={"Cache-Control": "no-store", "Pragma": "no-cache"},
    )

@app.get("/api/live/actuators/timings")
def compat_actuators_timings():
    mon = build_monitoring_payload()
    return JSONResponse(
        {
            "ts": mon.get("ts"),
            "actuators": [
                {
                    "actuator_id": int(t.get("actuator_id")),
                    "last": {
                        "ts_utc": mon.get("ts"),
                        "dt_abre_s": t.get("last", {}).get("dt_abre_s"),
                        "dt_fecha_s": t.get("last", {}).get("dt_fecha_s"),
                        "dt_ciclo_s": t.get("last", {}).get("dt_ciclo_s"),
                    },
                }
                for t in (mon.get("timings") or [])
            ],
        },
        headers={"Cache-Control": "no-store", "Pragma": "no-cache"},
    )

@app.get("/api/live/vibration")
def compat_live_vibration(window_s: int = Query(2, ge=1, le=60)):
    now = datetime.now(timezone.utc)
    start = now - timedelta(seconds=window_s)
    rows = fetch_all(
        f"SELECT mpu_id, ts_utc, ax_g, ay_g, az_g FROM {MPU_TABLE} WHERE ts_utc >= %s AND ts_utc < %s",
        (start, now),
    )
    by: Dict[int, Dict[str, List[float]]] = {}
    ts_by: Dict[int, List[datetime]] = {}
    for r in rows:
        mid = int(col(r, "mpu_id") or r[0])
        ax = col(r, "ax_g"); ay = col(r, "ay_g"); az = col(r, "az_g")
        by.setdefault(mid, {"ax": [], "ay": [], "az": []})
        ts_by.setdefault(mid, [])
        by[mid]["ax"].append(float(ax or 0.0))
        by[mid]["ay"].append(float(ay or 0.0))
        by[mid]["az"].append(float(az or 0.0))
        ts_by[mid].append(_coerce_to_datetime(col(r, "ts_utc") or r[1]) or now)

    items = []
    for mid, d in by.items():
        rms_ax = (sum(v*v for v in d["ax"]) / max(1, len(d["ax"]))) ** 0.5
        rms_ay = (sum(v*v for v in d["ay"]) / max(1, len(d["ay"]))) ** 0.5
        rms_az = (sum(v*v for v in d["az"]) / max(1, len(d["az"]))) ** 0.5
        overall = float((rms_ax**2 + rms_ay**2 + rms_az**2) ** 0.5)
        ts_list = ts_by.get(mid) or [start, now]
        items.append({
            "mpu_id": mid,
            "ts_start": dt_to_iso_utc(min(ts_list)),
            "ts_end": dt_to_iso_utc(max(ts_list)),
            "rms_ax": float(rms_ax),
            "rms_ay": float(rms_ay),
            "rms_az": float(rms_az),
            "overall": overall,
        })

    return JSONResponse({"items": items}, headers={"Cache-Control":"no-store","Pragma":"no-cache"})

# =============================================================================
# Alerts (com cache + ETag/304)
# =============================================================================
@app.get("/alerts")
@app.get("/api/alerts")
def compat_alerts(
    request: Request,
    limit: int = Query(5, ge=1, le=100),
    max_age_s: int = Query(1, ge=0, le=60),
):
    """
    Retorna alerts do cache preenchido pelo monitoring loop.
    - Usa ETag/If-None-Match e Last-Modified/If-Modified-Since para 304.
    - Cache-Control com max-age configurável (padrão 1s).
    """
    payload, etag, last_ts = _get_cached_alerts(limit=limit)
    if payload is None:
        # fallback (primeiro hit após start, antes do primeiro tick do loop)
        mon = build_monitoring_payload()
        _update_alerts_cache_from_mon(mon)
        payload, etag, last_ts = _get_cached_alerts(limit=limit)

    # Condicional por ETag
    inm = request.headers.get("if-none-match")
    if etag and inm and inm.strip('"') == etag:
        return Response(status_code=304)

    # Condicional por Last-Modified
    ims = request.headers.get("if-modified-since")
    if ims and last_ts:
        try:
            ims_dt = _coerce_to_datetime(ims)
            if ims_dt and ims_dt >= last_ts.replace(microsecond=0):
                return Response(status_code=304)
        except Exception:
            pass

    headers = {"Cache-Control": f"public, max-age={max_age_s}"}
    if etag:
        headers["ETag"] = f'"{etag}"'
    if last_ts:
        headers["Last-Modified"] = last_ts.strftime("%a, %d %b %Y %H:%M:%S GMT")

    return JSONResponse(payload, headers=headers)

# =============================================================================
# Health (mínimo e útil para telemetry do front)
# =============================================================================
@app.get("/health")
def health():
    row = fetch_one("SELECT NOW(6) AS now6")
    db_time_val = col(row, "now6") if isinstance(row, dict) else (row[0] if row else None)
    now_ms = int(time.time() * 1000)
    runtime_ms = now_ms - PROCESS_STARTED_MS
    started_at_iso = datetime.fromtimestamp(PROCESS_STARTED_MS / 1000, tz=timezone.utc).isoformat()
    server_time_iso = datetime.now(tz=timezone.utc).isoformat()
    return {
        "status": "ok",
        "runtime_ms": runtime_ms,
        "started_at": started_at_iso,
        "server_time": server_time_iso,
        "db_time": str(db_time_val) if db_time_val is not None else None,
    }

@app.get("/api/health")
def api_health():
    return health()

# =============================================================================
# Simulation (catalog + draw)
# =============================================================================
from json import loads as _json_loads

@app.get("/api/simulation/catalog")
def http_simulation_catalog():
    """
    Retorna o catálogo de erros para popular o <select> do front.
    Base: tabela error_catalog (id, code, name, grp, severity, ...).
    """
    rows = fetch_all("""
        SELECT id, code, name, grp, COALESCE(severity, 0) AS severity
        FROM error_catalog
        ORDER BY grp, code
    """)
    items = []
    for r in rows:
        items.append({
            "id": int(col(r, "id") or r[0]),
            "code": str(col(r, "code") or r[1]),
            "name": str(col(r, "name") or r[2]),
            "grp":  str(col(r, "grp")  or r[3] or "SISTEMA"),
            "label": f'{col(r,"code") or r[1]} — {col(r,"name") or r[2]}',
            "severity": int(col(r, "severity") or r[4] or 0),
        })
    return JSONResponse({"items": items}, headers={"Cache-Control":"no-store","Pragma":"no-cache"})

@app.post("/api/simulation/draw")
def http_simulation_draw(body: dict = Body(...)):
    """
    Gera um cenário a partir de um code do catálogo.
    Payload esperado pelo front:
      { scenario_id, actuator, error{...}, cause, actions[], params{}, ui{}, resume_allowed }
    """
    mode = str(body.get("mode", "by_code"))
    code = str(body.get("code") or "").strip()

    if mode != "by_code" or not code:
        raise HTTPException(status_code=422, detail="Use {mode:'by_code', code:'...'}")

    row = fetch_one("""
        SELECT id, code, name, grp, COALESCE(severity, 0) AS severity, default_actions, description
        FROM error_catalog
        WHERE code = %s
        LIMIT 1
    """, (code,))
    if not row:
        raise HTTPException(status_code=404, detail=f"code '{code}' não encontrado no catálogo")

    err_id = int(col(row, "id") or row[0])
    err_code = str(col(row, "code") or row[1])
    err_name = str(col(row, "name") or row[2])
    err_grp  = str(col(row, "grp")  or row[3] or "SISTEMA")
    err_sev  = int(col(row, "severity") or row[4] or 0)
    raw_actions = col(row, "default_actions") if isinstance(row, dict) else row[5]
    desc = (col(row, "description") if isinstance(row, dict) else row[6]) or ""

    # default_actions pode estar como JSON (["a","b"]) ou texto
    actions: list[str] = []
    if raw_actions:
        try:
            parsed = _json_loads(raw_actions)
            if isinstance(parsed, list):
                actions = [str(x) for x in parsed if isinstance(x, (str, int, float))]
        except Exception:
            pass
    if not actions:
        # fallback: separa por vírgula
        actions = [p.strip() for p in str(raw_actions or "").split(",") if p.strip()]

    # escolhe um atuador (heurística simples pelo grupo)
    actuator = 1 if err_grp.upper().startswith("MPU") else 2

    payload = {
        "scenario_id": f"{err_code}-{int(time.time()*1000)%100000}",
        "actuator": 1 if actuator == 1 else 2,
        "error": {
            "id": err_id,
            "code": err_code,
            "name": err_name,
            "grp": err_grp,
            "severity": err_sev,
        },
        # causa básica (você pode refinar usando error_scenarios no futuro)
        "cause": desc or f"Cenário simulado para {err_code}",
        "actions": actions or ["Parada controlada", "Inspecionar equipamento"],
        "params": {
            "seed": int(time.time() * 1000) % 1_000_000,
            "code": err_code,
        },
        # flags lidas pelo front pra pausar 3D/mostrar popup etc
        "ui": {
            "halt_sim": True,               # pausar a “lógica” da tela
            "halt_3d": (err_sev >= 4),      # pausa 3D em severidade alta
            "show_popup": True,
        },
        # se pode retomar (depende da severidade, por ex.)
        "resume_allowed": (err_sev <= 3),
    }
    return JSONResponse(payload, headers={"Cache-Control":"no-store","Pragma":"no-cache"})

# ---------- aliases sem /api (opcional) ----------
@app.get("/simulation/catalog")
def compat_sim_catalog(): return http_simulation_catalog()

@app.post("/simulation/draw")
def compat_sim_draw(body: dict = Body(...)): return http_simulation_draw(body)

