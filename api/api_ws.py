# api/api_ws.py
import os
import re
import time
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Any, Dict, Tuple

from dotenv import load_dotenv, find_dotenv
from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from mysql.connector.errors import PoolError

# -----------------------------------------------------------------------------
# .env
# -----------------------------------------------------------------------------
load_dotenv(find_dotenv())
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

# -----------------------------------------------------------------------------
# FastAPI + CORS
# -----------------------------------------------------------------------------
app = FastAPI(title="Festo DT API (HTTP only)", version="3.0.0")

ALLOWED_ORIGINS = [
    o.strip()
    for o in os.getenv(
        "ALLOWED_ORIGINS",
        "http://localhost:8080,http://127.0.0.1:8080"
    ).split(",")
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
    headers["Access-Control-Allow-Methods"] = request.headers.get(
        "access-control-request-method", "*"
    ) or "*"
    req_headers = request.headers.get("access-control-request-headers")
    headers["Access-Control-Allow-Headers"] = req_headers or "*"
    return Response(status_code=204, headers=headers)

@app.exception_handler(PoolError)
async def pool_error_handler(_, __):
    return JSONResponse(status_code=503, content={"detail": "DB pool exhausted"})

# -----------------------------------------------------------------------------
# Imports locais
# -----------------------------------------------------------------------------
from .database import get_db
try:
    # usado só para /metrics/minute-agg (Analytics)
    from .services.analytics import get_kpis  # type: ignore
except Exception:  # fallback se serviço não existir
    def get_kpis():
        return {}

# -----------------------------------------------------------------------------
# DB helpers
# -----------------------------------------------------------------------------
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

def first_col(row: Any) -> Any:
    if row is None: return None
    if isinstance(row, (list, tuple)): return row[0]
    if isinstance(row, dict):
        for _, v in row.items(): return v
    return row

def col(row: Any, key_or_index: Any) -> Any:
    if row is None: return None
    if isinstance(row, dict): return row.get(key_or_index)
    if isinstance(key_or_index, int): return row[key_or_index]
    return None

def cols(row: Any, *keys_or_idx: Any) -> Tuple[Any, ...]:
    return tuple(col(row, k) for k in keys_or_idx)

def table_exists(schema: str, table: str) -> bool:
    q = """
    SELECT COUNT(*) AS c
    FROM information_schema.tables
    WHERE table_schema=%s AND table_name=%s
    """
    row = fetch_one(q, (schema, table))
    v = first_col(row)
    try: return int(v) > 0
    except Exception: return False

def _coerce_to_datetime(value: Any) -> Optional[datetime]:
    if value is None: return None
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
                    dt = datetime.strptime(s, fmt)
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
    if dt is None: return None
    return dt.isoformat().replace("+00:00", "Z")

# -----------------------------------------------------------------------------
# Cache curtinho
# -----------------------------------------------------------------------------
_cache: Dict[str, Dict[str, Any]] = {}
def cached(key: str, ttl: float, producer):
    now = time.time()
    e = _cache.get(key)
    if e and now - e["ts"] < ttl:
        return e["val"]
    val = producer()
    _cache[key] = {"ts": now, "val": val}
    return val

# -----------------------------------------------------------------------------
# Helpers de janela e IDs
# -----------------------------------------------------------------------------
def _normalize_since(since: Optional[str], last: Optional[int], seconds: Optional[str],
                     window: Optional[str], duration: Optional[str]) -> Optional[str]:
    def with_s(val: str) -> str:
        v = val.strip().lower()
        if not v: return v
        if not v.startswith(("+", "-")): v = "-" + v
        if v.lstrip("+-").isdigit(): v += "s"
        return v
    if since:
        s = since.strip().lower()
        if s.lstrip("+-").isdigit(): return with_s(s)
        return s
    for cand in (seconds, window, duration):
        if cand: return with_s(cand)
    if last is not None:
        try:
            val = int(last)
            if val > 0: val = -val
            return f"{val}s"
        except Exception:
            pass
    return None

def _parse_since(s: Optional[str]) -> Optional[datetime]:
    if not s: return None
    s = s.strip().lower()
    m = re.fullmatch(r"([+-]?\d+)([smhd])", s)
    if m:
        qty = int(m.group(1)); unit = m.group(2); now = datetime.now(timezone.utc)
        if unit == "s": return now + timedelta(seconds=qty)
        if unit == "m": return now + timedelta(minutes=qty)
        if unit == "h": return now + timedelta(hours=qty)
        if unit == "d": return now + timedelta(days=qty)
    try:
        dt = datetime.fromisoformat(s.replace("z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        raise HTTPException(400, detail="since inválido. Use '-60s', '-15m', '-1h', '-7d' ou ISO8601.")

def _coerce_mpu_id(id_: Optional[str], mpu: Optional[str], device: Optional[str], name: Optional[str]) -> int:
    cand = None
    for v in (id_, mpu, device, name):
        if v: cand = v; break
    if cand is None:
        raise HTTPException(400, "Informe id=MPUA1|MPUA2 (ou mpu|device|name)")
    s = str(cand).strip().upper()
    if s in ("1", "MPU1", "MPUA1", "A1"): return 1
    if s in ("2", "MPU2", "MPUA2", "A2"): return 2
    raise HTTPException(400, "MPU inválido (use 1,2, MPUA1, MPUA2)")

def _coerce_act_from_path(act_raw: str) -> int:
    s = (act_raw or "").strip().upper()
    if s in ("1", "A1", "MPUA1", "MPU1"): return 1
    if s in ("2", "A2", "MPUA2", "MPU2"): return 2
    raise HTTPException(400, "act inválido (use 1|2|A1|A2)")

def _coerce_facet_from_path(facet_raw: str) -> str:
    s = (facet_raw or "").strip().upper()
    if s not in ("S1", "S2"):
        raise HTTPException(400, "facet inválido (use S1|S2)")
    return s

# -----------------------------------------------------------------------------
# Latch config (nomes reais dos teus sinais)
# -----------------------------------------------------------------------------
from dataclasses import dataclass

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

@dataclass
class _LatchState:
    displayed: StableState = "RECUADO"
    pending_target: Optional[str] = None
    fault: str = "NONE"
    started_at: Optional[datetime] = None
    elapsed_ms: int = 0

class _Deb:
    __slots__ = ("v","t")
    def __init__(self):
        self.v = False
        self.t = datetime.min.replace(tzinfo=timezone.utc)

def _stable(prev:_Deb, now_v:bool, ts:datetime, debounce_ms:int):
    if now_v != prev.v:
        prev.v = now_v
        prev.t = ts
        return False, now_v
    return ((ts - prev.t).total_seconds()*1000 >= debounce_ms), prev.v

def _compute_latched_state(
    cfg: _LatchCfg,
    rows: List[Tuple[datetime,str,Optional[int]]],
    initial_displayed: StableState = "RECUADO",
) -> _LatchState:
    st = _LatchState(displayed=initial_displayed)
    rec = _Deb(); adv = _Deb()
    for ts, name, vb in rows:
        if vb is None: continue
        v = bool(vb)

        if name == cfg.s_rec: _stable(rec, v, ts, cfg.debounce_ms)
        if name == cfg.s_adv: _stable(adv, v, ts, cfg.debounce_ms)

        rec_st, rec_val = _stable(rec, rec.v, ts, cfg.debounce_ms)
        adv_st, adv_val = _stable(adv, adv.v, ts, cfg.debounce_ms)

        if rec_st and adv_st and rec_val and adv_val:
            st.fault = "FAULT_SENSORS_CONFLICT"

        if name == cfg.v_av and v:
            st.pending_target = "AV";  st.started_at = ts; st.fault = "NONE"
        elif name == cfg.v_rec and v:
            st.pending_target = "REC"; st.started_at = ts; st.fault = "NONE"

        if not st.pending_target:
            if rec_st and rec_val and not (adv_st and adv_val):
                st.displayed = "RECUADO"
            elif adv_st and adv_val and not (rec_st and rec_val):
                st.displayed = "AVANÇADO"

        if st.pending_target:
            if st.started_at:
                st.elapsed_ms = int((ts - st.started_at).total_seconds()*1000)
                if st.elapsed_ms > int(cfg.expected_ms * cfg.timeout_factor):
                    st.fault = "FAULT_TIMEOUT"

            if st.pending_target == "AV" and adv_st and adv_val:
                st.displayed = "AVANÇADO"; st.pending_target = None; st.started_at = None; st.elapsed_ms = 0; st.fault = "NONE"
            elif st.pending_target == "REC" and rec_st and rec_val:
                st.displayed = "RECUADO";  st.pending_target = None; st.started_at = None; st.elapsed_ms = 0; st.fault = "NONE"
    return st

# --- Latch config (corrigido: V*_14 = AVANÇAR, V*_12 = RECUAR) ----------------
_CFG_A1 = _LatchCfg(
    id="A1",
    expected_ms=1500,
    debounce_ms=80,
    timeout_factor=1.5,
    v_av="V1_14",             # <-- era V1_12; AGORA correto: abrir/avançar
    v_rec="V1_12",            # <-- era V1_14; AGORA correto: fechar/recuar
    s_adv="Recuado_1S1",
    s_rec="Avancado_1S2",
)

_CFG_A2 = _LatchCfg(
    id="A2",
    expected_ms=500,
    debounce_ms=80,
    timeout_factor=1.5,
    v_av="V2_14",             # <-- era V2_12
    v_rec="V2_12",            # <-- era V2_14
    s_adv="Recuado_2S1",
    s_rec="Avancado_2S2",
)

_NAMES_LATCH = (_CFG_A1.v_av, _CFG_A1.v_rec, _CFG_A1.s_adv, _CFG_A1.s_rec,
                _CFG_A2.v_av, _CFG_A2.v_rec, _CFG_A2.s_adv, _CFG_A2.s_rec)

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def _fetch_latest_rows(names: Tuple[str, ...]) -> Dict[str, Dict[str, Any]]:
    if not names:
        return {}
    placeholders = ", ".join(["%s"] * len(names))
    try:
        sql = f"""
        WITH latest AS (
          SELECT name, value_bool, ts_utc,
                 ROW_NUMBER() OVER (PARTITION BY name ORDER BY ts_utc DESC) AS rn
          FROM opc_samples
          WHERE name IN ({placeholders})
        )
        SELECT name, value_bool, ts_utc
        FROM latest
        WHERE rn = 1
        """
        rows = fetch_all(sql, names)
    except Exception:
        sql = f"""
        SELECT s.name, s.value_bool, s.ts_utc
        FROM opc_samples s
        JOIN (
            SELECT name, MAX(ts_utc) AS ts_utc
            FROM opc_samples
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
    fault = "NONE"
    if adv_v and rec_v:
        displayed = "RECUADO"; fault = "FAULT_SENSORS_CONFLICT"
    elif adv_v:
        displayed = "AVANÇADO"
    elif rec_v:
        displayed = "RECUADO"

    pending = None
    started_at = None
    elapsed_ms = 0

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

    if pending and started_at:
        elapsed_ms = max(0, int((now - started_at).total_seconds() * 1000))
        if elapsed_ms > int(cfg.expected_ms * cfg.timeout_factor):
            fault = "FAULT_TIMEOUT"

    return {
        "state": displayed,
        "pending": pending,
        "fault": fault,
        "elapsed_ms": elapsed_ms,
        "started_at": (started_at.isoformat() if started_at else None),
    }

# -----------------------------------------------------------------------------
# Health
# -----------------------------------------------------------------------------
@app.get("/health")
def health():
    row = fetch_one("SELECT NOW(6) AS now6")
    db_time_val = first_col(row) if row is not None else None
    return {"status": "ok", "db_time": (str(db_time_val) if db_time_val is not None else None)}

@app.get("/api/health")
def api_health():
    return health()

# -----------------------------------------------------------------------------
# OPC history (para CPM por bordas, etc.)
# -----------------------------------------------------------------------------
@app.get("/opc/history")
def opc_history(
    request: Request,
    name: Optional[str] = Query(None),
    act: Optional[str] = Query(None, description="1|2|A1|A2"),
    facet: Optional[str] = Query(None, description="S1|S2"),
    last: Optional[str] = Query(None),
    since: Optional[str] = Query(None),
    seconds: Optional[str] = Query(None),
    window: Optional[str] = Query(None),
    duration: Optional[str] = Query(None),
    limit: int = Query(20000, ge=1, le=20000),
    offset: int = Query(0, ge=0),
    asc: bool = Query(False),
):
    act_int: Optional[int] = None
    facet_norm: Optional[str] = None

    if act is not None:
        act_int = _coerce_act_from_path(str(act))
    if facet is not None:
        facet_norm = _coerce_facet_from_path(str(facet))

    if not name and act_int and facet_norm:
        name = f"Recuado_{act_int}S1" if facet_norm == "S1" else f"Avancado_{act_int}S2"
    if not name:
        raise HTTPException(400, "Informe name=... ou act=..&facet=..")

    qp = request.query_params
    alt_since = qp.get("from") or qp.get("start") or qp.get("begin") or None
    alt_scalar = qp.get("period") or qp.get("seconds") or qp.get("duration") or None

    last_int: Optional[int] = None
    if last is not None:
        s = str(last).strip().lower()
        if s.endswith("s"): s = s[:-1]
        try: last_int = int(s)
        except Exception: last_int = None

    norm = _normalize_since(
        since=since or alt_since,
        last=last_int,
        seconds=seconds or alt_scalar,
        window=window,
        duration=duration,
    )
    dt = _parse_since(norm) if norm else None

    sql = """
        SELECT ts_utc AS ts_utc, value_bool AS value_bool
        FROM opc_samples
        WHERE name=%s
    """
    params: Tuple[Any, ...] = (name,)
    if dt:
        sql += " AND ts_utc >= %s"
        params = (name, dt)
    sql += f" ORDER BY ts_utc {'ASC' if asc else 'DESC'} LIMIT %s OFFSET %s"
    params += (limit, offset)

    rows = fetch_all(sql, params)
    items: List[Dict[str, Any]] = [{
        "ts_utc": dt_to_iso_utc(col(r, "ts_utc") if isinstance(r, dict) else r[0]),
        "value_bool": (None if (col(r, "value_bool") if isinstance(r, dict) else r[1]) is None
                       else int(col(r, "value_bool") if isinstance(r, dict) else r[1])),
    } for r in rows]

    # retorna como objeto com items (teu front também aceita esse shape)
    return {"name": name, "count": len(items), "items": items}

@app.get("/opc/history/{act}/{facet}")
def opc_history_by_path(
    request: Request,
    act: str,
    facet: str,
    **kwargs
):
    act_int = _coerce_act_from_path(act)
    facet_str = _coerce_facet_from_path(facet)
    return opc_history(request=request, name=None, act=str(act_int), facet=facet_str, **kwargs)

@app.get("/api/opc/history")
def api_opc_history(**kwargs):
    return opc_history(**kwargs)

# -----------------------------------------------------------------------------
# MPU: ids + history (para Analytics / vibração por histórico)
# -----------------------------------------------------------------------------
@app.get("/mpu/ids")
def mpu_ids():
    rows = fetch_all("SELECT DISTINCT mpu_id AS mid FROM mpu_samples ORDER BY mpu_id ASC")
    def id_to_str(i: int) -> str:
        return "MPUA1" if i == 1 else "MPUA2" if i == 2 else f"MPU{i}"
    ids = []
    for r in rows:
        mid = col(r, "mid")
        if mid is None: mid = col(r, 0)
        ids.append(id_to_str(int(mid)))
    return {"ids": ids}

@app.get("/api/mpu/ids")
def api_mpu_ids():
    return mpu_ids()

@app.get("/mpu/history")
def mpu_history(
    id: Optional[str] = Query(None, description="MPUA1|MPUA2|1|2"),
    mpu: Optional[str] = Query(None),
    device: Optional[str] = Query(None),
    name: Optional[str] = Query(None),
    since: Optional[str] = Query(None),
    last: Optional[int] = Query(None),
    seconds: Optional[str] = Query(None),
    window: Optional[str] = Query(None),
    duration: Optional[str] = Query(None),
    limit: int = Query(1000, ge=1, le=20000),
    offset: int = Query(0, ge=0),
    asc: bool = Query(False),
):
    mid = _coerce_mpu_id(id, mpu, device, name)
    norm = _normalize_since(since, last, seconds, window, duration)
    dt = _parse_since(norm) if norm else None

    sql = """
        SELECT ts_utc AS ts_utc, ax_g, ay_g, az_g, gx_dps, gy_dps, gz_dps
        FROM mpu_samples
        WHERE mpu_id=%s
    """
    params: Tuple[Any, ...] = (mid,)
    if dt:
        sql += " AND ts_utc >= %s"
        params = (mid, dt)
    sql += f" ORDER BY ts_utc {'ASC' if asc else 'DESC'} LIMIT %s OFFSET %s"
    params += (limit, offset)

    rows = fetch_all(sql, params)
    items = []
    for r in rows:
        items.append({
            "ts_utc": dt_to_iso_utc(r["ts_utc"]),
            "ax_g": r["ax_g"], "ay_g": r["ay_g"], "az_g": r["az_g"],
            "gx_dps": r["gx_dps"], "gy_dps": r["gy_dps"], "gz_dps": r["gz_dps"],
        })
    return {"id": ("MPUA1" if mid == 1 else "MPUA2"), "count": len(items), "items": items}

@app.get("/api/mpu/history")
def api_mpu_history(**kwargs):
    return mpu_history(**kwargs)

# -----------------------------------------------------------------------------
# Live: atuadores / ciclos / vibração / timings
# -----------------------------------------------------------------------------
def _live_actuators_state_data_window(since_ms: int = 12000) -> Dict[str, Any]:
    rows = fetch_all(
        """
        SELECT ts_utc, name, value_bool
        FROM opc_samples
        WHERE name IN (%s,%s,%s,%s,%s,%s,%s,%s)
          AND ts_utc >= NOW(6) - INTERVAL %s MICROSECOND
        ORDER BY ts_utc ASC, id ASC
        """,
        (*_NAMES_LATCH, since_ms * 1000)
    )

    def norm_row(r):
        if isinstance(r, dict): return (r["ts_utc"], r["name"], r["value_bool"])
        return (r[0], r[1], r[2])

    a1_rows = [norm_row(r) for r in rows if ((r["name"] if isinstance(r,dict) else r[1]) in (_CFG_A1.v_av, _CFG_A1.v_rec, _CFG_A1.s_adv, _CFG_A1.s_rec))]
    a2_rows = [norm_row(r) for r in rows if ((r["name"] if isinstance(r,dict) else r[1]) in (_CFG_A2.v_av, _CFG_A2.v_rec, _CFG_A2.s_adv, _CFG_A2.s_rec))]

    st1 = _compute_latched_state(_CFG_A1, a1_rows, initial_displayed="RECUADO")
    st2 = _compute_latched_state(_CFG_A2, a2_rows, initial_displayed="RECUADO")

    return {
        "ts": _now_iso(),
        "actuators": [
            {"actuator_id": 1, "state": st1.displayed, "pending": st1.pending_target,
             "fault": st1.fault, "elapsed_ms": st1.elapsed_ms,
             "started_at": (st1.started_at.isoformat() if st1.started_at else None)},
            {"actuator_id": 2, "state": st2.displayed, "pending": st2.pending_target,
             "fault": st2.fault, "elapsed_ms": st2.elapsed_ms,
             "started_at": (st2.started_at.isoformat() if st2.started_at else None)},
        ]
    }

def _live_actuators_state_data_fast() -> Dict[str, Any]:
    names = (
        _CFG_A1.v_av, _CFG_A1.v_rec, _CFG_A1.s_adv, _CFG_A1.s_rec,
        _CFG_A2.v_av, _CFG_A2.v_rec, _CFG_A2.s_adv, _CFG_A2.s_rec
    )
    latest = _fetch_latest_rows(names)
    a1 = _infer_state_from_latest(_CFG_A1, latest)
    a2 = _infer_state_from_latest(_CFG_A2, latest)
    return {
        "ts": _now_iso(),
        "actuators": [
            {"actuator_id": 1, **a1},
            {"actuator_id": 2, **a2},
        ]
    }

@app.get("/api/live/actuators/state", tags=["Live"])
def live_actuators_state(since_ms: int = 12000):
    # caminho rápido (último por nome)
    try:
        data = _live_actuators_state_data_fast()
        return JSONResponse(content=data, headers={"Cache-Control": "no-store", "Pragma": "no-cache"})
    except Exception:
        # fallback por janela curta
        data = _live_actuators_state_data_window(since_ms)
        return JSONResponse(content=data, headers={"Cache-Control": "no-store", "Pragma": "no-cache"})

_cycle_fsm: Dict[int, Dict[str, Any]] = {
    1: {"baseline": None, "last": None, "seen_other": False, "count": 0, "since_ts": time.time()},
    2: {"baseline": None, "last": None, "seen_other": False, "count": 0, "since_ts": time.time()},
}

def _normalize_state(s: Any) -> Optional[str]:
    if not s: return None
    u = str(s).upper()
    if "RECU" in u: return "RECUADO"
    if "AVAN" in u: return "AVANÇADO"
    return None

def _update_cycle_fsm_from_state(aid: int, state: Optional[str]) -> None:
    if aid not in _cycle_fsm: return
    f = _cycle_fsm[aid]
    if state not in ("RECUADO", "AVANÇADO"):
        f["last"] = state
        return
    if f["baseline"] is None:
        f["baseline"] = state
        f["last"] = state
        f["seen_other"] = False
        f["since_ts"] = f.get("since_ts", time.time())
        return
    last = f["last"]
    if state != last and state != f["baseline"]:
        f["seen_other"] = True
    if state == f["baseline"] and f.get("seen_other"):
        f["count"] = int(f.get("count", 0)) + 1
        f["seen_other"] = False
    f["last"] = state

@app.get("/api/live/cycles/total", tags=["Live"])
def live_cycles_total():
    def _impl():
        states = _live_actuators_state_data_fast().get("actuators", [])
        for it in states:
            aid = int(it.get("actuator_id") or it.get("id"))
            s = _normalize_state(it.get("state"))
            if aid in (1, 2): _update_cycle_fsm_from_state(aid, s)

        out = []; total = 0
        for aid in (1, 2):
            f = _cycle_fsm[aid]; c = int(f.get("count", 0)); total += c
            out.append({
                "actuator_id": aid,
                "cycles": c,
                "baseline": f.get("baseline"),
                "last_state": f.get("last"),
                "seen_other": f.get("seen_other"),
                "since": dt_to_iso_utc(datetime.fromtimestamp(f.get("since_ts", time.time()), tz=timezone.utc)),
            })
        return {"total": total, "actuators": out, "ts": _now_iso()}
    return cached("live_cycles_total", 0.05, _impl)

@app.get("/api/live/vibration", tags=["Live"])
def live_vibration(window_s: int = Query(2, ge=1, le=60)):
    def _impl():
        now = datetime.now(timezone.utc)
        start = now - timedelta(seconds=window_s)
        rows = fetch_all(
            """
            SELECT mpu_id AS mpu_id, ts_utc AS ts_utc, ax_g, ay_g, az_g
            FROM mpu_samples
            WHERE ts_utc >= %s AND ts_utc < %s
            """,
            (start, now),
        )
        if not rows:
            return {"items": []}
        by: Dict[int, Dict[str, List[Any]]] = {}
        for r in rows:
            mpu_id = int(col(r, "mpu_id") or col(r, 0))
            ts = col(r, "ts_utc") or col(r, 1)
            ax = col(r, "ax_g"); ay = col(r, "ay_g"); az = col(r, "az_g")
            d = by.setdefault(mpu_id, {"ax": [], "ay": [], "az": [], "ts": []})
            d["ax"].append(0.0 if ax is None else float(ax))
            d["ay"].append(0.0 if ay is None else float(ay))
            d["az"].append(0.0 if az is None else float(az))
            d["ts"].append(ts)
        items = []
        for mpu_id, d in by.items():
            rms_ax = (sum(v*v for v in d["ax"]) / max(1, len(d["ax"]))) ** 0.5
            rms_ay = (sum(v*v for v in d["ay"]) / max(1, len(d["ay"]))) ** 0.5
            rms_az = (sum(v*v for v in d["az"]) / max(1, len(d["az"]))) ** 0.5
            overall = float((rms_ax**2 + rms_ay**2 + rms_az**2) ** 0.5)
            items.append({
                "mpu_id": mpu_id,
                "ts_start": dt_to_iso_utc(min(d["ts"])),
                "ts_end": dt_to_iso_utc(max(d["ts"])),
                "rms_ax": float(rms_ax), "rms_ay": float(rms_ay), "rms_az": float(rms_az),
                "overall": overall,
            })
        return {"items": items}
    return cached(f"live_vibration:{window_s}", 0.05, _impl)

@app.get("/api/live/actuators/timings", tags=["Live"])
def live_actuator_timings():
    DB_NAME = os.getenv("MYSQL_DB") or os.getenv("DB_NAME") or ""
    def last_from_table(tname: str):
        if not DB_NAME or not table_exists(DB_NAME, tname):
            return None
        row = fetch_one(
            f"""
            SELECT ts_utc AS ts_utc, dt_abre_s AS dt_abre_s, dt_fecha_s AS dt_fecha_s, dt_ciclo_s AS dt_ciclo_s
            FROM {tname}
            ORDER BY ts_utc DESC
            LIMIT 1
            """
        )
        if not row: return None
        return {
            "ts_utc": dt_to_iso_utc(col(row, "ts_utc") or col(row, 0)),
            "dt_abre_s": col(row, "dt_abre_s"),
            "dt_fecha_s": col(row, "dt_fecha_s"),
            "dt_ciclo_s": col(row, "dt_ciclo_s"),
        }
    out = []
    r1 = last_from_table("cycles_atuador1")
    out.append({"actuator_id": 1, "last": (r1 or {"ts_utc": None, "dt_abre_s": None, "dt_fecha_s": None, "dt_ciclo_s": None})})
    r2 = last_from_table("cycles_atuador2")
    out.append({"actuator_id": 2, "last": (r2 or {"ts_utc": None, "dt_abre_s": None, "dt_fecha_s": None, "dt_ciclo_s": None})})
    return {"actuators": out}

# -----------------------------------------------------------------------------
# Analytics: minuto agregado (CPM/tempos/runtime)
# -----------------------------------------------------------------------------
@app.get("/metrics/minute-agg")
def metrics_minute_agg(act: str = Query("A1"), since: str = Query("-60m")):
    """
    Retorna **array** de agregações por minuto (o front espera `MinuteAgg[]`).
    """
    try:
        data = get_kpis() or {}
        items = data.get("minute_agg", [])
        # se vier no shape {"items":[...]} ou outro, normaliza
        if isinstance(items, dict) and "items" in items:
            items = items["items"]
        if isinstance(items, list):
            return items
        return []
    except Exception:
        return []

# --- ADD: router Simulation ---------------------------------------------------
try:
    from .routes import simulation  # novo arquivo abaixo
    app.include_router(simulation.router)
except Exception as e:
    print(f"[simulation] router não carregado: {e}")
# -----------------------------------------------------------------------------

# --- ADD: router Alerts -------------------------------------------------------
try:
    from .routes import alerts as alerts_route
    app.include_router(alerts_route.router)
except Exception as e:
    print(f"[alerts] router não carregado: {e}")
# -----------------------------------------------------------------------------

