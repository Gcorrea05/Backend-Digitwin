# api/api_ws.py
import os
import re
import time
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Any, Dict, Tuple
from dataclasses import dataclass

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
app = FastAPI(title="Festo DT API (HTTP only)", version="3.1.0")

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
PROCESS_STARTED_MS = int(time.time() * 1000)  # instante em que o processo subiu (epoch ms)


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
# Latch config (mantidas EXATAMENTE como você pediu)
# -----------------------------------------------------------------------------
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

# --- Latch config (mantido EXATAMENTE como tinha) -----------------------------
_CFG_A1 = _LatchCfg(
    id="A1",
    expected_ms=1500,
    debounce_ms=80,
    timeout_factor=1.5,
    v_av="V1_14",
    v_rec="V1_12",
    s_adv="Recuado_1S1",
    s_rec="Avancado_1S2",
)

_CFG_A2 = _LatchCfg(
    id="A2",
    expected_ms=500,
    debounce_ms=80,
    timeout_factor=1.5,
    v_av="V2_14",
    v_rec="V2_12",
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
    # Se criar o índice abaixo, você pode forçar o uso dele:
    # FORCE INDEX (idx_opc_name_ts)
    sql = f"""
        SELECT s.name, s.value_bool, s.ts_utc
        FROM opc_samples s /* FORCE INDEX (idx_opc_name_ts) */
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

# =========================
# Séries S1/S2 (helpers)
# =========================
OPC_TABLE = os.getenv("OPC_TABLE", "opc_samples")

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
        if dt is None:
            continue  # ignora ts inválido

        if vb_raw is None:
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
    """
    MESMA regra do Dashboard:
    ABERTO quando S1=0 & S2=1; FECHADO quando S1=1 & S2=0; senão mantém o último.
    Retorna (ABERTO[], FECHADO[]) com dedup por valor.
    """
    if not s1 and not s2:
        return [], []

    times = sorted({t for t,_ in s1} | {t for t,_ in s2})
    if not times:
        return [], []

    def val_at(seq: List[Tuple[datetime,int]], t: datetime) -> int:
        # último valor <= t (busca binária simples)
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
    """Duração do último 1...0 (segundos). Se termina em 1, conta até agora."""
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

# -----------------------------------------------------------------------------
# Health
# -----------------------------------------------------------------------------
@app.get("/health")
def health():
    # DB time (igual você já fazia)
    row = fetch_one("SELECT NOW(6) AS now6")
    db_time_val = first_col(row) if row is not None else None

    # runtime e started_at do processo
    now_ms = int(time.time() * 1000)
    runtime_ms = now_ms - PROCESS_STARTED_MS
    started_at_iso = datetime.fromtimestamp(PROCESS_STARTED_MS / 1000, tz=timezone.utc).isoformat()

    # (opcional) horário do servidor para comparação
    server_time_iso = datetime.now(tz=timezone.utc).isoformat()

    return {
        "status": "ok",
        "runtime_ms": runtime_ms,                     # <- o front usa isso direto
        "started_at": started_at_iso,                 # <- fallback/telemetria
        "server_time": server_time_iso,               # opcional
        "db_time": str(db_time_val) if db_time_val is not None else None,
    }

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
            "ts_utc": dt_to_iso_utc(col(r, "ts_utc") or col(r, 0)),
            "ax_g": col(r, "ax_g"), "ay_g": col(r, "ay_g"), "az_g": col(r, "az_g"),
            "gx_dps": col(r, "gx_dps"), "gy_dps": col(r, "gy_dps"), "gz_dps": col(r, "gz_dps"),
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

# --- NOVO endpoint estável para Monitoring (não conflita com o antigo) -------
@app.get("/api/live/actuators/state-mon", tags=["Live"])
def live_actuators_state_mon(since_ms: int = 12000):
    """
    Endpoint dedicado para a aba Monitoring. Não altera /api/live/actuators/state.
    """
    try:
        data = _live_actuators_state_data_fast()
    except Exception:
        data = _live_actuators_state_data_window(since_ms)
    return JSONResponse(content=data, headers={"Cache-Control": "no-store", "Pragma": "no-cache"})

# --- Aliases opcionais (mantidos, se quiser usar) -----------------------------
@app.get("/live/actuators/state", tags=["Live"])
def live_actuators_state_alias1(since_ms: int = 12000):
    try:
        data = _live_actuators_state_data_fast()
    except Exception:
        data = _live_actuators_state_data_window(since_ms)
    return JSONResponse(content=data, headers={"Cache-Control": "no-store", "Pragma": "no-cache"})

@app.get("/api/live/actuators/state2", tags=["Live"])
def live_actuators_state_alias2(since_ms: int = 12000):
    try:
        data = _live_actuators_state_data_fast()
    except Exception:
        data = _live_actuators_state_data_window(since_ms)
    return JSONResponse(content=data, headers={"Cache-Control": "no-store", "Pragma": "no-cache"})

# --- Cycles total (FSM leve, compat) -----------------------------------------
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

# --- Vibration (janela curta) -------------------------------------------------
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

# --- CPM por janela (S1/S2) ---------------------------------------------------
@app.get("/api/live/actuators/cpm", tags=["Live"])
def live_actuators_cpm(window_s: int = Query(60, ge=10, le=600)):
    try:
        s_map = {
            1: {"S1": _CFG_A1.s_adv, "S2": _CFG_A1.s_rec},  # atenção: nomes conforme config atual
            2: {"S1": _CFG_A2.s_adv, "S2": _CFG_A2.s_rec},
        }
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
            out.append({"id": aid, "window_s": window_s, "cycles": cycles, "cpm": cpm})

        return {"actuators": out, "ts": _now_iso()}
    except Exception as e:
        return JSONResponse(
            status_code=200,
            content={"actuators": [{"id": 1, "window_s": window_s, "cycles": 0, "cpm": 0.0},
                                   {"id": 2, "window_s": window_s, "cycles": 0, "cpm": 0.0}],
                     "ts": _now_iso(), "error": str(e)[:200]},
            headers={"Cache-Control": "no-store", "Pragma": "no-cache"},
        )

# --- Timings via S1/S2 (sem tabelas auxiliares) -------------------------------
@app.get("/api/live/actuators/timings", tags=["Live"])
def live_actuator_timings(window_s: int = Query(600, ge=60, le=3600)):
    try:
        s_map = {
            1: {"S1": _CFG_A1.s_adv, "S2": _CFG_A1.s_rec},
            2: {"S1": _CFG_A2.s_adv, "S2": _CFG_A2.s_rec},
        }
        names = [v for m in s_map.values() for v in (m["S1"], m["S2"])]
        raw = _fetch_series(names, window_s)
        compact = {k: _dedup(v) for k, v in raw.items()}

        out = []
        for aid, m in s_map.items():
            s1 = compact.get(m["S1"], [])
            s2 = compact.get(m["S2"], [])
            opened, closed = _derive_open_closed_from_S1S2(s1, s2)
            t_abre  = _last_pulse_duration(opened)
            t_fecha = _last_pulse_duration(closed)
            t_ciclo = (t_abre or 0.0) + (t_fecha or 0.0) if (t_abre or t_fecha) else None

            out.append({
                "actuator_id": aid,
                "last": {
                    "ts_utc": None,
                    "dt_abre_s":  t_abre,
                    "dt_fecha_s": t_fecha,
                    "dt_ciclo_s": t_ciclo,
                }
            })
        return {"actuators": out, "ts": _now_iso()}
    except Exception as e:
        safe = [
            {"actuator_id": 1, "last": {"ts_utc": None, "dt_abre_s": None, "dt_fecha_s": None, "dt_ciclo_s": None}},
            {"actuator_id": 2, "last": {"ts_utc": None, "dt_abre_s": None, "dt_fecha_s": None, "dt_ciclo_s": None}},
        ]
        return JSONResponse(
            status_code=200,
            content={"actuators": safe, "ts": _now_iso(), "error": str(e)[:200]},
            headers={"Cache-Control": "no-store", "Pragma": "no-cache"},
        )

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
        if isinstance(items, dict) and "items" in items:
            items = items["items"]
        if isinstance(items, list):
            return items
        return []
    except Exception:
        return []

# --- routers adicionais (se existirem) ---------------------------------------
try:
    from .routes import simulation  # opcional
    app.include_router(simulation.router)
except Exception as e:
    print(f"[simulation] router não carregado: {e}")

try:
    from .routes import alerts as alerts_route  # opcional
    app.include_router(alerts_route.router)
except Exception as e:
    print(f"[alerts] router não carregado: {e}")

# --- RESTAURA rota clássica usada pelo Dashboard ------------------------------
@app.get("/api/live/actuators/state", tags=["Live"])
def live_actuators_state(since_ms: int = 12000):
    """
    Compatibilidade: rota clássica usada pelo Dashboard/LiveContext.
    Tenta caminho rápido (últimos por nome) e cai para janela se necessário.
    """
    try:
        data = _live_actuators_state_data_fast()
    except Exception:
        data = _live_actuators_state_data_window(since_ms)
    return JSONResponse(content=data, headers={"Cache-Control": "no-store", "Pragma": "no-cache"})
