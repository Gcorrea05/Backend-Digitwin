# api/api_ws.py
import os
import re
import time
import math
import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Any, Dict, Tuple
import json

from dotenv import load_dotenv, find_dotenv
from fastapi import FastAPI, Request, HTTPException, Query, WebSocket, WebSocketDisconnect, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from mysql.connector.errors import PoolError
from collections import deque
from hashlib import sha1
from json import loads as _json_loads

# -----------------------------------------------------------------------------
# .env
# -----------------------------------------------------------------------------
load_dotenv(find_dotenv())
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

# -----------------------------------------------------------------------------
# FastAPI + CORS
# -----------------------------------------------------------------------------
app = FastAPI(title="GM Digital Twin API", version="0.7.0")

ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*")
if ALLOWED_ORIGINS == "*":
    allow_origins = ["*"]
else:
    allow_origins = [x.strip() for x in ALLOWED_ORIGINS.split(",") if x.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------------------------------------------------------
# DB CONFIG
# -----------------------------------------------------------------------------
DB_DSN = os.getenv("DB_DSN", "")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "entry")
DB_PASS = os.getenv("DB_PASS", "root")
DB_NAME = os.getenv("DB_NAME", "gmdigital")
DB_POOL_NAME = os.getenv("DB_POOL_NAME", "gmdigital_pool")
DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "8"))

OPC_TABLE = os.getenv("OPC_TABLE", "opc_values")
OPC_LATEST_VIEW = os.getenv("OPC_LATEST_VIEW", "opc_latest")
MPU_TABLE = os.getenv("MPU_TABLE", "mpu_values")

STORE_TZ = os.getenv("STORE_TZ", "UTC").upper()           # "UTC" ou "LOCAL"
LOCAL_TZ_OFFSET_SEC = int(os.getenv("LOCAL_TZ_OFFSET_SEC", "-10800"))  # UTC-3
DEV_TIME_OFFSET_SEC = int(os.getenv("DEV_TIME_OFFSET_SEC", "0"))

# -----------------------------------------------------------------------------
# Limites / Janelas
# -----------------------------------------------------------------------------
LIVE_TICK_MS       = int(os.getenv("LIVE_TICK_MS", "200"))
MON_TICK_MS        = int(os.getenv("MON_TICK_MS", "2000"))
SLOW_TICK_MS       = int(os.getenv("SLOW_TICK_MS", "60000"))
WS_BUFFER_MAX      = int(os.getenv("WS_BUFFER_MAX", "500"))
WS_HEARTBEAT_MS    = int(os.getenv("WS_HEARTBEAT_MS", "10000"))

# -----------------------------------------------------------------------------
# Helpers de tempo
# -----------------------------------------------------------------------------
def _now_for_db() -> datetime:
    """
    Retorna datetime 'naive' (sem tzinfo) no fuso escolhido para gravar em MySQL DATETIME.
    - STORE_TZ == 'LOCAL'  -> usa UTC-3 (BRT)
    - STORE_TZ == 'UTC'    -> usa UTC
    Aplica também DEV_TIME_OFFSET_SEC (se quiser simular).
    """
    t = datetime.utcnow()  # base em UTC
    if STORE_TZ == "LOCAL":
        t = t + timedelta(seconds=LOCAL_TZ_OFFSET_SEC)  # vai pra UTC-3
    # offset extra de DEV (opcional)
    t = t + timedelta(seconds=DEV_TIME_OFFSET_SEC)
    return t.replace(tzinfo=None)

def _epoch_ms_from_local_naive(ts_local_naive: datetime) -> int:
    """
    Converte um datetime 'naive' que representa horário local (caso STORE_TZ=='LOCAL')
    para epoch ms em UTC, assumindo LOCAL = UTC-3.
    Se STORE_TZ=='UTC', considera que o 'naive' já está em UTC.
    """
    if ts_local_naive.tzinfo is not None:
        # Se por algum motivo vier com tz, normaliza
        ts_utc = ts_local_naive.astimezone(timezone.utc)
        return int(ts_utc.timestamp() * 1000)

    if STORE_TZ == "LOCAL":
        # O 'naive' representa UTC-3 → para UTC: somar +3h
        ts_utc = ts_local_naive + timedelta(seconds=-LOCAL_TZ_OFFSET_SEC)
    else:
        # naive já é UTC
        ts_utc = ts_local_naive

    return int(ts_utc.replace(tzinfo=timezone.utc).timestamp() * 1000)

def _coerce_to_datetime(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    if isinstance(v, (int, float)):
        return datetime.fromtimestamp(float(v), tz=timezone.utc)
    s = str(v).strip()
    if not s:
        return None
    try:
        # tenta ISO
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            pass
    return None

# -----------------------------------------------------------------------------
# Conexão MySQL
# -----------------------------------------------------------------------------
try:
    import mysql.connector
    from mysql.connector.pooling import MySQLConnectionPool
except Exception:
    mysql = None
    MySQLConnectionPool = None  # type: ignore

_POOL: Optional[MySQLConnectionPool] = None

def _ensure_mysql_pool() -> MySQLConnectionPool:
    global _POOL
    if _POOL is not None:
        return _POOL
    if mysql is None or MySQLConnectionPool is None:
        raise RuntimeError("mysql.connector não disponível")

    dsn = os.getenv("DB_DSN", "").strip()
    if dsn:
        # (Opcional: parse DSN se for necessário; aqui usamos os envs diretos)
        pass

    _POOL = MySQLConnectionPool(
        pool_name=DB_POOL_NAME,
        pool_size=DB_POOL_SIZE,
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        autocommit=True,
        connect_timeout=4,
        pool_reset_session=True,
    )
    return _POOL

def _ensure_mysql_connection(conn_like: Any = None):
    """
    Aceita: None | "ENV" | dsn string | pool | connection
    Retorna (conn, created_here: bool)
    """
    if conn_like is None:
        # 1) tenta pool global
        try:
            pool = _ensure_mysql_pool()
            return pool.get_connection(), True
        except Exception:
            pass

    if isinstance(conn_like, str):
        s = conn_like.strip().lower()
        if s in ("env", "dsn", "default"):
            # 2) tenta pool global
            try:
                pool = _ensure_mysql_pool()
                return pool.get_connection(), True
            except Exception:
                pass
        else:
            # 3) poderíamos interpretar DSN aqui; por enquanto cai no 5)
            pass

    # 4) já é uma conexão?
    if conn_like is not None:
        try:
            if getattr(conn_like, "is_connected", lambda: False)():
                return conn_like, False
        except Exception:
            pass

    # 5) último recurso: ENV
    if mysql is None:
        raise RuntimeError("mysql.connector indisponível")
    try:
        c = mysql.connector.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "3306")),
            user=os.getenv("DB_USER", "entry"),
            password=os.getenv("DB_PASS", "root"),
            database=os.getenv("DB_NAME", "gmdigital"),
        )
        c.autocommit = True
        return c, True
    except Exception as e:
        raise RuntimeError(f"Falha ao abrir conexão MySQL: {e}")

def fetch_one(sql: str, params: Tuple[Any, ...] = (), conn_like: Any = None):
    c, created = _ensure_mysql_connection(conn_like)
    try:
        cur = c.cursor()
        cur.execute(sql, params)
        row = cur.fetchone()
        return row
    finally:
        try:
            cur.close()
        except Exception:
            pass
        if created:
            try:
                c.close()
            except Exception:
                pass
def fetch_all(sql: str, params: Tuple[Any, ...] = (), conn_like: Any = None):
    c, created = _ensure_mysql_connection(conn_like)
    try:
        cur = c.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        return rows
    finally:
        try:
            cur.close()
        except Exception:
            pass
        if created:
            try:
                c.close()
            except Exception:
                pass

def col(row: Any, key: Any, default=None):
    if row is None:
        return default
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        # se vier tupla (única coluna)
        if isinstance(key, int):
            return row[key]
        return row[0]
    except Exception:
        return default

# -----------------------------------------------------------------------------
# Modelos de Latch
# -----------------------------------------------------------------------------
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
    s1: int = 0
    s2: int = 0
    s1_ts: Optional[datetime] = None
    s2_ts: Optional[datetime] = None
    raw_state: Optional[str] = None  # "RECUADO" | "AVANCADO" | None (indef)
    pending: Optional[str] = None    # "AVANÇAR" | "RECUAR" | None
    last_state: Optional[str] = None
    note: Optional[str] = None

# -----------------------------------------------------------------------------
# CFGs (NÃO ALTERAR CONFORME PEDIDO)
# -----------------------------------------------------------------------------
_CFG_A1 = _LatchCfg(
    id="A1", expected_ms=1500, debounce_ms=80, timeout_factor=1.5,
    v_av="V1_14", v_rec="V1_12",
    s_rec="Recuado_1S1",  # (mantido conforme arquivo original)
    s_adv="Avancado_1S2",
)
_CFG_A2 = _LatchCfg(
    id="A2", expected_ms=500, debounce_ms=80, timeout_factor=1.5,
    v_av="V2_14", v_rec="V2_12",
    s_rec="Recuado_2S1",
    s_adv="Avancado_2S2",
)

# -----------------------------------------------------------------------------
# Mapeamento S1/S2 a partir dos nomes (regex simples)
# -----------------------------------------------------------------------------
def _facet_names(cfg: _LatchCfg) -> Tuple[str, str]:
    """
    Extrai os nomes S1 e S2 a partir dos campos s_adv/s_rec assumindo
    nomes no padrão '..._S1' e '..._S2'. Mantém exatamente como no original.
    """
    s1, s2 = None, None
    for n in (cfg.s_adv, cfg.s_rec):
        m = re.search(r"_S([12])\b", n, re.IGNORECASE)
        if m:
            if m.group(1) == "1":
                s1 = n
            elif m.group(1) == "2":
                s2 = n
    # fallback (caso nomes não sigam padrão, mantém ordem adv/rec)
    if s1 is None:
        s1 = cfg.s_adv
    if s2 is None:
        s2 = cfg.s_rec
    return s1, s2

# Pré-listas para consultas
a1_s1, a1_s2 = _facet_names(_CFG_A1)
a2_s1, a2_s2 = _facet_names(_CFG_A2)
_NAMES_LATCH = (
    a1_s1, a1_s2,
    a2_s1, a2_s2,
)
CONTROL_NAMES = (
    _CFG_A1.v_av, _CFG_A1.v_rec,
    _CFG_A2.v_av, _CFG_A2.v_rec,
)

# -----------------------------------------------------------------------------
# Leitura rápida de "latest" (com /opc_latest)
# -----------------------------------------------------------------------------
def _fetch_latest_rows(names: Tuple[str, ...]) -> Dict[str, Dict[str, Any]]:
    if not names:
        return {}
    placeholders = ", ".join(["%s"] * len(names))
    sql = f"""
    SELECT name, value, ts_utc
    FROM {OPC_LATEST_VIEW}
    WHERE name IN ({placeholders})
    """
    rows = fetch_all(sql, names) or []
    out: Dict[str, Dict[str, Any]] = {}
    for name, value, ts in rows:
        vb = _bool(value)  # <-- calcula bool a partir de 'value'
        out[str(name)] = {
            "value": value,
            "value_bool": vb,
            "ts_utc": _coerce_to_datetime(ts),
        }
    for n in names:
        out.setdefault(n, {"value": None, "value_bool": None, "ts_utc": None})
    return out

def _fetch_series(names: List[str], window_s: int) -> Dict[str, List[Tuple[datetime,int]]]:
    if not names:
        return {}
    placeholders = ", ".join(["%s"] * len(names))
    sql = f"""
    SELECT name, value_bool, ts_utc
    FROM {OPC_TABLE}
    WHERE name IN ({placeholders})
      AND ts_utc >= (UTC_TIMESTAMP(6) - INTERVAL %s SECOND)
    ORDER BY ts_utc ASC
    """
    rows = fetch_all(sql, tuple(names) + (window_s,)) or []
    series: Dict[str, List[Tuple[datetime,int]]] = {}
    for name, vbool, ts in rows:
        series.setdefault(str(name), []).append((_coerce_to_datetime(ts) or datetime.now(timezone.utc), int(vbool or 0)))
    return series

# -----------------------------------------------------------------------------
# Fast fetch (abre conexão localmente) — aceita conn_like/ENV
# -----------------------------------------------------------------------------
def _fetch_latest_rows_fast(conn_like) -> Dict[str, int]:
    """
    Retorna dict simples { name: value_bool(int) } via {OPC_LATEST_VIEW}.
    Compatível com views sem 'value_bool'.
    """
    c, created = _ensure_mysql_connection(conn_like)
    try:
        cur = c.cursor()
        placeholders = ", ".join(["%s"] * len(_NAMES_LATCH + CONTROL_NAMES))
        sql = f"""
        SELECT name, value
        FROM {OPC_LATEST_VIEW}
        WHERE name IN ({placeholders})
        """
        cur.execute(sql, _NAMES_LATCH + CONTROL_NAMES)
        rows = cur.fetchall() or []
        out: Dict[str, int] = {}
        for name, value in rows:
            out[str(name)] = _bool(value)  # <-- calcula aqui
        for n in (_NAMES_LATCH + CONTROL_NAMES):
            out.setdefault(n, 0)
        return out
    finally:
        try: cur.close()
        except Exception: pass
        if created:
            try: c.close()
            except Exception: pass

# -----------------------------------------------------------------------------
# Inferência de estado a partir do latest
# -----------------------------------------------------------------------------
def _infer_state_from_latest(cfg: _LatchCfg, latest: Dict[str, int]) -> _LatchState:
    st = _LatchState()
    # Localiza S1/S2
    s1_name, s2_name = _facet_names(cfg)
    st.s1 = int(latest.get(s1_name, 0))
    st.s2 = int(latest.get(s2_name, 0))

    if st.s1 == 1 and st.s2 == 0:
        st.raw_state = "RECUADO"
    elif st.s1 == 0 and st.s2 == 1:
        st.raw_state = "AVANÇADO"  # <- padronizado com cedilha
    elif st.s1 == 1 and st.s2 == 1:
        st.raw_state = None
        st.note = "S1=1 e S2=1 (estado físico inconsistente)"
    else:
        st.raw_state = None  # indefinido (entre sensores)

    # Pending pelo comando de válvula (heurística simples)
    v_av = latest.get(cfg.v_av, 0)
    v_rec = latest.get(cfg.v_rec, 0)
    if v_av and not v_rec:
        st.pending = "AVANÇAR"
    elif v_rec and not v_av:
        st.pending = "RECUAR"
    else:
        st.pending = None

    # timeouts etc. (placeholder)
    st.last_state = st.raw_state
    return st


# -----------------------------------------------------------------------------
# Payload de /api/live/snapshot
# -----------------------------------------------------------------------------
def build_live_payload() -> dict:
    try:
        latest = _LIVE_CACHE.get("vals") or {}
        if not latest:
            latest = _fetch_latest_rows_fast("ENV")
    except Exception as e:
        # se tudo falhar, segue com dict vazio e marca erro
        print(f"[build_live_payload] latest fetch failed: {e}")
        latest = {}
        error = str(e)
    else:
        error = None

    a1 = _infer_state_from_latest(_CFG_A1, latest)
    a2 = _infer_state_from_latest(_CFG_A2, latest)

    now_local_naive = _now_for_db()
    ts_ms = _epoch_ms_from_local_naive(now_local_naive)

    payload = {
        "type": "live",
        "ts_ms": ts_ms,
        "actuators": [
            {
                "id": 1,
                "s1": a1.s1,
                "s2": a1.s2,
                "state": a1.raw_state or (a1.last_state or "RECUADO"),
                "pending": a1.pending,
                "note": a1.note,
            },
            {
                "id": 2,
                "s1": a2.s1,
                "s2": a2.s2,
                "state": a2.raw_state or (a2.last_state or "RECUADO"),
                "pending": a2.pending,
                "note": a2.note,
            },
        ],
    }
    if error:
        payload["error"] = error
    return payload

# -----------------------------------------------------------------------------
# Monitoring payload (curto)
# -----------------------------------------------------------------------------
def build_monitoring_payload() -> dict:
    WINDOW_S_PRIMARY  = int(os.getenv("MON_TIMING_WINDOW_S", "60"))
    WINDOW_S_FALLBACK = int(os.getenv("MON_TIMING_FALLBACK_S", "60"))

    s_map = {1: {"S1": _facet_names(_CFG_A1)[0], "S2": _facet_names(_CFG_A1)[1]},
             2: {"S1": _facet_names(_CFG_A2)[0], "S2": _facet_names(_CFG_A2)[1]}}
    names = [v for m in s_map.values() for v in (m["S1"], m["S2"])]

    ref_row = fetch_one(f"SELECT COALESCE(MAX(ts_utc), NOW(6)) AS ref_ts FROM {OPC_TABLE}")
    ref_ts = _coerce_to_datetime(col(ref_row, "ref_ts") if isinstance(ref_row, dict) else (ref_row[0] if ref_row else None)) or datetime.now(timezone.utc)

    def _nonneg(x: Optional[float]) -> Optional[float]:
        if x is None: return None
        return max(0.0, float(x))

    def _age_s(ts: Optional[datetime]) -> Optional[float]:
        if ts is None: return None
        return _nonneg((ref_ts - ts).total_seconds())

    window_s = WINDOW_S_PRIMARY or 60
    series = _fetch_series(names, window_s)

    # Timeline simples por atuador
    out = {"type": "monitoring", "ref_ts": ref_ts.isoformat(), "window_s": window_s, "items": []}
    for aid in (1, 2):
        s1n, s2n = s_map[aid]["S1"], s_map[aid]["S2"]
        out["items"].append({
            "id": aid,
            "s1": series.get(s1n, []),
            "s2": series.get(s2n, []),
        })
    return out

# -----------------------------------------------------------------------------
# WebSocket infra
# -----------------------------------------------------------------------------
class WsHub:
    def __init__(self):
        self.conns: List[WebSocket] = []
        self.buf: deque = deque(maxlen=WS_BUFFER_MAX)

    async def add(self, ws: WebSocket):
        await ws.accept()
        self.conns.append(ws)

    def remove(self, ws: WebSocket):
        try:
            self.conns.remove(ws)
        except Exception:
            pass

    async def broadcast(self, msg: str):
        dead = []
        for ws in list(self.conns):
            try:
                await ws.send_text(msg)
            except Exception:
                dead.append(ws)
        for d in dead:
            self.remove(d)

WS_LIVE = WsHub()
WS_MON  = WsHub()
WS_SLOW = WsHub()

# -----------------------------------------------------------------------------
# Loops de produção
# -----------------------------------------------------------------------------
_LIVE_CACHE: Dict[str, Any] = {"vals": {}, "ts": None}

async def hot_drain_loop():
    """Placeholder para drenar fila quente (se usada futuramente)."""
    while True:
        await asyncio.sleep(0.250)
async def live_producer_loop():
    period = max(0.050, LIVE_TICK_MS / 1000.0)
    next_t = time.perf_counter()
    while True:
        next_t += period
        try:
            payload = build_live_payload()
        except Exception as e:
            # loga e emite um payload de erro para aparecer no DevTools
            print(f"[live_producer_loop] build_live_payload failed: {e}")
            payload = {"type": "live", "error": str(e), "ts": datetime.utcnow().isoformat()+"Z"}
        try:
            WS_LIVE.buf.append(payload)
            await WS_LIVE.broadcast(json.dumps(payload, ensure_ascii=False, default=str))
        except Exception as e:
            print(f"[live_producer_loop] broadcast failed: {e}")
        await asyncio.sleep(max(0, next_t - time.perf_counter()))
async def live_sampler_loop():
    names = _NAMES_LATCH + CONTROL_NAMES
    period = max(0.050, LIVE_TICK_MS / 1000.0)
    next_t = time.perf_counter()
    while True:
        next_t += period
        try:
            latest = await asyncio.to_thread(_fetch_latest_rows_fast, "ENV")
            _LIVE_CACHE["vals"] = latest
            _LIVE_CACHE["ts"] = datetime.now(timezone.utc)
        except Exception:
            pass
        await asyncio.sleep(max(0, next_t - time.perf_counter()))
async def live_sampler_loop():
    names = _NAMES_LATCH + CONTROL_NAMES
    period = max(0.050, LIVE_TICK_MS / 1000.0)
    next_t = time.perf_counter()
    while True:
        next_t += period
        try:
            latest = await asyncio.to_thread(_fetch_latest_rows_fast, "ENV")
            _LIVE_CACHE["vals"] = latest
            _LIVE_CACHE["ts"] = datetime.now(timezone.utc)
        except Exception as e:
            print(f"[live_sampler_loop] fetch_latest_rows_fast failed: {e}")
        await asyncio.sleep(max(0, next_t - time.perf_counter()))
async def monitoring_producer_loop():
    period = max(0.100, MON_TICK_MS / 1000.0)
    next_t = time.perf_counter()
    while True:
        next_t += period
        try:
            payload = build_monitoring_payload()
            WS_MON.buf.append(payload)
            msg = json.dumps(payload, ensure_ascii=False, default=str)
            await WS_MON.broadcast(msg)
        except Exception:
            pass
        await asyncio.sleep(max(0, next_t - time.perf_counter()))

async def slow_producer_loop():
    period = max(1.0, SLOW_TICK_MS / 1000.0)
    next_t = time.perf_counter()
    while True:
        next_t += period
        try:
            # payload lento (ex.: alertas, sumários); aqui, placeholder
            payload = {"type": "slow", "ts": datetime.utcnow().isoformat() + "Z"}
            WS_SLOW.buf.append(payload)
            await WS_SLOW.broadcast(json.dumps(payload))
        except Exception:
            pass
        await asyncio.sleep(max(0, next_t - time.perf_counter()))

# -----------------------------------------------------------------------------
# Startup
# -----------------------------------------------------------------------------
@app.on_event("startup")
async def _on_startup():
    # inicia loops
    for fn in (hot_drain_loop, live_sampler_loop, live_producer_loop, monitoring_producer_loop, slow_producer_loop):
        asyncio.create_task(fn())

# -----------------------------------------------------------------------------
# Rotas HTTP
# -----------------------------------------------------------------------------
@app.get("/health")
async def health():
    return {"ok": True, "ts": datetime.utcnow().isoformat() + "Z"}

@app.get("/api/live/snapshot")
async def api_live_snapshot():
    try:
        payload = build_live_payload()
        return JSONResponse(payload)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"snapshot error: {e}")

@app.get("/api/monitoring/snapshot")
async def api_monitoring_snapshot():
    try:
        payload = build_monitoring_payload()
        return JSONResponse(payload)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"monitoring error: {e}")

@app.get("/api/opc/latest")
async def api_opc_latest():
    try:
        rows = fetch_all(f"SELECT name, value, value_bool, ts_utc FROM {OPC_LATEST_VIEW}")
        out = []
        for name, value, vbool, ts in rows or []:
            out.append({
                "name": name,
                "value": value,
                "value_bool": int(vbool or 0),
                "ts_utc": (_coerce_to_datetime(ts) or datetime.now(timezone.utc)).isoformat(),
            })
        return JSONResponse({"rows": out})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"latest error: {e}")

@app.get("/api/opc/series")
async def api_opc_series(
    names: str = Query(..., description="CSV de nomes"),
    window_s: int = Query(60, ge=1, le=600),
):
    try:
        name_list = [x.strip() for x in names.split(",") if x.strip()]
        data = _fetch_series(name_list, window_s)
        # serializa
        out = {k: [(ts.isoformat(), v) for (ts, v) in vs] for k, vs in data.items()}
        return JSONResponse({"series": out})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"series error: {e}")

# -----------------------------------------------------------------------------
# WebSockets
# -----------------------------------------------------------------------------
@app.websocket("/ws/live")
async def ws_live(ws: WebSocket):
    await WS_LIVE.add(ws)
    try:
        # flush buffer inicial
        for item in list(WS_LIVE.buf)[-10:]:
            try:
                await ws.send_text(json.dumps(item, ensure_ascii=False))
            except Exception:
                pass

        hb_period = max(0.1, WS_HEARTBEAT_MS / 1000.0)
        next_hb = time.perf_counter()
        while True:
            try:
                _ = await asyncio.wait_for(ws.receive_text(), timeout=hb_period)
            except asyncio.TimeoutError:
                # heartbeat
                next_hb += hb_period
                try:
                    await ws.send_text('{"type":"hb"}')
                except Exception:
                    break
            except WebSocketDisconnect:
                break
            except Exception:
                # qualquer outra mensagem ignorada
                pass
    finally:
        WS_LIVE.remove(ws)
@app.websocket("/ws/monitoring")
async def ws_monitoring(ws: WebSocket):
    await WS_MON.add(ws)
    try:
        for item in list(WS_MON.buf)[-2:]:
            try:
                await ws.send_text(json.dumps(item, ensure_ascii=False, default=str))
            except Exception:
                pass

        hb_period = max(0.2, WS_HEARTBEAT_MS / 1000.0)
        next_hb = time.perf_counter()
        while True:
            try:
                _ = await asyncio.wait_for(ws.receive_text(), timeout=hb_period)
            except asyncio.TimeoutError:
                next_hb += hb_period
                try:
                    await ws.send_text('{"type":"hb"}')
                except Exception:
                    break
            except WebSocketDisconnect:
                break
            except Exception:
                pass
    finally:
        WS_MON.remove(ws)

@app.websocket("/ws/slow")
async def ws_slow(ws: WebSocket):
    await WS_SLOW.add(ws)
    try:
        for item in list(WS_SLOW.buf)[-2:]:
            try:
                await ws.send_text(json.dumps(item))
            except Exception:
                pass

        hb_period = max(0.5, WS_HEARTBEAT_MS / 1000.0)
        next_hb = time.perf_counter()
        while True:
            try:
                _ = await asyncio.wait_for(ws.receive_text(), timeout=hb_period)
            except asyncio.TimeoutError:
                next_hb += hb_period
                try:
                    await ws.send_text('{"type":"hb"}')
                except Exception:
                    break
            except WebSocketDisconnect:
                break
            except Exception:
                pass
    finally:
        WS_SLOW.remove(ws)

# -----------------------------------------------------------------------------
# DEBUG (opcionais)
# -----------------------------------------------------------------------------
@app.get("/api/debug/ping")
async def api_debug_ping():
    return {"pong": True, "ts": datetime.utcnow().isoformat() + "Z"}

@app.get("/api/debug/state")
async def api_debug_state():
    vals = _LIVE_CACHE.get("vals") or {}
    return JSONResponse({"live_vals": vals, "ts": datetime.utcnow().isoformat()+"Z"})

# -----------------------------------------------------------------------------
# (Opcional) endpoints de MPU
# -----------------------------------------------------------------------------
def _fetch_mpu_latest(conn_like=None, limit: int = 20):
    c, created = _ensure_mysql_connection(conn_like)
    try:
        cur = c.cursor()
        cur.execute(f"""
        SELECT sensor_id, ax, ay, az, gx, gy, gz, ts_local
        FROM {MPU_TABLE}
        ORDER BY ts_local DESC
        LIMIT %s
        """, (limit,))
        rows = cur.fetchall() or []
        out = []
        for sensor_id, ax, ay, az, gx, gy, gz, ts_local in rows:
            ts = _coerce_to_datetime(ts_local) or datetime.now(timezone.utc)
            out.append({
                "sensor_id": sensor_id,
                "ax": float(ax or 0),
                "ay": float(ay or 0),
                "az": float(az or 0),
                "gx": float(gx or 0),
                "gy": float(gy or 0),
                "gz": float(gz or 0),
                "ts": ts.isoformat(),
            })
        return out
    finally:
        try:
            cur.close()
        except Exception:
            pass
        if created:
            try:
                c.close()
            except Exception:
                pass

@app.get("/api/mpu/latest")
async def api_mpu_latest(limit: int = Query(20, ge=1, le=200)):
    try:
        out = _fetch_mpu_latest("ENV", limit=limit)
        return JSONResponse({"rows": out})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"mpu latest error: {e}")
# -----------------------------------------------------------------------------
# Auxiliares de hashing/ETag (se precisar)
# -----------------------------------------------------------------------------
def _etag_for(obj: Any) -> str:
    s = json.dumps(obj, sort_keys=True, ensure_ascii=False, default=str)
    return sha1(s.encode("utf-8")).hexdigest()

# -----------------------------------------------------------------------------
# Handlers adicionais (exemplo de sumários/alertas no futuro)
# -----------------------------------------------------------------------------
@app.get("/api/slow/summary")
async def api_slow_summary():
    try:
        payload = {
            "type": "slow-summary",
            "ts": datetime.utcnow().isoformat() + "Z",
            "meta": {"ok": True},
        }
        return JSONResponse(payload, headers={"ETag": _etag_for(payload)})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"slow summary error: {e}")

# -----------------------------------------------------------------------------
# Caso precise expor valores crus para debug do latch
# -----------------------------------------------------------------------------
def _latest_map_for_debug() -> Dict[str, Dict[str, Any]]:
    rows = fetch_all(f"SELECT name, value_bool, ts_utc FROM {OPC_LATEST_VIEW}")
    out: Dict[str, Dict[str, Any]] = {}
    for name, vb, ts in rows or []:
        out[str(name)] = {
            "value_bool": int(vb or 0),
            "ts_utc": (_coerce_to_datetime(ts) or datetime.now(timezone.utc)).isoformat(),
        }
    return out

@app.get("/api/debug/latest_map")
async def api_debug_latest_map():
    try:
        return JSONResponse(_latest_map_for_debug())
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"debug latest_map error: {e}")

# -----------------------------------------------------------------------------
# Monitoring - versão alternativa com métrica de latência (placeholder)
# -----------------------------------------------------------------------------
def _rows_for_names(names: List[str], window_s: int) -> Dict[str, List[Dict[str, Any]]]:
    raw = _fetch_series(names, 30)
    out: Dict[str, List[Dict[str, Any]]] = {}
    for k, arr in raw.items():
        out[k] = [{"ts": ts.isoformat(), "value": v} for (ts, v) in arr]
    return out

@app.get("/api/monitoring/rows")
async def api_monitoring_rows(
    window_s: int = Query(60, ge=1, le=600),
):
    try:
        names = list(_NAMES_LATCH)
        data = _rows_for_names(names, window_s)
        return JSONResponse({"window_s": window_s, "rows": data})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"monitoring rows error: {e}")
# -----------------------------------------------------------------------------
# Opcional: endpoints para controle de válvulas (se usar)
# -----------------------------------------------------------------------------
def _set_valve(name: str, value: int, conn_like=None) -> int:
    """
    Exemplo: atualiza comando de válvula em uma tabela de controle (não implementado).
    Retorna 1 se aplicado, 0 se ignorado.
    """
    # Placeholder; em projetos reais, chamar PLC/OPC write.
    return 0

@app.post("/api/valve/set")
async def api_valve_set(body: Dict[str, Any] = Body(...)):
    """
    body: { "name": "V1_14", "value": 1 }
    """
    try:
        name = str(body.get("name", "")).strip()
        value = int(body.get("value", 0))
        if not name:
            raise HTTPException(status_code=400, detail="name obrigatório")
        if value not in (0, 1):
            raise HTTPException(status_code=400, detail="value deve ser 0 ou 1")
        applied = _set_valve(name, value, "ENV")
        return {"ok": bool(applied)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"valve set error: {e}")

# -----------------------------------------------------------------------------
# Filtros/transformações auxiliares (se útil)
# -----------------------------------------------------------------------------
def _bool(v: Any) -> int:
    try:
        return 1 if int(v) else 0
    except Exception:
        s = str(v).strip().lower()
        return 1 if s in ("1", "true", "on", "yes") else 0

def _normalize_name(n: str) -> str:
    return re.sub(r"\s+", "_", n.strip())

# -----------------------------------------------------------------------------
# Exemplo de endpoint de series combinadas (S1/S2 por A1/A2)
# -----------------------------------------------------------------------------
@app.get("/api/series/latch")
async def api_series_latch(
    window_s: int = Query(60, ge=1, le=600),
):
    try:
        names = [
            _facet_names(_CFG_A1)[0], _facet_names(_CFG_A1)[1],
            _facet_names(_CFG_A2)[0], _facet_names(_CFG_A2)[1],
        ]
        data = _fetch_series(names, window_s)
        out = {k: [(ts.isoformat(), v) for (ts, v) in arr] for k, arr in data.items()}
        return JSONResponse({"window_s": window_s, "series": out})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"series latch error: {e}")

# -----------------------------------------------------------------------------
# Dump de configuração (útil para o front debugar nomes)
# -----------------------------------------------------------------------------
@app.get("/api/config/latch")
async def api_config_latch():
    try:
        def cfg_to_dict(cfg: _LatchCfg):
            s1, s2 = _facet_names(cfg)
            return {
                "id": cfg.id,
                "expected_ms": cfg.expected_ms,
                "debounce_ms": cfg.debounce_ms,
                "timeout_factor": cfg.timeout_factor,
                "v_av": cfg.v_av,
                "v_rec": cfg.v_rec,
                "s_adv": cfg.s_adv,
                "s_rec": cfg.s_rec,
                "S1": s1,
                "S2": s2,
            }
        return JSONResponse({
            "A1": cfg_to_dict(_CFG_A1),
            "A2": cfg_to_dict(_CFG_A2),
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"config latch error: {e}")
# -----------------------------------------------------------------------------
# Endpoints crus para inspeção de tabelas (cuidado em prod)
# -----------------------------------------------------------------------------
@app.get("/api/db/opc_values")
async def api_db_opc_values(limit: int = Query(100, ge=1, le=5000)):
    try:
        rows = fetch_all(f"""
        SELECT name, value, value_bool, ts_utc
        FROM {OPC_TABLE}
        ORDER BY ts_utc DESC
        LIMIT %s
        """, (limit,))
        out = []
        for name, value, vbool, ts in rows or []:
            out.append({
                "name": name,
                "value": value,
                "value_bool": int(vbool or 0),
                "ts_utc": (_coerce_to_datetime(ts) or datetime.now(timezone.utc)).isoformat(),
            })
        return JSONResponse({"rows": out})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"db opc_values error: {e}")

@app.get("/api/db/opc_latest")
async def api_db_opc_latest():
    try:
        rows = fetch_all(f"""
        SELECT name, value, value_bool, ts_utc
        FROM {OPC_LATEST_VIEW}
        """)
        out = []
        for name, value, vbool, ts in rows or []:
            out.append({
                "name": name,
                "value": value,
                "value_bool": int(vbool or 0),
                "ts_utc": (_coerce_to_datetime(ts) or datetime.now(timezone.utc)).isoformat(),
            })
        return JSONResponse({"rows": out})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"db opc_latest error: {e}")

@app.get("/api/db/mpu_values")
async def api_db_mpu_values(limit: int = Query(100, ge=1, le=5000)):
    try:
        rows = fetch_all(f"""
        SELECT sensor_id, ax, ay, az, gx, gy, gz, ts_local
        FROM {MPU_TABLE}
        ORDER BY ts_local DESC
        LIMIT %s
        """, (limit,))
        out = []
        for sid, ax, ay, az, gx, gy, gz, ts_local in rows or []:
            out.append({
                "sensor_id": sid,
                "ax": float(ax or 0),
                "ay": float(ay or 0),
                "az": float(az or 0),
                "gx": float(gx or 0),
                "gy": float(gy or 0),
                "gz": float(gz or 0),
                "ts_local": (_coerce_to_datetime(ts_local) or datetime.now(timezone.utc)).isoformat(),
            })
        return JSONResponse({"rows": out})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"db mpu_values error: {e}")

# -----------------------------------------------------------------------------
# Endpoint que consolida tudo (se quiser usar no Dashboard)
# -----------------------------------------------------------------------------
@app.get("/api/dashboard/summary")
async def api_dashboard_summary():
    try:
        live = build_live_payload()
        mon  = build_monitoring_payload()
        mpu  = _fetch_mpu_latest("ENV", limit=10)
        return JSONResponse({
            "live": live,
            "monitoring": mon,
            "mpu": mpu,
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"dashboard summary error: {e}")

# -----------------------------------------------------------------------------
# Versão compacta de /api/live/snapshot (para clients leves)
# -----------------------------------------------------------------------------
@app.get("/api/live/compact")
async def api_live_compact():
    try:
        latest = _LIVE_CACHE.get("vals") or {}
        if not latest:
            latest = _fetch_latest_rows_fast("ENV")
        a1 = _infer_state_from_latest(_CFG_A1, latest)
        a2 = _infer_state_from_latest(_CFG_A2, latest)
        return JSONResponse({
            "a1": {"s1": a1.s1, "s2": a1.s2, "state": a1.raw_state or a1.last_state},
            "a2": {"s1": a2.s1, "s2": a2.s2, "state": a2.raw_state or a2.last_state},
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"live compact error: {e}")
# -----------------------------------------------------------------------------
# Endpoint legacy: retorna latest direto para tags de interesse
# -----------------------------------------------------------------------------
@app.get("/api/opc/latest/by_names")
async def api_opc_latest_by_names(names: str = Query(..., description="CSV de nomes")):
    try:
        name_list = [x.strip() for x in names.split(",") if x.strip()]
        placeholders = ", ".join(["%s"] * len(name_list))
        rows = fetch_all(f"""
        SELECT name, value_bool, ts_utc
        FROM {OPC_LATEST_VIEW}
        WHERE name IN ({placeholders})
        """, tuple(name_list))
        out = []
        for name, vbool, ts in rows or []:
            out.append({
                "name": name,
                "value_bool": int(vbool or 0),
                "ts_utc": (_coerce_to_datetime(ts) or datetime.now(timezone.utc)).isoformat(),
            })
        return JSONResponse({"rows": out})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"latest by_names error: {e}")

# -----------------------------------------------------------------------------
# Endpoint de sanity check (S1/S2 por atuador)
# -----------------------------------------------------------------------------
@app.get("/api/latch/check")
async def api_latch_check():
    try:
        latest = _LIVE_CACHE.get("vals") or _fetch_latest_rows_fast("ENV")
        res = {}
        for cfg in (_CFG_A1, _CFG_A2):
            s1n, s2n = _facet_names(cfg)
            s1 = int(latest.get(s1n, 0))
            s2 = int(latest.get(s2n, 0))
            if s1 == 1 and s2 == 0:
                st = "RECUADO"
            elif s1 == 0 and s2 == 1:
                st = "AVANCADO"
            elif s1 == 1 and s2 == 1:
                st = "INCONSISTENTE"
            else:
                st = "INDEFINIDO"
            res[cfg.id] = {"S1": s1, "S2": s2, "state": st}
        return JSONResponse(res)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"latch check error: {e}")

# -----------------------------------------------------------------------------
# Endpoint para obter nomes usados internamente (debug front)
# -----------------------------------------------------------------------------
@app.get("/api/debug/names")
async def api_debug_names():
    try:
        return JSONResponse({
            "names_latch": list(_NAMES_LATCH),
            "names_control": list(CONTROL_NAMES),
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"debug names error: {e}")

# -----------------------------------------------------------------------------
# Endpoints de ws echo (debug)
# -----------------------------------------------------------------------------
@app.websocket("/ws/echo")
async def ws_echo(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            txt = await ws.receive_text()
            await ws.send_text(txt)
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        try:
            await ws.close()
        except Exception:
            pass
# -----------------------------------------------------------------------------
# Funções auxiliares (cálculos de tempo, etc.) — placeholders
# -----------------------------------------------------------------------------
def _debounced_state(s1: int, s2: int, debounce_ms: int) -> Optional[str]:
    """
    Placeholder para eventual implementação com timestamps reais de transições.
    Mantido simples por ora.
    """
    if s1 == 1 and s2 == 0:
        return "RECUADO"
    if s1 == 0 and s2 == 1:
        return "AVANCADO"
    return None

def _timeout_check(cfg: _LatchCfg, state: Optional[str], started_at: Optional[datetime]) -> Optional[str]:
    """
    Placeholder para avaliar timeouts conforme expected_ms e timeout_factor.
    """
    return None

# -----------------------------------------------------------------------------
# Export minimal OpenAPI route (doc JSON)
# -----------------------------------------------------------------------------
@app.get("/openapi.json")
async def openapi_json():
    from fastapi.openapi.utils import get_openapi
    return get_openapi(
        title=app.title,
        version=app.version,
        routes=app.routes,
    )

# -----------------------------------------------------------------------------
# Opcional: endpoint para limpar buffer WS (debug)
# -----------------------------------------------------------------------------
@app.post("/api/debug/ws/clear")
async def api_debug_ws_clear(which: str = Query("live")):
    try:
        if which == "live":
            WS_LIVE.buf.clear()
        elif which == "mon":
            WS_MON.buf.clear()
        elif which == "slow":
            WS_SLOW.buf.clear()
        else:
            raise HTTPException(status_code=400, detail="which inválido")
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"ws clear error: {e}")

# -----------------------------------------------------------------------------
# Endpoint para testar exceções (debug)
# -----------------------------------------------------------------------------
@app.get("/api/debug/boom")
async def api_debug_boom():
    raise RuntimeError("boom")

# -----------------------------------------------------------------------------
# Compat: endpoint raiz
# -----------------------------------------------------------------------------
@app.get("/")
async def root():
    return {"service": "GM Digital Twin API", "version": app.version}
# -----------------------------------------------------------------------------
# EXTRA: endpoints simples para valores unitários (se útil no front)
# -----------------------------------------------------------------------------
@app.get("/api/value")
async def api_value(name: str = Query(...)):
    try:
        rows = fetch_all(f"""
        SELECT value_bool, ts_utc
        FROM {OPC_LATEST_VIEW}
        WHERE name=%s
        """, (name,))
        if not rows:
            return JSONResponse({"name": name, "value_bool": None, "ts_utc": None})
        vbool, ts = rows[0]
        return JSONResponse({
            "name": name,
            "value_bool": int(vbool or 0),
            "ts_utc": (_coerce_to_datetime(ts) or datetime.now(timezone.utc)).isoformat(),
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"value error: {e}")

@app.get("/api/values")
async def api_values(names: str = Query(..., description="CSV de nomes")):
    try:
        name_list = [x.strip() for x in names.split(",") if x.strip()]
        placeholders = ", ".join(["%s"] * len(name_list))
        rows = fetch_all(f"""
        SELECT name, value_bool, ts_utc
        FROM {OPC_LATEST_VIEW}
        WHERE name IN ({placeholders})
        """, tuple(name_list))
        out = []
        for n, vbool, ts in rows or []:
            out.append({
                "name": n,
                "value_bool": int(vbool or 0),
                "ts_utc": (_coerce_to_datetime(ts) or datetime.now(timezone.utc)).isoformat(),
            })
        return JSONResponse({"rows": out})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"values error: {e}")

# -----------------------------------------------------------------------------
# EXTRA: estado textual direto (A1/A2)
# -----------------------------------------------------------------------------
@app.get("/api/latch/state")
async def api_latch_state():
    try:
        latest = _LIVE_CACHE.get("vals") or _fetch_latest_rows_fast("ENV")
        res = {}
        for cfg in (_CFG_A1, _CFG_A2):
            s1n, s2n = _facet_names(cfg)
            s1 = int(latest.get(s1n, 0))
            s2 = int(latest.get(s2n, 0))
            if s1 == 1 and s2 == 0:
                st = "RECUADO"
            elif s1 == 0 and s2 == 1:
                st = "AVANCADO"
            elif s1 == 1 and s2 == 1:
                st = "INCONSISTENTE"
            else:
                st = "INDEFINIDO"
            res[cfg.id] = st
        return JSONResponse(res)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"latch state error: {e}")
# -----------------------------------------------------------------------------
# EXTRA: ping de conexão DB
# -----------------------------------------------------------------------------
@app.get("/api/debug/db_ping")
async def api_debug_db_ping():
    try:
        c, created = _ensure_mysql_connection("ENV")
        try:
            cur = c.cursor()
            cur.execute("SELECT 1")
            row = cur.fetchone()
            ok = row and row[0] == 1
            return {"ok": bool(ok)}
        finally:
            try:
                cur.close()
            except Exception:
                pass
            if created:
                try:
                    c.close()
                except Exception:
                    pass
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"db ping error: {e}")

# -----------------------------------------------------------------------------
# EXTRA: time references (para debugar fuso/epoch)
# -----------------------------------------------------------------------------
@app.get("/api/debug/now")
async def api_debug_now():
    try:
        utc_now = datetime.utcnow().replace(tzinfo=timezone.utc)
        local_naive = _now_for_db()
        return {
            "utc_now": utc_now.isoformat(),
            "local_naive": local_naive.isoformat(),
            "epoch_ms_from_local": _epoch_ms_from_local_naive(local_naive),
            "STORE_TZ": STORE_TZ,
            "LOCAL_TZ_OFFSET_SEC": LOCAL_TZ_OFFSET_SEC,
            "DEV_TIME_OFFSET_SEC": DEV_TIME_OFFSET_SEC,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"now error: {e}")

# -----------------------------------------------------------------------------
# EXTRA: força refresh do cache live (debug)
# -----------------------------------------------------------------------------
@app.post("/api/debug/force_refresh")
async def api_debug_force_refresh():
    try:
        latest = _fetch_latest_rows_fast("ENV")
        _LIVE_CACHE["vals"] = latest
        _LIVE_CACHE["ts"] = datetime.now(timezone.utc)
        return {"ok": True, "size": len(latest)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"force refresh error: {e}")
