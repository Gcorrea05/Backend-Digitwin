# api/api_ws.py
import os
import re
import time
import math
import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Any, Dict, Tuple, Deque
import json

from dotenv import load_dotenv, find_dotenv
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from mysql.connector.errors import PoolError
from collections import deque
from hashlib import sha1

# -----------------------------------------------------------------------------
# .env
# -----------------------------------------------------------------------------
load_dotenv(find_dotenv())
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

# -----------------------------------------------------------------------------
# FastAPI + CORS
# -----------------------------------------------------------------------------
app = FastAPI(title="GM Digital Twin API", version="0.8.0")

ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*")
allow_origins = ["*"] if ALLOWED_ORIGINS == "*" else [x.strip() for x in ALLOWED_ORIGINS.split(",") if x.strip()]

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

# ⚠️ Historico sempre em opc_samples; view opc_latest pode existir para debug
OPC_TABLE = os.getenv("OPC_TABLE", "opc_samples")
OPC_LATEST_VIEW = os.getenv("OPC_LATEST_VIEW", "opc_latest")
MPU_TABLE = os.getenv("MPU_TABLE", "mpu_values")

STORE_TZ = os.getenv("STORE_TZ", "UTC").upper()           # "UTC" ou "LOCAL"
LOCAL_TZ_OFFSET_SEC = int(os.getenv("LOCAL_TZ_OFFSET_SEC", "-10800"))  # UTC-3
DEV_TIME_OFFSET_SEC = int(os.getenv("DEV_TIME_OFFSET_SEC", "0"))

# -----------------------------------------------------------------------------
# Limites / Janelas
# -----------------------------------------------------------------------------
LIVE_TICK_MS       = int(os.getenv("LIVE_TICK_MS", "100"))   # 100 ms
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
    t = t + timedelta(seconds=DEV_TIME_OFFSET_SEC)
    return t.replace(tzinfo=None)

def _epoch_ms_from_local_naive(ts_local_naive: datetime) -> int:
    """
    Converte um datetime 'naive' (local se STORE_TZ=='LOCAL') para epoch ms em UTC.
    """
    if ts_local_naive.tzinfo is not None:
        ts_utc = ts_local_naive.astimezone(timezone.utc)
        return int(ts_utc.timestamp() * 1000)
    ts_utc = ts_local_naive + timedelta(seconds=-LOCAL_TZ_OFFSET_SEC) if STORE_TZ == "LOCAL" else ts_local_naive
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

_POOL: Optional[Any] = None

def _ensure_mysql_pool() -> Any:
    global _POOL
    if _POOL is not None:
        return _POOL
    if mysql is None or MySQLConnectionPool is None:
        raise RuntimeError("mysql.connector não disponível")
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
    if conn_like is None or (isinstance(conn_like, str) and conn_like.lower() in ("env","dsn","default")):
        try:
            pool = _ensure_mysql_pool()
            return pool.get_connection(), True
        except Exception:
            pass
    if conn_like is not None:
        try:
            if getattr(conn_like, "is_connected", lambda: False)():
                return conn_like, False
        except Exception:
            pass
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
        try: cur.close()
        except Exception: pass
        if created:
            try: c.close()
            except Exception: pass

def fetch_all(sql: str, params: Tuple[Any, ...] = (), conn_like: Any = None):
    c, created = _ensure_mysql_connection(conn_like)
    try:
        cur = c.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        return rows
    finally:
        try: cur.close()
        except Exception: pass
        if created:
            try: c.close()
            except Exception: pass

def col(row: Any, key: Any, default=None):
    if row is None:
        return default
    if isinstance(row, dict):
        return row.get(key, default)
    try:
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
    raw_state: Optional[str] = None  # "RECUADO" | "AVANÇADO" | None (indef)
    pending: Optional[str] = None    # "AVANÇAR" | "RECUAR" | None
    last_state: Optional[str] = None
    note: Optional[str] = None

# -----------------------------------------------------------------------------
# CFGs
# -----------------------------------------------------------------------------
# Regra: V*_14 = AVANÇADO ; V*_12 = RECUADO
# Regra: S1 = RECUADO ; S2 = AVANÇADO
_CFG_A1 = _LatchCfg(
    id="A1", expected_ms=500, debounce_ms=80, timeout_factor=1.5,
    v_av="V1_14",  # AVANÇAR
    v_rec="V1_12", # RECUAR
    s_rec="Avancado_1S2",
    s_adv="Recuado_1S1",
)
_CFG_A2 = _LatchCfg(
    id="A2", expected_ms=500, debounce_ms=80, timeout_factor=1.5,
    v_av="V2_14",
    v_rec="V2_12",
    s_rec="Avancado_2S2",
    s_adv="Recuado_2S1",
)

# -----------------------------------------------------------------------------
# Mapeamento S1/S2 a partir dos nomes (regex simples)
# -----------------------------------------------------------------------------
def _facet_names(cfg: _LatchCfg) -> Tuple[str, str]:
    s1, s2 = None, None
    for n in (cfg.s_adv, cfg.s_rec):
        m = re.search(r"_S([12])\b", n, re.IGNORECASE)
        if m:
            if m.group(1) == "1":
                s1 = n
            elif m.group(1) == "2":
                s2 = n
    if s1 is None: s1 = cfg.s_adv
    if s2 is None: s2 = cfg.s_rec
    return s1, s2

# Pré-listas para consultas
a1_s1, a1_s2 = _facet_names(_CFG_A1)
a2_s1, a2_s2 = _facet_names(_CFG_A2)
_NAMES_LATCH = (a1_s1, a1_s2, a2_s1, a2_s2)
CONTROL_NAMES = (_CFG_A1.v_av, _CFG_A1.v_rec, _CFG_A2.v_av, _CFG_A2.v_rec)
_NAMES_ALL = _NAMES_LATCH + CONTROL_NAMES  # sensores + válvulas

# -----------------------------------------------------------------------------
# Leitura para endpoints HTTP (view pode ser usada aqui)
# -----------------------------------------------------------------------------
def _fetch_latest_rows(names: Tuple[str, ...]) -> Dict[str, Dict[str, Any]]:
    if not names:
        return {}
    placeholders = ", ".join(["%s"] * len(names))
    sql = f"""
    SELECT name, value_bool, ts_utc
    FROM {OPC_LATEST_VIEW}
    WHERE name IN ({placeholders})
    """
    rows = fetch_all(sql, names) or []
    out: Dict[str, Dict[str, Any]] = {}
    for name, vbool, ts in rows:
        out[str(name)] = {
            "value": int(vbool or 0),
            "value_bool": int(vbool or 0),
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
# Monitoring payload
# -----------------------------------------------------------------------------
def build_monitoring_payload() -> dict:
    """
    Monta séries de S1/S2 dos dois atuadores em uma janela configurável.
    Usa _fetch_series (lendo direto de opc_samples) para minimizar lag.
    """
    WINDOW_S_PRIMARY = int(os.getenv("MON_TIMING_WINDOW_S", "60"))
    window_s = max(1, WINDOW_S_PRIMARY)

    # Nomes corretos de S1/S2 para cada atuador, a partir das CFGs
    a1_s1, a1_s2 = _facet_names(_CFG_A1)
    a2_s1, a2_s2 = _facet_names(_CFG_A2)

    names = [a1_s1, a1_s2, a2_s1, a2_s2]
    series = _fetch_series(names, window_s)

    # timestamp de referência (agora) — ISO com TZ UTC
    ref_ts = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

    return {
        "type": "monitoring",
        "ref_ts": ref_ts,
        "window_s": window_s,
        "items": [
            {
                "id": 1,
                "s1": [(ts.isoformat(), v) for (ts, v) in series.get(a1_s1, [])],
                "s2": [(ts.isoformat(), v) for (ts, v) in series.get(a1_s2, [])],
            },
            {
                "id": 2,
                "s1": [(ts.isoformat(), v) for (ts, v) in series.get(a2_s1, [])],
                "s2": [(ts.isoformat(), v) for (ts, v) in series.get(a2_s2, [])],
            },
        ],
    }


# -----------------------------------------------------------------------------
# LIVE fast-paths
# -----------------------------------------------------------------------------
def _fetch_latest_rows_fast(names: Tuple[str, ...]) -> Dict[str, int]:
    """
    Lê o último value_bool (0/1) de cada 'name' diretamente em opc_samples
    usando join no MAX(ts_utc). Exige índice (name, ts_utc).
    """
    out: Dict[str, int] = {}
    if not names:
        return out
    c, created = _ensure_mysql_connection("ENV")
    try:
        cur = c.cursor()
        placeholders = ", ".join(["%s"] * len(names))
        sql = f"""
        SELECT s.name, s.value_bool
        FROM {OPC_TABLE} AS s
        JOIN (
            SELECT name, MAX(ts_utc) AS max_ts
            FROM {OPC_TABLE}
            WHERE name IN ({placeholders})
            GROUP BY name
        ) AS m
          ON m.name = s.name AND m.max_ts = s.ts_utc
        """
        cur.execute(sql, names)
        for n, vb in (cur.fetchall() or []):
            out[str(n)] = 1 if (vb in (1, True, "1", "true", "TRUE")) else 0
        # garantir chaves faltantes:
        for n in names:
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
    s1_name, s2_name = _facet_names(cfg)

    # Sensores (primário)
    st.s1 = int(latest.get(s1_name, 0))
    st.s2 = int(latest.get(s2_name, 0))

    # Válvulas (fallback / intenção)
    v_av = int(latest.get(cfg.v_av, 0))   # V*_14  -> AVANÇAR
    v_rec = int(latest.get(cfg.v_rec, 0)) # V*_12  -> RECUAR

    # 1) Primeiro: decisão por sensores (estados estáveis)
    if st.s1 == 1 and st.s2 == 0:
        st.raw_state = "RECUADO"
    elif st.s1 == 0 and st.s2 == 1:
        st.raw_state = "AVANÇADO"
    elif st.s1 == 1 and st.s2 == 1:
        st.raw_state = None
        st.note = "S1=1 e S2=1 (inconsistente)"
    else:
        # 2) S1=0 e S2=0 -> inferir posição pela válvula energizada
        if v_av == 1 and v_rec == 0:
            st.raw_state = "AVANÇADO"
        elif v_rec == 1 and v_av == 0:
            st.raw_state = "RECUADO"
        else:
            # ambas 0 ou ambas 1 -> ambíguo; fallback final será no build_live_payload
            st.raw_state = None
            st.note = "ambíguo por válvula (ambas 0 ou ambas 1)"

    # pending baseado em válvulas (sinal de intenção/comando)
    if v_av and not v_rec:
        st.pending = "AVANÇAR"
    elif v_rec and not v_av:
        st.pending = "RECUAR"
    else:
        st.pending = None

    return st

# -----------------------------------------------------------------------------
# FSM mínimo em memória para "estado esperado" + último estável
# -----------------------------------------------------------------------------
_FSM_EXPECTED: Dict[str, Optional[str]] = {"A1": None, "A2": None}
_LAST_STABLE:  Dict[str, Optional[str]] = {"A1": None, "A2": None}

@app.post("/api/debug/fsm/command")
async def api_debug_fsm_command(
    actuator: str = Query(..., regex="^(A1|A2)$"),
    cmd: str = Query(..., regex="^(ADVANCE|RETRACT)$"),
):
    """
    Debug: alimenta 'expected' sem DB.
    ADVANCE -> 'AVANCADO'; RETRACT -> 'RECUADO'.
    """
    target = "A1" if actuator == "A1" else "A2"
    _FSM_EXPECTED[target] = "AVANCADO" if cmd == "ADVANCE" else "RECUADO"
    return {"ok": True, "actuator": target, "expected": _FSM_EXPECTED[target]}

# -----------------------------------------------------------------------------
# Payload de /api/live/snapshot (com expected/mismatch)
# -----------------------------------------------------------------------------
def build_live_payload() -> dict:
    # lê cache do sampler / push
    latest = _LIVE_CACHE.get("vals") or {}
    a1 = _infer_state_from_latest(_CFG_A1, latest)
    a2 = _infer_state_from_latest(_CFG_A2, latest)

    # timestamp coerente com STORE_TZ
    now_local_naive = _now_for_db()
    ts_ms = _epoch_ms_from_local_naive(now_local_naive)
    ts_iso = datetime.utcfromtimestamp(ts_ms / 1000.0).replace(tzinfo=timezone.utc).isoformat()

    # "estado esperado" (FSM em memória; opcionalmente alimentado por /api/debug/fsm/command)
    exp_a1 = _FSM_EXPECTED.get("A1")
    exp_a2 = _FSM_EXPECTED.get("A2")

    def _finalize(cfg_id: str, st: _LatchState, expected: Optional[str]) -> Dict[str, Any]:
        # escolha do estado final com fallback:
        raw = st.raw_state
        reason = None
        confidence = "high"

        if raw is None:
            # 1) expected guia (medium)
            if expected in ("AVANÇADO", "RECUADO", "AVANCADO"):
                final = "AVANÇADO" if expected == "AVANCADO" else expected
                reason, confidence = "expected", "medium"
            # 2) último estável (low)
            elif _LAST_STABLE.get(cfg_id) in ("AVANÇADO", "RECUADO"):
                final = _LAST_STABLE[cfg_id]
                reason, confidence = "last_stable", "low"
            # 3) default conservador (low)
            else:
                final = "RECUADO"
                reason, confidence = "default", "low"
        else:
            final = raw
            reason = "sensor_or_valve"

        # atualiza último estável quando for confiável (não-default)
        if reason in ("sensor_or_valve", "expected"):
            if final in ("AVANÇADO", "RECUADO"):
                _LAST_STABLE[cfg_id] = final

        # normalização ascii
        state_ascii = "AVANCADO" if final == "AVANÇADO" else final

        mismatch = None
        if expected and state_ascii:
            expected_norm = "AVANÇADO" if expected == "AVANCADO" else expected
            if expected_norm != final:
                mismatch = "INCONSISTENT"

        return {
            "raw_state": st.raw_state,
            "resolved_state": final,
            "reason": reason,
            "confidence": confidence,
            "s1": st.s1, "s2": st.s2,
            "s1_ts": st.s1_ts.isoformat() if st.s1_ts else None,
            "s2_ts": st.s2_ts.isoformat() if st.s2_ts else None,
            "expected": expected,
            "pending": st.pending,
            "last_state": _LAST_STABLE.get(cfg_id),
            "note": st.note,
            "mismatch_kind": mismatch,
            # compat
            "state": final,
            "state_ascii": state_ascii,
            "is_avancado": state_ascii == "AVANCADO",
            "is_recuado": state_ascii == "RECUADO",
        }

    a1_pack = _finalize("A1", a1, exp_a1)
    a2_pack = _finalize("A2", a2, exp_a2)

    legacy = {
        "ts": ts_iso,
        "actuators": [
            {"id": 1, "actuator_id": 1, "state": a1_pack["state_ascii"], "pending": None, "fault": None, "elapsed_ms": None, "started_at": None},
            {"id": 2, "actuator_id": 2, "state": a2_pack["state_ascii"], "pending": None, "fault": None, "elapsed_ms": None, "started_at": None},
        ],
    }

    return {
        "type": "live",
        "ts_ms": ts_ms,
        "a1": a1_pack,
        "a2": a2_pack,
        "ts": legacy["ts"],
        "actuators": legacy["actuators"],
    }

# -----------------------------------------------------------------------------
# WebSocket infra (buffer limitado = backpressure)
# -----------------------------------------------------------------------------
class WsHub:
    def __init__(self, maxlen: int = WS_BUFFER_MAX):
        self.conns: List[WebSocket] = []
        self.buf: Deque[dict] = deque(maxlen=maxlen)

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

WS_LIVE = WsHub(maxlen=WS_BUFFER_MAX)
WS_MON  = WsHub(maxlen=WS_BUFFER_MAX)
WS_SLOW = WsHub(maxlen=WS_BUFFER_MAX)

# -----------------------------------------------------------------------------
# Loops de produção (push-on-change)
# -----------------------------------------------------------------------------
_LIVE_CACHE: Dict[str, Any] = {"vals": {}, "ts": None}
_LIVE_DIRTY = asyncio.Event()

async def hot_drain_loop():
    while True:
        await asyncio.sleep(0.250)

async def live_producer_loop():
    """
    Envia imediatamente quando o sampler/push marcar dirty; caso contrário, heartbeat leve.
    """
    hb_sec = max(0.5, WS_HEARTBEAT_MS / 1000.0)
    last_compact = None
    while True:
        try:
            # aguarda mudança ou timeout para heartbeat
            try:
                await asyncio.wait_for(_LIVE_DIRTY.wait(), timeout=hb_sec)
            except asyncio.TimeoutError:
                try:
                    await WS_LIVE.broadcast('{"type":"hb"}')
                except Exception:
                    pass
                continue

            _LIVE_DIRTY.clear()

            # verificação leve (evita recomputar JSON caro sem necessidade)
            vals = _LIVE_CACHE.get("vals") or {}
            compact = (
                vals.get("Recuado_1S1",0), vals.get("Avancado_1S2",0),
                vals.get("Recuado_2S1",0), vals.get("Avancado_2S2",0),
                vals.get("V1_12",0), vals.get("V1_14",0),
                vals.get("V2_12",0), vals.get("V2_14",0),
            )
            if compact == last_compact:
                continue
            last_compact = compact

            payload = build_live_payload()
            await WS_LIVE.broadcast(json.dumps(payload, ensure_ascii=False, default=str))
        except Exception as e:
            print(f"[live_producer_loop] failed: {e}")
            await asyncio.sleep(0.05)

async def live_sampler_loop():
    """
    Consulta dedicada aos 8 nomes (S1/S2 e válvulas) em opc_samples; atualiza cache
    e dispara envio imediato quando houver mudança.
    """
    period = max(0.050, LIVE_TICK_MS / 1000.0)
    next_t = time.perf_counter()
    while True:
        next_t += period
        try:
            t0 = time.perf_counter()
            latest = await asyncio.to_thread(_fetch_latest_rows_fast, _NAMES_ALL)
            t_db = (time.perf_counter() - t0) * 1000.0
            if latest != _LIVE_CACHE.get("vals"):
                _LIVE_CACHE["vals"] = latest
                _LIVE_CACHE["ts"] = datetime.now(timezone.utc)
                _LIVE_DIRTY.set()  # <- dispara envio imediato
            if t_db > 10:
                print(f"[live_sampler_loop] t_db={t_db:.2f} ms")
        except Exception as e:
            print(f"[live_sampler_loop] latest read failed: {e}")
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

# --------- HOT-PATH: collector empurra mudanças (sub-50ms) ---------
@app.post("/api/live/push_bit_update")
async def api_live_push_bit_update(body: Dict[str, Any] = Body(...)):
    """
    Body: {"name":"<OPC tag>", "value":0|1}
    Atualiza o cache live e dispara envio imediato se houver mudança.
    """
    try:
        n = str(body.get("name", "")).strip()
        v = 1 if int(body.get("value", 0)) else 0
        if not n:
            raise HTTPException(status_code=400, detail="name obrigatório")
        vals = _LIVE_CACHE.setdefault("vals", {})
        if vals.get(n) != v:
            vals[n] = v
            _LIVE_CACHE["ts"] = datetime.now(timezone.utc)
            _LIVE_DIRTY.set()
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"push_bit_update error: {e}")

@app.post("/api/live/push_bulk")
async def api_live_push_bulk(body: Dict[str, Any] = Body(...)):
    """
    Body: {"items":[{"name":"Recuado_1S1","value":1}, ...]}
    Atualiza vários bits de uma vez e dispara um único envio.
    """
    try:
        items = body.get("items", [])
        if not isinstance(items, list):
            raise HTTPException(status_code=400, detail="items deve ser lista")
        changed = False
        vals = _LIVE_CACHE.setdefault("vals", {})
        for it in items:
            n = str(it.get("name", "")).strip()
            if not n:
                continue
            v = 1 if int(it.get("value", 0)) else 0
            if vals.get(n) != v:
                vals[n] = v
                changed = True
        if changed:
            _LIVE_CACHE["ts"] = datetime.now(timezone.utc)
            _LIVE_DIRTY.set()
        return {"ok": True, "changed": changed}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"push_bulk error: {e}")

@app.get("/api/opc/latest")
async def api_opc_latest():
    try:
        rows = fetch_all(f"SELECT name, value_bool, ts_utc FROM {OPC_LATEST_VIEW}")
        out = []
        for name, vbool, ts in rows or []:
            out.append({
                "name": name,
                "value": int(vbool or 0),
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
        out = {k: [(ts.isoformat(), v) for (ts, v) in vs] for k, vs in data.items()}
        return JSONResponse({"series": out})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"series error: {e}")

# -----------------------------------------------------------------------------
# WebSockets (snapshot no handshake do LIVE)
# -----------------------------------------------------------------------------
@app.websocket("/ws/live")
async def ws_live(ws: WebSocket):
    await WS_LIVE.add(ws)
    try:
        # snapshot primeiro
        snap = build_live_payload()
        try:
            await ws.send_text(json.dumps({**snap, "snapshot": True}, ensure_ascii=False, default=str))
        except Exception:
            pass

        # flush últimos itens
        for item in list(WS_LIVE.buf)[-10:]:
            try:
                await ws.send_text(json.dumps(item, ensure_ascii=False))
            except Exception:
                pass

        hb_period = max(0.1, WS_HEARTBEAT_MS / 1000.0)
        while True:
            try:
                _ = await asyncio.wait_for(ws.receive_text(), timeout=hb_period)
            except asyncio.TimeoutError:
                try:
                    await ws.send_text('{"type":"hb"}')
                except Exception:
                    break
            except WebSocketDisconnect:
                break
            except Exception:
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
        while True:
            try:
                _ = await asyncio.wait_for(ws.receive_text(), timeout=hb_period)
            except asyncio.TimeoutError:
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
        while True:
            try:
                _ = await asyncio.wait_for(ws.receive_text(), timeout=hb_period)
            except asyncio.TimeoutError:
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
# DEBUG / utilidades
# -----------------------------------------------------------------------------
@app.get("/api/debug/ping")
async def api_debug_ping():
    return {"pong": True, "ts": datetime.utcnow().isoformat() + "Z"}

@app.get("/api/debug/state")
async def api_debug_state():
    vals = _LIVE_CACHE.get("vals") or {}
    return JSONResponse({"live_vals": vals, "ts": datetime.utcnow().isoformat()+"Z"})

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
        try: cur.close()
        except Exception: pass
        if created:
            try: c.close()
            except Exception: pass

@app.get("/api/mpu/latest")
async def api_mpu_latest(limit: int = Query(20, ge=1, le=200)):
    try:
        out = _fetch_mpu_latest("ENV", limit=limit)
        return JSONResponse({"rows": out})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"mpu latest error: {e}")

def _etag_for(obj: Any) -> str:
    s = json.dumps(obj, sort_keys=True, ensure_ascii=False, default=str)
    return sha1(s.encode("utf-8")).hexdigest()

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

def _rows_for_names(names: List[str], window_s: int) -> Dict[str, List[Dict[str, Any]]]:
    raw = _fetch_series(names, window_s)
    out: Dict[str, List[Dict[str, Any]]] = {}
    for k, arr in raw.items():
        out[k] = [{"ts": ts.isoformat(), "value": v} for (ts, v) in arr]
    return out

@app.get("/api/monitoring/rows")
async def api_monitoring_rows(window_s: int = Query(60, ge=1, le=600)):
    try:
        names = list(_NAMES_LATCH)
        data = _rows_for_names(names, window_s)
        return JSONResponse({"window_s": window_s, "rows": data})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"monitoring rows error: {e}")

def _set_valve(name: str, value: int, conn_like=None) -> int:
    return 0  # placeholder

@app.post("/api/valve/set")
async def api_valve_set(body: Dict[str, Any] = Body(...)):
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

def _bool(v: Any) -> int:
    try:
        return 1 if int(v) else 0
    except Exception:
        s = str(v).strip().lower()
        return 1 if s in ("1", "true", "on", "yes") else 0

def _normalize_name(n: str) -> str:
    return re.sub(r"\s+", "_", n.strip())

@app.get("/api/series/latch")
async def api_series_latch(window_s: int = Query(60, ge=1, le=600)):
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

@app.get("/api/db/opc_values")
async def api_db_opc_values(limit: int = Query(100, ge=1, le=5000)):
    try:
        rows = fetch_all(f"""
        SELECT name, value_bool, ts_utc
        FROM {OPC_TABLE}
        ORDER BY ts_utc DESC
        LIMIT %s
        """, (limit,))
        out = []
        for name, vbool, ts in rows or []:
            out.append({
                "name": name,
                "value": int(vbool or 0),
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
        SELECT name, value_bool, ts_utc
        FROM {OPC_LATEST_VIEW}
        """)
        out = []
        for name, vbool, ts in rows or []:
            out.append({
                "name": name,
                "value": int(vbool or 0),
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

@app.get("/api/dashboard/summary")
async def api_dashboard_summary():
    try:
        live = build_live_payload()
        mon  = build_monitoring_payload()
        mpu  = _fetch_mpu_latest("ENV", limit=10)
        return JSONResponse({"live": live, "monitoring": mon, "mpu": mpu})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"dashboard summary error: {e}")

@app.get("/api/live/compact")
async def api_live_compact():
    try:
        latest = _LIVE_CACHE.get("vals") or _fetch_latest_rows_fast(_NAMES_LATCH)
        a1 = _infer_state_from_latest(_CFG_A1, latest)
        a2 = _infer_state_from_latest(_CFG_A2, latest)
        return JSONResponse({
            "a1": {"s1": a1.s1, "s2": a1.s2, "state": a1.raw_state or a1.last_state},
            "a2": {"s1": a2.s1, "s2": a2.s2, "state": a2.raw_state or a2.last_state},
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"live compact error: {e}")

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

@app.get("/api/latch/check")
async def api_latch_check():
    try:
        latest = _LIVE_CACHE.get("vals") or _fetch_latest_rows_fast(_NAMES_LATCH)
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

@app.get("/api/debug/names")
async def api_debug_names():
    try:
        return JSONResponse({
            "names_latch": list(_NAMES_LATCH),
            "names_control": list(CONTROL_NAMES),
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"debug names error: {e}")

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
# OpenAPI JSON
# -----------------------------------------------------------------------------
@app.get("/openapi.json")
async def openapi_json():
    from fastapi.openapi.utils import get_openapi
    return get_openapi(title=app.title, version=app.version, routes=app.routes)

# -----------------------------------------------------------------------------
# Debug WS buffer clear / boom / db_ping / now / force_refresh
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

@app.get("/api/debug/boom")
async def api_debug_boom():
    raise RuntimeError("boom")

@app.get("/")
async def root():
    return {"service": "GM Digital Twin API", "version": app.version}

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

@app.get("/api/latch/state")
async def api_latch_state():
    try:
        latest = _LIVE_CACHE.get("vals") or _fetch_latest_rows_fast(_NAMES_LATCH)
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
            try: cur.close()
            except Exception: pass
            if created:
                try: c.close()
                except Exception: pass
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"db ping error: {e}")

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

@app.post("/api/debug/force_refresh")
async def api_debug_force_refresh():
    try:
        latest = _fetch_latest_rows_fast(_NAMES_LATCH)
        _LIVE_CACHE["vals"] = latest
        _LIVE_CACHE["ts"] = datetime.now(timezone.utc)
        _LIVE_DIRTY.set()
        return {"ok": True, "size": len(latest)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"force refresh error: {e}")
