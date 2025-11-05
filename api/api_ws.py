# api/api_ws.py
import os
import re
import time
import math
import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Any, Dict, Iterable, Tuple, Deque
import json

from dotenv import load_dotenv, find_dotenv
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from mysql.connector.errors import PoolError
from collections import deque
from hashlib import sha1
from bisect import bisect_right  # <-- novo (para a lógica de platôs)

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
DB_HOST = os.getenv("DB_HOST", "feierabendbier.ddns.net")
DB_PORT = int(os.getenv("DB_PORT", "9187"))
DB_USER = os.getenv("DB_USER", "gabs")
DB_PASS = os.getenv("DB_PASS", "ichbinpasqualesschlampe")
DB_NAME = os.getenv("DB_NAME", "gmdigital")
DB_POOL_NAME = os.getenv("DB_POOL_NAME", "gmdigital_pool")
DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "8"))

# ⚠️ Historico sempre em opc_samples; view opc_latest pode existir para debug
OPC_TABLE = os.getenv("OPC_TABLE", "opc_samples")
OPC_LATEST_VIEW = os.getenv("OPC_LATEST_VIEW", "opc_latest")
MPU_TABLE = os.getenv("MPU_TABLE", "mpu_samples")

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

# Timings robustos (parâmetros usados por outras heurísticas; mantidos por compat)
MON_TIMING_WINDOW_S = int(os.getenv("MON_TIMING_WINDOW_S", "60"))
MON_DEBOUNCE_MS     = int(os.getenv("MON_DEBOUNCE_MS", "80"))
MON_MIN_OPEN_MS     = int(os.getenv("MON_MIN_OPEN_MS", "80"))
MON_MIN_CLOSE_MS    = int(os.getenv("MON_MIN_CLOSE_MS", "80"))
MON_MAX_DT_MS       = int(os.getenv("MON_MAX_DT_MS", "5000"))

# 🔹 CPM (janela usada para estimar ciclos por minuto)
MON_CPM_WINDOW_S    = int(os.getenv("MON_CPM_WINDOW_S", "60"))

# 🔹 VIBRAÇÃO (janela curta para RMS/overall) — compat com “antigos”
MON_VIB_WINDOW_S    = int(os.getenv("MON_VIB_WINDOW_S", "2"))  # 2s por padrão

# -----------------------------------------------------------------------------
# Helpers de tempo
# -----------------------------------------------------------------------------
def _now_for_db() -> datetime:
    t = datetime.utcnow()  # base em UTC
    if STORE_TZ == "LOCAL":
        t = t + timedelta(seconds=LOCAL_TZ_OFFSET_SEC)  # vai pra UTC-3
    t = t + timedelta(seconds=DEV_TIME_OFFSET_SEC)
    return t.replace(tzinfo=None)

def _epoch_ms_from_local_naive(ts_local_naive: datetime) -> int:
    if ts_local_naive.tzinfo is not None:
        ts_utc = ts_local_naive.astimezone(timezone.utc)
        return int(ts_utc.timestamp() * 1000)
    ts_utc = ts_local_naive + timedelta(seconds=-LOCAL_TZ_OFFSET_SEC) if STORE_TZ == "LOCAL" else ts_local_naive
    return int(ts_utc.replace(tzinfo=timezone.utc).timestamp() * 1000)

def _coerce_to_datetime(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
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
    """
    Lê séries históricas do opc_samples (tabela), janela relativa (UTC),
    ordenado ASC. (Use apenas se seus ts_utc forem de fato UTC.)
    """
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
    for n in names:
        series.setdefault(n, [])
    return series

# -----------------------------------------------------------------------------
# Fallbacks/validações antigos (mantidos por compat para CPM etc.)
# -----------------------------------------------------------------------------
def _validated_edges(series: List[Tuple[datetime,int]], debounce_ms: int) -> Dict[str, List[datetime]]:
    rises: List[datetime] = []
    falls: List[datetime] = []
    if not series:
        return {"rises": rises, "falls": falls}

    for i in range(1, len(series)):
        ts, v = series[i]
        ts_prev, v_prev = series[i-1]
        if v == v_prev:
            continue
        ts_next = series[i+1][0] if i+1 < len(series) else None
        if ts_next is None:
            hold_ms = 10_000.0
        else:
            hold_ms = (ts_next - ts).total_seconds() * 1000.0
        if hold_ms is None or hold_ms >= debounce_ms:
            if v_prev == 0 and v == 1:
                rises.append(ts)
            elif v_prev == 1 and v == 0:
                falls.append(ts)
    return {"rises": rises, "falls": falls}

# -----------------------------------------------------------------------------
# >>> NOVO: Timings por "platôs" de estado (RECUADO/AVANÇADO visíveis na UI) <<<
# -----------------------------------------------------------------------------
def _val_at_or_last(seq: List[Tuple[datetime, int]], t: datetime) -> int:
    """Retorna o último valor <= t numa série [(ts, v)] ordenada ASC."""
    if not seq:
        return 0
    idx = bisect_right([x[0] for x in seq], t) - 1
    if idx < 0:
        return seq[0][1]
    return seq[idx][1]

def _state_from_s1s2(
    s1: List[Tuple[datetime, int]],
    s2: List[Tuple[datetime, int]],
) -> List[Tuple[datetime, str]]:
    """
    Reconstrói a linha do tempo de ESTADO:
      - 'RECUADO'  quando (S1=1, S2=0)
      - 'AVANCADO' quando (S1=0, S2=1)
      - 'TRANS'    nos demais casos (ambos 0 ou ambos 1)
    Saída: [(ts, state)] ordenado por ts (apenas pontos onde o estado muda).
    """
    if not s1 and not s2:
        return []

    times = sorted({t for t, _ in s1} | {t for t, _ in s2})
    if not times:
        return []

    out: List[Tuple[datetime, str]] = []
    prev_state: Optional[str] = None

    for t in times:
        v1 = _val_at_or_last(s1, t)
        v2 = _val_at_or_last(s2, t)
        if v1 == 1 and v2 == 0:
            st = "RECUADO"
        elif v1 == 0 and v2 == 1:
            st = "AVANCADO"
        else:
            st = "TRANS"
        if st != prev_state:
            out.append((t, st))
            prev_state = st

    return out

def _compress_runs(
    states: List[Tuple[datetime, str]],
    end_ts: Optional[datetime] = None,
) -> List[Tuple[datetime, datetime, str]]:
    """
    Comprime em segmentos estáveis (run-length):
    Entrada:  [(t0, ST0), (t1, ST1), ..., (tn, STn)]
    Saída:    [(start, end, ST), ...]  (end definido pelo próximo start; o último usa end_ts se passado)
    """
    if not states:
        return []

    runs: List[Tuple[datetime, datetime, str]] = []
    for i in range(len(states)):
        t0, st = states[i]
        t1 = states[i + 1][0] if i + 1 < len(states) else end_ts
        if t1 is None:
            break
        if t1 > t0:
            runs.append((t0, t1, st))
    return runs

def _last_plateau_timings(
    runs: List[Tuple[datetime, datetime, str]],
    min_plateau_s: float = 0.0,
) -> Dict[str, Optional[float]]:
    """
    Acha o último par útil de platôs consecutivos RECUADO ↔ AVANCADO e mede:
      - dt_abre  = duração do platô RECUADO imediatamente anterior a um AVANCADO
      - dt_fecha = duração do platô AVANCADO imediatamente anterior a um RECUADO
      - dt_ciclo = dt_abre + dt_fecha (quando ambos existirem)
    Ignora platôs 'TRANS'. Respeita limiar min_plateau_s (opcional).
    """
    if not runs:
        return {"dt_abre_s": None, "dt_fecha_s": None, "dt_ciclo_s": None}

    last_dt_abre: Optional[float] = None
    last_dt_fecha: Optional[float] = None

    useful = [
        (t0, t1, st)
        for (t0, t1, st) in runs
        if st in ("RECUADO", "AVANCADO") and (t1 - t0).total_seconds() >= min_plateau_s
    ]
    if not useful:
        return {"dt_abre_s": None, "dt_fecha_s": None, "dt_ciclo_s": None}

    # Procura do fim um padrão ... RECUADO -> AVANCADO -> RECUADO
    for i in range(len(useful) - 2, -1, -1):
        t0a, t1a, sta = useful[i]
        t0b, t1b, stb = useful[i + 1]
        if sta == "RECUADO" and stb == "AVANCADO":
            dt_abre = (t1a - t0a).total_seconds()
            # tenta fechar o ciclo com o próximo RECUADO
            if i + 2 < len(useful):
                t0c, t1c, stc = useful[i + 2]
                if stc == "RECUADO":
                    dt_fecha = (t1b - t0b).total_seconds()
                    return {
                        "dt_abre_s": round(dt_abre, 6),
                        "dt_fecha_s": round(dt_fecha, 6),
                        "dt_ciclo_s": round(dt_abre + dt_fecha, 6),
                    }
            # se não houver RECUADO seguinte, ainda assim guardamos o dt_abre
            last_dt_abre = dt_abre
            break

    # Se não formou ciclo, tenta pegar o último AVANCADO -> RECUADO (para dt_fecha)
    for i in range(len(useful) - 2, -1, -1):
        t0a, t1a, sta = useful[i]
        t0b, t1b, stb = useful[i + 1]
        if sta == "AVANCADO" and stb == "RECUADO":
            last_dt_fecha = (t1a - t0a).total_seconds()
            break

    return {
        "dt_abre_s": round(last_dt_abre, 6) if last_dt_abre is not None else None,
        "dt_fecha_s": round(last_dt_fecha, 6) if last_dt_fecha is not None else None,
        "dt_ciclo_s": (
            round(last_dt_abre + last_dt_fecha, 6)
            if (last_dt_abre is not None and last_dt_fecha is not None)
            else None
        ),
    }

def _timings_from_plateaus(
    s1_series: List[Tuple[datetime, int]],
    s2_series: List[Tuple[datetime, int]],
    end_ts: Optional[datetime] = None,
    min_plateau_s: float = 0.2,   # ajuste fino anti-glitch
) -> Dict[str, Optional[float]]:
    """
    Calcula dt_abre/dt_fecha/dt_ciclo a partir de PLATÔs (tempo visível em cada estado).
    - dt_abre  = duração do platô RECUADO imediatamente antes de AVANCADO
    - dt_fecha = duração do platô AVANCADO imediatamente antes de RECUADO
    - dt_ciclo = soma dos dois quando obtidos do mesmo bloco consecutivo
    """
    if end_ts is None:
        end_ts = datetime.utcnow().replace(tzinfo=timezone.utc)
    states = _state_from_s1s2(s1_series, s2_series)
    runs = _compress_runs(states, end_ts=end_ts)
    return _last_plateau_timings(runs, min_plateau_s=min_plateau_s)

# -----------------------------------------------------------------------------
# >>> VIBRAÇÃO (RMS por eixo + overall) conforme tabela atual (mpu_id/ts_utc) <<<
# -----------------------------------------------------------------------------
def fetch_mpu_window_local(window_s: int, tz_offset_sec: int = -10800):
    """
    Lê mpu_samples assumindo ts_utc gravado em UTC-3.
    A janela é (UTC_TIMESTAMP() + offset) - window_s.
    Retorna [{"mpu_id": mid, "overall": último_overall}, ...]
    """
    sql = """
    SELECT ts_utc, mpu_id, ax_g, ay_g, az_g
    FROM mpu_samples
    WHERE ts_utc >= (UTC_TIMESTAMP() + INTERVAL %s SECOND) - INTERVAL %s SECOND
      AND mpu_id IN (1,2)
    ORDER BY ts_utc
    """
    rows = fetch_all(sql, (tz_offset_sec, window_s)) or []

    by_id: Dict[int, List[Dict[str, Any]]] = {}
    for ts, mid, ax, ay, az in rows:
        try:
            overall = (float(ax)*float(ax) + float(ay)*float(ay) + float(az)*float(az)) ** 0.5
        except Exception:
            continue
        mid = int(mid)
        by_id.setdefault(mid, []).append({"ts": ts, "overall": overall})

    out = []
    for mid in sorted(by_id.keys()):
        serie = by_id[mid]
        out.append({"mpu_id": mid, "overall": serie[-1]["overall"] if serie else None})
    return out

def _vibration_items(window_s: int) -> List[Dict[str, float]]:
    """
    Calcula overall por sensor na janela dada.
    Usa fetch_mpu_window_local (ajuste UTC-3 apenas para MPU).
    """
    rows = fetch_mpu_window_local(window_s, -10800)
    if not rows:
        return []
    return rows

# -----------------------------------------------------------------------------
# OPC histórico assumindo ts_utc gravado em UTC-3 (somente aqui)
# -----------------------------------------------------------------------------
def fetch_opc_window_local(window_s: int, tz_offset_sec: int = -10800):
    """
    Lê opc_samples assumindo que ts_utc foi gravado em UTC-3.
    Janela: (UTC_TIMESTAMP() + offset) - window_s.
    Retorna {1: {'s1': [(ts,v)...], 's2': [...]}, 2: {...}}.
    """
    names = (
        'Recuado_1S1','Avancado_1S2',
        'Recuado_2S1','Avancado_2S2',
    )
    placeholders = ','.join(['%s'] * len(names))
    sql = f"""
    SELECT name, CAST(value_bool AS UNSIGNED) AS v, ts_utc
    FROM opc_samples
    WHERE name IN ({placeholders})
      AND ts_utc >= (UTC_TIMESTAMP() + INTERVAL %s SECOND) - INTERVAL %s SECOND
    ORDER BY ts_utc
    """
    rows = fetch_all(sql, (*names, tz_offset_sec, window_s)) or []

    out = {1: {'s1': [], 's2': []}, 2: {'s1': [], 's2': []}}
    for name, v, ts in rows:
        v = 1 if int(v) else 0
        ts_dt = _coerce_to_datetime(ts) or datetime.now(timezone.utc)  # <-- garantir tz-aware
        if   name == 'Recuado_1S1':  out[1]['s1'].append((ts_dt, v))
        elif name == 'Avancado_1S2': out[1]['s2'].append((ts_dt, v))
        elif name == 'Recuado_2S1':  out[2]['s1'].append((ts_dt, v))
        elif name == 'Avancado_2S2': out[2]['s2'].append((ts_dt, v))
    return out

# -----------------------------------------------------------------------------
# Monitoring payload (PLATÔs + vibração)
# -----------------------------------------------------------------------------
def build_monitoring_payload(window_s: int = 60):
    """
    Monitoring:
      - OPC histórico (UTC-3) -> items + timings (PLATÔs)
      - MPU histórico (UTC-3, curto) -> vibration.items (window_s=2)
    Não altera o 'Live'.
    """
    # OPC (UTC-3 apenas nesta leitura histórica)
    opc = fetch_opc_window_local(window_s=window_s, tz_offset_sec=-10800)

    # MPU (UTC-3 apenas nesta leitura histórica curta)
    vib_items = _vibration_items(2)

    items = [
        {"id": 1, "s1": opc[1]["s1"], "s2": opc[1]["s2"]},
        {"id": 2, "s1": opc[2]["s1"], "s2": opc[2]["s2"]},
    ]

    timings = []
    for aid in (1, 2):
        last = _timings_from_plateaus(
            opc[aid]["s1"], opc[aid]["s2"],
            end_ts=datetime.utcnow().replace(tzinfo=timezone.utc),
            min_plateau_s=0.2,
        )
        timings.append({"actuator_id": aid, "last": last})

    return {
        "type": "monitoring",
        "ref_ts": datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(),
        "window_s": int(window_s),
        "items": items,
        "timings": timings,
        "vibration": {"window_s": 2, "items": vib_items},
    }

# -----------------------------------------------------------------------------
# NEW: Compat endpoint -> /api/live/actuators/timings
# -----------------------------------------------------------------------------
@app.get("/api/live/actuators/timings")
async def api_live_actuators_timings(window_s: int = Query(60, ge=5, le=3600)):
    try:
        snap = build_monitoring_payload(window_s=window_s)
        return {"ts": snap.get("ref_ts"), "timings": snap.get("timings", [])}
    except Exception as e:
        print(f"[/api/live/actuators/timings] error: {e}")
        raise HTTPException(status_code=500, detail=f"erro ao montar timings: {e}")

# -----------------------------------------------------------------------------
# LIVE fast-paths
# -----------------------------------------------------------------------------
def _fetch_latest_rows_fast(names: Tuple[str, ...]) -> Dict[str, int]:
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
# Inferência de estado a partir do latest (para /live)
# -----------------------------------------------------------------------------
def _infer_state_from_latest(cfg: _LatchCfg, latest: Dict[str, int]) -> _LatchState:
    st = _LatchState()
    s1_name, s2_name = _facet_names(cfg)

    st.s1 = int(latest.get(s1_name, 0))
    st.s2 = int(latest.get(s2_name, 0))

    v_av = int(latest.get(cfg.v_av, 0))   # V*_14  -> AVANÇAR
    v_rec = int(latest.get(cfg.v_rec, 0)) # V*_12  -> RECUAR

    if st.s1 == 1 and st.s2 == 0:
        st.raw_state = "RECUADO"
    elif st.s1 == 0 and st.s2 == 1:
        st.raw_state = "AVANÇADO"
    elif st.s1 == 1 and st.s2 == 1:
        st.raw_state = None
        st.note = "S1=1 e S2=1 (inconsistente)"
    else:
        if v_av == 1 and v_rec == 0:
            st.raw_state = "AVANÇADO"
        elif v_rec == 1 and v_av == 0:
            st.raw_state = "RECUADO"
        else:
            st.raw_state = None
            st.note = "ambíguo por válvula (ambas 0 ou ambas 1)"

    if v_av and not v_rec:
        st.pending = "AVANÇAR"
    elif v_rec and not v_av:
        st.pending = "RECUAR"
    else:
        st.pending = None

    return st

# -----------------------------------------------------------------------------
# FSM mínimo (esperado + último estável)
# -----------------------------------------------------------------------------
_FSM_EXPECTED: Dict[str, Optional[str]] = {"A1": None, "A2": None}
_LAST_STABLE:  Dict[str, Optional[str]] = {"A1": None, "A2": None}

@app.post("/api/debug/fsm/command")
async def api_debug_fsm_command(
    actuator: str = Query(..., regex="^(A1|A2)$"),
    cmd: str = Query(..., regex="^(ADVANCE|RETRACT)$"),
):
    target = "A1" if actuator == "A1" else "A2"
    _FSM_EXPECTED[target] = "AVANCADO" if cmd == "ADVANCE" else "RECUADO"
    return {"ok": True, "actuator": target, "expected": _FSM_EXPECTED[target]}

# -----------------------------------------------------------------------------
# Payload de /api/live/snapshot
# -----------------------------------------------------------------------------
def build_live_payload() -> dict:
    latest = _LIVE_CACHE.get("vals") or {}
    a1 = _infer_state_from_latest(_CFG_A1, latest)
    a2 = _infer_state_from_latest(_CFG_A2, latest)

    now_local_naive = _now_for_db()
    ts_ms = _epoch_ms_from_local_naive(now_local_naive)
    ts_iso = datetime.utcfromtimestamp(ts_ms / 1000.0).replace(tzinfo=timezone.utc).isoformat()

    exp_a1 = _FSM_EXPECTED.get("A1")
    exp_a2 = _FSM_EXPECTED.get("A2")

    def _finalize(cfg_id: str, st: _LatchState, expected: Optional[str]) -> Dict[str, Any]:
        raw = st.raw_state
        reason = None
        confidence = "high"

        if raw is None:
            if expected in ("AVANÇADO", "RECUADO", "AVANCADO"):
                final = "AVANÇADO" if expected == "AVANCADO" else expected
                reason, confidence = "expected", "medium"
            elif _LAST_STABLE.get(cfg_id) in ("AVANÇADO", "RECUADO"):
                final = _LAST_STABLE[cfg_id]
                reason, confidence = "last_stable", "low"
            else:
                final = "RECUADO"
                reason, confidence = "default", "low"
        else:
            final = raw
            reason = "sensor_or_valve"

        if reason in ("sensor_or_valve", "expected"):
            if final in ("AVANÇADO", "RECUADO"):
                _LAST_STABLE[cfg_id] = final

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
# 🔹 CPM helpers (contagem a partir das bordas validadas)
# -----------------------------------------------------------------------------
def _cpm_from_edges(window_s: int,
                    s1_series: List[Tuple[datetime, int]],
                    s2_series: List[Tuple[datetime, int]]) -> float:
    """
    Estima CPM usando bordas validadas.
    Heurística: conta subidas (0→1) de S2 (AVANÇADO) como 1 ciclo por avanço.
    """
    if window_s <= 0:
        return 0.0
    _ = _validated_edges(s1_series, MON_DEBOUNCE_MS)  # mantido para futura heurística
    e2 = _validated_edges(s2_series, MON_DEBOUNCE_MS)
    advances = len(e2["rises"])  # cada avanço (S2↑) = 1 ciclo
    cpm = advances * (60.0 / float(window_s))
    return round(cpm, 2)


def build_cpm_payload(window_s: Optional[int] = None) -> dict:
    """
    Monta payload {type:'cpm', window_s, items:[{actuator_id, cpm}, ...]}.

    *** Importante ***
    Agora lê o histórico via fetch_opc_window_local(...) com tz_offset_sec=-10800
    (UTC-3), mantendo consistência com o Monitoring. Assim evitamos janela vazia
    quando os ts_utc do banco estão em UTC-3.
    """
    w = int(window_s or MON_CPM_WINDOW_S)

    # Usa a mesma leitura do Monitoring (UTC-3 no banco)
    opc = fetch_opc_window_local(window_s=w, tz_offset_sec=-10800)

    # Conta subidas validadas (0→1) de S2 (estado AVANÇADO) como 1 ciclo
    def _cpm_from_s2(series_s2: List[Tuple[datetime, int]]) -> float:
        e2 = _validated_edges(series_s2, MON_DEBOUNCE_MS)
        advances = len(e2["rises"])
        return round(advances * (60.0 / float(w)), 2)

    cpm1 = _cpm_from_s2(opc[1]["s2"])
    cpm2 = _cpm_from_s2(opc[2]["s2"])

    return {
        "type": "cpm",
        "ts": datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(),
        "window_s": w,
        "items": [
            {"actuator_id": 1, "cpm": cpm1},
            {"actuator_id": 2, "cpm": cpm2},
        ],
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
    hb_sec = max(0.5, WS_HEARTBEAT_MS / 1000.0)
    last_compact = None
    while True:
        try:
            try:
                await asyncio.wait_for(_LIVE_DIRTY.wait(), timeout=hb_sec)
            except asyncio.TimeoutError:
                try:
                    await WS_LIVE.broadcast('{"type":"hb"}')
                except Exception:
                    pass
                continue

            _LIVE_DIRTY.clear()

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
                _LIVE_DIRTY.set()
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
    period = max(1.0, SLOW_TICK_MS / 1000.0)        # cadência do “slow”
    cpm_period = max(2.0, int(os.getenv("CPM_TICK_MS", "10000")) / 1000.0)  # cadência do CPM
    next_t = time.perf_counter()
    next_cpm = time.perf_counter()  # dispara já no começo

    while True:
        next_t += period
        try:
            # pacote leve (marcador de atividade do canal)
            payload = {"type": "slow", "ts": datetime.utcnow().isoformat() + "Z"}
            WS_SLOW.buf.append(payload)
            await WS_SLOW.broadcast(json.dumps(payload))

            # envia CPM conforme cadência própria
            now = time.perf_counter()
            if now >= next_cpm:
                cpm_payload = build_cpm_payload(MON_CPM_WINDOW_S)
                WS_SLOW.buf.append(cpm_payload)
                await WS_SLOW.broadcast(json.dumps(cpm_payload, ensure_ascii=False, default=str))
                next_cpm = now + cpm_period

        except Exception as e:
            print(f"[slow_producer_loop] error: {e}")

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

@app.get("/api/health")
async def api_health_alias():
    return await health()

@app.get("/api/live/snapshot")
async def api_live_snapshot():
    try:
        payload = build_live_payload()
        return JSONResponse(payload)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"snapshot error: {e}")

@app.get("/api/monitoring/snapshot")
def api_monitoring_snapshot(window_s: int = Query(60, ge=5, le=600)):
    """
    Snapshot do Monitoring. Só leitura histórica; Live permanece igual.
    """
    try:
        payload = build_monitoring_payload(window_s=window_s)
        return payload
    except Exception as e:
        print(f"[monitoring/snapshot] erro: {e}", flush=True)
        raise HTTPException(status_code=500, detail=f"monitoring error: {e}")

# --------- HOT-PATH: collector empurra mudanças ---------
@app.post("/api/live/push_bit_update")
async def api_live_push_bit_update(body: Dict[str, Any] = Body(...)):
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

# 🔹 Fallback HTTP para CPM (útil para testes/debug)
@app.get("/api/slow/cpm")
async def api_slow_cpm(window_s: int = Query(MON_CPM_WINDOW_S, ge=10, le=600)):
    try:
        return JSONResponse(build_cpm_payload(window_s))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"cpm error: {e}")

# -----------------------------------------------------------------------------
# Endpoints MPU (alinhados ao esquema atual)
# -----------------------------------------------------------------------------
def _fetch_mpu_latest(conn_like=None, limit: int = 20):
    c, created = _ensure_mysql_connection(conn_like)
    try:
        cur = c.cursor()
        cur.execute(f"""
        SELECT mpu_id, ax_g, ay_g, az_g, gx_dps, gy_dps, gz_dps, ts_utc
        FROM {MPU_TABLE}
        ORDER BY ts_utc DESC
        LIMIT %s
        """, (limit,))
        rows = cur.fetchall() or []
        out = []
        for mpu_id, ax_g, ay_g, az_g, gx_dps, gy_dps, gz_dps, ts_utc in rows:
            ts = _coerce_to_datetime(ts_utc) or datetime.now(timezone.utc)
            out.append({
                "mpu_id": int(mpu_id),
                "ax_g": float(ax_g or 0),
                "ay_g": float(ay_g or 0),
                "az_g": float(az_g or 0),
                "gx_dps": float(gx_dps or 0),
                "gy_dps": float(gy_dps or 0),
                "gz_dps": float(gz_dps or 0),
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

def _parse_since_to_seconds(s: str) -> int:
    """
    Aceita formatos tipo '-10m', '-30s', '-1h', '-2h30m'. Default: 600s (10m).
    """
    if not s:
        return 600
    s = s.strip().lower()
    if not s.startswith("-"):
        return 600
    s = s[1:]
    total = 0
    num = ""
    unit = ""
    for ch in s:
        if ch.isdigit():
            if unit:
                if unit == "h": total += int(num) * 3600
                elif unit == "m": total += int(num) * 60
                elif unit == "s": total += int(num)
                num, unit = "", ""
            num += ch
        else:
            unit = ch if ch in "hms" else unit
    if num:
        if unit == "h": total += int(num) * 3600
        elif unit == "m": total += int(num) * 60
        else: total += int(num)
    return max(1, total or 600)

@app.get("/api/mpu/history")
def api_mpu_history(
    id: int = Query(..., ge=1, le=2, description="ID do MPU (1 ou 2)"),
    since: str = Query("-10m", description="Janela relativa (ex.: -5s, -2m, -1h)"),
    limit: int = Query(2000, ge=1, le=20000),
    asc: int = Query(1, description="1 = ASC, 0 = DESC"),
):
    """
    Histórico de MPU considerando que ts_utc no banco está em UTC-3.
    Apenas MPU usa este ajuste; nada de OPC é alterado aqui.
    """
    def _parse_since_to_seconds_local(expr: str) -> int:
        s = (expr or "").strip().lower()
        if not s.startswith("-"):
            if s.isdigit():
                return int(s) * 60
            raise HTTPException(status_code=400, detail="Parâmetro 'since' deve ser relativo, ex.: -10m, -5s.")
        m = re.match(r"^-\s*(\d+)\s*([smhd])?$", s)
        if not m:
            raise HTTPException(status_code=400, detail="Formato inválido para 'since'. Use -5s, -2m, -1h, -1d.")
        qty = int(m.group(1))
        unit = (m.group(2) or "m")
        mult = {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]
        return qty * mult

    try:
        window_seconds = _parse_since_to_seconds_local(since)
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=400, detail="Não foi possível interpretar 'since'.")

    try:
        tz_offset_sec = int(os.getenv("LOCAL_TZ_OFFSET_SEC", "-10800"))
    except Exception:
        tz_offset_sec = -10800

    order = "ASC" if asc else "DESC"

    sql = f"""
    SELECT ts_utc, mpu_id, ax_g, ay_g, az_g, gx_dps, gy_dps, gz_dps
    FROM mpu_samples
    WHERE mpu_id = %s
      AND ts_utc >= (UTC_TIMESTAMP() + INTERVAL %s SECOND) - INTERVAL %s SECOND
    ORDER BY ts_utc {order}
    LIMIT %s
    """

    rows: List[Tuple[Any, ...]] = fetch_all(sql, (id, tz_offset_sec, window_seconds, limit)) or []

    items = []
    for r in rows:
        ts, mid, ax, ay, az, gx, gy, gz = r
        items.append({
            "ts": ts,
            "mpu_id": int(mid),
            "ax_g": float(ax), "ay_g": float(ay), "az_g": float(az),
            "gx_dps": float(gx), "gy_dps": float(gy), "gz_dps": float(gz),
        })

    return {
        "id": id,
        "since": since,
        "count": len(items),
        "items": items,
    }

@app.get("/api/db/mpu_values")
async def api_db_mpu_values(limit: int = Query(100, ge=1, le=5000)):
    try:
        rows = fetch_all(f"""
        SELECT mpu_id, ax_g, ay_g, az_g, gx_dps, gy_dps, gz_dps, ts_utc
        FROM {MPU_TABLE}
        ORDER BY ts_utc DESC
        LIMIT %s
        """, (limit,))
        out = []
        for mpu_id, ax_g, ay_g, az_g, gx_dps, gy_dps, gz_dps, ts_utc in rows or []:
            out.append({
                "mpu_id": int(mpu_id),
                "ax_g": float(ax_g or 0),
                "ay_g": float(ay_g or 0),
                "az_g": float(az_g or 0),
                "gx_dps": float(gx_dps or 0),
                "gy_dps": float(gy_dps or 0),
                "gz_dps": float(gz_dps or 0),
                "ts_utc": (_coerce_to_datetime(ts_utc) or datetime.now(timezone.utc)).isoformat(),
            })
        return JSONResponse({"rows": out})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"db mpu_values error: {e}")

# -----------------------------------------------------------------------------
# Dashboard helpers
# -----------------------------------------------------------------------------
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

# -----------------------------------------------------------------------------
# WebSockets
# -----------------------------------------------------------------------------
@app.websocket("/ws/live")
async def ws_live(ws: WebSocket):
    await WS_LIVE.add(ws)
    try:
        snap = build_live_payload()
        try:
            await ws.send_text(json.dumps({**snap, "snapshot": True}, ensure_ascii=False, default=str))
        except Exception:
            pass

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
                "value_bool": int(vbool or 0),
                "ts_utc": (_coerce_to_datetime(ts) or datetime.now(timezone.utc)).isoformat(),
            })
        return JSONResponse({"rows": out})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"db opc_latest error: {e}")

# -----------------------------------------------------------------------------
# Utilidades extras
# -----------------------------------------------------------------------------
@app.get("/openapi.json")
async def openapi_json():
    from fastapi.openapi.utils import get_openapi
    return get_openapi(title=app.title, version=app.version, routes=app.routes)

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

# -----------------------------------------------------------------------------
# Simulation Catalog / Scenarios / Draw (error_catalog + error_scenarios)
# -----------------------------------------------------------------------------
def _json_or(v, fallback):
    try:
        if v is None:
            return fallback
        s = v if isinstance(v, str) else str(v)
        s = s.strip()
        if not s:
            return fallback
        return json.loads(s)
    except Exception:
        return fallback

def _load_catalog() -> Dict[str, Dict[str, Any]]:
    rows = fetch_all(
        """
        SELECT id, code, name, grp, severity, stop_required, description, default_actions
        FROM error_catalog
        ORDER BY id
        """
    ) or []
    by_code: Dict[str, Dict[str, Any]] = {}
    for (eid, code, name, grp, sev, stop_req, desc, def_actions) in rows:
        by_code[str(code)] = {
            "id": int(eid),                # <--- IMPORTANTE p/ sua tela
            "code": str(code),
            "name": name or str(code),
            "grp": grp or "SISTEMA",
            "severity": int(sev or 0),
            "stop_required": int(stop_req or 0),
            "description": desc,
            "default_actions": _json_or(def_actions, []),
            "scenarios": [],               # sobressalente (a UI ignora se não usar)
        }
    return by_code

def _load_scenarios(code: Optional[str] = None) -> List[Dict[str, Any]]:
    if code:
        rows = fetch_all(
            """
            SELECT s.scenario_id, c.id AS error_id, c.code, c.name, c.grp, c.severity,
                   s.title, s.description, s.signal_overrides, s.expected_alert, s.actions
            FROM error_scenarios s
            JOIN error_catalog c ON c.id = s.error_id
            WHERE c.code = %s
            ORDER BY s.scenario_id
            """,
            (code,),
        ) or []
    else:
        rows = fetch_all(
            """
            SELECT s.scenario_id, c.id AS error_id, c.code, c.name, c.grp, c.severity,
                   s.title, s.description, s.signal_overrides, s.expected_alert, s.actions
            FROM error_scenarios s
            JOIN error_catalog c ON c.id = s.error_id
            ORDER BY s.scenario_id
            """
        ) or []

    out: List[Dict[str, Any]] = []
    for (sid, err_id, ccode, cname, cgrp, csev, title, desc, sig_over, exp_alert, actions) in rows:
        out.append({
            "scenario_id": int(sid),
            "code": str(ccode),
            "error": {                       # facilita o POST /draw
                "id": int(err_id),
                "code": str(ccode),
                "name": cname or str(ccode),
                "grp": cgrp or "SISTEMA",
                "severity": int(csev or 0),
            },
            "title": title or f"Scenario {sid}",
            "description": desc,
            "signal_overrides": _json_or(sig_over, {}),
            "expected_alert": _json_or(exp_alert, {}),
            "actions": _json_or(actions, []),
        })
    return out

@app.get("/api/simulation/catalog")
async def api_simulation_catalog():
    """
    Retorna o catálogo a partir de error_catalog + scenarios aninhados.
    Formato: { items: [{id, code, name, grp, severity, ...}] }
    """
    try:
        by_code = _load_catalog()
        for sc in _load_scenarios():
            if sc["code"] in by_code:
                by_code[sc["code"]]["scenarios"].append(sc)
        # sua tela usa normalizeCatalog(j.items)
        return JSONResponse({"items": list(by_code.values())})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"catalog error: {e}")

@app.get("/api/simulation/scenarios")
async def api_simulation_scenarios(code: Optional[str] = Query(default=None, description="Filtra por error code")):
    """
    Lista de cenários; use ?code=VIB_HIGH para filtrar.
    """
    try:
        items = _load_scenarios(code.strip() if code else None)
        return JSONResponse(items)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"scenarios error: {e}")

@app.post("/api/simulation/draw")
async def api_simulation_draw(body: Dict[str, Any] = Body(...)):
    """
    Gera um Scenario mínimo viável no formato esperado pela sua UI.
    Entrada típica: { "mode": "by_code", "code": "VIB_HIGH" }
    Resposta: conforme o tipo Scenario do front.
    """
    try:
        mode = str(body.get("mode", "by_code")).strip().lower()
        code = str(body.get("code", "")).strip() if mode == "by_code" else ""

        # Busca o erro pelo code
        if not code:
            raise HTTPException(status_code=400, detail="Informe 'code'")

        cat = _load_catalog()
        if code not in cat:
            raise HTTPException(status_code=404, detail=f"code '{code}' não encontrado em error_catalog")

        err = cat[code]

        # Pega o primeiro cenário cadastrado para esse code (se houver) para enriquecer
        sc_list = _load_scenarios(code)
        sc = sc_list[0] if sc_list else None

        # Heurística simples para escolher atuador
        actuator = 1 if code in ("STATE_STUCK", "CYCLE_SLOW") else 2
        if isinstance(body.get("actuator"), int) and body["actuator"] in (1, 2):
            actuator = int(body["actuator"])

        # Monta no shape exato que sua tela espera
        scenario = {
            "scenario_id": str(sc["scenario_id"]) if sc else f"{code}-1",
            "actuator": actuator,
            "error": {
                "id": int(err["id"]),
                "code": err["code"],
                "name": err["name"],
                "grp": err["grp"],
                "severity": int(err.get("severity") or 0),
            },
            "cause": (sc.get("description") if sc else err.get("description")) or "—",
            "actions": sc["actions"] if sc and isinstance(sc.get("actions"), list) else (err.get("default_actions") or []),
            "params": sc.get("signal_overrides", {}) if sc else {},
            "ui": {
                "halt_sim": True,
                "halt_3d": False,
                "show_popup": True,
            },
            "resume_allowed": True if int(err.get("stop_required") or 0) == 0 else False,
        }

        return JSONResponse(scenario)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"draw error: {e}")

