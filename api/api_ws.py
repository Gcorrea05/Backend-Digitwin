# api/api_ws.py  —  Versão 100% HTTP (sem WebSockets / sem Redis)
import os
import re
import json
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Any, Dict, Tuple

import numpy as np
from dotenv import load_dotenv, find_dotenv
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# Carrega .env da raiz (se existir) e também api/.env
load_dotenv(find_dotenv())
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

# ------------ Config vindas do .env ------------
ALLOWED_ORIGINS = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "*").split(",")]

# ------------ Imports locais de serviços ------------
from .routes import alerts
from .services.analytics import get_kpis
from .services.cycle import get_cycle_rate
from .services.analytics_graphs import get_vibration_data

# Banco via wrapper centralizado
from .database import get_db


# ------------ Helpers gerais ------------
def _coerce_to_datetime(value: Any) -> Optional[datetime]:
    """Aceita datetime | str | int/float (epoch) e devolve datetime tz-aware em UTC."""
    if value is None:
        return None

    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(value, tz=timezone.utc)
    elif isinstance(value, str):
        s = value.strip()
        # tenta ISO (aceita 'Z')
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
    """Converte datetime/str/epoch para ISO-8601 UTC (sufixo 'Z')."""
    dt = _coerce_to_datetime(value)
    if dt is None:
        return None
    return dt.isoformat().replace("+00:00", "Z")


def fetch_one(q: str, params: Tuple[Any, ...] = ()):
    db = get_db()
    try:
        db.execute(q, params)
        return db.fetchone()
    finally:
        db.close()


def fetch_all(q: str, params: Tuple[Any, ...] = ()):
    db = get_db()
    try:
        db.execute(q, params)
        return db.fetchall()
    finally:
        db.close()


def first_col(row: Any) -> Any:
    """Retorna a primeira coluna, independente de tupla/dict."""
    if row is None:
        return None
    if isinstance(row, (list, tuple)):
        return row[0]
    if isinstance(row, dict):
        for _, v in row.items():
            return v
    return row


def col(row: Any, key_or_index: Any) -> Any:
    """Acessa linha retornada como dict/tupla de forma uniforme."""
    if row is None:
        return None
    if isinstance(row, dict):
        return row.get(key_or_index)
    if isinstance(key_or_index, int):
        return row[key_or_index]
    return None  # chave em tupla não existe


def cols(row: Any, *keys_or_idx: Any) -> Tuple[Any, ...]:
    """Extrai múltiplas colunas de um row dict/tupla."""
    return tuple(col(row, k) for k in keys_or_idx)


def mpu_id_from_str(s: str) -> int:
    s = (s or "").strip().upper()
    if s == "MPUA1":
        return 1
    if s == "MPUA2":
        return 2
    raise HTTPException(status_code=400, detail="id deve ser MPUA1 ou MPUA2")


def table_exists(schema: str, table: str) -> bool:
    q = """
    SELECT COUNT(*) AS c
    FROM information_schema.tables
    WHERE table_schema=%s AND table_name=%s
    """
    row = fetch_one(q, (schema, table))
    v = first_col(row)
    try:
        return int(v) > 0
    except Exception:
        return False


# ------------ FastAPI / CORS ------------
app = FastAPI(title="Festo DT API (HTTP only)", version="2.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if ALLOWED_ORIGINS == ["*"] else ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ------------ HTTP básicos ------------
@app.get("/health")
def health():
    row = fetch_one("SELECT NOW(6) AS now6")
    db_time_val = first_col(row) if row is not None else None
    return {
        "status": "ok",
        "db_time": (str(db_time_val) if db_time_val is not None else None),
    }


@app.get("/opc/latest")
def opc_latest(name: str):
    row = fetch_one(
        """
        SELECT
          ts_utc   AS ts_utc,
          name     AS name,
          value_bool AS value_bool
        FROM opc_samples
        WHERE name=%s
        ORDER BY ts_utc DESC
        LIMIT 1
        """,
        (name,),
    )
    if not row:
        raise HTTPException(404, "sem dados")

    ts, n, vb = cols(row, "ts_utc", "name", "value_bool")
    return {
        "ts_utc": dt_to_iso_utc(ts),
        "name": n,
        "value_bool": (None if vb is None else bool(vb)),
    }


@app.get("/mpu/latest")
def mpu_latest(id: str):
    mid = mpu_id_from_str(id)
    row = fetch_one(
        """
        SELECT
          ts_utc AS ts_utc,
          mpu_id AS mpu_id,
          ax_g, ay_g, az_g, gx_dps, gy_dps, gz_dps
        FROM mpu_samples
        WHERE mpu_id=%s
        ORDER BY ts_utc DESC
        LIMIT 1
        """,
        (mid,),
    )
    if not row:
        raise HTTPException(404, "sem dados")

    ts = col(row, "ts_utc")
    mpu_id = col(row, "mpu_id") or col(row, 1)
    ax, ay, az = col(row, "ax_g"), col(row, "ay_g"), col(row, "az_g")
    gx, gy, gz = col(row, "gx_dps"), col(row, "gy_dps"), col(row, "gz_dps")

    return {
        "ts_utc": dt_to_iso_utc(ts),
        "id": ("MPUA1" if int(mpu_id) == 1 else "MPUA2"),
        "ax_g": ax, "ay_g": ay, "az_g": az,
        "gx_dps": gx, "gy_dps": gy, "gz_dps": gz,
    }


# ------------ Parse de time window ------------
def _parse_since(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    s = s.strip().lower()

    m = re.fullmatch(r"-(\d+)([smhd])", s)
    if m:
        qty = int(m.group(1))
        unit = m.group(2)
        now = datetime.now(timezone.utc)
        if unit == "s":
            return now - timedelta(seconds=qty)
        if unit == "m":
            return now - timedelta(minutes=qty)
        if unit == "h":
            return now - timedelta(hours=qty)
        if unit == "d":
            return now - timedelta(days=qty)

    try:
        dt = datetime.fromisoformat(s.replace("z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        raise HTTPException(
            400,
            detail="since inválido. Use '-60s', '-15m', '-1h', '-7d' ou ISO8601.",
        )


# ------------ HTTP de histórico/listas ------------
@app.get("/opc/names")
def opc_names():
    rows = fetch_all("SELECT DISTINCT name FROM opc_samples ORDER BY name ASC")
    return {"names": [first_col(r) for r in rows]}


@app.get("/opc/history")
def opc_history(
    name: str = Query(..., description="Nome do sinal OPC"),
    since: Optional[str] = Query(None, description="ISO8601 ou relativo (-24h, -7d)"),
    limit: int = Query(20000, ge=1, le=20000),
    offset: int = Query(0, ge=0),
    asc: bool = Query(False, description="Ordena crescente se true"),
):
    dt = _parse_since(since)
    sql = """
        SELECT
          ts_utc AS ts_utc,
          value_bool AS value_bool
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
    items = []
    for r in rows:
        ts, vb = cols(r, "ts_utc", "value_bool")
        items.append(
            {
                "ts_utc": dt_to_iso_utc(ts),
                "value_bool": (None if vb is None else bool(vb)),
            }
        )
    return {"name": name, "count": len(items), "items": items}


@app.get("/mpu/ids")
def mpu_ids():
    rows = fetch_all("SELECT DISTINCT mpu_id AS mid FROM mpu_samples ORDER BY mpu_id ASC")

    def id_to_str(i: int) -> str:
        return "MPUA1" if i == 1 else "MPUA2" if i == 2 else f"MPU{i}"

    ids = []
    for r in rows:
        mid = col(r, "mid")
        if mid is None:
            mid = col(r, 0)
        ids.append(id_to_str(int(mid)))
    return {"ids": ids}


@app.get("/mpu/history")
def mpu_history(
    id: str = Query(..., description="MPUA1 ou MPUA2"),
    since: Optional[str] = Query(None, description="ISO8601 ou relativo (-1h, -24h)"),
    limit: int = Query(1000, ge=1, le=20000),
    offset: int = Query(0, ge=0),
    asc: bool = Query(False),
):
    mid = mpu_id_from_str(id)
    dt = _parse_since(since)
    sql = """
        SELECT
          ts_utc AS ts_utc,
          ax_g, ay_g, az_g, gx_dps, gy_dps, gz_dps
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
        ts = col(r, "ts_utc")
        ax, ay, az = col(r, "ax_g"), col(r, "ay_g"), col(r, "az_g")
        gx, gy, gz = col(r, "gx_dps"), col(r, "gy_dps"), col(r, "gz_dps")
        items.append(
            {
                "ts_utc": dt_to_iso_utc(ts),
                "ax_g": ax, "ay_g": ay, "az_g": az,
                "gx_dps": gx, "gy_dps": gy, "gz_dps": gz,
            }
        )
    return {"id": id, "count": len(items), "items": items}


# =========================
# LIVE DASHBOARD (HTTP)
# =========================

_SENSORS = {
    1: {"recuado": "Recuado_1S1", "avancado": "Avancado_1S2"},
    2: {"recuado": "Recuado_2S1", "avancado": "Avancado_2S2"},
}
_INICIA = "INICIA"
_PARA = "PARA"


def _decide_state(bit_recuado: Optional[int], bit_avancado: Optional[int]) -> str:
    if bit_recuado == 1 and bit_avancado == 0:
        return "fechado"
    if bit_avancado == 1 and bit_recuado == 0:
        return "aberto"
    if bit_recuado == 1 and bit_avancado == 1:
        return "erro"
    return "indef"


def _fetch_last_bool_row(name: str):
    return fetch_one(
        """
        SELECT
          ts_utc AS ts_utc,
          value_bool AS value_bool
        FROM opc_samples
        WHERE name=%s
        ORDER BY ts_utc DESC
        LIMIT 1
        """,
        (name,),
    )


@app.get("/api/live/actuators/state", tags=["Live Dashboard"])
def live_actuators_state():
    out = []
    for aid, m in _SENSORS.items():
        r = _fetch_last_bool_row(m["recuado"])
        a = _fetch_last_bool_row(m["avancado"])
        ts_r = col(r, "ts_utc") if r else None
        ts_a = col(a, "ts_utc") if a else None
        vb_r = None if not r else (None if col(r, "value_bool") is None else int(col(r, "value_bool")))
        vb_a = None if not a else (None if col(a, "value_bool") is None else int(col(a, "value_bool")))
        ts = max([t for t in (ts_r, ts_a) if t is not None], default=None)
        out.append(
            {
                "actuator_id": aid,
                "state": _decide_state(vb_r, vb_a),
                "ts": (dt_to_iso_utc(ts) if ts else None),
                "recuado": vb_r,
                "avancado": vb_a,
            }
        )
    return {"actuators": out}


def _clean_rms(values: List[float], max_g: float = 4.0) -> float:
    if not values:
        return 0.0
    a = np.array(values, dtype=float)
    a = a[np.isfinite(a)]
    if a.size == 0:
        return 0.0
    a = a[np.abs(a) <= max_g]
    if a.size == 0:
        return 0.0
    a = a - np.median(a)
    if a.size == 0:
        return 0.0
    return float((a**2).mean() ** 0.5)


@app.get("/api/live/cycles/rate", tags=["Live Dashboard"])
def live_cycles_rate(window_s: int = Query(5, ge=1, le=300)):
    now = datetime.now(timezone.utc)
    start = now - timedelta(seconds=window_s)
    rows = fetch_all(
        """
        SELECT name AS name, COUNT(*) AS cnt
        FROM opc_samples
        WHERE ts_utc >= %s
          AND name IN (%s, %s)
        GROUP BY name
        """,
        (start, _INICIA, _PARA),
    )
    c_inicia = 0
    c_para = 0
    for r in rows:
        nm, cnt = cols(r, "name", "cnt")
        if nm == _INICIA:
            c_inicia = int(cnt)
        elif nm == _PARA:
            c_para = int(cnt)
    pairs = min(c_inicia, c_para)
    cycles = pairs // 2
    cps = cycles / float(window_s)
    return {"window_seconds": window_s, "pairs_count": pairs, "cycles": cycles, "cycles_per_second": cps}


@app.get("/api/live/vibration", tags=["Live Dashboard"])
def live_vibration(window_s: int = Query(2, ge=1, le=60)):
    now = datetime.now(timezone.utc)
    start = now - timedelta(seconds=window_s)
    rows = fetch_all(
        """
        SELECT
          mpu_id AS mpu_id,
          ts_utc AS ts_utc,
          ax_g, ay_g, az_g
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
        rms_ax = _clean_rms(d["ax"])
        rms_ay = _clean_rms(d["ay"])
        rms_az = _clean_rms(d["az"])
        overall = float((rms_ax**2 + rms_ay**2 + rms_az**2) ** 0.5)
        items.append(
            {
                "mpu_id": mpu_id,
                "ts_start": dt_to_iso_utc(min(d["ts"])),
                "ts_end": dt_to_iso_utc(max(d["ts"])),
                "rms_ax": rms_ax,
                "rms_ay": rms_ay,
                "rms_az": rms_az,
                "overall": overall,
            }
        )
    return {"items": items}


# =========================
# ENDPOINTS — Monitoring (HTTP)
# =========================

@app.get("/api/live/runtime", tags=["Live Dashboard"])
def live_runtime():
    """
    Retorna tempo em execução (segundos) desde o último INICIA se ele for mais recente que o último PARA.
    Caso contrário, runtime = 0.
    """
    row_i = fetch_one("SELECT MAX(ts_utc) AS ts FROM opc_samples WHERE name=%s", (_INICIA,))
    row_p = fetch_one("SELECT MAX(ts_utc) AS ts FROM opc_samples WHERE name=%s", (_PARA,))
    ts_i = first_col(row_i)
    ts_p = first_col(row_p)

    if not ts_i:
        return {"runtime_seconds": 0, "since": None}

    if ts_p and _coerce_to_datetime(ts_p) >= _coerce_to_datetime(ts_i):
        return {"runtime_seconds": 0, "since": dt_to_iso_utc(ts_p)}

    now = datetime.now(timezone.utc)
    dt_i = _coerce_to_datetime(ts_i) or now
    runtime = max(0.0, (now - dt_i).total_seconds())
    return {"runtime_seconds": int(runtime), "since": dt_to_iso_utc(dt_i)}


@app.get("/api/live/actuators/timings", tags=["Live Dashboard"])
def live_actuator_timings():
    """
    Retorna, por atuador, as últimas durações:
      - dt_abre_s   (DTabre)
      - dt_fecha_s  (DTfecha)
      - dt_ciclo_s  (DTciclo)
    Se as tabelas de ciclos não existirem, retorna None nesses campos.
    """
    DB_NAME = os.getenv("MYSQL_DB") or os.getenv("DB_NAME") or ""

    def last_from_table(tname: str):
        if not DB_NAME or not table_exists(DB_NAME, tname):
            return None
        row = fetch_one(
            f"""
            SELECT
              ts_utc AS ts_utc,
              dt_abre_s AS dt_abre_s,
              dt_fecha_s AS dt_fecha_s,
              dt_ciclo_s AS dt_ciclo_s
            FROM {tname}
            ORDER BY ts_utc DESC
            LIMIT 1
            """
        )
        if not row:
            return None
        return {
            "ts_utc": dt_to_iso_utc(col(row, "ts_utc") or col(row, 0)),
            "dt_abre_s": col(row, "dt_abre_s"),
            "dt_fecha_s": col(row, "dt_fecha_s"),
            "dt_ciclo_s": col(row, "dt_ciclo_s"),
        }

    out = []
    r1 = last_from_table("cycles_atuador1")
    out.append({
        "actuator_id": 1,
        "last": (r1 or {"ts_utc": None, "dt_abre_s": None, "dt_fecha_s": None, "dt_ciclo_s": None})
    })
    r2 = last_from_table("cycles_atuador2")
    out.append({
        "actuator_id": 2,
        "last": (r2 or {"ts_utc": None, "dt_abre_s": None, "dt_fecha_s": None, "dt_ciclo_s": None})
    })

    return {"actuators": out}


# =========================
# SYSTEM STATUS para snapshot
# =========================

def _sev_from_ok(ok: bool) -> str:
    return "operational" if ok else "down"


def get_system_status() -> dict:
    """
    Monta um bloco 'system' para o snapshot:
      {
        "components": {
          "actuators": "operational|warning|down|unknown",
          "sensors":   "...",
          "transmission": "...",
          "control":   "..."
        }
      }
    """
    # --- Control (usa health: db)
    try:
        row = fetch_one("SELECT NOW(6) AS now6")
        db_ok = (row is not None and first_col(row) is not None)
    except Exception:
        db_ok = False
    control_sev = _sev_from_ok(db_ok)

    # --- Actuators (usa fins-de-curso recentes)
    try:
        states = live_actuators_state().get("actuators", [])
        any_valid = any(
            (it.get("recuado") in (0, 1)) or (it.get("avancado") in (0, 1))
            for it in states
        )
        actuators_sev = "operational" if any_valid else "unknown"
    except Exception:
        actuators_sev = "unknown"

    # --- Sensors (heurística simples: existem nomes em opc/names?)
    try:
        names = opc_names().get("names", [])
        sensors_sev = "operational" if len(names) > 0 else "unknown"
    except Exception:
        sensors_sev = "unknown"

    # --- Transmission (atividade recente de INICIA/PARA)
    try:
        now = datetime.now(timezone.utc)
        rows = fetch_all(
            """
            SELECT COUNT(*) AS cnt
            FROM opc_samples
            WHERE ts_utc >= %s AND name IN (%s, %s)
            """,
            (now - timedelta(seconds=10), _INICIA, _PARA),
        )
        cnt = int(first_col(rows[0]) if rows else 0)
        transmission_sev = "operational" if cnt > 0 else "warning"
    except Exception:
        transmission_sev = "unknown"

    return {
        "components": {
            "actuators": actuators_sev,
            "sensors": sensors_sev,
            "transmission": transmission_sev,
            "control": control_sev,
        }
    }


# =========================
# NOVAS ROTAS HTTP (equivalentes aos antigos WS)
# =========================

@app.get("/api/alerts")
def http_alerts(limit: int = Query(10, ge=1, le=100)):
    try:
        items = alerts.fetch_latest_alerts(limit=limit)
    except Exception:
        items = []
    return {"type": "alerts", "items": items}


@app.get("/api/analytics")
def http_analytics():
    try:
        result = get_kpis()  # dict serializável
    except Exception:
        result = {}
    return {"type": "analytics", **result}


@app.get("/api/cycles")
def http_cycles():
    """Atalho para cpm em janela de 1 minuto (mesma info que ws/cycles enviava)."""
    try:
        result = {"cpm": get_cycle_rate(window_minutes=1)}
    except Exception:
        result = {}
    return {"type": "cycles", **result}


@app.get("/api/vibration/window")
def http_vibration_window(seconds: float = Query(36.0, ge=1.0, le=300.0)):
    """Janela curta de vibração, equivalente ao ws que usava hours=0.01 (~36s)."""
    try:
        hours = seconds / 3600.0
        result = get_vibration_data(hours=hours)
    except Exception:
        result = {}
    return {"type": "vibration", **result}


@app.get("/api/snapshot")
def http_snapshot():
    """
    Agrega o 'snapshot' que o WS /api/ws/snapshot mandava periodicamente,
    para consumo via polling no front.
    """
    snap = {}
    try:
        snap.update({"analytics": get_kpis()})
    except Exception:
        snap.update({"analytics": {}})

    try:
        cpm = get_cycle_rate(window_minutes=1)
        snap.update({"cycles": {"cpm": cpm}})
    except Exception:
        snap.update({"cycles": {}})

    try:
        vib = get_vibration_data(hours=0.01)  # ~36s
        snap.update({"vibration": vib})
    except Exception:
        snap.update({"vibration": {}})

    try:
        sysblk = get_system_status()
        snap.update({"system": sysblk})
    except Exception:
        snap.update({"system": {"components": {}}})

    return {"type": "snapshot", **snap}

@app.get("/api/system/status", tags=["Live Dashboard"])
def system_status_http():
    return get_system_status()