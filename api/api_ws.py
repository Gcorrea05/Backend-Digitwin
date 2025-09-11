# api/api_ws.py
import os
import re
import json
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Any, Dict, Tuple, AsyncIterator

import numpy as np
from dotenv import load_dotenv, find_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import redis.asyncio as redis

# Carrega .env da raiz (se existir) e também api/.env
load_dotenv(find_dotenv())
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

# ------------ Config vindas do .env ------------
ALLOWED_ORIGINS = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "*").split(",")]
REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")

# Redis client (decodificação já em str)
rcli: redis.Redis = redis.from_url(
    REDIS_URL,
    decode_responses=True,
    health_check_interval=30,
)

# ------------ Imports locais de serviços ------------
from .routes import alerts
from .services.analytics import get_kpis
from .services.cycle import get_cycle_rate
from .services.analytics_graphs import get_vibration_data

# Banco via wrapper centralizado
from .database import get_db


# ------------ Helpers gerais ------------
def dt_to_iso_utc(dt: datetime) -> str:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
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


def mpu_id_from_str(s: str) -> int:
    s = (s or "").strip().upper()
    if s == "MPUA1":
        return 1
    if s == "MPUA2":
        return 2
    raise HTTPException(status_code=400, detail="id deve ser MPUA1 ou MPUA2")


# ------------ FastAPI / CORS ------------
app = FastAPI(title="Festo DT API+WS (live)", version="1.3.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if ALLOWED_ORIGINS == ["*"] else ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ------------ Registries p/ WS OPC/MPU ------------
subs_opc: Dict[str, set] = {}   # name -> set(WebSocket)
subs_mpu: Dict[str, set] = {}   # id   -> set(WebSocket)
clients_all_opc: set = set()    # ouvintes de todos OPC
clients_all_mpu: set = set()    # ouvintes de todos MPU


# ------------ Redis listeners (low-level streams) ------------
async def _pubsub_listen(channel: str) -> AsyncIterator[Dict[str, Any]]:
    pubsub = rcli.pubsub()
    await pubsub.subscribe(channel)
    try:
        async for msg in pubsub.listen():
            if msg.get("type") != "message":
                continue
            data = msg.get("data")
            # já deve ser str por decode_responses=True; ainda assim garantimos
            if isinstance(data, (bytes, bytearray)):
                try:
                    data = data.decode("utf-8")
                except Exception:
                    continue
            try:
                payload = json.loads(data)
            except Exception:
                payload = {"type": channel, "data": data}
            yield payload
    finally:
        try:
            await pubsub.unsubscribe(channel)
        finally:
            await pubsub.close()


async def listen_opc():
    async for ev in _pubsub_listen("opc_samples"):
        name = ev.get("name")
        payload = {"type": "opc_event", **ev}
        # por sinal
        for ws in list(subs_opc.get(name, set())):
            try:
                await ws.send_json(payload)
            except Exception:
                pass
        # todos
        for ws in list(clients_all_opc):
            try:
                await ws.send_json(payload)
            except Exception:
                pass


async def listen_mpu():
    async for ev in _pubsub_listen("mpu_samples"):
        id_str = ev.get("id")  # esperado "MPUA1"/"MPUA2"
        payload = {"type": "mpu_sample", **ev}
        for ws in list(subs_mpu.get(id_str, set())):
            try:
                await ws.send_json(payload)
            except Exception:
                pass
        for ws in list(clients_all_mpu):
            try:
                await ws.send_json(payload)
            except Exception:
                pass


@app.on_event("startup")
async def _startup():
    # listeners background p/ streams low-level (OPC/MPU)
    asyncio.create_task(listen_opc())
    asyncio.create_task(listen_mpu())
# ------------ HTTP básicos ------------
@app.get("/health")
def health():
    # DB ping
    row = fetch_one("SELECT NOW(6)")
    # Redis ping
    try:
        ok_redis = asyncio.get_event_loop().run_until_complete(rcli.ping())
    except Exception:
        ok_redis = False
    return {
        "status": "ok",
        "db_time": (str(row[0]) if row else None),
        "redis": bool(ok_redis),
    }


@app.get("/opc/latest")
def opc_latest(name: str):
    row = fetch_one(
        "SELECT ts_utc,name,value_bool FROM opc_samples WHERE name=%s ORDER BY ts_utc DESC LIMIT 1",
        (name,),
    )
    if not row:
        raise HTTPException(404, "sem dados")
    ts, n, vb = row
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
        SELECT ts_utc, mpu_id, ax_g, ay_g, az_g, gx_dps, gy_dps, gz_dps
        FROM mpu_samples WHERE mpu_id=%s ORDER BY ts_utc DESC LIMIT 1
        """,
        (mid,),
    )
    if not row:
        raise HTTPException(404, "sem dados")
    ts, mpu_id, ax, ay, az, gx, gy, gz = row
    return {
        "ts_utc": dt_to_iso_utc(ts),
        "id": ("MPUA1" if mpu_id == 1 else "MPUA2"),
        "ax_g": ax,
        "ay_g": ay,
        "az_g": az,
        "gx_dps": gx,
        "gy_dps": gy,
        "gz_dps": gz,
    }


# ------------ WS low-level (OPC/MPU via Redis) ------------
@app.websocket("/ws/opc")
async def ws_opc(ws: WebSocket, name: Optional[str] = None, all: Optional[bool] = False):
    await ws.accept()

    # snapshot inicial por nome (se fornecido)
    if name:
        row = fetch_one(
            "SELECT ts_utc,value_bool FROM opc_samples WHERE name=%s ORDER BY ts_utc DESC LIMIT 1",
            (name,),
        )
        if row:
            ts, vb = row
            await ws.send_json(
                {
                    "type": "hello",
                    "kind": "opc",
                    "name": name,
                    "last": {
                        "ts_utc": dt_to_iso_utc(ts),
                        "value_bool": (None if vb is None else bool(vb)),
                    },
                }
            )

    # registrar assinatura
    if name:
        subs_opc.setdefault(name, set()).add(ws)
    elif all:
        clients_all_opc.add(ws)

    try:
        while True:
            # envia ping a cada intervalo para manter conexão viva
            await ws.send_json({"type": "ping", "ts": datetime.utcnow().isoformat() + "Z"})
            await asyncio.sleep(WS_PING_INTERVAL)
    except Exception:
        # cliente fechou ou erro na conexão → encerra
        pass

@app.websocket("/ws/mpu")
async def ws_mpu(ws: WebSocket, id: Optional[str] = None, all: Optional[bool] = False):
    await ws.accept()

    # snapshot inicial por id (se fornecido)
    if id:
        mid = mpu_id_from_str(id)
        row = fetch_one(
            """
            SELECT ts_utc, ax_g, ay_g, az_g, gx_dps, gy_dps, gz_dps
            FROM mpu_samples WHERE mpu_id=%s ORDER BY ts_utc DESC LIMIT 1
            """,
            (mid,),
        )
        if row:
            ts, ax, ay, az, gx, gy, gz = row
            await ws.send_json(
                {
                    "type": "hello",
                    "kind": "mpu",
                    "id": id,
                    "last": {
                        "ts_utc": dt_to_iso_utc(ts),
                        "ax_g": ax,
                        "ay_g": ay,
                        "az_g": az,
                        "gx_dps": gx,
                        "gy_dps": gy,
                        "gz_dps": gz,
                        "temp_c": tc,
                    },
                }
            )

    # registrar assinatura
    if id:
        subs_mpu.setdefault(id, set()).add(ws)
    elif all:
        clients_all_mpu.add(ws)

    try:
        while True:
            await ws.send_json({"type": "ping", "ts": datetime.utcnow().isoformat() + "Z"})
            await asyncio.sleep(WS_PING_INTERVAL)
    except Exception:
        pass

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
    sql = "SELECT ts_utc,value_bool FROM opc_samples WHERE name=%s"
    params: Tuple[Any, ...] = (name,)
    if dt:
        sql += " AND ts_utc >= %s"
        params = (name, dt)
    sql += f" ORDER BY ts_utc {'ASC' if asc else 'DESC'} LIMIT %s OFFSET %s"
    params += (limit, offset)
    rows = fetch_all(sql, params)
    data = [
        {"ts_utc": dt_to_iso_utc(r[0]), "value_bool": (None if r[1] is None else bool(r[1]))}
        for r in rows
    ]
    return {"name": name, "count": len(data), "items": data}


@app.get("/mpu/ids")
def mpu_ids():
    rows = fetch_all("SELECT DISTINCT mpu_id FROM mpu_samples ORDER BY mpu_id ASC")

    def id_to_str(i: int) -> str:
        return "MPUA1" if i == 1 else "MPUA2" if i == 2 else f"MPU{i}"

    return {"ids": [id_to_str(r[0]) for r in rows]}


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
        SELECT ts_utc, ax_g, ay_g, az_g, gx_dps, gy_dps, gz_dps
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
    items = [
        {
            "ts_utc": dt_to_iso_utc(r[0]),
            "ax_g": r[1],
            "ay_g": r[2],
            "az_g": r[3],
            "gx_dps": r[4],
            "gy_dps": r[5],
            "gz_dps": r[6]
        }
        for r in rows
    ]
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
        "SELECT ts_utc, value_bool FROM opc_samples WHERE name=%s ORDER BY ts_utc DESC LIMIT 1",
        (name,),
    )


@app.get("/api/live/actuators/state", tags=["Live Dashboard"])
def live_actuators_state():
    out = []
    for aid, m in _SENSORS.items():
        r = _fetch_last_bool_row(m["recuado"])
        a = _fetch_last_bool_row(m["avancado"])
        ts_r = r[0] if r else None
        ts_a = a[0] if a else None
        vb_r = (None if not r or r[1] is None else int(r[1]))
        vb_a = (None if not a or a[1] is None else int(a[1]))
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
    # conta INICIA/PARA e infere ciclos
    rows = fetch_all(
        """
        SELECT name, COUNT(*)
        FROM opc_samples
        WHERE ts_utc >= %s
          AND name IN (%s, %s)
        GROUP BY name
        """,
        (start, _INICIA, _PARA),
    )
    c_inicia = 0
    c_para = 0
    for name, cnt in rows:
        if name == _INICIA:
            c_inicia = int(cnt)
        elif name == _PARA:
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
        SELECT mpu_id, ts_utc, ax_g, ay_g, az_g
        FROM mpu_samples
        WHERE ts_utc >= %s AND ts_utc < %s
        """,
        (start, now),
    )
    if not rows:
        return {"items": []}
    by: Dict[int, Dict[str, List[Any]]] = {}
    for mpu_id, ts, ax, ay, az in rows:
        d = by.setdefault(int(mpu_id), {"ax": [], "ay": [], "az": [], "ts": []})
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
# WS de alto nível (polling em serviços reais, ritmo 2s)
# =========================

@app.websocket("/ws/alerts")
async def ws_alerts(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            try:
                items = alerts.fetch_latest_alerts(limit=10)
            except Exception:
                items = []
            await ws.send_json({"type": "alerts", "items": items})
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        pass


@app.websocket("/ws/analytics")
async def ws_analytics(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            try:
                result = get_kpis()  # deve retornar dict serializável
            except Exception:
                result = {}
            await ws.send_json({"type": "analytics", **result})
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        pass


@app.websocket("/ws/cycles")
async def ws_cycles(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            try:
                # a função espera minutos; usamos 1 min como janela padrão do WS
                result = {"cpm": get_cycle_rate(window_minutes=1)}
            except Exception:
                result = {}
            await ws.send_json({"type": "cycles", **result})
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        pass


@app.websocket("/ws/vibration")
async def ws_vibration(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            try:
                # serviço dedicado para gráfico/agregado
                result = get_vibration_data(hours=0.01)  # ~36s de janela
            except Exception:
                result = {}
            await ws.send_json({"type": "vibration", **result})
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        pass
@app.websocket("/api/ws/snapshot")
async def ws_snapshot(ws: WebSocket):
    # aceita todas as origens; se precisar, valide ws.headers.get("origin")
    await ws.accept()
    try:
        while True:
            snap = {}
            # KPIs agregados
            try:
                snap.update({"analytics": get_kpis()})
            except Exception:
                snap.update({"analytics": {}})

            # taxa de ciclos (CPM ~ janela 1 min)
            try:
                cpm = get_cycle_rate(window_minutes=1)
                snap.update({"cycles": {"cpm": cpm}})
            except Exception:
                snap.update({"cycles": {}})

            # vibração (janela curtinha)
            try:
                vib = get_vibration_data(hours=0.01)  # ~36s
                snap.update({"vibration": vib})
            except Exception:
                snap.update({"vibration": {}})

            await ws.send_json({"type": "snapshot", **snap})
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        pass