# api/api_ws.py
import os, io, json, asyncio
import numpy as np
from typing import Optional, List, Any, Dict, Tuple
from datetime import datetime, timezone, timedelta

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, Response, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

import redis.asyncio as redis
import mysql.connector
from mysql.connector.pooling import MySQLConnectionPool

load_dotenv()

# ---------- env ----------
ALLOWED_ORIGINS = [o.strip() for o in os.getenv("ALLOWED_ORIGINS","*").split(",")]
REDIS_URL = os.getenv("REDIS_URL","redis://localhost:6379/0")
POOL = MySQLConnectionPool(
    pool_name="festo_pool",
    pool_size=8,
    host=os.getenv("MYSQL_HOST","localhost"),
    port=int(os.getenv("MYSQL_PORT","3306")),
    database=os.getenv("MYSQL_DB","festo_dt"),
    user=os.getenv("MYSQL_USER","root"),
    password=os.getenv("MYSQL_PASS",""),
    autocommit=True
)

def dt_to_iso_utc(dt: datetime) -> str:
    if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat().replace("+00:00","Z")

def fetch_one(q: str, params: Tuple[Any,...]=()):
    conn=POOL.get_connection()
    try:
        cur=conn.cursor()
        cur.execute(q, params)
        row = cur.fetchone()
        cur.close()
        return row
    finally:
        conn.close()

def fetch_all(q: str, params: Tuple[Any,...]=()):
    conn=POOL.get_connection()
    try:
        cur=conn.cursor()
        cur.execute(q, params)
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        conn.close()

def mpu_id_from_str(s: str) -> int:
    s=s.strip().upper()
    if s=="MPUA1": return 1
    if s=="MPUA2": return 2
    raise HTTPException(status_code=400, detail="id deve ser MPUA1 ou MPUA2")

app = FastAPI(title="Festo DT API+WS (live)", version="1.2.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS if ALLOWED_ORIGINS!=["*"] else ["*"],
    allow_credentials=True, allow_methods=["*"], allow_headers=["*"]
)

# ---------- HTTP mínimos ----------
@app.get("/health")
def health():
    row = fetch_one("SELECT NOW(6)")
    return {"status":"ok","db_time": (str(row[0]) if row else None)}

@app.get("/opc/latest")
def opc_latest(name: str):
    row = fetch_one("SELECT ts_utc,name,value_bool FROM opc_samples WHERE name=%s ORDER BY ts_utc DESC LIMIT 1",(name,))
    if not row: raise HTTPException(404, "sem dados")
    ts,n,vb = row
    return {"ts_utc": dt_to_iso_utc(ts), "name": n, "value_bool": (None if vb is None else bool(vb))}

@app.get("/mpu/latest")
def mpu_latest(id: str):
    mid = mpu_id_from_str(id)
    row = fetch_one("""
        SELECT ts_utc, mpu_id, ax_g, ay_g, az_g, gx_dps, gy_dps, gz_dps, temp_c
        FROM mpu_samples WHERE mpu_id=%s ORDER BY ts_utc DESC LIMIT 1
    """,(mid,))
    if not row: raise HTTPException(404, "sem dados")
    ts, mpu_id, ax, ay, az, gx, gy, gz, tc = row
    return {"ts_utc": dt_to_iso_utc(ts), "id": ("MPUA1" if mpu_id==1 else "MPUA2"),
            "ax_g": ax, "ay_g": ay, "az_g": az, "gx_dps": gx, "gy_dps": gy, "gz_dps": gz, "temp_c": tc}

# ---------- WS registries ----------
subs_opc: Dict[str, set] = {}     # name -> set(WebSocket)
subs_mpu: Dict[str, set] = {}     # id   -> set(WebSocket)
clients_all_opc: set = set()      # para quem quer “todos OPC”
clients_all_mpu: set = set()      # para quem quer “todos MPU”

# ---------- Redis listeners ----------
rcli: redis.Redis = redis.from_url(REDIS_URL, decode_responses=True)

async def listen_opc():
    pubsub = rcli.pubsub()
    await pubsub.subscribe("opc_samples")
    async for msg in pubsub.listen():
        if msg["type"] != "message": continue
        try: ev = json.loads(msg["data"])  # {"type":"opc_event","ts_utc":...,"name":...,"value_bool":...}
        except Exception: continue
        name = ev.get("name")
        payload = {"type":"opc_event", **ev}
        # broadcast p/ inscritos no nome específico
        for ws in list(subs_opc.get(name, set())):
            try: await ws.send_json(payload)
            except Exception: pass
        # broadcast p/ quem ouve “all”
        for ws in list(clients_all_opc):
            try: await ws.send_json(payload)
            except Exception: pass

async def listen_mpu():
    pubsub = rcli.pubsub()
    await pubsub.subscribe("mpu_samples")
    async for msg in pubsub.listen():
        if msg["type"] != "message": continue
        try: ev = json.loads(msg["data"])  # {"type":"mpu_sample","id":"MPUA1",...}
        except Exception: continue
        id_str = ev.get("id")
        payload = {"type":"mpu_sample", **ev}
        for ws in list(subs_mpu.get(id_str, set())):
            try: await ws.send_json(payload)
            except Exception: pass
        for ws in list(clients_all_mpu):
            try: await ws.send_json(payload)
            except Exception: pass

'''@app.on_event("startup")
async def _startup():
    asyncio.create_task(listen_opc())
    asyncio.create_task(listen_mpu())
'''
# ---------- WS endpoints ----------
@app.websocket("/ws/opc")
async def ws_opc(ws: WebSocket, name: Optional[str] = None, all: Optional[bool] = False):
    await ws.accept()
    # foto inicial (opcional por name)
    if name:
        row = fetch_one("SELECT ts_utc,value_bool FROM opc_samples WHERE name=%s ORDER BY ts_utc DESC LIMIT 1",(name,))
        if row:
            ts,vb = row
            await ws.send_json({"type":"hello","kind":"opc","name":name,
                                "last": {"ts_utc": dt_to_iso_utc(ts), "value_bool": (None if vb is None else bool(vb))}})
    # registrar
    if name:
        subs_opc.setdefault(name, set()).add(ws)
    elif all:
        clients_all_opc.add(ws)
    try:
        while True:
            await ws.receive_text()  # mantém aberto
    except WebSocketDisconnect:
        pass
    finally:
        if name and ws in subs_opc.get(name,set()): subs_opc[name].remove(ws)
        if all and ws in clients_all_opc: clients_all_opc.remove(ws)

@app.websocket("/ws/mpu")
async def ws_mpu(ws: WebSocket, id: Optional[str] = None, all: Optional[bool] = False):
    await ws.accept()
    if id:
        mid = mpu_id_from_str(id)
        row = fetch_one("""
            SELECT ts_utc, ax_g, ay_g, az_g, gx_dps, gy_dps, gz_dps, temp_c
            FROM mpu_samples WHERE mpu_id=%s ORDER BY ts_utc DESC LIMIT 1
        """,(mid,))
        if row:
            ts, ax, ay, az, gx, gy, gz, tc = row
            await ws.send_json({"type":"hello","kind":"mpu","id":id,
                                "last": {"ts_utc": dt_to_iso_utc(ts), "ax_g":ax, "ay_g":ay, "az_g":az,
                                         "gx_dps":gx,"gy_dps":gy,"gz_dps":gz,"temp_c":tc}})
    if id:
        subs_mpu.setdefault(id, set()).add(ws)
    elif all:
        clients_all_mpu.add(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        if id and ws in subs_mpu.get(id,set()): subs_mpu[id].remove(ws)
        if all and ws in clients_all_mpu: clients_all_mpu.remove(ws)

def _parse_since(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    # aceita ISO completo ou “-1h”, “-24h”, “-7d”
    s = s.strip().lower()
    if s.startswith("-"):
        # relativo
        qty = int(s[1:-1])
        unit = s[-1]
        now = datetime.now(timezone.utc)
        if unit == "h":
            return now - timedelta(hours=qty)
        if unit == "d":
            return now - timedelta(days=qty)
        if unit == "m":
            return now - timedelta(minutes=qty)
        raise HTTPException(400, detail="since relativo inválido. Use -1h, -24h, -7d…")
    # ISO
    try:
        dt = datetime.fromisoformat(s.replace("Z","+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        raise HTTPException(400, detail="since inválido. Informe ISO8601 ou '-24h' etc.")

@app.get("/opc/names")
def opc_names():
    rows = fetch_all("SELECT DISTINCT name FROM opc_samples ORDER BY name ASC")
    return {"names": [r[0] for r in rows]}

@app.get("/opc/history")
def opc_history(
    name: str = Query(..., description="Nome do sinal OPC"),
    since: Optional[str] = Query(None, description="ISO8601 ou relativo (-24h, -7d)"),
    limit: int = Query(500, ge=1, le=10000),
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
    data = [{"ts_utc": dt_to_iso_utc(r[0]), "value_bool": (None if r[1] is None else bool(r[1]))} for r in rows]
    # se veio em DESC e o cliente quer asc, já tratamos acima; se não, ok.
    return {"name": name, "count": len(data), "items": data}

@app.get("/mpu/ids")
def mpu_ids():
    # Se tiver tabela de cadastro, use ela. Aqui inferimos pelos dados existentes
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
        SELECT ts_utc, ax_g, ay_g, az_g, gx_dps, gy_dps, gz_dps, temp_c
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
    items = [{
        "ts_utc": dt_to_iso_utc(r[0]),
        "ax_g": r[1], "ay_g": r[2], "az_g": r[3],
        "gx_dps": r[4], "gy_dps": r[5], "gz_dps": r[6],
        "temp_c": r[7],
    } for r in rows]
    return {"id": id, "count": len(items), "items": items}
# =========================
# LIVE DASHBOARD ENDPOINTS
# =========================

# ---- Constantes de nomes (iguais ao Node-RED / OPC) ----
_SENSORS = {
    1: {"recuado": "Recuado_1S1", "avancado": "Avancado_1S2"},  # Atuador 1
    2: {"recuado": "Recuado_2S1", "avancado": "Avancado_2S2"},  # Atuador 2
}
_INICIA = "INICIA"
_PARA   = "PARA"

def _decide_state(bit_recuado: Optional[int], bit_avancado: Optional[int]) -> str:
    """Regra de estado do atuador."""
    if bit_recuado == 1 and bit_avancado == 0:
        return "fechado"
    if bit_avancado == 1 and bit_recuado == 0:
        return "aberto"
    if bit_recuado == 1 and bit_avancado == 1:
        return "erro"      # inconsistente eletricamente
    return "indef"         # transitório / sem leitura ainda

def _fetch_last_bool_row(conn, name: str):
    cur = conn.cursor()
    cur.execute(
        "SELECT ts_utc, value_bool FROM opc_samples WHERE name=%s ORDER BY ts_utc DESC LIMIT 1",
        (name,),
    )
    row = cur.fetchone()
    cur.close()
    return row  # (ts_utc, value_bool) ou None

@app.get("/api/live/actuators/state", tags=["Live Dashboard"])
def live_actuators_state():
    """
    Estados atuais (aberto/fechado/indef/erro) dos dois atuadores com base nos últimos bits.
    """
    conn = POOL.get_connection()
    try:
        out = []
        for aid, m in _SENSORS.items():
            r = _fetch_last_bool_row(conn, m["recuado"])
            a = _fetch_last_bool_row(conn, m["avancado"])
            ts_r = r[0] if r else None
            ts_a = a[0] if a else None
            vb_r = (None if not r or r[1] is None else int(r[1]))
            vb_a = (None if not a or a[1] is None else int(a[1]))
            ts = max([t for t in (ts_r, ts_a) if t is not None], default=None)
            out.append({
                "actuator_id": aid,
                "state": _decide_state(vb_r, vb_a),
                "ts": (dt_to_iso_utc(ts) if ts else None),
                "recuado": vb_r,
                "avancado": vb_a,
            })
        return {"actuators": out}
    finally:
        conn.close()

@app.get("/api/live/cycles/rate", tags=["Live Dashboard"])
def live_cycles_rate(window_s: int = Query(5, ge=1, le=300)):
    """
    Ciclos por segundo usando a regra definida:
      - conte quantas ocorrências de INICIA e de PARA existem na janela
      - pairs = min(contagem(INICIA), contagem(PARA))
      - cycles = floor(pairs / 2)
      - cps = cycles / window_s
    """
    now = datetime.now(timezone.utc)
    start = now - timedelta(seconds=window_s)
    conn = POOL.get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
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
        for name, cnt in cur.fetchall():
            if name == _INICIA:
                c_inicia = int(cnt)
            elif name == _PARA:
                c_para = int(cnt)
        cur.close()

        pairs = min(c_inicia, c_para)
        cycles = pairs // 2
        cps = cycles / float(window_s)

        return {
            "window_seconds": window_s,
            "pairs_count": pairs,
            "cycles": cycles,
            "cycles_per_second": cps,
        }
    finally:
        conn.close()

def _clean_rms(values: List[float], max_g: float = 4.0) -> float:
    """
    Limpeza leve + detrend por mediana + RMS.
    Importa numpy localmente para não poluir o topo do arquivo.
    """
    if not values:
        return 0.0
    a = np.array(values, dtype=float)
    a = a[np.isfinite(a)]
    if a.size == 0:
        return 0.0
    # corta saturações/leituras ruins
    a = a[np.abs(a) <= max_g]
    if a.size == 0:
        return 0.0
    # remove componente DC (gravidade/offset) por mediana
    a = a - np.median(a)
    if a.size == 0:
        return 0.0
    # RMS
    return float((a**2).mean() ** 0.5)

@app.get("/api/live/vibration", tags=["Live Dashboard"])
def live_vibration(window_s: int = Query(2, ge=1, le=60)):
    """
    Calcula RMS de vibração (ax/ay/az) por mpu_id na janela, com overall.
    """
    now = datetime.now(timezone.utc)
    start = now - timedelta(seconds=window_s)
    conn = POOL.get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT mpu_id, ts_utc, ax_g, ay_g, az_g
            FROM mpu_samples
            WHERE ts_utc >= %s AND ts_utc < %s
            """,
            (start, now),
        )
        rows = cur.fetchall()
        cur.close()

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
            items.append({
                "mpu_id": mpu_id,
                "ts_start": dt_to_iso_utc(min(d["ts"])),
                "ts_end": dt_to_iso_utc(max(d["ts"])),
                "rms_ax": rms_ax,
                "rms_ay": rms_ay,
                "rms_az": rms_az,
                "overall": overall,
            })

        return {"items": items}
    finally:
        conn.close()
