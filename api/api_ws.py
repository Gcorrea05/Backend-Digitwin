# api/api_ws.py
import os, io, json, asyncio
from typing import Optional, List, Any, Dict, Tuple
from datetime import datetime, timezone

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

@app.on_event("startup")
async def _startup():
    asyncio.create_task(listen_opc())
    asyncio.create_task(listen_mpu())

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
