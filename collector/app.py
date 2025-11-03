#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Collector DUAL (OPC-UA + MPU Serial)
- Apenas modo DUAL (remove OPCUA-only / MPU-only / simulate / dev etc.)
- Batch insert em MySQL
- Optional push_bit_update (...) para hot-path do /ws/live (se disponível)
"""

import os
import sys
import json
import time
import csv
import signal
import queue
import threading
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv, find_dotenv
import mysql.connector
from mysql.connector.pooling import MySQLConnectionPool

# ---- Opcional (se não instalados, seguimos sem) ----
try:
    from opcua import Client as OPCClient
    from opcua import ua
except Exception:
    OPCClient = None  # type: ignore
    ua = None         # type: ignore

try:
    import serial
    import serial.tools.list_ports
except Exception:
    serial = None  # type: ignore

# ---- Tentativa de hot-path para o backend (não é obrigatório) ----
try:
    # se o collector está no mesmo repo/venv do backend:
    from api.api_ws import push_bit_update  # type: ignore
except Exception:
    def push_bit_update(*args, **kwargs):  # noqa: E301
        return None


# =============================================================================
# ENV / CONFIG
# =============================================================================
load_dotenv(find_dotenv())
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASS", "")
DB_NAME = os.getenv("DB_NAME", "gmdigital")

OPC_ENDPOINT = os.getenv("OPC_ENDPOINT", "opc.tcp://127.0.0.1:4840")
OPC_NODES_CSV = os.getenv("OPC_NODES_CSV", os.path.join(os.path.dirname(__file__), "nodes.csv"))
OPC_TABLE = os.getenv("OPC_TABLE", "opc_samples")

SERIAL_PORT = os.getenv("SERIAL_PORT", "COM3")           # ex: /dev/ttyUSB0 no Linux
SERIAL_BAUD = int(os.getenv("SERIAL_BAUD", "115200"))
MPU_TABLE = os.getenv("MPU_TABLE", "mpu_samples")

# Batching & flush
BATCH_MAX = int(os.getenv("BATCH_MAX", "800"))          # máximo de registros por flush
FLUSH_MS = int(os.getenv("FLUSH_MS", "500"))            # período padrão de flush
OPC_POLL_MS = int(os.getenv("OPC_POLL_MS", "200"))      # usado se subscription indisponível
SER_LINE_TIMEOUT_S = float(os.getenv("SER_LINE_TIMEOUT_S", "0.2"))  # timeout leitura Serial

# Afinidades / tolerâncias
PRINT_EVERY = int(os.getenv("PRINT_EVERY", "2000"))     # prints de progresso
TZ_UTC = timezone.utc

# Ajuste de horário (ex.: -10800 para -3h em DEV)
DEV_TIME_OFFSET_SEC = int(os.getenv("DEV_TIME_OFFSET_SEC", "0") or "0")


# =============================================================================
# MySQL Pool
# =============================================================================
POOL_NAME = "gmdigital_pool"
POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "6"))

def _mk_pool() -> MySQLConnectionPool:
    return MySQLConnectionPool(
        pool_name=POOL_NAME,
        pool_size=POOL_SIZE,
        pool_reset_session=True,
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        autocommit=True,
        charset="utf8mb4",
        collation="utf8mb4_0900_ai_ci",
    )

DB_POOL = _mk_pool()

def db_exec_many(sql: str, rows: List[Tuple[Any, ...]]) -> int:
    if not rows:
        return 0
    conn = DB_POOL.get_connection()
    try:
        cur = conn.cursor()
        cur.executemany(sql, rows)
        conn.commit()
        return cur.rowcount or 0
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


# =============================================================================
# Buffers & Shutdown
# =============================================================================
shutdown_flag = threading.Event()

# buffers protegidos por lock
opc_buf: List[Tuple[Any, ...]] = []
mpu_buf: List[Tuple[Any, ...]] = []
opc_lock = threading.Lock()
mpu_lock = threading.Lock()

def _utcnow() -> datetime:
    """
    Retorna agora em UTC já com o offset de DEV aplicado.
    Use DEV_TIME_OFFSET_SEC=-10800 para "-3h".
    """
    return datetime.now(TZ_UTC) + timedelta(seconds=DEV_TIME_OFFSET_SEC)

def _utc_iso(dt: datetime) -> str:
    return dt.astimezone(TZ_UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


# =============================================================================
# OPC-UA (subscription se possível; fallback pra polling)
# =============================================================================
class _OPCHandler:  # callback para subscription
    def __init__(self, node_map: Dict[str, Any], name_by_nodeid: Dict[str, str]):
        self.node_map = node_map
        self.name_by_nodeid = name_by_nodeid

    def datachange_notification(self, node, val, data):
        # Mapear para nosso esquema (assumimos booleano; se numérico, converta)
        try:
            nodeid = str(node.nodeid)  # "ns=2;i=10853" etc
            name = self.name_by_nodeid.get(nodeid) or nodeid
            vbool = None
            if isinstance(val, (bool, int)):
                vbool = 1 if bool(val) else 0
            elif isinstance(val, float):
                # heurística: >0.5 = 1
                vbool = 1 if val >= 0.5 else 0

            ts = _utcnow()
            with opc_lock:
                opc_buf.append((name, vbool, ts))

            # hot-path pro backend
            if vbool is not None:
                push_bit_update(name, bool(vbool), ts_ms=int(ts.timestamp() * 1000))
        except Exception:
            pass


def opc_thread():
    if OPCClient is None:
        print("[OPC] python-opcua não instalado; pulando OPC.", file=sys.stderr)
        return

    print(f"[OPC] Conectando a {OPC_ENDPOINT} ...")
    client = OPCClient(OPC_ENDPOINT, timeout=4)
    sub = None
    subs_created = False

    # carregar CSV com colunas: name,nodeid  (aceita cabeçalho)
    node_rows: List[Tuple[str, str]] = []
    try:
        with open(OPC_NODES_CSV, "r", newline="", encoding="utf-8") as f:
            rd = csv.DictReader(f)
            if "name" in rd.fieldnames and "nodeid" in rd.fieldnames:
                for r in rd:
                    nm = r["name"].strip()
                    nid = r["nodeid"].strip()
                    if nm and nid:
                        node_rows.append((nm, nid))
            else:
                # fallback sem cabeçalho
                f.seek(0)
                rd2 = csv.reader(f)
                for row in rd2:
                    if len(row) >= 2:
                        node_rows.append((row[0].strip(), row[1].strip()))
    except Exception as e:
        print(f"[OPC] Falha lendo CSV {OPC_NODES_CSV}: {e}", file=sys.stderr)

    if not node_rows:
        print("[OPC] Nenhum node configurado (CSV vazio).", file=sys.stderr)
        return

    name_by_nodeid: Dict[str, str] = {}
    node_map: Dict[str, Any] = {}

    try:
        client.connect()
        print("[OPC] conectado.")
        # cria objetos Node
        for (name, nodeid) in node_rows:
            try:
                node = client.get_node(nodeid)
                node_map[name] = node
                name_by_nodeid[str(node.nodeid)] = name
            except Exception as e:
                print(f"[OPC] Falha get_node({nodeid}): {e}", file=sys.stderr)

        # tenta assinatura
        try:
            handler = _OPCHandler(node_map, name_by_nodeid)
            sub = client.create_subscription(200, handler)  # 200ms publishing
            for (_, nodeid) in node_rows:
                try:
                    sub.subscribe_data_change(client.get_node(nodeid))
                except Exception:
                    pass
            subs_created = True
            print("[OPC] Subscription criada.")
        except Exception as e:
            print(f"[OPC] Subscription indisponível: {e}. Usando polling...", file=sys.stderr)
            subs_created = False

        # loop principal: se sem subscription, faz polling
        last_print = 0
        while not shutdown_flag.is_set():
            if not subs_created:
                # polling
                for (name, nid) in node_rows:
                    try:
                        node = client.get_node(nid)
                        val = node.get_value()
                        ts = _utcnow()
                        if isinstance(val, (bool, int)):
                            vbool = 1 if bool(val) else 0
                        elif isinstance(val, float):
                            vbool = 1 if val >= 0.5 else 0
                        else:
                            vbool = None
                        with opc_lock:
                            opc_buf.append((name, vbool, ts))
                        if vbool is not None:
                            push_bit_update(name, bool(vbool), ts_ms=int(ts.timestamp() * 1000))
                    except Exception:
                        # ignora pontual
                        pass
                time.sleep(OPC_POLL_MS / 1000.0)
            else:
                # subscription -> apenas dorme curto
                time.sleep(0.05)

            last_print += 1
            if last_print % 200 == 0:
                with opc_lock:
                    n = len(opc_buf)
                print(f"[OPC] buffer={n}")

    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"[OPC] erro geral: {e}", file=sys.stderr)
    finally:
        try:
            if sub:
                sub.delete()
        except Exception:
            pass
        try:
            client.disconnect()
        except Exception:
            pass
        print("[OPC] finalizado.")


# =============================================================================
# MPU (Serial JSON por linha)
# =============================================================================
def _auto_serial_port(default: str) -> str:
    if serial is None:
        return default
    try:
        ports = list(serial.tools.list_ports.comports())
        # heurística: se default não existir, pegue o primeiro disponível
        names = [p.device for p in ports]
        if default in names:
            return default
        return names[0] if names else default
    except Exception:
        return default

def mpu_thread():
    if serial is None:
        print("[MPU] pyserial não instalado; pulando MPU.", file=sys.stderr)
        return

    port = _auto_serial_port(SERIAL_PORT)
    print(f"[MPU] Abrindo {port} @ {SERIAL_BAUD} ...")
    try:
        ser = serial.Serial(port=port, baudrate=SERIAL_BAUD, timeout=SER_LINE_TIMEOUT_S)
    except Exception as e:
        print(f"[MPU] Falha ao abrir serial {port}: {e}", file=sys.stderr)
        return

    line_no = 0
    try:
        while not shutdown_flag.is_set():
            raw = ser.readline()
            if not raw:
                continue
            line_no += 1
            try:
                s = raw.decode("utf-8", errors="ignore").strip()
                if not s:
                    continue
                obj = json.loads(s)
                # esperado: {"id":"MPUA1","ax":..,"ay":..,"az":..} etc (em g)
                mpu_id = obj.get("id") or obj.get("mpu_id") or obj.get("sensor")
                if isinstance(mpu_id, str):
                    # normaliza "MPUA1" / "MPUA2" -> 1 / 2
                    mpu_id_norm = 1 if mpu_id.upper().endswith("A1") else 2 if mpu_id.upper().endswith("A2") else None
                else:
                    mpu_id_norm = int(mpu_id) if mpu_id is not None else None

                ax = float(obj.get("ax") or obj.get("ax_g") or 0.0)
                ay = float(obj.get("ay") or obj.get("ay_g") or 0.0)
                az = float(obj.get("az") or obj.get("az_g") or 0.0)
                gx = float(obj.get("gx") or obj.get("gx_dps") or 0.0)
                gy = float(obj.get("gy") or obj.get("gy_dps") or 0.0)
                gz = float(obj.get("gz") or obj.get("gz_dps") or 0.0)

                # também aceitamos "actuator_id"
                actuator_id = obj.get("actuator_id")
                if actuator_id is None and mpu_id_norm in (1, 2):
                    actuator_id = mpu_id_norm

                ts = _utcnow()
                with mpu_lock:
                    mpu_buf.append((actuator_id, ts, ax, ay, az, gx, gy, gz))

            except Exception:
                # linha inválida, ignore
                pass

            if line_no % PRINT_EVERY == 0:
                with mpu_lock:
                    n = len(mpu_buf)
                print(f"[MPU] lin={line_no} buf={n}")

    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"[MPU] erro geral: {e}", file=sys.stderr)
    finally:
        try:
            ser.close()
        except Exception:
            pass
        print("[MPU] finalizado.")


# =============================================================================
# Flusher (MySQL)
# =============================================================================
SQL_OPC = f"""
INSERT INTO {OPC_TABLE} (name, value_bool, ts_utc)
VALUES (%s, %s, %s)
"""

SQL_MPU = f"""
INSERT INTO {MPU_TABLE} (actuator_id, ts_utc, ax_g, ay_g, az_g, gx_dps, gy_dps, gz_dps)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

def flusher_thread():
    last = time.monotonic()
    while not shutdown_flag.is_set():
        now = time.monotonic()
        elapsed_ms = (now - last) * 1000.0
        if elapsed_ms < FLUSH_MS:
            time.sleep(max(0.01, (FLUSH_MS - elapsed_ms) / 1000.0))
            continue
        last = time.monotonic()

        # Drena buffers
        try:
            with opc_lock:
                chunk_opc = opc_buf[:BATCH_MAX]
                del opc_buf[:len(chunk_opc)]
            with mpu_lock:
                chunk_mpu = mpu_buf[:BATCH_MAX]
                del mpu_buf[:len(chunk_mpu)]

            if chunk_opc:
                n = db_exec_many(SQL_OPC, chunk_opc)
                if n and n > 0:
                    print(f"[FLUSH] opc +{n}")

            if chunk_mpu:
                n = db_exec_many(SQL_MPU, chunk_mpu)
                if n and n > 0:
                    print(f"[FLUSH] mpu +{n}")

        except Exception as e:
            print(f"[FLUSH] erro: {e}", file=sys.stderr)
            # Em erro, evitamos recolocar no buffer pra não duplicar; confiamos na próxima leva


# =============================================================================
# Main
# =============================================================================
def _handle_sig(*_):
    shutdown_flag.set()

def main():
    print("=== Collector DUAL (OPC + MPU) ===")
    print(f"DB: {DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    print(f"OPC: {OPC_ENDPOINT} | CSV: {OPC_NODES_CSV}")
    print(f"SER: {SERIAL_PORT} @ {SERIAL_BAUD}")
    print(f"Flush: {FLUSH_MS} ms | Batch: {BATCH_MAX}")
    if DEV_TIME_OFFSET_SEC:
        print(f"[TIME] DEV_TIME_OFFSET_SEC={DEV_TIME_OFFSET_SEC} (aplicado a ts_utc)")

    # sinais
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _handle_sig)
        except Exception:
            pass

    th_opc = threading.Thread(target=opc_thread, name="opc", daemon=True)
    th_mpu = threading.Thread(target=mpu_thread, name="mpu", daemon=True)
    th_flush = threading.Thread(target=flusher_thread, name="flush", daemon=True)

    th_opc.start()
    th_mpu.start()
    th_flush.start()

    try:
        while not shutdown_flag.is_set():
            time.sleep(0.3)
    except KeyboardInterrupt:
        shutdown_flag.set()

    print("Aguardando threads finalizarem...")
    th_opc.join(timeout=3.0)
    th_mpu.join(timeout=3.0)
    th_flush.join(timeout=3.0)
    print("Collector DUAL finalizado.")

if __name__ == "__main__":
    main()
