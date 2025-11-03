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

OPC_ENDPOINT = os.getenv("OPC_ENDPOINT", "opc.tcp://192.168.0.40:4840")
OPC_NODES_CSV = os.getenv("OPC_NODES_CSV", os.path.join(os.path.dirname(__file__), "nodes.csv"))
OPC_TABLE = os.getenv("OPC_TABLE", "opc_samples")

SERIAL_PORT = os.getenv("SERIAL_PORT", "COM3")           # ex: /dev/ttyUSB0 no Linux
SERIAL_BAUD = int(os.getenv("SERIAL_BAUD", "115200"))
MPU_TABLE = os.getenv("MPU_TABLE", "mpu_samples")

# Batching & flush
BATCH_MAX = int(os.getenv("BATCH_MAX", "800"))           # máximo de registros por flush
FLUSH_MS = int(os.getenv("FLUSH_MS", "500"))             # período padrão de flush
OPC_POLL_MS = int(os.getenv("OPC_POLL_MS", "100"))       # polling a cada 100ms por padrão
SER_LINE_TIMEOUT_S = float(os.getenv("SER_LINE_TIMEOUT_S", "0.2"))  # timeout leitura Serial

# Afinidades / tolerâncias
PRINT_EVERY = int(os.getenv("PRINT_EVERY", "2000"))      # prints de progresso
TZ_UTC = timezone.utc

# Ajuste de horário (ex.: -10800 para -3h em DEV) — aplicado por _now_for_db()
DEV_TIME_OFFSET_SEC = int(os.getenv("DEV_TIME_OFFSET_SEC", "0") or "0")

# COMO gravar no MySQL (DATETIME "naive"):
# - LOCAL (padrão): grava horário "naive" já em UTC-3 (BRT)
# - UTC:    grava horário "naive" em UTC
STORE_TZ = (os.getenv("STORE_TZ", "LOCAL") or "LOCAL").upper()   # "LOCAL" | "UTC"
# Offset do fuso local (UTC-3 = -10800). Pode ajustar para horário de verão, se precisar.
LOCAL_TZ_OFFSET_SEC = int(os.getenv("LOCAL_TZ_OFFSET_SEC", "-10800") or "-10800")

# ---------------------------
# Coerção p/ bit e Watchlist
# ---------------------------
OPC_WATCH = set((os.getenv("OPC_WATCH", "") or "").replace(" ", "").split(",")) - {""}


def _to_vbool(val) -> Optional[int]:
    """Converte valores variados do OPC para 0/1.
       Regras:
       - bool -> 0/1
       - num  -> 1 se > 0, senão 0  (cobre 0/1, 0/100, -1/1, 0/255 etc.)
       - str  -> mapeia {'1','true','on','open','high','active','enabled'}=1,
                {'0','false','off','closed','low','inactive','disabled'}=0,
                ou tenta float com a mesma regra (>0 => 1).
       - caso impossível, retorna None (logaremos para ajustar a origem).
    """
    if isinstance(val, bool):
        return 1 if val else 0
    if isinstance(val, (int, float)):
        try:
            return 1 if float(val) > 0.0 else 0
        except Exception:
            return None
    if isinstance(val, str):
        s = val.strip().lower()
        if s in ("1", "true", "on", "open", "high", "active", "enabled"):
            return 1
        if s in ("0", "false", "off", "closed", "low", "inactive", "disabled"):
            return 0
        try:
            f = float(s)
            return 1 if f > 0.0 else 0
        except Exception:
            return None
    return None


# =============================================================================
# Helpers de tempo (CENTRALIZADOS)
# =============================================================================
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
    Converte um datetime 'naive' ASSUMIDO no fuso LOCAL (UTC-3 por padrão) para epoch ms correto.
    Útil para notificar WS (push_bit_update) com timestamp absoluto.
    """
    brt = timezone(timedelta(seconds=LOCAL_TZ_OFFSET_SEC))
    aware_local = ts_local_naive.replace(tzinfo=brt)
    return int(aware_local.astimezone(timezone.utc).timestamp() * 1000)


def _utc_iso_from_naive(db_naive: datetime) -> str:
    """
    Converte um 'naive' (gravado conforme STORE_TZ) para ISO em UTC (Z).
    Só use se for realmente necessário serializar para logs/depuração.
    """
    if db_naive.tzinfo is not None:
        aware = db_naive
    else:
        if STORE_TZ == "LOCAL":
            brt = timezone(timedelta(seconds=LOCAL_TZ_OFFSET_SEC))
            aware = db_naive.replace(tzinfo=brt)
        else:
            aware = db_naive.replace(tzinfo=timezone.utc)
    return aware.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


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


# =============================================================================
# OPC-UA (polling contínuo; subscription opcional removida)
# =============================================================================
class _OPCHandler:  # mantido apenas se você decidir reativar subscription depois
    def __init__(self, node_map: Dict[str, Any], name_by_nodeid: Dict[str, str]):
        self.node_map = node_map
        self.name_by_nodeid = name_by_nodeid

    def datachange_notification(self, node, val, data):
        # Caso volte a usar subscription, o timestamp também deve ser o _now_for_db()
        try:
            nodeid = str(node.nodeid)  # "ns=2;i=10853" etc
            name = self.name_by_nodeid.get(nodeid) or nodeid
            vbool = _to_vbool(val)

            ts = _now_for_db()
            with opc_lock:
                opc_buf.append((name, vbool, ts))

            # hot-path pro backend — agora com epoch ms correto
            if vbool is not None:
                ts_ms = _epoch_ms_from_local_naive(ts) if STORE_TZ == "LOCAL" else int(
                    ts.replace(tzinfo=timezone.utc).timestamp() * 1000
                )
                push_bit_update(name, bool(vbool), ts_ms=ts_ms)
        except Exception:
            pass


def opc_thread():
    """
    OPC em modo *polling-contínuo* (sem subscription).
    Lê TODAS as tags da lista a cada OPC_POLL_MS e empilha no buffer,
    independentemente de mudança de valor.
    """
    if OPCClient is None:
        print("[OPC] python-opcua não instalado; pulando OPC.", file=sys.stderr)
        return

    poll_ms = max(10, OPC_POLL_MS)  # mínimo 10ms
    print(f"[OPC] Polling contínuo habilitado: {poll_ms} ms")

    # carregar CSV com colunas: name,nodeid (aceita cabeçalho)
    node_rows: List[Tuple[str, str]] = []
    try:
        with open(OPC_NODES_CSV, "r", newline="", encoding="utf-8") as f:
            rd = csv.DictReader(f)
            if rd.fieldnames and "name" in rd.fieldnames and "nodeid" in rd.fieldnames:
                for r in rd:
                    nm = (r.get("name") or "").strip()
                    nid = (r.get("nodeid") or "").strip()
                    if nm and nid:
                        node_rows.append((nm, nid))
            else:
                f.seek(0)
                rd2 = csv.reader(f)
                for row in rd2:
                    if len(row) >= 2:
                        nm = (row[0] or "").strip()
                        nid = (row[1] or "").strip()
                        if nm and nid:
                            node_rows.append((nm, nid))
    except Exception as e:
        print(f"[OPC] Falha lendo CSV {OPC_NODES_CSV}: {e}", file=sys.stderr)

    if not node_rows:
        print("[OPC] Nenhum node configurado (CSV vazio).", file=sys.stderr)
        return

    client = None
    while not shutdown_flag.is_set():
        try:
            # Conecta
            print(f"[OPC] Conectando a {OPC_ENDPOINT} ...")
            client = OPCClient(OPC_ENDPOINT, timeout=4)
            client.connect()
            print("[OPC] conectado.")

            # Instancia objetos Node (uma vez por conexão)
            nodes: List[Tuple[str, Any]] = []
            for (name, nodeid) in node_rows:
                try:
                    node = client.get_node(nodeid)
                    nodes.append((name, node))
                except Exception as e:
                    print(f"[OPC] get_node({nodeid}) falhou: {e}", file=sys.stderr)

            if not nodes:
                print("[OPC] Nenhum node válido após get_node(); abortando.", file=sys.stderr)
                return

            # Loop de polling contínuo
            next_deadline = time.perf_counter()
            while not shutdown_flag.is_set():
                ts = _now_for_db()
                # Lê todos os nodes e empilha no buffer
                for (name, node) in nodes:
                    try:
                        val = node.get_value()
                        vbool = _to_vbool(val)

                        # Log dedicado para depuração (defina OPC_WATCH="S1_OPEN,S1_CLOSE" no .env)
                        if name in OPC_WATCH:
                            print(f"[OPC][{name}] raw={repr(val)}({type(val).__name__}) -> vbool={vbool}")

                        with opc_lock:
                            opc_buf.append((name, vbool, ts))

                        # Opcional: alerta ocasional quando vbool=None (evita flood)
                        if vbool is None:
                            if (hash(name) ^ int(time.time())) % 50 == 0:
                                print(f"[OPC][warn] {name} -> vbool=None (raw={repr(val)})")

                        # Importante: NÃO chamar push_bit_update a cada amostra de polling
                        # (se precisar hot-path, reative apenas em mudança e usando _epoch_ms_from_local_naive)
                    except Exception:
                        # falha pontual de leitura: ignora e segue
                        pass

                # Espera até o próximo slot (mantém a cadência)
                next_deadline += poll_ms / 1000.0
                delay = next_deadline - time.perf_counter()
                if delay > 0:
                    time.sleep(delay)
                else:
                    # se ficou atrasado, realinha pro próximo tick
                    next_deadline = time.perf_counter()

        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"[OPC] erro geral (reconectar em 2s): {e}", file=sys.stderr)
            time.sleep(2.0)
        finally:
            try:
                if client:
                    client.disconnect()
            except Exception:
                pass
            client = None

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
                # esperado: {"id":"MPUA1","ax":..,"ay":..,"az":..,"gx":..,"gy":..,"gz":..}
                mpu_id = obj.get("id") or obj.get("mpu_id") or obj.get("sensor")
                if isinstance(mpu_id, str):
                    up = mpu_id.upper()
                    mpu_id_norm = 1 if up.endswith("A1") else 2 if up.endswith("A2") else None
                else:
                    mpu_id_norm = int(mpu_id) if mpu_id is not None else None

                if mpu_id_norm not in (1, 2):
                    continue

                ax = float(obj.get("ax") or obj.get("ax_g") or 0.0)
                ay = float(obj.get("ay") or obj.get("ay_g") or 0.0)
                az = float(obj.get("az") or obj.get("az_g") or 0.0)
                gx = float(obj.get("gx") or obj.get("gx_dps") or 0.0)
                gy = float(obj.get("gy") or obj.get("gy_dps") or 0.0)
                gz = float(obj.get("gz") or obj.get("gz_dps") or 0.0)

                # timestamp "naive" conforme STORE_TZ (LOCAL/UTC)
                ts = _now_for_db()

                with mpu_lock:
                    # ordem: ts, mpu_id, ax_g, ay_g, az_g, gx_dps, gy_dps, gz_dps
                    mpu_buf.append((ts, mpu_id_norm, ax, ay, az, gx, gy, gz))

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
INSERT INTO {MPU_TABLE} (ts_utc, mpu_id, ax_g, ay_g, az_g, gx_dps, gy_dps, gz_dps)
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
    print(f"STORE_TZ={STORE_TZ} | LOCAL_TZ_OFFSET_SEC={LOCAL_TZ_OFFSET_SEC}")
    if DEV_TIME_OFFSET_SEC:
        print(f"[TIME] DEV_TIME_OFFSET_SEC={DEV_TIME_OFFSET_SEC} (aplicado em _now_for_db)")

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
