# app.py — Coletor unificado (DUAL): OPC UA + MPU (ESP32 serial) simultâneos
# Tabelas MySQL: festo_dt.opc_samples e festo_dt.mpu_samples
# Modos: SIMULATE | OPCUA | SERIAL_MPU | DUAL
import os
import csv
import json
import time
import signal
import sys
import threading
from datetime import datetime, UTC
from typing import List, Dict, Any, Iterable, Optional

from dotenv import load_dotenv
load_dotenv()

# =========================
# Utils & Config
# =========================
def now_utc_iso() -> str:
    return datetime.now(UTC).isoformat()

def iso_to_mysql_dt6(iso_str: str) -> str:
    s = iso_str.replace("T", " ")
    if "+" in s:
        s = s.split("+", 1)[0]
    if s.endswith("Z"):
        s = s[:-1]
    if "." not in s:
        s = s + ".000000"
    else:
        head, frac = s.split(".", 1)
        frac = (frac + "000000")[:6]
        s = f"{head}.{frac}"
    return s

DATA_MODE = os.getenv("DATA_MODE", "DUAL").upper()      # SIMULATE | OPCUA | SERIAL_MPU | DUAL
SINK_MODE = os.getenv("SINK_MODE", "MYSQL").upper()     # MYSQL | CSV

# OPC UA
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "1"))
OPCUA_ENDPOINT = os.getenv("OPCUA_ENDPOINT", "opc.tcp://192.168.0.40:4840")
OPCUA_USER = os.getenv("OPCUA_USER", "")
OPCUA_PASS = os.getenv("OPCUA_PASS", "")
NODES_CSV = os.getenv("OPCUA_NODEIDS_CSV", "nodes.csv")

# Serial MPU
SERIAL_PORT = os.getenv("SERIAL_PORT", "COM3")
SERIAL_BAUD = int(os.getenv("SERIAL_BAUD", "115200"))
SERIAL_TIMEOUT = float(os.getenv("SERIAL_TIMEOUT", "0.2"))

# CSV (apoio)
CSV_OPC_PATH = os.getenv("CSV_OPC_PATH", "bank_opc.csv")
CSV_MPU_PATH = os.getenv("CSV_MPU_PATH", "bank_mpu.csv")

# MySQL
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB   = os.getenv("MYSQL_DB", "festo_dt")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASS = os.getenv("MYSQL_PASS", "")

# Live bus (Redis)
REDIS_ENABLE = os.getenv("REDIS_ENABLE", "true").lower() in {"1", "true", "yes", "on"}
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
_rcli = None
if REDIS_ENABLE:
    try:
        import redis
        _rcli = redis.from_url(REDIS_URL, decode_responses=True)
    except Exception:
        _rcli = None

# =========================
# Leitura do nodes.csv
# =========================
def load_nodes(csv_path: str) -> List[Dict[str, str]]:
    items: List[Dict[str, str]] = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row["name"].strip()
            nodeid = row.get("nodeid", "").strip()
            dtype = row.get("datatype", "Boolean").strip()
            items.append({"name": name, "nodeid": nodeid, "datatype": dtype})
    if not items:
        raise RuntimeError(f"Nenhum node carregado de {csv_path}")
    return items

# =========================
# Fontes de dados
# =========================
class Simulator:
    def __init__(self, items: List[Dict[str, str]]):
        self.state = {i["name"]: False for i in items}
    def read_all(self) -> Dict[str, Any]:
        for k in list(self.state.keys()):
            self.state[k] = not self.state[k]
        return dict(self.state)

class OpcUaReader:
    def __init__(self, endpoint: str, items: List[Dict[str, str]]):
        self.endpoint = endpoint
        self.items = items
        self.client = None
    def connect(self):
        try:
            from opcua import Client
        except Exception as e:
            print("Erro importando 'opcua'. Rode 'pip install -r requirements.txt'.")
            raise e
        self.client = Client(self.endpoint)
        if OPCUA_USER and OPCUA_PASS:
            self.client.set_user(OPCUA_USER)
            self.client.set_password(OPCUA_PASS)
        self.client.application_uri = "opcua-py-runner"
        self.client.secure_channel_timeout = 60000
        self.client.session_timeout = 60000
        self.client.connect()
    def disconnect(self):
        if self.client:
            try:
                self.client.disconnect()
            except Exception:
                pass
            self.client = None
    def read_all(self) -> Dict[str, Any]:
        out = {}
        for it in self.items:
            name = it["name"]; nodeid_str = it["nodeid"]
            node = self.client.get_node(nodeid_str)
            try:
                val = node.get_value()
            except Exception:
                val = None
            out[name] = val
        return out

class SerialMpuReader:
    def __init__(self, port: str, baud: int, timeout: float):
        try:
            import serial
        except Exception as e:
            print("Erro importando 'pyserial'. Rode 'pip install -r requirements.txt'.")
            raise e
        self.serial_module = serial
        self.port = port; self.baud = baud; self.timeout = timeout
        self.ser = None
    def connect(self):
        self.ser = self.serial_module.Serial(self.port, self.baud, timeout=self.timeout)
    def disconnect(self):
        if self.ser:
            try:
                self.ser.close()
            except Exception:
                pass
            self.ser = None
    def read_one(self) -> Optional[Dict[str, Any]]:
        if not self.ser:
            return None
        line = self.ser.readline()
        if not line:
            return None
        try:
            s = line.decode("utf-8", errors="ignore").strip()
            if not s:
                return None
            obj = json.loads(s)
        except Exception:
            return None
        sensor = str(obj.get("id", obj.get("sensor", ""))).strip().upper()
        if sensor not in {"MPUA1", "MPUA2"}:
            return None
        mpu_id = 1 if sensor == "MPUA1" else 2
        try:
            ax = float(obj["ax"]); ay = float(obj["ay"]); az = float(obj["az"])
        except Exception:
            return None
        rec: Dict[str, Any] = {"mpu_id": mpu_id, "ax_g": ax, "ay_g": ay, "az_g": az}
        for k_src, k_dst in [("gx","gx_dps"), ("gy","gy_dps"), ("gz","gz_dps"),
                             ("gx_dps","gx_dps"), ("gy_dps","gy_dps"), ("gz_dps","gz_dps"),
                             ("temp","temp_c"), ("temp_c","temp_c")]:
            if k_src in obj:
                try:
                    rec[k_dst] = float(obj[k_src])
                except Exception:
                    pass
        return rec

# =========================
# Sinks (destinos)
# =========================
class CsvOpcSink:
    def __init__(self, path: str, fieldnames: Iterable[str]):
        self.path = path
        self.fieldnames = ["ts_utc", *fieldnames]
        if not os.path.exists(self.path):
            with open(self.path, "w", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=self.fieldnames).writeheader()
    def write_row(self, row: Dict[str, Any]):
        with open(self.path, "a", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=self.fieldnames).writerow(row)
    def close(self): pass

class CsvMpuSink:
    def __init__(self, path: str):
        self.path = path
        self.fields = ["ts_utc","mpu_id","ax_g","ay_g","az_g","gx_dps","gy_dps","gz_dps","temp_c"]
        if not os.path.exists(self.path):
            with open(self.path, "w", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=self.fields).writeheader()
    def write_sample(self, sample: Dict[str, Any]):
        with open(self.path, "a", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=self.fields).writerow(sample)
    def close(self): pass

class MySqlBase:
    def __init__(self):
        import mysql.connector
        self.mysql = mysql.connector
        self.conn = self.mysql.connect(
            host=MYSQL_HOST, port=MYSQL_PORT,
            database=MYSQL_DB, user=MYSQL_USER, password=MYSQL_PASS
        )
    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass

class MySqlOpcSink(MySqlBase):
    def __init__(self):
        super().__init__()
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS opc_samples (
              id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
              ts_utc DATETIME(6) NOT NULL,
              name VARCHAR(128) NOT NULL,
              value_bool TINYINT(1) NULL,
              INDEX idx_opc_name_ts (name, ts_utc),
              INDEX idx_opc_ts (ts_utc)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        cur.close()
        self.conn.commit()
    def write_many(self, ts_iso: str, values: Dict[str, Any]):
        ts_mysql = iso_to_mysql_dt6(ts_iso)
        cur = self.conn.cursor()
        data = []
        for name, v in values.items():
            vb = None if v is None else int(bool(v))
            data.append((ts_mysql, name, vb))
        cur.executemany(
            "INSERT INTO opc_samples (ts_utc, name, value_bool) VALUES (%s, %s, %s)", data
        )
        cur.close()
        self.conn.commit()

class MySqlMpuSink(MySqlBase):
    def __init__(self):
        super().__init__()
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS mpu_samples (
              id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
              ts_utc DATETIME(6) NOT NULL,
              mpu_id TINYINT UNSIGNED NOT NULL,
              ax_g DOUBLE NOT NULL,
              ay_g DOUBLE NOT NULL,
              az_g DOUBLE NOT NULL,
              gx_dps DOUBLE NULL,
              gy_dps DOUBLE NULL,
              gz_dps DOUBLE NULL,
              temp_c DOUBLE NULL,
              INDEX idx_mpu_id_ts (mpu_id, ts_utc),
              INDEX idx_mpu_ts (ts_utc)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        cur.close()
        self.conn.commit()
    def write_sample(self, ts_iso: str, rec: Dict[str, Any]):
        ts_mysql = iso_to_mysql_dt6(ts_iso)
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO mpu_samples
              (ts_utc, mpu_id, ax_g, ay_g, az_g, gx_dps, gy_dps, gz_dps, temp_c)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            ts_mysql,
            int(rec["mpu_id"]),
            float(rec["ax_g"]), float(rec["ay_g"]), float(rec["az_g"]),
            (None if rec.get("gx_dps") is None else float(rec["gx_dps"])),
            (None if rec.get("gy_dps") is None else float(rec["gy_dps"])),
            (None if rec.get("gz_dps") is None else float(rec["gz_dps"])),
            (None if rec.get("temp_c") is None else float(rec["temp_c"]))
        ))
        cur.close()
        self.conn.commit()

# =========================
# Loops (threads) e controle
# =========================
STOP = threading.Event()
def handle_stop(signum, frame):
    STOP.set()
signal.signal(signal.SIGINT, handle_stop)
signal.signal(signal.SIGTERM, handle_stop)

def _publish_opc_events(ts_iso: str, values: Dict[str, Any]):
    """Publica cada sinal individualmente no Redis (canal opc_samples) como opc_event."""
    if not _rcli:
        return
    for name, v in values.items():
        # se quiser ignorar INICIA no live principal, descomente:
        # if name == "INICIA":
        #     continue
        evt = {
            "type": "opc_event",
            "ts_utc": ts_iso,
            "name": name,
            "value_bool": (None if v is None else int(bool(v))),
        }
        try:
            _rcli.publish("opc_samples", json.dumps(evt))
        except Exception:
            pass

def _publish_mpu_sample(sample: Dict[str, Any]):
    """Publica amostra do MPU no Redis (canal mpu_samples) como mpu_sample."""
    if not _rcli:
        return
    try:
        id_str = "MPUA1" if int(sample["mpu_id"]) == 1 else "MPUA2"
        msg = {
            "type": "mpu_sample",
            "ts_utc": sample["ts_utc"],
            "id": id_str,
            "ax_g": sample["ax_g"], "ay_g": sample["ay_g"], "az_g": sample["az_g"],
            "gx_dps": sample.get("gx_dps"), "gy_dps": sample.get("gy_dps"),
            "gz_dps": sample.get("gz_dps"), "temp_c": sample.get("temp_c"),
        }
        _rcli.publish("mpu_samples", json.dumps(msg))
    except Exception:
        pass

def opc_loop():
    items = load_nodes(NODES_CSV)
    names = [i["name"] for i in items]
    # Fonte
    if DATA_MODE == "SIMULATE":
        reader = Simulator(items)
        connected = True
    else:
        reader = OpcUaReader(OPCUA_ENDPOINT, items)
        connected = False
    # Sink
    sink = MySqlOpcSink() if SINK_MODE == "MYSQL" else CsvOpcSink(CSV_OPC_PATH, names)

    try:
        while not STOP.is_set():
            # conecta se necessário
            if isinstance(reader, OpcUaReader) and not connected:
                try:
                    reader.connect()
                    connected = True
                except Exception:
                    time.sleep(2.0)
                    continue

            # lê e grava
            try:
                values = reader.read_all()
            except Exception:
                if isinstance(reader, OpcUaReader):
                    try:
                        reader.disconnect()
                    except Exception:
                        pass
                    connected = False
                time.sleep(1.0)
                continue

            ts = now_utc_iso()
            if isinstance(sink, MySqlOpcSink):
                sink.write_many(ts, values)
            else:
                sink.write_row({"ts_utc": ts, **values})

            # LIVE: publica cada sinal no Redis
            _publish_opc_events(ts, values)

            time.sleep(POLL_INTERVAL)
    finally:
        try:
            sink.close()
        except Exception:
            pass
        if isinstance(reader, OpcUaReader):
            reader.disconnect()

def mpu_loop():
    ser_reader = SerialMpuReader(SERIAL_PORT, SERIAL_BAUD, SERIAL_TIMEOUT)
    sink = MySqlMpuSink() if SINK_MODE == "MYSQL" else CsvMpuSink(CSV_MPU_PATH)

    try:
        # tenta conectar/reconectar continuamente
        while not STOP.is_set():
            try:
                ser_reader.connect()
                break
            except Exception:
                time.sleep(2.0)

        # leitura contínua
        while not STOP.is_set():
            rec = ser_reader.read_one()
            if rec is None:
                continue
            ts = now_utc_iso()
            sample = {
                "ts_utc": ts,
                "mpu_id": rec["mpu_id"],
                "ax_g": rec["ax_g"], "ay_g": rec["ay_g"], "az_g": rec["az_g"],
                "gx_dps": rec.get("gx_dps"), "gy_dps": rec.get("gy_dps"),
                "gz_dps": rec.get("gz_dps"), "temp_c": rec.get("temp_c")
            }
            if isinstance(sink, MySqlMpuSink):
                sink.write_sample(ts, sample)
            else:
                sink.write_sample(sample)

            # LIVE: publica amostra do MPU no Redis
            _publish_mpu_sample(sample)
    finally:
        try:
            sink.close()
        except Exception:
            pass
        ser_reader.disconnect()

def run_dual():
    t_opc = threading.Thread(target=opc_loop, name="opc_loop", daemon=True)
    t_mpu = threading.Thread(target=mpu_loop, name="mpu_loop", daemon=True)
    t_opc.start()
    t_mpu.start()
    # aguarda sinal de parada
    while t_opc.is_alive() or t_mpu.is_alive():
        if STOP.is_set():
            break
        time.sleep(0.2)
    # join suave
    t_opc.join(timeout=3.0)
    t_mpu.join(timeout=3.0)

def run_opc_only():
    opc_loop()

def run_mpu_only():
    mpu_loop()

def main():
    mode = DATA_MODE
    if mode == "DUAL":
        run_dual()
    elif mode == "OPCUA" or mode == "SIMULATE":
        run_opc_only()
    elif mode == "SERIAL_MPU":
        run_mpu_only()
    else:
        print(f"[ERRO] DATA_MODE inválido: {mode} (use SIMULATE | OPCUA | SERIAL_MPU | DUAL)")
        sys.exit(2)

if __name__ == "__main__":
    main()
