# app.py — Coletor unificado (DUAL): OPC UA + MPU (ESP32 serial) simultâneos
# Tabelas MySQL: festo_dt.opc_samples, festo_dt.mpu_samples, festo_dt.mpu_windows_200ms
# Modos: SIMULATE | OPCUA | SERIAL_MPU | DUAL
import os
import csv
import json
import time
import signal
import sys
import threading
import math
from datetime import datetime
from typing import List, Dict, Any, Iterable, Optional, Tuple

from dotenv import load_dotenv
load_dotenv()

# --- Compat UTC robusto (funciona em Py 3.9+)
from datetime import datetime
def _get_utc():
    try:
        # Python 3.11+
        from datetime import UTC  # type: ignore
        return UTC
    except Exception:
        # Python < 3.11
        from datetime import timezone
        return timezone.utc

UTC = _get_utc()

# =========================
# Utils & Config
# =========================
def now_utc() -> datetime:
    return datetime.now(UTC)

def now_utc_iso() -> str:
    return now_utc().isoformat()

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

def dt_to_mysql_dt6(dt: datetime) -> str:
    iso_str = dt.isoformat()
    return iso_to_mysql_dt6(iso_str)

def floor_dt_window(dt: datetime, window_ms: int) -> datetime:
    """Alinha dt para o início da janela (ex.: 200ms)."""
    # dt deve ser timezone-aware em UTC
    epoch_us = int(dt.timestamp() * 1_000_000)
    win_us = window_ms * 1000
    start_us = (epoch_us // win_us) * win_us
    return datetime.fromtimestamp(start_us / 1_000_000, UTC)

DATA_MODE = os.getenv("DATA_MODE", "DUAL").upper()      # SIMULATE | OPCUA | SERIAL_MPU | DUAL
SINK_MODE = os.getenv("SINK_MODE", "MYSQL").upper()     # MYSQL | CSV

# OPC UA
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "0.2"))  # 200ms por default
OPCUA_ENDPOINT = os.getenv("OPCUA_ENDPOINT", "opc.tcp://192.168.0.40:4840")
OPCUA_USER = os.getenv("OPCUA_USER", "")
OPCUA_PASS = os.getenv("OPCUA_PASS", "")
# Ajuste a definição de NODES_CSV para evitar o caminho duplicado
NODES_CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'nodes.csv')


# Serial MPU
SERIAL_PORT = os.getenv("SERIAL_PORT", "COM3")
SERIAL_BAUD = int(os.getenv("SERIAL_BAUD", "115200"))
SERIAL_TIMEOUT = float(os.getenv("SERIAL_TIMEOUT", "0.2"))

# Janela MPU (agregação)
MPU_AGG_ENABLE = os.getenv("MPU_AGG_ENABLE", "true").lower() in {"1", "true", "yes", "on"}
MPU_WINDOW_MS = int(os.getenv("MPU_WINDOW_MS", "200"))

# Thresholds (ex.: RMS máximos). Se vazio, não valida.
AX_RMS_MAX = os.getenv("AX_RMS_MAX", "")
AY_RMS_MAX = os.getenv("AY_RMS_MAX", "")
AZ_RMS_MAX = os.getenv("AZ_RMS_MAX", "")
GX_RMS_MAX = os.getenv("GX_RMS_MAX", "")
GY_RMS_MAX = os.getenv("GY_RMS_MAX", "")
GZ_RMS_MAX = os.getenv("GZ_RMS_MAX", "")
TEMP_MAX   = os.getenv("TEMP_MAX", "")

def _opt_float(s: str) -> Optional[float]:
    try:
        return float(s) if s != "" else None
    except Exception:
        return None

AX_RMS_MAX_F = _opt_float(AX_RMS_MAX)
AY_RMS_MAX_F = _opt_float(AY_RMS_MAX)
AZ_RMS_MAX_F = _opt_float(AZ_RMS_MAX)
GX_RMS_MAX_F = _opt_float(GX_RMS_MAX)
GY_RMS_MAX_F = _opt_float(GY_RMS_MAX)
GZ_RMS_MAX_F = _opt_float(GZ_RMS_MAX)
TEMP_MAX_F   = _opt_float(TEMP_MAX)

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
import os
import csv
from typing import List, Dict

def load_nodes(csv_path: str) -> List[Dict[str, str]]:
    # Garantir que o caminho do arquivo está correto
    full_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), csv_path)
    
    print(f"[DEBUG] Procurando arquivo nodes.csv em: {full_path}")  # Debug: Verifica o caminho

    if not os.path.exists(full_path):
        raise FileNotFoundError(f"O arquivo {full_path} não foi encontrado.")
    
    items: List[Dict[str, str]] = []
    with open(full_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row["name"].strip()
            nodeid = row.get("nodeid", "").strip()
            dtype = row.get("datatype", "Boolean").strip()
            items.append({"name": name, "nodeid": nodeid, "datatype": dtype})
    
    if not items:
        raise RuntimeError(f"Nenhum node carregado de {full_path}")
    
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
            print("[MPU] Erro importando 'pyserial'. Rode: pip install pyserial")
            raise e
        self.serial_module = serial
        self.port = port
        self.baud = baud
        self.timeout = timeout
        self.ser = None

    def connect(self):
        print(f"[MPU] Abrindo {self.port} @ {self.baud}...")
        self.ser = self.serial_module.Serial(self.port, self.baud, timeout=self.timeout)
        # descarta lixo inicial do buffer
        self.ser.reset_input_buffer()
        print("[MPU] Conectado.")

    def disconnect(self):
        if self.ser:
            try:
                self.ser.close()
                print("[MPU] Desconectado.")
            except Exception:
                pass
            self.ser = None

    def _extract_json(self, s: str) -> Optional[str]:
        # Em alguns terminais aparecem prefixos (timestamps, “-> ”, etc.). 
        # Aqui pegamos o trecho entre o 1º '{' e o último '}'.
        i = s.find("{")
        j = s.rfind("}")
        if i >= 0 and j > i:
            return s[i:j+1]
        return None

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
            js = self._extract_json(s) or s  # tenta “limpar” a linha
            obj = json.loads(js)
        except Exception:
            # linha inválida; ignore silenciosamente (ou log se quiser)
            return None

        sensor = str(obj.get("id", obj.get("sensor", ""))).strip().upper()
        if sensor not in {"MPUA1", "MPUA2"}:
            return None
        mpu_id = 1 if sensor == "MPUA1" else 2

        try:
            ax = float(obj["ax"])
            ay = float(obj["ay"])
            az = float(obj["az"])
        except Exception:
            return None

        rec: Dict[str, Any] = {"mpu_id": mpu_id, "ax_g": ax, "ay_g": ay, "az_g": az}
        for k_src, k_dst in [("gx","gx_dps"), ("gy","gy_dps"), ("gz","gz_dps"),
                             ("gx_dps","gx_dps"), ("gy_dps","gy_dps"), ("gz_dps","gz_dps")]:
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
        print(f"[MySQL] target={MYSQL_HOST}:{MYSQL_PORT} db={MYSQL_DB} user={MYSQL_USER}")
        self.conn = self.mysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            database=MYSQL_DB,
            user=MYSQL_USER,
            password=MYSQL_PASS,
            connection_timeout=5
        )
        self.conn.autocommit = True
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
              (ts_utc, mpu_id, ax_g, ay_g, az_g, gx_dps, gy_dps, gz_dps)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            ts_mysql,
            int(rec["mpu_id"]),
            float(rec["ax_g"]), float(rec["ay_g"]), float(rec["az_g"]),
            (None if rec.get("gx_dps") is None else float(rec["gx_dps"])),
            (None if rec.get("gy_dps") is None else float(rec["gy_dps"])),
            (None if rec.get("gz_dps") is None else float(rec["gz_dps"]))
        ))
        cur.close()
        self.conn.commit()

class MySqlMpuWinSink(MySqlBase):
    """Tabela agregada por janelas de MPU (ex.: 200ms)."""
    def __init__(self):
        super().__init__()
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS mpu_windows_200ms (
              id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
              ts_utc DATETIME(6) NOT NULL,
              mpu_id TINYINT UNSIGNED NOT NULL,
              n INT NOT NULL,

              ax_mean DOUBLE, ay_mean DOUBLE, az_mean DOUBLE,
              ax_rms  DOUBLE, ay_rms  DOUBLE, az_rms  DOUBLE,
              ax_min  DOUBLE, ay_min  DOUBLE, az_min  DOUBLE,
              ax_max  DOUBLE, ay_max  DOUBLE, az_max  DOUBLE,

              gx_mean DOUBLE, gy_mean DOUBLE, gz_mean DOUBLE,
              gx_rms  DOUBLE, gy_rms  DOUBLE, gz_rms  DOUBLE,
              gx_min  DOUBLE, gy_min  DOUBLE, gz_min  DOUBLE,
              gx_max  DOUBLE, gy_max  DOUBLE, gz_max  DOUBLE,

              temp_mean DOUBLE,
              INDEX idx_mpuw_ts (ts_utc),
              INDEX idx_mpuw_mpu_ts (mpu_id, ts_utc)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        cur.close()
        self.conn.commit()

    def insert_window(self, ts_start: datetime, mpu_id: int, n: int,
                      stats: Dict[str, Dict[str, Optional[float]]]):
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO mpu_windows_200ms
            (ts_utc, mpu_id, n,
             ax_mean, ay_mean, az_mean, ax_rms, ay_rms, az_rms,
             ax_min, ay_min, az_min, ax_max, ay_max, az_max,
             gx_mean, gy_mean, gz_mean, gx_rms, gy_rms, gz_rms,
             gx_min, gy_min, gz_min, gx_max, gy_max, gz_max,
             temp_mean)
            VALUES (%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,
                    %s)
        """, (
            dt_to_mysql_dt6(ts_start), int(mpu_id), int(n),
            stats["ax"]["mean"], stats["ay"]["mean"], stats["az"]["mean"],
            stats["ax"]["rms"],  stats["ay"]["rms"],  stats["az"]["rms"],
            stats["ax"]["min"],  stats["ay"]["min"],  stats["az"]["min"],
            stats["ax"]["max"],  stats["ay"]["max"],  stats["az"]["max"],
            stats["gx"]["mean"], stats["gy"]["mean"], stats["gz"]["mean"],
            stats["gx"]["rms"],  stats["gy"]["rms"],  stats["gz"]["rms"],
            stats["gx"]["min"],  stats["gy"]["min"],  stats["gz"]["min"],
            stats["gx"]["max"],  stats["gy"]["max"],  stats["gz"]["max"],
            stats["temp"]["mean"]
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

def _publish_mpu_alert(ts_start: datetime, mpu_id: int, kind: str, value: float, limit: float):
    if not _rcli:
        return
    try:
        msg = {
            "type": "mpu_alert",
            "ts_utc": ts_start.isoformat(),
            "mpu_id": int(mpu_id),
            "metric": kind,
            "value": float(value),
            "limit": float(limit)
        }
        _rcli.publish("alerts", json.dumps(msg))
    except Exception:
        pass

# ---------- Agregador de janelas 200ms ----------
class _Agg:
    __slots__ = ("n",
                 "sum_ax", "sum_ay", "sum_az", "sum_ax2", "sum_ay2", "sum_az2",
                 "min_ax", "min_ay", "min_az", "max_ax", "max_ay", "max_az",
                 "sum_gx", "sum_gy", "sum_gz", "sum_gx2", "sum_gy2", "sum_gz2",
                 "min_gx", "min_gy", "min_gz", "max_gx", "max_gy", "max_gz")

    def __init__(self):
        self.n = 0
        self.sum_ax = self.sum_ay = self.sum_az = 0.0
        self.sum_ax2 = self.sum_ay2 = self.sum_az2 = 0.0
        self.min_ax = self.min_ay = self.min_az = float("inf")
        self.max_ax = self.max_ay = self.max_az = float("-inf")

        self.sum_gx = self.sum_gy = self.sum_gz = 0.0
        self.sum_gx2 = self.sum_gy2 = self.sum_gz2 = 0.0
        self.min_gx = self.min_gy = self.min_gz = float("inf")
        self.max_gx = self.max_gy = self.max_gz = float("-inf")

    def add(self, ax, ay, az, gx=None, gy=None, gz=None):
        self.n += 1
        self.sum_ax += ax; self.sum_ay += ay; self.sum_az += az
        self.sum_ax2 += ax*ax; self.sum_ay2 += ay*ay; self.sum_az2 += az*az
        self.min_ax = min(self.min_ax, ax); self.min_ay = min(self.min_ay, ay); self.min_az = min(self.min_az, az)
        self.max_ax = max(self.max_ax, ax); self.max_ay = max(self.max_ay, ay); self.max_az = max(self.max_az, az)

        if gx is not None:
            self.sum_gx += gx; self.sum_gx2 += gx*gx
            self.min_gx = min(self.min_gx, gx); self.max_gx = max(self.max_gx, gx)
        if gy is not None:
            self.sum_gy += gy; self.sum_gy2 += gy*gy
            self.min_gy = min(self.min_gy, gy); self.max_gy = max(self.max_gy, gy)
        if gz is not None:
            self.sum_gz += gz; self.sum_gz2 += gz*gz
            self.min_gz = min(self.min_gz, gz); self.max_gz = max(self.max_gz, gz)

    def stats(self) -> Dict[str, Dict[str, Optional[float]]]:
        if self.n <= 0:
            return {k: {m: None for m in ("mean","rms","min","max")} for k in ("ax","ay","az","gx","gy","gz")}
        n = float(self.n)
        def mean_sum(s): return s / n
        def rms_sum(ss): return math.sqrt(ss / n)

        ax_mean = mean_sum(self.sum_ax); ay_mean = mean_sum(self.sum_ay); az_mean = mean_sum(self.sum_az)
        ax_rms  = rms_sum(self.sum_ax2); ay_rms  = rms_sum(self.sum_ay2); az_rms  = rms_sum(self.sum_az2)

        gx_mean = mean_sum(self.sum_gx) if self.sum_gx or self.n else None
        gy_mean = mean_sum(self.sum_gy) if self.sum_gy or self.n else None
        gz_mean = mean_sum(self.sum_gz) if self.sum_gz or self.n else None
        gx_rms  = rms_sum(self.sum_gx2) if self.sum_gx2 or self.n else None
        gy_rms  = rms_sum(self.sum_gy2) if self.sum_gy2 or self.n else None
        gz_rms  = rms_sum(self.sum_gz2) if self.sum_gz2 or self.n else None

        return {
            "ax": {"mean": ax_mean, "rms": ax_rms, "min": self.min_ax, "max": self.max_ax},
            "ay": {"mean": ay_mean, "rms": ay_rms, "min": self.min_ay, "max": self.max_ay},
            "az": {"mean": az_mean, "rms": az_rms, "min": self.min_az, "max": self.max_az},
            "gx": {"mean": gx_mean, "rms": gx_rms, "min": (None if self.min_gx==float('inf') else self.min_gx), "max": (None if self.max_gx==float('-inf') else self.max_gx)},
            "gy": {"mean": gy_mean, "rms": gy_rms, "min": (None if self.min_gy==float('inf') else self.min_gy), "max": (None if self.max_gy==float('-inf') else self.max_gy)},
            "gz": {"mean": gz_mean, "rms": gz_rms, "min": (None if self.min_gz==float('inf') else self.min_gz), "max": (None if self.max_gz==float('-inf') else self.max_gz)},
        }


def _check_thresholds_and_alert(ts_start: datetime, mpu_id: int, stats: Dict[str, Dict[str, Optional[float]]]):
    # Checa RMS contra limites (se definidos) e publica alerta
    checks: List[Tuple[str, Optional[float], Optional[float]]] = [
        ("ax_rms", stats["ax"]["rms"], AX_RMS_MAX_F),
        ("ay_rms", stats["ay"]["rms"], AY_RMS_MAX_F),
        ("az_rms", stats["az"]["rms"], AZ_RMS_MAX_F),
        ("gx_rms", stats["gx"]["rms"], GX_RMS_MAX_F),
        ("gy_rms", stats["gy"]["rms"], GY_RMS_MAX_F),
        ("gz_rms", stats["gz"]["rms"], GZ_RMS_MAX_F),
    ]
    for name, val, lim in checks:
        if val is not None and lim is not None and val > lim:
            _publish_mpu_alert(ts_start, mpu_id, name, val, lim)
    if TEMP_MAX_F is not None:
        tmean = stats["temp"]["mean"]
        if tmean is not None and tmean > TEMP_MAX_F:
            _publish_mpu_alert(ts_start, mpu_id, "temp_mean", tmean, TEMP_MAX_F)
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
                    time.sleep(0.5)
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
                time.sleep(0.5)
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
    raw_sink = MySqlMpuSink() if SINK_MODE == "MYSQL" else CsvMpuSink(CSV_MPU_PATH)
    win_sink = MySqlMpuWinSink() if (SINK_MODE == "MYSQL" and MPU_AGG_ENABLE) else None

    # buffers por mpu_id
    buffers: Dict[int, Dict[str, Any]] = {
        1: {"bucket": None, "agg": _Agg()},
        2: {"bucket": None, "agg": _Agg()},
    }

    def flush_bucket(mpu_id: int, bucket_start: datetime, agg: _Agg):
        if not MPU_AGG_ENABLE or agg.n <= 0:
            return
        stats = agg.stats()
        if win_sink:
            try:
                win_sink.insert_window(bucket_start, mpu_id, agg.n, stats)
            except Exception as e:
                print(f"[MPU] Erro ao gravar janela: {e}")
        _check_thresholds_and_alert(bucket_start, mpu_id, stats)

    try:
        # tenta conectar/reconectar continuamente
        while not STOP.is_set():
            try:
                ser_reader.connect()
                break
            except Exception as e:
                print(f"[MPU] Falha ao abrir {SERIAL_PORT}: {e}. Tentando de novo em 2s...")
                time.sleep(2.0)

        # leitura contínua
        while not STOP.is_set():
            rec = ser_reader.read_one()
            if rec is None:
                time.sleep(0.002)
                continue

            ts = now_utc()                # dt
            ts_iso = ts.isoformat()       # iso para raw
            sample = {
                "ts_utc": ts_iso,
                "mpu_id": rec["mpu_id"],
                "ax_g": rec["ax_g"], "ay_g": rec["ay_g"], "az_g": rec["az_g"],
                "gx_dps": rec.get("gx_dps"), "gy_dps": rec.get("gy_dps"),
                "gz_dps": rec.get("gz_dps"), "temp_c": rec.get("temp_c")
            }

            # RAW -> grava
            try:
                if isinstance(raw_sink, MySqlMpuSink):
                    raw_sink.write_sample(ts_iso, sample)
                else:
                    raw_sink.write_sample(sample)
            except Exception as e:
                print(f"[MPU] Erro ao gravar amostra: {e}")

            # LIVE: publica amostra do MPU no Redis
            _publish_mpu_sample(sample)

            # AGG -> janela 200ms (ou valor de MPU_WINDOW_MS)
            mpu_id = int(rec["mpu_id"])
            bucket_start = floor_dt_window(ts, MPU_WINDOW_MS)
            buf = buffers[mpu_id]
            if buf["bucket"] is None:
                buf["bucket"] = bucket_start

            # se mudou de janela, fecha a anterior
            if bucket_start != buf["bucket"]:
                try:
                    flush_bucket(mpu_id, buf["bucket"], buf["agg"])
                finally:
                    buf["bucket"] = bucket_start
                    buf["agg"] = _Agg()

            # acumula na janela atual
            buf["agg"].add(
                rec["ax_g"], rec["ay_g"], rec["az_g"],
                rec.get("gx_dps"), rec.get("gy_dps"), rec.get("gz_dps"),
            )

    finally:
        # flush final de qualquer janela aberta
        for mpu_id in (1, 2):
            b = buffers[mpu_id]
            if b["bucket"] is not None and isinstance(b["agg"], _Agg):
                try:
                    flush_bucket(mpu_id, b["bucket"], b["agg"])
                except Exception:
                    pass
        try:
            if hasattr(raw_sink, "close"):
                raw_sink.close()
        except Exception:
            pass
        if win_sink:
            try:
                win_sink.close()
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
