# app.py — DUAL “idêntico ao OPCUA” + MPU gravando amostras brutas (sem windows)
# Tabelas: festo_dt.opc_samples, festo_dt.mpu_samples
# Modos: SIMULATE | OPCUA | SERIAL_MPU | DUAL
import os, csv, json, time, signal, sys
from typing import List, Dict, Any, Iterable, Optional
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

# ===== UTC helpers =====
def _get_utc():
    try:
        from datetime import UTC
        return UTC
    except Exception:
        from datetime import timezone
        return timezone.utc
UTC = _get_utc()

def now_utc() -> datetime:
    from datetime import datetime as _dt
    return _dt.now(UTC)

def now_utc_iso() -> str:
    return now_utc().isoformat()

def iso_to_mysql_dt6(iso_str: str) -> str:
    s = iso_str.replace("T", " ")
    if "+" in s: s = s.split("+", 1)[0]
    if s.endswith("Z"): s = s[:-1]
    if "." not in s: s += ".000000"
    else:
        head, frac = s.split(".", 1)
        s = f"{head}.{(frac + '000000')[:6]}"
    return s

# ===== Config =====
DATA_MODE = os.getenv("DATA_MODE", "DUAL").upper()      # SIMULATE | OPCUA | SERIAL_MPU | DUAL
SINK_MODE = os.getenv("SINK_MODE", "MYSQL").upper()     # MYSQL | CSV

# OPC UA
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "0.2"))  # 200 ms
OPCUA_ENDPOINT = os.getenv("OPCUA_ENDPOINT", "opc.tcp://192.168.0.40:4840")
OPCUA_USER = os.getenv("OPCUA_USER", "")
OPCUA_PASS = os.getenv("OPCUA_PASS", "")
NODES_CSV = os.getenv("NODES_CSV", "nodes.csv")

# Serial MPU
SERIAL_PORT = os.getenv("SERIAL_PORT", "COM3")
SERIAL_BAUD = int(os.getenv("SERIAL_BAUD", "115200"))
SERIAL_TIMEOUT = float(os.getenv("SERIAL_TIMEOUT", "0.2"))

# CSV
CSV_OPC_PATH = os.getenv("CSV_OPC_PATH", "bank_opc.csv")
CSV_MPU_PATH = os.getenv("CSV_MPU_PATH", "bank_mpu.csv")

# MySQL
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB   = os.getenv("MYSQL_DB", "festo_dt")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASS = os.getenv("MYSQL_PASS", "")

# Live bus (Redis)
REDIS_ENABLE = os.getenv("REDIS_ENABLE", "true").lower() in {"1","true","yes","on"}
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
_rcli = None
if REDIS_ENABLE:
    try:
        import redis
        _rcli = redis.from_url(REDIS_URL, decode_responses=True)
    except Exception:
        _rcli = None

# ===== nodes.csv loader (robusto) =====
def load_nodes(csv_path: str) -> List[Dict[str, str]]:
    import os as _os
    full = csv_path if _os.path.isabs(csv_path) else _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), csv_path)
    print(f"[DEBUG] nodes.csv: {full}")
    if not _os.path.exists(full):
        raise FileNotFoundError(full)
    out: List[Dict[str, str]] = []
    with open(full, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            out.append({"name": row["name"].strip(),
                        "nodeid": row.get("nodeid","").strip(),
                        "datatype": row.get("datatype","Boolean").strip()})
    if not out: raise RuntimeError("nodes.csv vazio")
    return out

# ===== Fontes =====
class Simulator:
    def __init__(self, items: List[Dict[str, str]]):
        self.state = {i["name"]: False for i in items}
    def read_all(self) -> Dict[str, Any]:
        for k in list(self.state.keys()):
            self.state[k] = not self.state[k]
        return dict(self.state)

class OpcUaReader:
    def __init__(self, endpoint: str, items: List[Dict[str, str]]):
        self.endpoint = endpoint; self.items = items; self.client = None
    def connect(self):
        try:
            from opcua import Client
        except Exception as e:
            print("Instale: pip install opcua"); raise e
        self.client = Client(self.endpoint)
        if OPCUA_USER and OPCUA_PASS:
            self.client.set_user(OPCUA_USER); self.client.set_password(OPCUA_PASS)
        self.client.application_uri = "opcua-py-runner"
        self.client.secure_channel_timeout = 600000
        self.client.session_timeout = 600000
        try: self.client.set_keepalive(20000)
        except Exception: pass
        self.client.connect()
    def disconnect(self):
        if self.client:
            try: self.client.disconnect()
            except Exception: pass
            self.client = None
    def read_all(self) -> Dict[str, Any]:
        out = {}
        for it in self.items:
            node = self.client.get_node(it["nodeid"])
            try: val = node.get_value()
            except Exception: val = None
            out[it["name"]] = val
        return out

class SerialMpuReader:
    def __init__(self, port: str, baud: int, timeout: float):
        try:
            import serial
        except Exception as e:
            print("Instale: pip install pyserial"); raise e
        self.serial_module = serial
        self.port = port; self.baud = baud; self.timeout = timeout; self.ser = None
    def connect(self):
        print(f"[MPU] Abrindo {self.port} @ {self.baud}...")
        self.ser = self.serial_module.Serial(self.port, self.baud, timeout=self.timeout)
        self.ser.reset_input_buffer(); print("[MPU] Conectado.")
    def disconnect(self):
        if self.ser:
            try: self.ser.close(); print("[MPU] Desconectado.")
            except Exception: pass
            self.ser = None
    def _extract_json(self, s: str) -> Optional[str]:
        i = s.find("{"); j = s.rfind("}")
        if i >= 0 and j > i: return s[i:j+1]
        return None
    def read_one(self) -> Optional[Dict[str, Any]]:
        if not self.ser: return None
        line = self.ser.readline()
        if not line: return None
        try:
            s = line.decode("utf-8", errors="ignore").strip()
            if not s: return None
            obj = json.loads(self._extract_json(s) or s)
        except Exception: return None
        sensor = str(obj.get("id", obj.get("sensor", ""))).strip().upper()
        if sensor not in {"MPUA1","MPUA2"}: return None
        mpu_id = 1 if sensor == "MPUA1" else 2
        try:
            ax = float(obj["ax"]); ay = float(obj["ay"]); az = float(obj["az"])
        except Exception: return None
        rec: Dict[str, Any] = {"mpu_id": mpu_id, "ax_g": ax, "ay_g": ay, "az_g": az}
        for k_src, k_dst in [("gx","gx_dps"),("gy","gy_dps"),("gz","gz_dps"),
                             ("gx_dps","gx_dps"),("gy_dps","gy_dps"),("gz_dps","gz_dps")]:
            if k_src in obj:
                try: rec[k_dst] = float(obj[k_src])
                except Exception: pass
        if "temp" in obj:
            try: rec["temp_c"] = float(obj["temp"])
            except Exception: pass
        return rec

# ===== Sinks =====
class CsvOpcSink:
    def __init__(self, path: str, fieldnames: Iterable[str]):
        self.path = path; self.fields = ["ts_utc", *fieldnames]
        if not os.path.exists(self.path):
            with open(self.path, "w", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=self.fields).writeheader()
    def write_row(self, row: Dict[str, Any]):
        with open(self.path, "a", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=self.fields).writerow(row)
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
            host=MYSQL_HOST, port=MYSQL_PORT, database=MYSQL_DB,
            user=MYSQL_USER, password=MYSQL_PASS, connection_timeout=5
        )
        self.conn.autocommit = False  # commits explícitos
    def close(self):
        try: self.conn.close()
        except Exception: pass

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
        """); cur.close(); self.conn.commit()
    def write_many(self, ts_iso: str, values: Dict[str, Any]):
        ts_mysql = iso_to_mysql_dt6(ts_iso)
        cur = self.conn.cursor()
        cur.executemany(
            "INSERT INTO opc_samples (ts_utc,name,value_bool) VALUES (%s,%s,%s)",
            [(ts_mysql, name, (None if v is None else int(bool(v)))) for name, v in values.items()]
        )
        cur.close(); self.conn.commit()

class MySqlMpuSink(MySqlBase):
    def __init__(self):
        super().__init__()
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS mpu_samples (
              id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
              ts_utc DATETIME(6) NOT NULL,
              mpu_id TINYINT UNSIGNED NOT NULL,
              ax_g DOUBLE NOT NULL, ay_g DOUBLE NOT NULL, az_g DOUBLE NOT NULL,
              gx_dps DOUBLE NULL, gy_dps DOUBLE NULL, gz_dps DOUBLE NULL,
              INDEX idx_mpu_id_ts (mpu_id, ts_utc),
              INDEX idx_mpu_ts (ts_utc)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """); cur.close(); self.conn.commit()
    def write_sample(self, ts_iso: str, rec: Dict[str, Any]):
        ts_mysql = iso_to_mysql_dt6(ts_iso)
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO mpu_samples
              (ts_utc,mpu_id,ax_g,ay_g,az_g,gx_dps,gy_dps,gz_dps)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            ts_mysql, int(rec["mpu_id"]),
            float(rec["ax_g"]), float(rec["ay_g"]), float(rec["az_g"]),
            (None if rec.get("gx_dps") is None else float(rec["gx_dps"])),
            (None if rec.get("gy_dps") is None else float(rec["gy_dps"])),
            (None if rec.get("gz_dps") is None else float(rec["gz_dps"]))
        ))
        cur.close(); self.conn.commit()

# ===== Live publish =====
from threading import Event
STOP = Event()
def handle_stop(signum, frame): STOP.set()
signal.signal(signal.SIGINT, handle_stop); signal.signal(signal.SIGTERM, handle_stop)

def _publish_opc_events(ts_iso: str, values: Dict[str, Any]):
    if not _rcli: return
    for name, v in values.items():
        evt = {"type":"opc_event","ts_utc":ts_iso,"name":name,"value_bool":(None if v is None else int(bool(v)))}
        try: _rcli.publish("opc_samples", json.dumps(evt))
        except Exception: pass

def _publish_mpu_sample(sample: Dict[str, Any]):
    if not _rcli: return
    try:
        id_str = "MPUA1" if int(sample["mpu_id"]) == 1 else "MPUA2"
        msg = {"type":"mpu_sample","ts_utc":sample["ts_utc"],"id":id_str,
               "ax_g":sample["ax_g"],"ay_g":sample["ay_g"],"az_g":sample["az_g"],
               "gx_dps":sample.get("gx_dps"),"gy_dps":sample.get("gy_dps"),
               "gz_dps":sample.get("gz_dps"),"temp_c":sample.get("temp_c")}
        _rcli.publish("mpu_samples", json.dumps(msg))
    except Exception: pass

# ===== Loops =====
def opc_loop():
    items = load_nodes(NODES_CSV)
    names = [i["name"] for i in items]
    # Fonte
    if DATA_MODE == "SIMULATE":
        reader, connected = Simulator(items), True
    else:
        reader, connected = OpcUaReader(OPCUA_ENDPOINT, items), False
    # Sink
    sink = MySqlOpcSink() if SINK_MODE == "MYSQL" else CsvOpcSink(CSV_OPC_PATH, names)

    interval = max(0.01, float(POLL_INTERVAL))
    try:
        if hasattr(reader, "connect") and not connected:
            while not STOP.is_set() and not connected:
                try: reader.connect(); connected = True
                except Exception as e:
                    print(f"[OPC] Conexão falhou: {e}. Retentando em 0.5s..."); time.sleep(0.5)

        # === TICK DETERMINÍSTICO (idêntico ao modo OPCUA) ===
        next_t = time.perf_counter()
        while not STOP.is_set():
            next_t += interval
            try:
                values = reader.read_all()
            except Exception as e:
                print(f"[OPC] Erro leitura: {e}")
                if hasattr(reader, "disconnect"):
                    try: reader.disconnect()
                    except Exception: pass
                    connected = False
                    while not STOP.is_set() and not connected:
                        try: reader.connect(); connected = True
                        except Exception as e2:
                            print(f"[OPC] Reconnect falhou: {e2}. Retentando em 0.5s..."); time.sleep(0.5)
                values = {}
            ts = now_utc_iso()
            try:
                if isinstance(sink, MySqlOpcSink): sink.write_many(ts, values)
                else: sink.write_row({"ts_utc": ts, **values})
            except Exception as e:
                print(f"[OPC] Erro gravação: {e}")
            _publish_opc_events(ts, values)
            rem = next_t - time.perf_counter()
            if rem > 0: time.sleep(rem)
            else: next_t = time.perf_counter()
    finally:
        try: sink.close()
        except Exception: pass
        if hasattr(reader, "disconnect"): reader.disconnect()

def mpu_loop_forever():
    ser = SerialMpuReader(SERIAL_PORT, SERIAL_BAUD, SERIAL_TIMEOUT)
    sink = MySqlMpuSink() if SINK_MODE == "MYSQL" else CsvMpuSink(CSV_MPU_PATH)
    try:
        while True:
            try: ser.connect(); break
            except Exception as e:
                print(f"[MPU] Falha porta {SERIAL_PORT}: {e}. Retentando em 2s..."); time.sleep(2.0)
        while True:
            rec = ser.read_one()
            if rec is None:
                time.sleep(0.002); continue
            ts_iso = now_utc_iso()
            sample = {
                "ts_utc": ts_iso, "mpu_id": rec["mpu_id"],
                "ax_g": rec["ax_g"], "ay_g": rec["ay_g"], "az_g": rec["az_g"],
                "gx_dps": rec.get("gx_dps"), "gy_dps": rec.get("gy_dps"), "gz_dps": rec.get("gz_dps"),
                "temp_c": rec.get("temp_c")
            }
            try:
                if isinstance(sink, MySqlMpuSink): sink.write_sample(ts_iso, sample)
                else: sink.write_sample(sample)
            except Exception as e:
                print(f"[MPU] Erro gravação: {e}")
            _publish_mpu_sample(sample)
    except KeyboardInterrupt:
        pass
    finally:
        try:
            if hasattr(sink, "close"): sink.close()
        except Exception: pass
        ser.disconnect()

# ===== Execução =====
def run_dual():
    # OPC no processo principal (mesmo laço do modo OPCUA). MPU em outro processo.
    from multiprocessing import Process
    p_mpu = Process(target=mpu_loop_forever, name="mpu_proc", daemon=True)
    p_mpu.start()
    try:
        opc_loop()
    finally:
        if p_mpu.is_alive():
            p_mpu.terminate()
            p_mpu.join(timeout=3.0)

def run_opc_only(): opc_loop()
def run_mpu_only(): mpu_loop_forever()

def main():
    mode = DATA_MODE
    if mode == "DUAL": run_dual()
    elif mode in ("OPCUA","SIMULATE"): run_opc_only()
    elif mode == "SERIAL_MPU": run_mpu_only()
    else:
        print(f"[ERRO] DATA_MODE inválido: {mode} (use SIMULATE | OPCUA | SERIAL_MPU | DUAL)")
        sys.exit(2)

if __name__ == "__main__":
    main()
