# sim_mysql_feeder.py (autodetect + períodos por ENV + UTC/LOCAL timestamp)
import os
import time
import random
import mysql.connector as mysql
from mysql.connector.errors import ProgrammingError

# -----------------------------
# Conexão
# -----------------------------
DB = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER", "entry"),
    "password": os.getenv("DB_PASS", "root"),
    "database": os.getenv("DB_NAME", "gmdigital"),
}

# -----------------------------
# Tabelas alvo
# -----------------------------
TABLE_OPC = os.getenv("TABLE_OPC", "opc_samples")
TABLE_MPU = os.getenv("TABLE_MPU", "mpu_samples")

# -----------------------------
# Autodetecção de colunas
# -----------------------------
OPC_NAME_CANDS  = [os.getenv("OPC_NAME_COL", ""), "name", "signal", "tag", "point"]
OPC_VALUE_CANDS = [os.getenv("OPC_VALUE_COL",""), "value_bool", "value", "val", "state", "bit", "status", "v", "bool", "value_int"]
MPU_ID_CANDS    = [os.getenv("MPU_ID_COL",""), "mpu_id", "id", "sensor_id"]
AX_CANDS        = ["ax_g", "ax"]
AY_CANDS        = ["ay_g", "ay"]
AZ_CANDS        = ["az_g", "az"]

# String -> inteiro (se id for INT no schema)
MAP_IDS = {
    "MPUA1": int(os.getenv("MPU1_INT", "1")),
    "MPUA2": int(os.getenv("MPU2_INT", "2")),
}

# -----------------------------
# Modo de timestamp
# -----------------------------
# UTC: usa UTC_TIMESTAMP(3) (padrão e recomendado)
# LOCAL: usa NOW(3) e define time_zone da sessão (SESSION_TZ)
TIMESTAMP_MODE = os.getenv("TIMESTAMP_MODE", "UTC").upper()  # "UTC" ou "LOCAL"
SESSION_TZ     = os.getenv("SESSION_TZ", "-03:00")           # ex.: "-03:00" ou "America/Sao_Paulo"

def _ts_expr() -> str:
    """Retorna a expressão SQL correta para timestamp, conforme o modo."""
    return "UTC_TIMESTAMP(3)" if TIMESTAMP_MODE == "UTC" else "NOW(3)"

# -----------------------------
# Helpers de autodetecção
# -----------------------------
def fetch_columns(cur, table):
    cur.execute("""
        SELECT COLUMN_NAME FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
    """, (table,))
    return {name for (name,) in cur.fetchall()}

def pick(cols, candidates, label):
    for c in candidates:
        c = c.strip()
        if not c:
            continue
        if c in cols:
            return c
    raise RuntimeError(
        f"Nenhuma coluna compatível encontrada para {label}. "
        f"Tente definir via ENV. Colunas disponíveis: {sorted(cols)}"
    )

def autoconfigure(cur):
    # OPC
    opc_cols = fetch_columns(cur, TABLE_OPC)
    opc_name_col  = pick(opc_cols, OPC_NAME_CANDS,  f"{TABLE_OPC}.<name>")
    opc_value_col = pick(opc_cols, OPC_VALUE_CANDS, f"{TABLE_OPC}.<value>")

    # MPU
    mpu_cols = fetch_columns(cur, TABLE_MPU)
    mpu_id_col = pick(mpu_cols, MPU_ID_CANDS, f"{TABLE_MPU}.<id>")
    ax_col = pick(mpu_cols, AX_CANDS, f"{TABLE_MPU}.ax")
    ay_col = pick(mpu_cols, AY_CANDS, f"{TABLE_MPU}.ay")
    az_col = pick(mpu_cols, AZ_CANDS, f"{TABLE_MPU}.az")

    return {
        "opc_name": opc_name_col,
        "opc_value": opc_value_col,
        "mpu_id": mpu_id_col,
        "ax": ax_col, "ay": ay_col, "az": az_col,
    }

# -----------------------------
# Inserts
# -----------------------------
def insert_opc(cur, cfg, name, val):
    sql = (
        f"INSERT INTO `{TABLE_OPC}` (ts_utc, `{cfg['opc_name']}`, `{cfg['opc_value']}`) "
        f"VALUES ({_ts_expr()}, %s, %s)"
    )
    cur.execute(sql, (name, int(val)))

def insert_mpu(cur, cfg, m_id_str, ax, ay, az):
    # tenta mapear para INT; se não achar, usa string (caso id seja VARCHAR)
    m_id = MAP_IDS.get(m_id_str, m_id_str)
    sql = (
        f"INSERT INTO `{TABLE_MPU}` "
        f"(ts_utc, `{cfg['mpu_id']}`, `{cfg['ax']}`, `{cfg['ay']}`, `{cfg['az']}`) "
        f"VALUES ({_ts_expr()}, %s, %s, %s, %s)"
    )
    cur.execute(sql, (m_id, ax, ay, az))

# -----------------------------
# Sinal "MPU" de teste
# -----------------------------
def wave(noise=0.02):
    base = 1.0
    dyn  = (random.random() - 0.5) * 0.04
    n1 = (random.random() - 0.5) * noise
    n2 = (random.random() - 0.5) * noise
    n3 = (random.random() - 0.5) * noise
    return base + dyn + n1, base + dyn/2 + n2, base - dyn/2 + n3

# -----------------------------
# Controle de períodos por ENV
# -----------------------------
ACT1_PERIOD = float(os.getenv("ACT1_PERIOD", "2.0"))  # período em segundos (2.0 => troca a cada 1s)
ACT1_DUTY   = float(os.getenv("ACT1_DUTY",   "0.5"))  # 0.5 => 50% aberto / 50% fechado
ACT2_PERIOD = float(os.getenv("ACT2_PERIOD", "2.0"))
ACT2_DUTY   = float(os.getenv("ACT2_DUTY",   "0.5"))
ACT2_OFFSET = float(os.getenv("ACT2_OFFSET", "0.0"))  # defasagem do A2 (s)

# Jitter opcional (ms) para não ficar perfeitamente cravado (0 = desligado)
JITTER_MS = int(os.getenv("JITTER_MS", "0"))

# -----------------------------
# Máquina de ciclos não bloqueante
# -----------------------------
class ActuatorCycle:
    def __init__(self, act_id: int, period_s: float, duty_open: float, start_offset_s: float = 0.0):
        self.act_id = act_id
        self.period_s = max(0.05, period_s)            # sanity
        self.duty_open = max(0.0, min(1.0, duty_open)) # clamp
        self.nameS1 = f"Recuado_{act_id}S1"
        self.nameS2 = f"Avancado_{act_id}S2"
        now = time.time() + start_offset_s
        self.next_edge_times = []
        self._schedule_cycle(now)

    def _schedule_cycle(self, start_t):
        # jitter opcional
        if JITTER_MS > 0:
            jitter = (random.random() * 2 - 1) * (JITTER_MS / 1000.0)
            start_t = start_t + jitter

        t_rec = (1.0 - self.duty_open) * self.period_s  # tempo recuado
        # início do ciclo: RECUADO (S1=1, S2=0)
        self.next_edge_times.append((start_t,  self.nameS1, 1))
        self.next_edge_times.append((start_t,  self.nameS2, 0))
        # transição para AVANÇADO
        t_to_adv = start_t + t_rec
        self.next_edge_times.append((t_to_adv, self.nameS1, 0))
        self.next_edge_times.append((t_to_adv, self.nameS2, 1))
        # próximo ciclo
        self.next_cycle_start = start_t + self.period_s

    def maybe_fire(self, cur, cfg, now):
        fired = 0
        keep = []
        for (t, name, val) in self.next_edge_times:
            if now >= t:
                insert_opc(cur, cfg, name, val)
                fired += 1
            else:
                keep.append((t, name, val))
        self.next_edge_times = keep
        if not self.next_edge_times and now >= (self.next_cycle_start - 1e-4):
            self._schedule_cycle(self.next_cycle_start)
        return fired

# -----------------------------
# Loop principal
# -----------------------------
def run():
    cn = mysql.connect(**DB)
    cn.autocommit = True
    cur = cn.cursor()

    # Ajuste de time_zone da sessão (só impacta NOW(); UTC_TIMESTAMP ignora)
    try:
        if TIMESTAMP_MODE == "LOCAL" and SESSION_TZ:
            cur.execute("SET time_zone = %s", (SESSION_TZ,))
    except Exception as e:
        print(f"[feeder] aviso: não consegui definir time_zone da sessão: {e}")

    # Descobre as colunas certas
    cfg = autoconfigure(cur)

    print(
        f"[feeder] OPC: name={cfg['opc_name']} value={cfg['opc_value']} | "
        f"MPU: id={cfg['mpu_id']} ax={cfg['ax']} ay={cfg['ay']} az={cfg['az']}"
    )
    print(
        f"[feeder] A1: period={ACT1_PERIOD:.3f}s duty={ACT1_DUTY:.2f} | "
        f"A2: period={ACT2_PERIOD:.3f}s duty={ACT2_DUTY:.2f} offset={ACT2_OFFSET:.3f}s | "
        f"jitter={JITTER_MS}ms | ts_mode={TIMESTAMP_MODE} tz={SESSION_TZ}"
    )

    # Taxa de geração de MPU
    mpu_rate_hz = float(os.getenv("MPU_RATE_HZ", "100"))
    mpu_dt = 1.0 / max(1.0, mpu_rate_hz)
    next_mpu = time.time()

    # Dois atuadores com períodos/duty controláveis por ENV
    a1 = ActuatorCycle(act_id=1, period_s=ACT1_PERIOD, duty_open=ACT1_DUTY, start_offset_s=0.0)
    a2 = ActuatorCycle(act_id=2, period_s=ACT2_PERIOD, duty_open=ACT2_DUTY, start_offset_s=ACT2_OFFSET)

    try:
        while True:
            now = time.time()

            # MPU contínuo
            if now >= next_mpu:
                ax1, ay1, az1 = wave()
                ax2, ay2, az2 = wave()
                insert_mpu(cur, cfg, "MPUA1", ax1, ay1, az1)
                insert_mpu(cur, cfg, "MPUA2", ax2, ay2, az2)
                next_mpu += mpu_dt

            # Bordas OPC agendadas
            a1.maybe_fire(cur, cfg, now)
            a2.maybe_fire(cur, cfg, now)

            time.sleep(0.002)  # respira
    finally:
        cur.close()
        cn.close()

if __name__ == "__main__":
    run()
