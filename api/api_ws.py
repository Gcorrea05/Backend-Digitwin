# api/api_ws.py — HTTP only (sem WS/Redis), com CORS robusto e compat de parâmetros
import os
import re
import time
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Any, Dict, Tuple

from dotenv import load_dotenv, find_dotenv
from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from mysql.connector.errors import PoolError

# -----------------------------------------------------------------------------
# .env
# -----------------------------------------------------------------------------
load_dotenv(find_dotenv())
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

# -----------------------------------------------------------------------------
# CORS + FastAPI (precisa vir ANTES das rotas)
# -----------------------------------------------------------------------------
origins_env = os.getenv("ALLOWED_ORIGINS", "*")
ALLOWED_ORIGINS = [o.strip() for o in origins_env.split(",") if o.strip()]
wildcard = (len(ALLOWED_ORIGINS) == 1 and ALLOWED_ORIGINS[0] == "*")

app = FastAPI(title="Festo DT API (HTTP only)", version="2.1.0")
print(f"[CORS] ALLOWED_ORIGINS = {ALLOWED_ORIGINS} (wildcard={wildcard})")

# Força wildcard em DEV para evitar bloqueio de CORS local
if os.getenv("DEV_FORCE_CORS_WILDCARD", "1") == "1":
    ALLOWED_ORIGINS = ["*"]
    wildcard = True
    print("[CORS] DEV_FORCE_CORS_WILDCARD=1 -> usando '*'")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if wildcard else ALLOWED_ORIGINS,
    allow_credentials=not wildcard,  # '*' não pode credenciais
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
    max_age=600,
)

@app.exception_handler(PoolError)
async def pool_error_handler(_, __):
    return JSONResponse(status_code=503, content={"detail": "DB pool exhausted"})

# -----------------------------------------------------------------------------
# Imports locais (podem vir depois do app)
# -----------------------------------------------------------------------------
from .routes import alerts
from .services.analytics import get_kpis
from .services.cycle import get_cycle_rate
from .services.analytics_graphs import get_vibration_data
from .database import get_db

# -----------------------------------------------------------------------------
# Tipos e helpers gerais
# -----------------------------------------------------------------------------
StableState = str  # "RECUADO" | "AVANÇADO"
FaultState  = str  # "NONE" | "FAULT_TIMEOUT" | "FAULT_SENSORS_CONFLICT"

from dataclasses import dataclass

@dataclass
class _LatchCfg:
    id: str
    expected_ms: int
    debounce_ms: int
    timeout_factor: float
    v_av: str      # válvula que AVANÇA/ABRE
    v_rec: str     # válvula que RECUA/FECHA
    s_adv: str     # sensor avançado
    s_rec: str     # sensor recuado

@dataclass
class _LatchState:
    displayed: StableState = "RECUADO"
    pending_target: Optional[str] = None  # "AV"|"REC"|None
    fault: FaultState = "NONE"
    started_at: Optional[datetime] = None
    elapsed_ms: int = 0

class _Deb:
    __slots__ = ("v","t")
    def __init__(self):
        self.v = False
        self.t = datetime.min.replace(tzinfo=timezone.utc)

def _stable(prev:_Deb, now_v:bool, ts:datetime, debounce_ms:int):
    if now_v != prev.v:
        prev.v = now_v
        prev.t = ts
        return False, now_v
    return ((ts - prev.t).total_seconds()*1000 >= debounce_ms), prev.v

def _compute_latched_state(
    cfg: _LatchCfg,
    rows: List[Tuple[datetime,str,Optional[int]]],
    initial_displayed: StableState = "RECUADO",
) -> _LatchState:
    st = _LatchState(displayed=initial_displayed)
    rec = _Deb(); adv = _Deb()

    for ts, name, vb in rows:
        if vb is None:
            continue
        v = bool(vb)

        if name == cfg.s_rec: _stable(rec, v, ts, cfg.debounce_ms)
        if name == cfg.s_adv: _stable(adv, v, ts, cfg.debounce_ms)

        rec_st, rec_val = _stable(rec, rec.v, ts, cfg.debounce_ms)
        adv_st, adv_val = _stable(adv, adv.v, ts, cfg.debounce_ms)

        if rec_st and adv_st and rec_val and adv_val:
            st.fault = "FAULT_SENSORS_CONFLICT"

        if name == cfg.v_av and v:
            st.pending_target = "AV";  st.started_at = ts; st.fault = "NONE"
        elif name == cfg.v_rec and v:
            st.pending_target = "REC"; st.started_at = ts; st.fault = "NONE"

        if not st.pending_target:
            if rec_st and rec_val and not (adv_st and adv_val):
                st.displayed = "RECUADO"
            elif adv_st and adv_val and not (rec_st and rec_val):
                st.displayed = "AVANÇADO"

        if st.pending_target:
            if st.started_at:
                st.elapsed_ms = int((ts - st.started_at).total_seconds()*1000)
                if st.elapsed_ms > int(cfg.expected_ms * cfg.timeout_factor):
                    st.fault = "FAULT_TIMEOUT"

            if st.pending_target == "AV" and adv_st and adv_val:
                st.displayed = "AVANÇADO"; st.pending_target = None; st.started_at = None; st.elapsed_ms = 0; st.fault = "NONE"
            elif st.pending_target == "REC" and rec_st and rec_val:
                st.displayed = "RECUADO";  st.pending_target = None; st.started_at = None; st.elapsed_ms = 0; st.fault = "NONE"

    return st

# Cedilha padronizada e nomes dos teus sinais
_CFG_A1 = _LatchCfg(
    id="A1", expected_ms=3000, debounce_ms=80, timeout_factor=1.5,
    v_av="V1_12", v_rec="V1_14", s_adv="Avancado_1S2", s_rec="Recuado_1S1"
)
_CFG_A2 = _LatchCfg(
    id="A2", expected_ms=500, debounce_ms=80, timeout_factor=1.5,
    v_av="V2_12", v_rec="V2_14", s_adv="Avancado_2S2", s_rec="Recuado_2S1"
)
_NAMES_LATCH = (_CFG_A1.v_av, _CFG_A1.v_rec, _CFG_A1.s_adv, _CFG_A1.s_rec,
                _CFG_A2.v_av, _CFG_A2.v_rec, _CFG_A2.s_adv, _CFG_A2.s_rec)

# -----------------------------------------------------------------------------
# Utilidades DB / datas
# -----------------------------------------------------------------------------
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
    if row is None: return None
    if isinstance(row, (list, tuple)): return row[0]
    if isinstance(row, dict):
        for _, v in row.items(): return v
    return row

def col(row: Any, key_or_index: Any) -> Any:
    if row is None: return None
    if isinstance(row, dict): return row.get(key_or_index)
    if isinstance(key_or_index, int): return row[key_or_index]
    return None

def cols(row: Any, *keys_or_idx: Any) -> Tuple[Any, ...]:
    return tuple(col(row, k) for k in keys_or_idx)

def table_exists(schema: str, table: str) -> bool:
    q = """
    SELECT COUNT(*) AS c
    FROM information_schema.tables
    WHERE table_schema=%s AND table_name=%s
    """
    row = fetch_one(q, (schema, table))
    v = first_col(row)
    try: return int(v) > 0
    except Exception: return False

def _coerce_to_datetime(value: Any) -> Optional[datetime]:
    if value is None: return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(value, tz=timezone.utc)
    elif isinstance(value, str):
        s = value.strip()
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
    dt = _coerce_to_datetime(value)
    if dt is None: return None
    return dt.isoformat().replace("+00:00", "Z")

def _fetch_latest_rows(names: Tuple[str, ...]) -> Dict[str, Dict[str, Any]]:
    """
    Retorna { name: {"value_bool": int|None, "ts_utc": datetime|None} } para cada name informado.
    Usa ROW_NUMBER() (MySQL 8+). Se não suportado, cai no fallback por JOIN com MAX(ts_utc).
    """
    if not names:
        return {}
    placeholders = ", ".join(["%s"] * len(names))

    # Tenta com janela (MySQL 8+)
    try:
        sql = f"""
        WITH latest AS (
          SELECT 
            name, value_bool, ts_utc,
            ROW_NUMBER() OVER (PARTITION BY name ORDER BY ts_utc DESC) AS rn
          FROM opc_samples
          WHERE name IN ({placeholders})
        )
        SELECT name, value_bool, ts_utc
        FROM latest
        WHERE rn = 1
        """
        rows = fetch_all(sql, names)
    except Exception:
        # Fallback compatível (MySQL < 8): join com MAX(ts_utc)
        sql = f"""
        SELECT s.name, s.value_bool, s.ts_utc
        FROM opc_samples s
        JOIN (
            SELECT name, MAX(ts_utc) AS ts_utc
            FROM opc_samples
            WHERE name IN ({placeholders})
            GROUP BY name
        ) m ON m.name = s.name AND m.ts_utc = s.ts_utc
        """
        rows = fetch_all(sql, names)

    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        n = col(r, "name") or col(r, 0)
        vb = col(r, "value_bool") if isinstance(r, dict) else r[1]
        ts = col(r, "ts_utc") if isinstance(r, dict) else r[2]
        out[str(n)] = {"value_bool": (None if vb is None else int(vb)), "ts_utc": ts}
    # Garante chave para todos os names
    for n in names:
        out.setdefault(n, {"value_bool": None, "ts_utc": None})
    return out


def _infer_state_from_latest(cfg: _LatchCfg, latest: Dict[str, Dict[str, Any]]):
    """
    Usa APENAS os últimos valores (válvula e sensores) para montar o estado.
    Regras:
      - state: pelo sensor alvo ativo (adv=1 -> AVANÇADO; rec=1 -> RECUADO; ambos 1 -> conflito)
      - pending: se o comando mais recente (V*_12=1 ou V*_14=1) é posterior ao sensor alvo ainda não confirmado.
      - elapsed_ms: now - ts_cmd (se pending)
      - fault: timeout se pending e elapsed_ms > expected_ms * timeout_factor
    """
    now = datetime.now(timezone.utc)
    # últimos valores
    adv = latest.get(cfg.s_adv, {})
    rec = latest.get(cfg.s_rec, {})
    vav = latest.get(cfg.v_av, {})
    vrc = latest.get(cfg.v_rec, {})

    adv_v = int(adv.get("value_bool") or 0)
    rec_v = int(rec.get("value_bool") or 0)

    t_adv = _coerce_to_datetime(adv.get("ts_utc"))
    t_rec = _coerce_to_datetime(rec.get("ts_utc"))
    t_vav = _coerce_to_datetime(vav.get("ts_utc"))
    t_vrc = _coerce_to_datetime(vrc.get("ts_utc"))

    # Estado básico por sensor
    displayed = "RECUADO"
    fault = "NONE"
    if adv_v and rec_v:
        displayed = "RECUADO"  # arbitrário; marcamos conflito
        fault = "FAULT_SENSORS_CONFLICT"
    elif adv_v:
        displayed = "AVANÇADO"
    elif rec_v:
        displayed = "RECUADO"
    else:
        # nenhum fim-de-curso ativo -> mantenha RECUADO (ou heurística)
        displayed = "RECUADO"

    # Inferir pending pelo comando mais recente (apenas se o sensor alvo AINDA não confirmou)
    # Consideramos só comando "ligando" (última linha pode ser 0, então t_vav/t_vrc valem como "último evento")
    pending = None
    started_at = None
    elapsed_ms = 0

    # qual comando é mais recente?
    last_cmd_ts = None
    last_cmd_kind = None  # "AV" | "REC"
    if t_vav and (not t_vrc or t_vav >= t_vrc):
        last_cmd_ts = t_vav; last_cmd_kind = "AV"
    elif t_vrc:
        last_cmd_ts = t_vrc; last_cmd_kind = "REC"

    # Se o último comando é mais novo que o sensor alvo correspondente (ou se alvo não está ativo), considerar "pending"
    if last_cmd_kind == "AV":
        # alvo: adv
        if not adv_v or (t_adv and last_cmd_ts and last_cmd_ts > t_adv):
            pending = "AV"
            started_at = last_cmd_ts or now
    elif last_cmd_kind == "REC":
        # alvo: rec
        if not rec_v or (t_rec and last_cmd_ts and last_cmd_ts > t_rec):
            pending = "REC"
            started_at = last_cmd_ts or now

    if pending and started_at:
        elapsed_ms = max(0, int((now - started_at).total_seconds() * 1000))
        if elapsed_ms > int(cfg.expected_ms * cfg.timeout_factor):
            fault = "FAULT_TIMEOUT"

    return {
        "state": displayed,
        "pending": pending,
        "fault": fault,
        "elapsed_ms": elapsed_ms,
        "started_at": (started_at.isoformat() if started_at else None),
        # depuração opcional
        "_debug": {
            "t_adv": dt_to_iso_utc(t_adv), "t_rec": dt_to_iso_utc(t_rec),
            "t_vav": dt_to_iso_utc(t_vav), "t_vrc": dt_to_iso_utc(t_vrc),
            "adv_v": adv_v, "rec_v": rec_v,
        }
    }


# -----------------------------------------------------------------------------
# Cache curtinho
# -----------------------------------------------------------------------------
_cache: Dict[str, Dict[str, Any]] = {}
def cached(key: str, ttl: float, producer):
    now = time.time()
    e = _cache.get(key)
    if e and now - e["ts"] < ttl:
        return e["val"]
    val = producer()
    _cache[key] = {"ts": now, "val": val}
    return val

# -----------------------------------------------------------------------------
# Helpers de parse de janela, compat de IDs
# -----------------------------------------------------------------------------
def _normalize_since(since: Optional[str], last: Optional[int], seconds: Optional[str],
                     window: Optional[str], duration: Optional[str]) -> Optional[str]:
    def with_s(val: str) -> str:
        v = val.strip().lower()
        if not v: return v
        if not v.startswith(("+", "-")): v = "-" + v
        if v.lstrip("+-").isdigit(): v += "s"
        return v
    if since:
        s = since.strip().lower()
        if s.lstrip("+-").isdigit(): return with_s(s)
        return s
    for cand in (seconds, window, duration):
        if cand: return with_s(cand)
    if last is not None:
        try:
            val = int(last)
            if val > 0: val = -val
            return f"{val}s"
        except Exception:
            pass
    return None

def _parse_since(s: Optional[str]) -> Optional[datetime]:
    if not s: return None
    s = s.strip().lower()
    m = re.fullmatch(r"([+-]?\d+)([smhd])", s)
    if m:
        qty = int(m.group(1)); unit = m.group(2); now = datetime.now(timezone.utc)
        if unit == "s": return now + timedelta(seconds=qty)
        if unit == "m": return now + timedelta(minutes=qty)
        if unit == "h": return now + timedelta(hours=qty)
        if unit == "d": return now + timedelta(days=qty)
    try:
        dt = datetime.fromisoformat(s.replace("z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        raise HTTPException(400, detail="since inválido. Use '-60s', '-15m', '-1h', '-7d' ou ISO8601.")

def _coerce_mpu_id(id_: Optional[str], mpu: Optional[str], device: Optional[str], name: Optional[str]) -> int:
    cand = None
    for v in (id_, mpu, device, name):
        if v: cand = v; break
    if cand is None:
        raise HTTPException(400, "Informe id=MPUA1|MPUA2 (ou mpu|device|name)")
    s = str(cand).strip().upper()
    if s in ("1", "MPU1", "MPUA1", "A1"): return 1
    if s in ("2", "MPU2", "MPUA2", "A2"): return 2
    raise HTTPException(400, "MPU inválido (use 1,2, MPUA1, MPUA2)")

def _coerce_act_from_path(act_raw: str) -> int:
    s = (act_raw or "").strip().upper()
    if s in ("1", "A1", "MPUA1", "MPU1"): return 1
    if s in ("2", "A2", "MPUA2", "MPU2"): return 2
    raise HTTPException(400, "act inválido (use 1|2|A1|A2)")

def _coerce_facet_from_path(facet_raw: str) -> str:
    s = (facet_raw or "").strip().upper()
    if s not in ("S1", "S2"):
        raise HTTPException(400, "facet inválido (use S1|S2)")
    return s

# =========================
# SNAPSHOT SÍNCRONO EM MEMÓRIA
# =========================
import threading

class _SnapAct:
    __slots__ = ("id","state","pending","fault","elapsed_ms","started_at","v_av","v_rec","s_adv","s_rec","ts","adv_ms","rec_ms","cycle_ms")
    def __init__(self, id:str):
        self.id=id; self.state="RECUADO"; self.pending=None; self.fault="NONE"
        self.elapsed_ms=0; self.started_at=None
        self.v_av=0; self.v_rec=0; self.s_adv=0; self.s_rec=0
        self.adv_ms=None; self.rec_ms=None; self.cycle_ms=None
        self.ts=datetime.now(timezone.utc)

class _StateStore:
    def __init__(self):
        self._lock = threading.RLock()
        self.seq = 0
        self.ts = datetime.now(timezone.utc)
        self.act = {1:_SnapAct("A1"), 2:_SnapAct("A2")}
        self.mpu = None  # pode preencher depois

    def _bump(self):
        self.seq += 1
        self.ts = datetime.now(timezone.utc)

    def patch_act(self, aid:int, **fields):
        with self._lock:
            a = self.act[aid]
            for k,v in fields.items():
                setattr(a,k,v)
            a.ts = datetime.now(timezone.utc)
            self._bump()

    def get(self):
        with self._lock:
            def dump(a:_SnapAct):
                d = {k:getattr(a,k) for k in ("id","state","pending","fault","elapsed_ms","started_at","v_av","v_rec","s_adv","s_rec","adv_ms","rec_ms","cycle_ms")}
                d["last_update_ts"]=dt_to_iso_utc(a.ts)
                d["started_at"]= (a.started_at.isoformat() if a.started_at else None)
                return d
            return {
                "seq": self.seq,
                "ts": dt_to_iso_utc(self.ts),
                "actuators": {1: dump(self.act[1]), 2: dump(self.act[2])},
                "mpu": self.mpu,
            }

    def get_if_newer(self, since_seq: Optional[int]):
        with self._lock:
            if since_seq is None or self.seq > since_seq:
                return self.get()
            return None

STATE = _StateStore()

# =========================
# RUNTIME: debounce + FSM síncrona por atuador
# =========================
class _RuntimeAct:
    def __init__(self, cfg:_LatchCfg):
        self.cfg=cfg
        self.rec=_Deb(); self.adv=_Deb()
        self.vav=0; self.vrec=0
        self.state="RECUADO"
        self.pending=None      # "AV"|"REC"|None
        self.started_at=None   # datetime
        self.elapsed_ms=0
        self.adv_ms=None; self.rec_ms=None; self.cycle_ms=None

    def _elapsed(self, now:datetime):
        if not self.started_at: return 0
        return int((now - self.started_at).total_seconds()*1000)

    def ingest(self, name:str, val:int, ts:Optional[datetime]=None):
        now = ts or datetime.now(timezone.utc)
        # Atualiza sinais
        if name == self.cfg.s_rec: _stable(self.rec, bool(val), now, self.cfg.debounce_ms)
        if name == self.cfg.s_adv: _stable(self.adv, bool(val), now, self.cfg.debounce_ms)

        # pega estados estáveis
        rec_st, rec_val = _stable(self.rec, self.rec.v, now, self.cfg.debounce_ms)
        adv_st, adv_val = _stable(self.adv, self.adv.v, now, self.cfg.debounce_ms)

        # comandos (válvulas)
        if name == self.cfg.v_av:
            self.vav = int(bool(val))
            if self.vav==1:
                self.pending="AV"; self.started_at=now; self.elapsed_ms=0
        elif name == self.cfg.v_rec:
            self.vrec = int(bool(val))
            if self.vrec==1:
                self.pending="REC"; self.started_at=now; self.elapsed_ms=0

        # sem pending → estado reflete sensores
        if not self.pending:
            if rec_st and rec_val and not (adv_st and adv_val):
                self.state="RECUADO"
            elif adv_st and adv_val and not (rec_st and rec_val):
                self.state="AVANÇADO"

        # com pending → valida chegada, timeout e tempos
        if self.pending:
            self.elapsed_ms = self._elapsed(now)
            timeout_ms = int(self.cfg.expected_ms * self.cfg.timeout_factor)
            fault = "NONE"
            changed=False
            if self.pending=="AV":
                if adv_st and adv_val:
                    # concluiu avanço
                    self.state="AVANÇADO"; self.pending=None
                    self.adv_ms = self.elapsed_ms
                    changed=True
                elif self.elapsed_ms > timeout_ms:
                    fault="FAULT_TIMEOUT"
            elif self.pending=="REC":
                if rec_st and rec_val:
                    # concluiu recuo
                    self.state="RECUADO"; self.pending=None
                    self.rec_ms = self.elapsed_ms
                    changed=True
                elif self.elapsed_ms > timeout_ms:
                    fault="FAULT_TIMEOUT"
            # ciclo completo?
            if changed:
                if self.adv_ms is not None and self.rec_ms is not None:
                    self.cycle_ms = self.adv_ms + self.rec_ms
                    # zera para o próximo ciclo medido separadamente
                    self.adv_ms=None; self.rec_ms=None
            return {"fault": fault if fault!="NONE" else None}

        return {"fault": None}

# Instâncias por atuador
_RT = {
    1: _RuntimeAct(_CFG_A1),
    2: _RuntimeAct(_CFG_A2),
}

def _publish_runtime_to_snapshot(aid:int):
    rt = _RT[aid]
    STATE.patch_act(
        aid,
        id=rt.cfg.id,
        state=rt.state,
        pending=rt.pending,
        fault="NONE",  # pode trocar para refletir timeout visual
        elapsed_ms=rt.elapsed_ms,
        started_at=rt.started_at,
        v_av=rt.vav, v_rec=rt.vrec,
        s_adv=int(rt.adv.v), s_rec=int(rt.rec.v),
        adv_ms=rt.adv_ms, rec_ms=rt.rec_ms, cycle_ms=rt.cycle_ms,
    )

# -----------------------------------------------------------------------------
# HTTP básicos
# -----------------------------------------------------------------------------
@app.get("/health")
def health():
    row = fetch_one("SELECT NOW(6) AS now6")
    db_time_val = first_col(row) if row is not None else None
    return {"status": "ok", "db_time": (str(db_time_val) if db_time_val is not None else None)}

@app.get("/api/health")
def api_health():
    return health()

# -----------------------------------------------------------------------------
# OPC — latest / nomes / histórico (com compat)
# -----------------------------------------------------------------------------
@app.get("/opc/latest")
def opc_latest(name: str):
    row = fetch_one(
        """
        SELECT ts_utc AS ts_utc, name AS name, value_bool AS value_bool
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
    return {"ts_utc": dt_to_iso_utc(ts), "name": n, "value_bool": (None if vb is None else bool(vb))}

@app.get("/opc/names")
def opc_names():
    rows = fetch_all("SELECT DISTINCT name FROM opc_samples ORDER BY name ASC")
    return {"names": [first_col(r) for r in rows]}

@app.get("/opc/history")
def opc_history(
    request: Request,
    name: Optional[str] = Query(None),
    act: Optional[str] = Query(None, description="Atuador (1|2|A1|A2)"),
    facet: Optional[str] = Query(None, description="S1|S2"),
    last: Optional[str] = Query(None),
    since: Optional[str] = Query(None),
    seconds: Optional[str] = Query(None),
    window: Optional[str] = Query(None),
    duration: Optional[str] = Query(None),
    limit: int = Query(20000, ge=1, le=20000),
    offset: int = Query(0, ge=0),
    asc: bool = Query(False),
):
    act_int: Optional[int] = None
    facet_norm: Optional[str] = None

    if act is not None:
        act_int = _coerce_act_from_path(str(act))
    if facet is not None:
        facet_norm = _coerce_facet_from_path(str(facet))

    if not name and act_int and facet_norm:
        name = f"Recuado_{act_int}S1" if facet_norm == "S1" else f"Avancado_{act_int}S2"
    if not name:
        raise HTTPException(400, "Informe name=... ou act=..&facet=..")

    qp = request.query_params
    alt_since = qp.get("from") or qp.get("start") or qp.get("begin") or None
    alt_scalar = qp.get("period") or qp.get("seconds") or qp.get("duration") or None

    last_int: Optional[int] = None
    if last is not None:
        s = str(last).strip().lower()
        if s.endswith("s"): s = s[:-1]
        try: last_int = int(s)
        except Exception: last_int = None

    norm = _normalize_since(
        since=since or alt_since,
        last=last_int,
        seconds=seconds or alt_scalar,
        window=window,
        duration=duration,
    )
    dt = _parse_since(norm) if norm else None

    sql = """
        SELECT ts_utc AS ts_utc, value_bool AS value_bool
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
    items: List[Dict[str, Any]] = [{
        "ts_utc": dt_to_iso_utc(col(r, "ts_utc") if isinstance(r, dict) else r[0]),
        "value_bool": (None if (col(r, "value_bool") if isinstance(r, dict) else r[1]) is None
                       else int(col(r, "value_bool") if isinstance(r, dict) else r[1])),
    } for r in rows]

    return {"name": name, "count": len(items), "items": items}

@app.get("/opc/history/{act}/{facet}")
def opc_history_by_path(
    request: Request,
    act: str,
    facet: str,
    last: Optional[str] = Query(None),
    since: Optional[str] = Query(None),
    seconds: Optional[str] = Query(None),
    window: Optional[str] = Query(None),
    duration: Optional[str] = Query(None),
    limit: int = Query(20000, ge=1, le=20000),
    offset: int = Query(0, ge=0),
    asc: bool = Query(False),
):
    act_int = _coerce_act_from_path(act)
    facet_str = _coerce_facet_from_path(facet)
    return opc_history(
        request=request,
        name=None,
        act=act_int,
        facet=facet_str,
        last=last,
        since=since,
        seconds=seconds,
        window=window,
        duration=duration,
        limit=limit,
        offset=offset,
        asc=asc,
    )

@app.get("/api/opc/history")
def api_opc_history(**kwargs):
    return opc_history(**kwargs)

# -----------------------------------------------------------------------------
# MPU — latest / ids / histórico (com compat)
# -----------------------------------------------------------------------------
@app.get("/mpu/latest")
def mpu_latest(id: str):
    mid = _coerce_mpu_id(id, None, None, None)
    row = fetch_one(
        """
        SELECT ts_utc AS ts_utc, mpu_id AS mpu_id,
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

@app.get("/mpu/ids")
def mpu_ids():
    rows = fetch_all("SELECT DISTINCT mpu_id AS mid FROM mpu_samples ORDER BY mpu_id ASC")
    def id_to_str(i: int) -> str:
        return "MPUA1" if i == 1 else "MPUA2" if i == 2 else f"MPU{i}"
    ids = []
    for r in rows:
        mid = col(r, "mid")
        if mid is None: mid = col(r, 0)
        ids.append(id_to_str(int(mid)))
    return {"ids": ids}

@app.get("/api/mpu/ids")
def api_mpu_ids():
    return mpu_ids()

@app.get("/mpu/history")
def mpu_history(
    id: Optional[str] = Query(None, description="MPUA1|MPUA2|1|2"),
    mpu: Optional[str] = Query(None),
    device: Optional[str] = Query(None),
    name: Optional[str] = Query(None),
    since: Optional[str] = Query(None),
    last: Optional[int] = Query(None),
    seconds: Optional[str] = Query(None),
    window: Optional[str] = Query(None),
    duration: Optional[str] = Query(None),
    limit: int = Query(1000, ge=1, le=20000),
    offset: int = Query(0, ge=0),
    asc: bool = Query(False),
):
    mid = _coerce_mpu_id(id, mpu, device, name)
    norm = _normalize_since(since, last, seconds, window, duration)
    dt = _parse_since(norm) if norm else None

    sql = """
        SELECT ts_utc AS ts_utc, ax_g, ay_g, az_g, gx_dps, gy_dps, gz_dps
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
        items.append({
            "ts_utc": dt_to_iso_utc(r["ts_utc"]),
            "ax_g": r["ax_g"], "ay_g": r["ay_g"], "az_g": r["az_g"],
            "gx_dps": r["gx_dps"], "gy_dps": r["gy_dps"], "gz_dps": r["gz_dps"],
        })
    return {"id": ("MPUA1" if mid == 1 else "MPUA2"), "count": len(items), "items": items}

@app.get("/api/mpu/history")
def api_mpu_history(**kwargs):
    return mpu_history(**kwargs)

# -----------------------------------------------------------------------------
# LIVE DASHBOARD
# -----------------------------------------------------------------------------
_SENSORS = {
    1: {"recuado": "Recuado_1S1", "avancado": "Avancado_1S2"},
    2: {"recuado": "Recuado_2S1", "avancado": "Avancado_2S2"},
}
_INICIA = "INICIA"
_PARA = "PARA"

def _clean_rms(vals: List[float]) -> float:
    if not vals: return 0.0
    s = sum(v*v for v in vals) / float(len(vals))
    return float(s ** 0.5)

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

# -------- estado dos atuadores (função interna + rota) --------
def _live_actuators_state_data(since_ms: int = 12000) -> Dict[str, Any]:
    rows = fetch_all(
        """
        SELECT ts_utc, name, value_bool
        FROM opc_samples
        WHERE name IN (%s,%s,%s,%s,%s,%s,%s,%s)
          AND ts_utc >= NOW(6) - INTERVAL %s MICROSECOND
        ORDER BY ts_utc ASC, id ASC
        """,
        (*_NAMES_LATCH, since_ms * 1000)
    )

    def norm_row(r):
        if isinstance(r, dict): return (r["ts_utc"], r["name"], r["value_bool"])
        return (r[0], r[1], r[2])

    a1_rows = [norm_row(r) for r in rows if ((r["name"] if isinstance(r,dict) else r[1]) in (_CFG_A1.v_av, _CFG_A1.v_rec, _CFG_A1.s_adv, _CFG_A1.s_rec))]
    a2_rows = [norm_row(r) for r in rows if ((r["name"] if isinstance(r,dict) else r[1]) in (_CFG_A2.v_av, _CFG_A2.v_rec, _CFG_A2.s_adv, _CFG_A2.s_rec))]

    st1 = _compute_latched_state(_CFG_A1, a1_rows, initial_displayed="RECUADO")
    st2 = _compute_latched_state(_CFG_A2, a2_rows, initial_displayed="RECUADO")

    return {
        "ts": _now_iso(),
        "actuators": [
            {
                "actuator_id": 1,
                "state": st1.displayed,
                "pending": st1.pending_target,
                "fault": st1.fault,
                "elapsed_ms": st1.elapsed_ms,
                "started_at": (st1.started_at.isoformat() if st1.started_at else None),
            },
            {
                "actuator_id": 2,
                "state": st2.displayed,
                "pending": st2.pending_target,
                "fault": st2.fault,
                "elapsed_ms": st2.elapsed_ms,
                "started_at": (st2.started_at.isoformat() if st2.started_at else None),
            },
        ]
    }

@app.get("/api/live/actuators/state", tags=["Live Dashboard"])
def live_actuators_state(since_ms: int = 12000):
    try:
        # 1) Fast: último valor por nome (sem janela)
        data = _live_actuators_state_data_fast()
        return JSONResponse(content=data, headers={"Cache-Control": "no-store", "Pragma": "no-cache"})
    except Exception as e_fast:
        # 2) Snapshot (se você estiver alimentando STATE via /opc/ingest)
        try:
            snap = globals().get("STATE", None)
            if snap:
                s = snap.get()
                a1 = s["actuators"].get(1, {})
                a2 = s["actuators"].get(2, {})
                has_data = any([
                    a1.get("v_av") or a1.get("v_rec") or a1.get("s_adv") or a1.get("s_rec"),
                    a2.get("v_av") or a2.get("v_rec") or a2.get("s_adv") or a2.get("s_rec"),
                ])
                if has_data:
                    return JSONResponse(content={
                        "ts": s["ts"],
                        "actuators": [
                            {"actuator_id": 1, "state": a1.get("state"), "pending": a1.get("pending"),
                             "fault": a1.get("fault"), "elapsed_ms": a1.get("elapsed_ms"),
                             "started_at": a1.get("started_at")},
                            {"actuator_id": 2, "state": a2.get("state"), "pending": a2.get("pending"),
                             "fault": a2.get("fault"), "elapsed_ms": a2.get("elapsed_ms"),
                             "started_at": a2.get("started_at")},
                        ]
                    }, headers={"Cache-Control": "no-store", "Pragma": "no-cache"})
        except Exception:
            pass

        # 3) Fallback antigo com janela
        try:
            data = _live_actuators_state_data(since_ms)
            return JSONResponse(content=data, headers={"Cache-Control": "no-store", "Pragma": "no-cache"})
        except Exception as e_hist:
            return JSONResponse(
                status_code=500,
                content={"detail": "internal_error", "fast_error": str(e_fast)[:400], "hist_error": str(e_hist)[:400]},
                headers={"Cache-Control": "no-store"}
            )

# -------- trace para diagnóstico --------
def _compute_latched_state_with_trace(
    cfg: _LatchCfg,
    rows: List[Tuple[datetime,str,Optional[int]]],
    initial_displayed: StableState = "RECUADO",
):
    st = _LatchState(displayed=initial_displayed)
    rec = _Deb(); adv = _Deb()
    trace = []
    for ts, name, vb in rows:
        if vb is None: continue
        v = bool(vb)
        if name == cfg.s_rec: _stable(rec, v, ts, cfg.debounce_ms)
        if name == cfg.s_adv: _stable(adv, v, ts, cfg.debounce_ms)
        rec_st, rec_val = _stable(rec, rec.v, ts, cfg.debounce_ms)
        adv_st, adv_val = _stable(adv, adv.v, ts, cfg.debounce_ms)

        reason = []
        if rec_st and adv_st and rec_val and adv_val:
            st.fault = "FAULT_SENSORS_CONFLICT"; reason.append("conflict")
        if name == cfg.v_av and v:
            st.pending_target = "AV"; st.started_at = ts; st.fault = "NONE"; reason.append("cmd:AV")
        elif name == cfg.v_rec and v:
            st.pending_target = "REC"; st.started_at = ts; st.fault = "NONE"; reason.append("cmd:REC")

        if not st.pending_target:
            if rec_st and rec_val and not (adv_st and adv_val):
                st.displayed = "RECUADO"; reason.append("no-pending:REC")
            elif adv_st and adv_val and not (rec_st and rec_val):
                st.displayed = "AVANÇADO"; reason.append("no-pending:AV")

        if st.pending_target:
            if st.started_at:
                st.elapsed_ms = int((ts - st.started_at).total_seconds()*1000)
                if st.elapsed_ms > int(cfg.expected_ms * cfg.timeout_factor):
                    st.fault = "FAULT_TIMEOUT"; reason.append("timeout")
            if st.pending_target == "AV" and adv_st and adv_val:
                st.displayed = "AVANÇADO"; st.pending_target=None; st.started_at=None; st.elapsed_ms=0; st.fault="NONE"; reason.append("confirm:AV")
            elif st.pending_target == "REC" and rec_st and rec_val:
                st.displayed = "RECUADO";  st.pending_target=None; st.started_at=None; st.elapsed_ms=0; st.fault="NONE"; reason.append("confirm:REC")

        trace.append({
            "ts": dt_to_iso_utc(ts), "name": name, "v": int(v),
            "rec_stable": int(rec_st), "rec_val": int(bool(rec_val)) if rec_val is not None else None,
            "adv_stable": int(adv_st), "adv_val": int(bool(adv_val)) if adv_val is not None else None,
            "displayed": st.displayed, "pending": st.pending_target, "fault": st.fault,
            "elapsed_ms": st.elapsed_ms, "reason": ",".join(reason) if reason else "-",
        })
    return st, trace

def _infer_last_stable(cfg: _LatchCfg, lookback_ms: int = 60000) -> StableState:
    rows = fetch_all(
        """
        SELECT name, value_bool, ts_utc
        FROM opc_samples
        WHERE name IN (%s,%s)
          AND ts_utc >= NOW(6) - INTERVAL %s MICROSECOND
        ORDER BY ts_utc DESC
        LIMIT 50
        """,
        (cfg.s_rec, cfg.s_adv, lookback_ms * 1000)
    )
    last_rec = next((r for r in rows if (col(r,"name")==cfg.s_rec and col(r,"value_bool")==1)), None)
    last_adv = next((r for r in rows if (col(r,"name")==cfg.s_adv and col(r,"value_bool")==1)), None)
    if last_adv and not last_rec: return "AVANÇADO"
    if last_rec and not last_adv: return "RECUADO"
    if last_rec and last_adv:
        tr = _coerce_to_datetime(col(last_rec,"ts_utc"))
        ta = _coerce_to_datetime(col(last_adv,"ts_utc"))
        return "RECUADO" if (tr and ta and tr > ta) else "AVANÇADO"
    return "RECUADO"

@app.get("/api/live/actuators/trace", tags=["Live Dashboard"])
def live_actuators_trace(
    act: int = Query(1, ge=1, le=2),
    since_ms: int = Query(15000, ge=1000, le=60000)
):
    cfg = _CFG_A1 if act == 1 else _CFG_A2
    rows = fetch_all(
        """
        SELECT ts_utc, name, value_bool
        FROM opc_samples
        WHERE name IN (%s,%s,%s,%s)
          AND ts_utc >= NOW(6) - INTERVAL %s MICROSECOND
        ORDER BY ts_utc ASC, id ASC
        """,
        (cfg.v_av, cfg.v_rec, cfg.s_adv, cfg.s_rec, since_ms * 1000)
    )
    def norm_row(r):
        return (col(r, "ts_utc") or r[0], col(r, "name") or r[1], col(r, "value_bool") or r[2])
    nrows = [norm_row(r) for r in rows]
    init_state = _infer_last_stable(cfg, 60000)
    st, tr = _compute_latched_state_with_trace(cfg, nrows, initial_displayed=init_state)

    return JSONResponse(content={
        "actuator": act,
        "cfg": {"expected_ms": cfg.expected_ms, "debounce_ms": cfg.debounce_ms, "timeout_factor": cfg.timeout_factor},
        "init": init_state,
        "final": {
            "state": st.displayed, "pending": st.pending_target, "fault": st.fault,
            "elapsed_ms": st.elapsed_ms, "started_at": (st.started_at.isoformat() if st.started_at else None)
        },
        "events": [
            {"ts": dt_to_iso_utc(ts), "name": name, "v": (None if vb is None else int(bool(vb)))}
            for (ts, name, vb) in nrows
        ],
        "trace": tr
    }, headers={"Cache-Control": "no-store"})

# -------- ciclos: FSM em memória --------
_cycle_fsm: Dict[int, Dict[str, Any]] = {
    1: {"baseline": None, "last": None, "seen_other": False, "count": 0, "since_ts": time.time()},
    2: {"baseline": None, "last": None, "seen_other": False, "count": 0, "since_ts": time.time()},
}

def _normalize_state(s: Any) -> Optional[str]:
    if not s: return None
    u = str(s).upper()
    if "RECU" in u: return "RECUADO"
    if "AVAN" in u: return "AVANÇADO"
    return None

def _update_cycle_fsm_from_state(aid: int, state: Optional[str]) -> None:
    if aid not in _cycle_fsm: return
    f = _cycle_fsm[aid]
    if state not in ("RECUADO", "AVANÇADO"): 
        f["last"] = state
        return
    if f["baseline"] is None:
        f["baseline"] = state
        f["last"] = state
        f["seen_other"] = False
        f["since_ts"] = f.get("since_ts", time.time())
        return
    last = f["last"]
    if state != last and state != f["baseline"]:
        f["seen_other"] = True
    if state == f["baseline"] and f.get("seen_other"):
        f["count"] = int(f.get("count", 0)) + 1
        f["seen_other"] = False
    f["last"] = state

@app.get("/api/live/cycles/total", tags=["Live Dashboard"])
def live_cycles_total():
    def _impl():
        states = _live_actuators_state_data().get("actuators", [])
        for it in states:
            aid = int(it.get("actuator_id") or it.get("id"))
            s = _normalize_state(it.get("state"))
            if aid in (1, 2): _update_cycle_fsm_from_state(aid, s)

        out = []; total = 0
        for aid in (1, 2):
            f = _cycle_fsm[aid]; c = int(f.get("count", 0)); total += c
            out.append({
                "actuator_id": aid,
                "cycles": c,
                "baseline": f.get("baseline"),
                "last_state": f.get("last"),
                "seen_other": f.get("seen_other"),
                "since": dt_to_iso_utc(datetime.fromtimestamp(f.get("since_ts", time.time()), tz=timezone.utc)),
            })
        return {"total": total, "actuators": out, "ts": _now_iso()}
    return cached("live_cycles_total", 0.05, _impl)

@app.get("/api/live/cycles/rate", tags=["Live Dashboard"])
def live_cycles_rate(window_s: int = Query(5, ge=1, le=300)):
    def _impl():
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
        c_inicia = 0; c_para = 0
        for r in rows:
            nm, cnt = cols(r, "name", "cnt")
            if nm == _INICIA: c_inicia = int(cnt)
            elif nm == _PARA: c_para = int(cnt)
        pairs = min(c_inicia, c_para)
        cycles = pairs // 2
        cps = cycles / float(window_s)
        return {"window_seconds": window_s, "pairs_count": pairs, "cycles": cycles, "cycles_per_second": cps}
    return cached(f"live_cycles_rate:{window_s}", 0.5, _impl)

@app.get("/api/live/vibration", tags=["Live Dashboard"])
def live_vibration(window_s: int = Query(2, ge=1, le=60)):
    def _impl():
        now = datetime.now(timezone.utc)
        start = now - timedelta(seconds=window_s)
        rows = fetch_all(
            """
            SELECT mpu_id AS mpu_id, ts_utc AS ts_utc, ax_g, ay_g, az_g
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
            rms_ax = _clean_rms(d["ax"]); rms_ay = _clean_rms(d["ay"]); rms_az = _clean_rms(d["az"])
            overall = float((rms_ax**2 + rms_ay**2 + rms_az**2) ** 0.5)
            items.append({
                "mpu_id": mpu_id,
                "ts_start": dt_to_iso_utc(min(d["ts"])),
                "ts_end": dt_to_iso_utc(max(d["ts"])),
                "rms_ax": rms_ax, "rms_ay": rms_ay, "rms_az": rms_az,
                "overall": overall,
            })
        return {"items": items}
    return cached(f"live_vibration:{window_s}", 0.05, _impl)

# -----------------------------------------------------------------------------
# Monitoring e System status
# -----------------------------------------------------------------------------
@app.get("/api/live/runtime", tags=["Live Dashboard"])
def live_runtime():
    row_i = fetch_one("SELECT MAX(ts_utc) AS ts FROM opc_samples WHERE name=%s", ("INICIA",))
    row_p = fetch_one("SELECT MAX(ts_utc) AS ts FROM opc_samples WHERE name=%s", ("PARA",))
    ts_i = first_col(row_i); ts_p = first_col(row_p)
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
    DB_NAME = os.getenv("MYSQL_DB") or os.getenv("DB_NAME") or ""
    def last_from_table(tname: str):
        if not DB_NAME or not table_exists(DB_NAME, tname):
            return None
        row = fetch_one(
            f"""
            SELECT ts_utc AS ts_utc, dt_abre_s AS dt_abre_s, dt_fecha_s AS dt_fecha_s, dt_ciclo_s AS dt_ciclo_s
            FROM {tname}
            ORDER BY ts_utc DESC
            LIMIT 1
            """
        )
        if not row: return None
        return {
            "ts_utc": dt_to_iso_utc(col(row, "ts_utc") or col(row, 0)),
            "dt_abre_s": col(row, "dt_abre_s"),
            "dt_fecha_s": col(row, "dt_fecha_s"),
            "dt_ciclo_s": col(row, "dt_ciclo_s"),
        }
    out = []
    r1 = last_from_table("cycles_atuador1")
    out.append({"actuator_id": 1, "last": (r1 or {"ts_utc": None, "dt_abre_s": None, "dt_fecha_s": None, "dt_ciclo_s": None})})
    r2 = last_from_table("cycles_atuador2")
    out.append({"actuator_id": 2, "last": (r2 or {"ts_utc": None, "dt_abre_s": None, "dt_fecha_s": None, "dt_ciclo_s": None})})
    return {"actuators": out}

def _sev_from_ok(ok: bool) -> str:
    return "operational" if ok else "down"

def get_system_status() -> dict:
    try:
        row = fetch_one("SELECT NOW(6) AS now6")
        db_ok = (row is not None and first_col(row) is not None)
    except Exception:
        db_ok = False
    control_sev = _sev_from_ok(db_ok)

    try:
        states = _live_actuators_state_data().get("actuators", [])
        any_valid = any((it.get("state") in ("RECUADO", "AVANÇADO")) for it in states)
        actuators_sev = "operational" if any_valid else "unknown"
    except Exception:
        actuators_sev = "unknown"

    try:
        names = opc_names().get("names", [])
        sensors_sev = "operational" if len(names) > 0 else "unknown"
    except Exception:
        sensors_sev = "unknown"

    try:
        now = datetime.now(timezone.utc)
        rows = fetch_all(
            """
            SELECT COUNT(*) AS cnt
            FROM opc_samples
            WHERE ts_utc >= %s AND name IN (%s, %s)
            """,
            (now - timedelta(seconds=10), "INICIA", "PARA"),
        )
        cnt = int(first_col(rows[0]) if rows else 0)
        transmission_sev = "operational" if cnt > 0 else "warning"
    except Exception:
        transmission_sev = "unknown"

    return {"components": {
        "actuators": actuators_sev, "sensors": sensors_sev,
        "transmission": transmission_sev, "control": control_sev,
    }}

@app.get("/api/system/status", tags=["Live Dashboard"])
def system_status_http():
    return get_system_status()

@app.get("/system/status")
def system_status_plain():
    return get_system_status()

# -----------------------------------------------------------------------------
# Outras rotas HTTP (equivalentes aos antigos WS)
# -----------------------------------------------------------------------------
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
        result = get_kpis()
    except Exception:
        result = {}
    return {"type": "analytics", **result}

@app.get("/api/cycles")
def http_cycles():
    try:
        result = {"cpm": get_cycle_rate(window_minutes=1)}
    except Exception:
        result = {}
    return {"type": "cycles", **result}

@app.get("/api/vibration/window")
def http_vibration_window(seconds: float = Query(36.0, ge=1.0, le=300.0)):
    try:
        hours = seconds / 3600.0
        result = get_vibration_data(hours=hours)
    except Exception:
        result = {}
    return {"type": "vibration", **result}

@app.get("/api/snapshot")
def http_snapshot():
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

# =========================
# ENDPOINTS SÍNCRONOS
# =========================
@app.post("/opc/ingest", tags=["Live Sync"])
def opc_ingest(name: str, value: int = Query(..., ge=0, le=1), ts_utc: Optional[str] = Query(None)):
    """
    Síncrono: aplica o evento no runtime, publica snapshot e só então responde.
    """
    if name not in _NAMES_LATCH:
        # você pode optar por aceitar outros sinais também; por agora, foca nos de latch
        raise HTTPException(400, "sinal não monitorado para latch")
    ts = _coerce_to_datetime(ts_utc) if ts_utc else datetime.now(timezone.utc)

    # roteia por atuador
    aid = 1 if name in (_CFG_A1.v_av, _CFG_A1.v_rec, _CFG_A1.s_adv, _CFG_A1.s_rec) else 2
    _RT[aid].ingest(name, int(bool(value)), ts)
    _publish_runtime_to_snapshot(aid)

    # resposta já contém o snapshot atual (site == ao vivo)
    return {"ok": True, "snapshot": STATE.get()}

@app.get("/live/snapshot", tags=["Live Sync"])
def live_snapshot(since_seq: Optional[int] = Query(None)):
    snap = STATE.get_if_newer(since_seq)
    if not snap:
        return {"changed": False, "seq": since_seq}
    return {"changed": True, **snap}

def _live_actuators_state_data_fast() -> Dict[str, Any]:
    names = (
        _CFG_A1.v_av, _CFG_A1.v_rec, _CFG_A1.s_adv, _CFG_A1.s_rec,
        _CFG_A2.v_av, _CFG_A2.v_rec, _CFG_A2.s_adv, _CFG_A2.s_rec
    )
    latest = _fetch_latest_rows(names)

    a1 = _infer_state_from_latest(_CFG_A1, latest)
    a2 = _infer_state_from_latest(_CFG_A2, latest)

    return {
        "ts": _now_iso(),
        "actuators": [
            {"actuator_id": 1, **{k: v for k, v in a1.items() if not k.startswith("_")}},
            {"actuator_id": 2, **{k: v for k, v in a2.items() if not k.startswith("_")}},
        ]
    }
