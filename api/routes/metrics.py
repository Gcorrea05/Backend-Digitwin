# api/routes/metrics.py
from fastapi import APIRouter, HTTPException, Query
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple

# usa diretamente seu helper real
from ..database import fetch_all

router = APIRouter(prefix="/metrics", tags=["metrics"])

# === MAPEAMENTO DE SINAIS POR ATUADOR (ajuste se seus nomes diferirem) ===
SIGNALS = {
    "A1": {"S1": "Recuado_1S1", "S2": "Avancado_1S2", "V_AVANCO": "VAvanco_A1", "V_RECUO": "VRecuo_A1"},
    "A2": {"S1": "Recuado_2S1", "S2": "Avancado_2S2", "V_AVANCO": "VAvanco_A2", "V_RECUO": "VRecuo_A2"},
}

# ---------- helpers de normalização ----------
def _norm_act(act: Optional[str], id_: Optional[int], actuator: Optional[int]) -> str:
    """
    Aceita act=A1|A2|1|2 ou id/actuator=1|2 e normaliza para 'A1'/'A2'.
    """
    if act:
        s = str(act).strip().upper()
        if s in ("A1", "1"): return "A1"
        if s in ("A2", "2"): return "A2"
    if id_ is not None:
        return "A1" if int(id_) == 1 else "A2"
    if actuator is not None:
        return "A1" if int(actuator) == 1 else "A2"
    raise HTTPException(400, "parâmetro do atuador ausente. Use act=A1|A2 ou id=1|2 ou actuator=1|2")

def _parse_since(since: str) -> timedelta:
    """
    Aceita: -5m, -2h, -1d, -7200s
    """
    if not since or not since.startswith("-") or len(since) < 3:
        raise HTTPException(400, "since inválido (use -5m, -2h, -1d, -7200s).")
    unit = since[-1].lower()
    try:
        val = int(since[1:-1])
    except Exception:
        raise HTTPException(400, "since inválido (use -5m, -2h, -1d, -7200s).")
    if unit == "s": return timedelta(seconds=val)
    if unit == "m": return timedelta(minutes=val)
    if unit == "h": return timedelta(hours=val)
    if unit == "d": return timedelta(days=val)
    raise HTTPException(400, "since inválido (use -5m, -2h, -1d, -7200s).")

def _ts_minute(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0, tzinfo=timezone.utc)

# ---------- queries ----------
def _fetch_signal(name: str, since_dt: datetime, limit: int = 200000) -> List[Dict[str, Any]]:
    sql = """
      SELECT ts_utc, COALESCE(value_bool, value) AS v
      FROM opc_samples
      WHERE name=%s AND ts_utc >= %s
      ORDER BY ts_utc ASC
      LIMIT %s
    """
    rows = fetch_all(sql, (name, since_dt, limit))
    out: List[Dict[str, Any]] = []
    for r in rows:
        v = r["v"]
        if v is None:
            continue
        if isinstance(v, (int, float)):
            vv = 1 if v else 0
        elif isinstance(v, (bytes, str)):
            vv = 1 if str(v).lower() in ("1", "true", "t") else 0
        else:
            vv = 0
        out.append({"ts": r["ts_utc"], "v": vv})
    return out

def _edges(series: Optional[List[Dict[str, Any]]]) -> List[Tuple[datetime, int, int]]:
    out: List[Tuple[datetime, int, int]] = []
    if not series:
        return out
    prev = series[0]["v"]
    for i in range(1, len(series)):
        curr = series[i]["v"]
        if curr != prev:
            out.append((series[i]["ts"], prev, curr))
            prev = curr
    return out

def _reconstruct_cycles(
    s1: List[Dict[str, Any]],
    s2: List[Dict[str, Any]],
    v_open: Optional[List[Dict[str, Any]]] = None,
    v_close: Optional[List[Dict[str, Any]]] = None
) -> List[Dict[str, Any]]:
    e_s1 = _edges(s1)
    e_s2 = _edges(s2)
    e_vo = _edges(v_open)
    e_vc = _edges(v_close)

    events: List[Tuple[datetime, str]] = []
    for ts, a, b in e_s1:
        if (a, b) == (1, 0): events.append((ts, "OPEN_START"))
        if (a, b) == (0, 1): events.append((ts, "CLOSE_END"))
    for ts, a, b in e_s2:
        if (a, b) == (0, 1): events.append((ts, "OPEN_END"))
        if (a, b) == (1, 0): events.append((ts, "CLOSE_START"))
    for ts, a, b in e_vo:
        if (a, b) == (0, 1): events.append((ts, "OPEN_START_CMD"))
    for ts, a, b in e_vc:
        if (a, b) == (0, 1): events.append((ts, "CLOSE_START_CMD"))

    events.sort(key=lambda x: x[0])

    cycles: List[Dict[str, Any]] = []
    open_start: Optional[datetime] = None
    close_start: Optional[datetime] = None
    cycle_start: Optional[datetime] = None

    for ts, kind in events:
        if kind in ("OPEN_START", "OPEN_START_CMD"):
            open_start = ts
            if cycle_start is None:
                cycle_start = ts
        elif kind == "OPEN_END" and open_start:
            t_open = (ts - open_start).total_seconds() * 1000.0
            cycles.append({
                "open_start": open_start, "open_end": ts, "t_open_ms": t_open,
                "close_start": None, "close_end": None, "t_close_ms": None,
                "ts_start": cycle_start, "ts_end": None
            })
            open_start = None
        elif kind in ("CLOSE_START", "CLOSE_START_CMD"):
            close_start = ts
            if cycle_start is None:
                cycle_start = ts
        elif kind == "CLOSE_END" and close_start:
            t_close = (ts - close_start).total_seconds() * 1000.0
            target: Optional[Dict[str, Any]] = None
            for c in reversed(cycles):
                if c["close_end"] is None:
                    target = c
                    break
            if target is None:
                target = {
                    "open_start": None, "open_end": None, "t_open_ms": None,
                    "close_start": None, "close_end": None, "t_close_ms": None,
                    "ts_start": cycle_start, "ts_end": None
                }
                cycles.append(target)
            target["close_start"] = close_start
            target["close_end"] = ts
            target["t_close_ms"] = t_close
            target["ts_end"] = ts
            if target["ts_start"] and target["ts_end"]:
                target["t_cycle_ms"] = (target["ts_end"] - target["ts_start"]).total_seconds() * 1000.0
            close_start = None
            cycle_start = None

    return [
        c for c in cycles
        if c.get("t_open_ms") is not None and c.get("t_close_ms") is not None and c.get("t_cycle_ms") is not None
    ]

def _group_minute(cycles: List[Dict[str, Any]]) -> Dict[datetime, Dict[str, Any]]:
    agg: Dict[datetime, Dict[str, Any]] = {}
    for c in cycles:
        m = c["ts_start"].replace(second=0, microsecond=0, tzinfo=timezone.utc)
        a = agg.setdefault(m, {
            "t_open_ms_sum": 0.0, "t_open_n": 0,
            "t_close_ms_sum": 0.0, "t_close_n": 0,
            "t_cycle_ms_sum": 0.0, "t_cycle_n": 0,
            "runtime_s": 0.0, "cpm": 0
        })
        a["t_open_ms_sum"] += c["t_open_ms"]; a["t_open_n"] += 1
        a["t_close_ms_sum"] += c["t_close_ms"]; a["t_close_n"] += 1
        a["t_cycle_ms_sum"] += c["t_cycle_ms"]; a["t_cycle_n"] += 1
        a["runtime_s"] += (c["t_open_ms"] + c["t_close_ms"]) / 1000.0
        a["cpm"] += 1
    return agg

def _fetch_mpu_minute_avg(act: str, since_dt: datetime) -> Dict[datetime, float]:
    """
    Busca histórico MPU e devolve média por minuto (ex.: 'az').
    Compatível com esquema que usa 'mpu_id' OU 'id' na tabela mpu_samples.
    """
    mpu = f"MPU{act}"  # ex.: A1 -> MPUA1
    sql = """
      SELECT ts_utc, az
      FROM mpu_samples
      WHERE (mpu_id = %s OR id = %s) AND ts_utc >= %s
      ORDER BY ts_utc ASC
      LIMIT 200000
    """
    rows = fetch_all(sql, (mpu, mpu, since_dt))
    out: Dict[datetime, Dict[str, float]] = {}
    for r in rows:
        ts = r["ts_utc"].replace(second=0, microsecond=0, tzinfo=timezone.utc)
        d = out.setdefault(ts, {"sum": 0.0, "n": 0})
        val = float(r.get("az") or 0.0)
        d["sum"] += val
        d["n"] += 1
    return {k: (v["sum"] / v["n"]) for k, v in out.items() if v["n"] > 0}

def _compute_minute_agg(act: str, since: str) -> List[Dict[str, Any]]:
    if act not in SIGNALS:
        raise HTTPException(400, "act inválido (A1|A2).")
    since_dt = datetime.now(timezone.utc) - _parse_since(since)

    s1 = _fetch_signal(SIGNALS[act]["S1"], since_dt)
    s2 = _fetch_signal(SIGNALS[act]["S2"], since_dt)
    cycles = _reconstruct_cycles(s1, s2)

    agg = _group_minute(cycles)
    vib = _fetch_mpu_minute_avg(act, since_dt)

    keys = sorted(set(agg.keys()) | set(vib.keys()))
    out: List[Dict[str, Any]] = []
    for k in keys:
        a = agg.get(k, {})
        v_avg = vib.get(k)
        out.append({
            "minute": k.isoformat(),
            "t_open_ms_avg": (a.get("t_open_ms_sum", 0.0) / a.get("t_open_n", 1)) if a else None,
            "t_close_ms_avg": (a.get("t_close_ms_sum", 0.0) / a.get("t_close_n", 1)) if a else None,
            "t_cycle_ms_avg": (a.get("t_cycle_ms_sum", 0.0) / a.get("t_cycle_n", 1)) if a else None,
            "runtime_s": a.get("runtime_s", 0.0) if a else 0.0,
            "cpm": a.get("cpm", 0) if a else 0,
            "vib_avg": v_avg,
        })
    return out

# ------------------ endpoints ------------------

@router.get("/cycles")
def get_cycles(
    act: Optional[str] = Query(None),
    id: Optional[int] = Query(None),
    actuator: Optional[int] = Query(None),
    since: str = Query("-2h"),
    limit: int = 1000
):
    act_norm = _norm_act(act, id, actuator)
    since_dt = datetime.now(timezone.utc) - _parse_since(since)

    s1 = _fetch_signal(SIGNALS[act_norm]["S1"], since_dt)
    s2 = _fetch_signal(SIGNALS[act_norm]["S2"], since_dt)

    try:
        v_open = _fetch_signal(SIGNALS[act_norm]["V_AVANCO"], since_dt)
    except Exception:
        v_open = None
    try:
        v_close = _fetch_signal(SIGNALS[act_norm]["V_RECUO"], since_dt)
    except Exception:
        v_close = None

    cycles = _reconstruct_cycles(s1, s2, v_open, v_close)
    return cycles[-limit:]

@router.get("/minute-agg")
def get_minute_agg(
    act: Optional[str] = Query(None),
    id: Optional[int] = Query(None),
    actuator: Optional[int] = Query(None),
    since: str = Query("-2h")
):
    act_norm = _norm_act(act, id, actuator)
    return _compute_minute_agg(act=act_norm, since=since)

@router.get("/cpm-runtime-minute")
def get_cpm_runtime_minute(
    act: Optional[str] = Query(None),
    id: Optional[int] = Query(None),
    actuator: Optional[int] = Query(None),
    since: str = Query("-2h")
) -> Dict[str, List[Dict[str, Any]]]:
    act_norm = _norm_act(act, id, actuator)
    rows = _compute_minute_agg(act=act_norm, since=since)
    out = [{"minute": r["minute"], "cpm": r.get("cpm"), "runtime_s": r.get("runtime_s")} for r in rows]
    out.sort(key=lambda x: x["minute"])
    return {"data": out}
