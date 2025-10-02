# routes/metrics.py
from fastapi import APIRouter, HTTPException, Query
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple
import itertools

# Você já deve ter helpers de DB; aqui eu presumo funções fetch_all/fetch_one:
from .utils_db import fetch_all  # adapte para o seu helper real (ex.: from api.api_ws import fetch_all)

router = APIRouter(prefix="/metrics", tags=["metrics"])

# === MAPEAMENTO DE SINAIS POR ATUADOR (ajuste se seus nomes diferirem) ===
SIGNALS = {
    "A1": {"S1": "Recuado_1S1", "S2": "Avancado_1S2", "V_AVANCO": "VAvanco_A1", "V_RECUO": "VRecuo_A1"},
    "A2": {"S1": "Recuado_2S1", "S2": "Avancado_2S2", "V_AVANCO": "VAvanco_A2", "V_RECUO": "VRecuo_A2"},
}

def _parse_since(since: str) -> timedelta:
    # aceita "-5m", "-2h", "-1d"
    unit = since[-1]
    val = int(since[1:-1])
    if unit == "m": return timedelta(minutes=val)
    if unit == "h": return timedelta(hours=val)
    if unit == "d": return timedelta(days=val)
    raise ValueError("since inválido (use -5m, -2h, -1d).")

def _ts_minute(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0, tzinfo=timezone.utc)

def _fetch_signal(name: str, since_dt: datetime, limit: int = 200000) -> List[Dict[str, Any]]:
    sql = """
      SELECT ts_utc, COALESCE(value_bool, value) AS v
      FROM opc_samples
      WHERE name=%s AND ts_utc >= %s
      ORDER BY ts_utc ASC
      LIMIT %s
    """
    rows = fetch_all(sql, (name, since_dt, limit))
    # normaliza v -> 0/1 se boolean-like
    out = []
    for r in rows:
        v = r["v"]
        if v is None: continue
        if isinstance(v, (int, float)): vv = 1 if v else 0
        elif isinstance(v, (bytes, str)): vv = 1 if str(v).lower() in ("1", "true", "t") else 0
        else: vv = 0
        out.append({"ts": r["ts_utc"], "v": vv})
    return out

def _edges(series: List[Dict[str, Any]]) -> List[Tuple[datetime, int, int]]:
    # retorna [(ts, prev, curr)] para toda troca de estado
    out = []
    if not series: return out
    prev = series[0]["v"]
    for i in range(1, len(series)):
        curr = series[i]["v"]
        if curr != prev:
            out.append((series[i]["ts"], prev, curr))
            prev = curr
    return out
# routes/metrics.py (continuação)

def _reconstruct_cycles(s1: List[Dict[str, Any]], s2: List[Dict[str, Any]],
                        v_open: Optional[List[Dict[str, Any]]] = None,
                        v_close: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    """
    Reconstrói ciclos com base em S1/S2 (e opcionalmente bordas de válvula).
    Regra:
      - abertura inicia em borda S1: 1->0 (ou V_AVANCO 0->1)
      - abertura termina em borda S2: 0->1
      - fechamento inicia em borda S2: 1->0 (ou V_RECUO 0->1)
      - fechamento termina em borda S1: 0->1
    """
    # indexar bordas
    e_s1 = _edges(s1)
    e_s2 = _edges(s2)
    e_vo = _edges(v_open) if v_open else []
    e_vc = _edges(v_close) if v_close else []

    # coleciona eventos com tipo
    events = []
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

    cycles = []
    open_start = None
    close_start = None
    cycle_start = None

    for ts, kind in events:
        if kind in ("OPEN_START", "OPEN_START_CMD"):
            open_start = ts
            if cycle_start is None:
                cycle_start = ts
        elif kind == "OPEN_END" and open_start:
            t_open = (ts - open_start).total_seconds() * 1000.0
            cycles.append({"open_start": open_start, "open_end": ts,
                           "t_open_ms": t_open, "close_start": None,
                           "close_end": None, "t_close_ms": None,
                           "ts_start": cycle_start, "ts_end": None})
            open_start = None
        elif kind in ("CLOSE_START", "CLOSE_START_CMD"):
            close_start = ts
            if cycle_start is None:
                cycle_start = ts
        elif kind == "CLOSE_END" and close_start:
            t_close = (ts - close_start).total_seconds() * 1000.0
            target = None
            for c in reversed(cycles):
                if c["close_end"] is None:
                    target = c; break
            if target is None:
                target = {"open_start": None, "open_end": None, "t_open_ms": None,
                          "close_start": None, "close_end": None, "t_close_ms": None,
                          "ts_start": cycle_start, "ts_end": None}
                cycles.append(target)
            target["close_start"] = close_start
            target["close_end"] = ts
            target["t_close_ms"] = t_close
            target["ts_end"] = ts
            if target["ts_start"] and target["ts_end"]:
                target["t_cycle_ms"] = (target["ts_end"] - target["ts_start"]).total_seconds() * 1000.0
            close_start = None
            cycle_start = None

    # filtra só ciclos completos
    return [c for c in cycles if c.get("t_open_ms") is not None and c.get("t_close_ms") is not None and c.get("t_cycle_ms") is not None]

def _group_minute(cycles: List[Dict[str, Any]]) -> Dict[datetime, Dict[str, Any]]:
    agg = {}
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
    Adapte o ID conforme seu mapeamento: 'MPUA1' / 'MPUA2', etc.
    """
    mpu_id = f"MPU{act}"  # ex.: A1 -> MPUA1
    sql = """
      SELECT ts_utc, az
      FROM mpu_samples
      WHERE id=%s AND ts_utc >= %s
      ORDER BY ts_utc ASC
      LIMIT 200000
    """
    rows = fetch_all(sql, (mpu_id, since_dt))
    out = {}
    for r in rows:
        ts = r["ts_utc"].replace(second=0, microsecond=0, tzinfo=timezone.utc)
        d = out.setdefault(ts, {"sum": 0.0, "n": 0})
        val = float(r["az"] or 0.0)
        d["sum"] += val; d["n"] += 1
    return {k: (v["sum"] / v["n"]) for k, v in out.items() if v["n"] > 0}
# routes/metrics.py (final)

def _compute_minute_agg(act: str, since: str) -> List[Dict[str, Any]]:
    """
    Helper reutilizável: devolve a lista 'minute-agg' completa
    (minute, t_*_ms_avg, runtime_s, cpm, vib_avg).
    """
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

@router.get("/cycles")
def get_cycles(act: str = Query(..., pattern="^(A1|A2)$"),
               since: str = Query("-2h"),
               limit: int = 1000):
    if act not in SIGNALS:
        raise HTTPException(400, "act inválido (A1|A2).")
    since_dt = datetime.now(timezone.utc) - _parse_since(since)

    s1 = _fetch_signal(SIGNALS[act]["S1"], since_dt)
    s2 = _fetch_signal(SIGNALS[act]["S2"], since_dt)

    try:
        v_open = _fetch_signal(SIGNALS[act]["V_AVANCO"], since_dt)
    except Exception:
        v_open = None
    try:
        v_close = _fetch_signal(SIGNALS[act]["V_RECUO"], since_dt)
    except Exception:
        v_close = None

    cycles = _reconstruct_cycles(s1, s2, v_open, v_close)
    return cycles[-limit:]

@router.get("/minute-agg")
def get_minute_agg(act: str = Query(..., pattern="^(A1|A2)$"),
                   since: str = Query("-2h")):
    # mantém compat: retorna LISTA (sem wrapper)
    return _compute_minute_agg(act=act, since=since)

@router.get("/cpm-runtime-minute")
def get_cpm_runtime_minute(act: str = Query(..., pattern="^(A1|A2)$"),
                           since: str = Query("-2h")) -> Dict[str, List[Dict[str, Any]]]:
    """
    Série por minuto contendo somente CPM e Runtime para o atuador.
    Retorna {"data":[{"minute": ISO, "cpm": n, "runtime_s": n}]}
    """
    rows = _compute_minute_agg(act=act, since=since)
    out = [{"minute": r["minute"], "cpm": r.get("cpm"), "runtime_s": r.get("runtime_s")} for r in rows]
    # garante ordenação crescente por minute
    out.sort(key=lambda x: x["minute"])
    return {"data": out}
