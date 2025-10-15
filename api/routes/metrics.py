# api/routes/metrics.py
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Tuple, Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

# importa o helper real do seu projeto
from ..database import get_db

router = APIRouter(tags=["metrics"])

# ============================== #
# utils de data/hora (robustos)  #
# ============================== #

def _to_dt_utc(x: Any) -> datetime:
    """Coerce para datetime timezone-aware (UTC). Nunca levanta exceção."""
    if isinstance(x, datetime):
        return x if x.tzinfo else x.replace(tzinfo=timezone.utc)
    s = str(x).strip().replace("Z", "+00:00")
    fmts = (
        "%Y-%m-%d %H:%M:%S.%f%z",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
    )
    for fmt in fmts:
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            pass
    return datetime.now(timezone.utc)

def _dt_to_iso_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def _parse_since_to_seconds(s: str, default_s: int = 7200) -> int:
    """Aceita -60m, -2h, -7200s, ISO (interpreta janela = agora - ts)."""
    if not s:
        return default_s
    ss = str(s).strip()
    if ss.startswith("-"):
        try:
            if ss.endswith("h"):
                return int(float(ss[1:-1]) * 3600)
            if ss.endswith("m"):
                return int(float(ss[1:-1]) * 60)
            if ss.endswith("s"):
                return int(float(ss[1:-1]))
            return int(float(ss[1:]))
        except Exception:
            return default_s
    # ISO absoluto -> janela = now - since
    try:
        dt = _to_dt_utc(ss)
        delta = (datetime.now(timezone.utc) - dt).total_seconds()
        if math.isfinite(delta) and delta > 0:
            return int(delta)
    except Exception:
        pass
    return default_s

# ============================== #
# mapeamento de sinais por atuador
# ============================== #

@dataclass(frozen=True)
class _LatchCfg:
    id: str
    s1: str  # S1 = recuado
    s2: str  # S2 = avançado

# Mantido compatível com o api_ws.py
_CFG = {
    1: _LatchCfg(id="A1", s1="Recuado_1S1",  s2="Avancado_1S2"),
    2: _LatchCfg(id="A2", s1="Recuado_2S1",  s2="Avancado_2S2"),
}

def _resolve_actuator_id(act: Optional[str|int], actuator: Optional[str|int], _id: Optional[str|int]) -> int:
    cand: Optional[str|int] = act if act is not None else actuator if actuator is not None else _id
    if cand is None:
        raise HTTPException(status_code=400, detail="informe ?act=A1|A2 ou ?id=1|2")
    if isinstance(cand, str):
        s = cand.strip().upper()
        if s.startswith("A"):
            s = s[1:]
        try:
            aid = int(s)
        except Exception:
            raise HTTPException(status_code=422, detail="valor de 'act' inválido (use 1/2 ou A1/A2)")
    else:
        try:
            aid = int(cand)
        except Exception:
            raise HTTPException(status_code=422, detail="valor de 'id' inválido (use 1/2)")
    if aid not in _CFG:
        raise HTTPException(status_code=422, detail="atuador inválido (somente 1 ou 2)")
    return aid

# ============================== #
# acesso ao banco (mínimo)       #
# ============================== #

def _fetch_all(q: str, params: Tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
    db = get_db()
    try:
        db.execute(q, params)
        return db.fetchall()
    finally:
        db.close()

# ============================== #
# lógica: séries + contagem       #
# ============================== #

def _dedup_bool_series(rows: List[Tuple[datetime, int]]) -> List[Tuple[datetime, int]]:
    if not rows:
        return []
    out = [rows[0]]
    for t, v in rows[1:]:
        if v != out[-1][1]:
            out.append((t, v))
    return out

def _rising_edges_count_by_minute(series: List[Tuple[datetime, int]]) -> Dict[datetime, int]:
    """
    Conta bordas 0->1 e agrega por minuto (UTC).
    """
    result: Dict[datetime, int] = {}
    prev = None
    for i in range(len(series)):
        t, v = series[i]
        if prev is None:
            prev = v
            continue
        if prev == 0 and v == 1:
            bucket = t.replace(second=0, microsecond=0, tzinfo=timezone.utc)
            result[bucket] = result.get(bucket, 0) + 1
        prev = v
    return result

def _load_s2_series(name_s2: str, window_s: int) -> List[Tuple[datetime, int]]:
    sql = f"""
        SELECT ts_utc, value_bool
        FROM opc_samples
        WHERE name = %s
          AND ts_utc >= NOW(6) - INTERVAL %s SECOND
        ORDER BY ts_utc ASC
    """
    rows = _fetch_all(sql, (name_s2, window_s))
    series: List[Tuple[datetime, int]] = []
    for r in rows:
        ts = _to_dt_utc(r.get("ts_utc") if isinstance(r, dict) else r[0])
        vb = r.get("value_bool") if isinstance(r, dict) else r[1]
        try:
            v = 1 if int(vb) else 0
        except Exception:
            v = 1 if bool(vb) else 0
        series.append((ts, v))
    return _dedup_bool_series(series)

# ============================== #
# endpoint                       #
# ============================== #

@router.get("/metrics/minute-agg")
def metrics_minute_agg(
    act: Optional[str|int] = Query(None, description="A1/A2 ou 1/2"),
    id: Optional[str|int] = Query(None, description="sinônimo de act"),
    actuator: Optional[str|int] = Query(None, description="sinônimo de act"),
    since: str = Query("-60m", description="ex: -60m, -2h, -7200s, ou ISO absoluto"),
):
    """
    Agrega a **produção por minuto** (contagem de ciclos) usando as bordas de subida do S2.
    - Requer um atuador: ?act=A1|A2 ou ?id=1|2 (qualquer um dos três nomes serve).
    - 'since' define a janela retroativa (padrão 60m).
    """
    aid = _resolve_actuator_id(act, actuator, id)
    s2_name = _CFG[aid].s2
    window_s = _parse_since_to_seconds(since, default_s=3600)

    series = _load_s2_series(s2_name, window_s)
    buckets = _rising_edges_count_by_minute(series)

    # Preenche minutos faltantes com zero para facilitar o gráfico
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start = now - timedelta(seconds=window_s)
    cur = start.replace(second=0, microsecond=0)
    out_points: List[Dict[str, Any]] = []
    while cur <= now:
        out_points.append({"ts": _dt_to_iso_z(cur), "count": int(buckets.get(cur, 0))})
        cur += timedelta(minutes=1)

    return JSONResponse({
        "actuator": aid,
        "signal": s2_name,
        "bucket_s": 60,
        "since_s": window_s,
        "points": out_points,
        "count": len(out_points),
    })
