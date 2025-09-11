# api/services/analytics_graphs.py
from __future__ import annotations

from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple

from ..database import get_db


def _to_iso(ts: Any) -> str:
    try:
        return ts.isoformat()
    except Exception:
        return str(ts)


def get_cpm_timeseries(actuator_id: int, hours: int = 6) -> List[Dict[str, Any]]:
    """
    Retorna o histórico de CPM do atuador para uso em gráfico.
    OBS: mapeamos manualmente porque o cursor padrão retorna tuplas.
    """
    db = get_db()
    try:
        start = datetime.utcnow() - timedelta(hours=hours)
        query = """
            SELECT ts, cpm
            FROM actuator_stats
            WHERE actuator_id = %s AND ts >= %s AND cpm IS NOT NULL
            ORDER BY ts ASC
        """
        db.execute(query, (actuator_id, start))
        rows = db.fetchall() or []
        # rows: [(ts, cpm), ...]
        out: List[Dict[str, Any]] = []
        for ts, cpm in rows:
            out.append({"ts": _to_iso(ts), "cpm": float(cpm)})
        return out
    finally:
        db.close()


def get_cycle_summary_timeseries(actuator_id: int, hours: int = 6) -> List[Dict[str, Any]]:
    """
    Série temporal de tempos de ciclo/abertura/fechamento.
    """
    db = get_db()
    try:
        start = datetime.utcnow() - timedelta(hours=hours)
        query = """
            SELECT ts, cycle_time, open_time, close_time
            FROM actuator_cycles
            WHERE actuator_id = %s AND ts >= %s
            ORDER BY ts ASC
        """
        db.execute(query, (actuator_id, start))
        rows = db.fetchall() or []
        # rows: [(ts, cycle_time, open_time, close_time), ...]
        out: List[Dict[str, Any]] = []
        for ts, cycle_time, open_time, close_time in rows:
            out.append({
                "ts": _to_iso(ts),
                "cycle_time": float(cycle_time) if cycle_time is not None else None,
                "open_time": float(open_time) if open_time is not None else None,
                "close_time": float(close_time) if close_time is not None else None,
            })
        return out
    finally:
        db.close()


def _try_query_all(query: str, params: Tuple[Any, ...]) -> List[Tuple]:
    """SELECT ... -> lista de tuplas; silencioso a falhas (retorna [])."""
    try:
        db = get_db()
        try:
            db.execute(query, params)
            return db.fetchall() or []
        finally:
            db.close()
    except Exception:
        return []


def get_vibration_data(actuator_id: int = 1, minutes: int = 10) -> Dict[str, Any]:
    """
    Série temporal de vibração (RMS) para gráficos.
    Tenta múltiplas tabelas candidatas; ajuste conforme seu schema.
    Retorna:
        {"actuator_id": 1, "window_minutes": 10, "points": [{"t": "...", "rms": 0.12}, ...], "count": N}
    """
    since = datetime.utcnow() - timedelta(minutes=minutes)
    candidates = [
        # Usa agregados da janela de 200 ms (overall RMS = sqrt(ax_rms^2 + ay_rms^2 + az_rms^2))
        (
            """
            SELECT ts_utc AS ts,
                   SQRT(POW(ax_rms,2) + POW(ay_rms,2) + POW(az_rms,2)) AS rms
              FROM mpu_windows_200ms
             WHERE mpu_id = %s AND ts_utc >= %s
             ORDER BY ts_utc ASC
            """,
            (actuator_id, since),
        ),
        (
            "SELECT ts, rms FROM vibration_rms WHERE actuator_id=%s AND ts >= %s ORDER BY ts ASC",
            (actuator_id, since),
        ),
        (
            "SELECT ts, rms FROM analytics_vibration WHERE actuator_id=%s AND ts >= %s ORDER BY ts ASC",
            (actuator_id, since),
        ),
        (
            "SELECT ts, rms FROM vib_rms WHERE actuator_id=%s AND ts >= %s ORDER BY ts ASC",
            (actuator_id, since),
        ),
    ]

    rows: List[Tuple] = []
    for q, p in candidates:
        rows = _try_query_all(q, p)
        if rows:
            break

    points = [
        {"t": _to_iso(ts), "rms": float(rms)}
        for (ts, rms) in rows
        if rms is not None
    ]

    return {
        "actuator_id": actuator_id,
        "window_minutes": minutes,
        "points": points,
        "count": len(points),
    }

# Compat: se em algum lugar seu código antigo usar este nome:
get_vibration_trends = get_vibration_data
