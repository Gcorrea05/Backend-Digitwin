# api/services/cycle.py
from __future__ import annotations
from datetime import datetime, timedelta
from typing import List, Tuple, Optional, Any

from ..database import get_db


def parse_ts(ts_str: str) -> float:
    return datetime.fromisoformat(ts_str.replace("Z", "+00:00")).timestamp()


def extract_cycles(events: List[dict], actuator_id: int) -> List[dict]:
    """
    Detecta ciclos A->F->A ou F->A->F com base nos eventos recebidos.
    Espera eventos tipo: { "ts": ..., "name": "Avancado_1S2" ou "Recuado_1S1", "value": 1 }
    """
    state_stack: List[Tuple[str, float]] = []  # (estado, timestamp)
    cycles: List[dict] = []

    for event in events:
        name = event.get("name")
        value = event.get("value")
        ts = parse_ts(event.get("ts"))

        if value != 1:
            continue  # apenas mudanças para 1 são relevantes

        if name == f"Recuado_{actuator_id}S1":
            state_stack.append(("FECHADO", ts))
        elif name == f"Avancado_{actuator_id}S2":
            state_stack.append(("ABERTO", ts))

        # tenta formar ciclo
        if len(state_stack) >= 3:
            s1, s2, s3 = state_stack[-3:]
            tipos = [s for s, _ in (s1, s2, s3)]

            if tipos in (["ABERTO", "FECHADO", "ABERTO"], ["FECHADO", "ABERTO", "FECHADO"]):
                open_time = s2[1] - s1[1]
                close_time = s3[1] - s2[1]
                cycle_time = s3[1] - s1[1]
                cycles.append({
                    "type": "AFA" if tipos[0] == "ABERTO" else "FAF",
                    "start": s1[1],
                    "end": s3[1],
                    "open_time": open_time,
                    "close_time": close_time,
                    "cycle_time": cycle_time
                })
                state_stack = []  # limpa após ciclo

    return cycles


def summarize_latest(cycles: List[dict]) -> Optional[dict]:
    if not cycles:
        return None

    last = cycles[-1]
    return {
        "cycle_type": last["type"],
        "cycle_time": round(last["cycle_time"], 3),
        "open_time": round(last["open_time"], 3),
        "close_time": round(last["close_time"], 3)
    }


def _try_query_one(query: str, params: Tuple[Any, ...]) -> Tuple[bool, Tuple]:
    try:
        db = get_db()
        try:
            db.execute(query, params)
            row = db.fetchone()
            return (row is not None, row or ())
        finally:
            db.close()
    except Exception:
        return (False, ())


def _count_cycles_since(actuator_id: int, since_utc: datetime) -> Optional[int]:
    """
    Tenta contar ciclos concluídos desde 'since_utc' em tabelas candidatas.
    Ajuste os nomes se já souber o definitivo.
    """
    candidates = [
        ("SELECT COUNT(*) FROM cycles WHERE actuator_id=%s AND ts >= %s", (actuator_id, since_utc)),
        ("SELECT COUNT(*) FROM cycle_events WHERE actuator_id=%s AND ts >= %s AND kind='CYCLE_END'", (actuator_id, since_utc)),
        ("SELECT COUNT(*) FROM cycles_atuador WHERE actuator_id=%s AND ts >= %s", (actuator_id, since_utc)),
        ("SELECT COUNT(*) FROM cycles_atuador1 WHERE ts >= %s", (since_utc,)),  # sem coluna actuator_id
        ("SELECT COUNT(*) FROM cycles_atuador2 WHERE ts >= %s", (since_utc,)),
    ]
    for q, p in candidates:
        ok, row = _try_query_one(q, p)
        if ok and len(row) >= 1 and row[0] is not None:
            try:
                return int(row[0])
            except Exception:
                pass
    return None


def get_cycle_rate(actuator_id: int = 1, window_minutes: int = 5) -> Optional[float]:
    """
    Retorna CPM (cycles per minute) nos últimos 'window_minutes'.
    Se não achar dados/tabelas, retorna None.
    """
    since = datetime.utcnow() - timedelta(minutes=window_minutes)
    count = _count_cycles_since(actuator_id, since)
    if count is None or window_minutes <= 0:
        return None
    cpm = count / float(window_minutes)
    return round(cpm, 2)
