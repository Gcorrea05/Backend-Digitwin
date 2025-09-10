# services/cycle.py

from datetime import datetime
from typing import List, Tuple, Optional

def parse_ts(ts_str: str) -> float:
    return datetime.fromisoformat(ts_str.replace("Z", "+00:00")).timestamp()

def extract_cycles(events: List[dict], actuator_id: int) -> List[dict]:
    """
    Detecta ciclos A->F->A ou F->A->F com base nos eventos recebidos.
    Espera eventos tipo: { "ts": ..., "name": "Avancado_1S2" ou "Recuado_1S1", "value": 1 }
    """
    state_stack = []  # lista de (estado, timestamp)
    cycles = []

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

            if tipos == ["ABERTO", "FECHADO", "ABERTO"] or tipos == ["FECHADO", "ABERTO", "FECHADO"]:
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
