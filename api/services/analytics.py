# services/analytics.py
from ..database import get_db  # usa o helper de conexão
from datetime import datetime
from typing import Optional, Literal, Dict,Any,Tuple

from ..config.limits import (
    CPM_THRESHOLDS,
    VIBRATION_THRESHOLDS,
    TRANSITION_TIMEOUT_S,
    STATE_LABELS,
    ALERT_TYPES
)

Severity = Literal["green", "amber", "red"]


def analyze_state(s1: int, s2: int, v1: int, v2: int, last_change: Optional[str]) -> Dict[str, str]:
    key = (s1, s2, v1, v2)
    label, severity = STATE_LABELS.get(key, ("TRANSICAO", "amber"))
    alert = None

    if label.startswith("CONFLITO"):
        alert = ALERT_TYPES["state"]
    elif label == "TRANSICAO" and last_change:
        try:
            dt_last = datetime.fromisoformat(last_change)
            if (datetime.utcnow() - dt_last).total_seconds() > TRANSITION_TIMEOUT_S:
                alert = ALERT_TYPES["transition_timeout"]
                severity = "red"
        except Exception:
            pass

    return {"state": label, "stateSeverity": severity, "alert": alert}


def analyze_cpm(value: Optional[float]) -> Dict[str, str]:
    if value is None:
        return {"cpmSeverity": "gray"}
    if value >= CPM_THRESHOLDS["green"]:
        return {"cpmSeverity": "green"}
    if value >= CPM_THRESHOLDS["amber"]:
        return {"cpmSeverity": "amber"}
    return {"cpmSeverity": "red", "alert": ALERT_TYPES["cpm"]}


def analyze_vibration(rms: Optional[float]) -> Dict[str, str]:
    if rms is None:
        return {"vibrationSeverity": "gray"}
    if rms <= VIBRATION_THRESHOLDS["green"]:
        return {"vibrationSeverity": "green"}
    if rms <= VIBRATION_THRESHOLDS["amber"]:
        return {"vibrationSeverity": "amber"}
    return {"vibrationSeverity": "red", "alert": ALERT_TYPES["vibration"]}

def _try_query_one(query: str, params: Tuple[Any, ...]) -> Tuple[bool, Tuple]:
    """Executa SELECT ... LIMIT 1 e retorna (ok, row). Não levanta exceção."""
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

def _fetch_state_snapshot(actuator_id: int) -> Tuple[int|None,int|None,int|None,int|None,str|None]:
    """
    Tenta obter (s1, s2, v1, v2, last_change_iso) de tabelas candidatas.
    Ajuste os nomes se já souber exatamente.
    """
    candidates = [
        # (query, columns order)
        ("SELECT s1, s2, v1, v2, last_change FROM actuator_status "
         "WHERE actuator_id=%s ORDER BY updated_at DESC LIMIT 1", (actuator_id,)),
        ("SELECT s1, s2, v1, v2, last_change FROM states "
         "WHERE actuator_id=%s ORDER BY ts DESC LIMIT 1", (actuator_id,)),
        ("SELECT s1, s2, v1, v2, last_change FROM samples_latest "
         "WHERE actuator_id=%s LIMIT 1", (actuator_id,)),
    ]
    for q, p in candidates:
        ok, row = _try_query_one(q, p)
        if ok and len(row) >= 5:
            s1, s2, v1, v2, last_change = row
            return (int(s1), int(s2), int(v1), int(v2), str(last_change))
    # fallback: nada encontrado
    return (None, None, None, None, None)

def _fetch_cpm(actuator_id: int) -> float | None:
    """
    Tenta obter um CPM pronto. Se quiser calcular por janela, depois trocamos.
    """
    candidates = [
        ("SELECT cpm FROM cycle_rate WHERE actuator_id=%s "
         "ORDER BY ts DESC LIMIT 1", (actuator_id,)),
        ("SELECT cpm FROM cycle_stats WHERE actuator_id=%s "
         "ORDER BY ts DESC LIMIT 1", (actuator_id,)),
    ]
    for q, p in candidates:
        ok, row = _try_query_one(q, p)
        if ok and len(row) >= 1 and row[0] is not None:
            try:
                return float(row[0])
            except Exception:
                pass
    return None

def _fetch_vibration_rms(actuator_id: int) -> float | None:
    candidates = [
        ("SELECT rms FROM vibration_rms WHERE actuator_id=%s "
         "ORDER BY ts DESC LIMIT 1", (actuator_id,)),
        ("SELECT rms FROM analytics_vibration WHERE actuator_id=%s "
         "ORDER BY ts DESC LIMIT 1", (actuator_id,)),
    ]
    for q, p in candidates:
        ok, row = _try_query_one(q, p)
        if ok and len(row) >= 1 and row[0] is not None:
            try:
                return float(row[0])
            except Exception:
                pass
    return None

def get_kpis(actuator_id: int = 1) -> Dict[str, Any]:
    """
    Monta o payload de KPIs para o Dashboard/Monitoring.

    Retorno exemplo:
    {
      "state": "ABERTO",
      "stateSeverity": "green",
      "cpmSeverity": "amber",
      "vibrationSeverity": "green",
      "alerts": [...],
      "raw": {...}
    }
    """
    # 1) Coleta bruta (tolerante a ausência de dados/tabelas)
    s1, s2, v1, v2, last_change = _fetch_state_snapshot(actuator_id)
    cpm = _fetch_cpm(actuator_id)
    vib = _fetch_vibration_rms(actuator_id)

    # 2) Análises (usando suas funções)
    state_res = analyze_state(s1 or 0, s2 or 0, v1 or 0, v2 or 0, last_change)
    cpm_res   = analyze_cpm(cpm)
    vib_res   = analyze_vibration(vib)

    # 3) Agrega alerts (filtra Nones)
    alerts: list[Dict[str, Any]] = []
    for piece in (state_res, cpm_res, vib_res):
        if piece.get("alert"):
            alerts.append(piece["alert"])

    # 4) Payload final
    return {
        "actuator_id": actuator_id,
        "state": state_res.get("state"),
        "stateSeverity": state_res.get("stateSeverity", "gray"),
        "cpmSeverity": cpm_res.get("cpmSeverity", "gray"),
        "vibrationSeverity": vib_res.get("vibrationSeverity", "gray"),
        "alerts": alerts,
        "raw": {
            "s1": s1, "s2": s2, "v1": v1, "v2": v2,
            "last_change": last_change,
            "cpm": cpm,
            "vibration_rms": vib,
        },
    }
