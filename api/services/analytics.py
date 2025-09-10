# services/analytics.py

from datetime import datetime
from typing import Optional, Literal, Dict

from config.limits import (
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
