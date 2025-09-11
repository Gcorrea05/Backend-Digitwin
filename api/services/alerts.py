# services/alerts.py
from datetime import datetime
from typing import Dict, Any
from ..database import get_db
import json


def build_alert_payload(actuator_id: int, alert_type: str, severity: str, message: str, extra: dict = {}) -> Dict[str, Any]:
    return {
        "ts": datetime.utcnow().isoformat() + "Z",
        "actuator_id": actuator_id,
        "type": alert_type,
        "severity": severity,
        "message": message,
        "meta": extra
    }


def register_alert(alert: Dict[str, Any]):
    db = get_db()
    query = """
        INSERT INTO alerts (ts, actuator_id, type, severity, message, meta)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    db.execute(query, (
        alert["ts"].replace("Z", ""),  # MySQL DATETIME doesn't accept Z
        alert["actuator_id"],
        alert["type"],
        alert["severity"],
        alert["message"],
        json.dumps(alert.get("meta", {}))
    ))
    db.commit()