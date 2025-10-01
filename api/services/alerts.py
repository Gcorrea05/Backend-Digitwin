# api/services/alerts.py
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone
import json
from ..database import get_db

# Severidade padrão (1..5)
def _sev(val: Any) -> int:
    try:
        x = int(val)
        return max(1, min(5, x))
    except Exception:
        return 3

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _get_cursor():
    db = get_db()
    if hasattr(db, "cursor"):  # conexão
        cur = db.cursor(dictionary=True)
        return db, cur, True
    else:
        return None, db, True  # já é cursor

def _close(conn, cur):
    try:
        if cur and hasattr(cur, "close"): cur.close()
    finally:
        if conn and hasattr(conn, "close"): conn.close()

def register_alert(
    *,
    code: str,
    message: str,
    origin: Optional[str] = None,        # A1/A2/S1/S2
    type_: str = "tempo",                # categoria: tempo|vibracao|temperatura|pressao...
    severity: int = 3,                   # 1..5
    actuator_id: Optional[int] = None,
    value: Optional[float] = None,
    limit_value: Optional[float] = None,
    unit: Optional[str] = None,
    recommendations: Optional[List[str]] = None,
    causes: Optional[List[str]] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> int:
    """
    Registra um alerta em alert_events.
    Retorna o ID inserido.
    """
    conn, cur, must_close = _get_cursor()
    try:
        ts = _now_utc().replace(tzinfo=None)  # DATETIME (sem tz) no MySQL
        sql = """
            INSERT INTO alert_events
                (ts, code, type, severity, status, message, origin,
                 actuator_id, value, limit_value, unit,
                 recommendations, causes, meta)
            VALUES
                (%s, %s, %s, %s, 'open', %s, %s,
                 %s, %s, %s, %s,
                 %s, %s, %s)
        """
        recs = json.dumps(recommendations or [])
        cuses = json.dumps(causes or [])
        mjs = json.dumps(meta or {})

        params = (
            ts, code, type_, _sev(severity), message, origin,
            actuator_id, value, limit_value, unit,
            recs, cuses, mjs
        )
        cur.execute(sql, params)

        # commit se for conexão
        if conn and hasattr(conn, "commit"):
            conn.commit()

        # pegar ID
        inserted_id = None
        try:
            inserted_id = cur.lastrowid
        except Exception:
            pass
        return int(inserted_id or 0)
    finally:
        _close(conn, cur)


def build_alert_payload(
    actuator_id: int,
    alert_type: str,
    severity: int,
    message: str,
    *,
    code: Optional[str] = None,
    origin: Optional[str] = None,
    recommendations: Optional[List[str]] = None,
    causes: Optional[List[str]] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Mantido por compatibilidade com chamadas antigas (se você já usa).
    OBS: preferir chamar register_alert(...) diretamente.
    """
    return {
        "ts": datetime.utcnow().isoformat() + "Z",
        "actuator_id": actuator_id,
        "type": alert_type,
        "severity": severity,
        "message": message,
        "meta": {
            "code": code or alert_type,
            "origin": origin or (f"A{actuator_id}" if actuator_id else None),
            "recommendations": recommendations or [],
            "causes": causes or [],
            **(extra or {}),
        },
    }
