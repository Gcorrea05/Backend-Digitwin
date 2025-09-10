# routes/alerts.py
from fastapi import APIRouter, Query
from typing import List, Optional
from services.alerts import register_alert
from database import get_db

router = APIRouter()

@router.get("/alerts")
def get_alerts(actuator_id: Optional[int] = Query(None), limit: int = 100):
    db = get_db()
    if actuator_id:
     db.execute("""
        SELECT * FROM alerts
        WHERE actuator_id = %s
        ORDER BY ts DESC
        LIMIT %s
        """, (actuator_id, limit))
    else:
     db.execute("""
        SELECT * FROM alerts
        ORDER BY ts DESC
        LIMIT %s
        """, (limit,))
    return [dict(row) for row in db.fetchall()]