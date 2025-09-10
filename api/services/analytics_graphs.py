# services/analytics_graphs.py
from datetime import datetime, timedelta
from typing import List, Dict, Any
from database import get_db


def get_cpm_timeseries(actuator_id: int, hours: int = 6) -> List[Dict[str, Any]]:
    """Retorna o histórico de CPM do atuador para uso em gráfico."""
    db = get_db()
    query = """
        SELECT ts, cpm
        FROM actuator_stats
        WHERE actuator_id = %s AND ts >= %s AND cpm IS NOT NULL
        ORDER BY ts ASC
    """
    start = datetime.utcnow() - timedelta(hours=hours)
    db.execute(query, (actuator_id, start))
    return [dict(row) for row in db.fetchall()]


def get_cycle_summary_timeseries(actuator_id: int, hours: int = 6) -> List[Dict[str, Any]]:
    db = get_db()
    query = """
        SELECT ts, cycle_time, open_time, close_time
        FROM actuator_cycles
        WHERE actuator_id = %s AND ts >= %s
        ORDER BY ts ASC
    """
    start = datetime.utcnow() - timedelta(hours=hours)
    db.execute(query, (actuator_id, start))
    return [dict(row) for row in db.fetchall()]
