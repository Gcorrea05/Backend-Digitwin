# api/routes/alerts.py
from fastapi import APIRouter, Query, HTTPException
from typing import Any, Dict, List, Optional
import json
from ..database import get_db

router = APIRouter(prefix="", tags=["alerts"])  # expõe /alerts

# --- helpers ---------------------------------------------------------------

def _get_cursor():
    """
    Aceita tanto get_db() retornando conexão quanto cursor.
    Retorna (conn, cur) e uma flag que indica se precisamos fechar.
    """
    db = get_db()
    if hasattr(db, "cursor"):  # é conexão
        cur = db.cursor(dictionary=True)
        return db, cur, True
    else:
        # db já é cursor
        return None, db, True

def _close(conn, cur):
    try:
        if cur and hasattr(cur, "close"): cur.close()
    finally:
        if conn and hasattr(conn, "close"): conn.close()

def _table_exists(table_name: str) -> bool:
    conn, cur, must_close = _get_cursor()
    try:
        cur.execute(
            """
            SELECT COUNT(*) AS c
            FROM information_schema.tables
            WHERE table_name = %s
            """,
            (table_name,),
        )
        row = cur.fetchone()
        c = (row or {}).get("c", 0)
        return int(c or 0) > 0
    finally:
        _close(conn, cur)

def _json_load(x: Any, default):
    if isinstance(x, (bytes, str)):
        try: return json.loads(x)
        except Exception: return default
    return x if x is not None else default

def _iso(ts: Any) -> Optional[str]:
    if ts is None: return None
    try:
        return ts.isoformat().replace("+00:00", "Z")
    except Exception:
        return str(ts)

def _normalize_alert_row_from_events(r: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": r.get("id"),
        "code": r.get("code"),
        "type": r.get("type"),
        "severity": int(r.get("severity") or 0),
        "message": r.get("message"),
        "origin": r.get("origin"),
        "status": r.get("status", "open"),
        "created_at": _iso(r.get("ts")),
        "actuator_id": r.get("actuator_id"),
        "value": r.get("value"),
        "limit_value": r.get("limit_value"),
        "unit": r.get("unit"),
        "recommendations": _json_load(r.get("recommendations"), []),
        "causes": _json_load(r.get("causes"), []),
    }

def _normalize_alert_row_from_legacy(r: Dict[str, Any]) -> Dict[str, Any]:
    # Tabela legada: alerts(ts, actuator_id, type, severity, message, meta)
    meta = _json_load(r.get("meta"), {}) or {}
    return {
        "id": r.get("id"),
        "code": meta.get("code") or r.get("type"),
        "type": r.get("type"),
        "severity": int(meta.get("severity") or r.get("severity") or 0),
        "message": r.get("message"),
        "origin": meta.get("origin") or (f"A{r.get('actuator_id')}" if r.get("actuator_id") else None),
        "status": meta.get("status", "open"),
        "created_at": _iso(r.get("ts")),
        "actuator_id": r.get("actuator_id"),
        "value": meta.get("value"),
        "limit_value": meta.get("limit_value"),
        "unit": meta.get("unit"),
        "recommendations": meta.get("recommendations", []),
        "causes": meta.get("causes", []),
    }

# --- route -----------------------------------------------------------------

@router.get("/alerts")
def get_alerts(
    limit: int = Query(5, ge=1, le=50),
    status: Optional[str] = Query(None, description="open|ack|closed (opcional)"),
    actuator_id: Optional[int] = Query(None),
):
    """
    Retorna os últimos N alertas (default 5) já normalizados para a UI.
    Prioriza 'alert_events'; faz fallback para tabela legada 'alerts'.
    """
    use_legacy = False
    if _table_exists("alert_events"):
        table = "alert_events"
    elif _table_exists("alerts"):
        table = "alerts"
        use_legacy = True
    else:
        # nenhuma tabela encontrada
        return {"items": [], "count": 0}

    conn, cur, must_close = _get_cursor()
    try:
        where = []
        params: List[Any] = []

        if actuator_id is not None:
            where.append("actuator_id = %s")
            params.append(actuator_id)

        if status and not use_legacy:
            where.append("status = %s")
            params.append(status)

        sql = f"SELECT * FROM {table}"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY ts DESC LIMIT %s"
        params.append(limit)

        cur.execute(sql, params)
        rows = cur.fetchall() or []

        if use_legacy:
            items = [_normalize_alert_row_from_legacy(r) for r in rows]
        else:
            items = [_normalize_alert_row_from_events(r) for r in rows]

        return {"items": items, "count": len(items)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"alerts query failed: {e}")
    finally:
        _close(conn, cur)
