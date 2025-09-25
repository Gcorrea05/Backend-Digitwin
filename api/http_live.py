from fastapi import APIRouter, Query, HTTPException
from mysql.connector import pooling
from datetime import timezone
from typing import Dict, Any, List, Tuple
import os

router = APIRouter(prefix="/api/live", tags=["live"])

# pool injetado no startup (ver main.py)
pool: pooling.MySQLConnectionPool

WIN_LIMIT_SECS = int(os.getenv("RUNTIME_WIN_SECS", "3"))     # janela p/ pegar últimos
STALE_LIMIT_SECS = float(os.getenv("RUNTIME_STALE_SECS", "1"))  # leitura considerada “fresca”

def tags_for(act: str) -> Tuple[str, str, str, str]:
    if act.upper() == "A2":
        return ("V2_12","V2_14","Recuado_2S1","Avancado_2S2")
    return ("V1_12","V1_14","Recuado_1S1","Avancado_1S2")

@router.get("/runtime")
def get_runtime(act: str = Query("A1")) -> Dict[str, Any]:
    V_ABRE, V_FECHA, S_REC, S_AVA = tags_for(act)

    sql = f"""
    WITH ranked AS (
      SELECT name, value, ts,
             ROW_NUMBER() OVER (PARTITION BY name ORDER BY ts DESC) AS rn
      FROM opc_samples
      WHERE name IN (%s,%s,%s,%s)
        AND ts >= NOW(6) - INTERVAL {WIN_LIMIT_SECS} SECOND
    )
    SELECT name, value, ts FROM ranked WHERE rn=1;
    """

    conn = router.pool.get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, (V_ABRE, V_FECHA, S_REC, S_AVA))
        rows = cur.fetchall()
    finally:
        cur.close(); conn.close()

    vals: Dict[str, int] = {}
    tsmap: Dict[str, Any] = {}
    ts_max = None
    for name, value, ts in rows:
        vals[name] = int(value)
        ts = ts if getattr(ts, "tzinfo", None) else ts.replace(tzinfo=timezone.utc)
        tsmap[name] = ts
        ts_max = max(ts_max, ts) if ts_max else ts

    # classificador simples e robusto
    def fresh(tag: str) -> bool:
        if tag not in tsmap: return False
        age = (ts_max - tsmap[tag]).total_seconds()
        return age <= STALE_LIMIT_SECS

    rec = vals.get(S_REC) if fresh(S_REC) else None
    ava = vals.get(S_AVA) if fresh(S_AVA) else None

    if rec == 1 and ava == 0:
        estado = "FECHADO"
    elif rec == 0 and ava == 1:
        estado = "ABERTO"
    else:
        estado = "TRANSICAO"

    return {
        "ts": int(ts_max.timestamp()) if ts_max else None,
        "system": {"status": "ok" if ts_max else "unknown"},
        "actuators": [{
            "id": act.upper(),
            "estado": estado,
            "tags": vals
        }],
        "mpu": None
    }

@router.get("/actuators/timings")
def get_timings(act: str = Query("A1"), since: str = Query("-15m")) -> Dict[str, Any]:
    # implementação simplificada: você já tem algo parecido no seu projeto;
    # aqui deixamos o shape esperado e você pode reaproveitar seu código de bordas
    return {
        "act": act.upper(),
        "since": since,
        "cycles": [],
        "stats": {"avg_avancar": None, "avg_recuar": None, "count": 0}
    }
