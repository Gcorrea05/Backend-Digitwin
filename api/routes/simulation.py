# api/routes/simulation.py
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from ..database import get_db

router = APIRouter()

# -------- helpers locais --------
def _first(row: Any) -> Any:
    if row is None: return None
    if isinstance(row, dict): 
        for _, v in row.items(): return v
    if isinstance(row, (list, tuple)): return row[0]
    return row

def _col(row: Any, key_or_idx: Any) -> Any:
    if row is None: return None
    if isinstance(row, dict): return row.get(key_or_idx)
    if isinstance(key_or_idx, int): return row[key_or_idx]
    return None

def _fetch_all(q: str, params: Tuple[Any, ...] = ()) -> List[Any]:
    db = get_db()
    try:
        db.execute(q, params)
        return db.fetchall()
    finally:
        db.close()

def _fetch_one(q: str, params: Tuple[Any, ...] = ()) -> Any:
    db = get_db()
    try:
        db.execute(q, params)
        return db.fetchone()
    finally:
        db.close()

def _table_exists(schema: str, table: str) -> bool:
    try:
        r = _fetch_one(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",
            (schema, table),
        )
        return int(_first(r) or 0) > 0
    except Exception:
        return False

def _db_name() -> str:
    # tenta descobrir o schema atual
    try:
        r = _fetch_one("SELECT DATABASE()")
        s = _first(r)
        return str(s) if s else ""
    except Exception:
        return ""

def _ts_iso(dt: Optional[datetime]) -> Optional[str]:
    if not dt: return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

# -------- /simulation/errors (lista catálogos/cenários) --------
@router.get("/errors")
def list_errors():
    """
    Retorna lista de erros/cenários disponíveis.
    Não estoura 500: se as tabelas não existirem, retorna listas vazias.
    """
    schema = _db_name()

    out: Dict[str, Any] = {"catalog": [], "scenarios": []}

    # error_catalog (opcional)
    if schema and _table_exists(schema, "error_catalog"):
        rows = _fetch_all(
            """
            SELECT id, code, name, severity, message
            FROM error_catalog
            ORDER BY id ASC
            """
        )
        for r in rows:
            out["catalog"].append({
                "id":        _col(r, "id") or _col(r, 0),
                "code":      _col(r, "code") or _col(r, 1),
                "name":      _col(r, "name") or _col(r, 2),
                "severity":  int(_col(r, "severity") or 0),
                "message":   _col(r, "message") or "",
            })

    # error_scenarios (opcional)
    if schema and _table_exists(schema, "error_scenarios"):
        rows = _fetch_all(
            """
            SELECT id, error_id, title, description, sys_params, alert
            FROM error_scenarios
            ORDER BY id ASC
            """
        )
        for r in rows:
            out["scenarios"].append({
                "id":          _col(r, "id") or _col(r, 0),
                "error_id":    _col(r, "error_id") or _col(r, 1),
                "title":       _col(r, "title") or _col(r, 2),
                "description": _col(r, "description") or _col(r, 3),
                "sys_params":  _col(r, "sys_params") or _col(r, 4),
                "alert":       _col(r, "alert") or _col(r, 5),
            })

    # Sempre 200 com payload coerente
    return JSONResponse(content=out, headers={"Cache-Control": "no-store"})

# -------- /simulation/runs (lista execuções) --------
@router.get("/runs")
def list_runs(limit: int = Query(20, ge=1, le=100), offset: int = Query(0, ge=0)):
    """
    Lista últimas execuções de simulação.
    Se a tabela não existir, retorna lista vazia com count=0 (200).
    """
    schema = _db_name()
    items: List[Dict[str, Any]] = []

    if schema and _table_exists(schema, "simulation_runs"):
        rows = _fetch_all(
            f"""
            SELECT id, scenario_id, user_id, status, created_at, finished_at
            FROM simulation_runs
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )
        for r in rows:
            items.append({
                "id":          _col(r, "id") or _col(r, 0),
                "scenario_id": _col(r, "scenario_id") or _col(r, 1),
                "user_id":     _col(r, "user_id") or _col(r, 2),
                "status":      _col(r, "status") or _col(r, 3) or "UNKNOWN",
                "created_at":  _ts_iso(_col(r, "created_at") or _col(r, 4)),
                "finished_at": _ts_iso(_col(r, "finished_at") or _col(r, 5)),
            })

    return {"count": len(items), "items": items}

# -------- endpoints de ação (mantidos, mas sem 500) --------
@router.post("/preview")
def preview(body: Dict[str, Any]):
    """
    Ex.: { "scenario_id": 201, "user_id": "gabs", "create_alert": false, "stop_system": false, "meta": {...} }
    Se scenario_id não existe em error_scenarios, retorna 404 controlado.
    """
    scenario_id = body.get("scenario_id")
    if not scenario_id:
        raise HTTPException(status_code=404, detail="scenario_id not found")

    # valida cenário se a tabela existir
    schema = _db_name()
    if schema and _table_exists(schema, "error_scenarios"):
        r = _fetch_one("SELECT id FROM error_scenarios WHERE id=%s", (scenario_id,))
        if not r:
            raise HTTPException(status_code=404, detail="scenario_id not found")

    # Retorna só um eco da simulação (não aplica nada no sistema real)
    return {"ok": True, "preview": {"scenario_id": scenario_id, "applies": True}}

@router.post("/apply")
def apply(body: Dict[str, Any]):
    """
    Inicia uma execução e grava em simulation_runs se a tabela existir.
    Nunca 500: cai para retorno sintético se a tabela não existir.
    """
    user_id = body.get("user_id") or "anon"
    scenario_id = body.get("scenario_id")
    create_alert = bool(body.get("create_alert", False))

    if not scenario_id:
        raise HTTPException(400, detail="scenario_id is required")

    schema = _db_name()
    now = datetime.now(timezone.utc)

    run_id = None
    if schema and _table_exists(schema, "simulation_runs"):
        try:
            db = get_db()
            try:
                db.execute(
                    """
                    INSERT INTO simulation_runs (scenario_id, user_id, status, created_at)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (scenario_id, user_id, "RUNNING", now),
                )
                run_id = db.lastrowid
            finally:
                db.close()
        except Exception:
            # não quebra a API; devolve um run_id sintético
            run_id = None

    return {"ok": True, "run": {"id": run_id, "scenario_id": scenario_id, "status": "RUNNING", "create_alert": create_alert}}

@router.post("/end")
def end(body: Dict[str, Any]):
    """
    Finaliza uma execução.
    Se a tabela existir, tenta atualizar; caso contrário apenas confirma fim lógico.
    """
    run_id = body.get("run_id")
    status = str(body.get("status") or "ENDED").upper()

    if not run_id:
        raise HTTPException(status_code=404, detail="run_id not found")

    schema = _db_name()
    now = datetime.now(timezone.utc)
    if schema and _table_exists(schema, "simulation_runs"):
        try:
            db = get_db()
            try:
                db.execute(
                    "UPDATE simulation_runs SET status=%s, finished_at=%s WHERE id=%s",
                    (status, now, run_id),
                )
            finally:
                db.close()
        except Exception:
            pass

    return {"ok": True, "run": {"id": run_id, "status": status, "finished_at": _ts_iso(now)}}
