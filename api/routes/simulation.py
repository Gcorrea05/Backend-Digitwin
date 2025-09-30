# api/routes/simulation.py  — usa error_scenarios + error_catalog
from fastapi import APIRouter, HTTPException, Body
from typing import Any, Dict, List, Optional
import random, json
from ..database import fetch_all

router = APIRouter(prefix="/simulation", tags=["Simulation"])

# ---- severidade por código (fallbacks) ----
SEVERITY_MAP = {
    "IMU_SAT": 4,
    "STATE_STUCK": 4,
    "VIB_HIGH": 3,
    "IMU_STUCK": 3,
    "CYCLE_SLOW": 3,
    "NO_SAMPLES": 2,
}

def _parse_actions(v: Any) -> List[str]:
    if v is None: return []
    if isinstance(v, list): return [str(x) for x in v]
    if isinstance(v, (dict,)): return [str(x) for x in v.values()]
    s = v.decode() if isinstance(v, (bytes, bytearray)) else str(v)
    try:
        j = json.loads(s)
        if isinstance(j, list): return [str(x) for x in j]
        return [str(j)]
    except Exception:
        sep = ";" if ";" in s else ("," if "," in s else "\n")
        return [x.strip() for x in s.split(sep) if x.strip()]

def _parse_overrides(v: Any) -> Dict[str, Any]:
    if v is None: return {}
    s = v.decode() if isinstance(v, (bytes, bytearray)) else str(v)
    try:
        j = json.loads(s)
        return j if isinstance(j, dict) else {}
    except Exception:
        return {}

def _fetch_all() -> List[Dict[str, Any]]:
    rows = fetch_all(
        """
        SELECT s.scenario_id, s.error_id, s.title, s.description,
               s.signal_overrides, s.expected_alert, s.actions,
               c.id AS cat_id, c.code AS code, c.name AS error_name, c.grp AS grp
        FROM error_scenarios s
        LEFT JOIN error_catalog c ON c.id = s.error_id
        ORDER BY s.scenario_id ASC
        """
    )
    out = []
    for r in rows:
        code = (r.get("code") or "UNKNOWN").upper()
        label = r.get("expected_alert") or r.get("title") or r.get("error_name") or code
        actions = _parse_actions(r.get("actions"))
        overrides = _parse_overrides(r.get("signal_overrides"))
        sev = int(SEVERITY_MAP.get(code, 3))
        out.append({
            "id": r.get("scenario_id"),
            "error_id": r.get("error_id"),
            "code": code,
            "name": r.get("error_name") or r.get("title") or code,
            "grp": r.get("grp"),
            "label": str(label),
            "desc": r.get("description"),
            "actions": actions,
            "overrides": overrides,
            "severity": sev,
        })
    return out

def _find_by_label(label: str) -> Optional[Dict[str, Any]]:
    lab = label.strip().lower()
    for it in _fetch_all():
        if (it["label"] or "").strip().lower() == lab:
            return it
    return None

def _find_by_code(code: str) -> Optional[Dict[str, Any]]:
    cd = code.strip().upper()
    for it in _fetch_all():
        if (it["code"] or "").strip().upper() == cd:
            return it
    return None

def _rand_params_for(code: str) -> Dict[str, Any]:
    r = random; c = (code or "").upper()
    if c == "VIB_HIGH":   return {"rms_limit": round(r.uniform(1.2, 2.5), 2), "cycles": r.randint(3, 10)}
    if c == "IMU_SAT":    return {"saturation_pct": r.randint(90, 100), "cycles": r.randint(1, 5)}
    if c == "IMU_STUCK":  return ({"flat_seconds": r.randint(3, 15)} if r.random()<0.5 else {"drift_per_min": round(r.uniform(0.1, 0.8), 2)})
    if c == "CYCLE_SLOW": return {"cycle_delta_pct": r.randint(10, 30)}
    if c == "STATE_STUCK":return {"timeout_ms": r.randint(500, 2000)}
    if c == "NO_SAMPLES": return {"age_s": r.randint(5, 60)}
    return {}

@router.get("/catalog")
def catalog():
    items = _fetch_all()
    # dropdown por label humano
    return {"items": [
        {"id": it["id"], "code": it["code"], "name": it["name"], "grp": it["grp"], "label": it["label"], "severity": it["severity"]}
        for it in items
    ], "label_map": {it["code"]: it["label"] for it in items}}

@router.post("/draw")
def draw(payload: Dict[str, Any] = Body(...)):
    """
    { "mode":"by_label", "label":"<expected_alert/title>", "actuator":1 }
    ou
    { "mode":"by_code", "code":"VIB_HIGH", "actuator":2 }
    """
    mode = (payload.get("mode") or "by_label").lower()
    actuator = int(payload.get("actuator") or random.choice([1, 2]))
    if actuator not in (1, 2):
        raise HTTPException(400, "actuator deve ser 1 ou 2")

    row = _find_by_label(str(payload.get("label"))) if mode == "by_label" \
        else _find_by_code(str(payload.get("code") or ""))
    if not row:
        raise HTTPException(404, "cenário não encontrado")

    params = _rand_params_for(row["code"])
    # mescla com overrides do banco (sobrescreve o random se tiver)
    if row.get("overrides"):
        params.update(row["overrides"])

    return {
        "scenario_id": f"sim-{random.randint(100000, 999999)}",
        "actuator": actuator,
        "error": {
            "id": row["id"],
            "code": row["code"],
            "name": row["name"],
            "grp": row["grp"],
            "severity": row["severity"],
        },
        "cause": row["label"],                 # rótulo humano (expected_alert/title)
        "actions": row["actions"],             # lista já normalizada
        "params": params,                      # random + overrides
        "ui": {"halt_sim": True, "halt_3d": True, "show_popup": True},
        "resume_allowed": row["severity"] <= 3,
        "desc": row.get("desc"),
    }
