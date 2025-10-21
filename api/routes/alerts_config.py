# api/routes/alerts_config.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
from pathlib import Path
from datetime import datetime, timezone
import json

from ..database import get_db

router = APIRouter(prefix="/api/alerts", tags=["alerts-config"])

CFG_FILE = Path(__file__).resolve().parent.parent / "alerts_config.json"

class AlertsConfig(BaseModel):
    vibration_overall_threshold: float = Field(10.0, ge=0)
    latch_timeout_factor: float = Field(1.5, ge=0)
    expected_ms_A1: Optional[float] = Field(300.0, ge=0)
    expected_ms_A2: Optional[float] = Field(300.0, ge=0)
    vib_green: float = Field(5.0, ge=0)
    vib_amber: float = Field(10.0, ge=0)
    cpm_green: float = Field(20.0, ge=0)
    cpm_amber: float = Field(10.0, ge=0)
    updated_at: Optional[str] = None

# --------- helpers DB ----------
def _table_exists(cur, name: str) -> bool:
    cur.execute(
        """
        SELECT COUNT(*) AS c
        FROM information_schema.tables
        WHERE table_name = %s
        """,
        (name,),
    )
    row = cur.fetchone() or {}
    return int(row.get("c") or 0) > 0

def _ensure_table(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS alert_config (
            id TINYINT PRIMARY KEY DEFAULT 1,
            cfg JSON NOT NULL,
            updated_at TIMESTAMP NULL DEFAULT NULL
        )
        """
    )

def _select_cfg(cur) -> dict | None:
    cur.execute("SELECT cfg FROM alert_config WHERE id = 1")
    row = cur.fetchone()
    if not row:
        return None
    cfg = row.get("cfg")
    if isinstance(cfg, (bytes, str)):
        try:
            return json.loads(cfg)
        except Exception:
            return None
    return cfg

def _upsert_cfg(cur, cfg: dict):
    cur.execute(
        """
        INSERT INTO alert_config (id, cfg, updated_at)
        VALUES (1, %s, NOW())
        ON DUPLICATE KEY UPDATE cfg = VALUES(cfg), updated_at = VALUES(updated_at)
        """,
        (json.dumps(cfg, ensure_ascii=False),),
    )

# --------- helpers FILE ----------
def _file_load() -> dict:
    if CFG_FILE.exists():
        try:
            return json.loads(CFG_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}

def _file_save(cfg: dict):
    CFG_FILE.write_text(json.dumps(cfg, indent=2, ensure_ascii=False), encoding="utf-8")

# --------- load/save genéricos ----------
def _default_cfg() -> AlertsConfig:
    return AlertsConfig(updated_at=datetime.now(timezone.utc).isoformat())

def _load_cfg_any() -> AlertsConfig:
    # tenta DB
    try:
        db = get_db()
        conn = db if hasattr(db, "cursor") else None
        cur = db.cursor(dictionary=True) if conn else db
        try:
            if not _table_exists(cur, "alert_config"):
                _ensure_table(cur)
                conn and conn.commit()
            data = _select_cfg(cur)
            if not data:
                cfg = _default_cfg().model_dump()
                _upsert_cfg(cur, cfg)
                conn and conn.commit()
                return AlertsConfig(**cfg)
            return AlertsConfig(**data)
        finally:
            try: cur.close()
            except: pass
            try: conn and conn.close()
            except: pass
    except Exception:
        # fallback arquivo
        data = _file_load()
        if not data:
            cfg = _default_cfg().model_dump()
            _file_save(cfg)
            return AlertsConfig(**cfg)
        return AlertsConfig(**data)

def _save_cfg_any(patch: dict) -> AlertsConfig:
    cfg = _load_cfg_any().model_dump()
    cfg.update(patch or {})
    cfg["updated_at"] = datetime.now(timezone.utc).isoformat()

    # tenta DB
    try:
        db = get_db()
        conn = db if hasattr(db, "cursor") else None
        cur = db.cursor(dictionary=True) if conn else db
        try:
            if not _table_exists(cur, "alert_config"):
                _ensure_table(cur)
            _upsert_cfg(cur, cfg)
            conn and conn.commit()
            return AlertsConfig(**cfg)
        finally:
            try: cur.close()
            except: pass
            try: conn and conn.close()
            except: pass
    except Exception:
        # fallback arquivo
        _file_save(cfg)
        return AlertsConfig(**cfg)

# --------- routes ----------
@router.get("/config", response_model=AlertsConfig)
def get_alerts_config():
    return _load_cfg_any()

@router.post("/config", response_model=AlertsConfig)
def update_alerts_config(patch: dict):
    try:
        return _save_cfg_any(patch)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"invalid config: {e}")
