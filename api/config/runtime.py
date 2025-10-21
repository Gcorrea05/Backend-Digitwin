# api/config/runtime.py
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime
from threading import RLock

class AlertsConfig(BaseModel):
    # vibração: overall RMS limite para disparar alerta
    vibration_overall_threshold: float = Field(0.5, ge=0)
    # fator de timeout para pending (multiplica expected_ms)
    latch_timeout_factor: float = Field(1.5, ge=1.0, le=10.0)
    # expected_ms por atuador (override opcional dos _CFG_A1/_CFG_A2)
    expected_ms_A1: Optional[int] = Field(None, ge=100, le=60000)
    expected_ms_A2: Optional[int] = Field(None, ge=100, le=60000)

    # (opcional) limiares de severidade CPM/vibração para badges, etc.
    vib_green: float = 0.2
    vib_amber: float = 0.4
    cpm_green: float = 100
    cpm_amber: float = 50

    updated_at: datetime = Field(default_factory=datetime.utcnow)

_CFG = AlertsConfig()
_LOCK = RLock()

def get_alerts_config() -> AlertsConfig:
    with _LOCK:
        return AlertsConfig(**_CFG.model_dump())

def update_alerts_config(patch: dict) -> AlertsConfig:
    global _CFG
    with _LOCK:
        _CFG = _CFG.model_copy(update={**patch, "updated_at": datetime.utcnow()})
        return AlertsConfig(**_CFG.model_dump())
