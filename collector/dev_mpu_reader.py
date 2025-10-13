# dev_mpu_reader.py — Simulador de MPU para modo DEV/DEV_DUAL (sem CSV)
# Gera amostras determinísticas a cada tick para mpu_id 1 e 2,
# com perfis coerentes com ciclos de 2.6s (A1) e 3.6s (A2).

import os, time, math
from typing import Dict, Any, Tuple

def _now_monotonic() -> float:
    return time.perf_counter()

class _Pulse:
    """Pulso exponencial simples para simular 'trancos' na troca de fase."""
    def __init__(self, decay_ms: int = 180):
        self.decay_s = max(0.001, decay_ms) / 1000.0
        self.t0 = None
        self.sign = 0.0
    def trigger(self, sign: float):
        self.t0 = _now_monotonic()
        self.sign = float(sign)
    def value(self) -> float:
        if self.t0 is None: return 0.0
        age = _now_monotonic() - self.t0
        if age <= 0.0: return 0.0
        return self.sign * math.exp(-age / self.decay_s)

class _ActuatorClock:
    """Relógio de fase: alterna AVANCO/RECUO em período fixo (ms), sem jitter."""
    ADV = 1
    RET = -1
    def __init__(self, phase_ms: int):
        self.period = max(1, int(phase_ms)) / 1000.0
        self.dir = self.RET  # começa recuando
        self.t0 = _now_monotonic()
        self.last_flip = self.t0
    def step(self) -> Tuple[int, bool]:
        now = _now_monotonic()
        if (now - self.last_flip) >= self.period:
            self.dir = self.ADV if self.dir == self.RET else self.RET
            self.last_flip = now
            return self.dir, True
        return self.dir, False

class DevMpuReader:
    """
    Gerador de amostras brutas do MPU (sem porta serial).
    - tick é controlado pelo laço de quem chama (ex.: 20 ms)
    - a cada read_tick() retorna DUAS amostras (mpu_id 1 e 2)
    Campos: mpu_id, ax_g, ay_g, az_g, (gx_dps/gy_dps/gz_dps = None)
    """
    def __init__(self):
        # Durações de fase por atuador (ms) — iguais às do OPC DEV
        a1_ms = int(os.getenv("A1_PHASE_MS", "2600"))
        a2_ms = int(os.getenv("A2_PHASE_MS", "3600"))
        self.a1 = _ActuatorClock(a1_ms)
        self.a2 = _ActuatorClock(a2_ms)

        # Pulsos de troca de fase (tranco) por atuador
        self.pulse1 = _Pulse(decay_ms=int(os.getenv("MPU_PULSE_DECAY_MS", "180")))
        self.pulse2 = _Pulse(decay_ms=int(os.getenv("MPU_PULSE_DECAY_MS", "180")))

        # Fases lentas para 'respiração' da gravidade
        self._phi1 = 0.0
        self._phi2 = 0.0

        # Amplitudes (determinísticas)
        self.k_ax = float(os.getenv("MPU_AX_BASE", "0.02"))    # var. lenta em X
        self.k_ay = float(os.getenv("MPU_AY_BASE", "0.01"))    # var. lenta em Y
        self.k_az = float(os.getenv("MPU_AZ_BASE", "0.02"))    # var. lenta em Z (acima de 1g)

        # Passo de fase por tick (aprox) — usado como variação suave
        dev_tick_ms = float(os.getenv("DEV_TICK_MS_MPU", os.getenv("DEV_TICK_MS", "20")))
        dt = max(1.0, dev_tick_ms) / 1000.0
        self._dphi1 = 2.0 * math.pi * dt / 0.50   # 0.5 s p/ uma oscilação lenta
        self._dphi2 = 2.0 * math.pi * dt / 0.65   # 0.65 s p/ outra oscilação

    def _wave_triplet(self, phi: float) -> Tuple[float, float, float]:
        # Ondas suaves determinísticas (sem ruído aleatório)
        ax = self.k_ax * math.sin(phi)
        ay = self.k_ay * math.cos(2.0 * phi)
        az = 1.0 + self.k_az * math.sin(0.5 * phi)  # centrado em 1g
        return ax, ay, az

    def _one_sample(self, mpu_id: int, direction: int, pulse: _Pulse, phi: float) -> Dict[str, Any]:
        ax, ay, az = self._wave_triplet(phi)
        # Tranco de troca de fase: empurra ax
        ax += 0.30 * pulse.value()  # pico ~0.3g que decai exponencial
        # Inclinação leve conforme sentido:
        ax += 0.05 if direction == _ActuatorClock.ADV else -0.05
        # gyro desativado (NaN/None como no seu CSV)
        return {
            "mpu_id": mpu_id,
            "ax_g": round(ax, 6),
            "ay_g": round(ay, 6),
            "az_g": round(az, 6),
            "gx_dps": None, "gy_dps": None, "gz_dps": None
        }

    def read_tick(self) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        # Atualiza atuadores e dispara pulso na troca
        dir1, flip1 = self.a1.step()
        dir2, flip2 = self.a2.step()
        if flip1: self.pulse1.trigger(+1.0 if dir1 == _ActuatorClock.ADV else -1.0)
        if flip2: self.pulse2.trigger(+1.0 if dir2 == _ActuatorClock.ADV else -1.0)

        # Samples determinísticos
        s1 = self._one_sample(1, dir1, self.pulse1, self._phi1)
        s2 = self._one_sample(2, dir2, self.pulse2, self._phi2)

        # avança fases lentas
        self._phi1 += self._dphi1
        self._phi2 += self._dphi2
        return s1, s2
