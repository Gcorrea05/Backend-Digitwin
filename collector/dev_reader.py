# collectors/dev_reader.py
# Fonte DEV: gera estados booleanos a cada chamada (tick de 20 ms controlado pelo loop do app.py)
# Regras aprovadas:
# - A1 muda de fase (AVANCANDO <-> RECUANDO) a cada 2600 ms
# - A2 muda de fase (AVANCANDO <-> RECUANDO) a cada 3600 ms
# - Sem jitter, sem defasagem, sem dwell
# - Emite APENAS as tags existentes no nodes.csv
#
# Bits por fase:
#   AVANCANDO:  Avancado_*S2=1, Recuado_*S1=0, V*_14=1, V*_12=0
#   RECUANDO:   Recuado_*S1=1, Avancado_*S2=0, V*_12=1, V*_14=0
# Auxiliares (se existirem no CSV): INICIA=1, PARA=0

import os
import csv
import time
from typing import Dict, Any, List, Set

PHASE_ADV = "AVANCANDO"
PHASE_RET = "RECUANDO"

def _load_node_names(csv_path: str) -> Set[str]:
    names: Set[str] = set()
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            name = (row.get("name") or "").strip()
            if name:
                names.add(name)
    if not names:
        raise RuntimeError(f"nodes.csv vazio ou inválido: {csv_path}")
    return names

class _ActuatorFSM:
    """FSM simples com fase fixa (sem jitter/defasagem) em milissegundos por fase."""
    def __init__(self, t_phase_ms: int):
        self.t_phase_ms = max(1, int(t_phase_ms))
        self.phase = PHASE_RET  # começa RECUANDO (sensores recuo = 1)
        self.t_last = time.perf_counter()

    def step(self):
        now = time.perf_counter()
        elapsed_ms = (now - self.t_last) * 1000.0
        if elapsed_ms >= self.t_phase_ms:
            # troca de fase e reancora o relógio
            self.phase = PHASE_ADV if self.phase == PHASE_RET else PHASE_RET
            self.t_last = now

class DevReader:
    """
    Implementa a mesma interface conceitual do OpcUaReader: read_all() -> Dict[str, Any]
    O controle de 20 ms é feito pelo loop do app.py (sleep determinístico).
    """
    def __init__(self, nodes_csv: str):
        self.names: Set[str] = _load_node_names(nodes_csv)

        # Durações por fase (constantes)
        a1_ms = int(os.getenv("A1_PHASE_MS", "2600"))
        a2_ms = int(os.getenv("A2_PHASE_MS", "3600"))

        # FSMs independentes (simultâneos)
        self.a1 = _ActuatorFSM(a1_ms)
        self.a2 = _ActuatorFSM(a2_ms)

        # Presença de sinais auxiliares
        self.has_inicia = "INICIA" in self.names
        self.has_para   = "PARA"   in self.names

        # Mapa de nomes (existem apenas se presentes no CSV)
        self.n_a1_s1 = "Recuado_1S1"
        self.n_a1_s2 = "Avancado_1S2"
        self.n_a2_s1 = "Recuado_2S1"
        self.n_a2_s2 = "Avancado_2S2"
        self.n_v1_12 = "V1_12"
        self.n_v1_14 = "V1_14"
        self.n_v2_12 = "V2_12"
        self.n_v2_14 = "V2_14"

    def _emit_for_actuator(self, phase: str, tag_map: Dict[str, int], s1: str, s2: str, v12: str, v14: str):
        if phase == PHASE_ADV:
            # avanço
            if s1 in self.names:  tag_map[s1]  = 0
            if s2 in self.names:  tag_map[s2]  = 1
            if v12 in self.names: tag_map[v12] = 0
            if v14 in self.names: tag_map[v14] = 1
        else:
            # recuo
            if s1 in self.names:  tag_map[s1]  = 1
            if s2 in self.names:  tag_map[s2]  = 0
            if v12 in self.names: tag_map[v12] = 1
            if v14 in self.names: tag_map[v14] = 0

    def read_all(self) -> Dict[str, Any]:
        """
        Chamado a cada tick (20 ms) pelo loop do app.py.
        Retorna {name: 0/1} SOMENTE para as tags presentes no nodes.csv.
        """
        # avança FSMs conforme o tempo decorrido
        self.a1.step()
        self.a2.step()

        out: Dict[str, int] = {}

        # A1
        self._emit_for_actuator(
            self.a1.phase, out,
            self.n_a1_s1, self.n_a1_s2,
            self.n_v1_12, self.n_v1_14
        )
        # A2
        self._emit_for_actuator(
            self.a2.phase, out,
            self.n_a2_s1, self.n_a2_s2,
            self.n_v2_12, self.n_v2_14
        )

        # auxiliares
        if self.has_inicia:
            out["INICIA"] = 1
        if self.has_para:
            out["PARA"] = 0

        return out
