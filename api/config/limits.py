# config/limits.py

CPM_THRESHOLDS = {
    "green": 100,
    "amber": 50,
    "red": 0
}

VIBRATION_THRESHOLDS = {
    "green": 0.2,     # vibração leve
    "amber": 0.4,     # moderada
    "red": 0.4        # crítica acima disso
}

STATE_LABELS = {
    (1, 0, 0, 0): ("ABERTO", "green"),
    (0, 1, 0, 0): ("FECHADO", "green"),
    (1, 1, 0, 0): ("CONFLITO_SENSORES", "red"),
    (0, 0, 1, 0): ("ABRINDO", "amber"),
    (0, 0, 0, 1): ("FECHANDO", "amber"),
    (0, 0, 1, 1): ("CONFLITO_VALVULAS", "red"),
    (0, 0, 0, 0): ("TRANSICAO", "amber"),
}

TRANSITION_TIMEOUT_S = 2.0  # tempo máx de transição tolerada

ALERT_TYPES = {
    "state": "Estado inconsistente",
    "vibration": "Vibração anormal",
    "cpm": "Ciclos por minuto fora do padrão",
    "transition_timeout": "Tempo de transição excedido"
}

# Variáveis auxiliares para análises futuras
DYNAMIC_LIMITS = {
    "max_cycle_time": 3.0,           # tempo total tolerado por ciclo
    "max_open_time": 1.5,            # tempo máximo de abertura
    "max_close_time": 1.5            # tempo máximo de fechamento
}

# Parâmetro para runningTime
RUN_TRACKING_TOLERANCE = 4.0  # segundos sem inserir dados = sistema inativo
