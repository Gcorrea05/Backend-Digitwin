# config/limits.py

# ============================
# Limiares existentes (seus)
# ============================

CPM_THRESHOLDS = {
    "green": 100,
    "amber": 50,
    "red": 0,
}

VIBRATION_THRESHOLDS = {
    "green": 0.2,   # vibração leve
    "amber": 0.4,   # moderada
    "red": 0.4,     # crítica acima disso
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

# Tempo máximo tolerado para permanecer em transição
TRANSITION_TIMEOUT_S = 2.0

ALERT_TYPES = {
    "state": "Estado inconsistente",
    "vibration": "Vibração anormal",
    "cpm": "Ciclos por minuto fora do padrão",
    "transition_timeout": "Tempo de transição excedido",
}

# Variáveis auxiliares para análises futuras
DYNAMIC_LIMITS = {
    "max_cycle_time": 3.0,   # tempo total tolerado por ciclo (s)
    "max_open_time": 1.5,    # tempo máximo de abertura (s)
    "max_close_time": 1.5,   # tempo máximo de fechamento (s)
}

# Considera inatividade se não chegam dados dentro deste período
RUN_TRACKING_TOLERANCE = 4.0  # segundos


# ==============================================
# BLOCO NOVO — Configuração de ALERTS (opcional)
# ==============================================

# 1) Limites de tempo (ms) por atuador (para CYCLE_SLOW/FAST etc.)
TIME_LIMITS_MS = {
    "A1": {
        "open_max": 1500,    # ms
        "close_max": 1500,   # ms
        "cycle_max": 3000,   # ms
    },
    "A2": {
        "open_max": 1000,
        "close_max": 1000,
        "cycle_max": 2500,
    },
}

# 2) Bandas de severidade por “excesso” relativo ao limite
#    Ex.: até 10% acima => sev 2; 10–25% => 3; 25–50% => 4; >50% => 5
ALERT_SEVERITY_BANDS = [
    (0.10, 2),
    (0.25, 3),
    (0.50, 4),
    (9.99, 5),  # acima de 50%
]

# 3) Limiares de vibração (RMS em g) — compatível com VIBRATION_THRESHOLDS
VIBRATION_LIMITS = {
    "green_max": 0.20,
    "amber_max": 0.40,
    "red_min":   0.40,
}

# 4) Anti-spam: deduplicação e cooldown (segundos)
ALERT_DEDUPE_WINDOW_S = 30   # mesmo (code+origin) em 30s vira 1 alerta
ALERT_COOLDOWN_S       = 20   # intervalo mínimo para repetir o mesmo code

# 5) Textos/templates padrão por código (usados no popup se nada for passado)
ALERT_TEMPLATES = {
    "CYCLE_SLOW": {
        "type": "tempo",
        "unit": "ms",
        "recommendations": [
            "Realizar parada controlada",
            "Checar pressão e regulagem das válvulas",
            "Verificar obstruções mecânicas",
        ],
        "causes": [
            "Baixa pressão de linha",
            "Vazamento na tubulação",
            "Ajuste incorreto de válvula",
            "Atrito/contato no curso do atuador",
        ],
    },
    "CYCLE_FAST": {
        "type": "tempo",
        "unit": "ms",
        "recommendations": [
            "Reduzir velocidade de atuação",
            "Validar setpoint do CLP",
        ],
        "causes": [
            "Setpoint incorreto",
            "Válvula travada aberta",
        ],
    },
    "IMU_SAT": {
        "type": "vibracao",
        "unit": "g",
        "recommendations": [
            "Reduzir velocidade do ciclo",
            "Inspecionar fixação do sensor",
            "Verificar folgas no conjunto mecânico",
        ],
        "causes": [
            "Choque/impacto mecânico",
            "Parafuso solto/holgura",
            "Desbalanceamento",
        ],
    },
    "TEMP_HIGH": {
        "type": "temperatura",
        "unit": "°C",
        "recommendations": [
            "Checar ventilação e dissipação",
            "Inspecionar fonte e cabeamento",
        ],
        "causes": [
            "Superaquecimento do atuador",
            "Ambiente acima do especificado",
        ],
    },
    "PRESS_LOW": {
        "type": "pressao",
        "unit": "bar",
        "recommendations": [
            "Verificar compressores e reguladores",
            "Inspecionar vazamentos",
        ],
        "causes": [
            "Falta de pressão na linha",
            "Vazamento em conexões",
        ],
    },
}
