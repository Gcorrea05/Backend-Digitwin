# api/Dockerfile
FROM python:3.11-slim

# --- Variáveis de ambiente seguras/úteis ---
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    UVICORN_WORKERS=1 \
    UVICORN_HOST=0.0.0.0 \
    UVICORN_PORT=8000 \
    TZ=Etc/UTC

# --- Dependências do sistema (mínimas) ---
# Para numpy/pandas, as wheels manylinux resolvem sem toolchain.
# tzdata é opcional (só se você for setar fuso local).
RUN apt-get update && apt-get install -y --no-install-recommends \
    tzdata ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# --- Instala dependências Python primeiro (cache mais efetivo) ---
COPY requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

# --- Copia o código da API ---
COPY . /app

# --- Cria usuário não-root por segurança ---
RUN useradd -m -u 10001 appuser
USER appuser

EXPOSE 8000

# --- Healthcheck simples (requisição /health) ---
HEALTHCHECK --interval=30s --timeout=3s --retries=3 CMD curl -fsS http://127.0.0.1:8000/health || exit 1

# --- Comando de execução (uvicorn direto) ---
# Ajuste "api.api_ws:app" se seu módulo/arquivo tiver outro nome.
CMD ["uvicorn", "api.api_ws:app", "--host", "0.0.0.0", "--port", "8000"]
