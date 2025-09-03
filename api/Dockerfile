# api/Dockerfile
FROM python:3.11-slim

# Sistema básico + tzdata (opcional) + build deps leves
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential tzdata ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# requirements primeiro (cache de build)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# código da API
COPY . /app

# porta exposta
EXPOSE 8000

# (o comando é definido no docker-compose.yml)
