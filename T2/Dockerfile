# Imagem de um nó do KV store replicado (MC714, Trabalho 2).
# O nó usa apenas a biblioteca padrão do Python: não há `pip install`.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONIOENCODING=utf-8

WORKDIR /app
COPY main.py ./
COPY src ./src

# Toda a configuração vem de variáveis de ambiente (ver compose.yaml):
#   NO_ID, PARES, NO_HOST, DIR_DADOS, DIR_LOGS, HEARTBEAT, TIMEOUT_FD, TIMEOUT_RPC, ...
# /dados e /logs são volumes: o estado sobrevive a `docker compose kill` + `start`
# (crash-recovery), e os logs JSONL ficam acessíveis ao verificador no host.
VOLUME ["/dados", "/logs"]
EXPOSE 9000

ENTRYPOINT ["python", "main.py", "no"]
