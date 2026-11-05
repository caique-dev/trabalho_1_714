# Imagem única do projeto (MC714, Trabalho 2), usada por três serviços do compose.yaml:
#   no1..no5     um nó do KV store (ENTRYPOINT padrão: python main.py no)
#   testes       pytest (profile "testes")
#   experimentos python main.py experimentos (profile "experimentos")
# Os nós usam apenas a biblioteca padrão; as dependências do requirements.txt
# (pytest, numpy, matplotlib) servem aos testes, à campanha e aos gráficos.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONIOENCODING=utf-8 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Dependências primeiro: a camada fica em cache enquanto o requirements.txt não muda.
COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY main.py ./
COPY src ./src
COPY tests ./tests

# Toda a configuração do nó vem de variáveis de ambiente (ver compose.yaml):
#   NO_ID, PARES, NO_HOST, DIR_DADOS, DIR_LOGS, HEARTBEAT, TIMEOUT_FD, TIMEOUT_RPC, ...
# /dados e /logs são volumes: o estado sobrevive a `docker compose kill` + `start`
# (crash-recovery), e os logs JSONL ficam acessíveis ao verificador no host.
VOLUME ["/dados", "/logs"]
EXPOSE 9000

ENTRYPOINT ["python", "main.py", "no"]
