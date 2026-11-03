"""Formato das mensagens trocadas entre nós, clientes e o administrador.

Toda mensagem é um objeto JSON em uma única linha (delimitada por ``\\n``) com
os campos do envelope:

    tipo     nome da mensagem (constantes abaixo)
    origem   id inteiro do nó remetente (ou "cliente"/"admin")
    destino  id inteiro do nó destinatário
    epoca    maior época conhecida pelo remetente (usada para fencing)
    id       identificador único da mensagem
    inc      incarnação do remetente (distingue reinício de falsa suspeita)
    resp_a   (só em respostas) id da requisição respondida
    dados    carga útil específica do tipo

Escolhi JSON por linha porque o enquadramento fica trivial sobre TCP (basta
``readline``) e o log/depuração ficam legíveis; o custo de serialização é
irrelevante frente ao custo de conexão em localhost.
"""

import json

# --- Bully -----------------------------------------------------------------
ELEICAO = "ELEICAO"            # "há alguém mais alto vivo?" (enviada só a ids maiores)
OK = "OK"                      # resposta a ELEICAO: "estou vivo, assumo a eleição"
COORDENADOR = "COORDENADOR"    # anúncio do vencedor com a nova época

# --- Detector de falhas ----------------------------------------------------
HEARTBEAT = "HEARTBEAT"        # líder -> todos, periódico
HB_ACK = "HB_ACK"              # resposta ao heartbeat

# --- Replicação primário-backup --------------------------------------------
REPLICAR = "REPLICAR"          # primário -> backup: (época, seq, chave, valor)
ACK = "ACK"                    # resposta genérica de confirmação (dados.ok)
RECUSA = "RECUSA"              # fencing: época antiga/conflito; carrega época atual
SINC_CONSULTA = "SINC_CONSULTA"  # novo primário -> todos: (época, último seq)?
SINC_RESP = "SINC_RESP"
PEDIR_ESTADO = "PEDIR_ESTADO"  # pede o snapshot completo
ESTADO = "ESTADO"              # resposta com o snapshot
INSTALAR_ESTADO = "INSTALAR_ESTADO"  # primário -> backup: instale este snapshot
ENTRAR = "ENTRAR"              # membro pede ingresso na visão do primário

# --- Clientes e administração (fora da contagem de protocolo) --------------
CLI_PUT = "CLI_PUT"
CLI_GET = "CLI_GET"
RESP = "RESP"
ADM_STATUS = "ADM_STATUS"
ADM_FALHAS = "ADM_FALHAS"
ADM_PARAR = "ADM_PARAR"

TIPOS_EXTERNOS = {CLI_PUT, CLI_GET, RESP, ADM_STATUS, ADM_FALHAS, ADM_PARAR}

# Limite de uma linha: snapshots completos trafegam em uma única mensagem.
LIMITE_LINHA = 64 * 1024 * 1024


def nova(tipo, origem, destino, epoca, dados=None, *, id_msg, inc=0, resp_a=None):
    """Monta o dicionário de uma mensagem."""
    msg = {"tipo": tipo, "origem": origem, "destino": destino, "epoca": epoca,
           "id": id_msg, "inc": inc, "dados": dados if dados is not None else {}}
    if resp_a is not None:
        msg["resp_a"] = resp_a
    return msg


def codificar(msg):
    """Serializa uma mensagem em uma linha de bytes terminada em ``\\n``."""
    return (json.dumps(msg, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")


def decodificar(linha):
    """Inverso de :func:`codificar`; levanta ValueError se a linha for inválida."""
    msg = json.loads(linha.decode("utf-8"))
    if not isinstance(msg, dict) or "tipo" not in msg:
        raise ValueError("mensagem sem campo 'tipo'")
    return msg
