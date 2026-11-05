"""Cliente síncrono (sockets bloqueantes) e chamadas administrativas.

O cliente pode falar com qualquer nó: um nó que não é o primário responde com
``redirecionar`` (o líder que ele conhece) e o cliente refaz o pedido lá. Todas
as chamadas têm timeout; durante um failover o cliente repete a operação (com
rodízio entre os nós) até esgotar o ``prazo``.

Semântica de falha: se uma tentativa de PUT estoura o timeout, o resultado é
*indeterminado* (o primário pode ter confirmado e a resposta se perdido).
O cliente repete o PUT; como PUT é uma sobrescrita idempotente da mesma
chave/valor, a repetição é segura. Só uma resposta ``ok`` conta como escrita
confirmada.
"""

import itertools
import socket
import time
from dataclasses import dataclass, field

from . import mensagens as M

_contador = itertools.count(1)


def chamar_no(endereco, tipo, dados=None, timeout=1.0, origem="cliente"):
    """Uma requisição/resposta a um nó. Devolve a mensagem de resposta ou None."""
    msg = M.nova(tipo, origem, None, 0, dados, id_msg=f"{origem}:{next(_contador)}")
    prazo = time.monotonic() + timeout
    try:
        with socket.create_connection(endereco, timeout=timeout) as s:
            s.sendall(M.codificar(msg))
            buf = b""
            while not buf.endswith(b"\n"):
                restante = prazo - time.monotonic()
                if restante <= 0:
                    return None
                s.settimeout(restante)
                pedaco = s.recv(1 << 20)
                if not pedaco:
                    break
                buf += pedaco
        return M.decodificar(buf) if buf else None
    except (OSError, ValueError):
        return None


@dataclass
class Resultado:
    ok: bool
    dados: dict = field(default_factory=dict)
    latencia: float = 0.0   # segundos, incluindo repetições e redirecionamentos
    tentativas: int = 0
    erro: str = None
    no: int = None          # nó que atendeu


class Cliente:
    def __init__(self, enderecos, timeout=1.0, prazo=10.0):
        self.enderecos = dict(enderecos)
        self.ids = sorted(self.enderecos)
        self.timeout = timeout
        self.prazo = prazo
        self.dica = None
        self._rodizio = itertools.cycle(self.ids)

    def _executar(self, tipo, dados, prazo):
        t0 = time.perf_counter()  # alta resolução para a latência
        limite = time.monotonic() + (prazo if prazo is not None else self.prazo)
        tentativas, erro = 0, "prazo_esgotado"
        while time.monotonic() < limite:
            alvo = self.dica if self.dica is not None else next(self._rodizio)
            resp = chamar_no(self.enderecos[alvo], tipo, dados,
                             timeout=min(self.timeout, max(0.05, limite - time.monotonic())))
            tentativas += 1
            if resp is None:
                erro = "sem_resposta"
                self.dica = None
                time.sleep(0.02)
                continue
            d = resp["dados"]
            if d.get("ok"):
                self.dica = alvo
                return Resultado(True, d, time.perf_counter() - t0, tentativas, None, alvo)
            erro = d.get("erro", "erro")
            red = d.get("redirecionar")
            if red is not None and red != alvo and red in self.enderecos:
                self.dica = red
                continue
            self.dica = None
            time.sleep(0.05)
        return Resultado(False, {}, time.perf_counter() - t0, tentativas, erro, None)

    def put(self, chave, valor, prazo=None):
        return self._executar(M.CLI_PUT, {"chave": chave, "valor": valor}, prazo)

    def get(self, chave, prazo=None):
        return self._executar(M.CLI_GET, {"chave": chave}, prazo)


def status(endereco, incluir_dados=False, timeout=1.0):
    """Status de um nó (dict) ou None se não responder."""
    r = chamar_no(endereco, M.ADM_STATUS, {"dados": incluir_dados}, timeout, origem="admin")
    return None if r is None else r["dados"]


def resumo_status(s):
    """Linha legível do status de um nó, destacando se o líder tem quórum ativo agora.

    ``pronto`` diz apenas que o líder concluiu a sincronização da sua época;
    ``quorum_ativo`` diz se ele ainda enxerga a maioria (membros da visão com
    resposta recente). Um líder isolado por partição continua ``pronto``, mas
    com ``quorum_ativo=false`` não confirma escritas nem serve leituras.
    """
    partes = [f"papel={s['papel']}", f"epoca={s['epoca']}", f"lider={s['lider']}",
              f"pronto={str(s['pronto']).lower()}"]
    qa = s.get("quorum_ativo")
    if qa is not None:
        partes.append(f"quorum_ativo={str(qa).lower()}")
    partes += [f"visao={s['visao']}", f"seq={s['seq']}"]
    linha = " ".join(partes)
    if s["papel"] == "lider" and qa is False:
        linha += ("\n  >>> ATENÇÃO: líder SEM quórum ativo (isolado ou maioria indisponível): "
                  "não confirma escritas nem serve leituras")
    return linha


def configurar_falhas(endereco, timeout=1.0, **config):
    """Altera a injeção de falhas de um nó (ver ``InjetorFalhas.configurar``)."""
    r = chamar_no(endereco, M.ADM_FALHAS, config, timeout, origem="admin")
    return None if r is None else r["dados"]
