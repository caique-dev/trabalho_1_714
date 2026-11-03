"""Parâmetros do nó e leitura da lista de pares.

O mesmo código roda como processo local ou como contêiner; a única diferença é
a configuração: lista de pares (``id=host:porta``), diretórios e temporizações.
Cada opção pode vir de argumento de linha de comando ou de variável de ambiente
(é assim que o ``compose.yaml`` configura os contêineres).
"""

from dataclasses import dataclass, asdict


@dataclass
class Parametros:
    """Temporizações e opções do protocolo (segundos).

    intervalo_hb   período do heartbeat do líder.
    timeout_fd     silêncio após o qual o detector suspeita de um nó.
    timeout_rpc    prazo de cada chamada (OK da eleição, ACK de réplica, ...).
    timeout_coord  espera pelo COORDENADOR depois de receber um OK.
    quorum         nº mínimo de nós (incluindo o primário) para confirmar uma
                   escrita; None = maioria de N. Com 1 a replicação volta a ser
                   "ACK de todos da visão" sem proteção contra partição.
    fsync          força fsync a cada escrita (proteção contra queda do SO;
                   não é necessário para crash do processo).
    semente        semente do gerador da injeção de falhas.
    """
    intervalo_hb: float = 0.5
    timeout_fd: float = 2.0
    timeout_rpc: float = 1.0
    timeout_coord: float = None
    quorum: int = None
    fsync: bool = False
    semente: int = 0

    def __post_init__(self):
        if self.timeout_coord is None:
            self.timeout_coord = 4.0 * self.timeout_rpc

    def como_dict(self):
        return asdict(self)


def parse_pares(texto):
    """Converte ``"1=127.0.0.1:9001,2=no2:9000"`` em ``{1: ("127.0.0.1", 9001), ...}``."""
    pares = {}
    for item in texto.split(","):
        item = item.strip()
        if not item:
            continue
        ident, _, endereco = item.partition("=")
        host, _, porta = endereco.rpartition(":")
        if not ident or not host or not porta:
            raise ValueError(f"par inválido: {item!r} (esperado id=host:porta)")
        pares[int(ident)] = (host, int(porta))
    if not pares:
        raise ValueError("lista de pares vazia")
    return pares


def formatar_pares(enderecos):
    """Inverso de :func:`parse_pares`."""
    return ",".join(f"{i}={h}:{p}" for i, (h, p) in sorted(enderecos.items()))
