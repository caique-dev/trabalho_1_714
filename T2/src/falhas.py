"""Injeção de falhas no nível da aplicação, reprodutível por semente.

O injetor é consultado por :mod:`transporte` a cada mensagem que sai de um nó
para outro nó e decide, nesta ordem:

1. partição: o destino está no conjunto ``bloqueados`` -> descarta (e o
   transporte também ignora mensagens *recebidas* desses pares, de modo que
   configurar só um lado já isola o nó);
2. descarte aleatório com probabilidade ``prob_descarte``;
3. atraso ``atraso_fixo + U(0, jitter)``.

Tudo pode ser alterado em tempo de execução pelo comando administrativo
``ADM_FALHAS``. ``so_tipos`` restringe descarte/atraso a certos tipos de
mensagem (por exemplo só ``HEARTBEAT``, para medir falsas suspeitas). Mensagens
de/para clientes e administrador nunca são afetadas.

Reprodutibilidade: com a mesma semente a *sequência* de sorteios é a mesma, mas
como os nós são assíncronos a associação sorteio-mensagem pode variar entre
execuções; a distribuição das falhas, não cada evento, é reprodutível.
"""

import random


class InjetorFalhas:
    def __init__(self, semente=0):
        self.rng = random.Random(semente)
        self.semente = semente
        self.prob_descarte = 0.0
        self.atraso_fixo = 0.0
        self.jitter = 0.0
        self.bloqueados = set()
        self.so_tipos = None  # None = todos os tipos

    def configurar(self, prob_descarte=None, atraso_fixo=None, jitter=None,
                   bloqueados=None, so_tipos=None, semente=None, limpar=False):
        """Atualiza apenas os campos informados; ``limpar`` zera tudo antes."""
        if limpar:
            self.prob_descarte = self.atraso_fixo = self.jitter = 0.0
            self.bloqueados = set()
            self.so_tipos = None
        if semente is not None:
            self.semente = semente
            self.rng = random.Random(semente)
        if prob_descarte is not None:
            self.prob_descarte = float(prob_descarte)
        if atraso_fixo is not None:
            self.atraso_fixo = float(atraso_fixo)
        if jitter is not None:
            self.jitter = float(jitter)
        if bloqueados is not None:
            self.bloqueados = {int(b) for b in bloqueados}
        if so_tipos is not None:
            self.so_tipos = set(so_tipos) if so_tipos else None
        return self.estado()

    def bloqueado(self, par):
        return par in self.bloqueados

    def decidir(self, destino, tipo):
        """Devolve ``(causa_descarte | None, atraso_em_segundos)``."""
        if destino in self.bloqueados:
            return "particao", 0.0
        if self.so_tipos is not None and tipo not in self.so_tipos:
            return None, 0.0
        if self.prob_descarte > 0.0 and self.rng.random() < self.prob_descarte:
            return "descarte", 0.0
        atraso = self.atraso_fixo
        if self.jitter > 0.0:
            atraso += self.rng.uniform(0.0, self.jitter)
        return None, atraso

    def estado(self):
        return {"prob_descarte": self.prob_descarte, "atraso_fixo": self.atraso_fixo,
                "jitter": self.jitter, "bloqueados": sorted(self.bloqueados),
                "so_tipos": sorted(self.so_tipos) if self.so_tipos else None,
                "semente": self.semente}
