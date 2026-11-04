"""Estatística da campanha: média, desvio, IC de 95% (t de Student) e percentis.

Só biblioteca padrão (o IC usa t_{0.975, n-1} tabelado, como no Trabalho 1), de modo que os
testes de estatística não dependam de numpy. Percentis usam interpolação linear entre as
estatísticas de ordem (o mesmo método "linear" padrão do numpy e do Excel PERCENTIL.INC):

    posição = p/100 * (n-1);  valor = x[i] + (posição - i) * (x[i+1] - x[i])
"""

import math
from dataclasses import dataclass

#: t_{0.975, gl} (bicaudal, 95%). Para gl = 9 (10 réplicas): 2,262.
_T_CRITICO = {
    1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571,
    6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262, 10: 2.228,
    11: 2.201, 12: 2.179, 13: 2.160, 14: 2.145, 15: 2.131,
    16: 2.120, 17: 2.110, 18: 2.101, 19: 2.093, 20: 2.086,
    24: 2.064, 29: 2.045, 39: 2.023, 49: 2.010, 59: 2.001,
    99: 1.984,
}
_Z_CRITICO = 1.960


def t_critico(gl):
    """Valor crítico t de Student a 95% bicaudal (conservador entre pontos tabelados)."""
    if gl in _T_CRITICO:
        return _T_CRITICO[gl]
    if gl < 1:
        return float("nan")
    menores = [g for g in _T_CRITICO if g < gl]
    return _T_CRITICO[max(menores)] if menores else _Z_CRITICO


@dataclass
class Agregado:
    media: float
    desvio: float      # desvio padrão amostral (n-1)
    margem: float      # semi-largura do IC95
    n: int

    @property
    def inferior(self):
        return self.media - self.margem

    @property
    def superior(self):
        return self.media + self.margem


def agregar(valores):
    """Média, desvio padrão amostral e margem do IC95 (t de Student) de uma amostra."""
    amostra = [float(v) for v in valores]
    n = len(amostra)
    if n == 0:
        return Agregado(float("nan"), float("nan"), float("nan"), 0)
    media = sum(amostra) / n
    if n == 1:
        return Agregado(media, 0.0, float("nan"), 1)
    desvio = math.sqrt(sum((v - media) ** 2 for v in amostra) / (n - 1))
    return Agregado(media, desvio, t_critico(n - 1) * desvio / math.sqrt(n), n)


def percentil(valores, p):
    """Percentil ``p`` (0..100) por interpolação linear. NaN para amostra vazia."""
    xs = sorted(float(v) for v in valores)
    if not xs:
        return float("nan")
    if not 0 <= p <= 100:
        raise ValueError("p deve estar em [0, 100]")
    pos = p / 100.0 * (len(xs) - 1)
    i = int(math.floor(pos))
    if i >= len(xs) - 1:
        return xs[-1]
    return xs[i] + (pos - i) * (xs[i + 1] - xs[i])


def resumo_latencia(valores):
    """Média, desvio, mín., máx. e p50/p95/p99 de uma amostra de latências."""
    xs = [float(v) for v in valores]
    if not xs:
        nan = float("nan")
        return {"n": 0, "media": nan, "desvio": nan, "min": nan, "max": nan,
                "p50": nan, "p95": nan, "p99": nan}
    a = agregar(xs)
    return {"n": len(xs), "media": a.media, "desvio": a.desvio, "min": min(xs), "max": max(xs),
            "p50": percentil(xs, 50), "p95": percentil(xs, 95), "p99": percentil(xs, 99)}
