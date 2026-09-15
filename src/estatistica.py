"""Agregação das réplicas: média e intervalo de confiança de 95%.

O enunciado pede 10 réplicas por configuração, reportando a média e o
intervalo de confiança de 95%. Com n pequeno e variância desconhecida, o
intervalo correto usa a distribuição t de Student com n-1 graus de liberdade:

    IC_95 = média +- t_{0.975, n-1} * s / sqrt(n)

Os valores críticos são tabelados aqui em vez de virem do SciPy: a única
coisa que o SciPy acrescentaria a este projeto seria esta constante, e o
enunciado pede que o código rode sem atrito em Windows ou Linux.
"""

import math
from dataclasses import dataclass

#: t_{0.975, gl} para os graus de liberdade de interesse (bicaudal, 95%).
#: Para gl = 9 (as 10 réplicas do enunciado), t = 2.262.
_T_CRITICO = {
    1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571,
    6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262, 10: 2.228,
    11: 2.201, 12: 2.179, 13: 2.160, 14: 2.145, 15: 2.131,
    16: 2.120, 17: 2.110, 18: 2.101, 19: 2.093, 20: 2.086,
    24: 2.064, 29: 2.045, 39: 2.023, 49: 2.010, 59: 2.001,
    99: 1.984,
}

#: Limite normal, usado quando os graus de liberdade passam da tabela.
_Z_CRITICO = 1.960


def t_critico(gl):
    """Valor crítico t de Student a 95% bicaudal para `gl` graus de liberdade."""
    if gl in _T_CRITICO:
        return _T_CRITICO[gl]
    if gl < 1:
        return float("nan")
    # Entre pontos tabelados, usa o gl tabelado imediatamente menor, o que é
    # conservador (t decresce com gl); acima da tabela, converge para o normal.
    menores = [g for g in _T_CRITICO if g < gl]
    return _T_CRITICO[max(menores)] if menores else _Z_CRITICO


@dataclass
class Agregado:
    """Resumo de uma métrica sobre as réplicas de uma configuração."""

    media: float
    desvio: float          # desvio padrão amostral (denominador n-1)
    margem: float          # semi-largura do IC de 95%
    n: int

    @property
    def inferior(self):
        return self.media - self.margem

    @property
    def superior(self):
        return self.media + self.margem

    def __str__(self):
        return f"{self.media:.4f} ± {self.margem:.4f}"


def agregar(valores):
    """Média, desvio padrão amostral e margem do IC de 95% de uma amostra."""
    amostra = [float(v) for v in valores]
    n = len(amostra)
    if n == 0:
        return Agregado(float("nan"), float("nan"), float("nan"), 0)

    media = sum(amostra) / n
    if n == 1:
        return Agregado(media, 0.0, float("nan"), 1)

    # Variância amostral (denominador n-1), estimador não enviesado.
    variancia = sum((v - media) ** 2 for v in amostra) / (n - 1)
    desvio = math.sqrt(variancia)
    margem = t_critico(n - 1) * desvio / math.sqrt(n)
    return Agregado(media, desvio, margem, n)
