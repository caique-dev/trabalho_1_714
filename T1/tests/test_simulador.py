"""Testes de sanidade do simulador.

Incluem o cheque sugerido no enunciado (com lambda pequeno, E[R] -> E[S] = 1)
e as invariantes que o modelo analítico prevê.
"""

import math
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src import analitico                      # noqa: E402
from src.estatistica import agregar            # noqa: E402
from src.politicas import criar_politica       # noqa: E402
from src.simulador import simular              # noqa: E402

POLITICAS = ["aleatoria", "round_robin", "fila_curta"]


# ----------------------------------------------------------------------
# Políticas, isoladamente
# ----------------------------------------------------------------------

def test_round_robin_e_ciclico():
    import random
    p = criar_politica("round_robin", 3, random.Random(0))
    escolhas = [p.escolher([0, 0, 0]) for _ in range(7)]
    assert escolhas == [0, 1, 2, 0, 1, 2, 0]


def test_fila_curta_escolhe_o_menor():
    import random
    p = criar_politica("fila_curta", 3, random.Random(0))
    assert p.escolher([5, 2, 9]) == 1
    assert p.escolher([0, 4, 4]) == 0


def test_fila_curta_sorteia_empates():
    import random
    p = criar_politica("fila_curta", 3, random.Random(0))
    # Com os três empatados, todos os servidores devem ser escolhidos alguma vez.
    escolhas = {p.escolher([2, 2, 2]) for _ in range(200)}
    assert escolhas == {0, 1, 2}


def test_aleatoria_distribui_um_terco():
    import random
    p = criar_politica("aleatoria", 3, random.Random(7))
    contagem = [0, 0, 0]
    for _ in range(30000):
        contagem[p.escolher([0, 0, 0])] += 1
    for c in contagem:
        assert abs(c / 30000 - 1 / 3) < 0.02


# ----------------------------------------------------------------------
# Cheques de sanidade do simulador
# ----------------------------------------------------------------------

@pytest.mark.parametrize("politica", POLITICAS)
def test_carga_baixa_tende_ao_tempo_de_servico(politica):
    """Cheque do enunciado: com lambda pequeno, E[R] -> E[S] = 1 u.t."""
    r = simular(0.05, politica, duracao=20000, warmup=500, semente=1)
    assert r.tempo_resposta == pytest.approx(1.0, abs=0.08)
    # Praticamente sem espera em fila.
    assert r.tempo_fila < 0.05


@pytest.mark.parametrize("politica", POLITICAS)
def test_conservacao_de_requisicoes(politica):
    """Nada some: chegadas = concluídas + as que ficaram no sistema."""
    r = simular(1.8, politica, duracao=2000, warmup=0.0, semente=2)
    total_por_servidor = sum(r.distribuicao)
    assert total_por_servidor == r.chegadas_total
    assert r.concluidas_janela + r.n_final == r.chegadas_total


@pytest.mark.parametrize("politica", POLITICAS)
def test_vazao_iguala_lambda(politica):
    """Item (c): em regime estável X = lambda para qualquer política."""
    r = simular(1.8, politica, duracao=20000, warmup=2000, semente=3)
    assert r.vazao == pytest.approx(1.8, rel=0.05)


@pytest.mark.parametrize("politica", POLITICAS)
def test_utilizacao_iguala_rho(politica):
    """Item (c): U_i = rho = lambda/(3mu) para qualquer política."""
    lam = 1.8
    r = simular(lam, politica, duracao=20000, warmup=2000, semente=4)
    esperado = analitico.rho(lam)
    for u in r.utilizacoes:
        assert u == pytest.approx(esperado, abs=0.03)


@pytest.mark.parametrize("politica", POLITICAS)
def test_lei_de_little(politica):
    """Item (e): E[N] = X * E[R] nos dados de simulação."""
    r = simular(2.4, politica, duracao=30000, warmup=3000, semente=5)
    assert r.num_sistema == pytest.approx(r.vazao * r.tempo_resposta, rel=0.02)


def test_aleatoria_bate_com_o_modelo_mm1():
    """Item (b)/(d): a política aleatória reproduz E[R] = 1/(mu - lambda/3)."""
    lam = 1.8
    amostras = [
        simular(lam, "aleatoria", duracao=20000, warmup=2000, semente=s).tempo_resposta
        for s in range(8)
    ]
    ag = agregar(amostras)
    esperado = analitico.modelo(lam)["tempo_resposta"]
    # O valor analítico deve cair dentro do IC de 95% das réplicas.
    assert ag.inferior <= esperado <= ag.superior


def test_ordenacao_das_politicas():
    """Item (d): E[R] fila curta <= round robin <= aleatória."""
    lam = 2.4
    medias = {}
    for politica in POLITICAS:
        amostras = [
            simular(lam, politica, duracao=20000, warmup=2000, semente=s).tempo_resposta
            for s in range(8)
        ]
        medias[politica] = sum(amostras) / len(amostras)
    assert medias["fila_curta"] <= medias["round_robin"] <= medias["aleatoria"]


def test_round_robin_distribui_igualmente():
    """Round Robin entrega a cada servidor a mesma quantidade (±1)."""
    r = simular(1.8, "round_robin", duracao=3000, warmup=0.0, semente=6)
    assert max(r.distribuicao) - min(r.distribuicao) <= 1


# ----------------------------------------------------------------------
# Regime instável, item (f)
# ----------------------------------------------------------------------

def test_instavel_cresce_conforme_a_aproximacao_fluida():
    """Com lambda=3,3 > 3mu, N(t) cresce à taxa (lambda - 3mu)."""
    lam, duracao = 3.3, 5000.0
    r = simular(lam, "fila_curta", duracao=duracao, warmup=0.0, semente=7)
    previsto = analitico.aproximacao_fluida(duracao, lam)
    assert r.n_final == pytest.approx(previsto, rel=0.25)
    # A vazão satura na capacidade agregada, 3*mu = 3.
    assert r.vazao == pytest.approx(3.0, rel=0.05)
    assert r.vazao < lam


def test_instavel_tem_metricas_analiticas_infinitas():
    m = analitico.modelo(3.3)
    assert not m["estavel"]
    assert math.isinf(m["tempo_resposta"])
    assert m["vazao"] == pytest.approx(3.0)


# ----------------------------------------------------------------------
# Reprodutibilidade
# ----------------------------------------------------------------------

def test_mesma_semente_mesmo_resultado():
    a = simular(1.8, "fila_curta", duracao=2000, semente=99)
    b = simular(1.8, "fila_curta", duracao=2000, semente=99)
    assert a.tempo_resposta == b.tempo_resposta
    assert a.distribuicao == b.distribuicao


def test_politicas_veem_a_mesma_carga():
    """Números aleatórios comuns: mesma semente, mesmas chegadas."""
    resultados = [
        simular(1.8, p, duracao=2000, warmup=0.0, semente=123) for p in POLITICAS
    ]
    # Todas as políticas processam exatamente o mesmo número de chegadas.
    assert len({r.chegadas_total for r in resultados}) == 1


def test_warmup_invalido():
    with pytest.raises(ValueError):
        simular(1.0, "aleatoria", duracao=100, warmup=100)
