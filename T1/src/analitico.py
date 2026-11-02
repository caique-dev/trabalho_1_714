"""Modelo analítico do sistema — itens (a) a (f) do enunciado.

Dedução (resumo; o desenvolvimento completo vai no relatório)
------------------------------------------------------------
(a) Decomposição (splitting) de Poisson. Sob a política aleatória, cada
    chegada é marcada, de forma independente das demais, com o rótulo i
    (i = 1, 2, 3) com probabilidade p_i = 1/3. O teorema da decomposição diz
    que marcar independentemente um processo de Poisson de taxa `lam` gera
    subprocessos de Poisson de taxas p_i * lam, **mutuamente independentes**.
    Logo cada servidor recebe um Poisson de taxa lam_i = lam/3, e os três
    fluxos são independentes entre si.

(b) Cada servidor é então exatamente uma fila M/M/1: chegadas de Poisson de
    taxa lam/3, serviço exponencial de taxa mu, um servidor, fila FCFS
    ilimitada. Com rho = lam_i/mu = lam/(3*mu), a distribuição estacionária é

        p_k = (1 - rho) * rho^k,    k = 0, 1, 2, ...

    de onde saem, por soma direta das séries,

        U_i    = P(N_i > 0) = 1 - p_0 = rho
        E[N_i] = sum_k k*p_k          = rho / (1 - rho)
        E[R]   = E[N_i] / lam_i       = 1 / (mu - lam/3)      (Little no servidor)
        E[T_Q] = E[R] - 1/mu          = rho / (mu - lam/3)

    A condição de estabilidade é rho < 1, ou seja **lam < 3*mu**; em regime
    estável nada se acumula e a vazão é X = lam.

(c) X e U_i valem para as três políticas, não só para a aleatória. Por
    conservação de trabalho: toda requisição que chega acaba servida por
    algum servidor, então em regime estável X = lam para qualquer política.
    A utilização total segue de Little aplicada ao conjunto dos servidores:
    o número médio de requisições *em execução* é lam * E[S] = lam/mu, que
    dividido pelos 3 servidores dá U_i = lam/(3*mu) = rho. Nenhum dos dois
    argumentos usa como a carga foi repartida -- apenas que ela é toda
    servida --, logo independem da política.

(f) Para lam > 3*mu o sistema é instável: a taxa de chegada excede a
    capacidade agregada 3*mu, as filas crescem sem limite e não existe
    distribuição estacionária, de modo que as fórmulas acima deixam de valer
    (dariam valores negativos). O crescimento é descrito pela aproximação
    fluida N(t) ~= N(0) + (lam - 3*mu) * t, válida enquanto os servidores
    permanecem saturados.
"""

import math

#: Valores de lambda do experimento principal (requisições por u.t.).
LAMBDAS = [0.6, 1.2, 1.8, 2.4, 2.7]

#: Cenário adicional, instável, do item (f).
LAMBDA_INSTAVEL = 3.3


def estavel(lam, mu=1.0, n_servidores=3):
    """Condição de estabilidade do sistema: lam < n * mu."""
    return lam < n_servidores * mu


def rho(lam, mu=1.0, n_servidores=3):
    """Intensidade de tráfego por servidor: rho = lam / (n * mu)."""
    return lam / (n_servidores * mu)


def modelo(lam, mu=1.0, n_servidores=3):
    """Métricas analíticas do sistema sob a política de escolha aleatória.

    Devolve um dicionário com a taxa por servidor, rho, U_i, E[N_i], E[N],
    E[T_Q], E[R] e X. No caso instável (lam >= n*mu) as métricas que dependem
    do regime estacionário vêm como infinito ou NaN, e `estavel` vem False.
    """
    lam_i = lam / n_servidores          # taxa vista por cada servidor
    r = rho(lam, mu, n_servidores)
    est = estavel(lam, mu, n_servidores)

    if not est:
        return {
            "lam": lam,
            "lam_i": lam_i,
            "rho": r,
            "estavel": False,
            "utilizacao": 1.0,           # servidores saturados
            "num_servidor": math.inf,
            "num_sistema": math.inf,
            "tempo_fila": math.inf,
            "tempo_resposta": math.inf,
            "vazao": n_servidores * mu,  # limitada pela capacidade agregada
            "taxa_crescimento": lam - n_servidores * mu,  # inclinação fluida
        }

    e_n_i = r / (1 - r)                  # E[N_i] = rho / (1 - rho)
    e_r = 1.0 / (mu - lam_i)             # E[R]   = 1 / (mu - lam/3)
    return {
        "lam": lam,
        "lam_i": lam_i,
        "rho": r,
        "estavel": True,
        "utilizacao": r,                 # U_i = rho
        "num_servidor": e_n_i,
        "num_sistema": n_servidores * e_n_i,
        "tempo_fila": e_r - 1.0 / mu,    # E[T_Q] = E[R] - E[S]
        "tempo_resposta": e_r,
        "vazao": lam,                    # X = lam em regime estável
        "taxa_crescimento": 0.0,
    }


def tabela(lambdas=None, mu=1.0, n_servidores=3):
    """Tabela do item (b): métricas analíticas para cada lambda do experimento."""
    if lambdas is None:
        lambdas = LAMBDAS
    return [modelo(lam, mu, n_servidores) for lam in lambdas]


def aproximacao_fluida(t, lam, mu=1.0, n_servidores=3, n0=0.0):
    """Aproximação fluida do item (f): N(t) ~= N(0) + (lam - n*mu) * t.

    Enquanto os três servidores estão saturados, o sistema recebe `lam` e
    escoa `n*mu` por unidade de tempo; a diferença se acumula linearmente.
    """
    return n0 + (lam - n_servidores * mu) * t
