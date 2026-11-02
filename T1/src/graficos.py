"""Gráficos do relatório.

Dimensionados para o formato IEEE de coluna dupla (largura de coluna de
3,5 pol), salvos em PDF vetorial para inclusão no LaTeX e em PNG para
inspeção rápida.

Sobre as cores: a paleta das três políticas é a trinca azul / vermelhão /
verde-azulado de Okabe-Ito, segura para daltonismo (separação mínima entre
pares adjacentes de dE 11,0 em deuteranopia). Como reforço -- e porque
relatórios costumam ser impressos em tons de cinza --, cada política também
recebe um marcador de forma distinta, de modo que a identidade nunca depende
só da cor. A curva analítica não é uma quarta série colorida: é uma
referência, e por isso vai em cinza tracejado.
"""

import os

import matplotlib
matplotlib.use("Agg")  # backend sem interface gráfica: não exige display
import matplotlib.pyplot as plt

from . import analitico
from .politicas import ROTULOS

#: Paleta categórica (Okabe-Ito), na ordem fixa das políticas.
CORES = {
    "aleatoria": "#0072B2",     # azul
    "round_robin": "#D55E00",   # vermelhão
    "fila_curta": "#009E73",    # verde-azulado
}

#: Codificação secundária: forma do marcador, legível também em cinza.
MARCADORES = {
    "aleatoria": "o",
    "round_robin": "s",
    "fila_curta": "^",
}

#: Cinza de referência para a curva analítica e para os eixos.
CINZA = "#4a4a4a"
CINZA_CLARO = "#cccccc"

LARGURA_COLUNA = 3.5  # polegadas, coluna simples do formato IEEE


def _estilo():
    """Estilo enxuto: tipografia pequena, grade recessiva, sem molduras."""
    plt.rcParams.update({
        "figure.dpi": 300,
        "savefig.dpi": 300,
        "font.size": 8,
        "axes.titlesize": 9,
        "axes.labelsize": 8,
        "legend.fontsize": 7,
        "xtick.labelsize": 7,
        "ytick.labelsize": 7,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.edgecolor": CINZA,
        "axes.linewidth": 0.6,
        "grid.color": CINZA_CLARO,
        "grid.linewidth": 0.5,
        "legend.frameon": False,
        "savefig.bbox": "tight",
    })


def _salvar(fig, saida, nome):
    """Salva a figura em PDF (para o LaTeX) e PNG (para conferência)."""
    caminhos = []
    for ext in ("pdf", "png"):
        caminho = os.path.join(saida, f"{nome}.{ext}")
        fig.savefig(caminho)
        caminhos.append(caminho)
    plt.close(fig)
    return caminhos


def _por_politica(configuracoes):
    """Reorganiza as configurações em {política: [cfg ordenada por lambda]}."""
    agrupado = {}
    for cfg in configuracoes:
        agrupado.setdefault(cfg["politica"], []).append(cfg)
    for lista in agrupado.values():
        lista.sort(key=lambda c: c["lam"])
    return agrupado


def grafico_tempo_resposta(configuracoes, saida, mu=1.0, n_servidores=3):
    """E[R] analítico (política aleatória) contra os pontos simulados.

    É o gráfico central pedido na seção "Comparação" do enunciado: a curva
    analítica com os pontos das três políticas e barras de erro de 95%.
    """
    _estilo()
    fig, ax = plt.subplots(figsize=(LARGURA_COLUNA, 2.7))

    # Curva analítica contínua E[R] = 1 / (mu - lambda/3). A malha para no
    # maior lambda do experimento: perto da saturação (lambda -> 3*mu) a curva
    # dispara para o infinito e, se desenhada até lá, comprimiria toda a
    # região de interesse contra o eixo.
    lam_max = max(c["lam"] for c in configuracoes)
    passos = 200
    grade = [0.1 + (lam_max - 0.1) * k / passos for k in range(passos + 1)]
    ax.plot(
        grade, [analitico.modelo(l, mu, n_servidores)["tempo_resposta"] for l in grade],
        color=CINZA, linewidth=1.4, linestyle="--", zorder=1,
        label=r"Analítico M/M/1: $1/(\mu-\lambda/3)$",
    )

    for politica, cfgs in _por_politica(configuracoes).items():
        lambdas = [c["lam"] for c in cfgs]
        medias = [c["agregados"]["tempo_resposta"].media for c in cfgs]
        margens = [c["agregados"]["tempo_resposta"].margem for c in cfgs]
        ax.errorbar(
            lambdas, medias, yerr=margens,
            color=CORES[politica], marker=MARCADORES[politica],
            markersize=4.5, linewidth=1.4, capsize=2.5, elinewidth=1.0,
            markeredgecolor="white", markeredgewidth=0.5,  # anel de 2px na sobreposição
            label=ROTULOS[politica], zorder=3,
        )

    ax.set_xlabel(r"Taxa de chegada $\lambda$ (req./u.t.)")
    ax.set_ylabel(r"Tempo médio de resposta $E[R]$ (u.t.)")
    ax.set_ylim(bottom=0)
    ax.grid(True, axis="y", alpha=0.6)
    ax.set_axisbelow(True)
    ax.legend(loc="upper left")
    return _salvar(fig, saida, "er_vs_lambda")


def grafico_utilizacao(configuracoes, saida, mu=1.0, n_servidores=3):
    """Item (c): U_i simulado bate com rho = lambda/(3mu) nas três políticas."""
    _estilo()
    fig, ax = plt.subplots(figsize=(LARGURA_COLUNA, 2.4))

    limite = n_servidores * mu
    grade = [0.1 + (limite * 0.97 - 0.1) * k / 200 for k in range(201)]
    ax.plot(
        grade, [analitico.rho(l, mu, n_servidores) for l in grade],
        color=CINZA, linewidth=1.4, linestyle="--", zorder=1,
        label=r"Analítico: $\rho=\lambda/3\mu$",
    )

    for politica, cfgs in _por_politica(configuracoes).items():
        lambdas = [c["lam"] for c in cfgs]
        # Média das utilizações dos 3 servidores, com o IC da média entre réplicas.
        medias, margens = [], []
        for c in cfgs:
            us = [c["agregados"][f"utilizacao_{i + 1}"] for i in range(n_servidores)]
            medias.append(sum(u.media for u in us) / n_servidores)
            margens.append(max(u.margem for u in us))
        ax.errorbar(
            lambdas, medias, yerr=margens,
            color=CORES[politica], marker=MARCADORES[politica],
            markersize=4.5, linewidth=0, capsize=2.5, elinewidth=1.0,
            markeredgecolor="white", markeredgewidth=0.5,
            label=ROTULOS[politica], zorder=3,
        )

    ax.set_xlabel(r"Taxa de chegada $\lambda$ (req./u.t.)")
    ax.set_ylabel(r"Utilização média $U_i$")
    ax.set_ylim(0, 1)
    ax.grid(True, axis="y", alpha=0.6)
    ax.set_axisbelow(True)
    ax.legend(loc="upper left")
    return _salvar(fig, saida, "utilizacao_vs_lambda")


def grafico_instavel(resultados, saida, mu=1.0, n_servidores=3, passo=5.0):
    """Item (f): N(t) no cenário instável contra a aproximação fluida."""
    _estilo()
    fig, ax = plt.subplots(figsize=(LARGURA_COLUNA, 2.5))

    lam = resultados[0].lam
    duracao = resultados[0].duracao

    # Reta fluida N(t) ~= N(0) + (lambda - 3mu) t.
    ax.plot(
        [0, duracao],
        [0, analitico.aproximacao_fluida(duracao, lam, mu, n_servidores)],
        color=CINZA, linewidth=1.4, linestyle="--", zorder=1,
        label=r"Fluida: $(\lambda-3\mu)\,t$",
    )

    for r in resultados:
        # Reamostra o trace (um ponto por evento) em passos regulares.
        ts, ns, alvo, ultimo = [], [], 0.0, 0
        for t, n in r.trace:
            while t >= alvo:
                ts.append(alvo)
                ns.append(ultimo)
                alvo += passo
            ultimo = n
        ax.plot(
            ts, ns, color=CORES[r.politica], linewidth=1.2,
            label=ROTULOS[r.politica], zorder=3, alpha=0.9,
        )

    ax.set_xlabel("Tempo (u.t.)")
    ax.set_ylabel(r"Requisições no sistema $N(t)$")
    ax.set_title(rf"Regime instável: $\lambda={lam}$ > $3\mu={n_servidores * mu:.0f}$")
    ax.grid(True, axis="y", alpha=0.6)
    ax.set_axisbelow(True)
    ax.legend(loc="upper left")
    return _salvar(fig, saida, "instavel_Nt")


def gerar_todos(configuracoes, instaveis, saida, mu=1.0, n_servidores=3):
    """Gera as três figuras e devolve os caminhos dos arquivos criados."""
    caminhos = []
    caminhos += grafico_tempo_resposta(configuracoes, saida, mu, n_servidores)
    caminhos += grafico_utilizacao(configuracoes, saida, mu, n_servidores)
    caminhos += grafico_instavel(instaveis, saida, mu, n_servidores)
    return caminhos
