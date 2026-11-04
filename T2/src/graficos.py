"""Gráficos da campanha (mesmo estilo do Trabalho 1).

Largura de coluna IEEE (3,5 pol), PDF vetorial para o LaTeX e PNG para conferência. Paleta
Okabe-Ito (segura para daltonismo) com marcadores distintos como codificação secundária;
referências teóricas em cinza tracejado. Barras de erro = IC95% (t de Student) entre réplicas.
"""

import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker

from . import estatistica as est

AZUL, VERMELHAO, VERDE, LARANJA, CELESTE = "#0072B2", "#D55E00", "#009E73", "#E69F00", "#56B4E9"
CINZA, CINZA_CLARO = "#4a4a4a", "#cccccc"
LARGURA_COLUNA = 3.5

ROTULO_VARIANTE = {"natural": "natural (todos detectam)", "melhor_caso": "melhor caso (só N−1 detecta)",
                   "medio_caso": "caso médio (só o nó ⌈N/2⌉ detecta)",
                   "pior_caso": "pior caso (só o nó 1 detecta)"}
COR_VARIANTE = {"natural": AZUL, "melhor_caso": VERDE, "medio_caso": LARANJA, "pior_caso": VERMELHAO}
MARC_VARIANTE = {"natural": "o", "melhor_caso": "^", "medio_caso": "D", "pior_caso": "s"}
ORDEM_VARIANTES = ("pior_caso", "natural", "medio_caso", "melhor_caso")


def _estilo():
    plt.rcParams.update({
        "figure.dpi": 300, "savefig.dpi": 300, "font.size": 8, "axes.titlesize": 9,
        "axes.labelsize": 8, "legend.fontsize": 7, "xtick.labelsize": 7, "ytick.labelsize": 7,
        "axes.spines.top": False, "axes.spines.right": False, "axes.edgecolor": CINZA,
        "axes.linewidth": 0.6, "grid.color": CINZA_CLARO, "grid.linewidth": 0.5,
        "legend.frameon": False, "savefig.bbox": "tight",
    })


def _salvar(fig, saida, nome):
    caminhos = []
    for ext in ("pdf", "png"):
        caminho = os.path.join(saida, f"{nome}.{ext}")
        fig.savefig(caminho)
        caminhos.append(caminho)
    plt.close(fig)
    return caminhos


def serie(linhas, x, y, filtro=None, escala=1.0):
    """(xs, médias, margens IC95) da métrica ``y`` agrupada por ``x``."""
    xs, med, mar = [], [], []
    grupos = {}
    for r in linhas:
        if filtro and not all(r[k] == v for k, v in filtro.items()):
            continue
        if isinstance(r.get(y), (int, float)) and r[y] == r[y]:
            grupos.setdefault(r[x], []).append(r[y] * escala)
    for k in sorted(grupos):
        a = est.agregar(grupos[k])
        xs.append(k)
        med.append(a.media)
        mar.append(0.0 if a.margem != a.margem else a.margem)
    return xs, med, mar


def _erro(ax, xs, med, mar, cor, marc, rotulo, linha=True):
    ax.errorbar(xs, med, yerr=mar, color=cor, marker=marc, markersize=4.5,
                linewidth=1.4 if linha else 0, capsize=2.5, elinewidth=1.0,
                markeredgecolor="white", markeredgewidth=0.5, label=rotulo, zorder=3)


def _grade(ax):
    ax.grid(True, axis="y", alpha=0.6)
    ax.set_axisbelow(True)


# ----------------------------------------------------------------------
# E1
# ----------------------------------------------------------------------
def e1_mensagens(e1, saida):
    _estilo()
    fig, ax = plt.subplots(figsize=(LARGURA_COLUNA, 2.7))
    ns = sorted({r["n"] for r in e1})
    grade = list(range(min(ns), max(ns) + 1))
    ax.plot(grade, [n * (n - 1) for n in grade], color=VERMELHAO, linestyle="--", linewidth=1.0,
            label=r"teórico pior caso $N(N-1)$", zorder=1)
    ax.plot(grade, [n for n in grade], color=VERDE, linestyle="--", linewidth=1.0,
            label=r"teórico melhor caso $N$", zorder=1)
    ax.plot(ns, [(n - (n + 1) // 2) ** 2 + n - 1 for n in ns], color=LARANJA, linestyle=":",
            linewidth=1.0, label=r"teórico caso médio $(N-m)^2+N-1$", zorder=1)
    for v in ORDEM_VARIANTES:
        xs, med, mar = serie(e1, "n", "msg_total_env", {"variante": v})
        if xs:
            _erro(ax, xs, med, mar, COR_VARIANTE[v], MARC_VARIANTE[v], ROTULO_VARIANTE[v])
    ax.set_xlabel("Número de nós $N$")
    ax.set_ylabel("Mensagens de eleição enviadas")
    ax.set_xticks(ns)
    ax.set_ylim(bottom=0)
    _grade(ax)
    ax.legend(loc="upper left", fontsize=6)
    return _salvar(fig, saida, "e1_mensagens_vs_n")


def e1_tempos(e1, saida):
    _estilo()
    fig, axs = plt.subplots(1, 2, figsize=(2 * LARGURA_COLUNA, 2.5))
    for v in ORDEM_VARIANTES:
        xs, med, mar = serie(e1, "n", "t_eleicao_s", {"variante": v}, 1000)
        if xs:
            _erro(axs[0], xs, med, mar, COR_VARIANTE[v], MARC_VARIANTE[v], ROTULO_VARIANTE[v])
        xs, med, mar = serie(e1, "n", "t_ate_pronto_s", {"variante": v}, 1000)
        if xs:
            _erro(axs[1], xs, med, mar, COR_VARIANTE[v], MARC_VARIANTE[v], ROTULO_VARIANTE[v])
    axs[0].set_ylabel("Tempo de eleição (ms)\n1ª eleição iniciada → COORDENADOR")
    axs[1].set_ylabel("Crash → líder pronto (ms)")
    for ax in axs:
        ax.set_xlabel("Número de nós $N$")
        ax.set_xticks(sorted({r["n"] for r in e1}))
        ax.set_ylim(bottom=0)
        _grade(ax)
    axs[0].legend(loc="upper left")
    return _salvar(fig, saida, "e1_tempos_vs_n")


def e1_decomposicao(e1, saida):
    """Barras empilhadas: detecção, eleição e sincronização (variante natural)."""
    _estilo()
    fig, ax = plt.subplots(figsize=(LARGURA_COLUNA, 2.5))
    ns = sorted({r["n"] for r in e1 if r["variante"] == "natural"})
    det = [serie(e1, "n", "t_deteccao_s", {"variante": "natural"}, 1000)[1][i] for i in range(len(ns))]
    # tempo entre detecção e início da eleição é ~0; o anúncio vem após t_eleicao_s
    ele = [serie(e1, "n", "t_eleicao_s", {"variante": "natural"}, 1000)[1][i] for i in range(len(ns))]
    syn = [serie(e1, "n", "t_sync_s", {"variante": "natural"}, 1000)[1][i] for i in range(len(ns))]
    pos = list(range(len(ns)))
    ax.bar(pos, det, color=CELESTE, label="detecção (crash → 1ª suspeita)")
    ax.bar(pos, ele, bottom=det, color=AZUL, label="eleição (→ COORDENADOR)")
    ax.bar(pos, syn, bottom=[a + b for a, b in zip(det, ele)], color=LARANJA,
           label="sincronização (→ líder pronto)")
    ax.set_xticks(pos)
    ax.set_xticklabels(ns)
    ax.set_xlabel("Número de nós $N$")
    ax.set_ylabel("Tempo (ms)")
    _grade(ax)
    ax.legend(loc="upper left", fontsize=6)
    ax.set_ylim(0, max(a + b + c for a, b, c in zip(det, ele, syn)) * 1.45)
    return _salvar(fig, saida, "e1_decomposicao")


# ----------------------------------------------------------------------
# E2
# ----------------------------------------------------------------------
def _latencias(e2, eixo, filtro, saida, nome, xlabel):
    _estilo()
    fig, ax = plt.subplots(figsize=(LARGURA_COLUNA, 2.5))
    for m, cor, marc, rot in (("lat_p50_ms", AZUL, "o", "p50"), ("lat_p95_ms", LARANJA, "s", "p95"),
                              ("lat_p99_ms", VERMELHAO, "^", "p99")):
        xs, med, mar = serie(e2, eixo, m, filtro)
        _erro(ax, xs, med, mar, cor, marc, rot)
    ax.set_xlabel(xlabel)
    ax.set_ylabel("Latência de escrita (ms)")
    ax.set_xticks(xs)
    ax.set_ylim(bottom=0)
    _grade(ax)
    ax.legend(loc="upper left")
    return _salvar(fig, saida, nome)


def e2_latencia_n(e2, saida):
    return _latencias(e2, "n", {"atraso_ms": 0}, saida, "e2_latencia_vs_n", "Número de nós $N$")


def e2_latencia_atraso(e2, saida):
    return _latencias(e2, "atraso_ms", {"n": 5}, saida, "e2_latencia_vs_atraso",
                      "Atraso injetado por mensagem (ms), $N=5$")


def e2_vazao(e2, saida):
    _estilo()
    fig, axs = plt.subplots(1, 2, figsize=(2 * LARGURA_COLUNA, 2.4))
    xs, med, mar = serie(e2, "n", "vazao_ops_s", {"atraso_ms": 0})
    _erro(axs[0], xs, med, mar, AZUL, "o", "medida")
    axs[0].set_xlabel("Número de nós $N$")
    axs[0].set_xticks(xs)
    xs, med, mar = serie(e2, "atraso_ms", "vazao_ops_s", {"n": 5})
    _erro(axs[1], xs, med, mar, AZUL, "o", "medida")
    axs[1].set_xlabel("Atraso injetado por mensagem (ms), $N=5$")
    axs[1].set_xticks(xs)
    axs[0].set_ylabel("Vazão (escritas/s, 1 cliente)")
    for ax in axs:
        ax.set_ylim(bottom=0)
        _grade(ax)
    return _salvar(fig, saida, "e2_vazao")


def e2_mensagens(e2, saida):
    _estilo()
    fig, ax = plt.subplots(figsize=(LARGURA_COLUNA, 2.4))
    xs, med, mar = serie(e2, "n", "msg_por_escrita", {"atraso_ms": 0})
    ax.plot(xs, [2 * (n - 1) for n in xs], color=CINZA, linestyle="--", linewidth=1.2,
            label=r"teórico $2(N-1)$", zorder=1)
    _erro(ax, xs, med, mar, AZUL, "o", "REPLICAR + ACK por escrita")
    ax.set_xlabel("Número de nós $N$")
    ax.set_ylabel("Mensagens por escrita")
    ax.set_xticks(xs)
    ax.set_ylim(bottom=0)
    _grade(ax)
    ax.legend(loc="upper left")
    return _salvar(fig, saida, "e2_mensagens_por_escrita")


# ----------------------------------------------------------------------
# E3
# ----------------------------------------------------------------------
def e3a_janela(e3a, saida):
    _estilo()
    fig, axs = plt.subplots(1, 2, figsize=(2 * LARGURA_COLUNA, 2.5))
    xs, med, mar = serie(e3a, "timeout_fd_s", "janela_s", None, 1000)
    ax = axs[0]
    ax.plot(xs, [x * 1000 for x in xs], color=CINZA, linestyle="--", linewidth=1.0,
            label=r"$T_{fd}$ (timeout)", zorder=1)
    ax.plot(xs, [x * 1000 * 1.2 for x in xs], color=CINZA, linestyle=":", linewidth=1.0,
            label=r"$T_{fd}+\Delta_{hb}$", zorder=1)
    _erro(ax, xs, med, mar, AZUL, "o", "janela média (IC95)")
    for m, cor, marc, rot in (("janela_s", LARANJA, "s", "p95"),):
        grupos = {}
        for r in e3a:
            grupos.setdefault(r["timeout_fd_s"], []).append(r[m] * 1000)
        ax.plot(sorted(grupos), [est.percentil(grupos[k], 95) for k in sorted(grupos)], color=cor,
                marker=marc, markersize=4.5, linewidth=0, label=rot, zorder=4,
                markeredgecolor="white", markeredgewidth=0.5)
    ax.set_xlabel("Timeout do detector $T_{fd}$ (s)")
    ax.set_ylabel("Janela de indisponibilidade (ms)")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xticks(xs)
    ax.set_xticklabels([f"{x:g}" for x in xs])
    ax.xaxis.set_minor_formatter(matplotlib.ticker.NullFormatter())
    ax.legend(loc="upper left")
    _grade(ax)

    ax = axs[1]
    det = serie(e3a, "timeout_fd_s", "t_deteccao_s", None, 1000)[1]
    ele = serie(e3a, "timeout_fd_s", "t_eleicao_s", None, 1000)[1]
    syn = serie(e3a, "timeout_fd_s", "t_sync_s", None, 1000)[1]
    jan = serie(e3a, "timeout_fd_s", "janela_s", None, 1000)[1]
    pos = list(range(len(xs)))
    ax.bar(pos, det, color=CELESTE, label="detecção")
    ax.bar(pos, ele, bottom=det, color=AZUL, label="eleição")
    ax.bar(pos, syn, bottom=[a + b for a, b in zip(det, ele)], color=LARANJA, label="sincronização")
    resto = [j - (a + b + c) for j, a, b, c in zip(jan, det, ele, syn)]
    ax.bar(pos, resto, bottom=[a + b + c for a, b, c in zip(det, ele, syn)], color=CINZA_CLARO,
           label="cliente (espera/retentativa)")
    ax.set_xticks(pos)
    ax.set_xticklabels([f"{x:g}" for x in xs])
    ax.set_xlabel("Timeout do detector $T_{fd}$ (s)")
    ax.set_ylabel("Tempo médio (ms)")
    ax.legend(loc="upper left", fontsize=6)
    _grade(ax)
    return _salvar(fig, saida, "e3a_janela_vs_timeout")


def e3b_falsas(e3b, saida):
    _estilo()
    fig, axs = plt.subplots(1, 2, figsize=(2 * LARGURA_COLUNA, 2.5))
    pontos = lambda y: serie(e3b, "perda", y)  # noqa: E731
    xs, med, mar = pontos("susp_detector_por_min")
    _erro(axs[0], xs, med, mar, AZUL, "o", "suspeitas do detector (heartbeat/ACK)")
    axs[0].set_ylabel("Falsas suspeitas por minuto")
    xs, med, mar = pontos("eleicoes_espurias_por_min")
    _erro(axs[1], xs, med, mar, VERMELHAO, "s", "eleições espúrias")
    axs[1].set_ylabel("Eleições espúrias por minuto")
    for ax in axs:
        ax.set_xlabel("Probabilidade de perda de mensagem")
        ax.set_xticks(xs)
        ax.set_ylim(bottom=0)
        _grade(ax)
    axs[0].legend(loc="upper left")
    return _salvar(fig, saida, "e3b_falsas_suspeitas_vs_perda")


def gerar_todos(dados, saida):
    """Gera todos os gráficos possíveis com os dados disponíveis."""
    arquivos = []
    if dados.get("e1"):
        arquivos += e1_mensagens(dados["e1"], saida) + e1_tempos(dados["e1"], saida)
        arquivos += e1_decomposicao(dados["e1"], saida)
    if dados.get("e2"):
        arquivos += e2_latencia_n(dados["e2"], saida) + e2_latencia_atraso(dados["e2"], saida)
        arquivos += e2_vazao(dados["e2"], saida) + e2_mensagens(dados["e2"], saida)
    if dados.get("e3a"):
        arquivos += e3a_janela(dados["e3a"], saida)
    if dados.get("e3b"):
        arquivos += e3b_falsas(dados["e3b"], saida)
    return arquivos
