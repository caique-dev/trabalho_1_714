"""Campanha experimental do enunciado.

Experimento principal
    5 valores de lambda x 3 políticas = 15 configurações, cada uma com 10
    réplicas de sementes distintas, 5000 u.t. de duração e 500 u.t. de
    aquecimento descartadas.

Experimento adicional
    lambda = 3,3 (> 3*mu), instável: acompanha N(t) ao longo do tempo e
    compara com a aproximação fluida do item (f).

Todas as réplicas de um mesmo lambda compartilham o mesmo conjunto de
sementes entre as três políticas, o que aplica números aleatórios comuns: as
políticas são comparadas sobre exatamente a mesma carga de trabalho.
"""

import csv
import os

from . import analitico
from .estatistica import agregar
from .politicas import ROTULOS
from .simulador import simular

#: Réplicas por configuração, conforme o enunciado.
N_REPLICAS = 10
DURACAO = 5000.0
WARMUP = 500.0
MU = 1.0
N_SERVIDORES = 3

#: Ordem em que as políticas aparecem nas tabelas e gráficos.
POLITICAS = ["aleatoria", "round_robin", "fila_curta"]


def sementes(lam, n_replicas=N_REPLICAS):
    """Sementes de uma configuração, iguais para as três políticas.

    Derivar a semente de lambda (e não da política) é o que garante que as
    três políticas processem a mesma carga de trabalho em cada réplica.
    """
    base = int(round(lam * 1000))
    return [base * 100 + r for r in range(n_replicas)]


def executar_configuracao(lam, politica, n_replicas=N_REPLICAS, duracao=DURACAO,
                          warmup=WARMUP, mu=MU, verboso=True):
    """Roda as `n_replicas` réplicas de uma configuração e agrega as métricas."""
    resultados = []
    for semente in sementes(lam, n_replicas):
        resultados.append(
            simular(
                lam, politica, mu=mu, n_servidores=N_SERVIDORES,
                duracao=duracao, warmup=warmup, semente=semente,
            )
        )

    agregados = {
        "vazao": agregar([r.vazao for r in resultados]),
        "tempo_resposta": agregar([r.tempo_resposta for r in resultados]),
        "tempo_fila": agregar([r.tempo_fila for r in resultados]),
        "num_sistema": agregar([r.num_sistema for r in resultados]),
    }
    # Utilização de cada servidor, agregada réplica a réplica.
    for i in range(N_SERVIDORES):
        agregados[f"utilizacao_{i + 1}"] = agregar([r.utilizacoes[i] for r in resultados])
    # Fração das requisições encaminhada a cada servidor: mostra a dinâmica de
    # distribuição de carga exigida no enunciado.
    for i in range(N_SERVIDORES):
        agregados[f"fracao_{i + 1}"] = agregar(
            [r.distribuicao[i] / sum(r.distribuicao) for r in resultados]
        )

    config = {
        "lam": lam,
        "politica": politica,
        "n_replicas": n_replicas,
        "agregados": agregados,
        "resultados": resultados,
    }
    if verboso:
        er = agregados["tempo_resposta"]
        x = agregados["vazao"]
        print(
            f"  lambda={lam:<4} {ROTULOS[politica]:<16} "
            f"E[R]={er.media:6.4f} ± {er.margem:.4f}   "
            f"X={x.media:6.4f}   E[N]={agregados['num_sistema'].media:7.4f}"
        )
    return config


def campanha(lambdas=None, politicas=None, n_replicas=N_REPLICAS,
             duracao=DURACAO, warmup=WARMUP, mu=MU, verboso=True):
    """Executa as 15 configurações do experimento principal."""
    lambdas = analitico.LAMBDAS if lambdas is None else lambdas
    politicas = POLITICAS if politicas is None else politicas

    configuracoes = []
    for lam in lambdas:
        if verboso:
            print(f"\nlambda = {lam} (rho = {analitico.rho(lam, mu):.3f})")
        for politica in politicas:
            configuracoes.append(
                executar_configuracao(
                    lam, politica, n_replicas=n_replicas, duracao=duracao,
                    warmup=warmup, mu=mu, verboso=verboso,
                )
            )
    return configuracoes


def experimento_instavel(lam=analitico.LAMBDA_INSTAVEL, politicas=None,
                         duracao=DURACAO, warmup=0.0, mu=MU, semente=3300,
                         verboso=True):
    """Item (f): lambda > 3*mu, com a série N(t) de cada política.

    Aqui o warm-up é zero de propósito: o objetivo não é medir um regime
    estacionário (que não existe), e sim observar o crescimento de N(t) desde
    o sistema vazio, para comparar com a reta da aproximação fluida.
    """
    politicas = POLITICAS if politicas is None else politicas
    saida = []
    for politica in politicas:
        resultado = simular(
            lam, politica, mu=mu, n_servidores=N_SERVIDORES, duracao=duracao,
            warmup=warmup, semente=semente, coletar_trace=True,
        )
        saida.append(resultado)
        if verboso:
            previsto = analitico.aproximacao_fluida(duracao, lam, mu, N_SERVIDORES)
            print(
                f"  {ROTULOS[politica]:<16} N(final)={resultado.n_final:6d}   "
                f"fluida={previsto:7.1f}   X={resultado.vazao:.4f} "
                f"(capacidade={N_SERVIDORES * mu:.1f})"
            )
    return saida


# ----------------------------------------------------------------------
# Escrita dos resultados
# ----------------------------------------------------------------------

def salvar_metricas(configuracoes, caminho):
    """CSV com média e IC de 95% de cada métrica, por configuração."""
    metricas = ["vazao", "tempo_resposta", "tempo_fila", "num_sistema"]
    metricas += [f"utilizacao_{i + 1}" for i in range(N_SERVIDORES)]
    metricas += [f"fracao_{i + 1}" for i in range(N_SERVIDORES)]

    cabecalho = ["lambda", "politica", "n_replicas"]
    for m in metricas:
        cabecalho += [f"{m}_media", f"{m}_ic95", f"{m}_desvio"]

    with open(caminho, "w", newline="", encoding="utf-8") as f:
        escritor = csv.writer(f)
        escritor.writerow(cabecalho)
        for cfg in configuracoes:
            linha = [cfg["lam"], cfg["politica"], cfg["n_replicas"]]
            for m in metricas:
                a = cfg["agregados"][m]
                linha += [f"{a.media:.6f}", f"{a.margem:.6f}", f"{a.desvio:.6f}"]
            escritor.writerow(linha)
    return caminho


def salvar_comparacao(configuracoes, caminho, mu=MU):
    """CSV da comparação analítico x simulação (itens (c), (d) e (e)).

    Para cada configuração traz: E[R] analítico da política aleatória, E[R]
    simulado com IC, o ganho percentual em relação à aleatória, e a
    verificação da Lei de Little (E[N] contra X * E[R]).
    """
    # E[R] simulado da política aleatória, referência do ganho percentual.
    referencia = {
        cfg["lam"]: cfg["agregados"]["tempo_resposta"].media
        for cfg in configuracoes if cfg["politica"] == "aleatoria"
    }

    with open(caminho, "w", newline="", encoding="utf-8") as f:
        escritor = csv.writer(f)
        escritor.writerow([
            "lambda", "politica", "rho",
            "ER_analitico", "ER_simulado", "ER_ic95", "erro_relativo_pct",
            "ganho_vs_aleatoria_pct",
            "X_analitico", "X_simulado",
            "U_analitico", "U_simulado_medio",
            "EN_simulado", "little_X_vezes_ER", "little_erro_pct",
        ])
        for cfg in configuracoes:
            lam = cfg["lam"]
            ag = cfg["agregados"]
            mod = analitico.modelo(lam, mu, N_SERVIDORES)

            er_sim = ag["tempo_resposta"].media
            er_ana = mod["tempo_resposta"]
            erro_rel = 100.0 * (er_sim - er_ana) / er_ana

            base = referencia.get(lam)
            ganho = 100.0 * (base - er_sim) / base if base else float("nan")

            u_sim = sum(
                ag[f"utilizacao_{i + 1}"].media for i in range(N_SERVIDORES)
            ) / N_SERVIDORES

            # Lei de Little: E[N] deve bater com X * E[R].
            little = ag["vazao"].media * er_sim
            en_sim = ag["num_sistema"].media
            little_erro = 100.0 * (en_sim - little) / little

            escritor.writerow([
                lam, cfg["politica"], f"{mod['rho']:.4f}",
                f"{er_ana:.6f}", f"{er_sim:.6f}", f"{ag['tempo_resposta'].margem:.6f}",
                f"{erro_rel:+.2f}", f"{ganho:+.2f}",
                f"{mod['vazao']:.6f}", f"{ag['vazao'].media:.6f}",
                f"{mod['utilizacao']:.6f}", f"{u_sim:.6f}",
                f"{en_sim:.6f}", f"{little:.6f}", f"{little_erro:+.2f}",
            ])
    return caminho


def salvar_analitico(caminho, lambdas=None, mu=MU):
    """CSV da tabela analítica do item (b)."""
    lambdas = analitico.LAMBDAS if lambdas is None else lambdas
    with open(caminho, "w", newline="", encoding="utf-8") as f:
        escritor = csv.writer(f)
        escritor.writerow([
            "lambda", "lambda_i", "rho", "estavel", "U_i",
            "E_Ni", "E_N", "E_TQ", "E_R", "X",
        ])
        for lam in lambdas:
            m = analitico.modelo(lam, mu, N_SERVIDORES)
            escritor.writerow([
                lam, f"{m['lam_i']:.6f}", f"{m['rho']:.6f}", m["estavel"],
                f"{m['utilizacao']:.6f}", f"{m['num_servidor']:.6f}",
                f"{m['num_sistema']:.6f}", f"{m['tempo_fila']:.6f}",
                f"{m['tempo_resposta']:.6f}", f"{m['vazao']:.6f}",
            ])
    return caminho


def salvar_trace_instavel(resultados, caminho, passo=5.0):
    """CSV de N(t) no cenário instável, reamostrado em passos regulares.

    O trace bruto tem um ponto por evento (centenas de milhares deles); para
    o gráfico basta uma amostra a cada `passo` unidades de tempo.
    """
    series = {}
    for r in resultados:
        amostras = []
        alvo = 0.0
        ultimo_n = 0
        for t, n in r.trace:
            while t >= alvo:
                amostras.append((alvo, ultimo_n))
                alvo += passo
            ultimo_n = n
        series[r.politica] = amostras

    n_pontos = min(len(v) for v in series.values())
    politicas = list(series)
    with open(caminho, "w", newline="", encoding="utf-8") as f:
        escritor = csv.writer(f)
        escritor.writerow(["t"] + [f"N_{p}" for p in politicas] + ["N_fluida"])
        for k in range(n_pontos):
            t = series[politicas[0]][k][0]
            fluida = analitico.aproximacao_fluida(
                t, resultados[0].lam, resultados[0].mu, N_SERVIDORES
            )
            escritor.writerow(
                [f"{t:.1f}"] + [series[p][k][1] for p in politicas] + [f"{fluida:.2f}"]
            )
    return caminho


def executar_tudo(saida="resultados", n_replicas=N_REPLICAS, duracao=DURACAO,
                  warmup=WARMUP, mu=MU, gerar_graficos=True):
    """Roda a campanha inteira e grava tabelas e gráficos em `saida`."""
    os.makedirs(saida, exist_ok=True)

    print("=" * 72)
    print("Experimento principal: 5 valores de lambda x 3 políticas x "
          f"{n_replicas} réplicas")
    print(f"duração={duracao:.0f} u.t., warm-up={warmup:.0f} u.t., mu={mu}")
    print("=" * 72)
    configuracoes = campanha(
        n_replicas=n_replicas, duracao=duracao, warmup=warmup, mu=mu
    )

    print("\n" + "=" * 72)
    print(f"Experimento adicional: lambda = {analitico.LAMBDA_INSTAVEL} (instável)")
    print("=" * 72)
    instaveis = experimento_instavel(duracao=duracao, mu=mu)

    arquivos = [
        salvar_analitico(os.path.join(saida, "analitico.csv"), mu=mu),
        salvar_metricas(configuracoes, os.path.join(saida, "metricas.csv")),
        salvar_comparacao(configuracoes, os.path.join(saida, "comparacao.csv"), mu=mu),
        salvar_trace_instavel(instaveis, os.path.join(saida, "instavel_Nt.csv")),
    ]

    if gerar_graficos:
        from . import graficos
        arquivos += graficos.gerar_todos(configuracoes, instaveis, saida, mu=mu)

    print("\nArquivos gerados:")
    for caminho in arquivos:
        print(f"  {caminho}")
    return configuracoes, instaveis
