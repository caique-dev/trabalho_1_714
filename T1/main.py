"""MC714 — Trabalho 1: balanceador de carga. Interface de linha de comando.

Exemplos
--------
    python main.py simular --politica fila_curta --lam 1.8 --log-intervalo 250
    python main.py analitico
    python main.py experimentos --saida resultados
"""

import argparse
import sys

# O console do Windows costuma abrir em cp1252, o que embaralha os acentos das
# mensagens. Reconfigurar a saída para UTF-8 resolve sem exigir `chcp` do
# usuário; em plataformas onde já é UTF-8 a chamada é inócua.
for _fluxo in (sys.stdout, sys.stderr):
    if hasattr(_fluxo, "reconfigure"):
        try:
            _fluxo.reconfigure(encoding="utf-8", errors="replace")
        except (ValueError, OSError):  # fluxo redirecionado ou não reconfigurável
            pass

from src import analitico, experimentos  # noqa: E402
from src.politicas import POLITICAS, ROTULOS  # noqa: E402
from src.simulador import simular  # noqa: E402


def cmd_simular(args):
    """Roda uma única execução e imprime as métricas da janela de medição."""
    resultado = simular(
        args.lam, args.politica,
        mu=args.mu,
        n_servidores=args.servidores,
        duracao=args.duracao,
        warmup=args.warmup,
        semente=args.semente,
        log_intervalo=args.log_intervalo,
    )

    modelo = analitico.modelo(args.lam, args.mu, args.servidores)
    total = sum(resultado.distribuicao)

    print()
    print("=" * 60)
    print(f"Política: {ROTULOS[args.politica]}   lambda={args.lam}   mu={args.mu}")
    print(f"Duração: {args.duracao:.0f} u.t. (warm-up {args.warmup:.0f} u.t. "
          f"descartadas)   semente={args.semente}")
    print(f"rho = lambda/(n*mu) = {modelo['rho']:.4f}   "
          f"sistema {'estável' if modelo['estavel'] else 'INSTÁVEL'}")
    print("=" * 60)
    print(f"Chegadas geradas .............. {resultado.chegadas_total}")
    print(f"Concluídas na janela .......... {resultado.concluidas_janela}")
    print(f"N ao final da execução ........ {resultado.n_final}")
    print()
    print("Métricas na janela de medição:")
    print(f"  X    (vazão) ................ {resultado.vazao:.4f} req./u.t.")
    print(f"  E[R] (tempo de resposta) .... {resultado.tempo_resposta:.4f} u.t.")
    print(f"  E[T_Q] (espera em fila) ..... {resultado.tempo_fila:.4f} u.t.")
    print(f"  E[N] (no sistema) ........... {resultado.num_sistema:.4f}")
    print()
    print("Distribuição de carga por servidor:")
    for i, (recebidas, u) in enumerate(zip(resultado.distribuicao,
                                           resultado.utilizacoes), start=1):
        print(f"  Servidor {i}: {recebidas:6d} req. ({100.0 * recebidas / total:5.2f}%)   "
              f"U_{i} = {u:.4f}")

    if modelo["estavel"]:
        print()
        print("Comparação com o modelo M/M/1 (válido para a política aleatória):")
        print(f"  E[R] analítico .............. {modelo['tempo_resposta']:.4f} u.t.")
        print(f"  E[N] analítico .............. {modelo['num_sistema']:.4f}")
        print(f"  U_i  analítico .............. {modelo['utilizacao']:.4f}")
        print(f"  X    analítico .............. {modelo['vazao']:.4f} req./u.t.")
        # Lei de Little, item (e): E[N] deve bater com X * E[R].
        little = resultado.vazao * resultado.tempo_resposta
        erro = 100.0 * (resultado.num_sistema - little) / little
        print(f"  Lei de Little: X*E[R] = {little:.4f} vs E[N] = "
              f"{resultado.num_sistema:.4f}  ({erro:+.2f}%)")
    return 0


def cmd_analitico(args):
    """Imprime a tabela analítica do item (b)."""
    print()
    print(f"Modelo analítico M/M/1 por servidor (mu={args.mu}, "
          f"{args.servidores} servidores)")
    print(f"Condição de estabilidade: lambda < n*mu = {args.servidores * args.mu:.1f}")
    print()
    cab = f"{'lambda':>7} {'lambda_i':>9} {'rho':>7} {'U_i':>7} {'E[N_i]':>9} " \
          f"{'E[N]':>9} {'E[T_Q]':>9} {'E[R]':>9} {'X':>7}"
    print(cab)
    print("-" * len(cab))
    for lam in analitico.LAMBDAS + [analitico.LAMBDA_INSTAVEL]:
        m = analitico.modelo(lam, args.mu, args.servidores)
        if not m["estavel"]:
            print(f"{lam:>7} {m['lam_i']:>9.4f} {m['rho']:>7.4f} "
                  f"{'--':>7} {'inf':>9} {'inf':>9} {'inf':>9} {'inf':>9} "
                  f"{m['vazao']:>7.4f}   (instável)")
            continue
        print(f"{lam:>7} {m['lam_i']:>9.4f} {m['rho']:>7.4f} {m['utilizacao']:>7.4f} "
              f"{m['num_servidor']:>9.4f} {m['num_sistema']:>9.4f} "
              f"{m['tempo_fila']:>9.4f} {m['tempo_resposta']:>9.4f} {m['vazao']:>7.4f}")
    return 0


def cmd_experimentos(args):
    """Roda a campanha completa e grava tabelas e gráficos."""
    experimentos.executar_tudo(
        saida=args.saida,
        n_replicas=args.replicas,
        duracao=args.duracao,
        warmup=args.warmup,
        mu=args.mu,
        gerar_graficos=not args.sem_graficos,
    )
    return 0


def construir_parser():
    parser = argparse.ArgumentParser(
        prog="main.py",
        description="Simulador de balanceador de carga — MC714 Trabalho 1",
    )
    sub = parser.add_subparsers(dest="comando", required=True)

    # Opções comuns aos subcomandos.
    comum = argparse.ArgumentParser(add_help=False)
    comum.add_argument("--mu", type=float, default=1.0,
                       help="taxa de serviço de cada servidor (padrão: 1.0)")
    comum.add_argument("--servidores", type=int, default=3,
                       help="número de servidores (padrão: 3)")
    comum.add_argument("--duracao", type=float, default=5000.0,
                       help="duração da execução em u.t. (padrão: 5000)")
    comum.add_argument("--warmup", type=float, default=500.0,
                       help="período de aquecimento descartado (padrão: 500)")

    p_sim = sub.add_parser("simular", parents=[comum],
                           help="executa uma única simulação")
    p_sim.add_argument("--lam", "--lambda", dest="lam", type=float, required=True,
                       help="taxa de chegada lambda (req./u.t.)")
    p_sim.add_argument("--politica", choices=sorted(POLITICAS), required=True,
                       help="política de balanceamento")
    p_sim.add_argument("--semente", type=int, default=42,
                       help="semente do gerador aleatório (padrão: 42)")
    p_sim.add_argument("--log-intervalo", type=float, default=None,
                       help="imprime a ocupação das filas a cada N u.t.")
    p_sim.set_defaults(func=cmd_simular)

    p_ana = sub.add_parser("analitico", parents=[comum],
                           help="tabela do modelo analítico M/M/1")
    p_ana.set_defaults(func=cmd_analitico)

    p_exp = sub.add_parser("experimentos", parents=[comum],
                           help="campanha completa: 15 configurações x réplicas")
    p_exp.add_argument("--saida", default="resultados",
                       help="diretório de saída (padrão: resultados)")
    p_exp.add_argument("--replicas", type=int, default=10,
                       help="réplicas por configuração (padrão: 10)")
    p_exp.add_argument("--sem-graficos", action="store_true",
                       help="gera apenas os CSVs, sem as figuras")
    p_exp.set_defaults(func=cmd_experimentos)

    return parser


def main(argv=None):
    args = construir_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
