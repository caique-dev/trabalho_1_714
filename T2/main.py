"""MC714 — Trabalho 2: KV store replicado tolerante a falhas. Interface de linha de comando.

Exemplos
--------
    python main.py cluster --n 5 --dir execucao        # sobe 5 nós locais
    python main.py cliente --pares 1=127.0.0.1:9001,... put chave valor
    python main.py admin --pares ... status
    python main.py verificar --logs execucao/logs --dados execucao/dados
    python main.py no --id 1 --pares 1=no1:9000,2=no2:9000,3=no3:9000   (modo contêiner)
"""

import argparse
import asyncio
import json
import os
import signal
import socket
import sys
import time

# O console do Windows costuma abrir em cp1252, o que embaralha os acentos das
# mensagens. Reconfigurar a saída para UTF-8 resolve sem exigir `chcp` do
# usuário; em plataformas onde já é UTF-8 a chamada é inócua.
for _fluxo in (sys.stdout, sys.stderr):
    if hasattr(_fluxo, "reconfigure"):
        try:
            _fluxo.reconfigure(encoding="utf-8", errors="replace")
        except (ValueError, OSError):  # fluxo redirecionado ou não reconfigurável
            pass

from src import cliente as cli  # noqa: E402
from src import verificador  # noqa: E402
from src.config import Parametros, parse_pares  # noqa: E402


def _env(nome, padrao=None):
    return os.environ.get(nome, padrao)


def _flutuante(nome, padrao):
    return float(_env(nome, padrao))


# ----------------------------------------------------------------------
# no: um nó (processo local ou contêiner)
# ----------------------------------------------------------------------
def cmd_no(args):
    from src.no import No
    if not args.pares:
        print("erro: informe --pares ou a variável PARES", file=sys.stderr)
        return 2
    enderecos = parse_pares(args.pares)
    if args.id not in enderecos:
        print(f"erro: id {args.id} não está na lista de pares", file=sys.stderr)
        return 2
    params = Parametros(intervalo_hb=args.heartbeat, timeout_fd=args.timeout_fd,
                        timeout_rpc=args.timeout_rpc, timeout_coord=args.timeout_coord,
                        quorum=args.quorum, fsync=args.fsync, semente=args.semente)
    if args.porta:
        enderecos[args.id] = (enderecos[args.id][0], args.porta)
    no = No(args.id, enderecos, args.dados, args.logs, params, host=args.host)

    async def principal():
        loop = asyncio.get_running_loop()
        for sinal in (signal.SIGINT, signal.SIGTERM):  # encerramento limpo (contêiner)
            try:
                loop.add_signal_handler(sinal, no.parando.set)
            except (NotImplementedError, RuntimeError, ValueError):
                pass  # Windows: o crash é Popen.kill(); Ctrl+C cai em KeyboardInterrupt
        loop.set_exception_handler(lambda _l, _c: None)  # ruído de ConnectionReset ao fechar
        await no.executar()

    if sys.platform == "win32":
        # Resolução do temporizador do Windows: sem isso asyncio.sleep(0.005) dura ~16 ms
        # (granularidade de 15,6 ms), o que distorce timeouts curtos e o atraso injetado.
        try:
            import ctypes
            ctypes.windll.winmm.timeBeginPeriod(1)
        except (OSError, AttributeError):
            pass
        # O loop Selector evita ruído de ConnectionResetError do Proactor ao fechar sockets.
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(principal())
    except KeyboardInterrupt:
        pass
    return 0


# ----------------------------------------------------------------------
# cluster: orquestra N nós locais
# ----------------------------------------------------------------------
def cmd_cluster(args):
    from src.cluster import Cluster
    params = Parametros(intervalo_hb=args.heartbeat, timeout_fd=args.timeout_fd,
                        timeout_rpc=args.timeout_rpc, quorum=args.quorum, semente=args.semente)
    portas = None
    if args.porta_base:
        portas = [args.porta_base + i for i in range(args.n)]
    c = Cluster(args.n, diretorio=args.dir, params=params, portas=portas)
    try:
        c.iniciar()
        lider = c.aguardar_lider()
        print(f"Cluster com {args.n} nós em {c.diretorio}")
        pares = ",".join(f"{i}={h}:{p}" for i, (h, p) in sorted(c.enderecos.items()))
        print(f"--pares {pares}")
        # flush: a linha --pares precisa aparecer mesmo com a saída redirecionada.
        print(f"Líder eleito: nó {lider}. Logs em {c.dir_logs}; Ctrl+C encerra "
              "(ou encerra sozinho quando todos os nós iniciados por ele morrerem).", flush=True)
        while c.vivos():
            time.sleep(1.0)
        print("Todos os nós iniciados por este orquestrador terminaram; encerrando.", flush=True)
    except KeyboardInterrupt:
        pass
    finally:
        c.parar()
    return 0


# ----------------------------------------------------------------------
# cliente e admin
# ----------------------------------------------------------------------
def cmd_cliente(args):
    c = cli.Cliente(parse_pares(args.pares), timeout=args.timeout, prazo=args.prazo)
    if args.operacao == "put":
        r = c.put(args.chave, args.valor)
    else:
        r = c.get(args.chave)
    print(json.dumps({"ok": r.ok, "erro": r.erro, "no": r.no, "tentativas": r.tentativas,
                      "latencia_ms": round(r.latencia * 1000, 3), "dados": r.dados},
                     ensure_ascii=False))
    return 0 if r.ok else 1


def cmd_admin(args):
    enderecos = parse_pares(args.pares)
    if args.acao == "status":
        alvos = [args.no] if args.no else sorted(enderecos)
        for i in alvos:
            s = cli.status(enderecos[i], incluir_dados=args.dados)
            if not s:
                print(f"no {i}: sem resposta")
                continue
            print(f"no {i}: {cli.resumo_status(s)}")
            print(f"  {json.dumps(s, ensure_ascii=False)}")
    elif args.acao == "falhas":
        r = cli.configurar_falhas(
            enderecos[args.no], prob_descarte=args.descarte, atraso_fixo=args.atraso,
            jitter=args.jitter, bloqueados=args.bloquear, so_tipos=args.so_tipos,
            semente=args.semente_falhas, limpar=args.limpar)
        print(json.dumps(r, ensure_ascii=False))
    elif args.acao == "matar":
        # Crash do processo (kill): no Windows os.kill(SIGTERM) equivale a TerminateProcess.
        s = cli.status(enderecos[args.no])
        if not s:
            print("nó sem resposta")
            return 1
        # O pid só vale na máquina onde o nó roda: contra um nó em contêiner ele
        # apontaria para outro processo do host. Recusa e indica o comando certo.
        if s.get("host") not in (None, socket.gethostname()):
            print(f"nó {args.no} roda em outro host ({s['host']}); 'matar' só serve para "
                  f"nós locais. Em contêiner, use: docker compose kill no{args.no}")
            return 1
        os.kill(s["pid"], signal.SIGTERM)
        print(f"nó {args.no} (pid {s['pid']}) encerrado abruptamente")
    return 0


def cmd_verificar(args):
    eventos = verificador.carregar_eventos(args.logs)
    estados = None
    if args.dados:
        from src.persistencia import ler_estado_disco
        vivos = [int(x) for x in args.vivos.split(",")] if args.vivos else None
        estados = {}
        for nome in sorted(os.listdir(args.dados)):
            if nome.startswith("no") and nome[2:].isdigit():
                i = int(nome[2:])
                if vivos is None or i in vivos:
                    estados[i] = ler_estado_disco(os.path.join(args.dados, nome))
    res = verificador.verificar(eventos, estados)
    print(verificador.formatar(res))
    return 0 if res.ok else 1


def cmd_experimentos(args):
    from src import experimentos
    so = args.so.split(",") if args.so else None
    # O modo rápido grava à parte por padrão: os resultados versionados da
    # campanha completa (entregáveis) não devem ser sobrescritos por um smoke test.
    if args.saida is None:
        args.saida = "resultados_rapido" if args.rapido else "resultados"
    if args.regerar:
        experimentos.agregar_e_plotar(args.saida)
        return 0
    experimentos.executar(saida=args.saida, rapido=args.rapido, so=so, replicas=args.replicas)
    return 0


def construir_parser():
    parser = argparse.ArgumentParser(
        prog="main.py", description="KV store replicado tolerante a falhas — MC714 Trabalho 2")
    sub = parser.add_subparsers(dest="comando", required=True)

    def temporizacao(p, env=False):
        p.add_argument("--heartbeat", type=float, default=_flutuante("HEARTBEAT", 0.5),
                       help="intervalo do heartbeat em s (padrão: 0.5; env HEARTBEAT)")
        p.add_argument("--timeout-fd", type=float, default=_flutuante("TIMEOUT_FD", 2.0),
                       help="silêncio que gera suspeita, em s (padrão: 2.0; env TIMEOUT_FD)")
        p.add_argument("--timeout-rpc", type=float, default=_flutuante("TIMEOUT_RPC", 1.0),
                       help="prazo de cada chamada, em s (padrão: 1.0; env TIMEOUT_RPC)")
        p.add_argument("--quorum", type=int, default=int(_env("QUORUM", 0)) or None,
                       help="nº mínimo de nós para confirmar escrita (padrão: maioria; env QUORUM)")
        p.add_argument("--semente", type=int, default=int(_env("SEMENTE", 0)),
                       help="semente da injeção de falhas (padrão: 0; env SEMENTE)")

    p = sub.add_parser("no", help="sobe um nó (processo local ou contêiner)")
    p.add_argument("--id", type=int, default=int(_env("NO_ID", 0)) or None, required=_env("NO_ID") is None,
                   help="id do nó, 1..N (env NO_ID)")
    p.add_argument("--pares", default=_env("PARES"),
                   help="lista id=host:porta separada por vírgulas (env PARES)")
    p.add_argument("--host", default=_env("NO_HOST", "127.0.0.1"),
                   help="endereço de escuta (padrão: 127.0.0.1; contêiner: 0.0.0.0; env NO_HOST)")
    p.add_argument("--porta", type=int, default=int(_env("PORTA", 0)) or None,
                   help="porta de escuta, se diferente da lista de pares (env PORTA)")
    p.add_argument("--dados", default=_env("DIR_DADOS", "dados"),
                   help="diretório de dados persistentes (env DIR_DADOS)")
    p.add_argument("--logs", default=_env("DIR_LOGS", "logs"),
                   help="diretório dos logs JSONL (env DIR_LOGS)")
    p.add_argument("--timeout-coord", type=float,
                   default=float(_env("TIMEOUT_COORD", 0)) or None,
                   help="espera pelo COORDENADOR, em s (padrão: 4 x timeout-rpc)")
    p.add_argument("--fsync", action="store_true", default=_env("FSYNC") == "1",
                   help="fsync a cada escrita (env FSYNC=1)")
    temporizacao(p)
    p.set_defaults(func=cmd_no)

    p = sub.add_parser("cluster", help="sobe N nós como processos locais")
    p.add_argument("--n", type=int, default=5, help="número de nós (padrão: 5)")
    p.add_argument("--dir", default="execucao", help="diretório da execução (dados e logs)")
    p.add_argument("--porta-base", type=int, default=None,
                   help="primeira porta (padrão: portas livres sorteadas)")
    temporizacao(p)
    p.set_defaults(func=cmd_cluster)

    p = sub.add_parser("cliente", help="put/get no cluster (com redirecionamento)")
    p.add_argument("--pares", required=True, help="lista id=host:porta")
    p.add_argument("--timeout", type=float, default=1.0)
    p.add_argument("--prazo", type=float, default=10.0)
    p.add_argument("operacao", choices=["put", "get"])
    p.add_argument("chave")
    p.add_argument("valor", nargs="?")
    p.set_defaults(func=cmd_cliente)

    p = sub.add_parser("admin", help="status, injeção de falhas e crash de um nó")
    p.add_argument("--pares", required=True, help="lista id=host:porta")
    p.add_argument("acao", choices=["status", "falhas", "matar"])
    p.add_argument("--no", type=int, help="nó alvo")
    p.add_argument("--dados", action="store_true", help="status: inclui o conteúdo do dicionário")
    p.add_argument("--descarte", type=float, help="falhas: probabilidade de descarte")
    p.add_argument("--atraso", type=float, help="falhas: atraso fixo (s)")
    p.add_argument("--jitter", type=float, help="falhas: jitter uniforme (s)")
    p.add_argument("--bloquear", type=int, nargs="*", help="falhas: pares bloqueados (partição)")
    p.add_argument("--so-tipos", nargs="*", help="falhas: restringe a estes tipos de mensagem")
    p.add_argument("--semente-falhas", type=int, help="falhas: reinicia o gerador com esta semente")
    p.add_argument("--limpar", action="store_true", help="falhas: remove todas as falhas")
    p.set_defaults(func=cmd_admin)

    p = sub.add_parser("verificar", help="verifica as invariantes a partir dos logs")
    p.add_argument("--logs", required=True, help="diretório com os no*.jsonl")
    p.add_argument("--dados", help="diretório de dados dos nós (estado final em disco)")
    p.add_argument("--vivos", help="ids dos nós vivos, ex.: 1,2,3 (padrão: todos)")
    p.set_defaults(func=cmd_verificar)

    p = sub.add_parser("experimentos", help="campanha experimental (fase 2)")
    p.add_argument("--rapido", action="store_true", help="poucas réplicas e janelas curtas (smoke test)")
    p.add_argument("--so", help="subconjunto, ex.: E1,E3 (E1, E2, E3 = E3a+E3b, E4; padrão: todos)")
    p.add_argument("--saida", default=None,
                   help="diretório de saída (padrão: resultados; com --rapido: resultados_rapido)")
    p.add_argument("--replicas", type=int, help="réplicas por configuração (padrão: 10; --rapido: 2)")
    p.add_argument("--regerar", action="store_true",
                   help="só refaz agregados e gráficos a partir de <saida>/brutos (não mede nada)")
    p.set_defaults(func=cmd_experimentos)
    return parser


def main(argv=None):
    args = construir_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
