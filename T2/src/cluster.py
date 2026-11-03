"""Orquestrador local: sobe N nós como processos independentes em localhost.

É o equivalente, sem Docker, do ``compose.yaml``: cada nó é um ``python main.py
no ...`` com a lista de pares em ``127.0.0.1:porta``. Crash = ``Popen.kill()``
(``TerminateProcess`` no Windows, ``SIGKILL`` no POSIX): o processo morre sem
rodar nenhum código de encerramento, como uma queda real. Recuperação =
reiniciar o mesmo comando com o mesmo diretório de dados.

A classe é a interface que os testes e a campanha experimental (fase 2)
usam: sobe/mata/reinicia nós, consulta status (contadores por tipo de
mensagem, líder, época, visão, seq), configura falhas e particiona a rede.
Eventos da orquestração (início, kill) vão para ``logs/cluster.jsonl`` com
timestamp, para que se possa medir, por exemplo, o tempo de failover a partir do
instante do kill.
"""

import os
import socket
import subprocess
import sys
import tempfile
import time

from . import cliente
from .config import Parametros, formatar_pares
from .logs import LogEventos

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def portas_livres(n, host="127.0.0.1"):
    """Reserva ``n`` portas livres (abre todos os sockets antes de fechar, para serem distintas)."""
    socks = []
    try:
        for _ in range(n):
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind((host, 0))
            socks.append(s)
        return [s.getsockname()[1] for s in socks]
    finally:
        for s in socks:
            s.close()


class Cluster:
    def __init__(self, n=5, diretorio=None, params=None, host="127.0.0.1", portas=None):
        self.n = n
        self.params = params or Parametros()
        self.host = host
        self.diretorio = os.path.abspath(diretorio or tempfile.mkdtemp(prefix="mc714_t2_"))
        self.dir_dados = os.path.join(self.diretorio, "dados")
        self.dir_logs = os.path.join(self.diretorio, "logs")
        os.makedirs(self.dir_dados, exist_ok=True)
        os.makedirs(self.dir_logs, exist_ok=True)
        portas = portas or portas_livres(n, host)
        self.enderecos = {i + 1: (host, portas[i]) for i in range(n)}
        self.procs = {}
        self._saidas = {}
        self.log = LogEventos(os.path.join(self.dir_logs, "cluster.jsonl"), "cluster")

    # ------------------------------------------------------------------
    # Ciclo de vida
    # ------------------------------------------------------------------
    def comando_no(self, i):
        p = self.params
        cmd = [sys.executable, os.path.join(RAIZ, "main.py"), "no",
               "--id", str(i), "--pares", formatar_pares(self.enderecos),
               "--host", self.host, "--dados", self.dir_dados, "--logs", self.dir_logs,
               "--heartbeat", str(p.intervalo_hb), "--timeout-fd", str(p.timeout_fd),
               "--timeout-rpc", str(p.timeout_rpc), "--timeout-coord", str(p.timeout_coord),
               "--semente", str(p.semente)]
        if p.quorum:
            cmd += ["--quorum", str(p.quorum)]
        if p.fsync:
            cmd.append("--fsync")
        return cmd

    def iniciar_no(self, i):
        if i in self.procs and self.procs[i].poll() is None:
            return
        saida = open(os.path.join(self.dir_logs, f"no{i}.out"), "ab")
        self._saidas[i] = saida
        env = dict(os.environ, PYTHONUNBUFFERED="1", PYTHONIOENCODING="utf-8")
        self.procs[i] = subprocess.Popen(self.comando_no(i), stdout=saida, stderr=subprocess.STDOUT,
                                         cwd=RAIZ, env=env)
        self.log("no_iniciado", alvo=i, pid=self.procs[i].pid, porta=self.enderecos[i][1])

    def iniciar(self, esperar=True, timeout=15.0):
        for i in self.enderecos:
            self.iniciar_no(i)
        if esperar:
            self.aguardar(lambda: all(self.status(i) is not None for i in self.enderecos),
                          timeout, "todos os nós respondendo")
        return self

    def matar(self, i):
        """Crash do nó ``i`` (kill imediato, sem encerramento limpo)."""
        proc = self.procs.get(i)
        if proc is None or proc.poll() is not None:
            return
        self.log("no_morto", alvo=i, pid=proc.pid)
        proc.kill()
        proc.wait(timeout=10)
        self._saidas[i].close()

    def reiniciar(self, i):
        """Recuperação do nó ``i``: mesmo diretório de dados, nova incarnação."""
        self.matar(i)
        self.iniciar_no(i)
        self.aguardar(lambda: self.status(i) is not None, 15.0, f"nó {i} respondendo")

    def vivo(self, i):
        p = self.procs.get(i)
        return p is not None and p.poll() is None

    def vivos(self):
        return [i for i in sorted(self.enderecos) if self.vivo(i)]

    def parar(self):
        for i in list(self.procs):
            try:
                self.matar(i)
            except Exception:  # noqa: BLE001  limpeza nunca deve falhar o teste
                pass
        self.log.fechar()

    def __enter__(self):
        return self.iniciar()

    def __exit__(self, *exc):
        self.parar()

    # ------------------------------------------------------------------
    # Observação
    # ------------------------------------------------------------------
    def status(self, i, incluir_dados=False, timeout=0.5):
        if not self.vivo(i):
            return None
        return cliente.status(self.enderecos[i], incluir_dados, timeout)

    def status_vivos(self, incluir_dados=False):
        s = {i: self.status(i, incluir_dados) for i in self.vivos()}
        return {i: v for i, v in s.items() if v is not None}

    def aguardar(self, condicao, timeout=15.0, descricao="condição", intervalo=0.05):
        limite = time.monotonic() + timeout
        while time.monotonic() < limite:
            try:
                if condicao():
                    return True
            except Exception:  # noqa: BLE001
                pass
            time.sleep(intervalo)
        raise TimeoutError(f"tempo esgotado aguardando: {descricao}")

    def lider_consensual(self, ids=None):
        """Id do líder se todos os nós vivos (ou ``ids``) concordam e ele está pronto; senão None."""
        ids = list(ids) if ids is not None else self.vivos()
        st = {i: self.status(i) for i in ids}
        if not ids or any(s is None for s in st.values()):
            return None
        lideres = {s["lider"] for s in st.values()}
        epocas = {s["epoca"] for s in st.values()}
        if len(lideres) != 1 or len(epocas) != 1:
            return None
        lider = lideres.pop()
        if lider is None or lider not in st:
            return None
        s = st[lider]
        if s["papel"] != "lider" or not s["pronto"]:
            return None
        return lider

    def aguardar_lider(self, esperado=None, timeout=20.0, ids=None):
        """Espera todos concordarem em um líder pronto (opcionalmente ``esperado``)."""
        achado = {}

        def _ok():
            lider = self.lider_consensual(ids)
            if lider is None or (esperado is not None and lider != esperado):
                return False
            achado["lider"] = lider
            return True
        self.aguardar(_ok, timeout, f"líder pronto (esperado={esperado})")
        return achado["lider"]

    def aguardar_convergencia(self, timeout=20.0):
        """Espera todos os nós vivos terem o mesmo (ult_epoca, seq, hash) e estarem na visão."""
        def _ok():
            st = self.status_vivos()
            if len(st) != len(self.vivos()) or not st:
                return False
            chaves = {(s["ult_epoca"], s["seq"], s["hash"]) for s in st.values()}
            if len(chaves) != 1:
                return False
            lider = self.lider_consensual()
            if lider is None:
                return False
            return set(st[lider]["visao"]) | {lider} == set(st)
        self.aguardar(_ok, timeout, "réplicas convergidas e na visão")

    def cliente(self, **kw):
        return cliente.Cliente(self.enderecos, **kw)

    # ------------------------------------------------------------------
    # Falhas de rede (injeção no nível da aplicação)
    # ------------------------------------------------------------------
    def falhas(self, i, **config):
        return cliente.configurar_falhas(self.enderecos[i], **config)

    def isolar(self, i):
        """Isola o nó ``i`` de todos os outros (nos dois sentidos)."""
        outros = [j for j in self.enderecos if j != i]
        self.log("isolar", alvo=i)
        self.falhas(i, bloqueados=outros)
        for j in outros:
            if self.vivo(j):
                self.falhas(j, bloqueados=sorted(set(self._bloqueados(j)) | {i}))

    def _bloqueados(self, j):
        s = self.status(j)
        return s["falhas"]["bloqueados"] if s else []

    def particionar(self, grupo_a, grupo_b):
        """Partição de rede entre dois grupos de nós."""
        self.log("particionar", a=sorted(grupo_a), b=sorted(grupo_b))
        for i in grupo_a:
            self.falhas(i, bloqueados=sorted(grupo_b))
        for i in grupo_b:
            self.falhas(i, bloqueados=sorted(grupo_a))

    def curar(self):
        """Remove todas as falhas injetadas em todos os nós vivos."""
        self.log("curar")
        for i in self.vivos():
            self.falhas(i, limpar=True)
