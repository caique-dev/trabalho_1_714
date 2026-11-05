"""O nó: estado comum, época persistida, despacho de mensagens e status.

Um nó é um processo asyncio que compõe três módulos de protocolo, cada um
registrando seus tratadores de mensagem aqui:

    Bully            eleição de líder (``bully.py``)
    DetectorFalhas   heartbeat e suspeitas (``heartbeat.py``)
    Replicacao       primário-backup síncrono e sincronização (``replicacao.py``)

Estado compartilhado: ``epoca`` (maior época vista), ``lider`` (líder
reconhecido *nessa* época) e ``papel``. O par (época, líder) é persistido a cada
mudança: depois de um crash o nó não pode aceitar outro líder para uma época em
que já reconheceu um (é o que garante, junto com o quórum da sincronização, que
no máximo um líder por época chegue a servir escritas).
"""

import asyncio
import os
import random
import socket
import time

from . import mensagens as M
from .bully import Bully
from .falhas import InjetorFalhas
from .heartbeat import DetectorFalhas
from .logs import LogEventos
from .persistencia import Armazenamento
from .replica import Replica
from .replicacao import Replicacao
from .transporte import Contadores, Transporte

SEGUIDOR = "seguidor"
LIDER = "lider"


class No:
    def __init__(self, id_no, enderecos, dir_dados, dir_logs, params, host=None):
        self.id = id_no
        self.enderecos = dict(enderecos)
        self.pares = sorted(p for p in enderecos if p != id_no)
        self.n_total = len(enderecos)
        self.params = params
        self.quorum = params.quorum or (self.n_total // 2 + 1)
        self.host = host or "127.0.0.1"
        self.porta = enderecos[id_no][1]
        self.inc = int(time.time() * 1000)  # incarnação: muda a cada (re)início
        self.t_inicio = time.monotonic()

        self.log = LogEventos(os.path.join(dir_logs, f"no{id_no}.jsonl"), id_no, self.inc)
        self.armazenamento = Armazenamento(os.path.join(dir_dados, f"no{id_no}"),
                                           fsync=params.fsync)
        self.epoca = self.armazenamento.meta["epoca"]
        self.lider = self.armazenamento.meta["lider"]
        self.replica = Replica(self.armazenamento)

        self.papel = SEGUIDOR
        self.ultimo_hb = time.monotonic()
        self.suspeito_lider = False
        self.suspeitos = {}   # alvo -> (instante, incarnação conhecida na suspeita)
        self.incs = {}        # par -> última incarnação vista
        self.n_suspeitas = 0
        self.n_suspeitas_falsas = 0
        self.parando = asyncio.Event()

        self.injetor = InjetorFalhas((params.semente or 0) * 1000 + id_no)
        self.contadores = Contadores()
        self.transporte = Transporte(id_no, self.enderecos, self.injetor, self.contadores,
                                     incarnacao=self.inc, observador=self.observar)
        self.handlers = {}
        self.tarefas = set()
        self.bully = Bully(self)
        self.detector = DetectorFalhas(self)
        self.replicacao = Replicacao(self)
        self.registrar(M.ADM_STATUS, self._adm_status)
        self.registrar(M.ADM_FALHAS, self._adm_falhas)
        self.registrar(M.ADM_PARAR, self._adm_parar)

    # ------------------------------------------------------------------
    # Infraestrutura
    # ------------------------------------------------------------------
    def registrar(self, tipo, tratador):
        self.handlers[tipo] = tratador

    def criar_tarefa(self, corrotina, nome="tarefa"):
        """Cria uma tarefa em segundo plano cujas exceções vão para o log (nunca somem em silêncio)."""
        async def _envolver():
            try:
                await corrotina
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self.log("erro_tarefa", tarefa=nome, erro=repr(exc))
        tarefa = asyncio.get_running_loop().create_task(_envolver())
        self.tarefas.add(tarefa)
        tarefa.add_done_callback(self.tarefas.discard)
        return tarefa

    async def chamar(self, destino, tipo, dados=None, timeout=None, epoca=None):
        return await self.transporte.chamar(
            destino, tipo, self.epoca if epoca is None else epoca, dados or {},
            timeout or self.params.timeout_rpc)

    async def enviar(self, destino, tipo, dados=None, timeout=None, epoca=None):
        return await self.transporte.enviar(
            destino, tipo, self.epoca if epoca is None else epoca, dados or {},
            timeout or self.params.timeout_rpc)

    def resp(self, tipo, dados=None):
        """Resposta no formato esperado pelo transporte: ``(tipo, época, dados)``."""
        return (tipo, self.epoca, dados or {})

    def recusa(self):
        """Fencing: recusa informando a época e o líder que o nó reconhece."""
        return (M.RECUSA, self.epoca, {"lider": self.lider})

    async def tratar(self, msg):
        tratador = self.handlers.get(msg.get("tipo"))
        if tratador is None:
            return None
        try:
            return await tratador(msg)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            self.log("erro_tratador", tipo=msg.get("tipo"), erro=repr(exc))
            return None

    def observar(self, msg):
        """Chamado para toda mensagem recebida: rastreia incarnações e revoga suspeitas."""
        origem = msg.get("origem")
        if not isinstance(origem, int) or origem == self.id:
            return
        inc = msg.get("inc")
        susp = self.suspeitos.pop(origem, None)
        if susp is not None:
            instante, inc_suspeita = susp
            falsa = inc == inc_suspeita
            if falsa:
                self.n_suspeitas_falsas += 1
            self.log("suspeita_revogada", alvo=origem, falsa=falsa,
                     apos=round(time.monotonic() - instante, 4))
        self.incs[origem] = inc

    def suspeitar(self, alvo, motivo, **extra):
        """Registra uma suspeita (uma por alvo até ela ser revogada)."""
        if alvo in self.suspeitos:
            return False
        self.suspeitos[alvo] = (time.monotonic(), self.incs.get(alvo))
        self.n_suspeitas += 1
        self.log("suspeita", alvo=alvo, motivo=motivo, epoca=self.epoca, **extra)
        return True

    # ------------------------------------------------------------------
    # Época e liderança
    # ------------------------------------------------------------------
    def definir_epoca(self, epoca, lider):
        """Persiste (época, líder). A época nunca diminui."""
        assert epoca >= self.epoca
        self.epoca, self.lider = epoca, lider
        self.armazenamento.gravar_meta(epoca, lider)

    def adotar_epoca(self, epoca):
        """Toma conhecimento de uma época maior sem saber quem é o líder dela."""
        if epoca > self.epoca:
            if self.papel == LIDER:
                self.renunciar("epoca_superior")
            self.definir_epoca(epoca, None)
            self.suspeito_lider = False
            return True
        return False

    def aceitar_lider(self, epoca, lider):
        """Regra única de aceitação de um líder (COORDENADOR, HEARTBEAT, REPLICAR, SINC...).

        - época menor que a minha: recusa (fencing);
        - mesma época: só aceito o líder que já reconheci (ou se ainda não
          reconheci nenhum); dois candidatos na mesma época não passam;
        - época maior: aceito, renunciando se eu era líder.
        """
        if epoca < self.epoca:
            return False
        if epoca == self.epoca:
            if self.lider is not None and self.lider != lider:
                return False
            if self.lider is None:
                self.definir_epoca(epoca, lider)
        else:
            if self.papel == LIDER:
                self.renunciar("epoca_superior")
            self.definir_epoca(epoca, lider)
        self.ultimo_hb = time.monotonic()
        self.suspeito_lider = False
        self.bully.evento_lider.set()
        return True

    def renunciar(self, motivo):
        if self.papel == LIDER:
            self.papel = SEGUIDOR
            self.log("renuncia", motivo=motivo, epoca=self.epoca)
            self.replicacao.ao_renunciar()

    def processar_recusa(self, resp):
        """Trata uma RECUSA recebida: alguém conhece época maior ou outro líder na mesma época."""
        epoca = resp["epoca"]
        lider = resp["dados"].get("lider")
        if epoca > self.epoca:
            self.log("recusa_epoca", de=resp["origem"], epoca_recusa=epoca, minha_epoca=self.epoca)
            if lider is not None and lider != self.id:
                self.aceitar_lider(epoca, lider)
            else:
                self.adotar_epoca(epoca)
        elif epoca == self.epoca and self.papel == LIDER and lider != self.id:
            self.log("recusa_conflito", de=resp["origem"], epoca=epoca, outro_lider=lider)
            self.renunciar("conflito_epoca")
            self.bully.agendar("conflito_epoca", atraso=self.rng_atraso())

    def rng_atraso(self):
        """Pequeno atraso aleatório para desfazer simetria entre candidatos concorrentes."""
        return random.uniform(0.0, 2 * self.params.intervalo_hb)

    def verificar_valentao(self, lider):
        """Bully: se o líder tem id menor que o meu, eu (mais forte) devo assumir."""
        if (lider is not None and lider < self.id and self.papel != LIDER
                and not self.bully.ativa
                and time.monotonic() - self.bully.fim_ultima > self.params.timeout_fd):
            self.bully.agendar("valentao")

    # ------------------------------------------------------------------
    # Administração
    # ------------------------------------------------------------------
    def status(self, incluir_dados=False):
        r = self.replicacao
        s = {
            "id": self.id, "pid": os.getpid(), "host": socket.gethostname(), "inc": self.inc,
            "papel": self.papel, "lider": self.lider, "epoca": self.epoca,
            "pronto": r.pronto, "visao": sorted(r.visao),
            # Só informativo (o protocolo não o usa): o líder tem, agora, um quórum de
            # membros que responderam há menos de timeout_fd? None se não é líder.
            "quorum_ativo": r.tem_quorum_vivo() if self.papel == LIDER else None,
            "seq": self.replica.seq, "ult_epoca": self.replica.ult_epoca,
            "n_chaves": len(self.replica.dados), "hash": self.replica.hash_estado(),
            "eleicao_ativa": self.bully.ativa, "quorum": self.quorum, "n_total": self.n_total,
            "suspeitas": self.n_suspeitas, "suspeitas_falsas": self.n_suspeitas_falsas,
            "uptime": round(time.monotonic() - self.t_inicio, 3),
            "contadores": self.contadores.instantaneo(),
            "falhas": self.injetor.estado(), "params": self.params.como_dict(),
        }
        if incluir_dados:
            s["dados"] = self.replica.dados
        return s

    async def _adm_status(self, msg):
        return self.resp(M.RESP, self.status(bool(msg["dados"].get("dados"))))

    async def _adm_falhas(self, msg):
        d = msg["dados"]
        estado = self.injetor.configurar(
            prob_descarte=d.get("prob_descarte"), atraso_fixo=d.get("atraso_fixo"),
            jitter=d.get("jitter"), bloqueados=d.get("bloqueados"),
            so_tipos=d.get("so_tipos"), semente=d.get("semente"),
            limpar=bool(d.get("limpar")))
        self.log("falhas_configuradas", **estado)
        return self.resp(M.RESP, {"ok": True, "falhas": estado})

    async def _adm_parar(self, msg):
        self.log("parada_solicitada")
        asyncio.get_running_loop().call_later(0.05, self.parando.set)
        return self.resp(M.RESP, {"ok": True})

    # ------------------------------------------------------------------
    # Ciclo de vida
    # ------------------------------------------------------------------
    async def executar(self):
        await self.transporte.servir(self.host, self.porta, self.tratar)
        self.log("no_iniciado", porta=self.porta, epoca=self.epoca, lider=self.lider,
                 seq=self.replica.seq, ult_epoca=self.replica.ult_epoca,
                 quorum=self.quorum, params=self.params.como_dict())
        self.criar_tarefa(self.detector.loop(), "detector")
        self.bully.agendar("inicio")  # quem (re)inicia dispara eleição
        await self.parando.wait()
        self.log("no_encerrado")
        for t in list(self.tarefas):
            t.cancel()
        await self.transporte.parar()
        self.armazenamento.fechar()
        self.log.fechar()
