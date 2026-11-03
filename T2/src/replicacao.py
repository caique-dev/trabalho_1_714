"""Replicação primário-backup síncrona (remote-write) com fencing por época.

Papéis: o líder do Bully é o primário; os demais são backups. O primário mantém
uma *visão* (conjunto de backups que ele considera em dia).

Escrita (CLI_PUT no primário), sob uma trava que serializa as escritas:
1. atribui o próximo ``seq``, aplica e persiste localmente;
2. envia REPLICAR(época, seq, prev_epoca, chave, valor) a todos da visão;
3. cada backup recusa época antiga (RECUSA), aplica em ordem e persiste antes de
   responder ACK; se faltar escrita ou o histórico divergir, responde
   ``ok=false`` e o primário envia o estado completo (INSTALAR_ESTADO);
4. quem estourou o timeout sai da visão; recusa por época maior => o primário
   renuncia e **não** confirma;
5. confirma ao cliente somente se ``1 + |backups que deram ACK| >= quórum``.

O passo 5 vai além do enunciado "ACK de todos da visão": sem ele, um primário
isolado por partição removeria todos os backups por timeout e confirmaria
sozinho escritas que o resto do grupo (que já elegeu outro primário) nunca verá.
Com quórum de maioria, duas confirmações em épocas diferentes sempre se
interceptam em pelo menos um nó. ``quorum=1`` desliga a proteção.

Fase de sincronização (novo primário, antes de aceitar escritas):
consulta todos (SINC_CONSULTA, que também *promete* a época: ninguém mais aceita
primário de época menor), exige respostas de um quórum, adota o estado de maior
``(ult_epoca, seq)``, empurra esse estado a quem estiver diferente e só então
fica ``pronto``. Como toda escrita confirmada está em um quórum e o quórum da
sincronização intercepta esse quórum, nenhuma escrita confirmada se perde.

Reingresso: o primário anuncia a visão e ``pronto`` nos heartbeats; um nó que
não está na visão (voltou de crash, foi removido por timeout) envia ENTRAR com a
sua posição; sob a trava de escrita o primário lhe empurra o estado, se preciso,
e só então o inclui na visão. Um nó reiniciado, portanto, só volta a contar como
"em dia" depois de re-sincronizar.
"""

import asyncio
import time

from . import mensagens as M
from .replica import APLICADO, DUPLICADO


class Replicacao:
    def __init__(self, no):
        self.no = no
        self.pronto = False
        self.visao = set()
        self.ultimo_ack = {}
        self.trava = asyncio.Lock()
        self.tarefa_sync = None
        self.entrando = False
        for tipo, fn in ((M.REPLICAR, self.tratar_replicar),
                         (M.INSTALAR_ESTADO, self.tratar_instalar),
                         (M.PEDIR_ESTADO, self.tratar_pedir_estado),
                         (M.SINC_CONSULTA, self.tratar_sinc_consulta),
                         (M.ENTRAR, self.tratar_entrar),
                         (M.CLI_PUT, self.tratar_put),
                         (M.CLI_GET, self.tratar_get)):
            no.registrar(tipo, fn)

    # ------------------------------------------------------------------
    # Ciclo de vida do primário
    # ------------------------------------------------------------------
    def ao_assumir(self, epoca):
        self.pronto = False
        self.visao = set()
        self.ultimo_ack = {}

    def ao_renunciar(self):
        self.pronto = False
        self.visao = set()
        if self.tarefa_sync is not None and not self.tarefa_sync.done():
            self.tarefa_sync.cancel()

    def iniciar_sincronizacao(self, epoca):
        self.tarefa_sync = self.no.criar_tarefa(self.sincronizar(epoca), "sincronizacao")

    def _ainda_lider(self, epoca):
        return self.no.papel == "lider" and self.no.epoca == epoca and not self.no.parando.is_set()

    async def sincronizar(self, epoca):
        """Fase de sincronização pós-eleição. Repete até conseguir ou deixar de ser líder."""
        no = self.no
        t0 = time.monotonic()
        while self._ainda_lider(epoca):
            respostas = await asyncio.gather(
                *[no.chamar(p, M.SINC_CONSULTA, epoca=epoca) for p in no.pares])
            if not self._ainda_lider(epoca):
                return
            vivos = {}
            for p, r in zip(no.pares, respostas):
                if r is None:
                    continue
                if r["tipo"] == M.SINC_RESP:
                    vivos[p] = (r["dados"]["ult_epoca"], r["dados"]["seq"])
                elif r["tipo"] == M.RECUSA:
                    no.processar_recusa(r)
            if not self._ainda_lider(epoca):
                return
            if 1 + len(vivos) < no.quorum:
                no.log("sync_sem_quorum", epoca=epoca, respostas=sorted(vivos))
                await asyncio.sleep(no.params.intervalo_hb)
                continue

            minha = no.replica.chave_estado()
            if vivos:
                melhor_id, melhor = max(vivos.items(), key=lambda kv: kv[1])
                if melhor > minha:
                    r = await no.chamar(melhor_id, M.PEDIR_ESTADO, timeout=2 * no.params.timeout_rpc)
                    if r is None or r["tipo"] != M.ESTADO or not self._ainda_lider(epoca):
                        await asyncio.sleep(no.params.intervalo_hb)
                        continue
                    no.replica.instalar(r["dados"])
                    no.log("transferencia_estado", direcao="recebido", de=melhor_id,
                           seq=no.replica.seq, ult_epoca=no.replica.ult_epoca)

            chave_lider = no.replica.chave_estado()

            async def alinhar(p):
                if vivos[p] == chave_lider:
                    return True
                return await self._empurrar_estado(p, epoca)

            candidatos = sorted(vivos)
            resultados = await asyncio.gather(*[alinhar(p) for p in candidatos])
            if not self._ainda_lider(epoca):
                return
            visao = {p for p, ok in zip(candidatos, resultados) if ok}
            if 1 + len(visao) < no.quorum:
                no.log("sync_sem_quorum", epoca=epoca, respostas=sorted(visao))
                await asyncio.sleep(no.params.intervalo_hb)
                continue
            agora = time.monotonic()
            self.visao = visao
            self.ultimo_ack = {p: agora for p in visao}
            self.pronto = True
            no.log("lider_pronto", epoca=epoca, seq=no.replica.seq, visao=sorted(visao),
                   duracao_sync=round(agora - t0, 4))
            return

    async def _empurrar_estado(self, par, epoca):
        """Envia o snapshot completo a ``par``; True se ele instalou (ACK ok)."""
        no = self.no
        snap = no.replica.snapshot()
        r = await no.chamar(par, M.INSTALAR_ESTADO, snap, timeout=2 * no.params.timeout_rpc,
                            epoca=epoca)
        if r is not None and r["tipo"] == M.RECUSA:
            no.processar_recusa(r)
        ok = r is not None and r["tipo"] == M.ACK and r["dados"].get("ok", False)
        no.log("transferencia_estado", direcao="enviado", para=par, seq=snap["seq"],
               ult_epoca=snap["ult_epoca"], ok=ok)
        return ok

    def verificar_visao(self):
        """Remove da visão quem ficou mais de ``timeout_fd`` sem responder."""
        no = self.no
        agora = time.monotonic()
        for p in list(self.visao):
            silencio = agora - self.ultimo_ack.get(p, 0.0)
            if silencio > no.params.timeout_fd:
                self.visao.discard(p)
                no.suspeitar(p, "backup_sem_resposta", silencio=round(silencio, 4))
                no.log("visao_removido", alvo=p, motivo="heartbeat", visao=sorted(self.visao))

    def tem_quorum_vivo(self):
        no = self.no
        agora = time.monotonic()
        vivos = 1 + sum(1 for p in self.visao
                        if agora - self.ultimo_ack.get(p, 0.0) <= no.params.timeout_fd)
        return vivos >= no.quorum

    # ------------------------------------------------------------------
    # Lado do primário: operações de cliente
    # ------------------------------------------------------------------
    def _erro(self, erro, redirecionar=None, **extra):
        return self.no.resp(M.RESP, {"ok": False, "erro": erro, "redirecionar": redirecionar,
                                     "epoca": self.no.epoca, **extra})

    def _nao_lider(self):
        no = self.no
        if no.papel != "lider":
            dica = no.lider if (no.lider not in (None, no.id) and not no.suspeito_lider) else None
            return self._erro("nao_lider", redirecionar=dica)
        if not self.pronto:
            return self._erro("lider_nao_pronto")
        return None

    async def tratar_get(self, msg):
        no = self.no
        erro = self._nao_lider()
        if erro is not None:
            return erro
        if not self.tem_quorum_vivo():
            return self._erro("sem_quorum")
        chave = msg["dados"]["chave"]
        existe = chave in no.replica.dados
        return no.resp(M.RESP, {"ok": True, "existe": existe,
                                "valor": no.replica.dados.get(chave), "epoca": no.epoca,
                                "seq": no.replica.seq})

    async def tratar_put(self, msg):
        no = self.no
        erro = self._nao_lider()
        if erro is not None:
            return erro
        chave, valor = msg["dados"]["chave"], msg["dados"]["valor"]
        t0 = time.perf_counter()  # alta resolução (monotonic tem ~15 ms no Windows)
        async with self.trava:
            erro = self._nao_lider()
            if erro is not None:
                return erro
            if 1 + len(self.visao) < no.quorum:
                return self._erro("sem_quorum")
            epoca = no.epoca
            seq, prev = no.replica.escrever_local(epoca, chave, valor)
            no.log("escrita_local", epoca=epoca, seq=seq, chave=chave, valor=valor)
            backups = sorted(self.visao)
            resultados = await asyncio.gather(
                *[self._replicar_um(b, epoca, seq, prev, chave, valor) for b in backups])
            if not self._ainda_lider(epoca):
                no.log("escrita_nao_confirmada", epoca=epoca, seq=seq, chave=chave,
                       motivo="lider_deposto")
                return self._erro("lider_deposto", seq=seq)
            acks = []
            for b, ok in zip(backups, resultados):
                if ok:
                    acks.append(b)
                else:
                    self.visao.discard(b)
                    no.suspeitar(b, "replicar_sem_ack", seq=seq)
                    no.log("visao_removido", alvo=b, motivo="replicar", seq=seq,
                           visao=sorted(self.visao))
            if 1 + len(acks) < no.quorum:
                no.log("escrita_nao_confirmada", epoca=epoca, seq=seq, chave=chave,
                       motivo="sem_quorum", acks=acks)
                return self._erro("sem_quorum", seq=seq)
        latencia = time.perf_counter() - t0
        no.log("escrita_confirmada", epoca=epoca, seq=seq, chave=chave, valor=valor,
               acks=acks, latencia_ms=round(latencia * 1000, 3))
        return no.resp(M.RESP, {"ok": True, "seq": seq, "epoca": epoca,
                                "latencia_servidor_ms": round(latencia * 1000, 3)})

    async def _replicar_um(self, backup, epoca, seq, prev, chave, valor):
        """REPLICAR a um backup. True se ele confirmou (aplicando ou recebendo o estado)."""
        no = self.no
        r = await no.chamar(backup, M.REPLICAR, {"seq": seq, "prev_epoca": prev,
                                                 "chave": chave, "valor": valor}, epoca=epoca)
        if r is None:
            return False
        if r["tipo"] == M.RECUSA:
            no.processar_recusa(r)
            return False
        if r["tipo"] != M.ACK:
            return False
        if r["dados"].get("ok"):
            self.ultimo_ack[backup] = time.monotonic()
            return True
        # Lacuna/divergência: transferência de estado completa.
        ok = await self._empurrar_estado(backup, epoca)
        if ok:
            self.ultimo_ack[backup] = time.monotonic()
        return ok

    # ------------------------------------------------------------------
    # Lado do backup
    # ------------------------------------------------------------------
    async def tratar_replicar(self, msg):
        no = self.no
        if not no.aceitar_lider(msg["epoca"], msg["origem"]):
            no.log("recusa_replicar", de=msg["origem"], epoca_msg=msg["epoca"], epoca=no.epoca)
            return no.recusa()
        d = msg["dados"]
        res = no.replica.aplicar(msg["epoca"], d["seq"], d["prev_epoca"], d["chave"], d["valor"])
        if res == APLICADO:
            no.log("escrita_aplicada", epoca=msg["epoca"], seq=d["seq"], chave=d["chave"])
        elif res != DUPLICADO:
            no.log("lacuna_detectada", seq_msg=d["seq"], meu_seq=no.replica.seq)
        return no.resp(M.ACK, {"ok": res in (APLICADO, DUPLICADO), "seq": no.replica.seq,
                               "ult_epoca": no.replica.ult_epoca})

    async def tratar_instalar(self, msg):
        no = self.no
        if not no.aceitar_lider(msg["epoca"], msg["origem"]):
            return no.recusa()
        no.replica.instalar(msg["dados"])
        no.log("transferencia_estado", direcao="instalado", de=msg["origem"],
               seq=no.replica.seq, ult_epoca=no.replica.ult_epoca)
        return no.resp(M.ACK, {"ok": True, "seq": no.replica.seq,
                               "ult_epoca": no.replica.ult_epoca})

    async def tratar_pedir_estado(self, msg):
        return self.no.resp(M.ESTADO, self.no.replica.snapshot())

    async def tratar_sinc_consulta(self, msg):
        no = self.no
        if not no.aceitar_lider(msg["epoca"], msg["origem"]):
            return no.recusa()
        return no.resp(M.SINC_RESP, {"seq": no.replica.seq, "ult_epoca": no.replica.ult_epoca})

    # ------------------------------------------------------------------
    # Reingresso na visão
    # ------------------------------------------------------------------
    def agendar_entrada(self):
        if not self.entrando and self.no.papel != "lider":
            self.entrando = True
            self.no.criar_tarefa(self._entrar(), "entrar")

    async def _entrar(self):
        no = self.no
        try:
            lider = no.lider
            if lider is None or lider == no.id:
                return
            r = await no.chamar(lider, M.ENTRAR, {"chave_estado": list(no.replica.chave_estado())},
                                timeout=4 * no.params.timeout_rpc)
            if r is not None and r["tipo"] == M.RECUSA:
                no.processar_recusa(r)
        finally:
            self.entrando = False

    async def tratar_entrar(self, msg):
        no = self.no
        if no.papel != "lider" or not self.pronto:
            return no.resp(M.ACK, {"ok": False, "motivo": "nao_pronto"})
        membro = msg["origem"]
        epoca = no.epoca
        async with self.trava:
            if not self._ainda_lider(epoca) or not self.pronto:
                return no.resp(M.ACK, {"ok": False, "motivo": "nao_pronto"})
            if tuple(msg["dados"]["chave_estado"]) != no.replica.chave_estado():
                if not await self._empurrar_estado(membro, epoca):
                    return no.resp(M.ACK, {"ok": False, "motivo": "falha_estado"})
            self.visao.add(membro)
            self.ultimo_ack[membro] = time.monotonic()
            no.log("visao_adicionado", alvo=membro, visao=sorted(self.visao))
        return no.resp(M.ACK, {"ok": True})
