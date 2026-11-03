"""Detector de falhas por heartbeat (terceiro algoritmo, bônus).

O líder envia HEARTBEAT a todos os pares a cada ``intervalo_hb`` (uma tarefa por
par, para que um par lento ou morto não atrase os demais). O heartbeat carrega
época, líder, visão de replicação, ``pronto`` e a posição (seq) do líder; o par
responde HB_ACK com a sua posição.

- Seguidor: se não recebe heartbeat (nem outra mensagem válida do líder) por
  ``timeout_fd``, *suspeita* do líder e dispara uma eleição Bully.
- Líder: se um membro da visão não responde por ``timeout_fd``, suspeita dele e
  o remove da visão (ver ``Replicacao.verificar_visao``); ele volta pelo
  protocolo de ENTRAR quando voltar a responder.

O detector é *não confiável* por construção: com atraso ou perda acima do
timeout ele suspeita de nós vivos (falsa suspeita). Cada suspeita é registrada;
se o suspeito volta a falar com a mesma incarnação, o log marca ``falsa=true``
(com incarnação diferente foi um reinício real). As épocas garantem que uma
falsa suspeita no máximo causa uma nova eleição, nunca dois primários
confirmando escritas.
"""

import asyncio
import time

from . import mensagens as M


class DetectorFalhas:
    def __init__(self, no):
        self.no = no
        self.em_voo = set()
        no.registrar(M.HEARTBEAT, self.tratar_heartbeat)

    async def loop(self):
        no = self.no
        while True:
            await asyncio.sleep(no.params.intervalo_hb)
            if no.papel == "lider":
                for p in no.pares:
                    if p not in self.em_voo:
                        no.criar_tarefa(self.enviar_hb(p), "hb")
                no.replicacao.verificar_visao()
            else:
                self._tick_seguidor()

    def _tick_seguidor(self):
        no = self.no
        if no.bully.ativa:
            return
        if no.lider is None or no.lider == no.id:
            no.bully.agendar("sem_lider")
            return
        silencio = time.monotonic() - no.ultimo_hb
        if silencio > no.params.timeout_fd:
            no.suspeito_lider = True
            no.suspeitar(no.lider, "heartbeat_ausente", silencio=round(silencio, 4))
            no.bully.agendar("lider_suspeito")

    async def enviar_hb(self, par):
        """Envia um heartbeat a ``par`` e processa a resposta."""
        no = self.no
        if par in self.em_voo or no.papel != "lider":
            return
        self.em_voo.add(par)
        try:
            r = no.replicacao
            epoca = no.epoca
            resp = await no.chamar(par, M.HEARTBEAT, {
                "lider": no.id, "visao": sorted(r.visao), "pronto": r.pronto,
                "seq": no.replica.seq, "ult_epoca": no.replica.ult_epoca})
            if resp is None or no.papel != "lider" or no.epoca != epoca:
                return
            if resp["tipo"] == M.HB_ACK:
                r.ultimo_ack[par] = time.monotonic()
            elif resp["tipo"] == M.RECUSA:
                no.processar_recusa(resp)
        finally:
            self.em_voo.discard(par)

    async def tratar_heartbeat(self, msg):
        no = self.no
        if not no.aceitar_lider(msg["epoca"], msg["origem"]):
            return no.recusa()
        d = msg["dados"]
        if d.get("pronto") and no.id not in d.get("visao", []):
            no.replicacao.agendar_entrada()
        no.verificar_valentao(msg["origem"])
        return no.resp(M.HB_ACK, {"seq": no.replica.seq, "ult_epoca": no.replica.ult_epoca})
