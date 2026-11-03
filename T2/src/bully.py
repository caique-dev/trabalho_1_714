"""Eleição de líder pelo algoritmo do Valentão (Bully, Garcia-Molina).

Mensagens: ELEICAO (para ids maiores), OK (resposta de quem está vivo) e
COORDENADOR (anúncio do vencedor, com a nova época).

Procedimento de um candidato:
1. envia ELEICAO a todos os ids maiores e espera OK por ``timeout_rpc``;
2. se ninguém respondeu OK, vence: nova época = ``max(épocas vistas) + 1``,
   persiste (época, eu) e anuncia COORDENADOR a todos os outros;
3. se alguém respondeu OK, espera ``timeout_coord`` por um COORDENADOR (ou um
   HEARTBEAT de líder válido, que vale o mesmo) e, se não vier, recomeça.

Decisões de projeto:
- Quem responde ELEICAO com OK e é o líder atual (já eleito) manda imediatamente
  um heartbeat ao solicitante em vez de abrir nova eleição: evita incrementar a
  época por causa de uma falsa suspeita de um seguidor.
- Se um nó descobre que o líder tem id menor que o seu (por exemplo depois de uma
  partição), ele dispara eleição: o maior id vivo sempre acaba líder.
- A eleição só escolhe o líder; quem pode servir escritas é decidido pela fase de
  sincronização em ``replicacao.py`` (exige quórum). Assim, em partição, o Bully
  pode eleger "líder" nos dois lados, mas só o lado com maioria o torna efetivo.
"""

import asyncio
import time

from . import mensagens as M


class Bully:
    def __init__(self, no):
        self.no = no
        self.ativa = False
        self.fim_ultima = 0.0
        self.evento_lider = asyncio.Event()
        no.registrar(M.ELEICAO, self.tratar_eleicao)
        no.registrar(M.COORDENADOR, self.tratar_coordenador)

    def agendar(self, motivo, atraso=0.0):
        """Dispara uma eleição em segundo plano (no máximo uma por vez)."""
        if self.ativa:
            return
        self.ativa = True
        self.no.criar_tarefa(self._executar(motivo, atraso), "eleicao")

    async def tratar_eleicao(self, msg):
        no = self.no
        no.adotar_epoca(msg["epoca"])
        if no.papel == "lider":
            # Já sou o líder: confirmo com um heartbeat direto em vez de reeleger.
            no.criar_tarefa(no.detector.enviar_hb(msg["origem"]), "hb_direto")
        else:
            self.agendar("eleicao_recebida")
        return no.resp(M.OK)

    async def tratar_coordenador(self, msg):
        no = self.no
        aceito = no.aceitar_lider(msg["epoca"], msg["origem"])
        no.log("coordenador_aceito" if aceito else "coordenador_ignorado",
               lider=msg["origem"], epoca_msg=msg["epoca"], epoca=no.epoca)
        if aceito:
            no.verificar_valentao(msg["origem"])
        return None

    async def _executar(self, motivo, atraso):
        no = self.no
        try:
            if atraso > 0:
                await asyncio.sleep(atraso)
            tentativa = 0
            while not no.parando.is_set():
                tentativa += 1
                self.evento_lider.clear()
                no.log("eleicao_iniciada", motivo=motivo, tentativa=tentativa, epoca=no.epoca)
                superiores = [p for p in no.pares if p > no.id]
                oks = 0
                if superiores:
                    respostas = await asyncio.gather(
                        *[no.chamar(p, M.ELEICAO) for p in superiores])
                    for p, r in zip(superiores, respostas):
                        if r is not None and r["tipo"] == M.OK:
                            oks += 1
                            no.adotar_epoca(r["epoca"])
                            no.log("ok_recebido", de=p)
                if self.evento_lider.is_set():
                    return  # um líder válido (COORDENADOR/HEARTBEAT) apareceu durante a rodada
                if oks == 0:
                    await self._tornar_lider()
                    return
                try:
                    await asyncio.wait_for(self.evento_lider.wait(), no.params.timeout_coord)
                    return
                except asyncio.TimeoutError:
                    no.log("coordenador_timeout", tentativa=tentativa)
        finally:
            self.ativa = False
            self.fim_ultima = time.monotonic()

    async def _tornar_lider(self):
        no = self.no
        nova_epoca = no.epoca + 1
        no.definir_epoca(nova_epoca, no.id)
        no.papel = "lider"
        no.suspeito_lider = False
        no.replicacao.ao_assumir(nova_epoca)
        no.log("coordenador_anunciado", epoca=nova_epoca)
        # O anúncio e a fase de sincronização correm em paralelo: o SINC_CONSULTA
        # também carrega a época, então um nó que perdeu o COORDENADOR a aprende
        # ali, e esperar o timeout de nós mortos só atrasaria o failover.
        no.criar_tarefa(self._anunciar(nova_epoca), "anuncio")
        no.replicacao.iniciar_sincronizacao(nova_epoca)

    async def _anunciar(self, epoca):
        no = self.no
        await asyncio.gather(*[no.enviar(p, M.COORDENADOR, epoca=epoca) for p in no.pares])
