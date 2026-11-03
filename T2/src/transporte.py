"""Transporte TCP com mensagens JSON por linha, timeouts explícitos e contadores.

Modelo de comunicação: cada chamada abre uma conexão, envia uma linha e (em
``chamar``) lê uma linha de resposta. Abrir uma conexão por mensagem é mais
lento que manter canais persistentes, mas elimina estado de conexão (reconexão,
descarte de resposta antiga) e torna cada mensagem independente, que é
exatamente o canal que Bully e heartbeat assumem: não FIFO entre mensagens
distintas, sem duplicação, com perda possível (injetada ou por queda do par).

Toda operação de rede é envolvida em ``asyncio.wait_for``: nenhuma chamada fica
sem prazo. Mensagens perdidas por injeção são tratadas como o remetente as
veria na rede real: ``chamar`` só descobre pelo estouro do timeout.

Contadores (métrica de complexidade de mensagens): ``enviadas`` conta toda
mensagem emitida, mesmo as que a injeção descartou; ``recebidas`` só as que
chegaram à aplicação. Mensagens externas (cliente/admin) ficam em contadores
separados para não poluir a contagem do protocolo.
"""

import asyncio
import itertools

from . import mensagens as M


class Contadores:
    def __init__(self):
        self.enviadas = {}
        self.recebidas = {}
        self.externas_recebidas = {}
        self.descartadas = {"particao_saida": 0, "descarte_saida": 0, "particao_entrada": 0}

    @staticmethod
    def _inc(d, tipo):
        d[tipo] = d.get(tipo, 0) + 1

    def enviada(self, tipo):
        self._inc(self.enviadas, tipo)

    def recebida(self, tipo):
        self._inc(self.externas_recebidas if tipo in M.TIPOS_EXTERNOS else self.recebidas, tipo)

    def instantaneo(self):
        return {"enviadas": dict(self.enviadas), "recebidas": dict(self.recebidas),
                "externas_recebidas": dict(self.externas_recebidas),
                "descartadas": dict(self.descartadas),
                "total_enviadas": sum(self.enviadas.values()),
                "total_recebidas": sum(self.recebidas.values())}


class Transporte:
    def __init__(self, id_no, enderecos, injetor, contadores, incarnacao=0,
                 observador=None, log=None):
        self.id = id_no
        self.enderecos = enderecos
        self.injetor = injetor
        self.contadores = contadores
        self.inc = incarnacao
        self.observador = observador  # chamado com toda mensagem recebida
        self.log = log
        self._seq = itertools.count(1)
        self._tratador = None
        self._servidor = None

    def _id_msg(self):
        return f"{self.id}:{self.inc}:{next(self._seq)}"

    # ------------------------------------------------------------------
    # Lado servidor
    # ------------------------------------------------------------------
    async def servir(self, host, porta, tratador):
        """Escuta em ``host:porta``; ``tratador(msg)`` devolve ``(tipo, epoca, dados)`` ou None."""
        self._tratador = tratador
        self._servidor = await asyncio.start_server(self._conexao, host, porta,
                                                    limit=M.LIMITE_LINHA)

    async def parar(self):
        if self._servidor is not None:
            self._servidor.close()

    async def _segurar(self, reader):
        """Mantém a conexão aberta até o par desistir (simula perda sem RST)."""
        try:
            await asyncio.wait_for(reader.read(1), 5.0)
        except (asyncio.TimeoutError, OSError, ConnectionError):
            pass

    async def _conexao(self, reader, writer):
        try:
            linha = await asyncio.wait_for(reader.readline(), 5.0)
            if not linha:
                return
            msg = M.decodificar(linha)
            origem = msg.get("origem")
            if isinstance(origem, int) and self.injetor.bloqueado(origem):
                self.contadores.descartadas["particao_entrada"] += 1
                await self._segurar(reader)
                return
            self.contadores.recebida(msg["tipo"])
            if self.observador is not None:
                self.observador(msg)
            resposta = await self._tratador(msg)
            if resposta is None:
                return
            tipo, epoca, dados = resposta
            if isinstance(origem, int):
                self.contadores.enviada(tipo)
                causa, atraso = self.injetor.decidir(origem, tipo)
                if causa is not None:
                    self.contadores.descartadas["particao_saida" if causa == "particao"
                                                else "descarte_saida"] += 1
                    await self._segurar(reader)
                    return
                if atraso > 0:
                    await asyncio.sleep(atraso)
            resp = M.nova(tipo, self.id, origem, epoca, dados, id_msg=self._id_msg(),
                          inc=self.inc, resp_a=msg.get("id"))
            writer.write(M.codificar(resp))
            await asyncio.wait_for(writer.drain(), 5.0)
        except (asyncio.TimeoutError, OSError, ValueError):
            pass
        finally:
            try:
                writer.close()
            except OSError:
                pass

    # ------------------------------------------------------------------
    # Lado cliente (nó -> nó)
    # ------------------------------------------------------------------
    async def chamar(self, destino, tipo, epoca, dados, timeout):
        """Envia e espera a resposta. Devolve a mensagem de resposta ou None (timeout/falha)."""
        msg = M.nova(tipo, self.id, destino, epoca, dados, id_msg=self._id_msg(), inc=self.inc)
        self.contadores.enviada(tipo)
        causa, atraso = self.injetor.decidir(destino, tipo)
        if causa is not None:
            self.contadores.descartadas["particao_saida" if causa == "particao"
                                        else "descarte_saida"] += 1
            await asyncio.sleep(timeout)  # o remetente só percebe a perda pelo timeout
            return None

        async def _io():
            if atraso > 0:
                await asyncio.sleep(atraso)
            host, porta = self.enderecos[destino]
            reader, writer = await asyncio.open_connection(host, porta, limit=M.LIMITE_LINHA)
            try:
                writer.write(M.codificar(msg))
                await writer.drain()
                return await reader.readline()
            finally:
                writer.close()

        try:
            linha = await asyncio.wait_for(_io(), timeout)
            if not linha:
                return None
            resp = M.decodificar(linha)
        except (asyncio.TimeoutError, OSError, ValueError):
            return None
        self.contadores.recebida(resp["tipo"])
        if self.observador is not None:
            self.observador(resp)
        return resp

    async def enviar(self, destino, tipo, epoca, dados, timeout):
        """Envio sem resposta (ex.: COORDENADOR). Devolve True se entregue ao socket."""
        msg = M.nova(tipo, self.id, destino, epoca, dados, id_msg=self._id_msg(), inc=self.inc)
        self.contadores.enviada(tipo)
        causa, atraso = self.injetor.decidir(destino, tipo)
        if causa is not None:
            self.contadores.descartadas["particao_saida" if causa == "particao"
                                        else "descarte_saida"] += 1
            return False

        async def _io():
            if atraso > 0:
                await asyncio.sleep(atraso)
            host, porta = self.enderecos[destino]
            reader, writer = await asyncio.open_connection(host, porta, limit=M.LIMITE_LINHA)
            try:
                writer.write(M.codificar(msg))
                await writer.drain()
                # Espera o par fechar para não encerrar a conexão antes da leitura.
                await reader.read(1)
            finally:
                writer.close()

        try:
            await asyncio.wait_for(_io(), timeout)
            return True
        except (asyncio.TimeoutError, OSError):
            return False
