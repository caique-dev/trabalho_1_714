"""Motor de simulação por eventos discretos do balanceador de carga.

O sistema simulado é o da figura do enunciado: um balanceador instantâneo
recebe um processo de Poisson de taxa `lam` e encaminha cada requisição a um
de `n_servidores` servidores homogêneos. Cada servidor tem uma única thread e
uma fila FCFS ilimitada, com tempo de serviço exponencial de média 1/`mu`.

A simulação é dirigida por eventos: o relógio salta diretamente de um evento
ao seguinte, o que dispensa qualquer passo de tempo fixo e torna as métricas
exatas dentro do modelo.

Eventos
-------
CHEGADA  uma requisição chega ao balanceador, que a encaminha na hora.
PARTIDA  um servidor conclui a requisição que estava em execução.
LOG      instante periódico de amostragem da ocupação das filas (opcional,
         não altera o estado do sistema nem o consumo de números aleatórios).
"""

import heapq
import random
from collections import deque
from dataclasses import dataclass, field

from .politicas import criar_politica

# Tipos de evento. A ordem dos valores também define o desempate no heap
# quando dois eventos caem exatamente no mesmo instante: primeiro as partidas
# (liberam servidor), depois as chegadas, por fim os logs. Com tempos
# contínuos isso é um caso de probabilidade nula, mas fixá-lo mantém a
# execução determinística para uma dada semente.
PARTIDA = 0
CHEGADA = 1
LOG = 2


@dataclass
class Servidor:
    """Estado de um servidor: fila FCFS ilimitada e uma thread de execução."""

    idx: int
    #: Requisições presentes, como pares (instante de chegada, tempo de
    #: serviço); a cabeça da fila é a que está em execução.
    fila: deque = field(default_factory=deque)
    #: Total de requisições recebidas do balanceador (inclui o warm-up).
    recebidas: int = 0
    #: Requisições concluídas dentro da janela de medição.
    concluidas: int = 0
    #: Tempo ocupado (n > 0) acumulado dentro da janela de medição.
    tempo_ocupado: float = 0.0

    @property
    def n(self):
        """Requisições no servidor: as que esperam na fila mais a em serviço."""
        return len(self.fila)


@dataclass
class Resultado:
    """Métricas de uma execução, já descontado o período de aquecimento."""

    politica: str
    lam: float
    mu: float
    duracao: float
    warmup: float
    semente: int

    vazao: float                    # X: partidas por unidade de tempo
    tempo_resposta: float           # E[R]
    tempo_fila: float               # E[T_Q]
    num_sistema: float              # E[N], pela área sob a curva de N(t)
    utilizacoes: list               # U_i, um por servidor
    distribuicao: list              # requisições encaminhadas a cada servidor

    chegadas_total: int             # chegadas geradas em toda a execução
    concluidas_janela: int          # partidas dentro da janela de medição
    n_final: int                    # N(duracao), útil no caso instável

    #: Amostras (t, N(t)) quando `coletar_trace` está ligado; senão, vazio.
    trace: list = field(default_factory=list)

    @property
    def utilizacao_media(self):
        return sum(self.utilizacoes) / len(self.utilizacoes)


class Simulacao:
    """Uma execução do sistema para uma política e um valor de lambda."""

    def __init__(
        self,
        lam,
        politica,
        mu=1.0,
        n_servidores=3,
        duracao=5000.0,
        warmup=500.0,
        semente=0,
        coletar_trace=False,
        log_intervalo=None,
        log_saida=None,
    ):
        """
        Parâmetros
        ----------
        lam : float
            Taxa de chegada do processo de Poisson.
        politica : str
            Chave da política: 'aleatoria', 'round_robin' ou 'fila_curta'.
        mu : float
            Taxa de serviço de cada servidor (E[S] = 1/mu).
        duracao : float
            Duração total da execução, em unidades de tempo.
        warmup : float
            Trecho inicial descartado; as métricas cobrem [warmup, duracao].
        semente : int
            Semente da réplica; fixa toda a aleatoriedade da execução.
        coletar_trace : bool
            Se True, guarda a série N(t) (usada no cenário instável lambda=3,3).
        log_intervalo : float or None
            Se dado, registra a ocupação das filas a cada `log_intervalo` u.t.
        log_saida : callable or None
            Função que recebe cada linha de log; o padrão é `print`.
        """
        if warmup >= duracao:
            raise ValueError("o warm-up deve ser menor que a duração da execução")
        if lam <= 0 or mu <= 0:
            raise ValueError("lambda e mu devem ser positivos")

        self.lam = lam
        self.mu = mu
        self.duracao = float(duracao)
        self.warmup = float(warmup)
        self.semente = semente
        self.coletar_trace = coletar_trace
        self.log_intervalo = log_intervalo
        self.log_saida = log_saida or print

        # Três geradores independentes: chegadas, tempos de serviço e
        # decisões da política. A separação implementa números aleatórios
        # comuns (common random numbers): para uma mesma semente, as três
        # políticas enxergam exatamente a mesma carga de trabalho -- os
        # mesmos instantes de chegada e as mesmas demandas de serviço --, e
        # só o roteamento difere. Isso reduz bastante a variância da
        # comparação entre políticas.
        #
        # O tempo de serviço é sorteado na chegada e viaja com a requisição,
        # em vez de ser sorteado quando ela entra em execução. Como os
        # servidores são homogêneos (todos Exp(mu)), as duas construções são
        # estatisticamente equivalentes, mas esta mantém a carga idêntica
        # entre políticas mesmo que o roteamento mude a ordem de atendimento.
        self.rng_chegadas = random.Random(semente)
        self.rng_servico = random.Random(semente ^ 0x9E3779B9)
        self.rng_politica = random.Random(semente ^ 0x5DEECE66)

        self.nome_politica = politica
        self.politica = criar_politica(politica, n_servidores, self.rng_politica)
        self.servidores = [Servidor(i) for i in range(n_servidores)]

        # Relógio e estado agregado.
        self.t = 0.0
        self._t_ultimo = 0.0          # instante da última atualização de área
        self.n_sistema = 0
        self._area_n = 0.0            # integral de N(t) na janela de medição

        # Acumuladores das métricas por requisição.
        self._soma_resposta = 0.0
        self._n_resposta = 0
        self._soma_fila = 0.0
        self._n_fila = 0
        self._chegadas = 0
        self._concluidas_janela = 0

        self._eventos = []            # heap de (tempo, seq, tipo, dados)
        self._seq = 0                 # desempate estável no heap
        self._trace = []

    # ------------------------------------------------------------------
    # Amostragem de variáveis aleatórias
    # ------------------------------------------------------------------

    def _entre_chegadas(self):
        """Tempo até a próxima chegada: Exp(lambda), média 1/lambda."""
        return self.rng_chegadas.expovariate(self.lam)

    def _tempo_servico(self):
        """Tempo de serviço de uma requisição: Exp(mu), média 1/mu."""
        return self.rng_servico.expovariate(self.mu)

    # ------------------------------------------------------------------
    # Fila de eventos
    # ------------------------------------------------------------------

    def _agendar(self, tempo, tipo, dados=None):
        self._seq += 1
        heapq.heappush(self._eventos, (tempo, tipo, self._seq, dados))

    # ------------------------------------------------------------------
    # Contabilidade dependente do tempo
    # ------------------------------------------------------------------

    def _avancar_para(self, novo_t):
        """Acumula as integrais no tempo até `novo_t`, antes de mudar o estado.

        Só conta o trecho que cai dentro da janela de medição
        [warmup, duracao], o que trata de forma exata o intervalo que
        atravessa a fronteira do warm-up: dele, apenas a parte posterior
        ao aquecimento entra na conta.
        """
        inicio = max(self._t_ultimo, self.warmup)
        fim = min(novo_t, self.duracao)
        if fim > inicio:
            delta = fim - inicio
            self._area_n += self.n_sistema * delta
            for s in self.servidores:
                if s.n > 0:
                    s.tempo_ocupado += delta
        self._t_ultimo = novo_t

    def _registrar_trace(self):
        if self.coletar_trace:
            self._trace.append((self.t, self.n_sistema))

    # ------------------------------------------------------------------
    # Tratamento dos eventos
    # ------------------------------------------------------------------

    def _iniciar_servico(self, servidor):
        """Coloca em execução a requisição na cabeça da fila do servidor.

        Chamado quando o servidor está ocioso e recebe uma requisição, e
        também a cada partida que deixa a fila não vazia. O instante em que
        isso ocorre é exatamente o fim da espera em fila da requisição.
        """
        t_chegada, servico = servidor.fila[0]
        if self.t >= self.warmup:
            self._soma_fila += self.t - t_chegada
            self._n_fila += 1
        self._agendar(self.t + servico, PARTIDA, servidor.idx)

    def _tratar_chegada(self):
        self._chegadas += 1
        self.n_sistema += 1

        # O balanceador decide na hora, a partir da ocupação atual (fila + em
        # execução) de cada servidor.
        ocupacoes = [s.n for s in self.servidores]
        escolhido = self.servidores[self.politica.escolher(ocupacoes)]
        escolhido.recebidas += 1
        escolhido.fila.append((self.t, self._tempo_servico()))

        # Servidor estava ocioso: a requisição entra em serviço imediatamente.
        if escolhido.n == 1:
            self._iniciar_servico(escolhido)

        self._agendar(self.t + self._entre_chegadas(), CHEGADA)

    def _tratar_partida(self, idx):
        servidor = self.servidores[idx]
        t_chegada, _ = servidor.fila.popleft()
        self.n_sistema -= 1

        if self.t >= self.warmup:
            self._soma_resposta += self.t - t_chegada
            self._n_resposta += 1
            self._concluidas_janela += 1
            servidor.concluidas += 1

        # Fila não vazia: a próxima requisição entra em serviço agora.
        if servidor.n > 0:
            self._iniciar_servico(servidor)

    def _tratar_log(self):
        ocupacoes = [s.n for s in self.servidores]
        recebidas = [s.recebidas for s in self.servidores]
        marca = "aquecimento" if self.t < self.warmup else "medicao"
        self.log_saida(
            f"t={self.t:8.1f} [{marca:11s}] N={self.n_sistema:5d} "
            f"filas={ocupacoes} recebidas={recebidas}"
        )
        if self.log_intervalo:
            self._agendar(self.t + self.log_intervalo, LOG)

    # ------------------------------------------------------------------
    # Laço principal
    # ------------------------------------------------------------------

    def executar(self):
        """Roda a simulação até `duracao` e devolve o `Resultado`."""
        self._agendar(self._entre_chegadas(), CHEGADA)
        if self.log_intervalo:
            self._agendar(0.0, LOG)
        self._registrar_trace()

        while self._eventos:
            tempo, tipo, _, dados = self._eventos[0]
            if tempo > self.duracao:
                break
            heapq.heappop(self._eventos)

            # Integra N(t) e as ocupações no trecho anterior ao evento, com o
            # estado antigo, e só então aplica a mudança de estado.
            self._avancar_para(tempo)
            self.t = tempo

            if tipo == CHEGADA:
                self._tratar_chegada()
            elif tipo == PARTIDA:
                self._tratar_partida(dados)
            else:
                self._tratar_log()
                continue  # log não altera o estado: não vale amostrar o trace

            self._registrar_trace()

        # Fecha a janela: integra o trecho do último evento até o fim.
        self._avancar_para(self.duracao)
        self.t = self.duracao
        self._registrar_trace()

        return self._montar_resultado()

    def _montar_resultado(self):
        janela = self.duracao - self.warmup
        return Resultado(
            politica=self.nome_politica,
            lam=self.lam,
            mu=self.mu,
            duracao=self.duracao,
            warmup=self.warmup,
            semente=self.semente,
            # X: partidas concluídas por unidade de tempo na janela.
            vazao=self._concluidas_janela / janela,
            # E[R]: média sobre as requisições concluídas na janela.
            tempo_resposta=(
                self._soma_resposta / self._n_resposta if self._n_resposta else float("nan")
            ),
            tempo_fila=(
                self._soma_fila / self._n_fila if self._n_fila else float("nan")
            ),
            # E[N]: área sob a curva de N(t) dividida pela janela, como pede a
            # dica do enunciado (e não a média sobre chegadas).
            num_sistema=self._area_n / janela,
            utilizacoes=[s.tempo_ocupado / janela for s in self.servidores],
            distribuicao=[s.recebidas for s in self.servidores],
            chegadas_total=self._chegadas,
            concluidas_janela=self._concluidas_janela,
            n_final=self.n_sistema,
            trace=self._trace,
        )


def simular(lam, politica, **kwargs):
    """Atalho: cria a `Simulacao`, executa e devolve o `Resultado`."""
    return Simulacao(lam, politica, **kwargs).executar()
