"""Políticas de balanceamento de carga.

Cada política recebe o vetor de ocupações dos servidores (fila + em serviço) e
devolve o índice, base 0, do servidor que deve receber a próxima requisição.
O balanceador é instantâneo: a escolha não consome tempo de simulação.
"""

from abc import ABC, abstractmethod


class Politica(ABC):
    """Interface comum às três políticas do enunciado."""

    nome = "abstrata"

    def __init__(self, n_servidores, rng):
        """
        Parâmetros
        ----------
        n_servidores : int
            Quantidade de servidores atrás do balanceador.
        rng : random.Random
            Gerador de números aleatórios da réplica. É deliberadamente um
            gerador separado do usado para chegadas e serviços, para que a
            troca de política não desloque o fluxo de números aleatórios que
            gera o tráfego -- assim as três políticas enxergam exatamente a
            mesma sequência de chegadas para uma dada semente.
        """
        self.n_servidores = n_servidores
        self.rng = rng

    @abstractmethod
    def escolher(self, ocupacoes):
        """Devolve o índice do servidor escolhido para a próxima requisição.

        Parâmetros
        ----------
        ocupacoes : list[int]
            ocupacoes[i] é o número de requisições no servidor i, contando a
            que está em execução mais as que aguardam na fila.
        """
        raise NotImplementedError


class Aleatoria(Politica):
    """Escolha Aleatória: sorteia um dos servidores com probabilidade 1/n.

    É a política para a qual vale o modelo analítico: pelo teorema da
    decomposição (splitting) de um processo de Poisson, marcar cada chegada
    de forma independente com probabilidade 1/3 produz três processos de
    Poisson independentes de taxa lambda/3, um por servidor.
    """

    nome = "aleatoria"

    def escolher(self, ocupacoes):
        return self.rng.randrange(self.n_servidores)


class RoundRobin(Politica):
    """Round Robin: distribui ciclicamente (1, 2, 3, 1, 2, 3, ...).

    Determinística e sem consulta de estado. Cada servidor recebe exatamente
    uma a cada n chegadas, o que torna o processo de chegadas visto por um
    servidor um processo de Erlang-n (soma de n exponenciais), menos variável
    que o Poisson da política aleatória -- daí o E[R] menor.
    """

    nome = "round_robin"

    def __init__(self, n_servidores, rng):
        super().__init__(n_servidores, rng)
        self._proximo = 0

    def escolher(self, ocupacoes):
        escolhido = self._proximo
        self._proximo = (self._proximo + 1) % self.n_servidores
        return escolhido


class FilaMaisCurta(Politica):
    """Fila Mais Curta (Join the Shortest Queue).

    Consulta a ocupação de todos os servidores e envia ao menos carregado;
    empates são resolvidos por sorteio uniforme entre os empatados. É a única
    das três que usa realimentação do estado do sistema, e por isso a que
    melhor evita o cenário em que uma requisição espera numa fila enquanto
    outro servidor está ocioso.
    """

    nome = "fila_curta"

    def escolher(self, ocupacoes):
        menor = min(ocupacoes)
        # Caminho rápido: sem empate, evita alocar a lista de candidatos.
        candidatos = [i for i, n in enumerate(ocupacoes) if n == menor]
        if len(candidatos) == 1:
            return candidatos[0]
        return candidatos[self.rng.randrange(len(candidatos))]


#: Mapeia a chave usada na linha de comando para a classe da política.
POLITICAS = {
    Aleatoria.nome: Aleatoria,
    RoundRobin.nome: RoundRobin,
    FilaMaisCurta.nome: FilaMaisCurta,
}

#: Rótulos legíveis, usados nas tabelas e gráficos do relatório.
ROTULOS = {
    Aleatoria.nome: "Aleatória",
    RoundRobin.nome: "Round Robin",
    FilaMaisCurta.nome: "Fila Mais Curta",
}


def criar_politica(nome, n_servidores, rng):
    """Instancia a política pelo nome; levanta ValueError se não existir."""
    if nome not in POLITICAS:
        disponiveis = ", ".join(sorted(POLITICAS))
        raise ValueError(f"política desconhecida: {nome!r} (use uma de: {disponiveis})")
    return POLITICAS[nome](n_servidores, rng)
