# MC714 — Trabalho 1: Balanceador de Carga para um Sistema Distribuído

Simulador de eventos discretos de um balanceador de carga que distribui requisições
entre 3 servidores homogêneos (fila FCFS ilimitada, uma thread cada), com comparação
contra o modelo analítico M/M/1.

Universidade Estadual de Campinas — Instituto de Computação
MC714 — Sistemas Distribuídos — 2º Semestre de 2026

## Modelo

- **Chegadas:** processo de Poisson de taxa `λ` (tempos entre chegadas ~ Exp(λ)).
- **Serviço:** exponencial de média `1/µ`, com `µ = 1.0` req./u.t.
- **Servidores:** 3 homogêneos, fila FCFS ilimitada, uma requisição por vez.
- **Balanceador:** instantâneo (sem tempo de processamento próprio).

## Políticas implementadas

| Chave | Política | Descrição |
|---|---|---|
| `aleatoria` | Escolha Aleatória | Sorteia um dos 3 servidores com probabilidade 1/3 cada. |
| `round_robin` | Round Robin | Distribui ciclicamente (1, 2, 3, 1, 2, 3, ...). |
| `fila_curta` | Fila Mais Curta | Envia ao servidor com menor ocupação (fila + em serviço); empates sorteados. |

## Requisitos

- Python 3.9+
- `numpy` e `matplotlib` (apenas para a campanha experimental e os gráficos)

```bash
pip install -r requirements.txt
```

O simulador em si (`src/simulador.py`, `src/politicas.py`) usa somente a biblioteca
padrão, então uma execução individual roda sem dependência alguma.

## Como executar

### Uma execução individual

```bash
python main.py simular --politica fila_curta --lam 1.8
```

Opções principais: `--lam`, `--mu`, `--politica`, `--duracao`, `--warmup`,
`--semente`, `--log-intervalo` (imprime a ocupação das filas ao longo do tempo).

### Tabela do modelo analítico

```bash
python main.py analitico
```

### Campanha experimental completa

15 configurações (5 valores de λ × 3 políticas) × 10 réplicas, mais o cenário
instável com λ = 3,3:

```bash
python main.py experimentos
```

Gera em `resultados/`: CSVs com média e IC de 95% por configuração, a comparação
analítico × simulação e os gráficos do relatório.

### Testes de sanidade

```bash
python -m pytest tests/ -v
```

## Estrutura

```
src/simulador.py     Motor de simulação por eventos discretos e coleta de métricas
src/politicas.py     As três políticas de balanceamento
src/analitico.py     Modelo M/M/1 (itens (a)–(f) do enunciado)
src/estatistica.py   Média e intervalo de confiança de 95%
src/experimentos.py  Campanha experimental (15 configurações × 10 réplicas)
src/graficos.py      Gráficos de E[R] e de N(t) no caso instável
main.py              Interface de linha de comando
tests/               Testes de sanidade do simulador
```

## Métricas coletadas

Medidas na janela `[warmup, duração]`, descartando o período de aquecimento:

- **X** — vazão: partidas na janela ÷ duração da janela.
- **E[R]** — tempo médio de resposta das requisições concluídas na janela.
- **E[N]** — número médio no sistema pela *área sob a curva* de `N(t)`
  (integral no tempo ÷ duração da janela), conforme a dica do enunciado.
- **U_i** — utilização de cada servidor: fração do tempo com `n_i > 0`.

## Divisão de trabalho

A ser preenchida pela dupla, de forma compatível com o histórico de commits.
