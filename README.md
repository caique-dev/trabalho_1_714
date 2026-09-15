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

## Resultados obtidos

Campanha completa (10 réplicas, 5000 u.t., warm-up de 500 u.t., µ = 1,0). Todos
os números abaixo saem de `resultados/comparacao.csv`.

**Validação do modelo (itens (c), (d) e (e)):**

- **Vazão e utilização independem da política** — para as três políticas,
  `X ≈ λ` e `U_i ≈ ρ = λ/3µ` com erro abaixo de 0,6%, confirmando o argumento
  de conservação de trabalho do item (c).
- **Lei de Little** — `E[N]` contra `X · E[R]` fecha com erro máximo de
  **0,13%** em todas as 15 configurações.
- **Ordenação do item (d)** — `E[R]` de Fila Mais Curta ≤ Round Robin ≤
  Aleatória em todos os λ, e a Aleatória adere ao analítico `1/(µ − λ/3)`.

**Ganho percentual de E[R] em relação à política aleatória:**

| λ | ρ | Round Robin | Fila Mais Curta |
|---:|---:|---:|---:|
| 0,6 | 0,20 | +14,5% | +17,7% |
| 1,2 | 0,40 | +23,0% | +32,4% |
| 1,8 | 0,60 | +28,7% | +44,2% |
| 2,4 | 0,80 | +31,4% | +54,7% |
| 2,7 | 0,90 | +28,7% | +58,3% |

O ganho cresce com a carga: quanto maior ρ, mais caro é o desperdício de
mandar uma requisição para um servidor ocupado enquanto outro está livre —
exatamente o que a política de fila mais curta evita.

**Ponto de atenção para o relatório.** Em λ = 2,7 (ρ = 0,9) o `E[R]` simulado
da política aleatória fica ~8,9% *abaixo* do analítico (9,11 contra 10,00),
enquanto para λ ≤ 2,4 o erro é de no máximo 2,2%. Não é um bug: é o viés de
horizonte finito típico de carga alta — em ρ = 0,9 o tempo de relaxação da
fila M/M/1 cresce como `1/(1−ρ)²`, e 5000 u.t. com 500 de aquecimento não
bastam para o sistema esquecer o estado inicial vazio. Rodar mais tempo
(ou aumentar o warm-up) aproxima o valor do analítico; vale citar o efeito
na discussão em vez de escondê-lo.

**Cenário instável (item (f)).** Com λ = 3,3 > 3µ, a vazão satura na
capacidade agregada (`X ≈ 3,00` para as três políticas) e `N(t)` cresce
linearmente, acompanhando a aproximação fluida `(λ − 3µ)·t`. As três curvas
praticamente se sobrepõem: com todos os servidores saturados, a política de
roteamento deixa de importar.

## Divisão de trabalho

A ser preenchida pela dupla, de forma compatível com o histórico de commits.
