# MC714 — Trabalho 2: KV Store Replicado Tolerante a Falhas

Cluster de N nós (padrão 5, ids `1..N`) que implementa um armazenamento chave-valor
replicado combinando três algoritmos distribuídos, todos escritos do zero (só biblioteca
padrão do Python, `asyncio` + TCP):

| Algoritmo | Tema do enunciado | Módulo |
|---|---|---|
| **Bully** (Garcia-Molina) — eleição do primário | Eleição de líder | `src/bully.py` |
| **Replicação primário-backup síncrona** (remote-write) com fencing por época e fase de sincronização pós-eleição | Replicação e consistência | `src/replicacao.py`, `src/replica.py` |
| **Detector de falhas por heartbeat** (bônus) — dispara eleições e remove backups falhos da visão | Detecção de falhas | `src/heartbeat.py` |

Universidade Estadual de Campinas — Instituto de Computação
MC714 — Sistemas Distribuídos — 2º Semestre de 2026

## Modelo do sistema

- **Temporal: parcialmente síncrono.** Bully e heartbeat só são corretos/vivos se existirem limites
  de atraso; assumimos que, depois de algum tempo, mensagens entre nós corretos chegam em menos
  de `timeout_rpc` e a ausência de heartbeat por `timeout_fd` indica falha. Quando o atraso
  excede esses limites o detector comete *falsas suspeitas* (registradas em log) e o sistema pode
  reeleger sem necessidade; a **segurança** (nenhuma escrita confirmada perdida, no máximo um
  líder efetivo por época) não depende dos tempos, só a **vivacidade** depende.
- **Canais:** TCP em localhost. Cada mensagem usa uma conexão própria (uma linha JSON, `\n`
  como delimitador), portanto: entrega confiável e sem duplicação *enquanto o par está vivo*;
  **sem ordenação** entre mensagens distintas (os protocolos toleram reordenação: backups
  aplicam por `seq` e detectam lacunas); perda possível por queda do par ou pela camada de
  injeção de falhas. Toda chamada tem timeout explícito (`asyncio.wait_for`).
- **Falhas:** crash-stop e **crash-recovery** (estado em disco: época, líder reconhecido, WAL +
  snapshot do dicionário). Partições e perda/atraso de mensagens por injeção na aplicação.
  Falhas bizantinas **não** são toleradas.
- **Relógios:** só relógios locais monotônicos para timeouts; nenhuma sincronização entre nós.
  Timestamps nos logs servem apenas à análise (mesma máquina).
- **Persistência/reinício:** cada nó grava em `dados/no<ID>/` (`meta.json`, `snapshot.json`,
  `wal.<g>.jsonl`). Um nó reiniciado dispara eleição, recarrega o estado do disco e só volta à
  visão de replicação depois de re-sincronizar com o primário. `Popen.kill()` preserva tudo que
  foi escrito (flush a cada operação); `--fsync` cobre queda da máquina.
- **Contêineres na mesma máquina não são domínios de falha independentes:** compartilham kernel,
  disco, relógio e energia. Os nós "independentes" aqui são processos/contêineres que só se
  comunicam pela rede; falhas correlacionadas (queda do host) não são exercitadas.

### Propriedades de correção

**Segurança**
1. *(S1)* No máximo um líder **efetivo** (que chega a servir escritas) por época.
2. *(S2)* Toda escrita confirmada ao cliente está presente no estado de todo líder posterior e,
   portanto, no estado final de todas as réplicas vivas (desde que algum nó com a escrita sobreviva
   e participe da sincronização).
3. *(S3)* Um primário deposto/isolado não confirma escritas (fencing por época + quórum).
4. *(S4)* Réplicas em dia convergem para o mesmo estado.

**Vivacidade (sob sincronia parcial e maioria de nós vivos)**
1. Eventualmente todos os nós vivos concordam no mesmo líder (o maior id vivo).
2. Eventualmente um líder pronto aceita escritas e as confirma.
3. Um nó que volta de crash eventualmente entra na visão e fica em dia.

Essas propriedades são checadas pelos **testes de integração** e pelo **verificador de logs**
(`python main.py verificar`, ver abaixo).

## Arquitetura

```
main.py            CLI: no | cluster | cliente | admin | verificar | experimentos
src/
  mensagens.py     tipos e envelope das mensagens (JSON por linha)
  transporte.py    servidor/cliente TCP asyncio, timeouts, contadores por tipo de mensagem
  falhas.py        injeção de falhas reprodutível por semente (descarte, atraso, partição)
  persistencia.py  meta.json + snapshot + WAL (crash-recovery)
  replica.py       máquina de estado da réplica: dict, seq, época da última escrita (sem rede)
  no.py            nó: época/líder persistidos, regra de aceitação de líder, despacho, status
  bully.py         eleição Bully
  heartbeat.py     detector de falhas
  replicacao.py    primário-backup, fase de sincronização, reingresso, PUT/GET
  logs.py          log estruturado JSONL
  verificador.py   verificação de invariantes a partir dos logs
  cliente.py       cliente síncrono com redirecionamento + chamadas administrativas
  cluster.py       orquestrador local de processos (Popen) usado por testes e experimentos
tests/             unitários (sem rede) e de integração (processos reais)
Dockerfile, compose.yaml   o mesmo código como 5 contêineres
experimentos/ (reservado), resultados/, relatorio/   fases 2 e 3
```

### Visão geral do funcionamento

1. **Eleição (Bully).** Quem (re)inicia, ou suspeita do líder, envia `ELEICAO` aos ids maiores.
   Sem `OK`, vence: nova época = `max(épocas vistas)+1`, persistida, e anuncia `COORDENADOR`.
   Com `OK`, espera o `COORDENADOR` (ou um heartbeat de líder válido) por `timeout_coord`, senão
   recomeça. Se o líder atual recebe `ELEICAO`, responde `OK` e manda heartbeat direto em vez de
   reeleger (evita trocar de época por causa de uma falsa suspeita).
2. **Regra única de aceitação de líder** (`No.aceitar_lider`): época menor → recusa (`RECUSA`);
   mesma época → só o líder já reconhecido (ou nenhum ainda); época maior → aceita e, se era líder,
   renuncia. O par (época, líder) é persistido: após um crash o nó não "vota" duas vezes na mesma época.
3. **Fase de sincronização.** O novo primário consulta todos (`SINC_CONSULTA`, que também
   *promete* a época), exige respostas de um **quórum** (maioria), adota o estado de maior
   `(ult_epoca, seq)` (`PEDIR_ESTADO`), empurra esse estado a quem estiver diferente
   (`INSTALAR_ESTADO`) e só então fica `pronto`. Antes disso responde `lider_nao_pronto`.
4. **Escrita.** `PUT` no primário, sob uma trava: atribui `seq`, persiste, envia `REPLICAR` a todos
   da visão; cada backup recusa época antiga, aplica em ordem (idempotente), **persiste antes de
   responder `ACK`**; lacuna/divergência → transferência de estado. Backup que estoura o timeout sai
   da visão. A escrita só é confirmada se `1 + |ACKs| >= quórum`. Recusa por época maior → o primário
   renuncia e não confirma.
5. **Reingresso.** O heartbeat anuncia a visão; um nó fora dela envia `ENTRAR` com a sua posição;
   sob a trava de escrita o primário lhe empurra o estado, se preciso, e o inclui na visão.
6. **Cliente.** Fala com qualquer nó; não-primário responde `nao_lider` + `redirecionar`. Tentativas
   repetem durante failovers até o prazo. `GET` é servido pelo primário (com verificação de que
   ainda enxerga um quórum recente).

### Formato das mensagens

Envelope JSON, uma linha por mensagem:
`{"tipo", "origem", "destino", "epoca", "id", "inc", "resp_a"?, "dados"}` — `epoca` é a maior época
conhecida pelo remetente (fencing), `inc` é a incarnação do remetente (distingue reinício de
falsa suspeita), `resp_a` liga resposta e requisição.

| Tipo | Sentido | `dados` | Resposta |
|---|---|---|---|
| `ELEICAO` | candidato → ids maiores | — | `OK` |
| `OK` | resposta | — | — |
| `COORDENADOR` | vencedor → todos | — (época no envelope) | nenhuma |
| `HEARTBEAT` | líder → todos | `lider, visao, pronto, seq, ult_epoca` | `HB_ACK{seq, ult_epoca}` |
| `REPLICAR` | primário → backups | `seq, prev_epoca, chave, valor` | `ACK{ok, seq}` ou `RECUSA` |
| `SINC_CONSULTA` | novo primário → todos | — | `SINC_RESP{seq, ult_epoca}` ou `RECUSA` |
| `PEDIR_ESTADO` | qualquer → qualquer | — | `ESTADO{dados, seq, ult_epoca}` |
| `INSTALAR_ESTADO` | primário → backup | snapshot | `ACK{ok}` ou `RECUSA` |
| `ENTRAR` | nó → primário | `chave_estado` | `ACK{ok}` |
| `RECUSA` | resposta (fencing) | `lider` (época atual no envelope) | — |
| `CLI_PUT`, `CLI_GET` | cliente → nó | `chave[, valor]` | `RESP{ok, erro, redirecionar, seq, ...}` |
| `ADM_STATUS`, `ADM_FALHAS`, `ADM_PARAR` | admin → nó | ver abaixo | `RESP` |

Contadores de mensagens: cada nó conta **enviadas** (inclui as descartadas pela injeção) e
**recebidas** por tipo; mensagens de cliente/admin ficam em `externas_recebidas`. Custo esperado:
uma escrita = `REPLICAR` + `ACK` por backup = `2(N-1)` mensagens.

### Injeção de falhas (reprodutível por semente)

Aplicada na camada de transporte, em tempo de execução, por `ADM_FALHAS` /
`python main.py admin falhas`: probabilidade de descarte (`--descarte`), atraso fixo + jitter
(`--atraso`, `--jitter`), partição (`--bloquear`: conjunto de pares, vale para entrada e saída),
filtro por tipo de mensagem (`--so-tipos`) e semente (`--semente-falhas`). Cada nó usa um gerador
próprio derivado de `semente*1000 + id`. A *distribuição* das falhas é reprodutível; a associação
exata sorteio↔mensagem pode variar entre execuções porque os nós são assíncronos. Crash = matar o
processo; recuperação = reiniciá-lo com o mesmo diretório de dados.

### Status, contadores e eventos (dados para a fase 2)

`ADM_STATUS` (`python main.py admin status`, ou `Cluster.status(i)`) devolve por nó: `papel`,
`lider`, `epoca`, `pronto`, `visao`, `seq`, `ult_epoca`, `hash` do estado, `n_chaves`, `quorum`,
`suspeitas`, `suspeitas_falsas`, `uptime`, parâmetros, estado da injeção de falhas e
`contadores` (`enviadas`/`recebidas` por tipo, `externas_recebidas`, `descartadas`).

Cada nó escreve `logs/no<ID>.jsonl` (e o orquestrador `logs/cluster.jsonl`, com `no_morto` /
`no_iniciado`). Cada linha: `t` (`time.time()`), `p` (`time.perf_counter()`, alta resolução e
mesma origem em todos os processos da máquina — **use `p` para intervalos**; no Windows
`time.time()` tem resolução ~15 ms), `no`, `inc`, `evento` e campos. Eventos principais:
`no_iniciado`, `eleicao_iniciada`, `ok_recebido`, `coordenador_anunciado`, `coordenador_aceito`,
`coordenador_timeout`, `lider_pronto` (com `duracao_sync`), `suspeita`, `suspeita_revogada`
(`falsa`), `renuncia`, `recusa_epoca`, `recusa_replicar`, `visao_removido`, `visao_adicionado`,
`escrita_local`, `escrita_aplicada`, `escrita_confirmada` (com `latencia_ms`),
`escrita_nao_confirmada`, `lacuna_detectada`, `transferencia_estado`, `falhas_configuradas`.
Exemplos de métricas: tempo de eleição = `lider_pronto.p` − `no_morto.p`; mensagens da eleição =
diferença dos contadores antes/depois; latência de escrita = `latencia_ms` / medida no cliente.

### Verificador de invariantes

`python main.py verificar --logs DIR [--dados DIR --vivos 1,2,3]` lê todos os `*.jsonl`, ordena
por `p` e checa:

- **I1** no máximo um `lider_pronto` por época (líder efetivo);
- **I2** a época de cada nó nunca diminui;
- **I3** toda `escrita_confirmada` veio de um nó que já era `lider_pronto` naquela época;
- **I4** dois `seq` confirmados iguais têm o mesmo conteúdo (históricos não divergem);
- **I5** (com estados finais) toda escrita confirmada está no estado final de cada réplica viva
  (o valor final é o da última confirmada ou de uma escrita posterior, que pode ter sido adotada na
  sincronização sem nunca ter sido confirmada);
- **I6** (com estados finais) as réplicas vivas convergiram para o mesmo estado.

Mais de um `coordenador_anunciado` na mesma época é apenas aviso (possível em partição; só um vira
efetivo). Os estados finais vêm do disco (`--dados`) ou, nos testes, de `ADM_STATUS`.

## Decisões de projeto e casos em que as garantias **não** valem

- **Quórum de confirmação (além do enunciado).** O enunciado pede "ACK de todos da visão, removendo
  quem estoura o timeout". Sozinha, essa regra deixa um primário isolado remover todos os backups e
  confirmar sozinho. Por isso a confirmação exige `1 + |ACKs| >= quórum` (maioria por padrão;
  `--quorum 1` reproduz a regra literal, sem proteção contra partição). Custo: com menos que a
  maioria dos nós vivos, **não há escritas nem leituras** (disponibilidade sacrificada por segurança).
- **Partição + Bully → dois "líderes".** O Bully não exige maioria: nos dois lados de uma partição
  pode haver um candidato que vence e anuncia `COORDENADOR`; os dois podem até usar a mesma época
  (`max+1`). Isso é **tolerado**: o líder só serve depois da fase de sincronização, que exige
  quórum e promessa de época (um nó não aceita dois líderes na mesma época). Só um lado tem
  maioria, então no máximo um é efetivo (`lider_pronto`); o outro permanece "não pronto" e o
  primário antigo, isolado, perde o quórum, não confirma e renuncia ao ver a época maior. Quem
  tiver id maior que o líder visto retoma a liderança (Bully).
- **Escritas não confirmadas.** Um erro/timeout em `PUT` significa resultado *indeterminado*: a
  escrita pode ter ficado em alguns backups e ser adotada pela sincronização do próximo primário.
  Só respostas `ok` são garantias. O cliente repete o PUT (idempotente).
- **"Nenhuma escrita confirmada se perde" exige que algum nó com ela sobreviva e participe da
  sincronização.** Se todos os nós que a têm morrem (ou perdem o disco) e os sobreviventes formam
  quórum, ela se perde. Dados em disco sobrevivem a `kill`; `--fsync` é necessário para queda da máquina.
- **Leituras no primário.** `GET` verifica que o primário vê um quórum com ack recente (lease de
  `timeout_fd`); entre a perda de liderança e esse limite uma leitura obsoleta é possível (depende
  da hipótese de sincronia parcial). Não há leitura em backups.
- **Falsas suspeitas.** Atrasos/perdas acima dos timeouts causam suspeitas indevidas, eleições e
  trocas de época desnecessárias (vivacidade degradada); a segurança permanece.
- **Desempenho.** Uma conexão TCP por mensagem e escritas serializadas no primário: simplicidade e
  independência entre mensagens, ao custo de vazão. Snapshots completos trafegam em uma linha JSON.
- **Sem reconfiguração de membros:** o conjunto de N nós é fixo (lista de pares).

## Requisitos

- Python 3.11+ (os nós usam apenas a biblioteca padrão); `pytest` para os testes
  (`pip install -r requirements.txt`).
- Docker + Compose v2 (opcional) para a execução em contêineres.

## Como executar

### Opção A — processos locais (sem Docker)

```bash
# construir: nada a compilar; instalar o pytest
pip install -r requirements.txt

# iniciar: 5 nós como processos independentes (portas livres sorteadas; Ctrl+C encerra)
python main.py cluster --n 5 --dir execucao --heartbeat 0.5 --timeout-fd 2 --timeout-rpc 1
#   imprime a linha "--pares 1=127.0.0.1:PORTA,2=..." usada pelos comandos abaixo
#   (use --porta-base 9000 para portas fixas: nó i escuta em 9000+i-1, e
#    PARES=1=127.0.0.1:9000,2=127.0.0.1:9001,3=127.0.0.1:9002,4=127.0.0.1:9003,5=127.0.0.1:9004)

# usar (em outro terminal; troque PARES pela linha impressa)
python main.py cliente --pares PARES put cor azul
python main.py cliente --pares PARES get cor          # pode começar por qualquer nó (redireciona)
python main.py admin   --pares PARES status           # líder, época, visão, seq, contadores
python main.py admin   --pares PARES status --no 3 --dados

# falhas
python main.py admin --pares PARES matar --no 5                           # crash do nó 5 (kill)
python main.py admin --pares PARES falhas --no 3 --descarte 0.1 --atraso 0.05 --jitter 0.05 --semente-falhas 7
python main.py admin --pares PARES falhas --no 5 --bloquear 1 2 3 4       # isola o nó 5
python main.py admin --pares PARES falhas --no 5 --limpar                 # remove as falhas

# recuperar o nó 5 (mesmo diretório de dados => crash-recovery); linha vem da mensagem de `cluster`
python main.py no --id 5 --pares PARES --dados execucao/dados --logs execucao/logs \
       --heartbeat 0.5 --timeout-fd 2 --timeout-rpc 1

# verificar as invariantes
python main.py verificar --logs execucao/logs --dados execucao/dados --vivos 1,2,3,4,5
```

```bash
# testar (unitários + integração com processos reais; ~40 s)
python -m pytest tests/ -v

# remover: Ctrl+C no terminal do `cluster` encerra o orquestrador e todos os nós que ele
# iniciou; nós iniciados à mão com `main.py no` (ex.: o nó 5 recuperado) são encerrados com
# `admin --pares PARES matar --no 5`. Depois, apagar os dados da execução:
rm -rf execucao            # PowerShell: Remove-Item -Recurse -Force execucao
```

### Opção B — Docker Compose (v2)

A única diferença é a configuração: o mesmo código lê `NO_ID`, `PARES` (`id=host:porta`, com o
nome do serviço como host), `NO_HOST`, `DIR_DADOS`, `DIR_LOGS` e as temporizações de variáveis de
ambiente (ver `compose.yaml`).

```bash
# construir
docker compose build

# iniciar os 5 nós (portas publicadas em 127.0.0.1:9001..9005; logs em ./execucao_compose/logs)
docker compose up -d
docker compose ps

# usar (do host)
P=1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003,4=127.0.0.1:9004,5=127.0.0.1:9005
python main.py cliente --pares $P put cor azul
python main.py admin   --pares $P status

# falhas: crash e recuperação
docker compose kill no5        # SIGKILL: crash do nó 5
docker compose start no5       # recuperação com o volume de dados preservado
python main.py admin --pares $P falhas --no 5 --bloquear 1 2 3 4    # partição na aplicação

# verificar os logs (invariantes I1-I4; para I5/I6 use `status --dados` ou os testes)
python main.py verificar --logs execucao_compose/logs

# testar: a suíte pytest usa processos locais; no compose o teste é o roteiro acima + verificador

# remover contêineres, rede e volumes de dados
docker compose down -v
rm -rf execucao_compose
```

> O `compose.yaml` não pôde ser executado no ambiente de desenvolvimento (Windows sem Docker);
> a execução local com processos é equivalente, pois só a configuração muda.

## Testes

`python -m pytest tests/ -v`

- `tests/test_unitarios.py` — réplica (ordem, idempotência, lacuna, histórico divergente,
  snapshot), persistência (recuperação, WAL truncado), injeção de falhas (semente, partição,
  filtro), mensagens, configuração, contadores e verificador (inclusive detecção de violações).
- `tests/test_estatistica.py` — percentis, média/desvio/IC95 t de Student, CSV agregado e sementes (campanha).
- `tests/test_integracao.py` — processos reais em localhost:
  - Bully normal (N=5 → líder 5, um líder por época), líder morre → 4 → 5 reinicia e retoma,
    várias falhas (5, 4, 3) e recuperação;
  - heartbeat: suspeita do líder morto; falsa suspeita (partição 1↔5 sem crash) registrada e revogada;
  - replicação normal (réplicas idênticas, custo `2(N-1)`, redirecionamento), backup morto sai da
    visão, **primário morto durante carga** (nenhuma escrita confirmada se perde, réplicas
    convergem), nó recuperado re-sincroniza e volta à visão, reinício do cluster inteiro a partir do
    disco, escrita sem quórum não é confirmada, **fencing** (primário isolado não confirma; ao curar,
    renuncia e sua escrita não confirmada é descartada) e perda aleatória de mensagens;
  - todos terminam com o verificador de logs (I1–I6).

## Resultados

Campanha completa em **37,8 min** (2265 s), 10 réplicas independentes por configuração; cada réplica é um
cluster novo de processos locais (`Popen`, 127.0.0.1, **sem contêineres**; o `compose.yaml` roda o mesmo código).
Máquina: Intel Core i5-13420H, 12 núcleos lógicos, 15,7 GB de RAM, Windows 11, Python 3.11.9, numpy 2.3.3,
matplotlib 3.10.6. Parâmetros, sementes, aquecimento e duração de cada experimento: `resultados/metadados.json`.

**Reproduzir** (a partir de `T2/`; `--so E1,E3` roda um subconjunto; `--regerar` refaz agregados e gráficos a
partir de `<saida>/brutos/`, sem medir):

```
pip install -r requirements.txt                 # pytest, numpy e matplotlib
python main.py experimentos --rapido            # smoke test: 2 réplicas, ~5–7 min, grava em resultados_rapido/
python main.py experimentos                     # campanha completa: 10 réplicas, ~38 min, SOBRESCREVE resultados/
```

O modo rápido grava por padrão em `resultados_rapido/` (ignorado pelo git) para não sobrescrever os dados
versionados da campanha completa; `--saida DIR` escolhe outro diretório.

Código: `src/experimentos.py`, `src/estatistica.py`, `src/graficos.py`. Saídas: `resultados/brutos/*.csv` (uma linha por
réplica; `e2_latencias.csv` tem cada escrita), `resultados/e*_agregado.csv` (média, desvio, IC95 t de Student, mín, p50/p95/p99,
máx; formato longo), `resultados/e2_latencia_agrupada.csv` (percentis sobre todas as escritas), gráficos `resultados/*.pdf|png`.

Convenções: `heartbeat = 0,1 s`, `timeout_fd = 0,5 s`, `timeout_rpc = 0,3 s` (salvo onde o parâmetro varia). Sementes =
`exp*10000 + config*100 + réplica` (também semeiam a injeção de falhas); a fase do crash em relação ao heartbeat é sorteada
por réplica. Tempos com `perf_counter` (campo `p` dos logs). Toda réplica termina com o verificador (I1–I6) e, nas de falha,
com uma checagem do lado do cliente (toda escrita confirmada está em todas as réplicas vivas): **310 réplicas, 0 erros,
0 violações, 0 escritas confirmadas perdidas** (coluna `invariantes_ok`). Nenhum processo restou ao final.

### E1 — Bully: crash do líder (N ∈ {3,5,7,9}); `resultados/e1_agregado.csv`

Mensagens = ELEICAO+OK+COORDENADOR **enviadas** (delta dos contadores; inclui as endereçadas ao nó morto). Teoria: se só o nó `m`
detecta, os iniciadores são `m..N−1` e o total é `(N−m)²+(N−1)`: melhor caso (`m=N−1`) = `N` (1 ELEICAO + N−1 COORDENADOR;
entregues: N−2), pior caso (`m=1`) = `N(N−1)` = O(N²). As variantes forçadas elevam o `timeout_fd` (30 s) dos demais nós;
`natural` = todos detectam por timeout.

| N | melhor (só N−1) | médio (só ⌈N/2⌉) | pior (só o nó 1) | natural | teoria: N / (N−m)²+N−1 / N(N−1) |
|--:|--:|--:|--:|--:|--:|
| 3 | 3 | 3 | 6 | 6 | 3 / 3 / 6 |
| 5 | 5 | 8 | 20 | 20 | 5 / 8 / 20 |
| 7 | 7 | 15 | 42 | 42 | 7 / 15 / 42 |
| 9 | 9 | 24 | 72 | 72 | 9 / 24 / 72 |

Desvio padrão **zero** nas 160 réplicas: a contagem é determinística e coincide exatamente com a teoria. Achado: a variante
*natural* já é o **pior caso**, porque com detecção por timeout o nó de menor id também detecta e a eleição em cascata (cada OK faz o
nó maior abrir a sua eleição) torna todos os N−1 vivos iniciadores (`n_iniciadores = N−1`). Mensagens entregues (soma nos vivos):
melhor `N−2`, pior `N(N−2)` (63 para N=9).

Tempos (variante `natural`, média ± IC95, s; as demais variantes são equivalentes dentro do IC):

| N | detecção (kill→1ª suspeita) | eleição (1ª `eleicao_iniciada`→COORDENADOR) | sincronização (→`lider_pronto`) | kill→líder pronto |
|--:|--:|--:|--:|--:|
| 3 | 0,510 ± 0,024 | 0,303 ± 0,004 | 0,301 ± 0,006 | 1,113 ± 0,025 |
| 5 | 0,482 ± 0,025 | 0,317 ± 0,013 | 0,304 ± 0,008 | 1,104 ± 0,033 |
| 7 | 0,470 ± 0,030 | 0,314 ± 0,005 | 0,301 ± 0,007 | 1,085 ± 0,034 |
| 9 | 0,476 ± 0,020 | 0,311 ± 0,008 | 0,311 ± 0,010 | 1,098 ± 0,024 |

Análise: a detecção (~0,47–0,54 s) é `timeout_fd` menos a fase do último heartbeat, independente de N. O tempo de eleição **não
cresce com N**, apesar das O(N²) mensagens: as mensagens de uma rodada vão em paralelo e a eleição termina quando o ELEICAO ao nó
morto estoura o `timeout_rpc` (0,3 s): o tempo de eleição ≈ T_OK do Bully. Idem a sincronização (SINC_CONSULTA ao nó morto espera
outros 0,3 s). No Windows conectar a uma porta fechada leva ~2 s em vez de falhar na hora (medido), então toda chamada ao nó morto só
termina pelo timeout. O custo O(N²) aparece em mensagens/carga, não em latência (em localhost). Gráficos: `e1_mensagens_vs_n`,
`e1_tempos_vs_n`, `e1_decomposicao`.

### E2 — Primário-backup: latência e vazão (1 cliente síncrono; 6 s medidos após 1,5 s de aquecimento descartado)

Latência = `perf_counter` no cliente (inclui abrir a conexão TCP). `srv p50` = latência medida no primário (trava até os ACK).
Valores: média entre as 10 réplicas ± IC95 (cada réplica gera seu p50/p95/p99); `resultados/e2_agregado.csv`.

| config | escritas/réplica | vazão (ops/s) | média (ms) | p50 | p95 | p99 | srv p50 | msgs/escrita |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| N=3, 0 ms | 302 | 50,2 ± 3,1 | 20,1 ± 1,3 | 17,7 ± 4,1 | 37,0 ± 0,5 | 40,6 ± 1,1 | 7,1 | 4 |
| N=5, 0 ms | 228 | 37,8 ± 3,1 | 26,8 ± 2,4 | 25,0 ± 4,3 | 45,4 ± 1,7 | 50,1 ± 1,9 | 12,8 | 8 |
| N=7, 0 ms | 194 | 32,3 ± 2,3 | 31,3 ± 2,4 | 28,6 ± 4,0 | 55,9 ± 1,8 | 63,1 ± 2,2 | 17,6 | 12 |
| N=9, 0 ms | 158 | 26,2 ± 1,6 | 38,5 ± 2,8 | 34,2 ± 3,6 | 69,5 ± 1,8 | 75,6 ± 2,0 | 23,5 | 16 |
| N=5, 5 ms | 167 | 27,7 ± 0,9 | 36,2 ± 1,1 | 35,0 ± 2,1 | 55,7 ± 1,9 | 61,8 ± 1,3 | 23,6 | 8 |
| N=5, 20 ms | 85 | 14,1 ± 0,3 | 71,2 ± 1,5 | 71,8 ± 2,1 | 88,8 ± 1,8 | 93,2 ± 2,6 | 59,7 | 8 |

Mensagens por escrita (REPLICAR+ACK enviados / escritas no servidor) = **exatamente 2(N−1)** (4, 8, 12, 16), desvio 0, nas 60
réplicas. A latência cresce ~linearmente com N (o primário abre N−1 conexões e trata N−1 ACK no mesmo laço de eventos): ≈ +2,7 ms
no `srv p50` por backup adicional. O atraso injetado soma ≈ 2×atraso por escrita (REPLICAR e ACK; os backups em paralelo): `srv p50`
sobe 10,8 ms (nominal 10) e 46,9 ms (nominal 40). A latência absoluta (≥ 17 ms com N=3) é dominada pelo custo de **abrir uma
conexão TCP por mensagem** no Windows (medido: `connect` com timeout ≈ 12 ms contra ≈ 2 ms bloqueante) e não representa Linux/contêineres;
o resultado relevante é a tendência em N e no atraso. Essa quantização torna a distribuição bimodal, daí o IC largo do p50 frente ao de
p95/p99. Vazão ≈ 1/latência (um cliente; o primário serializa as escritas sob trava). Gráficos: `e2_latencia_vs_n`,
`e2_latencia_vs_atraso`, `e2_vazao`, `e2_mensagens_por_escrita`.

### E3a — Failover vs `timeout_fd` (N=5, escritor contínuo, crash do primário; heartbeat = timeout/5)

Janela = maior intervalo entre duas escritas confirmadas consecutivas que cobre o crash (vista pelo cliente).
`resultados/e3a_agregado.csv`.

| timeout_fd (s) | detecção (s) | eleição | sincronização | kill→pronto | **janela** média ± IC95 | p50 | p95 | p99 |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 0,25 | 0,255 | 0,305 | 0,299 | 0,859 | **1,29 ± 0,04** | 1,30 | 1,34 | 1,37 |
| 0,5 | 0,522 | 0,311 | 0,304 | 1,137 | **1,36 ± 0,21** | 1,18 | 1,79 | 1,81 |
| 1,0 | 1,052 | 0,310 | 0,303 | 1,664 | **1,96 ± 0,18** | 1,78 | 2,26 | 2,27 |
| 2,0 | 2,189 | 0,313 | 0,301 | 2,803 | **3,10 ± 0,22** | 2,92 | 3,54 | 3,55 |

Análise: indisponibilidade ≈ `timeout_fd + 2·timeout_rpc` + retentativa do cliente. A detecção ≈ `timeout_fd` (compromisso clássico:
timeout curto reage rápido, mas gera falsas suspeitas, ver E3b); há um piso de ~0,6 s fixo (eleição + sincronização esperando o
`timeout_rpc` do nó morto; `timeout_rpc` não foi variado). A janela do cliente excede o `kill→lider_pronto` em 0,2–0,45 s: a primeira
tentativa ao primário morto só falha após o timeout de 0,5 s do cliente e o rodízio ainda revisita o nó morto, daí a bimodalidade
(p50 < média) em 0,5 e 1,0 s. Nenhuma escrita falhou (o cliente re-tenta) e nenhuma confirmada foi perdida. Gráfico:
`e3a_janela_vs_timeout` (janela e decomposição).

### E3b — Falsas suspeitas vs perda de mensagens (N=5, sem crash; janela de 15 s após 2 s de aquecimento; escritor a ~12/s)

Como ninguém cai, toda suspeita é falsa (coluna `susp_falsas_revogadas`: a suspeita foi revogada pelo mesmo incarnation). "Detector" =
`heartbeat_ausente` (seguidor→líder) + `backup_sem_resposta` (líder→backup); `replicar_sem_ack` = remoção de backup durante a escrita.
Eleições espúrias = `eleicao_iniciada` com motivo ≠ `inicio`. `resultados/e3b_agregado.csv`.

| perda | suspeitas do detector /min | suspeitas por replicação (15 s) | eleições espúrias /min | mudanças de líder (15 s) | escritas OK (15 s) |
|--:|--:|--:|--:|--:|--:|
| 0 | 0 | 0 | 0 | 0 | 188 |
| 0,05 | 5,2 ± 3,3 | 28,0 | 9,2 ± 8,6 | 0 | 75 |
| 0,10 | 26,0 ± 10,6 | 31,3 | 42,0 ± 15,0 | 0,4 | 47 |
| 0,20 | 72,4 ± 10,0 | 22,7 | 110 ± 14 | 0,8 | 15 |

Análise: sem perda, zero falsas suspeitas; a taxa cresce **mais que linearmente** com a perda (um seguidor só suspeita após ~5
heartbeats seguidos sem chegar, e a perda também derruba ACK e mensagens de eleição, realimentando o processo). Cerca de metade das
eleições espúrias é `eleicao_recebida` (um seguidor que suspeita arrasta os de id maior). Quase nunca trocam o líder: o líder atual,
ao receber ELEICAO, responde com heartbeat em vez de reeleger, e as épocas impedem dois primários. Mas custam mensagens e, nos
raros casos de troca, disponibilidade. O efeito mais visível é na vazão: cada REPLICAR/ACK perdido custa um `timeout_rpc` (0,3 s) e
a remoção do backup da visão (reentrada por ENTRAR), então as escritas confirmadas caem de 188 para 15 em 15 s com 20% de perda, sem
perder nenhuma confirmada. Gráfico: `e3b_falsas_suspeitas_vs_perda`. (Jitter não foi variado.)

### E4 (bônus) — Partição isolando o primário (N=5, ~4 s de partição, 10 réplicas)

| métrica | média ± IC95 | mín–máx |
|---|--:|--:|
| tentativas de escrita no primário isolado | 55,7 ± 1,4 | 52–59 |
| **escritas confirmadas pelo primário isolado** | **0 ± 0** | 0–0 (todas `sem_quorum`) |
| lado majoritário: novo líder (nó 4) pronto após o isolamento (s) | 1,131 ± 0,014 | 1,10–1,16 |
| escritas confirmadas no lado majoritário (de 20) | 20 ± 0 | 20–20 |
| renúncia do nó 5 após curar (s) | 0,261 ± 0,064 | 0,11–0,38 |
| nó 5 retoma a liderança (maior id) após curar (s) | 0,487 ± 0,090 | 0,27–0,64 |
| convergência total após curar (s) | 0,627 ± 0,098 | 0,38–0,77 |
| escritas órfãs (do primário isolado) no estado final | 0 | 0–0 |

O quórum de maioria faz o primário isolado nunca confirmar nada; o lado majoritário elege e continua escrevendo; ao curar, o
primário antigo renuncia pela época maior e tudo converge (I1–I6 ok, 0 confirmadas perdidas, 10/10 réplicas).

### Anomalias e observações

- **Variante natural = pior caso do Bully** (E1): não é bug, é a cascata do algoritmo.
- **Eleição e sincronização ≈ `timeout_rpc`**: efeito de esperar o nó morto (agravado no Windows, onde conectar a porta fechada leva
  ~2 s). Não variamos `timeout_rpc`; a relação é inferida do código e dos dados (0,29–0,36 s).
- **Granularidade do temporizador do Windows**: `asyncio.sleep(0,005)` durava ~16 ms (tick de 15,6 ms), distorcendo o atraso
  injetado e os timeouts curtos. Corrigido em `main.py` com `timeBeginPeriod(1)` no processo do nó.
- **Latência absoluta alta** por abrir uma conexão TCP por mensagem no Windows (E2); as conclusões são por tendência.
- Época colidida quando o maior id volta (comportamento conhecido e seguro): nada anômalo observado; I1 sempre satisfeita.
- Em E3a a janela inclui a retentativa do cliente (timeout de 0,5 s por tentativa); `kill→pronto` é o lado do servidor.
- `experimentos/` ficou reservado e vazio: a campanha está em `src/experimentos.py` (como o T1).

## Divisão de trabalho

- **Caique Pinheiro Andrade:** implementação dos algoritmos e da infraestrutura
  de comunicação, e execução da campanha experimental.
- **Luiz Henrique Marques Gonçalves:** modelo do sistema e argumentos de
  correção, análise dos resultados e redação do relatório.
