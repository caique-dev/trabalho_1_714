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
                   (dentro ou fora de contêiner: os nós são sempre processos independentes)
tests/             unitários (sem rede) e de integração (processos reais)
Dockerfile, compose.yaml, .dockerignore   o mesmo código como 5 contêineres + serviços `testes` e `experimentos`
experimentos/README.md (aponta para src/experimentos.py), resultados/, relatorio/
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
`lider`, `epoca`, `pronto`, `quorum_ativo`, `visao`, `seq`, `ult_epoca`, `hash` do estado, `n_chaves`, `quorum`,
`suspeitas`, `suspeitas_falsas`, `uptime`, parâmetros, estado da injeção de falhas e
`contadores` (`enviadas`/`recebidas` por tipo, `externas_recebidas`, `descartadas`).

**`pronto` x `quorum_ativo`.** `pronto` significa que o líder concluiu a fase de sincronização da sua
época (é o que o verificador usa, junto com o evento `lider_pronto`) e **não** muda quando o líder é
isolado. `quorum_ativo` (novo, só informativo: nenhum protocolo o consulta) diz se **agora** o líder
enxerga um quórum: `1 + |membros da visão com HB_ACK/ACK há menos de timeout_fd| >= quórum` (a mesma
verificação de lease que o `GET` usa). É `true`/`false` no líder e `null` nos seguidores. Um primário
isolado por partição fica `papel=lider, pronto=true, quorum_ativo=false` (em até `timeout_fd` após o
isolamento) e não confirma escritas nem serve leituras; `admin status` imprime uma linha-resumo com
esses campos e, nesse caso, um aviso explícito, seguida do JSON completo:

```
no 5: papel=lider epoca=2 lider=5 pronto=true quorum_ativo=false visao=[] seq=3
  >>> ATENÇÃO: líder SEM quórum ativo (isolado ou maioria indisponível): não confirma escritas nem serve leituras
  {"id": 5, "papel": "lider", ...}
```

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

- **Nós, cliente, admin e verificador: só Python 3.11+ e a biblioteca padrão** (nenhum `pip install`).
  Os comandos `cliente`, `admin`, `verificar` e `cluster` rodam **no host, com o Python do sistema**,
  mesmo quando os nós estão em contêineres (eles falam com os nós por TCP em `127.0.0.1`).
- `requirements.txt` — só para testes e experimentos:
  - `pytest`: suíte de testes (`tests/`);
  - `numpy`: estatística da campanha experimental (médias, desvios, IC95, percentis);
  - `matplotlib`: gráficos da campanha.

  Para rodar testes/experimentos **no host**, sugere-se um ambiente virtual:

  ```bash
  python -m venv .venv
  source .venv/bin/activate            # PowerShell: .venv\Scripts\Activate.ps1
  pip install -r requirements.txt
  ```

  Via Docker (abaixo) nada disso é necessário: a imagem já instala as dependências.
- Docker + Compose v2 (opcional) para a execução em contêineres, testes e experimentos no contêiner.

## Como executar

Há duas formas de subir o cluster (A: processos locais; B: Docker Compose). A lista de pares usada
pelos comandos `cliente`/`admin` muda só nas portas:

| | portas no host | variável de pares |
|---|---|---|
| A — `cluster --porta-base 9000` | nó *i* em `9000+i-1` (9000..9004); sem `--porta-base`, portas livres sorteadas e a linha `--pares ...` é impressa | `1=127.0.0.1:9000,...,5=127.0.0.1:9004` |
| B — Compose | nó *i* em `9000+i` (9001..9005), mapeadas da porta `9000` de cada contêiner | `1=127.0.0.1:9001,...,5=127.0.0.1:9005` |

(No compose todos os contêineres escutam em `9000` *dentro* da rede `mc714-t2_default` e a lista
interna é `1=no1:9000,...`; o mapeamento `127.0.0.1:900i -> 9000` existe só para o host.)

### Opção A — processos locais (sem Docker)

```bash
# construir: nada a compilar (só biblioteca padrão para os nós)

# iniciar: 5 nós como processos independentes; Ctrl+C encerra (ver "Encerramento" abaixo)
python main.py cluster --n 5 --dir execucao --porta-base 9000 --heartbeat 0.5 --timeout-fd 2 --timeout-rpc 1
#   imprime "--pares 1=127.0.0.1:9000,..." e "Líder eleito: nó 5"
```

Em outro terminal, defina a variável de pares:

```bash
# bash
PARES=1=127.0.0.1:9000,2=127.0.0.1:9001,3=127.0.0.1:9002,4=127.0.0.1:9003,5=127.0.0.1:9004
```
```powershell
# PowerShell
$PARES = "1=127.0.0.1:9000,2=127.0.0.1:9001,3=127.0.0.1:9002,4=127.0.0.1:9003,5=127.0.0.1:9004"
```

Os comandos abaixo valem em bash e PowerShell (`$PARES` nos dois):

```bash
python main.py cliente --pares $PARES put cor azul
python main.py cliente --pares $PARES get cor          # pode começar por qualquer nó (redireciona)
python main.py admin   --pares $PARES status           # resumo por nó + JSON (líder, época, visão, seq, contadores)
python main.py admin   --pares $PARES status --no 3 --dados

# falhas
python main.py admin --pares $PARES matar --no 5                           # crash do nó 5 (kill)
python main.py admin --pares $PARES falhas --no 3 --descarte 0.1 --atraso 0.05 --jitter 0.05 --semente-falhas 7
python main.py admin --pares $PARES falhas --no 5 --bloquear 1 2 3 4       # isola o nó 5
python main.py admin --pares $PARES status --no 5                          # papel=lider pronto=true quorum_ativo=false + aviso
python main.py admin --pares $PARES falhas --no 5 --limpar                 # remove as falhas

# recuperar o nó 5 (mesmo diretório de dados => crash-recovery)
python main.py no --id 5 --pares $PARES --dados execucao/dados --logs execucao/logs \
       --heartbeat 0.5 --timeout-fd 2 --timeout-rpc 1        # PowerShell: use ` no lugar de \

# verificar as invariantes
python main.py verificar --logs execucao/logs --dados execucao/dados --vivos 1,2,3,4,5
```

```bash
# testar no host (unitários + integração com processos reais; ~45 s; precisa do pip install acima)
python -m pytest tests/ -q
```

**Encerramento.** `Ctrl+C` no terminal do `cluster` encerra o orquestrador e todos os nós que ele
iniciou. O orquestrador também **encerra sozinho** (imprimindo "Todos os nós iniciados por este
orquestrador terminaram; encerrando.") quando nenhum dos processos que ele iniciou está vivo, por
exemplo depois de `admin matar` em todos os nós. Nós iniciados à mão com `main.py no` (como o nó 5
recuperado acima) não contam para isso; encerre-os com `admin --pares $PARES matar --no 5`. Depois,
apague os dados da execução: `rm -rf execucao` (PowerShell: `Remove-Item -Recurse -Force execucao`).

> `admin matar` envia o sinal ao **pid informado pelo nó**, então só serve para nós da mesma
> máquina (opção A). Se o nó informa outro hostname (um contêiner), o comando recusa e indica
> `docker compose kill noN`.

### Opção B — Docker Compose (v2)

Uma única imagem (`mc714-t2`: `python:3.11-slim` + dependências do `requirements.txt` + `main.py`,
`src/` e `tests/`) serve a todos os serviços; o `.dockerignore` deixa de fora `.git`, `.venv`,
`__pycache__`, `execucao*`, `resultados*`, `relatorio*` etc. O mesmo código lê `NO_ID`, `PARES`
(`id=host:porta`, com o nome do serviço como host), `NO_HOST`, `DIR_DADOS`, `DIR_LOGS` e as
temporizações de variáveis de ambiente (ver `compose.yaml`). As temporizações da demonstração
(`HEARTBEAT=0.5`, `TIMEOUT_FD=2.0`, `TIMEOUT_RPC=1.0`) são mais folgadas que as da campanha
(0,1 / 0,5 / 0,3 s), para que dê tempo de observar cada passo à mão; a campanha define as suas.

```bash
# construir (uma imagem; a camada de dependências fica em cache enquanto requirements.txt não mudar)
docker compose build

# iniciar os 5 nós (portas em 127.0.0.1:9001..9005; logs em ./execucao_compose/logs)
docker compose up -d
docker compose ps
```

**Quanto esperar.** Depois de `up -d`, o líder costuma aparecer em 1–3 s (o Python dos nós leva
algumas centenas de ms para subir, e a eleição e a sincronização terminam logo em seguida); se
o `status` ainda não mostrar líder, espere alguns segundos e repita. Confirme que há líder com
`admin status` (variável de pares do compose, portas 9001..9005):

```bash
# bash
PARES=1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003,4=127.0.0.1:9004,5=127.0.0.1:9005
```
```powershell
# PowerShell
$PARES = "1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003,4=127.0.0.1:9004,5=127.0.0.1:9005"
```
```bash
python main.py admin --pares $PARES status
# esperado: nó 5 com "papel=lider ... pronto=true quorum_ativo=true visao=[1, 2, 3, 4]"
#           e os nós 1-4 com "papel=seguidor lider=5 pronto=false"
```

Demonstração (do host, com o Python do sistema; sem dependências):

```bash
python main.py cliente --pares $PARES put cor azul
python main.py cliente --pares $PARES get cor
python main.py admin   --pares $PARES status

# crash e recuperação do nó 5 (kill/start do contêiner; o volume de dados é preservado)
docker compose kill no5
docker compose start no5
python main.py admin --pares $PARES status      # aguarde ~5 s: o nó 5 volta e retoma a liderança (maior id)

# partição na aplicação: o primário isolado fica com quorum_ativo=false
python main.py admin --pares $PARES falhas --no 5 --bloquear 1 2 3 4
python main.py admin --pares $PARES status --no 5     # papel=lider pronto=true quorum_ativo=false + aviso
python main.py admin --pares $PARES falhas --no 5 --limpar

# verificar os logs (invariantes I1-I4; para I5/I6 use `status --dados` ou os testes)
python main.py verificar --logs execucao_compose/logs
```

**Testes dentro do contêiner** (serviço `testes`, profile `testes`; mesma suíte de `tests/`, ~30 s):

```bash
docker compose run --rm testes                              # pytest tests/ -q
docker compose run --rm testes tests/test_unitarios.py -q   # argumentos extras substituem o padrão
```

**Experimentos dentro do contêiner** (serviço `experimentos`, profile `experimentos`; os argumentos
vão para `python main.py experimentos`; as saídas aparecem no host por bind mount de `./resultados`
e `./resultados_rapido`):

```bash
docker compose run --rm experimentos --rapido                        # smoke test, ~4 min -> ./resultados_rapido
docker compose run --rm experimentos --rapido --so E4 --replicas 1   # menor smoke test, ~10 s
docker compose run --rm experimentos                                 # CAMPANHA COMPLETA (10 réplicas, ~32 min)
                                                                     # -> ./resultados (SOBRESCREVE os dados versionados)
```

Dentro desses contêineres os nós continuam sendo processos independentes (`python main.py no ...`)
que só se comunicam por TCP em `127.0.0.1` do próprio contêiner (`src/cluster.py`). `docker compose
up -d` **não** inicia `testes` nem `experimentos` (profiles). O `metadados.json` gerado registra
`"conteiner": true`, a distribuição e o kernel Linux, as CPUs disponíveis (afinidade e limite de
cgroup), a RAM e as versões; sem repositório git no contêiner o commit só aparece se a variável
`GIT_COMMIT` for passada (`docker compose run --rm -e GIT_COMMIT=$(git rev-parse HEAD) experimentos ...`).
No Docker Desktop (Windows/macOS) o contêiner roda numa VM Linux: CPUs e RAM são os da VM. Em Linux
a conexão a um nó morto falha imediatamente (no Windows leva ~2 s), então os tempos de eleição e
sincronização medidos no contêiner são de milissegundos. Os dados de `resultados/` são justamente desta campanha em contêiner
(ver `## Resultados`); medidas em processos locais no Windows dão tempos de eleição bem maiores (≈ `timeout_rpc`) e não são comparáveis.

**Remover tudo.**

```bash
docker compose down -v --rmi local            # contêineres, rede, volumes de dados e a imagem mc714-t2
rm -rf execucao_compose resultados_rapido     # pastas geradas (PowerShell: Remove-Item -Recurse -Force execucao_compose, resultados_rapido)
```

(`resultados/` é entregável versionado: não apague.) `testes` e `experimentos` compartilham a mesma
imagem, portanto o `down --rmi local` também a remove depois deles.

## Testes

`python -m pytest tests/ -v` (host) ou `docker compose run --rm testes` (contêiner Linux); 53 testes.

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
  - `quorum_ativo` do status: o líder isolado fica `pronto=true, quorum_ativo=false` e volta a `true` ao curar;
  - `main.py cluster` encerra sozinho quando todos os nós que iniciou morrem;
  - todos terminam com o verificador de logs (I1–I6).

## Resultados

Campanha completa em **32,3 min** (1941 s), 10 réplicas independentes por configuração, executada **dentro de um
contêiner Docker** (`docker compose run --rm experimentos`): cada réplica é um cluster novo de processos
independentes (`Popen`, 127.0.0.1) que só se comunicam por TCP, orquestrado por `src/cluster.py`. Não é um contêiner por
nó (esse arranjo, do `compose up`, foi exercitado como demonstração, não para medidas).
Ambiente (`resultados/metadados.json`): contêiner Debian 13 (kernel Linux 6.18.54.1-microsoft-standard-WSL2, Docker Desktop
sobre Windows 11), Intel Core i5-13420H, 12 núcleos lógicos, 7,6 GB de RAM visíveis ao contêiner, Python 3.11.18, numpy 2.4.6,
matplotlib 3.11.2, pytest 9.2.0. Parâmetros, sementes, aquecimento e duração de cada experimento: `resultados/metadados.json`.

**Reproduzir** (a partir de `T2/`; `--so E1,E3` roda um subconjunto; `--regerar` refaz agregados e gráficos a
partir de `<saida>/brutos/`, sem medir):

```
pip install -r requirements.txt                 # pytest, numpy e matplotlib (sugere-se um venv)
python main.py experimentos --rapido            # smoke test: 2 réplicas, grava em resultados_rapido/
python main.py experimentos                     # campanha completa: 10 réplicas, SOBRESCREVE resultados/
```

Ou, sem instalar nada no host, dentro do contêiner (Linux; mesmos argumentos; saídas por bind mount):

```
docker compose run --rm experimentos --rapido   # smoke test -> ./resultados_rapido
docker compose run --rm experimentos            # campanha completa (~32 min) -> ./resultados (SOBRESCREVE)
```

Os dados versionados em `resultados/` foram gerados **dentro do contêiner** (comando acima). Uma campanha anterior, como
processos locais no Windows, foi substituída; seus números (por exemplo, eleição ≈ 0,3 s e custo sempre `N(N−1)` na variante
natural) **não valem mais** e só existem no histórico do git. Rodar a campanha em outra máquina muda os tempos absolutos.

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

**Diferença importante entre Linux e Windows (afeta o E1 e o E3a).** No Bully o candidato chama ELEICAO nos ids maiores e espera
cada resposta por no máximo `timeout_rpc`; uma falha de conexão retorna `None` (`src/bully.py`, `src/transporte.py`). Em Linux
(contêiner), conectar a uma porta fechada de localhost é recusado na hora (RST), então a chamada ao nó morto termina em
milissegundos. No Windows o SO retenta por ~2 s e a chamada só terminava no `timeout_rpc` (por isso a campanha antiga mostrava
eleição ≈ 0,3 s). Numa rede real em que o nó caído não responde RST (máquina desligada ou inalcançável), o comportamento
esperado volta a ser dominado por `timeout_rpc`; isso **não foi medido** (`timeout_rpc` ficou fixo em 0,3 s).

### E1 — Bully: crash do líder (N ∈ {3,5,7,9}); `resultados/e1_agregado.csv`

Mensagens = ELEICAO+OK+COORDENADOR **enviadas** (delta dos contadores; inclui as endereçadas ao nó morto). Teoria: se só o nó `m`
detecta, os iniciadores são `m..N−1` e o total é `(N−m)²+(N−1)`: melhor caso (`m=N−1`) = `N` (1 ELEICAO + N−1 COORDENADOR;
entregues: N−2), pior caso (`m=1`) = `N(N−1)` = O(N²). As variantes forçadas elevam o `timeout_fd` (30 s) dos demais nós;
`natural` = todos detectam por timeout.

| N | melhor (só N−1) | médio (só ⌈N/2⌉) | pior (só o nó 1) | natural: média ± IC95 (mín–máx; mediana) | teoria: N / (N−m)²+N−1 / N(N−1) |
|--:|--:|--:|--:|--:|--:|
| 3 | 3 | 3 | 6 | 5,4 ± 0,9 (3–6; 6) | 3 / 3 / 6 |
| 5 | 5 | 8 | 20 | 11,4 ± 4,8 (5–20; 10,5) | 5 / 8 / 20 |
| 7 | 7 | 15 | 42 | 23,2 ± 9,0 (7–45; 18,5) | 7 / 15 / 42 |
| 9 | 9 | 24 | 72 | 40,9 ± 17,5 (12–72; 44) | 9 / 24 / 72 |

Variantes forçadas (120 réplicas): desvio padrão **zero**, contagem determinística e igual à teoria (`msg_total_env =
msg_teorico_env` nas 120). A variante **natural** *não* coincide mais com o pior caso: como a conexão ao nó morto é recusada na
hora, a eleição do primeiro seguidor que detecta termina em poucos ms, antes que os demais detectem. O custo depende de quem
detecta primeiro (o menor iniciador `m`): a cascata (cada OK faz o nó maior abrir a sua eleição) leva os ids `m..N−1` a
iniciar. Em 37 das 40 réplicas naturais o total é `(N−m)²+(N−1)` com `N−m = n_iniciadores`; nas 3 restantes (N=7 duas vezes,
N=9 uma) um nó abriu duas rodadas (`rodadas = n_iniciadores+1`) e somou 3 mensagens; uma delas (45, N=7) excedeu `N(N−1)=42`, ou
seja, `N(N−1)` é o pior caso para uma rodada por iniciador, não um limite absoluto sob detecções concorrentes. Mais de um nó
detectou o crash em 12 das 40 réplicas (`n_detectores`). Mensagens entregues (soma nos vivos): melhor `N−2`, pior `N(N−2)`.

Tempos (variante `natural`, média ± IC95; as demais variantes diferem na eleição, ver abaixo):

| N | detecção (kill→1ª suspeita, s) | eleição (1ª `eleicao_iniciada`→COORDENADOR, ms) | sincronização (→`lider_pronto`, ms) | kill→líder pronto (s) |
|--:|--:|--:|--:|--:|
| 3 | 0,474 ± 0,042 | 1,4 ± 0,5 | 2,3 ± 0,2 | 0,478 ± 0,042 |
| 5 | 0,453 ± 0,025 | 1,4 ± 0,6 | 3,0 ± 0,4 | 0,457 ± 0,025 |
| 7 | 0,462 ± 0,027 | 2,1 ± 0,7 | 5,2 ± 0,7 | 0,470 ± 0,027 |
| 9 | 0,470 ± 0,014 | 3,0 ± 1,0 | 7,4 ± 1,4 | 0,480 ± 0,015 |

Análise: a detecção (média 0,45–0,47 s na variante natural; 0,46–0,51 s nas demais) é `timeout_fd` menos a fase do último
heartbeat, independente de N, e responde por mais de 97% do kill→pronto. Eleição e sincronização duram **milissegundos** e
crescem com N: na variante natural, 1,4–3,0 ms e 2,3–7,4 ms; no pior caso forçado a eleição vai de 1,8 ms (N=3) a 5,0 ms
(N=9), e no melhor caso fica em 0,6–0,9 ms. O custo O(N²) aparece em mensagens, não em latência (loopback, sem perda). Isso
decorre de a chamada ao nó morto falhar de imediato (RST); com um nó que apenas se cala, esperaríamos que cada rodada durasse até
`timeout_rpc` e que os demais detectassem dentro dela, aproximando a variante natural do pior caso (hipótese, não medida).
Gráficos: `e1_mensagens_vs_n`, `e1_tempos_vs_n`, `e1_decomposicao`.

### E2 — Primário-backup: latência e vazão (1 cliente síncrono; 6 s medidos após 1,5 s de aquecimento descartado)

Latência = `perf_counter` no cliente (inclui abrir a conexão TCP). `srv p50` = latência medida no primário (trava até os ACK).
Valores: média entre as 10 réplicas ± IC95 (cada réplica gera seu p50/p95/p99); `resultados/e2_agregado.csv`.

| config | escritas/réplica | vazão (ops/s) | média (ms) | p50 | p95 | p99 | srv p50 | msgs/escrita |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| N=3, 0 ms | 3004 | 501 ± 56 | 2,04 ± 0,26 | 1,96 ± 0,27 | 2,77 ± 0,48 | 3,38 ± 0,56 | 1,23 | 4 |
| N=5, 0 ms | 1762 | 294 ± 22 | 3,42 ± 0,25 | 3,30 ± 0,24 | 4,46 ± 0,43 | 5,58 ± 0,62 | 2,33 | 8 |
| N=7, 0 ms | 1357 | 226 ± 11 | 4,43 ± 0,22 | 4,23 ± 0,22 | 6,05 ± 0,38 | 7,70 ± 0,53 | 3,17 | 12 |
| N=9, 0 ms | 1022 | 170 ± 19 | 6,01 ± 0,77 | 5,62 ± 0,64 | 9,07 ± 1,73 | 12,57 ± 4,50 | 4,35 | 16 |
| N=5, 5 ms | 436 | 72,6 ± 2,0 | 13,78 ± 0,38 | 13,73 ± 0,36 | 15,26 ± 0,61 | 16,08 ± 0,74 | 12,78 | 8 |
| N=5, 20 ms | 136 | 22,6 ± 0,2 | 44,24 ± 0,46 | 44,10 ± 0,45 | 45,76 ± 0,60 | 46,48 ± 0,88 | 43,20 | 8 |

Mensagens por escrita (REPLICAR+ACK enviados / escritas no servidor) = **exatamente 2(N−1)** (4, 8, 12, 16), desvio 0, nas 60
réplicas. A latência cresce com N (o primário abre N−1 conexões e trata N−1 ACK no mesmo laço de eventos): `srv p50` sobe de
1,23 a 4,35 ms de N=3 a 9, ≈ +0,52 ms por backup adicional, e a dispersão também (IC95 do p99: ± 0,56 ms em N=3, ± 4,50 ms em
N=9). O atraso injetado soma ≈ 2×atraso por escrita (REPLICAR e ACK; os backups em paralelo): `srv p50` sobe 10,4 ms (nominal
10) e 40,9 ms (nominal 40) sobre os 2,33 ms sem atraso. A vazão cai na mesma proporção (um cliente; o primário serializa as
escritas sob trava). As latências absolutas valem para loopback num contêiner, com uma conexão TCP por mensagem; para redes
reais, vale a tendência em N e no atraso. Gráficos: `e2_latencia_vs_n`, `e2_latencia_vs_atraso`, `e2_vazao`,
`e2_mensagens_por_escrita`.

### E3a — Failover vs `timeout_fd` (N=5, escritor contínuo, crash do primário; heartbeat = timeout/5)

Janela = maior intervalo entre duas escritas confirmadas consecutivas que cobre o crash (vista pelo cliente).
`resultados/e3a_agregado.csv`.

| timeout_fd (s) | detecção (s) | eleição | sincronização | kill→pronto (s) | **janela** média ± IC95 (s) | p50 | p95 | p99 |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 0,25 | 0,259 | 2,4 ms | 5,3 ms | 0,267 | **0,292 ± 0,020** | 0,281 | 0,335 | 0,348 |
| 0,5 | 0,521 | 2,6 ms | 7,2 ms | 0,531 | **0,548 ± 0,012** | 0,543 | 0,577 | 0,586 |
| 1,0 | 1,036 | 2,0 ms | 3,8 ms | 1,042 | **1,062 ± 0,030** | 1,053 | 1,118 | 1,122 |
| 2,0 | 2,128 | 2,2 ms | 6,8 ms | 2,137 | **2,166 ± 0,075** | 2,168 | 2,307 | 2,321 |

Análise: a janela é a detecção mais 0,026–0,037 s (eleição + sincronização somam 6–10 ms; o restante vem da espera de 20–50 ms
entre retentativas do cliente e da primeira escrita após a eleição). A detecção fica 1,04–1,06 vez `timeout_fd` e a janela
1,06–1,17 vez: a indisponibilidade é, na prática, `timeout_fd` (compromisso clássico: timeout curto reage rápido, mas gera falsas
suspeitas, ver E3b). Como a chamada ao primário morto falha de imediato (RST), não aparecem pisos de `timeout_rpc` nem do
timeout de 0,5 s por tentativa do cliente; numa rede que não recusa conexões, esperaríamos que voltem (hipótese, não medida).
Nenhuma escrita falhou (`escritas_falhas = 0` nas 40 réplicas) e nenhuma confirmada foi perdida. Gráfico: `e3a_janela_vs_timeout`.

### E3b — Falsas suspeitas vs perda de mensagens (N=5, sem crash; janela de 15 s após 2 s de aquecimento; escritor a ~20/s)

Como ninguém cai, toda suspeita é falsa (coluna `susp_falsas_revogadas`: a suspeita foi revogada pelo mesmo incarnation). "Detector" =
`heartbeat_ausente` (seguidor→líder) + `backup_sem_resposta` (líder→backup); `replicar_sem_ack` = remoção de backup durante a escrita.
Eleições espúrias = `eleicao_iniciada` com motivo ≠ `inicio`. `resultados/e3b_agregado.csv`.

| perda | suspeitas do detector /min | suspeitas totais (15 s) | eleições espúrias /min | mudanças de líder (15 s) | `eleicao_recebida` (15 s) | escritas OK (15 s) |
|--:|--:|--:|--:|--:|--:|--:|
| 0 | 0 | 0 | 0 | 0 | 0 | 277,4 ± 1,9 |
| 0,05 | 5,6 ± 4,3 | 31,2 | 7,6 ± 5,9 | 0 | 1,0 | 95,0 ± 5,7 |
| 0,10 | 20,8 ± 3,5 | 38,4 | 36,4 ± 7,3 | 0,4 | 5,7 | 48,1 ± 6,6 |
| 0,20 | 75,2 ± 9,6 | 40,4 | 117,2 ± 24,7 | 2,0 | 16,0 | 15,4 ± 3,3 |

Análise: sem perda, zero falsas suspeitas e zero eleições espúrias; a taxa de suspeitas do detector cresce **mais que
linearmente** com a perda (×3,7 de 5% a 10% e ×3,6 de 10% a 20%; um seguidor só suspeita após ~5 heartbeats seguidos sem
chegar, e a perda também derruba ACK e mensagens de eleição, realimentando o processo). As suspeitas totais por janela são
dominadas, nas perdas baixas, por `replicar_sem_ack` (com 5%: 29,8 de 31,2). A troca de líder é rara (média 0,4 e 2,0 por janela
com 10% e 20%; máximo 2 e 6): o líder atual, ao receber ELEICAO, responde com heartbeat em vez de reeleger, e as épocas impedem
dois primários. O efeito mais visível é na vazão: cada REPLICAR/ACK perdido custa um `timeout_rpc` (0,3 s) e a remoção do backup
da visão (reentrada por ENTRAR), e o commit exige quórum, então as escritas confirmadas caem de 277,4 para 15,4 em 15 s com 20%
de perda (−94%), sem perder nenhuma confirmada. Gráfico: `e3b_falsas_suspeitas_vs_perda`. (Jitter não foi variado.)

### E4 (bônus) — Partição isolando o primário (N=5, ~4 s de partição, 10 réplicas)

| métrica | média ± IC95 | mín–máx |
|---|--:|--:|
| tentativas de escrita no primário isolado | 72,0 ± 0,3 | 71–73 |
| **escritas confirmadas pelo primário isolado** | **0 ± 0** | 0–0 (todas `sem_quorum`) |
| lado majoritário: novo líder (nó 4) pronto após o isolamento (s) | 1,000 ± 0,158 | 0,679–1,164 |
| escritas confirmadas no lado majoritário (de 20) | 20 ± 0 | 20–20 |
| renúncia do nó 5 após curar (s) | 0,105 ± 0,051 | 0,015–0,197 |
| nó 5 retoma a liderança (maior id) após curar (s) | 0,149 ± 0,046 | 0,067–0,235 |
| convergência total após curar (s) | 0,157 ± 0,045 | 0,078–0,243 |
| escritas órfãs (do primário isolado) no estado final | 0 | 0–0 |

O quórum de maioria faz o primário isolado nunca confirmar nada; o lado majoritário elege e continua escrevendo; ao curar, o
primário antigo renuncia pela época maior e tudo converge (I1–I6 ok, 0 confirmadas perdidas, 10/10 réplicas). O tempo até o nó 4
ficar pronto é bimodal: 3 réplicas em ≈ 0,68 s e 7 entre 1,11 e 1,16 s (diferença de ≈ 0,44 s; **causa não investigada**).

### Anomalias e observações

- **Variante natural do Bully** (E1): com a conexão recusada na hora (Linux), deixou de ser sempre o pior caso; o custo varia entre
  `N` e `N(N−1)` conforme quem detecta primeiro, e uma réplica (N=7) chegou a 45 > 42 por uma rodada extra.
- **Eleição e sincronização em milissegundos** (E1, E3a): efeito de a chamada ao nó morto falhar de imediato; numa rede em que o
  nó apenas se cala, esperamos tempos dominados por `timeout_rpc` (não medido).
- **Nós como processos de um único contêiner**: a campanha não usa um contêiner por nó; crash = morte de processo, rede = loopback.
- **Latência absoluta** de milissegundos (E2): loopback com uma conexão TCP por mensagem; as conclusões são por tendência.
- Época colidida quando o maior id volta (comportamento conhecido e seguro): nada anômalo observado; I1 sempre satisfeita.
- Em E3a a janela inclui a retentativa do cliente (espera de 20–50 ms entre tentativas); `kill→pronto` é o lado do servidor.
- `experimentos/` só tem um README que aponta para `src/experimentos.py`, onde está a campanha (como no T1).

## Divisão de trabalho

- **Caique Pinheiro Andrade:** implementação dos algoritmos e da infraestrutura
  de comunicação, e execução da campanha experimental.
- **Luiz Henrique Marques Gonçalves:** modelo do sistema e argumentos de
  correção, análise dos resultados e redação do relatório.
