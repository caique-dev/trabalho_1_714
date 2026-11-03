"""Persistência em disco para crash-recovery.

Cada nó tem um diretório próprio com:

    meta.json            {"epoca", "lider"}: maior época vista e o líder que o
                         nó reconheceu nela (equivale ao "voto" do Raft: impede
                         aceitar dois líderes diferentes na mesma época);
    snapshot.json        {"geracao", "seq", "ult_epoca", "dados"};
    wal.<geracao>.jsonl  log de escritas posteriores ao snapshot, uma por linha.

Por que WAL + snapshot: gravar o dicionário inteiro a cada PUT custaria O(tamanho)
por escrita; o WAL custa O(1). O snapshot é regravado ao iniciar (compactação) e
ao instalar um estado vindo de outro nó. A troca é atômica (``os.replace``) e o
WAL novo tem número de geração diferente, de modo que uma queda no meio da troca
nunca mistura histórico antigo com snapshot novo.

Uma queda do processo (``kill``) preserva os dados já entregues ao SO com
``flush``; ``fsync`` só é necessário para queda da máquina e fica opcional.
Uma última linha truncada do WAL (queda no meio da escrita) é descartada.
"""

import json
import os
import time


def _ler_json(caminho):
    try:
        with open(caminho, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def _escrever_atomico(caminho, objeto, fsync):
    tmp = caminho + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(objeto, f, ensure_ascii=False)
        f.flush()
        if fsync:
            os.fsync(f.fileno())
    # No Windows o replace pode falhar transitoriamente (antivírus, indexador).
    for tentativa in range(20):
        try:
            os.replace(tmp, caminho)
            return
        except PermissionError:
            time.sleep(0.01 * (tentativa + 1))
    os.replace(tmp, caminho)


def _reconstruir(diretorio):
    """Lê meta, snapshot e WAL sem alterar nada. Devolve (meta, geracao, dados, seq, ult_epoca)."""
    meta = _ler_json(os.path.join(diretorio, "meta.json")) or {"epoca": 0, "lider": None}
    snap = _ler_json(os.path.join(diretorio, "snapshot.json")) or \
        {"geracao": 0, "seq": 0, "ult_epoca": 0, "dados": {}}
    dados = dict(snap["dados"])
    seq, ult = snap["seq"], snap["ult_epoca"]
    wal = os.path.join(diretorio, f"wal.{snap['geracao']}.jsonl")
    if os.path.exists(wal):
        with open(wal, encoding="utf-8") as f:
            for linha in f:
                try:
                    r = json.loads(linha)
                except ValueError:
                    break  # linha truncada pela queda: fim do prefixo válido
                if r.get("seq") != seq + 1:
                    break
                dados[r["k"]] = r["v"]
                seq, ult = r["seq"], r["e"]
    return meta, snap["geracao"], dados, seq, ult


def ler_estado_disco(diretorio):
    """Estado persistido de um nó (somente leitura): ``{"dados", "seq", "ult_epoca", "epoca"}``."""
    meta, _, dados, seq, ult = _reconstruir(diretorio)
    return {"dados": dados, "seq": seq, "ult_epoca": ult, "epoca": meta["epoca"]}


class Armazenamento:
    def __init__(self, diretorio, fsync=False):
        self.dir = diretorio
        self.fsync = fsync
        os.makedirs(diretorio, exist_ok=True)
        meta, geracao, dados, seq, ult = _reconstruir(diretorio)
        self.meta = meta
        self.geracao = geracao
        self.estado_inicial = (dados, seq, ult)
        self._wal = None
        # Compacta ao iniciar: também elimina qualquer cauda truncada do WAL.
        self.gravar_snapshot(dados, seq, ult)

    def _caminho(self, nome):
        return os.path.join(self.dir, nome)

    def gravar_meta(self, epoca, lider):
        self.meta = {"epoca": epoca, "lider": lider}
        _escrever_atomico(self._caminho("meta.json"), self.meta, self.fsync)

    def anexar(self, epoca, seq, chave, valor):
        """Registra uma escrita no WAL (O(1))."""
        self._wal.write(json.dumps({"seq": seq, "e": epoca, "k": chave, "v": valor},
                                   ensure_ascii=False) + "\n")
        self._wal.flush()
        if self.fsync:
            os.fsync(self._wal.fileno())

    def gravar_snapshot(self, dados, seq, ult_epoca):
        """Substitui todo o estado persistido por um snapshot (nova geração de WAL)."""
        nova_geracao = self.geracao + 1
        if self._wal is not None:
            self._wal.close()
        self._wal = open(self._caminho(f"wal.{nova_geracao}.jsonl"), "w", encoding="utf-8")
        _escrever_atomico(self._caminho("snapshot.json"),
                          {"geracao": nova_geracao, "seq": seq, "ult_epoca": ult_epoca,
                           "dados": dados}, self.fsync)
        self.geracao = nova_geracao
        for nome in os.listdir(self.dir):  # WALs de gerações antigas/órfãs
            if nome.startswith("wal.") and nome != f"wal.{nova_geracao}.jsonl":
                try:
                    os.remove(self._caminho(nome))
                except OSError:
                    pass

    def fechar(self):
        if self._wal is not None:
            self._wal.close()
            self._wal = None
