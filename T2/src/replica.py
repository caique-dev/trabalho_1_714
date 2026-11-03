"""Máquina de estado da réplica (sem rede): o dicionário chave-valor e o log.

Estado: ``dados`` (dict), ``seq`` (nº da última escrita aplicada) e
``ult_epoca`` (época do primário que produziu essa última escrita). O par
``(ult_epoca, seq)`` é a "posição" da réplica: a comparação lexicográfica diz
qual réplica está mais avançada. A época vem primeiro porque, como no Raft, uma
escrita de época maior sempre vence uma cauda não confirmada de época menor
(essa cauda vem de um primário deposto que nunca chegou a confirmar).

``aplicar`` exige que a escrita seja a próxima (``seq == self.seq + 1``) *e* que
o primário informe a época da escrita anterior (``prev_epoca``) igual à nossa.
Sem o segundo teste, uma réplica com cauda divergente aceitaria silenciosamente
a próxima escrita do novo primário e os históricos divergiriam; com ele a
divergência vira ``LACUNA`` e o primário reenvia o estado completo.
"""

import hashlib
import json

APLICADO = "aplicado"
DUPLICADO = "duplicado"
LACUNA = "lacuna"  # falta escrita anterior ou histórico divergente


class Replica:
    def __init__(self, armazenamento=None):
        self.armazenamento = armazenamento
        if armazenamento is not None:
            dados, seq, ult = armazenamento.estado_inicial
            self.dados, self.seq, self.ult_epoca = dict(dados), seq, ult
        else:
            self.dados, self.seq, self.ult_epoca = {}, 0, 0

    def chave_estado(self):
        """Posição da réplica no histórico: ``(ult_epoca, seq)``."""
        return (self.ult_epoca, self.seq)

    def escrever_local(self, epoca, chave, valor):
        """Usada pelo primário: atribui o próximo seq e persiste. Devolve ``(seq, prev_epoca)``."""
        prev = self.ult_epoca
        self.seq += 1
        self.ult_epoca = epoca
        self.dados[chave] = valor
        if self.armazenamento is not None:
            self.armazenamento.anexar(epoca, self.seq, chave, valor)
        return self.seq, prev

    def aplicar(self, epoca, seq, prev_epoca, chave, valor):
        """Usada pelo backup. Idempotente e em ordem de ``seq``."""
        if seq == self.seq + 1 and prev_epoca == self.ult_epoca:
            self.seq, self.ult_epoca = seq, epoca
            self.dados[chave] = valor
            if self.armazenamento is not None:
                self.armazenamento.anexar(epoca, seq, chave, valor)
            return APLICADO
        # Retransmissão de algo que já temos: mesma época de primário e seq já
        # coberto (uma época tem um único primário, logo a escrita é a mesma).
        if seq <= self.seq and epoca == self.ult_epoca:
            return DUPLICADO
        return LACUNA

    def snapshot(self):
        return {"dados": self.dados, "seq": self.seq, "ult_epoca": self.ult_epoca}

    def instalar(self, snap):
        """Substitui todo o estado (transferência de estado)."""
        self.dados = dict(snap["dados"])
        self.seq = snap["seq"]
        self.ult_epoca = snap["ult_epoca"]
        if self.armazenamento is not None:
            self.armazenamento.gravar_snapshot(self.dados, self.seq, self.ult_epoca)

    def hash_estado(self):
        """Resumo do conteúdo, para comparar réplicas sem trafegar os dados."""
        texto = json.dumps(self.dados, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(texto.encode("utf-8")).hexdigest()[:16]
