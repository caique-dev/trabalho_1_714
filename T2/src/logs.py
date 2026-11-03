"""Log estruturado em JSONL.

Cada linha é um evento ``{"t": time.time(), "p": time.perf_counter(), "no": id,
"inc": incarnação, "evento": nome, ...campos}``. ``t`` é o relógio de parede;
``p`` é o contador de alta resolução (``perf_counter``), monotônico e com a mesma
origem em todos os processos da máquina, usado para ordenar eventos de nós
diferentes e medir intervalos (no Windows ``time.time()`` e ``time.monotonic()``
têm resolução de ~15 ms, insuficiente para latências de milissegundos). O arquivo é aberto em modo
append, então um nó reiniciado continua o mesmo arquivo (a incarnação separa as
vidas do processo).
"""

import json
import os
import threading
import time


class LogEventos:
    def __init__(self, caminho, no, inc=0):
        os.makedirs(os.path.dirname(os.path.abspath(caminho)), exist_ok=True)
        self._arq = open(caminho, "a", encoding="utf-8", buffering=1)
        self._trava = threading.Lock()
        self.no = no
        self.inc = inc

    def __call__(self, evento, **campos):
        linha = {"t": time.time(), "p": time.perf_counter(), "no": self.no,
                 "inc": self.inc, "evento": evento}
        linha.update(campos)
        texto = json.dumps(linha, ensure_ascii=False, default=str)
        with self._trava:
            self._arq.write(texto + "\n")

    def fechar(self):
        with self._trava:
            self._arq.close()
