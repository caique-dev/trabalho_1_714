"""Verificador de invariantes a partir dos logs JSONL (e dos estados finais).

Entrada: os eventos de todos os nós (``logs/no*.jsonl``) e, opcionalmente, o
estado final das réplicas vivas (via status/``ADM_STATUS`` ou lido do disco).

Invariantes checadas (violação = bug ou hipótese do modelo violada):

 I1  no máximo um líder *efetivo* por época: no máximo um nó registra
     ``lider_pronto`` em cada época (a fase de sincronização exige quórum e a
     promessa de época, então dois não conseguem);
 I2  a época de cada nó nunca diminui (considerando os eventos de liderança);
 I3  toda escrita confirmada ao cliente foi confirmada por um nó que era
     ``lider_pronto`` naquela época (fencing: primário deposto não confirma);
 I4  duas escritas confirmadas com o mesmo ``seq`` têm a mesma chave/valor
     (históricos não divergem entre épocas);
 I5  (com estados finais) toda escrita confirmada está refletida no estado final
     de todas as réplicas vivas: o valor final de cada chave é o da última
     escrita confirmada, ou o de uma escrita posterior a ela (uma escrita não
     confirmada pode ter sido adotada na sincronização; isso é permitido);
 I6  (com estados finais) as réplicas vivas convergem para o mesmo estado.

Avisos (não são violações): mais de um COORDENADOR anunciado na mesma época
(possível em partição; só um vira efetivo, ver I1).
"""

import glob
import json
import os
from dataclasses import dataclass, field


@dataclass
class Resultado:
    violacoes: list = field(default_factory=list)
    avisos: list = field(default_factory=list)
    resumo: dict = field(default_factory=dict)

    @property
    def ok(self):
        return not self.violacoes


def carregar_eventos(dir_logs):
    """Lê todos os ``*.jsonl`` do diretório e ordena por timestamp."""
    eventos = []
    for caminho in sorted(glob.glob(os.path.join(dir_logs, "*.jsonl"))):
        with open(caminho, encoding="utf-8") as f:
            for linha in f:
                linha = linha.strip()
                if not linha:
                    continue
                try:
                    eventos.append(json.loads(linha))
                except ValueError:
                    continue  # última linha truncada por kill
    eventos.sort(key=lambda e: e["p"])  # "p": perf_counter, mesma origem em todos os processos
    return eventos


def verificar(eventos, estados_finais=None):
    """Aplica as invariantes. ``estados_finais``: ``{id: {"dados":..., "seq":..., ...}}`` das réplicas vivas."""
    res = Resultado()

    # I1 e aviso de COORDENADOR duplicado
    prontos, anunciados = {}, {}
    for e in eventos:
        if e["evento"] == "lider_pronto":
            prontos.setdefault(e["epoca"], set()).add(e["no"])
        elif e["evento"] == "coordenador_anunciado":
            anunciados.setdefault(e["epoca"], set()).add(e["no"])
    for epoca, nos in sorted(prontos.items()):
        if len(nos) > 1:
            res.violacoes.append(f"I1: época {epoca} teve {len(nos)} líderes efetivos: {sorted(nos)}")
    for epoca, nos in sorted(anunciados.items()):
        if len(nos) > 1:
            res.avisos.append(f"época {epoca} teve COORDENADOR anunciado por {sorted(nos)} "
                              f"(efetivo: {sorted(prontos.get(epoca, []))})")

    # I2: monotonicidade da época por nó
    ultima = {}
    for e in eventos:
        if e["evento"] in ("coordenador_anunciado", "coordenador_aceito", "lider_pronto") \
                and "epoca" in e and e["no"] != "cluster":
            no = e["no"]
            if e["epoca"] < ultima.get(no, 0):
                res.violacoes.append(f"I2: nó {no} voltou da época {ultima[no]} para {e['epoca']}")
            ultima[no] = max(ultima.get(no, 0), e["epoca"])

    # I3 e I4: escritas confirmadas
    confirmadas = [e for e in eventos if e["evento"] == "escrita_confirmada"]
    inicio_pronto = {}  # (nó, época) -> instante em que ficou pronto
    for p in eventos:
        if p["evento"] == "lider_pronto":
            inicio_pronto.setdefault((p["no"], p["epoca"]), p["p"])
    for c in confirmadas:
        if inicio_pronto.get((c["no"], c["epoca"]), float("inf")) > c["p"]:
            res.violacoes.append(f"I3: nó {c['no']} confirmou seq={c['seq']} na época "
                                 f"{c['epoca']} sem ter sido líder pronto")
    por_seq = {}
    for c in confirmadas:
        anterior = por_seq.setdefault(c["seq"], c)
        if (anterior["chave"], anterior["valor"]) != (c["chave"], c["valor"]):
            res.violacoes.append(f"I4: seq {c['seq']} confirmado com conteúdos distintos "
                                 f"({anterior['chave']!r} vs {c['chave']!r})")

    res.resumo = {"eventos": len(eventos), "epocas_com_lider_efetivo": len(prontos),
                  "escritas_confirmadas": len(confirmadas),
                  "suspeitas": sum(1 for e in eventos if e["evento"] == "suspeita"),
                  "suspeitas_falsas": sum(1 for e in eventos
                                          if e["evento"] == "suspeita_revogada" and e.get("falsa"))}

    # I5 e I6
    if estados_finais:
        ultimo_conf = {}  # chave -> escrita confirmada de maior (epoca, seq)
        for c in confirmadas:
            atual = ultimo_conf.get(c["chave"])
            if atual is None or (c["epoca"], c["seq"]) > (atual["epoca"], atual["seq"]):
                ultimo_conf[c["chave"]] = c
        tentativas = {}  # chave -> [(epoca, seq, valor)]
        for e in eventos:
            if e["evento"] == "escrita_local":
                tentativas.setdefault(e["chave"], []).append((e["epoca"], e["seq"], e["valor"]))
        for no, est in sorted(estados_finais.items()):
            for chave, c in ultimo_conf.items():
                if chave not in est["dados"]:
                    res.violacoes.append(f"I5: nó {no} perdeu a chave {chave!r} confirmada (seq {c['seq']})")
                    continue
                final = est["dados"][chave]
                permitidos = [c["valor"]] + [v for (e_, s_, v) in tentativas.get(chave, [])
                                             if (e_, s_) > (c["epoca"], c["seq"])]
                if final not in permitidos:
                    res.violacoes.append(f"I5: nó {no} tem {chave!r}={final!r}, "
                                         f"esperado {c['valor']!r} (seq {c['seq']})")
        impressoes = {(est["seq"], json.dumps(est["dados"], sort_keys=True))
                      for est in estados_finais.values()}
        if len(impressoes) > 1:
            res.violacoes.append("I6: réplicas vivas não convergiram: " + ", ".join(
                f"nó {n}: seq={e['seq']} chaves={len(e['dados'])}"
                for n, e in sorted(estados_finais.items())))
        res.resumo["replicas_verificadas"] = len(estados_finais)
    return res


def formatar(res):
    """Texto legível do resultado."""
    linhas = [f"Resumo: {res.resumo}"]
    linhas += [f"AVISO: {a}" for a in res.avisos]
    linhas += [f"VIOLAÇÃO: {v}" for v in res.violacoes]
    linhas.append("RESULTADO: " + ("todas as invariantes verificadas" if res.ok else "VIOLAÇÕES ENCONTRADAS"))
    return "\n".join(linhas)
