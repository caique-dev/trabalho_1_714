"""Campanha experimental do Trabalho 2 (fase 2).

Cada réplica sobe um cluster novo de processos locais (``Cluster``; sem contêineres),
mede, verifica as invariantes com o verificador de logs e destrói tudo (``try/finally``).
Os dados brutos (uma linha por réplica) vão para ``<saida>/brutos/*.csv``; os agregados
(média, desvio, IC95 t de Student, p50/p95/p99), os gráficos e ``metadados.json`` são
derivados deles (``agregar_e_plotar`` pode ser rodado sem refazer as medições).

Medição de tempo: ``time.perf_counter`` (campo ``p`` dos logs, mesma origem em todos os
processos da máquina, resolução < 1 us). ``time.time``/``time.monotonic`` têm ~15 ms no Windows.

Experimentos
------------
E1  Bully: crash do líder, N em {3,5,7,9}; três variantes de detecção:
    natural (todos detectam por timeout), melhor caso (só o nó N-1 detecta) e pior caso
    (só o nó 1 detecta). As variantes forçadas usam um ``timeout_fd`` muito alto nos demais nós.
E2  Primário-backup: latência de escrita (cliente) e vazão vs N e vs atraso injetado.
E3a Failover: janela de indisponibilidade vista por um escritor contínuo vs timeout_fd.
E3b Falsas suspeitas e eleições espúrias vs probabilidade de perda (sem crash).
E4  Partição que isola o primário (fencing/quórum) e cura.
"""

import csv
import json
import os
import platform
import random
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from contextlib import contextmanager
from datetime import datetime

from . import cliente as cli
from . import estatistica as est
from . import mensagens as M
from . import verificador
from .cluster import Cluster, RAIZ
from .config import Parametros

# ----------------------------------------------------------------------
# Parâmetros da campanha
# ----------------------------------------------------------------------
N_REPLICAS = 10
BASE = dict(intervalo_hb=0.1, timeout_fd=0.5, timeout_rpc=0.3)  # temporizações curtas
TIMEOUT_ALTO = 30.0          # "nunca detecta" (>> duração de qualquer réplica)

E1_NS = (3, 5, 7, 9)
E1_VARIANTES = ("natural", "melhor_caso", "medio_caso", "pior_caso")
E1_ASSENTAMENTO_S = 1.0      # aquecimento: heartbeats estáveis antes do crash
E2_NS = (3, 5, 7, 9)
E2_ATRASOS_MS = (0, 5, 20)
E2_N_ATRASO = 5
E2_AQUECIMENTO_S = 1.5
E2_DURACAO_S = 6.0
E3A_TIMEOUTS = (0.25, 0.5, 1.0, 2.0)
E3A_RAZAO_HB = 5             # heartbeat = timeout_fd / 5 (cinco batidas por janela de suspeita)
E3A_AQUECIMENTO_S = 1.5
E3A_POS_S = 1.0              # escritas após a recuperação antes de parar
E3B_PERDAS = (0.0, 0.05, 0.1, 0.2)
E3B_AQUECIMENTO_S = 2.0
E3B_JANELA_S = 15.0
E3B_PAUSA_ESCRITA_S = 0.05
E4_PARTICAO_S = 4.0

IDS = ("E1", "E2", "E3", "E4")   # E3 = E3a + E3b


def semente(exp, cfg, rep):
    """Semente única e registrada de cada réplica."""
    return exp * 10000 + cfg * 100 + rep


# ----------------------------------------------------------------------
# Infraestrutura
# ----------------------------------------------------------------------
class ClusterHeterogeneo(Cluster):
    """Cluster em que alguns nós têm ``timeout_fd`` próprio (para forçar quem detecta a falha)."""

    def __init__(self, *a, timeout_por_no=None, **kw):
        super().__init__(*a, **kw)
        self.timeout_por_no = timeout_por_no or {}

    def comando_no(self, i):
        cmd = super().comando_no(i)
        if i in self.timeout_por_no:
            cmd[cmd.index("--timeout-fd") + 1] = str(self.timeout_por_no[i])
        return cmd


@contextmanager
def cluster_temp(n, params, **kw):
    """Cluster de processos locais em diretório temporário; sempre encerrado e apagado."""
    d = tempfile.mkdtemp(prefix="mc714_exp_")
    c = ClusterHeterogeneo(n, diretorio=d, params=params, **kw)
    try:
        c.iniciar()
        yield c
    finally:
        c.parar()
        for _ in range(10):   # no Windows o handle do WAL do processo morto pode demorar a ser liberado
            try:
                shutil.rmtree(d)
                break
            except OSError:
                time.sleep(0.2)
        else:
            shutil.rmtree(d, ignore_errors=True)


def params(semente_, **extra):
    return Parametros(**{**BASE, **extra, "semente": semente_})


def estabilizar(c, lider):
    """Espera o líder esperado pronto e as réplicas convergidas (início de toda réplica)."""
    c.aguardar_lider(esperado=lider, timeout=30)
    c.aguardar_convergencia(timeout=30)


def eventos_apos(eventos, t, nome, **filtro):
    return [e for e in eventos if e["p"] > t and e["evento"] == nome and e["no"] != "cluster"
            and all(e.get(k) == v for k, v in filtro.items())]


def primeiro_p(eventos, t, nome, **filtro):
    ev = eventos_apos(eventos, t, nome, **filtro)
    return min((e["p"] for e in ev), default=None)


def instante_kill(eventos, alvo):
    return next(e["p"] for e in eventos if e["evento"] == "no_morto" and e["alvo"] == alvo)


def delta(antes, depois, campo, tipos):
    """Soma, nos nós presentes nos dois instantâneos, da variação dos contadores ``tipos``."""
    out = {t: 0 for t in tipos}
    for i, s in depois.items():
        if i not in antes:
            continue
        for t in tipos:
            out[t] += s["contadores"][campo].get(t, 0) - antes[i]["contadores"][campo].get(t, 0)
    return out


def verificar_final(c, escritas_ok=None, timeout=40):
    """Cura falhas, converge e roda o verificador (I1-I6). Devolve (ok, violações, perdidas_cliente).

    ``escritas_ok``: lista de (chave, valor) confirmadas ao cliente (chaves únicas): conta quantas
    não estão, com o mesmo valor, em alguma réplica viva (verificação do lado do cliente).
    """
    c.curar()
    c.aguardar_lider(timeout=timeout)
    c.aguardar_convergencia(timeout=timeout)
    estados = {i: {"dados": s["dados"], "seq": s["seq"]}
               for i, s in c.status_vivos(incluir_dados=True).items()}
    res = verificador.verificar(verificador.carregar_eventos(c.dir_logs), estados)
    perdidas = None
    if escritas_ok is not None:
        perdidas = sum(1 for k, v in escritas_ok
                       if any(e["dados"].get(k) != v for e in estados.values()))
    return res.ok, "; ".join(res.violacoes), perdidas


class Escritor(threading.Thread):
    """Cliente que escreve sem parar (chaves únicas ``k<i>``), registrando início/fim de cada PUT."""

    def __init__(self, cl, pausa=0.0, prazo=10.0):
        super().__init__(daemon=True)
        self.cl, self.pausa, self.prazo = cl, pausa, prazo
        self.parar_ev = threading.Event()
        self.registros = []   # (i, t_ini, t_fim, ok, tentativas, erro)

    def run(self):
        i = 0
        while not self.parar_ev.is_set():
            t0 = time.perf_counter()
            r = self.cl.put(f"k{i}", i, prazo=self.prazo)
            self.registros.append((i, t0, time.perf_counter(), r.ok, r.tentativas, r.erro))
            i += 1
            if self.pausa:
                time.sleep(self.pausa)

    def encerrar(self):
        self.parar_ev.set()
        self.join(timeout=30)

    def confirmadas(self):
        return [(f"k{r[0]}", r[0]) for r in self.registros if r[3]]


# ----------------------------------------------------------------------
# E1 — Bully
# ----------------------------------------------------------------------
def msgs_teoricas(n, m):
    """Mensagens (ELEICAO+OK+COORDENADOR enviadas) quando só o nó ``m`` detecta o crash do nó ``n``.

    O nó m envia ELEICAO aos ids maiores; cada vivo maior responde OK e abre a sua própria eleição.
    Assim os iniciadores são m..n-1: ELEICAO = soma_{i=m}^{n-1} (n-i), OK = soma (n-1-i) e o vencedor
    (n-1) anuncia COORDENADOR a n-1 nós (o morto incluído). Total = (n-m)^2 + (n-1);
    m=n-1 dá n (melhor caso) e m=1 dá n(n-1) (pior caso).
    """
    return (n - m) ** 2 + (n - 1)


def e1_replica(n, variante, sem):
    rng = random.Random(sem)
    lider = n
    iniciador = {"melhor_caso": n - 1,          # só o maior id vivo detecta
                 "medio_caso": (n + 1) // 2,    # só um id intermediário detecta
                 "pior_caso": 1}.get(variante)  # só o menor id detecta
    timeouts = {} if iniciador is None else {i: TIMEOUT_ALTO for i in range(1, n) if i != iniciador}
    with cluster_temp(n, params(sem), timeout_por_no=timeouts) as c:
        estabilizar(c, lider)
        cl = c.cliente(timeout=1.0)
        for k in range(3):                       # estado não vazio (aquecimento, descartado)
            cl.put(f"w{k}", k)
        time.sleep(E1_ASSENTAMENTO_S)
        antes = c.status_vivos()
        time.sleep(rng.uniform(0, BASE["intervalo_hb"]))   # fase do crash vs heartbeat
        c.matar(lider)

        def assentou():
            if c.lider_consensual() != n - 1:
                return False
            return not any(s["eleicao_ativa"] for s in c.status_vivos().values())
        c.aguardar(assentou, 60.0, "eleição concluída")
        time.sleep(0.5)                          # mensagens em voo
        depois = c.status_vivos()
        ev = verificador.carregar_eventos(c.dir_logs)
        t_kill = instante_kill(ev, lider)
        t_det = primeiro_p(ev, t_kill, "suspeita", alvo=lider)
        t_ini = primeiro_p(ev, t_kill, "eleicao_iniciada")
        t_ann = primeiro_p(ev, t_kill, "coordenador_anunciado")
        t_pro = primeiro_p(ev, t_kill, "lider_pronto")
        tipos = (M.ELEICAO, M.OK, M.COORDENADOR)
        env = delta(antes, depois, "enviadas", tipos)
        rec = delta(antes, depois, "recebidas", tipos)
        novo_lider = c.lider_consensual()
        ok_inv, viol, _ = verificar_final(c, None)
        return {
            "n": n, "variante": variante, "novo_lider": novo_lider, "iniciador_forcado": iniciador,
            "msg_teorico_env": msgs_teoricas(n, iniciador) if iniciador else "",
            "t_deteccao_s": t_det - t_kill, "t_eleicao_s": t_ann - t_ini,
            "t_ate_anuncio_s": t_ann - t_kill, "t_sync_s": t_pro - t_ann,
            "t_ate_pronto_s": t_pro - t_kill,
            "msg_eleicao": env[M.ELEICAO], "msg_ok": env[M.OK], "msg_coord": env[M.COORDENADOR],
            "msg_total_env": sum(env.values()), "msg_total_rec": sum(rec.values()),
            "n_iniciadores": len({e["no"] for e in eventos_apos(ev, t_kill, "eleicao_iniciada")}),
            "n_detectores": len({e["no"] for e in eventos_apos(ev, t_kill, "suspeita", alvo=lider)}),
            "rodadas": len(eventos_apos(ev, t_kill, "eleicao_iniciada")),
            "invariantes_ok": int(ok_inv), "violacoes": viol,
        }


# ----------------------------------------------------------------------
# E2 — Primário-backup
# ----------------------------------------------------------------------
def e2_replica(n, atraso_ms, sem):
    with cluster_temp(n, params(sem)) as c:
        estabilizar(c, n)
        if atraso_ms:
            for i in c.vivos():
                c.falhas(i, atraso_fixo=atraso_ms / 1000.0, semente=sem * 100 + i)
        cl = c.cliente(timeout=3.0)
        t_fim_aq = time.perf_counter() + E2_AQUECIMENTO_S
        k = 0
        while time.perf_counter() < t_fim_aq:    # aquecimento (descartado)
            cl.put(f"k{k % 100}", k)
            k += 1
        antes = c.status_vivos()
        seq0 = antes[n]["seq"]
        lats, srvs, falhas = [], [], 0
        t0 = time.perf_counter()
        while time.perf_counter() - t0 < E2_DURACAO_S:
            r = cl.put(f"k{k % 100}", k)
            k += 1
            if r.ok:
                lats.append(r.latencia * 1000.0)
                srvs.append(r.dados["latencia_servidor_ms"])
            else:
                falhas += 1
        dur = time.perf_counter() - t0
        depois = c.status_vivos()
        escritas = depois[n]["seq"] - seq0
        env = delta(antes, depois, "enviadas", (M.REPLICAR, M.ACK))
        ok_inv, viol, _ = verificar_final(c, None)
        r = est.resumo_latencia(lats)
        sv = est.resumo_latencia(srvs)
        return ({
            "n": n, "atraso_ms": atraso_ms, "n_escritas": r["n"], "falhas": falhas,
            "duracao_s": dur, "vazao_ops_s": r["n"] / dur,
            "lat_media_ms": r["media"], "lat_desvio_ms": r["desvio"], "lat_min_ms": r["min"],
            "lat_p50_ms": r["p50"], "lat_p95_ms": r["p95"], "lat_p99_ms": r["p99"],
            "lat_max_ms": r["max"],
            "srv_media_ms": sv["media"], "srv_p50_ms": sv["p50"], "srv_p95_ms": sv["p95"],
            "srv_p99_ms": sv["p99"],
            "msg_replicar": env[M.REPLICAR], "msg_ack": env[M.ACK], "escritas_servidor": escritas,
            "msg_por_escrita": (env[M.REPLICAR] + env[M.ACK]) / escritas if escritas else float("nan"),
            "invariantes_ok": int(ok_inv), "violacoes": viol,
        }, (lats, srvs))


# ----------------------------------------------------------------------
# E3a — janela de indisponibilidade no failover
# ----------------------------------------------------------------------
def e3a_replica(timeout_fd, sem):
    n = 5
    hb = timeout_fd / E3A_RAZAO_HB
    rng = random.Random(sem)
    with cluster_temp(n, params(sem, intervalo_hb=hb, timeout_fd=timeout_fd)) as c:
        estabilizar(c, n)
        cl = c.cliente(timeout=0.5, prazo=20.0)
        w = Escritor(cl)
        try:
            w.start()
            time.sleep(E3A_AQUECIMENTO_S)
            time.sleep(rng.uniform(0, hb))
            c.matar(n)
            c.aguardar_lider(esperado=n - 1, timeout=60, ids=c.vivos())
            time.sleep(E3A_POS_S)
        finally:
            w.encerrar()
        ev = verificador.carregar_eventos(c.dir_logs)
        t_kill = instante_kill(ev, n)
        # Janela = maior intervalo entre duas confirmações consecutivas (por conclusão) que cobre o
        # crash. (Uma escrita pode concluir OK nos ms entre o log "no_morto" e o kill efetivo; por
        # isso não se usa "a primeira OK depois de t_kill".)
        oks = sorted(r[2] for r in w.registros if r[3])
        lacunas = [(a, b) for a, b in zip(oks, oks[1:]) if b > t_kill - 0.05]
        t_ult, t_prim = max(lacunas, key=lambda ab: ab[1] - ab[0])
        t_det = primeiro_p(ev, t_kill, "suspeita", alvo=n)
        t_ini = primeiro_p(ev, t_kill, "eleicao_iniciada")
        t_ann = primeiro_p(ev, t_kill, "coordenador_anunciado")
        t_pro = primeiro_p(ev, t_kill, "lider_pronto")
        ok_inv, viol, perd = verificar_final(c, w.confirmadas())
        lat_normal = [(r[2] - r[1]) * 1000 for r in w.registros if r[3] and r[2] < t_kill - 0.2]
        return {
            "timeout_fd_s": timeout_fd, "heartbeat_s": hb,
            "janela_s": t_prim - t_ult, "t_deteccao_s": t_det - t_kill,
            "t_eleicao_s": t_ann - t_ini, "t_sync_s": t_pro - t_ann,
            "t_ate_pronto_s": t_pro - t_kill,
            "t_ult_ok_antes_s": t_kill - t_ult, "t_prim_ok_depois_s": t_prim - t_kill,
            "escritas_ok": sum(1 for r in w.registros if r[3]),
            "escritas_falhas": sum(1 for r in w.registros if not r[3]),
            "lat_normal_p50_ms": est.percentil(lat_normal, 50) if lat_normal else float("nan"),
            "perdidas_cliente": perd, "invariantes_ok": int(ok_inv and perd == 0),
            "violacoes": viol,
        }


# ----------------------------------------------------------------------
# E3b — falsas suspeitas vs perda de mensagens
# ----------------------------------------------------------------------
def e3b_replica(perda, sem):
    n = 5
    with cluster_temp(n, params(sem)) as c:
        estabilizar(c, n)
        for i in c.vivos():
            c.falhas(i, prob_descarte=perda, semente=sem * 100 + i)
        cl = c.cliente(timeout=0.5, prazo=10.0)
        w = Escritor(cl, pausa=E3B_PAUSA_ESCRITA_S)
        try:
            w.start()
            time.sleep(E3B_AQUECIMENTO_S)
            antes = c.status_vivos()
            t0 = time.perf_counter()
            time.sleep(E3B_JANELA_S)
            t1 = time.perf_counter()
            depois = c.status_vivos()
        finally:
            w.encerrar()
        ev = [e for e in verificador.carregar_eventos(c.dir_logs) if t0 <= e["p"] <= t1]
        susp = [e for e in ev if e["evento"] == "suspeita"]
        hb = delta(antes, depois, "enviadas", (M.HEARTBEAT,))[M.HEARTBEAT]
        minutos = (t1 - t0) / 60.0
        por_motivo = {m: sum(1 for e in susp if e["motivo"] == m)
                      for m in ("heartbeat_ausente", "backup_sem_resposta", "replicar_sem_ack")}
        det = por_motivo["heartbeat_ausente"] + por_motivo["backup_sem_resposta"]
        eleicoes = [e for e in ev if e["evento"] == "eleicao_iniciada" and e["motivo"] != "inicio"]
        espurias = len(eleicoes)
        reg = [r for r in w.registros if t0 <= r[2] <= t1]
        ok_inv, viol, perd = verificar_final(c, w.confirmadas())
        return {
            "perda": perda, "janela_s": t1 - t0, "heartbeats_enviados": hb,
            "susp_total": len(susp), "susp_heartbeat_ausente": por_motivo["heartbeat_ausente"],
            "susp_backup_sem_resposta": por_motivo["backup_sem_resposta"],
            "susp_replicar_sem_ack": por_motivo["replicar_sem_ack"],
            "susp_detector": det, "susp_detector_por_min": det / minutos,
            "susp_falsas_revogadas": sum(1 for e in ev if e["evento"] == "suspeita_revogada"
                                         and e.get("falsa")),
            "eleicoes_espurias": espurias, "eleicoes_espurias_por_min": espurias / minutos,
            "el_lider_suspeito": sum(1 for e in eleicoes if e["motivo"] == "lider_suspeito"),
            "el_eleicao_recebida": sum(1 for e in eleicoes if e["motivo"] == "eleicao_recebida"),
            "el_outros": sum(1 for e in eleicoes if e["motivo"] not in ("lider_suspeito", "eleicao_recebida")),
            "mudancas_de_lider": sum(1 for e in ev if e["evento"] == "coordenador_anunciado"),
            "remocoes_de_visao": sum(1 for e in ev if e["evento"] == "visao_removido"),
            "escritas_ok": sum(1 for r in reg if r[3]), "escritas_falhas": sum(1 for r in reg if not r[3]),
            "perdidas_cliente": perd, "invariantes_ok": int(ok_inv and perd == 0), "violacoes": viol,
        }


# ----------------------------------------------------------------------
# E4 — partição isolando o primário
# ----------------------------------------------------------------------
def e4_replica(sem):
    n = 5
    with cluster_temp(n, params(sem)) as c:
        estabilizar(c, n)
        cl0 = c.cliente(timeout=1.0)
        ok_todas = []
        for k in range(5):
            if cl0.put(f"antes{k}", k).ok:
                ok_todas.append((f"antes{k}", k))
        maioria = [1, 2, 3, 4]
        cl_maioria = cli.Cliente({i: c.enderecos[i] for i in maioria}, timeout=0.8, prazo=10.0)

        tentativas = []   # (instante, ok, erro) das escritas ao primário isolado
        parar = threading.Event()

        def escritor_antigo():
            j = 0
            while not parar.is_set():
                r = cli.chamar_no(c.enderecos[n], M.CLI_PUT,
                                  {"chave": f"orfa{j}", "valor": j}, timeout=1.0)
                d = r["dados"] if r else {}
                tentativas.append((time.perf_counter(), bool(d.get("ok")),
                                   None if d.get("ok") else d.get("erro", "sem_resposta")))
                j += 1
                time.sleep(0.05)

        th = threading.Thread(target=escritor_antigo, daemon=True)
        t_iso = time.perf_counter()
        c.isolar(n)
        th.start()
        try:
            c.aguardar_lider(esperado=n - 1, timeout=30, ids=maioria)
            t_consenso = time.perf_counter() - t_iso
            for k in range(20):
                r = cl_maioria.put(f"maioria{k}", k)
                if r.ok:
                    ok_todas.append((f"maioria{k}", k))
            time.sleep(max(0.0, E4_PARTICAO_S - (time.perf_counter() - t_iso)))
        finally:
            parar.set()
            th.join(timeout=10)
        t_cura = time.perf_counter()
        c.curar()
        c.aguardar_lider(esperado=n, timeout=40)
        t_reconv = time.perf_counter() - t_cura
        c.aguardar_convergencia(timeout=40)
        t_conv = time.perf_counter() - t_cura
        ev = verificador.carregar_eventos(c.dir_logs)
        t_ren = primeiro_p(ev, t_iso, "renuncia", no=n)
        t_pronto_ant = [e["p"] for e in ev if e["evento"] == "lider_pronto" and e["no"] == n - 1
                        and e["p"] > t_iso]
        ok_inv, viol, perd = verificar_final(c, ok_todas)
        estado = c.status(1, incluir_dados=True)["dados"]
        erros = {}
        for (_t, ok, erro) in tentativas:
            if not ok:
                erros[erro] = erros.get(erro, 0) + 1
        return {
            "tent_primario_antigo": len(tentativas),
            "conf_primario_antigo": sum(1 for t in tentativas if t[1]),
            "erros_primario_antigo": json.dumps(erros, sort_keys=True),
            "t_novo_lider_pronto_s": (t_pronto_ant[0] - t_iso) if t_pronto_ant else float("nan"),
            "t_novo_lider_consenso_s": t_consenso,
            "escritas_maioria_ok": sum(1 for k, _ in ok_todas if k.startswith("maioria")),
            "t_renuncia_apos_cura_s": (t_ren - t_cura) if t_ren else float("nan"),
            "t_reconvergencia_lider_s": t_reconv, "t_convergencia_s": t_conv,
            "orfas_no_estado_final": sum(1 for k in estado if k.startswith("orfa")),
            "perdidas_cliente": perd, "invariantes_ok": int(ok_inv and perd == 0),
            "violacoes": viol,
        }


# ----------------------------------------------------------------------
# CSV e agregação
# ----------------------------------------------------------------------
class Bruto:
    """CSV bruto com uma linha por réplica, gravado (e descarregado) a cada réplica."""

    def __init__(self, caminho, colunas):
        os.makedirs(os.path.dirname(caminho), exist_ok=True)
        self.caminho = caminho
        self._f = open(caminho, "w", newline="", encoding="utf-8")
        self._w = csv.DictWriter(self._f, fieldnames=colunas, restval="", extrasaction="ignore")
        self._w.writeheader()

    def escrever(self, linha):
        self._w.writerow({k: (f"{v:.6f}" if isinstance(v, float) else v) for k, v in linha.items()})
        self._f.flush()

    def fechar(self):
        self._f.close()


def carregar_csv(caminho):
    """Lê um CSV convertendo números; devolve lista de dicts."""
    linhas = []
    with open(caminho, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            for k, v in r.items():
                try:
                    r[k] = int(v)
                except (TypeError, ValueError):
                    try:
                        r[k] = float(v)
                    except (TypeError, ValueError):
                        pass
            linhas.append(r)
    return linhas


def validas(linhas):
    return [r for r in linhas if not r.get("erro")]


def agrupar(linhas, chaves):
    grupos = {}
    for r in linhas:
        grupos.setdefault(tuple(r[k] for k in chaves), []).append(r)
    return grupos


def agregado_csv(linhas, chaves, metricas, caminho):
    """Formato longo: uma linha por (configuração, métrica) com n, média, desvio, IC95, percentis."""
    cab = list(chaves) + ["metrica", "n_replicas", "media", "desvio", "ic95_meia_largura", "ic95_inf",
                          "ic95_sup", "min", "p50", "p95", "p99", "max"]
    with open(caminho, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cab)
        for cfg, grupo in sorted(agrupar(linhas, chaves).items()):
            for m in metricas:
                vals = [r[m] for r in grupo if isinstance(r.get(m), (int, float)) and r[m] == r[m]]
                if not vals:
                    continue
                a = est.agregar(vals)
                w.writerow(list(cfg) + [m, a.n] + [f"{x:.6f}" for x in (
                    a.media, a.desvio, a.margem, a.inferior, a.superior, min(vals),
                    est.percentil(vals, 50), est.percentil(vals, 95), est.percentil(vals, 99),
                    max(vals))])
    return caminho


# ----------------------------------------------------------------------
# Execução
# ----------------------------------------------------------------------
COLUNAS = {
    "e1_bully": ["n", "variante", "rep", "semente", "novo_lider", "iniciador_forcado",
                 "msg_teorico_env", "t_deteccao_s", "t_eleicao_s",
                 "t_ate_anuncio_s", "t_sync_s", "t_ate_pronto_s", "msg_eleicao", "msg_ok", "msg_coord",
                 "msg_total_env", "msg_total_rec", "n_iniciadores", "n_detectores", "rodadas",
                 "invariantes_ok", "violacoes", "erro"],
    "e2_escrita": ["n", "atraso_ms", "rep", "semente", "n_escritas", "falhas", "duracao_s",
                   "vazao_ops_s", "lat_media_ms", "lat_desvio_ms", "lat_min_ms", "lat_p50_ms",
                   "lat_p95_ms", "lat_p99_ms", "lat_max_ms", "srv_media_ms", "srv_p50_ms",
                   "srv_p95_ms", "srv_p99_ms", "msg_replicar", "msg_ack",
                   "escritas_servidor", "msg_por_escrita", "invariantes_ok", "violacoes", "erro"],
    "e3a_failover": ["timeout_fd_s", "heartbeat_s", "rep", "semente", "janela_s", "t_deteccao_s",
                     "t_eleicao_s", "t_sync_s", "t_ate_pronto_s", "t_ult_ok_antes_s",
                     "t_prim_ok_depois_s", "escritas_ok", "escritas_falhas", "lat_normal_p50_ms",
                     "perdidas_cliente", "invariantes_ok", "violacoes", "erro"],
    "e3b_falsas_suspeitas": ["perda", "rep", "semente", "janela_s", "heartbeats_enviados",
                             "susp_total", "susp_heartbeat_ausente", "susp_backup_sem_resposta",
                             "susp_replicar_sem_ack", "susp_detector", "susp_detector_por_min",
                             "susp_falsas_revogadas", "eleicoes_espurias",
                             "eleicoes_espurias_por_min", "el_lider_suspeito", "el_eleicao_recebida",
                             "el_outros", "mudancas_de_lider", "remocoes_de_visao",
                             "escritas_ok", "escritas_falhas", "perdidas_cliente",
                             "invariantes_ok", "violacoes", "erro"],
    "e4_particao": ["rep", "semente", "tent_primario_antigo", "conf_primario_antigo",
                    "erros_primario_antigo", "t_novo_lider_pronto_s", "t_novo_lider_consenso_s",
                    "escritas_maioria_ok", "t_renuncia_apos_cura_s", "t_reconvergencia_lider_s",
                    "t_convergencia_s", "orfas_no_estado_final", "perdidas_cliente",
                    "invariantes_ok", "violacoes", "erro"],
}


def cfg_dict(nome, cfg):
    if nome == "e1_bully":
        return {"n": cfg[0], "variante": cfg[1]}
    if nome == "e2_escrita":
        return {"n": cfg[0], "atraso_ms": cfg[1]}
    if nome == "e3a_failover":
        return {"timeout_fd_s": cfg}
    if nome == "e3b_falsas_suspeitas":
        return {"perda": cfg}
    return {}


_RESUMO = {"e1_bully": ("t_deteccao_s", "t_eleicao_s", "msg_total_env"),
           "e2_escrita": ("lat_p50_ms", "vazao_ops_s", "msg_por_escrita"),
           "e3a_failover": ("janela_s",),
           "e3b_falsas_suspeitas": ("susp_detector", "eleicoes_espurias"),
           "e4_particao": ("conf_primario_antigo", "t_novo_lider_pronto_s")}


def _resumo_linha(nome, r):
    if r.get("erro"):
        return ""
    return " ".join(f"{k}={r[k]:.3g}" if isinstance(r[k], float) else f"{k}={r[k]}"
                    for k in _RESUMO[nome] if k in r) + f" inv={r.get('invariantes_ok')}"


def _rodar(nome, configs, reps, fn, saida, exp_id, log, extra_bruto=None):
    """Executa ``reps`` réplicas de cada configuração, gravando o CSV bruto a cada uma."""
    bruto = Bruto(os.path.join(saida, "brutos", f"{nome}.csv"), COLUNAS[nome])
    sementes, t0 = [], time.time()
    try:
        for ci, cfg in enumerate(configs):
            for rep in range(reps):
                sem = semente(exp_id, ci, rep)
                sementes.append(sem)
                t_rep = time.time()
                linha = {**cfg_dict(nome, cfg), "rep": rep, "semente": sem}
                try:
                    r = fn(cfg, sem)
                    if isinstance(r, tuple):
                        r, lats = r
                        if extra_bruto is not None:
                            extra_bruto(cfg, rep, lats)
                    linha.update(r)
                except Exception as exc:  # noqa: BLE001 — uma réplica ruim não derruba a campanha
                    linha["erro"] = repr(exc)[:200]
                    log(f"  !! erro em {cfg_dict(nome, cfg)} rep {rep}: {linha['erro']}")
                bruto.escrever(linha)
                log(f"  [{nome}] {cfg_dict(nome, cfg)} rep {rep + 1}/{reps} "
                    f"({time.time() - t_rep:.1f}s) " + _resumo_linha(nome, linha))
    finally:
        bruto.fechar()
    return {"replicas_por_configuracao": reps, "sementes": sementes,
            "duracao_execucao_s": round(time.time() - t0, 1)}


def executar(saida="resultados", rapido=False, so=None, replicas=None, log=print):
    """Roda a campanha (ou parte dela), agrega, plota e grava ``metadados.json``."""
    global E2_DURACAO_S, E3B_JANELA_S, E2_AQUECIMENTO_S, E3B_AQUECIMENTO_S
    so = {s.strip().upper() for s in so} if so else set(IDS)
    if "E3" in so:
        so |= {"E3A", "E3B"}
    reps = replicas or (2 if rapido else N_REPLICAS)
    if rapido:
        E2_DURACAO_S, E2_AQUECIMENTO_S, E3B_JANELA_S, E3B_AQUECIMENTO_S = 1.5, 0.5, 5.0, 1.0
    os.makedirs(os.path.join(saida, "brutos"), exist_ok=True)
    t_campanha = time.time()
    inicio = datetime.now().astimezone().isoformat(timespec="seconds")
    info = {}
    log(f"Campanha: {sorted(so)} | {reps} réplicas/configuração | saída: {saida}")

    if "E1" in so:
        log("E1 — Bully: crash do líder")
        cfgs = [(n, v) for v in E1_VARIANTES for n in E1_NS]
        info["E1"] = _rodar("e1_bully", cfgs, reps, lambda cfg, s: e1_replica(cfg[0], cfg[1], s),
                            saida, 1, log)
        info["E1"].update({
            "descricao": "crash (Popen.kill) do líder N; variantes: natural, melhor caso (só N-1 "
                         "detecta), pior caso (só o nó 1 detecta; demais com timeout_fd=30 s)",
            "N": list(E1_NS), "variantes": list(E1_VARIANTES), "parametros": BASE,
            "aquecimento": f"cluster estável + 3 escritas + {E1_ASSENTAMENTO_S}s de heartbeats antes do crash",
            "duracao_por_replica": "até a eleição assentar (~1 s) + 0,5 s",
            "fase_do_crash": "U(0, heartbeat) após o assentamento (semente da réplica)"})

    if "E2" in so:
        log("E2 — Primário-backup: latência e vazão")
        cfgs = [(n, 0) for n in E2_NS] + [(E2_N_ATRASO, a) for a in E2_ATRASOS_MS if a != 0]
        lat_f = open(os.path.join(saida, "brutos", "e2_latencias.csv"), "w", newline="",
                     encoding="utf-8")
        lat_w = csv.writer(lat_f)
        lat_w.writerow(["n", "atraso_ms", "rep", "i", "lat_ms", "lat_servidor_ms"])

        def grava(cfg, rep, lats):
            lat_w.writerows([cfg[0], cfg[1], rep, i, f"{v:.4f}", f"{sv:.4f}"]
                            for i, (v, sv) in enumerate(zip(*lats)))
            lat_f.flush()
        try:
            info["E2"] = _rodar("e2_escrita", cfgs, reps, lambda cfg, s: e2_replica(cfg[0], cfg[1], s),
                                saida, 2, log, extra_bruto=grava)
        finally:
            lat_f.close()
        info["E2"].update({
            "descricao": "um cliente síncrono em laço fechado escreve no primário (PUT, chaves cíclicas); "
                         "latência = perf_counter no cliente, incluindo redirecionamentos",
            "N": list(E2_NS), "atraso_ms_com_N5": list(E2_ATRASOS_MS), "parametros": BASE,
            "aquecimento": f"{E2_AQUECIMENTO_S}s de escritas descartadas",
            "duracao_por_replica": f"{E2_DURACAO_S}s de medição",
            "atraso_injetado": "atraso_fixo em todas as mensagens nó-a-nó (inclui o ACK de resposta)"})

    if "E3A" in so:
        log("E3a — Failover vs timeout_fd")
        info["E3a"] = _rodar("e3a_failover", list(E3A_TIMEOUTS), reps,
                             lambda cfg, s: e3a_replica(cfg, s), saida, 3, log)
        info["E3a"].update({
            "descricao": "N=5; escritor contínuo; crash do primário (nó 5); janela = fim da última "
                         "escrita OK antes do crash -> fim da primeira OK depois",
            "timeout_fd_s": list(E3A_TIMEOUTS), "heartbeat": f"timeout_fd/{E3A_RAZAO_HB}",
            "parametros": {**BASE, "timeout_fd": "varia", "intervalo_hb": "timeout_fd/5"},
            "aquecimento": f"{E3A_AQUECIMENTO_S}s de escrita contínua antes do crash",
            "duracao_por_replica": f"até a eleição concluir + {E3A_POS_S}s de escritas",
            "cliente": {"timeout_tentativa_s": 0.5, "prazo_s": 20,
                        "espera_entre_tentativas_s": "0,02-0,05"}})

    if "E3B" in so:
        log("E3b — Falsas suspeitas vs perda")
        info["E3b"] = _rodar("e3b_falsas_suspeitas", list(E3B_PERDAS), reps,
                             lambda cfg, s: e3b_replica(cfg, s), saida, 4, log)
        info["E3b"].update({
            "descricao": "N=5, sem crash; perda aleatória (prob_descarte) em todas as mensagens nó-a-nó; "
                         "escritor leve (1 PUT a cada 50 ms); contagem em janela fixa",
            "perdas": list(E3B_PERDAS), "parametros": BASE,
            "aquecimento": f"{E3B_AQUECIMENTO_S}s com a perda já ativa (descartados)",
            "duracao_por_replica": f"{E3B_JANELA_S}s de janela"})

    if "E4" in so:
        log("E4 — Partição isolando o primário")
        info["E4"] = _rodar("e4_particao", [None], reps, lambda cfg, s: e4_replica(s), saida, 5, log)
        info["E4"].update({
            "descricao": "N=5; nó 5 (primário) isolado dos demais por ~4 s; um cliente tenta escrever "
                         "nele a cada 50 ms, outro escreve no lado majoritário; depois a partição é curada",
            "particao_s": E4_PARTICAO_S, "parametros": BASE, "aquecimento": "5 escritas antes da partição",
            "duracao_por_replica": f"{E4_PARTICAO_S}s de partição + convergência"})

    duracao = round(time.time() - t_campanha, 1)
    escrever_metadados(saida, info, inicio, duracao, rapido, reps)
    agregar_e_plotar(saida, log)
    log(f"Campanha concluída em {duracao:.1f} s ({duracao / 60:.1f} min).")
    return info


# ----------------------------------------------------------------------
# Metadados
# ----------------------------------------------------------------------
def _cmd(cmd):
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=20,
                              cwd=RAIZ).stdout.strip()
    except Exception:  # noqa: BLE001
        return ""


def _ram_gb():
    try:
        if sys.platform == "win32":
            import ctypes

            class MS(ctypes.Structure):
                _fields_ = [("l", ctypes.c_ulong), ("m", ctypes.c_ulong), ("total", ctypes.c_ulonglong),
                            ("a", ctypes.c_ulonglong), ("b", ctypes.c_ulonglong),
                            ("c", ctypes.c_ulonglong), ("d", ctypes.c_ulonglong),
                            ("e", ctypes.c_ulonglong), ("f", ctypes.c_ulonglong)]
            ms = MS()
            ms.l = ctypes.sizeof(MS)
            ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(ms))
            return round(ms.total / 2 ** 30, 1)
        with open("/proc/meminfo") as f:
            return round(int(f.readline().split()[1]) / 2 ** 20, 1)
    except Exception:  # noqa: BLE001
        return None


def _cpu_nome():
    if sys.platform == "win32":
        nome = _cmd(["powershell", "-NoProfile", "-Command", "(Get-CimInstance Win32_Processor).Name"])
        if nome:
            return nome.splitlines()[0].strip()
    return platform.processor() or platform.machine()


def _versoes():
    out = {}
    for mod in ("numpy", "matplotlib", "pytest"):
        try:
            out[mod] = __import__(mod).__version__
        except Exception:  # noqa: BLE001
            pass
    return out


def escrever_metadados(saida, info, inicio, duracao, rapido, reps):
    """Grava/atualiza ``metadados.json`` (mantém experimentos de execuções parciais anteriores)."""
    caminho = os.path.join(saida, "metadados.json")
    meta = {}
    if os.path.exists(caminho):
        try:
            with open(caminho, encoding="utf-8") as f:
                meta = json.load(f)
        except ValueError:
            meta = {}
    exps = meta.get("experimentos", {})
    exps.update(info)
    meta.update({
        "modo_execucao": "processos locais independentes (python main.py no ...), 127.0.0.1, "
                         "SEM contêineres/Docker (mesmo código que o compose.yaml)",
        "rapido": rapido,
        "data_inicio_ultima_execucao": inicio,
        "duracao_ultima_execucao_s": duracao,
        "comando": "python main.py experimentos" + (" --rapido" if rapido else ""),
        "hardware": {"cpu": _cpu_nome(), "nucleos_logicos": os.cpu_count(), "ram_gb": _ram_gb(),
                     "maquina": platform.machine(), "hostname": socket.gethostname()},
        "so": f"{platform.system()} {platform.release()} ({platform.version()})",
        "python": sys.version, "bibliotecas": _versoes(),
        "git": {"commit": _cmd(["git", "rev-parse", "HEAD"]),
                "arvore_com_alteracoes": bool(_cmd(["git", "status", "--porcelain"]))},
        "relogio": "time.perf_counter (campo p dos logs), comparável entre processos",
        "replicas_por_configuracao": reps,
        "experimentos": exps,
    })
    with open(caminho, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    return caminho


# ----------------------------------------------------------------------
# Agregação e gráficos a partir dos CSV brutos
# ----------------------------------------------------------------------
def agregar_e_plotar(saida, log=print):
    from . import graficos

    def bruto(nome):
        return os.path.join(saida, "brutos", nome + ".csv")
    arquivos, dados = [], {}
    if os.path.exists(bruto("e1_bully")):
        dados["e1"] = validas(carregar_csv(bruto("e1_bully")))
        arquivos.append(agregado_csv(
            dados["e1"], ["variante", "n"],
            ["t_deteccao_s", "t_eleicao_s", "t_ate_anuncio_s", "t_sync_s", "t_ate_pronto_s",
             "msg_eleicao", "msg_ok", "msg_coord", "msg_total_env", "msg_total_rec",
             "n_iniciadores", "n_detectores", "rodadas"], os.path.join(saida, "e1_agregado.csv")))
    if os.path.exists(bruto("e2_escrita")):
        dados["e2"] = validas(carregar_csv(bruto("e2_escrita")))
        arquivos.append(agregado_csv(
            dados["e2"], ["n", "atraso_ms"],
            ["vazao_ops_s", "lat_media_ms", "lat_desvio_ms", "lat_p50_ms", "lat_p95_ms", "lat_p99_ms",
             "srv_media_ms", "srv_p50_ms", "srv_p95_ms", "srv_p99_ms", "msg_replicar", "msg_ack", "msg_por_escrita", "n_escritas"],
            os.path.join(saida, "e2_agregado.csv")))
        if os.path.exists(bruto("e2_latencias")):
            arquivos.append(_e2_agrupado(bruto("e2_latencias"),
                                         os.path.join(saida, "e2_latencia_agrupada.csv")))
    if os.path.exists(bruto("e3a_failover")):
        dados["e3a"] = validas(carregar_csv(bruto("e3a_failover")))
        arquivos.append(agregado_csv(
            dados["e3a"], ["timeout_fd_s"],
            ["janela_s", "t_deteccao_s", "t_eleicao_s", "t_sync_s", "t_ate_pronto_s",
             "t_ult_ok_antes_s", "t_prim_ok_depois_s", "escritas_ok", "escritas_falhas"],
            os.path.join(saida, "e3a_agregado.csv")))
    if os.path.exists(bruto("e3b_falsas_suspeitas")):
        dados["e3b"] = validas(carregar_csv(bruto("e3b_falsas_suspeitas")))
        arquivos.append(agregado_csv(
            dados["e3b"], ["perda"],
            ["susp_total", "susp_heartbeat_ausente", "susp_backup_sem_resposta", "susp_replicar_sem_ack",
             "susp_detector", "susp_detector_por_min", "susp_falsas_revogadas", "eleicoes_espurias",
             "eleicoes_espurias_por_min", "el_lider_suspeito", "el_eleicao_recebida", "el_outros",
             "mudancas_de_lider", "remocoes_de_visao",
             "escritas_ok", "escritas_falhas"], os.path.join(saida, "e3b_agregado.csv")))
    if os.path.exists(bruto("e4_particao")):
        dados["e4"] = validas(carregar_csv(bruto("e4_particao")))
        for r in dados["e4"]:
            r["grupo"] = 0
        arquivos.append(agregado_csv(
            dados["e4"], ["grupo"],
            ["tent_primario_antigo", "conf_primario_antigo", "t_novo_lider_pronto_s", "escritas_maioria_ok",
             "t_renuncia_apos_cura_s", "t_reconvergencia_lider_s", "t_convergencia_s",
             "orfas_no_estado_final", "perdidas_cliente"], os.path.join(saida, "e4_agregado.csv")))
    arquivos += graficos.gerar_todos(dados, saida)
    log("Arquivos de agregados/gráficos:")
    for a in arquivos:
        log(f"  {a}")
    return arquivos


def _e2_agrupado(caminho_lat, destino):
    """Percentis sobre todas as escritas de todas as réplicas (amostra agrupada)."""
    grupos, srv = {}, {}
    with open(caminho_lat, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            grupos.setdefault((int(r["n"]), int(r["atraso_ms"])), []).append(float(r["lat_ms"]))
            srv.setdefault((int(r["n"]), int(r["atraso_ms"])), []).append(float(r["lat_servidor_ms"]))
    with open(destino, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["n", "atraso_ms", "n_escritas", "media_ms", "desvio_ms", "min_ms", "p50_ms",
                    "p95_ms", "p99_ms", "max_ms", "srv_p50_ms", "srv_p95_ms", "srv_p99_ms"])
        for (n, a), vals in sorted(grupos.items()):
            r = est.resumo_latencia(vals)
            w.writerow([n, a, r["n"]] + [f"{r[k]:.4f}" for k in
                                         ("media", "desvio", "min", "p50", "p95", "p99", "max")]
                                         + [f"{est.percentil(srv[(n, a)], q):.4f}" for q in (50, 95, 99)])
    return destino
