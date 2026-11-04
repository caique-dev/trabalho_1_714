"""Testes de integração: processos reais, TCP em localhost, crash = Popen.kill().

Cobrem, para cada algoritmo, um cenário normal e um cenário de falha. As
propriedades de correção (um líder por época, escritas confirmadas não se
perdem, réplicas convergem) são checadas pelo verificador de logs.
"""

import json
import os
import threading
import time

from src import cliente as cli
from src import verificador
from src.cliente import Cliente, chamar_no
from src import mensagens as M


# ----------------------------------------------------------------------
# Utilitários
# ----------------------------------------------------------------------

def verificar_logs(c, vivos=None):
    """Roda o verificador sobre os logs e o estado final das réplicas vivas."""
    c.aguardar_convergencia()
    estados = {i: {"dados": s["dados"], "seq": s["seq"]}
               for i, s in c.status_vivos(incluir_dados=True).items()
               if vivos is None or i in vivos}
    res = verificador.verificar(verificador.carregar_eventos(c.dir_logs), estados)
    assert res.ok, verificador.formatar(res)
    return res


def eventos(c, nome, no=None, **filtro):
    out = [e for e in verificador.carregar_eventos(c.dir_logs) if e["evento"] == nome]
    if no is not None:
        out = [e for e in out if e["no"] == no]
    return [e for e in out if all(e.get(k) == v for k, v in filtro.items())]


# ----------------------------------------------------------------------
# Bully (eleição) + heartbeat
# ----------------------------------------------------------------------

def test_bully_normal_converge_para_o_maior_id(criar_cluster):
    c = criar_cluster(5)
    assert c.aguardar_lider(esperado=5) == 5
    st = c.status_vivos()
    assert {s["lider"] for s in st.values()} == {5}
    assert len({s["epoca"] for s in st.values()}) == 1
    assert [i for i, s in st.items() if s["papel"] == "lider"] == [5]
    res = verificar_logs(c)
    assert res.resumo["epocas_com_lider_efetivo"] >= 1


def test_bully_lider_morre_e_volta(criar_cluster):
    c = criar_cluster(5)
    c.aguardar_lider(esperado=5)
    epoca_antes = c.status(5)["epoca"]
    c.matar(5)
    assert c.aguardar_lider(esperado=4) == 4
    st = c.status_vivos()
    assert {s["lider"] for s in st.values()} == {4}
    assert all(s["epoca"] > epoca_antes for s in st.values())
    # o nó 5 reinicia, dispara eleição e, sendo o maior id, retoma a liderança
    c.reiniciar(5)
    assert c.aguardar_lider(esperado=5) == 5
    assert c.status(5)["epoca"] > c.status(1)["epoca"] - 1
    verificar_logs(c)


def test_bully_varias_falhas(criar_cluster):
    c = criar_cluster(5)
    c.aguardar_lider(esperado=5)
    c.matar(5)
    c.matar(4)
    assert c.aguardar_lider(esperado=3) == 3          # 1,2,3 vivos: ainda há maioria
    c.matar(3)
    # com 1 e 2 vivos o Bully ainda elege o 2 (sem maioria ele não fica "pronto")
    c.aguardar(lambda: all((c.status(i) or {}).get("lider") == 2 for i in (1, 2)),
               timeout=15, descricao="nós 1 e 2 concordam em líder 2")
    assert c.status(2)["pronto"] is False
    for i in (3, 4, 5):
        c.reiniciar(i)
    assert c.aguardar_lider(esperado=5) == 5
    verificar_logs(c)


def test_heartbeat_registra_suspeita_quando_lider_morre(criar_cluster):
    c = criar_cluster(5)
    c.aguardar_lider(esperado=5)
    t_kill = time.time()
    c.matar(5)
    c.aguardar_lider(esperado=4)
    susp = eventos(c, "suspeita", alvo=5)
    assert susp, "nenhum nó registrou suspeita do líder morto"
    assert all(e["motivo"] in ("heartbeat_ausente", "backup_sem_resposta") for e in susp)
    assert all(e["t"] >= t_kill - 0.1 for e in susp)


def test_falsa_suspeita_e_registrada_e_revogada(criar_cluster):
    """Pares 1 e 5 ficam incomunicáveis (sem crash): 1 suspeita de 5; ao curar, a suspeita é falsa."""
    c = criar_cluster(5)
    c.aguardar_lider(esperado=5)
    c.falhas(1, bloqueados=[5])
    c.falhas(5, bloqueados=[1])
    c.aguardar(lambda: eventos(c, "suspeita", no=1, alvo=5), 10, "nó 1 suspeita do líder")
    c.curar()
    c.aguardar(lambda: eventos(c, "suspeita_revogada", no=1, alvo=5, falsa=True), 10,
               "suspeita revogada como falsa")
    assert c.aguardar_lider(esperado=5) == 5
    c.aguardar_convergencia()
    assert c.status(1)["suspeitas_falsas"] >= 1
    verificar_logs(c)


# ----------------------------------------------------------------------
# Replicação primário-backup
# ----------------------------------------------------------------------

def test_replicacao_normal_replicas_identicas(criar_cluster):
    c = criar_cluster(5)
    c.aguardar_lider(esperado=5)
    cl = c.cliente()
    for i in range(30):
        r = cl.put(f"chave{i}", {"n": i})
        assert r.ok and r.dados["seq"] == i + 1
    c.aguardar_convergencia()
    st = c.status_vivos(incluir_dados=True)
    assert len({s["hash"] for s in st.values()}) == 1
    assert all(s["seq"] == 30 and s["dados"]["chave7"] == {"n": 7} for s in st.values())
    assert cl.get("chave3").dados["valor"] == {"n": 3}
    assert cl.get("inexistente").dados["existe"] is False
    # custo de mensagens: 2(N-1) por escrita = 4 REPLICAR + 4 ACK
    cont = c.status(5)["contadores"]
    assert cont["enviadas"]["REPLICAR"] == 30 * 4
    assert cont["recebidas"]["ACK"] >= 30 * 4
    verificar_logs(c)


def test_nao_primario_redireciona_cliente(criar_cluster):
    c = criar_cluster(5)
    c.aguardar_lider(esperado=5)
    resp = chamar_no(c.enderecos[1], M.CLI_PUT, {"chave": "k", "valor": 1}, timeout=2.0)
    assert resp["dados"]["ok"] is False
    assert resp["dados"]["erro"] == "nao_lider" and resp["dados"]["redirecionar"] == 5
    # o cliente que começa por um seguidor acaba no primário
    cl = Cliente({1: c.enderecos[1]} | {5: c.enderecos[5]})
    cl.dica = 1
    r = cl.put("k", 1)
    assert r.ok and r.no == 5 and r.tentativas == 2


def test_backup_morto_sai_da_visao_e_escrita_continua(criar_cluster):
    c = criar_cluster(5)
    c.aguardar_lider(esperado=5)
    cl = c.cliente()
    c.matar(2)
    for i in range(5):
        assert cl.put(f"k{i}", i).ok       # confirmadas com os 4 restantes (maioria = 3)
    assert 2 not in c.status(5)["visao"]
    assert eventos(c, "visao_removido", no=5, alvo=2)


def test_primario_morre_durante_carga_nao_perde_escritas_confirmadas(criar_cluster):
    c = criar_cluster(5)
    c.aguardar_lider(esperado=5)
    confirmadas, falhas = [], []
    parar_carga = threading.Event()

    def carga():
        cl = c.cliente(timeout=0.8, prazo=15.0)
        i = 0
        while not parar_carga.is_set() and i < 80:
            r = cl.put(f"k{i:03d}", f"v{i}")
            (confirmadas if r.ok else falhas).append(i)
            i += 1

    t = threading.Thread(target=carga)
    t.start()
    c.aguardar(lambda: len(confirmadas) >= 15, 15, "15 escritas antes do crash")
    c.matar(5)                       # crash do primário no meio da carga
    t.join(timeout=60)
    parar_carga.set()
    assert not t.is_alive()
    assert len(confirmadas) >= 60, (len(confirmadas), falhas)   # o cliente atravessou o failover

    assert c.aguardar_lider(esperado=4) == 4
    c.reiniciar(5)                   # o nó recuperado recebe o estado por transferência
    assert c.aguardar_lider(esperado=5) == 5
    c.aguardar_convergencia()
    for s in c.status_vivos(incluir_dados=True).values():
        assert all(s["dados"].get(f"k{i:03d}") == f"v{i}" for i in confirmadas)
    res = verificar_logs(c)
    assert res.resumo["escritas_confirmadas"] >= len(confirmadas)


def test_no_recuperado_re_sincroniza_antes_de_entrar_na_visao(criar_cluster):
    c = criar_cluster(5)
    c.aguardar_lider(esperado=5)
    cl = c.cliente()
    for i in range(5):
        assert cl.put(f"a{i}", i).ok
    c.matar(2)
    for i in range(10):
        assert cl.put(f"b{i}", i).ok      # o nó 2 perde estas escritas
    c.reiniciar(2)                        # volta com o estado antigo (5 escritas) do disco
    c.aguardar_convergencia()
    s2 = c.status(2, incluir_dados=True)
    assert s2["seq"] == 15 and s2["dados"]["b9"] == 9
    assert 2 in c.status(5)["visao"]
    assert eventos(c, "visao_adicionado", no=5, alvo=2)
    assert eventos(c, "transferencia_estado", no=2, direcao="instalado")
    verificar_logs(c)


def test_cluster_inteiro_reinicia_e_recupera_do_disco(criar_cluster):
    c = criar_cluster(5)
    c.aguardar_lider(esperado=5)
    cl = c.cliente()
    for i in range(10):
        assert cl.put(f"p{i}", i).ok
    for i in range(1, 6):
        c.matar(i)
    for i in range(1, 6):
        c.iniciar_no(i)
    assert c.aguardar_lider(esperado=5, timeout=30) == 5
    cl = c.cliente()
    assert all(cl.get(f"p{i}").dados["valor"] == i for i in range(10))
    assert cl.put("depois", 1).dados["seq"] == 11
    verificar_logs(c)


def test_sem_quorum_nao_confirma_escrita(criar_cluster):
    c = criar_cluster(5)
    c.aguardar_lider(esperado=5)
    for i in (1, 2, 3):
        c.matar(i)               # restam 4 e 5: menos que a maioria (3)
    c.aguardar(lambda: c.status(5)["visao"] == [4], 10, "visão do primário encolher")
    r = chamar_no(c.enderecos[5], M.CLI_PUT, {"chave": "x", "valor": 1}, timeout=2.0)
    assert r["dados"]["ok"] is False and r["dados"]["erro"] == "sem_quorum"
    assert chamar_no(c.enderecos[5], M.CLI_GET, {"chave": "x"}, timeout=2.0)["dados"]["ok"] is False


def test_fencing_primario_isolado_nao_confirma_e_e_corrigido(criar_cluster):
    c = criar_cluster(5)
    c.aguardar_lider(esperado=5)
    cl_todos = c.cliente()
    for i in range(3):
        assert cl_todos.put(f"antes{i}", i).ok

    c.isolar(5)                                       # partição: o primário fica sozinho
    # 1) o primário antigo ainda se julga líder, mas não consegue confirmar
    r = chamar_no(c.enderecos[5], M.CLI_PUT, {"chave": "orfa", "valor": "x"}, timeout=3.0)
    assert r["dados"]["ok"] is False
    assert r["dados"]["erro"] in ("sem_quorum", "lider_deposto")

    # 2) o outro lado elege o 4 e continua escrevendo
    assert c.aguardar_lider(esperado=4, ids=[1, 2, 3, 4]) == 4
    cl_maioria = Cliente({i: c.enderecos[i] for i in (1, 2, 3, 4)}, timeout=0.8)
    for i in range(3):
        assert cl_maioria.put(f"depois{i}", i).ok

    # 3) ao curar a partição o 5 renuncia, retoma a liderança (maior id), e a
    #    escrita não confirmada dele é descartada (estado do outro lado prevalece)
    c.curar()
    assert c.aguardar_lider(esperado=5, timeout=30) == 5
    c.aguardar_convergencia()
    for s in c.status_vivos(incluir_dados=True).values():
        assert "orfa" not in s["dados"]
        assert all(s["dados"][f"depois{i}"] == i for i in range(3))
    assert not eventos(c, "escrita_confirmada", chave="orfa")
    assert eventos(c, "renuncia", no=5)
    verificar_logs(c)


def test_descarte_de_mensagens_nao_viola_invariantes(criar_cluster):
    """Perda aleatória de 10% de todas as mensagens entre nós: o sistema pode ficar lento ou
    trocar de líder, mas as invariantes de segurança continuam valendo."""
    c = criar_cluster(5)
    c.aguardar_lider(esperado=5)
    for i in c.enderecos:
        c.falhas(i, prob_descarte=0.1, semente=7)
    cl = c.cliente(timeout=1.0, prazo=20.0)
    confirmadas = [i for i in range(15) if cl.put(f"d{i}", i).ok]
    c.curar()
    c.aguardar_lider(timeout=40)
    c.aguardar_convergencia(timeout=40)
    assert len(confirmadas) >= 10
    for s in c.status_vivos(incluir_dados=True).values():
        assert all(s["dados"].get(f"d{i}") == i for i in confirmadas)
    verificar_logs(c)


def test_status_expoe_contadores_e_estado(criar_cluster):
    c = criar_cluster(5)
    c.aguardar_lider(esperado=5)
    s = c.status(5)
    for campo in ("papel", "lider", "epoca", "visao", "seq", "contadores", "quorum", "pronto"):
        assert campo in s
    assert s["contadores"]["enviadas"]["HEARTBEAT"] > 0
    assert c.status(4)["contadores"]["recebidas"]["HEARTBEAT"] > 0
    # eventos com timestamp no log (a fase 2 mede tempos a partir deles)
    pronto = eventos(c, "lider_pronto", no=5)[0]
    assert {"t", "p", "epoca", "duracao_sync"} <= set(pronto)
