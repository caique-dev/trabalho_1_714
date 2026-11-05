"""Testes unitários das máquinas de estado e utilitários (sem rede, rápidos)."""

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src import mensagens as M                       # noqa: E402
from src.config import Parametros, parse_pares, formatar_pares  # noqa: E402
from src.falhas import InjetorFalhas                 # noqa: E402
from src.persistencia import Armazenamento, ler_estado_disco  # noqa: E402
from src.replica import APLICADO, DUPLICADO, LACUNA, Replica  # noqa: E402
from src.transporte import Contadores                # noqa: E402
from src import verificador                          # noqa: E402


# ----------------------------------------------------------------------
# Replica
# ----------------------------------------------------------------------

def test_escrever_local_incrementa_seq_e_registra_epoca():
    r = Replica()
    assert r.escrever_local(1, "a", 1) == (1, 0)
    assert r.escrever_local(1, "b", 2) == (2, 1)
    assert r.chave_estado() == (1, 2)
    assert r.dados == {"a": 1, "b": 2}


def test_aplicar_em_ordem_e_idempotente():
    r = Replica()
    assert r.aplicar(1, 1, 0, "a", 1) == APLICADO
    assert r.aplicar(1, 2, 1, "b", 2) == APLICADO
    # retransmissão da mesma escrita: ignorada, estado inalterado
    assert r.aplicar(1, 2, 1, "b", 2) == DUPLICADO
    assert r.aplicar(1, 1, 0, "a", 1) == DUPLICADO
    assert r.chave_estado() == (1, 2) and r.dados == {"a": 1, "b": 2}


def test_aplicar_detecta_lacuna():
    r = Replica()
    assert r.aplicar(1, 3, 1, "c", 3) == LACUNA
    assert r.seq == 0 and r.dados == {}


def test_aplicar_detecta_historico_divergente():
    """Cauda não confirmada de um primário antigo não deve aceitar a escrita do novo."""
    r = Replica()
    r.aplicar(1, 1, 0, "a", 1)
    r.aplicar(1, 2, 1, "x", "nao-confirmada")      # veio do primário da época 1
    # o novo primário (época 2) parte do seq 1 e escreve o seq 2 com prev_epoca = 1
    assert r.aplicar(2, 2, 1, "b", 2) == LACUNA     # seq 2 já ocupado por outra época
    # ou, se o novo primário já está no seq 2 da época 2, a escrita 3 tem prev_epoca = 2
    assert r.aplicar(2, 3, 2, "c", 3) == LACUNA     # nossa ult_epoca é 1, não 2


def test_instalar_snapshot_substitui_estado():
    origem = Replica()
    origem.escrever_local(2, "k", "v")
    destino = Replica()
    destino.aplicar(1, 1, 0, "lixo", 0)
    destino.instalar(origem.snapshot())
    assert destino.dados == {"k": "v"} and destino.chave_estado() == (2, 1)


def test_ordem_de_posicao_prioriza_epoca():
    antigo, novo = Replica(), Replica()
    for i in range(10):
        antigo.escrever_local(1, f"k{i}", i)
    novo.escrever_local(2, "k", 0)
    assert novo.chave_estado() > antigo.chave_estado()  # época 2 vence seq maior de época 1


def test_hash_igual_para_mesmo_conteudo():
    a, b = Replica(), Replica()
    a.escrever_local(1, "x", 1); a.escrever_local(1, "y", 2)
    b.escrever_local(1, "y", 2); b.escrever_local(1, "x", 1)
    assert a.hash_estado() == b.hash_estado()


# ----------------------------------------------------------------------
# Persistência (crash-recovery)
# ----------------------------------------------------------------------

def test_persistencia_recupera_estado_e_meta(tmp_path):
    d = str(tmp_path / "no1")
    arm = Armazenamento(d)
    rep = Replica(arm)
    arm.gravar_meta(3, 5)
    for i in range(5):
        rep.escrever_local(3, f"k{i}", i)
    arm.fechar()  # (um kill não fecharia, mas o flush por escrita garante o mesmo conteúdo)

    arm2 = Armazenamento(d)
    rep2 = Replica(arm2)
    assert arm2.meta == {"epoca": 3, "lider": 5}
    assert rep2.dados == {f"k{i}": i for i in range(5)}
    assert rep2.chave_estado() == (3, 5)


def test_persistencia_ignora_linha_truncada_do_wal(tmp_path):
    d = str(tmp_path / "no1")
    arm = Armazenamento(d)
    rep = Replica(arm)
    rep.escrever_local(1, "a", 1)
    rep.escrever_local(1, "b", 2)
    caminho = os.path.join(d, f"wal.{arm.geracao}.jsonl")
    arm.fechar()
    with open(caminho, "a", encoding="utf-8") as f:
        f.write('{"seq": 3, "e": 1, "k": "c", "v"')   # queda no meio da escrita
    estado = ler_estado_disco(d)
    assert estado["seq"] == 2 and estado["dados"] == {"a": 1, "b": 2}
    rep2 = Replica(Armazenamento(d))   # reabrir compacta e continua gravando normalmente
    rep2.escrever_local(1, "c", 3)
    assert ler_estado_disco(d)["dados"] == {"a": 1, "b": 2, "c": 3}


def test_persistencia_instalar_descarta_historico_antigo(tmp_path):
    d = str(tmp_path / "no1")
    rep = Replica(Armazenamento(d))
    rep.escrever_local(1, "velho", 1)
    rep.instalar({"dados": {"novo": 9}, "seq": 7, "ult_epoca": 4})
    rep.escrever_local(4, "depois", 2)
    estado = ler_estado_disco(d)
    assert estado["dados"] == {"novo": 9, "depois": 2} and estado["seq"] == 8


# ----------------------------------------------------------------------
# Injeção de falhas
# ----------------------------------------------------------------------

def test_injetor_reprodutivel_por_semente():
    a, b = InjetorFalhas(42), InjetorFalhas(42)
    for inj in (a, b):
        inj.configurar(prob_descarte=0.3, atraso_fixo=0.01, jitter=0.02)
    sa = [a.decidir(2, "REPLICAR") for _ in range(200)]
    sb = [b.decidir(2, "REPLICAR") for _ in range(200)]
    assert sa == sb
    assert 0.2 < sum(1 for causa, _ in sa if causa) / 200 < 0.4
    c = InjetorFalhas(43)
    c.configurar(prob_descarte=0.3, atraso_fixo=0.01, jitter=0.02)
    assert [c.decidir(2, "REPLICAR") for _ in range(200)] != sa


def test_injetor_particao_e_filtro_de_tipo():
    inj = InjetorFalhas(1)
    inj.configurar(bloqueados=[3], prob_descarte=1.0, so_tipos=["HEARTBEAT"])
    assert inj.decidir(3, "REPLICAR") == ("particao", 0.0)   # partição vale para todos os tipos
    assert inj.decidir(2, "HEARTBEAT")[0] == "descarte"
    assert inj.decidir(2, "REPLICAR") == (None, 0.0)         # fora do filtro
    inj.configurar(limpar=True)
    assert inj.decidir(3, "HEARTBEAT") == (None, 0.0)


def test_injetor_atraso_dentro_da_faixa():
    inj = InjetorFalhas(7)
    inj.configurar(atraso_fixo=0.05, jitter=0.1)
    for _ in range(100):
        causa, atraso = inj.decidir(1, "X")
        assert causa is None and 0.05 <= atraso <= 0.15


# ----------------------------------------------------------------------
# Mensagens, configuração, contadores
# ----------------------------------------------------------------------

def test_mensagem_ida_e_volta():
    msg = M.nova(M.REPLICAR, 5, 2, 3, {"chave": "ação", "valor": [1, 2]}, id_msg="5:1:1", inc=9)
    linha = M.codificar(msg)
    assert linha.endswith(b"\n") and linha.count(b"\n") == 1
    assert M.decodificar(linha) == msg
    with pytest.raises(ValueError):
        M.decodificar(b'{"sem_tipo": 1}\n')


def test_parse_pares():
    pares = parse_pares("1=127.0.0.1:9001, 2=no2:9000")
    assert pares == {1: ("127.0.0.1", 9001), 2: ("no2", 9000)}
    assert parse_pares(formatar_pares(pares)) == pares
    with pytest.raises(ValueError):
        parse_pares("1=semporta")


def test_parametros_timeout_coord_padrao():
    assert Parametros(timeout_rpc=0.3).timeout_coord == pytest.approx(1.2)


def test_contadores_separam_protocolo_de_externas():
    c = Contadores()
    c.enviada("ELEICAO"); c.enviada("ELEICAO")
    c.recebida("OK"); c.recebida("CLI_PUT")
    s = c.instantaneo()
    assert s["enviadas"] == {"ELEICAO": 2} and s["recebidas"] == {"OK": 1}
    assert s["externas_recebidas"] == {"CLI_PUT": 1} and s["total_enviadas"] == 2


# ----------------------------------------------------------------------
# Verificador de invariantes
# ----------------------------------------------------------------------

def _ev(p, no, evento, **campos):
    return {"t": p, "p": p, "no": no, "inc": 1, "evento": evento, **campos}


def _historia_valida():
    return [
        _ev(1, 5, "coordenador_anunciado", epoca=1),
        _ev(2, 5, "lider_pronto", epoca=1, seq=0, visao=[1, 2]),
        _ev(3, 5, "escrita_local", epoca=1, seq=1, chave="a", valor=1),
        _ev(4, 5, "escrita_confirmada", epoca=1, seq=1, chave="a", valor=1),
    ]


def test_verificador_aceita_historia_valida():
    est = {n: {"dados": {"a": 1}, "seq": 1} for n in (1, 2, 5)}
    res = verificador.verificar(_historia_valida(), est)
    assert res.ok, res.violacoes


def test_verificador_detecta_dois_lideres_na_mesma_epoca():
    ev = _historia_valida() + [_ev(5, 4, "lider_pronto", epoca=1, seq=0, visao=[])]
    res = verificador.verificar(ev)
    assert any(v.startswith("I1") for v in res.violacoes)


def test_verificador_dois_coordenadores_sao_apenas_aviso():
    ev = _historia_valida() + [_ev(1.5, 4, "coordenador_anunciado", epoca=1)]
    res = verificador.verificar(ev)
    assert res.ok and res.avisos


def test_verificador_detecta_escrita_confirmada_perdida():
    est = {1: {"dados": {"a": 1}, "seq": 1}, 2: {"dados": {}, "seq": 0}}
    res = verificador.verificar(_historia_valida(), est)
    assert any(v.startswith("I5") for v in res.violacoes)
    assert any(v.startswith("I6") for v in res.violacoes)


def test_verificador_detecta_confirmacao_de_quem_nao_era_lider():
    ev = _historia_valida() + [_ev(6, 3, "escrita_confirmada", epoca=1, seq=2, chave="b", valor=2)]
    res = verificador.verificar(ev)
    assert any(v.startswith("I3") for v in res.violacoes)


def test_verificador_detecta_seq_confirmado_com_conteudos_distintos():
    ev = _historia_valida() + [
        _ev(5, 4, "lider_pronto", epoca=2, seq=0, visao=[]),
        _ev(6, 4, "escrita_confirmada", epoca=2, seq=1, chave="outra", valor=9)]
    res = verificador.verificar(ev)
    assert any(v.startswith("I4") for v in res.violacoes)


def test_verificador_permite_escrita_posterior_nao_confirmada_adotada():
    ev = _historia_valida() + [_ev(5, 5, "escrita_local", epoca=1, seq=2, chave="a", valor=7)]
    est = {n: {"dados": {"a": 7}, "seq": 2} for n in (1, 2, 5)}
    assert verificador.verificar(ev, est).ok


def test_verificador_carrega_logs_e_ignora_linha_truncada(tmp_path):
    caminho = tmp_path / "no5.jsonl"
    with open(caminho, "w", encoding="utf-8") as f:
        for e in _historia_valida():
            f.write(json.dumps(e) + "\n")
        f.write('{"t": 9, "p": 9, "no"')
    eventos = verificador.carregar_eventos(str(tmp_path))
    assert len(eventos) == 4 and verificador.verificar(eventos).ok


def test_resumo_status_destaca_lider_sem_quorum():
    from src.cliente import resumo_status
    base = {"papel": "lider", "epoca": 3, "lider": 5, "pronto": True, "visao": [], "seq": 7}
    sem = resumo_status({**base, "quorum_ativo": False})
    assert "pronto=true" in sem and "quorum_ativo=false" in sem and "SEM quórum ativo" in sem
    com = resumo_status({**base, "quorum_ativo": True, "visao": [1, 2, 3, 4]})
    assert "quorum_ativo=true" in com and "SEM quórum" not in com
    backup = resumo_status({**base, "papel": "seguidor", "pronto": False, "quorum_ativo": None})
    assert "quorum_ativo" not in backup
