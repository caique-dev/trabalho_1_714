"""Testes rápidos da estatística da campanha (percentis, IC95, agregação) e do CSV agregado."""

import csv
import math
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src import estatistica as est               # noqa: E402
from src import experimentos as exp              # noqa: E402


def test_percentil_interpolacao_linear():
    xs = [1, 2, 3, 4, 5]
    assert est.percentil(xs, 0) == 1
    assert est.percentil(xs, 50) == 3
    assert est.percentil(xs, 100) == 5
    assert est.percentil(xs, 25) == 2
    assert est.percentil(xs, 90) == pytest.approx(4.6)       # pos = 3,6 -> 4 + 0,6*(5-4)


def test_percentil_ignora_ordem_e_aceita_um_elemento():
    assert est.percentil([5, 1, 3, 2, 4], 50) == 3
    assert est.percentil([7], 99) == 7
    assert math.isnan(est.percentil([], 50))
    with pytest.raises(ValueError):
        est.percentil([1, 2], 101)


def test_percentis_de_latencia_batem_com_a_definicao():
    xs = list(range(1, 101))                                   # 1..100
    r = est.resumo_latencia(xs)
    assert r["p50"] == pytest.approx(50.5)
    assert r["p95"] == pytest.approx(95.05)
    assert r["p99"] == pytest.approx(99.01)
    assert r["min"] == 1 and r["max"] == 100 and r["n"] == 100
    assert r["media"] == pytest.approx(50.5)


def test_resumo_vazio_nao_quebra():
    r = est.resumo_latencia([])
    assert r["n"] == 0 and math.isnan(r["p99"])


def test_agregar_media_desvio_e_ic95_t_student():
    a = est.agregar([10, 12, 14, 16, 18, 11, 13, 15, 17, 14])  # n = 10, gl = 9 -> t = 2,262
    assert a.n == 10 and a.media == pytest.approx(14.0)
    desvio = math.sqrt(sum((v - 14.0) ** 2 for v in [10, 12, 14, 16, 18, 11, 13, 15, 17, 14]) / 9)
    assert a.desvio == pytest.approx(desvio)
    assert a.margem == pytest.approx(2.262 * desvio / math.sqrt(10))
    assert a.inferior == pytest.approx(a.media - a.margem)
    assert a.superior == pytest.approx(a.media + a.margem)


def test_agregar_casos_limite():
    um = est.agregar([3.0])
    assert um.media == 3.0 and um.desvio == 0.0 and math.isnan(um.margem)
    assert est.agregar([5, 5, 5]).margem == 0.0
    assert est.agregar([]).n == 0


def test_t_critico():
    assert est.t_critico(9) == 2.262
    assert est.t_critico(1000) == 1.984 or est.t_critico(1000) == 1.960
    assert est.t_critico(30) == 2.045                          # conservador: usa gl tabelado <= 30
    assert math.isnan(est.t_critico(0))


def test_agregado_csv_formato_longo(tmp_path):
    linhas = [{"n": 3, "x": 1.0}, {"n": 3, "x": 3.0}, {"n": 5, "x": 10.0}, {"n": 5, "x": 12.0}]
    caminho = exp.agregado_csv(linhas, ["n"], ["x"], str(tmp_path / "a.csv"))
    with open(caminho, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert [r["n"] for r in rows] == ["3", "5"]
    assert float(rows[0]["media"]) == pytest.approx(2.0)
    assert float(rows[1]["p50"]) == pytest.approx(11.0)
    assert int(rows[0]["n_replicas"]) == 2 and rows[0]["metrica"] == "x"


def test_bruto_csv_roundtrip(tmp_path):
    b = exp.Bruto(str(tmp_path / "b.csv"), ["a", "b", "erro"])
    b.escrever({"a": 1, "b": 2.5})
    b.escrever({"a": 2, "erro": "falhou"})
    b.fechar()
    rows = exp.carregar_csv(str(tmp_path / "b.csv"))
    assert rows[0]["a"] == 1 and rows[0]["b"] == 2.5
    assert len(exp.validas(rows)) == 1


def test_sementes_sao_unicas_por_replica():
    s = {exp.semente(e, c, r) for e in (1, 2, 3, 4, 5) for c in range(12) for r in range(30)}
    assert len(s) == 5 * 12 * 30
