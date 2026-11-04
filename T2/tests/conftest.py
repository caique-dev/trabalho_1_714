"""Fixtures dos testes de integração: clusters de processos reais em localhost."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.cluster import Cluster          # noqa: E402
from src.config import Parametros        # noqa: E402

# Temporizações curtas para a suíte terminar em poucos minutos: heartbeat de
# 100 ms, suspeita após 500 ms, chamadas com prazo de 300 ms.
PARAMS_TESTE = dict(intervalo_hb=0.1, timeout_fd=0.5, timeout_rpc=0.3)


@pytest.fixture
def criar_cluster(tmp_path, request):
    """Fábrica de clusters. Todos os processos são mortos ao final, mesmo se o teste falhar."""
    criados = []

    def criar(n=5, **extra):
        params = Parametros(**{**PARAMS_TESTE, **extra})
        c = Cluster(n, diretorio=str(tmp_path / f"exec{len(criados)}"), params=params)
        criados.append(c)           # registrado antes de iniciar: a limpeza cobre falha na subida
        c.iniciar()
        return c

    def limpar():
        for c in criados:
            c.parar()

    request.addfinalizer(limpar)
    return criar
