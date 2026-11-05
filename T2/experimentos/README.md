# Experimentos

Os scripts da campanha experimental não ficam nesta pasta: estão em
[`../src/experimentos.py`](../src/experimentos.py) (cenários E1–E4),
[`../src/estatistica.py`](../src/estatistica.py) (média, desvio, IC95%, percentis) e
[`../src/graficos.py`](../src/graficos.py). Os dados brutos, os agregados, os gráficos e os
metadados ficam em [`../resultados/`](../resultados/).

```bash
docker compose run --rm experimentos --rapido   # smoke test, grava em resultados_rapido/
docker compose run --rm experimentos            # campanha completa (~32 min), sobrescreve resultados/
```

Sem Docker: `python main.py experimentos [--rapido]` (ver o README principal).
