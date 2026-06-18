"""
TESTE DOCUMENTAL - FASE 1

Objetivo:
  Validar a decisão arquitetural da camada Bronze.

Regras:
  - Bronze é imutável (append-only).
  - Cada execução gera um novo collection_id.
  - O estado operacional é mutável.
  - O estado mantém somente a última execução bem-sucedida.

Não acessa AWS.
Não lê CloudWatch.
Não grava arquivos.
Serve como documentação executável da Fase 1.
"""

from datetime import datetime, timezone
from pathlib import Path
import json
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.collect_cloudwatch_bronze import update_state_after_success


def print_state(title, state):
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)
    print(json.dumps(state, indent=2, ensure_ascii=False))


def main():
    state = {}
    print_state("ESTADO INICIAL", state)

    # ------------------------------------------------------------------
    # Simulação da primeira coleta
    # ------------------------------------------------------------------
    state = update_state_after_success(
        state=state,
        collection_id="20260610T040000Z",
        window_start=datetime(2026, 6, 10, 3, 0, 0, tzinfo=timezone.utc),
        window_end=datetime(2026, 6, 10, 4, 0, 0, tzinfo=timezone.utc),
        output_paths=[
            Path(
                "data/bronze/cloudwatch/"
                "year=2026/month=06/day=10/"
                "collection_id=20260610T040000Z/"
                "logs.parquet"
            )
        ],
        events_count=100,
        now=datetime(2026, 6, 10, 4, 0, 5, tzinfo=timezone.utc),
    )
    print_state(
        "APÓS EXECUÇÃO 1 (COLLECTION_ID=20260610T040000Z)",
        state,
    )

    # ------------------------------------------------------------------
    # Simulação da segunda coleta
    # ------------------------------------------------------------------
    state = update_state_after_success(
        state=state,
        collection_id="20260610T050000Z",
        window_start=datetime(2026, 6, 10, 4, 0, 0, tzinfo=timezone.utc),
        window_end=datetime(2026, 6, 10, 5, 0, 0, tzinfo=timezone.utc),
        output_paths=[
            Path(
                "data/bronze/cloudwatch/"
                "year=2026/month=06/day=10/"
                "collection_id=20260610T050000Z/"
                "logs.parquet"
            )
        ],
        events_count=80,
        now=datetime(2026, 6, 10, 5, 0, 3, tzinfo=timezone.utc),
    )
    print_state(
        "APÓS EXECUÇÃO 2 (COLLECTION_ID=20260610T050000Z)",
        state,
    )

    # ------------------------------------------------------------------
    # Simulação de coleta que atravessa a meia-noite
    # (valida que output_paths pode ter múltiplas entradas)
    # ------------------------------------------------------------------
    state = update_state_after_success(
        state=state,
        collection_id="20260610T235959Z",
        window_start=datetime(2026, 6, 10, 23, 50, 0, tzinfo=timezone.utc),
        window_end=datetime(2026, 6, 11, 0, 10, 0, tzinfo=timezone.utc),
        output_paths=[
            Path(
                "data/bronze/cloudwatch/"
                "year=2026/month=06/day=10/"
                "collection_id=20260610T235959Z/"
                "logs.parquet"
            ),
            Path(
                "data/bronze/cloudwatch/"
                "year=2026/month=06/day=11/"
                "collection_id=20260610T235959Z/"
                "logs.parquet"
            ),
        ],
        events_count=45,
        now=datetime(2026, 6, 11, 0, 10, 2, tzinfo=timezone.utc),
    )
    print_state(
        "APÓS EXECUÇÃO 3 — JANELA QUE ATRAVESSA MEIA-NOITE (2 PARTIÇÕES)",
        state,
    )

    # ------------------------------------------------------------------
    # Asserções
    # ------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("VERIFICAÇÕES")
    print("=" * 80)

    cw = state["cloudwatch_to_bronze"]

    assert cw["last_collection_id"] == "20260610T235959Z", \
        "Estado deve refletir a última coleta"
    assert len(cw["last_output_paths"]) == 2, \
        "Coleta que atravessa meia-noite deve registrar 2 partições"
    assert cw["last_events_count"] == 45
    print("OK — estado reflete apenas a última execução bem-sucedida.")

    print("\n" + "=" * 80)
    print("RESULTADO ESPERADO")
    print("=" * 80)
    print("""
1. A primeira execução gerou:
   collection_id = 20260610T040000Z

2. A segunda execução gerou:
   collection_id = 20260610T050000Z

3. A terceira execução atravessou a meia-noite e gerou 2 partições:
   collection_id = 20260610T235959Z
   last_output_paths com 2 entradas (day=10 e day=11)

4. O estado operacional foi atualizado a cada execução.

5. A Bronze continua imutável:
   cada collection_id representa uma nova coleta independente.

6. O arquivo de controle mantém apenas
   a última execução bem-sucedida.
""")


if __name__ == "__main__":
    main()