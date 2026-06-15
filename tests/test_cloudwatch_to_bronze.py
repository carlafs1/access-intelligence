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
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


from datetime import datetime, timezone
from pathlib import Path
import json

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
        window_start=datetime(
            2026, 6, 10, 3, 0, 0,
            tzinfo=timezone.utc
        ),
        window_end=datetime(
            2026, 6, 10, 4, 0, 0,
            tzinfo=timezone.utc
        ),
        output_path=Path(
            "data/bronze/cloudwatch/"
            "year=2026/month=06/day=10/"
            "collection_id=20260610T040000Z/"
            "logs.parquet"
        ),
        events_count=100,
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
        window_start=datetime(
            2026, 6, 10, 4, 0, 0,
            tzinfo=timezone.utc
        ),
        window_end=datetime(
            2026, 6, 10, 5, 0, 0,
            tzinfo=timezone.utc
        ),
        output_path=Path(
            "data/bronze/cloudwatch/"
            "year=2026/month=06/day=10/"
            "collection_id=20260610T050000Z/"
            "logs.parquet"
        ),
        events_count=80,
    )

    print_state(
        "APÓS EXECUÇÃO 2 (COLLECTION_ID=20260610T050000Z)",
        state,
    )

    print("\n" + "=" * 80)
    print("RESULTADO ESPERADO")
    print("=" * 80)

    print("""
1. A primeira execução gerou:
   collection_id = 20260610T040000Z

2. A segunda execução gerou:
   collection_id = 20260610T050000Z

3. O estado operacional foi atualizado.

4. A Bronze continua imutável:
   cada collection_id representa uma nova coleta.

5. O arquivo de controle mantém apenas
   a última execução bem-sucedida.
""")


if __name__ == "__main__":
    main()