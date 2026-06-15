####---------------------------------------------------------------------------------------####
####----              Silver Orchestrator — Executa a transformação completa.          ----####
####---------------------------------------------------------------------------------------####
####----                                                                               ----####
####----  Objetivo:                                                                    ----####
####----    Orquestrar os módulos da camada Silver, transformando a Bronze Parquet em  ----####
####----    eventos estruturados, classificados e enriquecidos.                        ----####
####----                                                                               ----####
####----  Fluxo:                                                                       ----####
####----    1. Ler Bronze Parquet                                                      ----####
####----    2. Reconstruir blocos START → END da Lambda controle                       ----####
####----    3. Extrair eventos HTTP do JSON recebido                                   ----####
####----    4. Classificar decisões operacionais                                       ----####
####----    5. Enriquecer visitantes                                                   ----####
####----    6. Deduplicar por request_id                                               ----####
####----    7. Gravar Silver final em Parquet                                          ----####
####----                                                                               ----####
####----  Saída oficial:                                                               ----####
####----    data/silver/access_events/year=YYYY/month=MM/day=DD/events.parquet         ----####
####----                                                                               ----####
####---------------------------------------------------------------------------------------####

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from scripts.silver.reconstruct_blocks import load_bronze, reconstruct_blocks
from scripts.silver.extract_events import extract_events
from scripts.silver.classify_events import classify_events
from scripts.silver.enrich_visitors import (
    enrich_visitors,
    propagate_visitor_id_to_operational_events,
)


####------------------------####
####----  Configuração  ----####
####------------------------####

SILVER_BASE = Path("data/silver/access_events")
STATE_FILE = Path("data/control/bronze_to_silver.json")


####---------------------------####
####----  Escrita Parquet  ----####
####---------------------------####

def write_silver(records: list[dict]) -> Path:
    """
    Grava a Silver final em Parquet, particionada pela data do primeiro evento.
    """
    df = pd.DataFrame(records)

    if df.empty:
        raise ValueError("Nenhum registro para gravar na Silver.")

    df = df.drop(columns=["block_text"], errors="ignore")

    df = df.drop_duplicates(
        subset=["request_id"],
        keep="last",
    )

    df["timestamp_utc_dt"] = pd.to_datetime(
        df["timestamp_utc"],
        utc=True,
        errors="coerce",
    )

    first_ts = df["timestamp_utc_dt"].dropna().min()

    if pd.isna(first_ts):
        partition_dt = datetime.now(timezone.utc)
    else:
        partition_dt = first_ts.to_pydatetime()

    out_dir = (
        SILVER_BASE
        / f"year={partition_dt.year:04d}"
        / f"month={partition_dt.month:02d}"
        / f"day={partition_dt.day:02d}"
    )

    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / "events.parquet"

    df = df.drop(columns=["timestamp_utc_dt"], errors="ignore")

    df.to_parquet(
        out_path,
        index=False,
        compression="snappy",
    )

    return out_path


####-----------------------------####
####----  Controle incremental ----####
####-----------------------------####

def update_state(bronze_files: list[Path], silver_path: Path) -> None:
    """
    Atualiza o controle incremental somente após a Silver ser gravada com sucesso.
    """
    if not bronze_files:
        raise ValueError("Lista de arquivos Bronze vazia. Controle não atualizado.")

    last_processed_file = str(sorted(bronze_files)[-1])

    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)

    state = {
        "bronze_to_silver": {
            "last_processed_file": last_processed_file,
            "last_silver_file": str(silver_path),
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        }
    }

    STATE_FILE.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"Controle Silver atualizado: {STATE_FILE}")
    print(f"Último Bronze processado: {last_processed_file}")


####-----------------------####
####----  Entry point  ----####
####-----------------------####

def main() -> None:
    """
    Executa a transformação completa da camada Silver.
    """
    bronze_df, bronze_files = load_bronze()
    print(f"Eventos Bronze lidos: {len(bronze_df)}")

    blocks = reconstruct_blocks(bronze_df)
    print(f"Blocos Lambda reconstruídos: {len(blocks)}")

    records = extract_events(blocks)
    print(f"Eventos extraídos: {len(records)}")

    records = classify_events(records)
    print("Classificação operacional concluída.")

    records = enrich_visitors(records)
    print("Enriquecimento de visitantes concluído.")

    records = propagate_visitor_id_to_operational_events(records)
    print("Propagação de visitor_id para eventos operacionais concluída.")

    out_path = write_silver(records)
    print(f"Arquivo Silver gerado: {out_path}")

    update_state(
        bronze_files=bronze_files,
        silver_path=out_path,
    )


if __name__ == "__main__":
    main()