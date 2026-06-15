####---------------------------------------------------------------------------------------####
####----      Silver 01 — Reconstrói execuções da Lambda a partir da camada Bronze.    ----####
####---------------------------------------------------------------------------------------####
####----                                                                               ----####
####----  Objetivo:                                                                    ----####
####----    Ler os eventos brutos coletados do CloudWatch na Bronze Parquet e remontar ----####
####----    cada execução da Lambda controle como um bloco único START → END.          ----####
####----                                                                               ----####
####----  Regra de leitura incremental:                                                ----####
####----    - Se data/control/bronze_to_silver.json não existir, processa todo o       ----####
####----      histórico Bronze.                                                        ----####
####----    - Se existir, processa apenas os arquivos posteriores ao último arquivo    ----####
####----      Bronze processado com sucesso.                                           ----####
####----    - Este módulo apenas lê o controle. A atualização do JSON deve ocorrer     ----####
####----      somente ao final do run_silver.py, após gravação bem-sucedida da Silver. ----####
####----                                                                               ----####
####----  Entrada:                                                                     ----####
####----    data/bronze/cloudwatch/**/*.parquet                                        ----####
####----                                                                               ----####
####----  Saída em memória:                                                            ----####
####----    DataFrame Bronze + lista de arquivos Bronze selecionados                   ----####
####----    Lista de blocos com request_id, log_stream, start_ts, end_ts e block_text  ----####
####----                                                                               ----####
####---------------------------------------------------------------------------------------####

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pandas as pd


####------------------------####
####----  Configuração  ----####
####------------------------####

DEFAULT_BRONZE_BASE = "data/bronze/cloudwatch"
DEFAULT_STATE_FILE = "data/control/bronze_to_silver.json"


####-------------------####
####----  Regexes  ----####
####-------------------####

RE_START = re.compile(r"START RequestId:\s*(?P<request_id>\S+)")
RE_END = re.compile(r"END RequestId:\s*(?P<request_id>\S+)")


####--------------------------------------------####
####----  Seleção incremental dos arquivos  ----####
####--------------------------------------------####

def select_bronze_files(
    bronze_base: str = DEFAULT_BRONZE_BASE,
    state_file: str = DEFAULT_STATE_FILE,
) -> list[Path]:
    """
    Seleciona os arquivos Bronze que ainda precisam ser processados pela Silver.

    Critério:
      - Sem controle: processa todos os Parquets encontrados.
      - Com controle: processa somente arquivos com caminho maior que o último
        arquivo processado com sucesso.

    Observação:
      A ordenação funciona porque a Bronze está particionada por:
        year=YYYY/month=MM/day=DD/collection_id=YYYYMMDDTHHMMSSZ/logs.parquet
    """
    files = sorted(Path(bronze_base).rglob("*.parquet"))

    if not files:
        raise FileNotFoundError(
            f"Nenhum arquivo Parquet encontrado em {bronze_base}"
        )

    state_path = Path(state_file)

    if not state_path.exists():
        print("Controle Silver não encontrado. Processando todo o histórico Bronze.")
        return files

    state = json.loads(state_path.read_text(encoding="utf-8"))

    last_processed_file = (
        state
        .get("bronze_to_silver", {})
        .get("last_processed_file")
    )

    if not last_processed_file:
        print("Controle Silver existe, mas não possui last_processed_file. Processando tudo.")
        return files

    last_path = Path(last_processed_file)

    files_to_process = [
        path for path in files
        if str(path) > str(last_path)
    ]

    if not files_to_process:
        raise FileNotFoundError(
            "Nenhum arquivo Bronze pendente para processar na Silver."
        )

    return files_to_process


####-------------------------------------####
####----  Leitura da Bronze Parquet  ----####
####-------------------------------------####

def load_bronze(
    bronze_base: str = DEFAULT_BRONZE_BASE,
    state_file: str = DEFAULT_STATE_FILE,
) -> tuple[pd.DataFrame, list[Path]]:
    """
    Lê os arquivos Parquet da Bronze selecionados para a execução atual.

    Retorna:
      - DataFrame Pandas com os eventos Bronze selecionados.
      - Lista de arquivos Bronze lidos, para o orquestrador registrar no controle
        somente após a Silver finalizar com sucesso.
    """
    files = select_bronze_files(
        bronze_base=bronze_base,
        state_file=state_file,
    )

    print("Arquivos Bronze selecionados para a Silver:", files)

    for path in files:
        print(f" - {path}")

    dataframes = [pd.read_parquet(path) for path in files]

    df = pd.concat(
        dataframes,
        ignore_index=True,
    )

    required_columns = {
        "event_id",
        "timestamp_utc",
        "log_stream",
        "message",
    }

    missing = required_columns - set(df.columns)

    if missing:
        raise ValueError(
            f"Colunas obrigatórias ausentes na Bronze: {sorted(missing)}"
        )

    df["timestamp_utc"] = pd.to_datetime(
        df["timestamp_utc"],
        utc=True,
        errors="coerce",
    )

    df = df.sort_values(
        ["log_stream", "timestamp_utc", "event_id"],
        kind="stable",
    )

    df = df.drop_duplicates(
        subset=["event_id"],
        keep="last",
    )

    print(f"Eventos Bronze carregados: {len(df)}")

    return df, files


####---------------------------------------------####
####----  Reconstrução dos blocos START/END  ----####
####---------------------------------------------####

def reconstruct_blocks(df: pd.DataFrame) -> list[dict[str, Any]]:
    """
    Reconstrói blocos de execução da Lambda controle.

    O CloudWatch entrega cada linha do log como um evento separado.
    A Silver precisa reagrupar essas linhas em uma execução lógica,
    delimitada por:

      START RequestId: ...
      ...
      END RequestId: ...

    Cada bloco reconstruído será usado nas próximas etapas da Silver para:
      - extrair o JSON recebido;
      - classificar decisões operacionais;
      - enriquecer visitantes.
    """
    blocks: list[dict[str, Any]] = []

    for log_stream, group in df.groupby("log_stream", sort=False):
        current: dict[str, Any] | None = None

        for _, row in group.iterrows():
            message = str(row.get("message") or "")
            timestamp = row.get("timestamp_utc")
            log_group = row.get("log_group", "")

            start = RE_START.search(message)

            if start:
                if current is not None:
                    current["block_text"] = "".join(current["lines"])
                    current["closed"] = False
                    blocks.append(current)

                current = {
                    "request_id": start.group("request_id"),
                    "log_group": log_group,
                    "log_stream": log_stream,
                    "start_ts": timestamp,
                    "end_ts": None,
                    "lines": [message],
                    "closed": False,
                }

                continue

            end = RE_END.search(message)

            if end and current is not None:
                current["lines"].append(message)
                current["end_ts"] = timestamp
                current["block_text"] = "".join(current["lines"])
                current["closed"] = True

                blocks.append(current)
                current = None

                continue

            if current is not None:
                current["lines"].append(message)

        if current is not None:
            current["block_text"] = "".join(current["lines"])
            current["closed"] = False
            blocks.append(current)

    return blocks