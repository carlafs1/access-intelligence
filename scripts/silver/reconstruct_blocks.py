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
####----    - "Nenhum arquivo pendente" não é erro — é o estado normal após uma        ----####
####----      execução bem-sucedida sem novos dados. Retorna lista vazia.              ----####
####----    - Este módulo apenas lê o controle. A atualização do JSON deve ocorrer     ----####
####----      somente ao final do run_silver.py, após gravação bem-sucedida da Silver. ----####
####----                                                                               ----####
####----  Entrada:                                                                     ----####
####----    data/bronze/cloudwatch/**/*.parquet                                        ----####
####----                                                                               ----####
####----  Saída em memória:                                                            ----####
####----    DataFrame Bronze + lista de arquivos Bronze selecionados                   ----####
####----    Lista de blocos com os campos abaixo (insumos para extract_events.py):     ----####
####----                                                                               ----####
####----  Campos produzidos por bloco (dicionário de dados — aba Final):               ----####
####----    request_id   — extraído da linha START RequestId (Silver e Gold)           ----####
####----    log_group    — preservado da Bronze (rastreabilidade)                      ----####
####----    log_stream   — preservado da Bronze (rastreabilidade)                      ----####
####----    start_ts     — datetime UTC do evento START (base do timestamp_utc)        ----####
####----    end_ts       — datetime UTC do evento END (None se bloco incompleto)       ----####
####----    block_closed — True se END foi encontrado (Silver: Reconstrução START/END) ----####
####----    block_text   — texto completo do bloco; campo técnico temporário,          ----####
####----                   removido antes da gravação da Silver final                  ----####
####----                                                                               ----####
####----  Deduplicação:                                                                ----####
####----    event_id é usado para deduplicação antes da reconstrução, eliminando       ----####
####----    duplicatas geradas pelo overlap de segurança da Bronze.                    ----####
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

DEFAULT_BRONZE_BASE = "data/bronze"
DEFAULT_STATE_FILE  = "data/control/bronze_to_silver.json"


####-------------------####
####----  Regexes  ----####
####-------------------####

RE_START = re.compile(r"START RequestId:\s*(?P<request_id>\S+)")
RE_END   = re.compile(r"END RequestId:\s*(?P<request_id>\S+)")


####--------------------------------------------####
####----  Seleção incremental dos arquivos  ----####
####--------------------------------------------####

def select_bronze_files(
    bronze_base: str = DEFAULT_BRONZE_BASE,
    state_file: str  = DEFAULT_STATE_FILE,
) -> list[Path]:
    """
    Seleciona os arquivos Bronze que ainda precisam ser processados pela Silver.

    Critério:
      - Sem controle: processa todos os Parquets encontrados.
      - Com controle: processa somente arquivos com caminho maior que o último
        arquivo processado com sucesso.

    Retorna lista vazia quando não há arquivos pendentes — comportamento normal
    após execução bem-sucedida sem novos dados. Não levanta exceção nesse caso,
    pois o orquestrador (run_silver.py) decide o que fazer com lista vazia.

    Observação:
      A ordenação por caminho funciona porque a Bronze está particionada por:
        year=YYYY/month=MM/day=DD/collection_id=YYYYMMDDTHHMMSSZ/logs.parquet
    """
    all_files = sorted(Path(bronze_base).rglob("*.parquet"))

    if not all_files:
        print(f"Nenhum arquivo Parquet encontrado em: {bronze_base}")
        return []

    state_path = Path(state_file)

    if not state_path.exists():
        print("Controle Silver não encontrado. Processando todo o histórico Bronze.")
        return all_files

    state = json.loads(state_path.read_text(encoding="utf-8"))

    last_processed_file = (
        state
        .get("bronze_to_silver", {})
        .get("last_processed_file")
    )

    if not last_processed_file:
        print(
            "Controle Silver existe, mas não possui last_processed_file. "
            "Processando todo o histórico Bronze."
        )
        return all_files

    last_path = Path(last_processed_file)

    pending = [f for f in all_files if str(f) > str(last_path)]

    if not pending:
        print("Nenhum arquivo Bronze pendente. Silver já está atualizada.")

    return pending


####-------------------------------------####
####----  Leitura da Bronze Parquet  ----####
####-------------------------------------####

def load_bronze(
    bronze_base: str = DEFAULT_BRONZE_BASE,
    state_file: str  = DEFAULT_STATE_FILE,
) -> tuple[pd.DataFrame, list[Path]]:
    """
    Lê os arquivos Parquet da Bronze selecionados para a execução atual.

    Etapas:
      1. Seleciona arquivos pendentes (ou vazio se já atualizado).
      2. Lê e concatena os Parquets selecionados.
      3. Valida colunas obrigatórias.
      4. Converte timestamp_utc para datetime UTC.
      5. Ordena por log_stream → timestamp_utc → event_id (ordem de chegada).
      6. Deduplica por event_id (keep="last" — garante dado mais recente
         em caso de reprocessamento com overlap de janela).

    Retorna:
      - DataFrame com eventos Bronze prontos para reconstrução.
      - Lista de arquivos lidos (para o orquestrador registrar no controle
        somente após a Silver finalizar com sucesso).
    """
    files = select_bronze_files(bronze_base=bronze_base, state_file=state_file)

    if not files:
        return pd.DataFrame(), []

    for path in files:
        print(f"  Lendo: {path}")

    df = pd.concat(
        [pd.read_parquet(path) for path in files],
        ignore_index=True,
    )

    # Valida colunas obrigatórias — event_id é essencial para deduplicação
    required_columns = {"event_id", "timestamp_utc", "log_stream", "message"}
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

    # Ordena antes de deduplicar para garantir que "last" seja o evento
    # cronologicamente mais recente (relevante no overlap de segurança da Bronze).
    df = df.sort_values(
        ["log_stream", "timestamp_utc", "event_id"],
        kind="stable",
    ).drop_duplicates(
        subset=["event_id"],
        keep="last",
    )

    print(f"Eventos Bronze carregados após deduplicação: {len(df)}")

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

      START RequestId: <request_id>
      ...linhas intermediárias...
      END RequestId: <request_id>

    Comportamento para blocos incompletos:
      - Novo START antes de END do anterior: fecha o bloco anterior com
        block_closed = False e inicia o novo. Ocorre quando a Lambda
        é encerrada abruptamente (timeout, OOM) ou quando o log foi
        coletado antes do END ser emitido.
      - Fim do log_stream sem END: o bloco em aberto é fechado com
        block_closed = False.

    Separador "\n" entre linhas:
      As mensagens do CloudWatch podem ou não terminar com "\n". Usar
      "\n".join() garante que linhas consecutivas nunca fiquem coladas,
      o que quebraria os regexes do extract_events.py.

    Cada bloco produzido alimenta diretamente extract_events.py.
    O campo block_text é técnico e temporário — deve ser removido
    antes da gravação final da Silver.
    """
    blocks: list[dict[str, Any]] = []

    for log_stream, group in df.groupby("log_stream", sort=False):
        current: dict[str, Any] | None = None

        for _, row in group.iterrows():
            message   = str(row.get("message") or "")
            timestamp = row.get("timestamp_utc")
            log_group = str(row.get("log_group", "") or "")

            start_match = RE_START.search(message)

            if start_match:
                # Fecha bloco anterior sem END (bloco incompleto)
                if current is not None:
                    current["block_text"] = "\n".join(current["_lines"])
                    current["block_closed"] = False
                    del current["_lines"]
                    blocks.append(current)

                current = {
                    "request_id":   start_match.group("request_id"),
                    "log_group":    log_group,
                    "log_stream":   log_stream,
                    "start_ts":     timestamp,
                    "end_ts":       None,
                    "block_closed": False,
                    "_lines":       [message],
                }
                continue

            end_match = RE_END.search(message)

            if (
                end_match
                and current is not None
                and end_match.group("request_id") == current["request_id"]
            ):
                current["_lines"].append(message)
                current["end_ts"]       = timestamp
                current["block_text"]   = "\n".join(current["_lines"])
                current["block_closed"] = True
                del current["_lines"]
                blocks.append(current)
                current = None
                continue

            if current is not None:
                current["_lines"].append(message)

        # Fim do log_stream: fecha bloco em aberto como incompleto
        if current is not None:
            current["block_text"]   = "\n".join(current["_lines"])
            current["block_closed"] = False
            del current["_lines"]
            blocks.append(current)

    return blocks