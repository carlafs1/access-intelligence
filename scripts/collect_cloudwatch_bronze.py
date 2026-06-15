####---------------------------------------------------------------------------------------####
####----          Fase 1 — Coleta logs do CloudWatch e grava a camada Bronze.          ----####
####---------------------------------------------------------------------------------------####
####----                                                                               ----#### 
####----  Objetivo:                                                                    ----####
####----    Coletar eventos da Lambda controle e armazenar os logs brutos em Parquet,  ----####
####----    preservando metadados de coleta para rastreabilidade e reprocessamento.    ----####
####----                                                                               ----####
####----  Estratégia:                                                                  ----####
####----    1. Lê o último ponto de controle em data/control/pipeline_state.json       ----####
####----    2. Se existir execução anterior, coleta a partir do último window_end      ----####
####----    3. Se não existir, coleta as últimas 25 horas                              ----####
####----    4. Grava os eventos em data/bronze/cloudwatch/...                          ----####
####----    5. Atualiza o estado somente após gravação bem-sucedida                    ----####
####---------------------------------------------------------------------------------------####

from datetime import datetime, timezone, timedelta
from pathlib import Path
import argparse
import json

import boto3
import pandas as pd


####-------------------------------####
####----  Configuração padrão  ----####
####-------------------------------####

DEFAULT_LOG_GROUP     = "/aws/lambda/website-s3-iac-cv-controle"
DEFAULT_OUTPUT_BASE   = "data/bronze"
DEFAULT_STATE_FILE = "data/control/cloudwatch_to_bronze.json"

# Sobreposição de segurança para evitar perda de eventos entre execuções.
# A deduplicação posterior na Silver tratará eventuais duplicidades.
SSM_OVERLAP_PARAMETER =  "/website-s3-iac-cv/bronze-overlap-minutes"


####----------------------------####
####----  Funções de tempo  ----####
####----------------------------####

def utc_now():
    """Retorna o timestamp atual em UTC."""
    return datetime.now(timezone.utc)


def to_millis(dt):
    """Converte datetime UTC para timestamp em milissegundos, formato usado pela AWS."""
    return int(dt.timestamp() * 1000)


def build_collection_id(dt):
    """Cria um identificador único para a coleta."""
    return dt.strftime("%Y%m%dT%H%M%SZ")


####------------------------------------------####
####----  Controle de estado do pipeline  ----####
####------------------------------------------####

def load_state(state_file):
    """
    Lê o estado da última execução bem-sucedida.

    Exemplo esperado:
      {
        "cloudwatch_to_bronze": {
          "last_successful_window_end": "2026-06-10T03:00:00+00:00",
          "last_collection_id": "20260610T030000Z"
        }
      }
    """
    path = Path(state_file)

    if not path.exists():
        return {}

    return json.loads(path.read_text(encoding="utf-8"))


def save_state(state_file, state):
    """Grava o estado atualizado do pipeline."""
    path = Path(state_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(state, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def resolve_collection_window(state):
    """
    Define a janela de coleta.

    Se já houve execução anterior:
      começa no último window_end, com pequena sobreposição.

    Se nunca executou:
      coleta as últimas 25 horas.
    """
    now = utc_now()

    previous = state.get("cloudwatch_to_bronze", {})
    last_end = previous.get("last_successful_window_end")

    if last_end:
        window_start = datetime.fromisoformat(last_end)

        if window_start.tzinfo is None:
            window_start = window_start.replace(tzinfo=timezone.utc)

        overlap_minutes = get_overlap_minutes()

        window_start = window_start - timedelta(minutes=overlap_minutes)
    else:
        window_start = now - timedelta(hours=25)

    window_end = now

    return window_start, window_end


def update_state_after_success(
    state,
    collection_id,
    window_start,
    window_end,
    output_path,
    events_count,
):
    """Atualiza o estado somente após a Bronze ser gravada com sucesso."""
    state["cloudwatch_to_bronze"] = {
        "last_successful_window_start": window_start.isoformat(),
        "last_successful_window_end": window_end.isoformat(),
        "last_collection_id": collection_id,
        "last_output_path": str(output_path),
        "last_events_count": events_count,
        "updated_at_utc": utc_now().isoformat(),
    }

    return state


####--------------------------####
####----  Parâmetros SSM  ----####
####--------------------------####

def get_overlap_minutes():
    """
    Lê o overlap configurado no SSM.

    Em caso de erro utiliza valor padrão.
    """
    try:
        ssm = boto3.client("ssm")

        response = ssm.get_parameter(
            Name=SSM_OVERLAP_PARAMETER
        )

        return int(response["Parameter"]["Value"])

    except Exception as exc:
        print(
            f"[WARNING] Falha ao ler "
            f"{SSM_OVERLAP_PARAMETER}. "
            f"Usando valor padrão 5. "
            f"Erro: {exc}"
        )

        return 5


####--------------------------------####
####----  Coleta no CloudWatch  ----####
####--------------------------------####

def collect_events(log_group, start_time_ms, end_time_ms):
    """
    Coleta eventos do CloudWatch Logs usando paginação.

    O paginator é importante porque a AWS pode devolver os resultados
    em várias páginas.
    """
    client = boto3.client("logs")
    paginator = client.get_paginator("filter_log_events")

    events = []

    for page in paginator.paginate(
        logGroupName=log_group,
        startTime=start_time_ms,
        endTime=end_time_ms,
    ):
        events.extend(page.get("events", []))

    return events


####------------------------------------------####
####----  Construção do DataFrame Bronze  ----####
####------------------------------------------####

def build_dataframe(
    events,
    log_group,
    collected_at,
    collection_id,
    window_start,
    window_end,
):
    """
    Transforma os eventos brutos do CloudWatch em tabela Bronze.

    A Bronze ainda não interpreta a mensagem.
    Ela apenas preserva:
      - mensagem original
      - timestamp do evento
      - log stream
      - metadados da coleta
    """
    rows = []

    for event in events:
        rows.append(
            {
                "event_id": event.get("eventId"),
                "timestamp_utc": datetime.fromtimestamp(
                    event.get("timestamp") / 1000,
                    tz=timezone.utc,
                ).isoformat(),
                "ingestion_time": datetime.fromtimestamp(
                    event.get("ingestionTime") / 1000,
                    tz=timezone.utc,
                ).isoformat(),
                "log_group": log_group,
                "log_stream": event.get("logStreamName"),
                "message": event.get("message"),
                "collected_at_utc": collected_at.isoformat(),
                "collection_id": collection_id,
                "collection_window_start": window_start.isoformat(),
                "collection_window_end": window_end.isoformat(),
            }
        )

    return pd.DataFrame(rows)


####------------------------------------####
####----  Escrita da camada Bronze  ----####
####------------------------------------####

def write_bronze(df, output_base, reference_date, collection_id):
    """
    Grava a Bronze particionada por data de referência e collection_id.

    Exemplo:
      data/bronze/cloudwatch/year=2026/month=06/day=10/
        collection_id=20260610T120000Z/logs.parquet
    """
    output_path = (
        output_base
        / "cloudwatch"
        / f"year={reference_date.year:04d}"
        / f"month={reference_date.month:02d}"
        / f"day={reference_date.day:02d}"
        / f"collection_id={collection_id}"
        / "logs.parquet"
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_parquet(
        output_path,
        index=False,
        compression="snappy",
    )

    return output_path


####-----------------------####
####----  Entry point  ----####
####-----------------------####

def main():
    parser = argparse.ArgumentParser(
        description="Coleta logs do CloudWatch e grava a camada Bronze em Parquet."
    )

    parser.add_argument(
        "--log-group",
        default=DEFAULT_LOG_GROUP,
    )

    parser.add_argument(
        "--output-base",
        default=DEFAULT_OUTPUT_BASE,
    )

    parser.add_argument(
        "--state-file",
        default=DEFAULT_STATE_FILE,
    )

    args = parser.parse_args()

    state = load_state(args.state_file)

    window_start, window_end = resolve_collection_window(state)

    collected_at = utc_now()
    collection_id = build_collection_id(collected_at)

    events = collect_events(
        log_group=args.log_group,
        start_time_ms=to_millis(window_start),
        end_time_ms=to_millis(window_end),
    )

    df = build_dataframe(
        events=events,
        log_group=args.log_group,
        collected_at=collected_at,
        collection_id=collection_id,
        window_start=window_start,
        window_end=window_end,
    )

    if df.empty:
        print("Nenhum evento coletado. Estado não atualizado.")
        return

    output_path = write_bronze(
        df=df,
        output_base=Path(args.output_base),
        reference_date=window_end,
        collection_id=collection_id,
    )

    state = update_state_after_success(
        state=state,
        collection_id=collection_id,
        window_start=window_start,
        window_end=window_end,
        output_path=output_path,
        events_count=len(df),
    )

    save_state(args.state_file, state)

    print(f"Eventos coletados: {len(df)}")
    print(f"Collection ID: {collection_id}")
    print(f"Janela: {window_start.isoformat()} até {window_end.isoformat()}")
    print(f"Arquivo Bronze gerado: {output_path}")
    print(f"Estado atualizado: {args.state_file}")


if __name__ == "__main__":
    main()