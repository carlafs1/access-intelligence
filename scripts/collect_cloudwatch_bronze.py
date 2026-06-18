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
####----    4. Grava os eventos em data/bronze/cloudwatch/..., particionados pela      ----####
####----       data real de cada evento (não pelo momento da coleta)                   ----####
####----    5. Atualiza o estado somente após gravação bem-sucedida                    ----####
####----    6. Suporta janela manual (--window-start/--window-end) para reprocessar    ----####
####----       um intervalo específico sem depender do state_file                      ----####
####----                                                                               ----####
####----  Schema Bronze (ordem das colunas):                                           ----####
####----    event_id                — ID único do evento no CloudWatch (deduplicação)  ----####
####----    timestamp_ms            — Timestamp do evento em ms (precisão original AWS)----####
####----    timestamp_utc           — Timestamp do evento em ISO 8601 UTC              ----####
####----    ingestion_time_ms       — Timestamp de ingestão em ms (precisão original)  ----####
####----    ingestion_time          — Timestamp de ingestão em ISO 8601 UTC            ----####
####----    log_group               — Log group do CloudWatch                          ----####
####----    log_stream              — Log stream do CloudWatch                         ----####
####----    message                 — Mensagem bruta (JSON ou texto livre da Lambda)   ----####
####----    message_size            — Tamanho em bytes de message (detecção anomalia)  ----####
####----    source_service          — Origem do log (cloudwatch_logs, waf_logs etc.)   ----####
####----    aws_region              — Região AWS da coleta                             ----####
####----    collected_at_utc        — Momento exato desta coleta em ISO 8601 UTC       ----####
####----    collection_id           — ID único da execução desta coleta                ----####
####----    collection_type         — incremental | manual_reprocess                   ----####
####----    collection_window_start — Início da janela de coleta em ISO 8601 UTC       ----####
####----    collection_window_end   — Fim da janela de coleta em ISO 8601 UTC          ----####
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

DEFAULT_LOG_GROUP   = "/aws/lambda/website-s3-iac-cv-controle"
DEFAULT_OUTPUT_BASE = "data/bronze"
DEFAULT_STATE_FILE  = "data/control/cloudwatch_to_bronze.json"
SOURCE_SERVICE      = "cloudwatch_logs"

# Ordem canônica das colunas da camada Bronze.
# Aplicada explicitamente no build_dataframe para garantir consistência
# independente da implementação interna do pd.DataFrame.
#
# IMPORTANTE: df[BRONZE_COLUMNS] levanta KeyError se qualquer coluna
# estiver faltando — comportamento intencional (falha alto, falha cedo).
# NÃO substituir por df.reindex(columns=BRONZE_COLUMNS): reindex preenche
# colunas ausentes com NaN silenciosamente, escondendo bugs no schema.
BRONZE_COLUMNS = [
    "event_id",
    "timestamp_ms",
    "timestamp_utc",
    "ingestion_time_ms",
    "ingestion_time",
    "log_group",
    "log_stream",
    "message",
    "message_size",
    "source_service",
    "aws_region",
    "collected_at_utc",
    "collection_id",
    "collection_type",
    "collection_window_start",
    "collection_window_end",
]

# Sobreposição de segurança para evitar perda de eventos entre execuções.
# A deduplicação posterior na Silver tratará eventuais duplicidades.
SSM_OVERLAP_PARAMETER = "/website-s3-iac-cv/bronze-overlap-minutes"


####----------------------------####
####----  Funções de tempo  ----####
####----------------------------####

def utc_now():
    """Retorna o timestamp atual em UTC."""
    return datetime.now(timezone.utc)


def parse_iso_utc(value):
    """Converte uma string ISO em datetime UTC, assumindo UTC quando não há tzinfo."""
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


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


def resolve_collection_window(state, now):
    """
    Define a janela de coleta a partir do estado salvo.

    Se já houve execução anterior:
      começa no último window_end, com pequena sobreposição.

    Se nunca executou:
      coleta as últimas 25 horas.

    O `now` é recebido como parâmetro (e não chamado internamente) para garantir
    que toda a execução compartilhe o mesmo instante de referência.
    """
    previous = state.get("cloudwatch_to_bronze", {})
    last_end = previous.get("last_successful_window_end")

    if last_end:
        window_start = parse_iso_utc(last_end)
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
    output_paths,
    events_count,
    now,
):
    """Atualiza o estado somente após a Bronze ser gravada com sucesso."""
    state["cloudwatch_to_bronze"] = {
        "last_successful_window_start": window_start.isoformat(),
        "last_successful_window_end": window_end.isoformat(),
        "last_collection_id": collection_id,
        "last_output_paths": [str(p) for p in output_paths],
        "last_events_count": events_count,
        "updated_at_utc": now.isoformat(),
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


####-------------------------------####
####----  Metadados de sessão  ----####
####-------------------------------####

def get_aws_region():
    """
    Resolve a região AWS ativa na sessão boto3.

    Útil para rastreabilidade em ambientes multi-região ou DR.
    Fallback para us-east-2 caso a sessão não tenha região configurada.
    """
    session = boto3.session.Session()
    return session.region_name or "us-east-2"


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
    collection_type,
    window_start,
    window_end,
    aws_region,
):
    """
    Transforma os eventos brutos do CloudWatch em tabela Bronze.

    A Bronze não interpreta a mensagem — preserva o conteúdo original
    integralmente e acrescenta metadados de coleta para rastreabilidade.

    Campos adicionados além dos originais da AWS:
      - timestamp_ms / ingestion_time_ms: valores em ms para auditoria e
        comparações sem risco de perda de precisão na conversão ISO.
      - message_size: detecta explosão de logs e eventos anômalos.
      - source_service: prepara o schema para futuras origens (waf_logs etc.).
      - aws_region: viabiliza rastreabilidade em ambientes multi-região ou DR.
      - collection_type: diferencia coletas automáticas de reprocessamentos manuais.
    """
    rows = []

    for event in events:
        ts_ms   = event.get("timestamp")
        ing_ms  = event.get("ingestionTime")
        message = event.get("message")

        rows.append(
            {
                "event_id": event.get("eventId"),

                # Timestamps em ms (precisão original da AWS) e ISO 8601 UTC.
                # Proteção contra eventos corrompidos: se o valor vier None
                # (raro, mas possível), gravar None sem explodir a coleta.
                "timestamp_ms": ts_ms,
                "timestamp_utc": (
                    datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()
                    if ts_ms is not None
                    else None
                ),
                "ingestion_time_ms": ing_ms,
                "ingestion_time": (
                    datetime.fromtimestamp(ing_ms / 1000, tz=timezone.utc).isoformat()
                    if ing_ms is not None
                    else None
                ),

                # Origem do log
                "log_group": log_group,
                "log_stream": event.get("logStreamName"),

                # Conteúdo bruto + tamanho para detecção de anomalia.
                # encode("utf-8") mede bytes reais: caracteres acentuados
                # ocupam 2+ bytes em UTF-8, então len(str) subestimaria o tamanho.
                "message": message,
                "message_size": (
                    len(message.encode("utf-8"))
                    if message
                    else 0
                ),

                # Metadados de coleta
                "source_service": SOURCE_SERVICE,
                "aws_region": aws_region,
                "collected_at_utc": collected_at.isoformat(),
                "collection_id": collection_id,
                "collection_type": collection_type,
                "collection_window_start": window_start.isoformat(),
                "collection_window_end": window_end.isoformat(),
            }
        )

    # df[BRONZE_COLUMNS] garante a ordem canônica E levanta KeyError se
    # qualquer coluna esperada estiver faltando — falha alto, falha cedo.
    return pd.DataFrame(rows)[BRONZE_COLUMNS]


####------------------------------------####
####----  Escrita da camada Bronze  ----####
####------------------------------------####

def write_bronze(df, output_base, collection_id):
    """
    Grava a Bronze particionada pela data REAL de cada evento (timestamp_utc)
    e pelo collection_id da execução.

    Importante: uma única execução pode coletar eventos que pertencem a dias
    diferentes (ex.: janela que atravessa a meia-noite). Por isso, o DataFrame
    é agrupado por dia antes da escrita, gerando um arquivo por partição:

      data/bronze/cloudwatch/year=2026/month=06/day=10/
        collection_id=20260610T120000Z/logs.parquet
      data/bronze/cloudwatch/year=2026/month=06/day=11/
        collection_id=20260610T120000Z/logs.parquet

    Isso evita que eventos do dia anterior fiquem "escondidos" na partição
    do dia em que a coleta terminou.
    """
    df = df.copy()
    df["_event_date"] = pd.to_datetime(df["timestamp_utc"], utc=True).dt.date

    output_paths = []

    for event_date, group in df.groupby("_event_date"):
        group = group.drop(columns=["_event_date"])

        output_path = (
            output_base
            / "cloudwatch"
            / f"year={event_date.year:04d}"
            / f"month={event_date.month:02d}"
            / f"day={event_date.day:02d}"
            / f"collection_id={collection_id}"
            / "logs.parquet"
        )

        output_path.parent.mkdir(parents=True, exist_ok=True)

        group.to_parquet(
            output_path,
            index=False,
            compression="snappy",
        )

        output_paths.append(output_path)

    return output_paths


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

    parser.add_argument(
        "--window-start",
        default=None,
        help=(
            "Início manual da janela de coleta, em ISO 8601 UTC "
            "(ex.: 2026-06-10T00:00:00+00:00). Use junto com --window-end "
            "para reprocessar um intervalo específico, ignorando o state_file."
        ),
    )

    parser.add_argument(
        "--window-end",
        default=None,
        help="Fim manual da janela de coleta, em ISO 8601 UTC.",
    )

    parser.add_argument(
        "--update-state",
        action="store_true",
        help=(
            "Quando usado em conjunto com --window-start/--window-end, força "
            "a atualização do state_file com a janela manual. Por padrão, "
            "janelas manuais NÃO atualizam o state_file, para não interferir "
            "no fluxo incremental automático."
        ),
    )

    args = parser.parse_args()

    # Único instante de referência para toda a execução: evita pequenas
    # divergências entre collection_window_end e collected_at_utc.
    now = utc_now()

    state = load_state(args.state_file)

    manual_window = bool(args.window_start and args.window_end)
    collection_type = "manual_reprocess" if manual_window else "incremental"

    if manual_window:
        window_start = parse_iso_utc(args.window_start)
        window_end = parse_iso_utc(args.window_end)
        print(
            f"Janela manual informada: {window_start.isoformat()} "
            f"até {window_end.isoformat()}"
        )
    else:
        window_start, window_end = resolve_collection_window(state, now=now)

    collected_at   = now
    collection_id  = build_collection_id(collected_at)
    aws_region     = get_aws_region()

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
        collection_type=collection_type,
        window_start=window_start,
        window_end=window_end,
        aws_region=aws_region,
    )

    if df.empty:
        print("Nenhum evento coletado. Estado não atualizado.")
        return

    output_paths = write_bronze(
        df=df,
        output_base=Path(args.output_base),
        collection_id=collection_id,
    )

    should_update_state = (not manual_window) or args.update_state

    if should_update_state:
        state = update_state_after_success(
            state=state,
            collection_id=collection_id,
            window_start=window_start,
            window_end=window_end,
            output_paths=output_paths,
            events_count=len(df),
            now=now,
        )

        save_state(args.state_file, state)
    else:
        print(
            "Janela manual usada para reprocessamento: state_file NÃO foi "
            "atualizado (use --update-state para forçar)."
        )

    print(f"Eventos coletados:  {len(df)}")
    print(f"Collection ID:      {collection_id}")
    print(f"Collection type:    {collection_type}")
    print(f"AWS region:         {aws_region}")
    print(f"Janela:             {window_start.isoformat()} até {window_end.isoformat()}")
    print("Arquivos Bronze gerados:")
    for path in output_paths:
        print(f"  - {path}")
    if should_update_state:
        print(f"Estado atualizado:  {args.state_file}")


if __name__ == "__main__":
    main()