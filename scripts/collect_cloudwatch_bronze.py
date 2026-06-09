from datetime import datetime, timezone, timedelta
from pathlib import Path
import argparse

import boto3
import pandas as pd


DEFAULT_LOG_GROUP = "/aws/lambda/website-s3-iac-cv-controle"


def utc_now():
    return datetime.now(timezone.utc)


def to_millis(dt):
    return int(dt.timestamp() * 1000)


def build_collection_id(dt):
    return dt.strftime("%Y%m%dT%H%M%SZ")


def collect_events(log_group, start_time_ms, end_time_ms):
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


def build_dataframe(
    events,
    log_group,
    collected_at,
    collection_id,
    window_start,
    window_end,
):
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


def write_bronze(df, output_base, reference_date, collection_id):
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
        default="data/bronze",
    )

    args = parser.parse_args()

    window_end = utc_now()
    window_start = window_end - timedelta(hours=25)

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
       print("Nenhum evento coletado. Arquivo não gerado.")
       return

    output_path = write_bronze(
        df=df,
        output_base=Path(args.output_base),
        reference_date=window_end,
        collection_id=collection_id,
    )

    print(f"Eventos coletados: {len(df)}")
    print(f"Collection ID: {collection_id}")
    print(f"Arquivo Bronze gerado: {output_path}")


if __name__ == "__main__":
    main()