####---------------------------------------------------------------------------------------####
####----              Silver Orchestrator — Executa a transformação completa.          ----####
####---------------------------------------------------------------------------------------####
####----                                                                               ----####
####----  Objetivo:                                                                    ----####
####----    Orquestrar os módulos da camada Silver, transformando a Bronze Parquet em  ----####
####----    eventos estruturados, classificados e enriquecidos.                        ----####
####----                                                                               ----####
####----  Fluxo:                                                                       ----####
####----    1. Ler controle bronze_to_silver do R2 (-> espelho local)                  ----####
####----    2. Ler Bronze Parquet (disco local — Bronze ainda não migrou para o R2)    ----####
####----    3. Reconstruir blocos START → END da Lambda controle                       ----####
####----    4. Extrair eventos HTTP do JSON recebido                                   ----####
####----    5. Classificar decisões operacionais                                       ----####
####----    6. Enriquecer visitantes (identidade, navegador)                           ----####
####----    7. Agrupar o ciclo de vida do ambiente efêmero (site_session_id)           ----####
####----    8. Propagar visitor_id para eventos operacionais (destroy/timeout)         ----####
####----    9. Enriquecer geolocalização/rede (GeoIP, com cache de IPs no R2)          ----####
####----   10. Deduplicar por request_id e gravar Silver final em Parquet no R2        ----####
####----   11. Gravar controle bronze_to_silver atualizado no R2 — só depois do        ----####
####----       passo 10 conifrmar sucesso (mesma ordem segura do cloudwatch_to_bronze) ----####
####----                                                                               ----####
####----  Saída oficial:                                                               ----####
####----    r2://access-intelligence-silver/access_events/                             ----####
####----      year=YYYY/month=MM/day=DD/events.parquet                                 ----####
####----                                                                               ----####
####----  Pendência conhecida (decisão futura, fora do escopo deste ajuste):           ----####
####----    Bronze ainda está em disco local — migração para o R2 será um passo        ----####
####----    separado.                                                                  ----####
####---------------------------------------------------------------------------------------####

from __future__ import annotations

import io
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

from scripts.config import load_r2_config, CONTROL_KEY_BRONZE_TO_SILVER
from scripts.control import (
    load_control_state_from_r2,
    save_control_state_to_r2,
)
from scripts.silver.reconstruct_blocks import load_bronze, reconstruct_blocks
from scripts.silver.extract_events import extract_events
from scripts.silver.classify_events import classify_events
from scripts.silver.enrich_visitors import (
    enrich_visitors,
    propagate_visitor_id_to_operational_events,
    assign_site_session_id,
)
from scripts.silver.enrich_geoip import (
    enrich_geoip,
    build_cached_lookup,
    load_geoip_cache_from_r2,
    save_geoip_cache_to_r2,
    make_maxmind_lookup,
    make_ipinfo_lookup,
    cross_validate_with_ipinfo,
    make_r2_client,
)


####------------------------####
####----  Configuração  ----####
####------------------------####

# Bronze permanece local por enquanto — ver observação no cabeçalho do
# módulo. O controle incremental, porém, já vive no R2 (control/
# bronze_to_silver.json, no bucket de cache) — este caminho local é só um
# espelho temporário usado internamente por reconstruct_blocks.py a cada run.
STATE_FILE = Path("data/control/bronze_to_silver.json")

SILVER_ACCESS_EVENTS_PREFIX = "access_events"
GEOIP_CACHE_KEY = "geoip/geoip_cache.parquet"
IPINFO_CACHE_KEY = "geoip/ipinfo_cache.parquet"


####--------------------------------####
####----  Escrita Parquet (R2)  ----####
####--------------------------------####

def write_silver(records: list[dict], r2_client, bucket: str) -> str:
    """
    Grava a Silver final em Parquet, particionada pela data do primeiro
    evento, direto no bucket R2 (sem escrever em disco local antes).

    Retorna a key (caminho dentro do bucket) onde o arquivo foi gravado.
    """
    df = pd.DataFrame(records)

    if df.empty:
        raise ValueError("Nenhum registro para gravar na Silver.")

    df = df.drop(columns=["block_text"], errors="ignore")
    df = df.drop_duplicates(subset=["request_id"], keep="last")

    df["timestamp_utc_dt"] = pd.to_datetime(
        df["timestamp_utc"], utc=True, errors="coerce",
    )

    first_ts = df["timestamp_utc_dt"].dropna().min()
    partition_dt = (
        datetime.now(timezone.utc) if pd.isna(first_ts) else first_ts.to_pydatetime()
    )

    key = (
        f"{SILVER_ACCESS_EVENTS_PREFIX}/"
        f"year={partition_dt.year:04d}/"
        f"month={partition_dt.month:02d}/"
        f"day={partition_dt.day:02d}/"
        f"events.parquet"
    )

    df = df.drop(columns=["timestamp_utc_dt"], errors="ignore")

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, compression="snappy")
    buffer.seek(0)

    r2_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )

    return key


####--------------------------------####
####----  Controle incremental  ----####
####--------------------------------####

def update_state(
    bronze_files: list[Path],
    silver_key: str,
    r2_client,
    control_bucket: str,
) -> None:
    """
    Atualiza o controle incremental — chamado SÓ DEPOIS que write_silver()
    já confirmou sucesso (ordem segura: nunca marcar como processado algo
    que não foi de fato persistido).

    Grava em dois lugares:
      - STATE_FILE local: espelho usado por reconstruct_blocks.select_bronze_files()
        no próximo run (esse módulo só sabe ler controle do disco).
      - R2 (control/bronze_to_silver.json, bucket de cache): fonte de
        verdade real, sobrevive mesmo se a VM for recriada do zero.
    """
    if not bronze_files:
        raise ValueError("Lista de arquivos Bronze vazia. Controle não atualizado.")

    last_processed_file = str(sorted(bronze_files)[-1])

    state = {
        "bronze_to_silver": {
            "last_processed_file": last_processed_file,
            "last_silver_key": silver_key,
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        }
    }

    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    save_control_state_to_r2(
        state, r2_client, bucket=control_bucket, key=CONTROL_KEY_BRONZE_TO_SILVER,
    )

    print(f"Controle Silver atualizado: {STATE_FILE} "
          f"(espelhado em r2://{control_bucket}/{CONTROL_KEY_BRONZE_TO_SILVER})")
    print(f"Último Bronze processado: {last_processed_file}")


####-----------------------####
####----  Entry point  ----####
####-----------------------####

def main() -> None:
    """
    Executa a transformação completa da camada Silver.
    """
    config = load_r2_config()

    r2_client = make_r2_client(
        config.account_id,
        config.access_key_id,
        config.secret_access_key,
    )

    # ── 1. Sincroniza o controle: R2 (fonte de verdade) -> espelho local ────
    # reconstruct_blocks.select_bronze_files() só sabe ler controle do disco
    # local, então baixamos o estado do R2 para esse caminho antes de ler a
    # Bronze. Se o R2 ainda não tiver controle (primeiro run), não escreve
    # nada local — select_bronze_files() trata a ausência do arquivo como
    # "processar todo o histórico", igual já fazia antes desta mudança.
    control_state = load_control_state_from_r2(
        r2_client, bucket=config.bucket_cache, key=CONTROL_KEY_BRONZE_TO_SILVER,
    )

    if control_state:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        STATE_FILE.write_text(
            json.dumps(control_state, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"Controle sincronizado do R2 para o espelho local: {STATE_FILE}")
    else:
        print("Nenhum controle encontrado no R2. Processando todo o histórico Bronze.")

    # ── 2-3. Bronze -> blocos ────────────────────────────────────────────────
    bronze_df, bronze_files = load_bronze()
    print(f"Eventos Bronze lidos: {len(bronze_df)}")

    if not bronze_files:
        print("Nenhum arquivo Bronze pendente. Encerrando sem gerar Silver.")
        return

    blocks = reconstruct_blocks(bronze_df)
    print(f"Blocos Lambda reconstruídos: {len(blocks)}")

    # ── 4-8. Extração, classificação e enriquecimento de visitantes ─────────
    records = extract_events(blocks)
    print(f"Eventos extraídos: {len(records)}")

    records = classify_events(records)
    print("Classificação operacional concluída.")

    records = enrich_visitors(records)
    print("Enriquecimento de visitantes concluído.")

    records = assign_site_session_id(records)
    print("Agrupamento de site_session_id concluído.")

    records = propagate_visitor_id_to_operational_events(records)
    print("Propagação de visitor_id para eventos operacionais concluída.")

    # ── 9. GeoIP, com cache de IPs já consultados no R2 ──────────────────────
    geoip_cache = load_geoip_cache_from_r2(
        r2_client, bucket=config.bucket_cache, key=GEOIP_CACHE_KEY,
    )
    print(f"Cache GeoIP carregado: {len(geoip_cache)} IPs já conhecidos.")

    maxmind_lookup = make_maxmind_lookup(
        config.geoip_city_db_path, config.geoip_asn_db_path,
    )
    cached_lookup = build_cached_lookup(maxmind_lookup, geoip_cache)

    records = enrich_geoip(records, cached_lookup)
    print("Enriquecimento de geolocalização/rede concluído.")

    save_geoip_cache_to_r2(
        geoip_cache, r2_client, bucket=config.bucket_cache, key=GEOIP_CACHE_KEY,
    )
    print(f"Cache GeoIP salvo no R2: {len(geoip_cache)} IPs ao todo.")

    # ── Cross-validação com IPinfo (segunda fonte, só para IPs novos) ────────
    # MaxMind continua sendo a fonte principal — IPinfo só mede divergência,
    # nunca substitui geo_city/geo_latitude/etc. Mesma camada de cache, em
    # arquivo separado (geo_cache != ipinfo_cache).
    ipinfo_cache = load_geoip_cache_from_r2(
        r2_client, bucket=config.bucket_cache, key=IPINFO_CACHE_KEY,
    )
    print(f"Cache IPinfo carregado: {len(ipinfo_cache)} IPs já conhecidos.")

    cached_ipinfo_lookup = build_cached_lookup(make_ipinfo_lookup(), ipinfo_cache)

    records = cross_validate_with_ipinfo(records, cached_ipinfo_lookup)
    divergentes = sum(1 for r in records if r.get("geo_sources_divergent"))
    print(f"Cross-validação com IPinfo concluída: {divergentes} registro(s) com divergência alta.")

    save_geoip_cache_to_r2(
        ipinfo_cache, r2_client, bucket=config.bucket_cache, key=IPINFO_CACHE_KEY,
    )
    print(f"Cache IPinfo salvo no R2: {len(ipinfo_cache)} IPs ao todo.")

    # ── 10. Gravação física do Parquet final (PRIMEIRO) ──────────────────────
    silver_key = write_silver(records, r2_client, bucket=config.bucket_silver)
    print(f"Arquivo Silver gerado: r2://{config.bucket_silver}/{silver_key}")

    # ── 11. Controle só é atualizado DEPOIS da gravação confirmada ───────────
    update_state(
        bronze_files=bronze_files,
        silver_key=silver_key,
        r2_client=r2_client,
        control_bucket=config.bucket_cache,
    )


if __name__ == "__main__":
    main()