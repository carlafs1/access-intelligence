####---------------------------------------------------------------------------------------####
####----        Silver 02 — Extrai eventos HTTP dos blocos reconstruídos da Lambda.    ----####
####---------------------------------------------------------------------------------------####
####----                                                                               ----####
####----  Objetivo:                                                                    ----####
####----    Receber blocos START → END reconstruídos e extrair o JSON registrado após  ----####
####----    "Evento recebido:", transformando o evento bruto da API Gateway em colunas ----####
####----    estruturadas para a camada Silver.                                         ----####
####----                                                                               ----####
####----  Entrada em memória:                                                          ----####
####----    Lista de blocos gerada por reconstruct_blocks.py                           ----####
####----                                                                               ----####
####----  Saída em memória:                                                            ----####
####----    Lista de registros com campos estruturados de acesso:                      ----####
####----      - request_id, timestamp_utc                                              ----####
####----      - ip, pais_cf, source_ip_cloudflare                                      ----####
####----      - user_agent, referer, accept_language                                   ----####
####----      - accept, accept_encoding, range                                         ----####
####----      - sec_ch_ua, sec_ch_ua_mobile, sec_ch_ua_platform                        ----####
####----      - sec_fetch_dest, sec_fetch_mode, sec_fetch_site, sec_fetch_user         ----####
####----      - raw_path, raw_query_string, method, host                               ----####
####----      - cf_ray, x_forwarded_for                                                ----####
####----      - block_text preservado apenas para classificação posterior              ----####
####----                                                                               ----####
####----  Observação:                                                                  ----####
####----    block_text é campo técnico temporário. Ele precisa existir em memória para ----####
####----    classify_events.py detectar EventBridge, bucket, apply, destroy e status   ----####
####----    de confiança, mas deve ser removido antes da gravação da Silver final.     ----####
####----                                                                               ----####
####----  Campos NÃO extraídos aqui (responsabilidade de outros scripts):              ----####
####----    - browser_family, device_type, is_scanner_user_agent,                      ----####
####----      is_social_preview → classify_events.py                                   ----####
####----    - network_prefix, visitor_id, site_session_id → enrich_visitors.py         ----####
####----    - geo_* → enrich_geoip.py                                                  ----####
####----    - suspicion_score, human_probability, visitor_type → classify_events.py    ----####
####----                                                                               ----####
####---------------------------------------------------------------------------------------####

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any


####-------------------####
####----  Regexes  ----####
####-------------------####

RE_EVENTO_RECEBIDO = re.compile(r"Evento recebido:")
RE_FIRST_JSON      = re.compile(r"\{")


####-------------------------------####
####----  Funções utilitárias  ----####
####-------------------------------####

def safe_get(obj: Any, *keys: str, default: Any = None) -> Any:
    """
    Navega de forma segura em dicionários aninhados.

    Evita vários blocos try/except ou checagens repetidas quando precisamos
    acessar estruturas como:

      event["requestContext"]["http"]["method"]
    """
    current = obj

    for key in keys:
        if not isinstance(current, dict):
            return default

        current = current.get(key)

        if current is None:
            return default

    return current


####-----------------------------------------------####
####----  Extração do JSON do evento recebido  ----####
####-----------------------------------------------####

def extract_event_json(block_text: str) -> tuple[dict[str, Any] | None, str | None]:
    """
    Extrai o JSON registrado pela Lambda após o marcador "Evento recebido:".

    A Lambda escreve o JSON em uma linha separada, mas o bloco reconstruído
    contém toda a execução concatenada. Por isso usamos uma estratégia robusta:

      1. localizar "Evento recebido:"
      2. encontrar a primeira chave "{"
      3. contar abertura/fechamento de chaves
      4. respeitar strings e escapes
      5. aplicar json.loads no trecho completo

    Retorna:
      - dict do evento, se conseguir parsear
      - mensagem de erro, se falhar
    """
    marker = RE_EVENTO_RECEBIDO.search(block_text)

    if not marker:
        return None, "Evento recebido não encontrado"

    tail       = block_text[marker.end():]
    first_json = RE_FIRST_JSON.search(tail)

    if not first_json:
        return None, "JSON não encontrado após Evento recebido"

    raw       = tail[first_json.start():]

    depth     = 0
    end_idx   = 0
    in_string = False
    escape    = False

    for i, ch in enumerate(raw):
        if escape:
            escape = False
            continue

        if ch == "\\":
            escape = True
            continue

        if ch == '"':
            in_string = not in_string
            continue

        if in_string:
            continue

        if ch == "{":
            depth += 1

        elif ch == "}":
            depth -= 1

            if depth == 0:
                end_idx = i + 1
                break

    if end_idx == 0:
        return None, "JSON incompleto"

    json_str = raw[:end_idx]

    try:
        return json.loads(json_str), None
    except json.JSONDecodeError as exc:
        return None, f"json_decode_error: {exc}"


####-----------------------------------------####
####----  Construção dos eventos Silver  ----####
####-----------------------------------------####

def extract_events(blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Converte blocos de execução Lambda em registros estruturados de acesso.

    Esta etapa não classifica se o acesso é bot/humano, confiável ou suspeito.
    Ela apenas extrai os campos do evento recebido e preserva o block_text para
    a próxima etapa da Silver.

    Importante:
      - block_text deve seguir para classify_events.py.
      - block_text não deve ser gravado na Silver final.
    """
    records: list[dict[str, Any]] = []

    for block in blocks:
        block_text = block.get("block_text", "")

        event, error = extract_event_json(block_text)
        event = event or {}

        headers         = safe_get(event, "headers",      default={}) or {}
        request_context = safe_get(event, "requestContext", default={}) or {}
        http            = safe_get(request_context, "http", default={}) or {}

        # Preferência pelo timeEpoch do requestContext (mais preciso que o
        # timestamp do CloudWatch, que é o momento de ingestão do log).
        timestamp_utc = block.get("start_ts")
        epoch_ms      = safe_get(request_context, "timeEpoch")

        if epoch_ms:
            try:
                timestamp_utc = datetime.fromtimestamp(
                    epoch_ms / 1000,
                    tz=timezone.utc,
                )
            except Exception:
                pass

        # Protege datetime ingênuo (tzinfo is None): adiciona UTC explicitamente
        # para evitar que isoformat() produza strings sem offset, que quebram
        # comparações temporais na Silver.
        if isinstance(timestamp_utc, datetime) and timestamp_utc.tzinfo is None:
            timestamp_utc = timestamp_utc.replace(tzinfo=timezone.utc)

        records.append(
            {
                # ── Requisição HTTP ──────────────────────────────────────────
                "request_id":      block["request_id"],
                # Formato Z (ex: 2026-06-18T04:17:49.258Z) em vez de +00:00:
                # mais compacto, amplamente suportado e consistente com o
                # formato que a AWS usa nos próprios eventos.
                "timestamp_utc": (
                    timestamp_utc.isoformat().replace("+00:00", "Z")
                    if timestamp_utc else None
                ),
                "method":          safe_get(http, "method",  default="") or "",
                "raw_path":        event.get("rawPath",         ""),
                "raw_query_string":event.get("rawQueryString",  ""),
                "host":            headers.get("host",          ""),

                # ── Rede e Geolocalização ────────────────────────────────────
                "ip":                   headers.get("cf-connecting-ip", ""),
                "pais_cf":              headers.get("cf-ipcountry",     ""),
                "cf_ray":               headers.get("cf-ray",           ""),
                "x_forwarded_for":      headers.get("x-forwarded-for",  ""),
                "source_ip_cloudflare": safe_get(http, "sourceIp", default="") or "",

                # ── Navegador e Comportamento ────────────────────────────────
                "user_agent":        headers.get("user-agent",        ""),
                "referer":           headers.get("referer",
                                     headers.get("referrer",          "")),
                "accept_language":   headers.get("accept-language",   ""),
                "accept":            headers.get("accept",            ""),
                "accept_encoding":   headers.get("accept-encoding",   ""),
                "range":             headers.get("range",             ""),

                # Sec-CH-UA (Chromium Client Hints — cobertura parcial)
                "sec_ch_ua":         headers.get("sec-ch-ua",         ""),
                "sec_ch_ua_mobile":  headers.get("sec-ch-ua-mobile",  ""),
                "sec_ch_ua_platform":headers.get("sec-ch-ua-platform",""),

                # Sec-Fetch (navegadores modernos)
                "sec_fetch_dest":    headers.get("sec-fetch-dest",    ""),
                "sec_fetch_mode":    headers.get("sec-fetch-mode",    ""),
                "sec_fetch_site":    headers.get("sec-fetch-site",    ""),
                "sec_fetch_user":    headers.get("sec-fetch-user",    ""),

                # ── Operacional / Controle de qualidade ──────────────────────
                "log_group":    block.get("log_group",  ""),
                "log_stream":   block.get("log_stream", ""),
                # Fallback block_closed → closed: reconstruct_blocks.py pode
                # nomear a chave de forma diferente dependendo da versão.
                # Tenta "block_closed" primeiro; cai em "closed" se não achar.
                "block_closed": bool(
                    block.get("block_closed", block.get("closed", False))
                ),
                "is_http_event": bool(headers) and bool(http),

                # parse_status segue os valores do dicionário: "ok" | "erro"
                "parse_status": "ok"   if error is None else "erro",
                "parse_error":  ""     if error is None else error,

                # Campo técnico temporário.
                # Necessário para classify_events.py detectar decisões operacionais.
                # Deve ser removido antes da gravação da Silver final.
                "block_text": block_text,
            }
        )

    return records