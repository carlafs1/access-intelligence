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
####----      - request_id                                                             ----####
####----      - timestamp_utc / timestamp_bsb                                          ----####
####----      - ip                                                                     ----####
####----      - país Cloudflare                                                        ----####
####----      - user_agent                                                             ----####
####----      - path, método, host                                                     ----####
####----      - headers relevantes                                                     ----####
####----      - block_text preservado apenas para classificação posterior              ----####
####----                                                                               ----####
####----  Observação:                                                                  ----####
####----    block_text é campo técnico temporário. Ele precisa existir em memória para ----####
####----    classify_events.py detectar EventBridge, bucket, apply, destroy e status   ----####
####----    de confiança, mas deve ser removido antes da gravação da Silver final.      ----####
####----                                                                               ----####
####---------------------------------------------------------------------------------------####

from __future__ import annotations

import json
import re
from datetime import datetime, timezone, timedelta
from typing import Any


####------------------------####
####----  Configuração  ----####
####------------------------####

TZ_BSB = timezone(timedelta(hours=-3))


####-------------------####
####----  Regexes  ----####
####-------------------####

RE_EVENTO_RECEBIDO = re.compile(r"Evento recebido:")
RE_FIRST_JSON = re.compile(r"\{")


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

    tail = block_text[marker.end():]
    first_json = RE_FIRST_JSON.search(tail)

    if not first_json:
        return None, "JSON não encontrado após Evento recebido"

    raw = tail[first_json.start():]

    depth = 0
    end_idx = 0
    in_string = False
    escape = False

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
        return None, f"json.loads falhou: {exc}"


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

        headers = safe_get(event, "headers", default={}) or {}
        request_context = safe_get(event, "requestContext", default={}) or {}
        http = safe_get(request_context, "http", default={}) or {}

        timestamp_utc = block.get("start_ts")

        epoch_ms = safe_get(request_context, "timeEpoch")

        if epoch_ms:
            try:
                timestamp_utc = datetime.fromtimestamp(
                    epoch_ms / 1000,
                    tz=timezone.utc,
                )
            except Exception:
                pass

        timestamp_bsb = timestamp_utc.astimezone(TZ_BSB) if timestamp_utc else None

        records.append(
            {
                "request_id": block["request_id"],
                "api_gateway_request_id": safe_get(
                    request_context,
                    "requestId",
                    default="",
                ) or "",

                "timestamp_utc": timestamp_utc.isoformat() if timestamp_utc else None,

                "ip": headers.get("cf-connecting-ip", ""),
                "pais_cf": headers.get("cf-ipcountry", ""),
                "method": safe_get(http, "method", default="") or "",
                "route_key": event.get("routeKey", ""),
                "raw_path": event.get("rawPath", ""),
                "host": headers.get("host", ""),
                "user_agent": headers.get("user-agent", ""),
                "referer": headers.get("referer", headers.get("referrer", "")),
                "accept_language": headers.get("accept-language", ""),
                "cf_ray": headers.get("cf-ray", ""),
                "x_forwarded_for": headers.get("x-forwarded-for", ""),
                "source_ip_cloudflare": safe_get(http, "sourceIp", default="") or "",

                "log_group": block.get("log_group", ""),
                "log_stream": block.get("log_stream", ""),
                "block_closed": bool(block.get("closed", False)),

                # Campo técnico temporário.
                # Necessário para classify_events.py detectar decisões operacionais.
                # Deve ser removido antes da gravação da Silver final.
                "block_text": block_text,

                "is_http_event": bool(headers) and bool(http),

                "parse_status": "success" if error is None else "error",
                "parse_error": error or "",
            }
        )

    return records