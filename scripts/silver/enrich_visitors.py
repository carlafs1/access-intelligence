####---------------------------------------------------------------------------------------####
####----       Silver 04 — Enriquece visitantes para análise comportamental.           ----####
####---------------------------------------------------------------------------------------####
####----                                                                               ----####
####----  Objetivo:                                                                    ----####
####----    Adicionar atributos analíticos que facilitam a identificação de padrões de ----####
####----    acesso, recorrência e classificação de visitantes.                         ----####
####----                                                                               ----####
####----  Campos derivados:                                                            ----####
####----    - network_prefix                                                           ----####
####----    - browser_family                                                           ----####
####----    - device_type                                                              ----####
####----    - visitor_type                                                             ----####
####----    - cf_pop                                                                   ----####
####----    - visitor_id                                                               ----####
####----                                                                               ----####
####----  Observação importante:                                                       ----####
####----    visitor_id é uma assinatura técnica aproximada. Não identifica uma pessoa  ----####
####----    real. Serve apenas para análise de recorrência provável.                   ----####
####----                                                                               ----####
####---------------------------------------------------------------------------------------####

from __future__ import annotations

import hashlib
import ipaddress


####------------------------------------####
####----  Prefixo de rede IPv6 /64  ----####
####------------------------------------####

def get_ipv6_prefix64(ip: str) -> str:
    """
    Extrai os 4 primeiros blocos de um IPv6.

    Exemplo:
      2804:14c:65a0:430b:39a8:7451:bd4c:796f
      ↓
      2804:14c:65a0:430b

    Para IPv4 retorna string vazia.
    """
    try:
        obj = ipaddress.ip_address(ip)

        if obj.version != 6:
            return ""

        parts = ip.split(":")
        return ":".join(parts[:4])

    except Exception:
        return ""


####--------------------------------####
####----  Família de navegador  ----####
####--------------------------------####

def browser_family(user_agent: str) -> str:
    """
    Classifica a família principal do navegador/bot a partir do user-agent.
    """
    ua = user_agent.lower()

    if "linkedinbot" in ua:
        return "linkedinbot"

    if "whatsapp" in ua:
        return "whatsapp"

    if "facebookexternalhit" in ua or "facebot" in ua:
        return "facebookbot"

    if "twitterbot" in ua:
        return "twitterbot"

    if "googlebot" in ua:
        return "googlebot"

    if "chrome" in ua:
        return "chrome"

    if "safari" in ua:
        return "safari"

    if "firefox" in ua:
        return "firefox"

    if "curl" in ua:
        return "curl"

    if "python" in ua:
        return "python"

    return "unknown"


####-------------------------------####
####----  Tipo de dispositivo  ----####
####-------------------------------####

def device_type(user_agent: str) -> str:
    """
    Classifica o tipo provável de dispositivo.
    """
    ua = user_agent.lower()

    if "bot" in ua or "crawler" in ua or "spider" in ua:
        return "bot"

    if "mobile" in ua or "android" in ua or "iphone" in ua:
        return "mobile"

    return "desktop"


####-----------------------------####
####----  Tipo de visitante  ----####
####-----------------------------####

def visitor_type(user_agent: str, status_confianca: str) -> str:
    """
    Classifica o visitante em uma categoria analítica simples.
    """
    ua = user_agent.lower()

    bot_tokens = [
        "bot",
        "crawler",
        "spider",
        "whatsapp",
        "facebookexternalhit",
        "linkedinbot",
        "twitterbot",
    ]

    if any(token in ua for token in bot_tokens):
        return "bot"

    if status_confianca == "rejeitado":
        return "suspeito"

    return "humano_provavel"


####------------------------------------####
####----  Enriquecimento principal  ----####
####------------------------------------####

def enrich_visitors(records: list[dict]) -> list[dict]:
    """
    Adiciona campos derivados para análise de visitantes.

    O visitor_id é calculado com base em atributos técnicos estáveis o suficiente
    para análise de recorrência, mas sem pretensão de identificar a pessoa real.
    """
    for record in records:
        ip = record.get("ip", "")
        user_agent = record.get("user_agent", "")
        accept_language = record.get("accept_language", "")
        pais = record.get("pais_cf", "")
        cf_ray = record.get("cf_ray", "")

        prefix64 = get_ipv6_prefix64(ip)
        browser = browser_family(user_agent)
        device = device_type(user_agent)
        vtype = visitor_type(
            user_agent=user_agent,
            status_confianca=record.get("status_confianca", ""),
        )

        cf_pop = ""

        if "-" in cf_ray:
            cf_pop = cf_ray.split("-")[-1]

        visitor_source = "|".join(
            [
                prefix64 or ip,
                browser,
                device,
                accept_language,
                pais,
                cf_pop,
            ]
        )

        visitor_id = hashlib.sha256(
            visitor_source.encode("utf-8")
        ).hexdigest()[:16]

        record["network_prefix"] = prefix64
        record["browser_family"] = browser
        record["device_type"] = device
        record["visitor_type"] = vtype
        record["cf_pop"] = cf_pop
        record["visitor_id"] = visitor_id

    return records


####----------------------------------------------------------####
####----  Propagação de visitor_id para eventos operacionais  ----####
####----------------------------------------------------------####

def propagate_visitor_id_to_operational_events(records: list[dict]) -> list[dict]:
    """
    Propaga visitor_id para eventos operacionais de destroy/timeout.

    Regra correta:
      - primeiro considera o arquivo inteiro já enriquecido;
      - para cada evento de destroy/timeout com bucket real:
          1. lê o bucket_name do destroy;
          2. busca no histórico completo todos os registros do mesmo bucket_name;
          3. ignora o próprio evento de destroy/timeout;
          4. seleciona o registro mais recente do bucket com visitor_id válido;
          5. copia esse visitor_id para o destroy.

    Importante:
      O visitor_id do destroy só pode ser resolvido depois que 100% dos registros
      do arquivo já foram tratados.
    """

    def is_real_bucket(bucket_name: str) -> bool:
        return bool(bucket_name) and bucket_name != "TEMPORARIO"

    def is_destroy_event(record: dict) -> bool:
        bucket_name = record.get("bucket_name", "")

        return (
            is_real_bucket(bucket_name)
            and (
                record.get("disparou_destroy") is True
                or record.get("estado_resposta") in [
                    "eventbridge_timeout",
                    "eventbridge_destroy",
                ]
            )
        )

    def timestamp_value(record: dict) -> str:
        return record.get("timestamp_utc") or ""

    destroy_records = [
        record for record in records
        if is_destroy_event(record)
    ]

    for destroy_record in destroy_records:
        bucket_name = destroy_record.get("bucket_name", "")

        candidatos = [
            record for record in records
            if record.get("bucket_name", "") == bucket_name
            and not is_destroy_event(record)
            and record.get("visitor_id")
        ]

        if not candidatos:
            continue

        ultimo_registro = max(
            candidatos,
            key=timestamp_value,
        )

        destroy_record["visitor_id"] = ultimo_registro["visitor_id"]

    return records