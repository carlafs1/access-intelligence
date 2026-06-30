####---------------------------------------------------------------------------------------####
####----       Silver 04 — Enriquece visitantes para análise comportamental.           ----####
####---------------------------------------------------------------------------------------####
####----                                                                               ----####
####----  Objetivo:                                                                    ----####
####----    Adicionar atributos analíticos que facilitam a identificação de padrões de ----####
####----    acesso, recorrência e classificação de visitantes — alinhado à aba Final   ----####
####----    do dicionário de dados.                                                    ----####
####----                                                                               ----####
####----  Campos derivados (aba Final):                                                ----####
####----    - network_prefix                                                           ----####
####----    - browser_family                                                           ----####
####----    - device_type                                                              ----####
####----    - is_scanner_user_agent                                                    ----####
####----    - is_social_preview                                                        ----####
####----    - visitor_id                                                               ----####
####----    - visitor_type                                                             ----####
####----    - site_session_id                                                          ----####
####----                                                                               ----####
####----  Removido (não consta na aba Final):                                          ----####
####----    - cf_pop — dispensável; cf_ray já embute o POP no sufixo.                  ----####
####----                                                                               ----####
####----  Observações importantes:                                                     ----####
####----    visitor_id é uma assinatura técnica aproximada. Não identifica uma pessoa  ----####
####----    real. Serve apenas para análise de recorrência provável.                   ----####
####----                                                                               ----####
####----    site_session_id deveria nascer de um event_id semente, gravado no          ----####
####----    DynamoDB pela própria Lambda (ver aba Final). Como event_id não sobrevive  ----####
####----    além da deduplicação Bronze→Silver (não é coluna persistida), usamos       ----####
####----    request_id da execução que disparou o apply como semente equivalente —     ----####
####----    ele já é único por execução e cumpre o mesmo papel de identificador do     ----####
####----    início do ciclo.                                                           ----####
####----                                                                               ----####
####---------------------------------------------------------------------------------------####

from __future__ import annotations

import hashlib
import ipaddress


####------------------------------------------####
####----  Prefixo de rede (IPv6 /64 e IPv4 /24)  ----####
####------------------------------------------####

def get_network_prefix(ip: str) -> str:
    """
    Extrai o prefixo de rede do IP, tratando IPv6 e IPv4 separadamente.

    IPv6 — 4 primeiros blocos (/64):
      2804:14c:65a0:430b:39a8:7451:bd4c:796f
      ↓
      2804:14c:65a0:430b

    IPv4 — 3 primeiros octetos (/24):
      163.116.230.152
      ↓
      163.116.230

    Por quê /24 no IPv4: provedores residenciais/móveis costumam variar o
    último octeto do IP entre sessões (IP dinâmico dentro do mesmo bloco),
    igual ao que já acontece com o sufixo do IPv6. Sem esse agrupamento,
    visitor_id perderia a recorrência a cada troca de IP dentro da mesma
    rede — exatamente o problema que o prefixo /64 já resolve para IPv6.

    Retorna string vazia se o IP não puder ser parseado.
    """
    try:
        obj = ipaddress.ip_address(ip)

        if obj.version == 6:
            parts = ip.split(":")
            return ":".join(parts[:4])

        if obj.version == 4:
            parts = ip.split(".")
            return ".".join(parts[:3])

        return ""

    except Exception:
        return ""


# Mantido como alias para compatibilidade — nome anterior tratava só IPv6.
get_ipv6_prefix64 = get_network_prefix


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

    if "edg" in ua:
        return "edge"

    if "opr" in ua or "opera" in ua:
        return "opera"

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

    Aba Final só lista mobile/desktop como valores válidos — "bot" não é
    responsabilidade deste campo (isso é papel de is_scanner_user_agent).
    """
    ua = user_agent.lower()

    if "mobile" in ua or "android" in ua or "iphone" in ua:
        return "mobile"

    return "desktop"


####-------------------------------------------####
####----  Sinalizadores de UA (segurança)  ----####
####-------------------------------------------####

SCANNER_UA_TOKENS = [
    "curl",
    "wget",
    "zgrab",
    "python",
    "python-requests",
    "scrapy",
    "libwww",
    "go-http-client",
    "nmap",
    "masscan",
    "censys",
    "shodan",
    "palo alto",
    "paloalto",
    "nuclei",
    "httpclient",
    "java/",
]

SOCIAL_PREVIEW_UA_TOKENS = [
    "whatsapp",
    "linkedinbot",
    "facebookexternalhit",
    "facebot",
    "twitterbot",
    "telegrambot",
    "slackbot",
    "discordbot",
]


def is_scanner_user_agent(user_agent: str) -> bool:
    """
    Indica se o user-agent corresponde a uma ferramenta de varredura/scanner
    conhecida (curl, zgrab, Palo Alto, python, etc.).

    Sinal de segurança central — maior peso no suspicion_score.
    """
    ua = user_agent.lower()
    return any(token in ua for token in SCANNER_UA_TOKENS)


def is_social_preview(user_agent: str, range_header: str) -> bool:
    """
    Indica se a requisição é uma prévia gerada por app social
    (WhatsApp, LinkedIn, Facebook etc.).

    Regra: UA reconhecido de app social OU presença de header Range
    (usado por essas prévias para buscar parte do conteúdo).
    """
    ua = user_agent.lower()

    if any(token in ua for token in SOCIAL_PREVIEW_UA_TOKENS):
        return True

    if range_header and any(token in ua for token in SOCIAL_PREVIEW_UA_TOKENS + ["bot"]):
        return True

    return False


####----------------------------------------------------####
####----  Tipo de visitante (via motivo_confianca) -----####
####----------------------------------------------------####

# Mapeamento oficial — aba Domínios do dicionário de dados.
MOTIVO_CONFIANCA_TO_VISITOR_TYPE = {
    "allowed_search_bot":              "crawler_buscador",
    "social_preview_bot":              "social_preview",
    "scanner_agent":                   "scanner",
    "suspicious_path":                 "scanner",
    "non_get_method":                  "scanner",
    "no_user_agent":                   "bot_generico",
    "missing_accept_header":           "bot_generico",
    "missing_accept_language_allowed": "humano_provavel",
    "all_checks_passed":               "humano_provavel",
}


def visitor_type(motivo_confianca: str) -> str:
    """
    Classifica o visitante mapeando diretamente motivo_confianca (emitido pela
    Lambda em access_decision.reason, propagado pela Silver 03).

    Valores possíveis: humano_provavel, social_preview, scanner,
    crawler_buscador, bot_generico.

    Quando motivo_confianca está vazio (execuções sem access_decision, como
    EventBridge puro), retorna string vazia — não há base para classificar.
    """
    if not motivo_confianca:
        return ""

    return MOTIVO_CONFIANCA_TO_VISITOR_TYPE.get(motivo_confianca, "bot_generico")


####------------------------------------####
####----  Enriquecimento principal  ----####
####------------------------------------####

def enrich_visitors(records: list[dict]) -> list[dict]:
    """
    Adiciona campos derivados de identidade e navegador/comportamento.

    O visitor_id é calculado com base em network_prefix + user_agent — estável
    o suficiente para análise de recorrência, mas sem pretensão de identificar
    a pessoa real (troca de navegador ou versão de UA quebra a continuidade).
    """
    for record in records:
        ip = record.get("ip", "")
        user_agent = record.get("user_agent", "")
        range_header = record.get("range", "")
        motivo_confianca = record.get("motivo_confianca", "")

        prefix = get_network_prefix(ip)
        browser = browser_family(user_agent)
        device = device_type(user_agent)
        scanner_ua = is_scanner_user_agent(user_agent)
        social_preview = is_social_preview(user_agent, range_header)
        vtype = visitor_type(motivo_confianca)

        # visitor_id = hash(network_prefix + user_agent) — aba Final.
        # network_prefix agora cobre IPv6 (/64) e IPv4 (/24); a proteção
        # "unknown_network" cobre o caso raro de IP ausente/inválido, para
        # não gerar um visitor_id baseado só no user_agent (que agruparia
        # visitantes de redes completamente diferentes sob o mesmo id).
        visitor_source = "|".join(
            [
                prefix or "unknown_network",
                user_agent,
            ]
        )
        visitor_id = hashlib.sha256(
            visitor_source.encode("utf-8")
        ).hexdigest()[:16]

        record["network_prefix"]        = prefix
        record["browser_family"]        = browser
        record["device_type"]           = device
        record["is_scanner_user_agent"] = scanner_ua
        record["is_social_preview"]     = social_preview
        record["visitor_type"]          = vtype
        record["visitor_id"]            = visitor_id

    return records


####----------------------------------------------------------####
####----  Propagação de visitor_id para eventos operacionais  ----####
####----------------------------------------------------------####

def propagate_visitor_id_to_operational_events(records: list[dict]) -> list[dict]:
    """
    Propaga visitor_id para eventos operacionais de destroy/timeout.

    Regra:
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


####------------------------------------------------------------####
####----  Agrupamento do ciclo de vida do ambiente efêmero  ----####
####------------------------------------------------------------####

def assign_site_session_id(records: list[dict]) -> list[dict]:
    """
    Agrupa todos os eventos de um mesmo ciclo de vida do ambiente efêmero
    (do primeiro apply até o destroy) sob um único site_session_id.

    Regra de abertura do ciclo:
      - O ciclo abre no evento com disparou_apply = True (primeiro acesso que
        provisiona o ambiente). A semente do hash é o request_id dessa
        execução (ver observação no cabeçalho do módulo sobre a ausência de
        event_id na Silver).

    Regra de propagação:
      - Todo registro cronologicamente posterior recebe o mesmo
        site_session_id enquanto o ciclo estiver aberto — isso cobre os
        refreshes durante aguardando_criacao (antes do bucket_name existir),
        as checagens do EventBridge e os acessos já no ambiente ativo.

    Regra de fechamento do ciclo:
      - O ciclo fecha no evento com disparou_destroy = True (que também
        recebe o site_session_id antes de fechar).
      - Se um novo disparou_apply aparecer com o ciclo ainda aberto (anomalia
        — não deveria ocorrer dado que active_items é sempre 0 ou 1), o ciclo
        anterior é fechado sem destroy correspondente e um novo é aberto.

    Registros fora de qualquer ciclo (antes do primeiro apply, ou eventos
    isolados como scanners/previews que nunca chegaram a abrir um ambiente)
    recebem um site_session_id próprio, gerado a partir do request_id daquele
    registro — único por execução. Na prática, qualquer site_session_id com
    contagem == 1 no agrupamento é, por construção, um evento que nunca
    pertenceu a um ciclo real: sinal útil para auditar tanto a geração do
    hash quanto a coerência da classificação (ex.: esperar visitor_type
    scanner/social_preview nesses casos isolados).
    """

    def timestamp_key(record: dict) -> str:
        return record.get("timestamp_utc") or ""

    ordered = sorted(records, key=timestamp_key)

    current_session_id: str | None = None

    for record in ordered:
        disparou_apply = bool(record.get("disparou_apply"))
        disparou_destroy = bool(record.get("disparou_destroy"))

        if disparou_apply:
            # Se current_session_id já não for None aqui, é a anomalia
            # documentada acima: novo apply com ciclo ainda aberto. O ciclo
            # anterior simplesmente é substituído (fica sem destroy
            # correspondente), e um novo ciclo começa a partir desta linha.
            seed = record.get("request_id", "")
            current_session_id = hashlib.sha256(
                seed.encode("utf-8")
            ).hexdigest()[:16]

        if current_session_id is not None:
            record["site_session_id"] = current_session_id
        else:
            # Fora de qualquer ciclo: hash próprio a partir do request_id,
            # único por execução — não fica em branco.
            orphan_seed = record.get("request_id", "")
            record["site_session_id"] = hashlib.sha256(
                orphan_seed.encode("utf-8")
            ).hexdigest()[:16]

        if disparou_destroy:
            current_session_id = None

    return records