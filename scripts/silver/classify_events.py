####---------------------------------------------------------------------------------------####
####----       Silver 03 — Classifica decisões operacionais da Lambda controle.        ----####
####---------------------------------------------------------------------------------------####
####----                                                                               ----####
####----  Objetivo:                                                                    ----####
####----    Analisar o block_text de cada execução e extrair os campos operacionais    ----####
####----    emitidos pela Lambda como JSONs estruturados, transformando-os em colunas  ----####
####----    booleanas/categóricas da camada Silver.                                    ----####
####----                                                                               ----####
####----  Estratégia de extração:                                                      ----####
####----    A Lambda emite eventos operacionais como JSONs em linhas separadas.        ----####
####----    Cada linha pode conter um objeto com campo "event" identificando o tipo.   ----####
####----    Este script escaneia cada linha do block_text, tenta parsear JSON e        ----####
####----    indexa os eventos por tipo para extração posterior.                        ----####
####----                                                                               ----####
####----  Eventos operacionais reconhecidos:                                           ----####
####----    lambda_execution         → origem_eventbridge, temp_item, active_items,    ----####
####----                               bucket                                          ----####
####----    access_decision          → trusted, reason, user_agent                     ----####
####----    deploy_triggered         → disparou_apply                                  ----####
####----    destroy_triggered        → disparou_destroy, bucket, last_accessed_at,     ----####
####----                               expiration_time                                 ----####
####----    active_site_untrusted_access → active_site_untrusted_access                ----####
####----                                                                               ----####
####----  Campos produzidos (dicionário de dados — aba Final):                         ----####
####----    event_type               — Silver (discriminador estrutural)               ----####
####----    origem_eventbridge       — Silver                                          ----####
####----    status_confianca         — Silver e Gold  ("trusted" | "untrusted" | "")   ----####
####----    motivo_confianca         — Silver e Gold                                   ----####
####----    decision_user_agent      — Silver e Gold                                   ----####
####----    disparou_apply           — Silver e Gold                                   ----####
####----    disparou_destroy         — Silver e Gold                                   ----####
####----    bucket_name              — Silver e Gold                                   ----####
####----    last_accessed_at         — Silver                                          ----####
####----    expiration_time          — Silver                                          ----####
####----    active_site_untrusted_access — Silver e Gold                               ----####
####----    serviu_site              — Silver                                          ----####
####----    aguardando_criacao       — Silver                                          ----####
####----    renovou_timeout          — Silver                                          ----####
####----    deploy_avoided           — Silver e Gold                                   ----####
####----    estado_resposta          — Silver                                          ----####
####----    manter_para_analise      — Silver                                          ----####
####----    motivo_analise           — Silver                                          ----####
####----                                                                               ----####
####----  Campos insumo (usados internamente, não viram colunas):                      ----####
####----    temp_item, active_items  — derivam estado_resposta e aguardando_criacao    ----####
####----                                                                               ----####
####---------------------------------------------------------------------------------------####

from __future__ import annotations

import json
import re


####-------------------------------------------------####
####----  Extração de eventos operacionais JSON  ----####
####-------------------------------------------------####

def extract_operational_events(block_text: str) -> dict[str, dict]:
    """
    Escaneia cada linha do block_text e extrai eventos operacionais JSON.

    A Lambda emite uma linha por evento, podendo ter timestamp e outros
    prefixos antes do JSON. O parser encontra o primeiro '{' da linha
    e tenta parsear o JSON a partir daí.

    Retorna dict mapeando event_name → dados do evento.
    Se o mesmo tipo de evento aparecer mais de uma vez (improvável),
    o último prevalece.
    """
    events: dict[str, dict] = {}

    for line in block_text.splitlines():
        line = line.strip()
        brace_idx = line.find("{")

        if brace_idx == -1:
            continue

        json_str = line[brace_idx:]

        try:
            obj = json.loads(json_str)

            if isinstance(obj, dict) and "event" in obj:
                events[obj["event"]] = obj

        except (json.JSONDecodeError, ValueError):
            continue

    return events


####-------------------------------------####
####----  Classificação operacional  ----####
####-------------------------------------####

def classify_events(records: list[dict]) -> list[dict]:
    """
    Adiciona campos operacionais aos registros vindos de extract_events.py.

    Lê os JSONs estruturados emitidos pela Lambda dentro do block_text
    e deriva os campos do dicionário de dados (aba Final).

    Campos insumo (usados internamente, não propagados como colunas):
      temp_item    — indica registro TEMPORARIO no DynamoDB
      active_items — quantidade de ambientes ativos no momento da execução

    Esses dois campos, junto com bucket_name, alimentam estado_resposta
    e aguardando_criacao, mas não aparecem como colunas finais.
    """
    classified = []

    for record in records:
        result = dict(record)
        text = str(result.get("block_text") or "")

        # ── Extração dos eventos operacionais ────────────────────────────────
        op = extract_operational_events(text)

        # ── lambda_execution ─────────────────────────────────────────────────
        lambda_exec  = op.get("lambda_execution", {})
        origem_eventbridge = bool(lambda_exec.get("origem_eventbridge", False))
        temp_item    = bool(lambda_exec.get("temp_item", False))   # insumo interno
        active_items = int(lambda_exec.get("active_items", 0))     # insumo interno
        bucket_exec  = lambda_exec.get("bucket") or ""

        # ── access_decision ──────────────────────────────────────────────────
        # status_confianca: "trusted" | "untrusted" | ""
        # Vazio quando não há decisão de acesso (ex.: execuções EventBridge).
        access_dec = op.get("access_decision", {})

        if access_dec:
            status_confianca    = "trusted" if access_dec.get("trusted") else "untrusted"
            motivo_confianca    = access_dec.get("reason", "")
            decision_user_agent = access_dec.get("user_agent", "")
        else:
            status_confianca    = ""
            motivo_confianca    = ""
            decision_user_agent = ""

        # ── deploy_triggered ─────────────────────────────────────────────────
        disparou_apply = "deploy_triggered" in op

        # ── destroy_triggered ────────────────────────────────────────────────
        destroy_ev      = op.get("destroy_triggered", {})
        disparou_destroy = bool(destroy_ev)
        last_accessed_at = destroy_ev.get("last_accessed_at", "")
        expiration_time  = destroy_ev.get("expiration_time",  "")
        bucket_destroy   = destroy_ev.get("bucket") or ""

        # ── bucket_name ──────────────────────────────────────────────────────
        # Prioridade: bucket do destroy (definitivo) > bucket ativo da execução
        # Nunca usa "TEMPORARIO" como valor — esse é o nome do registro
        # no DynamoDB antes do bucket existir, não o nome do bucket real.
        bucket_name = bucket_destroy or bucket_exec or ""

        # ── active_site_untrusted_access ─────────────────────────────────────
        active_site_untrusted_access = "active_site_untrusted_access" in op

        # ── event_type ───────────────────────────────────────────────────────
        # Discriminador estrutural: evento mais específico da execução.
        # Prioridade: destroy > deploy > acesso rejeitado > decisão de acesso
        #             > execução sem decisão (EB puro)
        if disparou_destroy:
            event_type = "destroy_triggered"
        elif disparou_apply:
            event_type = "deploy_triggered"
        elif active_site_untrusted_access:
            event_type = "active_site_untrusted_access"
        elif access_dec:
            event_type = "access_decision"
        elif lambda_exec:
            event_type = "lambda_execution"
        else:
            event_type = ""

        # ── serviu_site ──────────────────────────────────────────────────────
        # Detectado por mensagem de texto — a Lambda não emite JSON específico
        # para esse evento; usa o log textual diretamente.
        serviu_site = "servindo site via proxy s3" in text.lower()

        # ── aguardando_criacao ───────────────────────────────────────────────
        # Acesso HTTP durante criação do ambiente:
        #   temp_item=True  → TEMPORARIO existe (apply já foi disparado antes)
        #   is_http_event   → é requisição do usuário, não EventBridge
        #   not disparou_apply → esse acesso NÃO disparou o apply (só aguarda)
        is_http_event      = bool(result.get("is_http_event", False))
        aguardando_criacao = temp_item and is_http_event and not disparou_apply

        # ── renovou_timeout ──────────────────────────────────────────────────
        # Acesso confiável com site já ativo renova o prazo de expiração.
        # Derivável de estado_resposta + status_confianca; mantido por
        # clareza operacional.
        renovou_timeout = (
            status_confianca == "trusted"
            and serviu_site
            and not disparou_apply
            and not disparou_destroy
            and not origem_eventbridge
        )

        # ── deploy_avoided ───────────────────────────────────────────────────
        # Acesso não confiável bloqueado quando não havia ambiente ativo.
        # Mostra o ROI da camada de segurança: deploy que foi evitado.
        deploy_avoided = (
            not origem_eventbridge
            and status_confianca == "untrusted"
            and not disparou_apply
            and active_items == 0
            and not temp_item
        )

        # ── estado_resposta ──────────────────────────────────────────────────
        # Resume o resultado operacional da execução:
        #
        #   eventbridge_destroy  — EB expirou o timeout e destruiu o ambiente
        #   eventbridge_timeout  — EB avaliou timeout sem destruir
        #   criou_ambiente       — primeiro acesso válido disparou o apply
        #   aguardando_criacao   — refresh enquanto o apply ainda está rodando
        #   site_servido         — site entregue a partir do bucket efêmero
        #   indefinido           — execução não se encaixou nas regras acima
        if origem_eventbridge and disparou_destroy:
            estado_resposta = "eventbridge_destroy"
        elif origem_eventbridge:
            estado_resposta = "eventbridge_timeout"
        elif disparou_apply:
            estado_resposta = "criou_ambiente"
        elif aguardando_criacao:
            estado_resposta = "aguardando_criacao"
        elif active_site_untrusted_access:
            # Site ativo servido a acesso não confiável — timeout NÃO renovado.
            # Distinto de site_servido: active_site_untrusted_access foi criado
            # para capturar este cenário; colapsá-lo em site_servido ocultaria
            # a informação no estado_resposta.
            estado_resposta = "site_servido_nao_confiavel"
        elif serviu_site:
            estado_resposta = "site_servido"
        else:
            estado_resposta = "indefinido"

        # ── manter_para_analise / motivo_analise ─────────────────────────────
        # A Silver não descarta registros. O campo apenas sinaliza se o evento
        # deve entrar nas visões analíticas principais da Gold.
        # Atualmente só refresh de provisionamento é marcado como False.
        manter_para_analise = estado_resposta != "aguardando_criacao"

        MOTIVO_MAP = {
            "aguardando_criacao":              "refresh_aguardando_apply",
            "criou_ambiente":                  "primeiro_acesso_disparou_apply",
            "site_servido":                    "acesso_site_servido",
            "site_servido_nao_confiavel": "bot_ou_preview_em_ambiente_ativo",
            "eventbridge_destroy":             "destroy_automatico",
            "eventbridge_timeout":             "validacao_timeout_eventbridge",
        }
        motivo_analise = MOTIVO_MAP.get(estado_resposta, "classificacao_indefinida")

        # ── Atualiza o registro ──────────────────────────────────────────────
        result.update({
            "event_type":                    event_type,
            "origem_eventbridge":            origem_eventbridge,
            "status_confianca":              status_confianca,
            "motivo_confianca":              motivo_confianca,
            "decision_user_agent":           decision_user_agent,
            "disparou_apply":                disparou_apply,
            "disparou_destroy":              disparou_destroy,
            "bucket_name":                   bucket_name,
            "last_accessed_at":              last_accessed_at,
            "expiration_time":               expiration_time,
            "active_site_untrusted_access":  active_site_untrusted_access,
            "serviu_site":                   serviu_site,
            "aguardando_criacao":            aguardando_criacao,
            "renovou_timeout":               renovou_timeout,
            "deploy_avoided":                deploy_avoided,
            "estado_resposta":               estado_resposta,
            "manter_para_analise":           manter_para_analise,
            "motivo_analise":                motivo_analise,
        })

        # Remove campos do script anterior que não constam no dicionário
        for campo_obsoleto in ("acordou_ambiente", "ambiente_ativo", "carregou_site"):
            result.pop(campo_obsoleto, None)

        classified.append(result)

    return classified