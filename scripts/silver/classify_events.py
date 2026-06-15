####---------------------------------------------------------------------------------------####
####----       Silver 03 — Classifica decisões operacionais da Lambda controle.        ----####
####---------------------------------------------------------------------------------------####
####----                                                                               ----####
####----  Objetivo:                                                                    ----####
####----    Analisar o texto completo de cada execução da Lambda controle e transfor-  ----####
####----    mar mensagens operacionais em colunas booleanas/categóricas.               ----####
####----                                                                               ----####
####----  Exemplos de decisões detectadas:                                             ----####
####----    - Origem EventBridge                                                       ----####
####----    - Acesso confiável ou rejeitado                                            ----####
####----    - Workflow apply.yml disparado                                             ----####
####----    - Workflow destroy.yml disparado                                           ----####
####----    - Ambiente ativo encontrado                                                ----####
####----    - Site servido via proxy S3                                                ----####
####----    - Ambiente temporário em criação                                           ----####
####----    - Refresh enquanto ambiente está em criação                                ----####
####----                                                                               ----####
####---------------------------------------------------------------------------------------####

from __future__ import annotations

import re


####-------------------------------####
####----  Funções utilitárias  ----####
####-------------------------------####

def _safe_text(value) -> str:
    if value is None:
        return ""
    return str(value)


def normalize_motivo(texto: str) -> str:
    """
    Normaliza mensagens textuais da Lambda para valores técnicos padronizados.
    """
    t = _safe_text(texto).lower().strip()

    if "headers básicos de browser ausentes" in t:
        return "headers_basicos_browser_ausentes"

    if "passou todas as verificações" in t:
        return "passou_todas_verificacoes"

    if "bot permitido" in t:
        return "bot_permitido"

    if "acesso não confiável" in t:
        return "acesso_nao_confiavel"

    return (
        t.replace(":", "")
         .replace(".", "")
         .replace("/", "_")
         .replace("-", "_")
         .replace(" ", "_")
         .replace("__", "_")
         .strip("_")
    )


####-------------------------------------####
####----  Classificação operacional  ----####
####-------------------------------------####

def classify_events(records: list[dict]) -> list[dict]:
    """
    Adiciona campos operacionais aos registros extraídos.

    Esta etapa interpreta as mensagens geradas pela própria Lambda controle.
    """
    classified = []

    for record in records:
        result = dict(record)
        text = _safe_text(result.get("block_text"))

        origem_eventbridge = bool(
            re.search(r"Origem EventBridge:\s*True", text, re.I)
        )

        ambiente_match = re.search(
            r"Ambientes ativos encontrados:\s*(\d+)",
            text,
            re.I,
        )

        ####-----------------------------------####
        ####----  Extração do bucket ativo ----####
        ####-----------------------------------####
        ####
        #### Regras:
        ####
        #### 1. Quando o ambiente já existe:
        ####      S3 ativo encontrado: website-s3-iac-cv-efemero-xxxx
        ####    ou:
        ####      Proxy S3. Bucket: website-s3-iac-cv-efemero-xxxx
        ####
        ####    Neste caso o bucket real deve ser utilizado.
        ####
        #### 2. Quando o ambiente ainda está em criação:
        ####      "bucket_name": "TEMPORARIO"
        ####
        ####    O registro TEMPORARIO do DynamoDB representa um ambiente
        ####    ainda não provisionado.
        ####
        #### Prioridade:
        ####    bucket real > bucket TEMPORARIO
        ####

        bucket_match = re.search(
            r"(?:Bucket ativo|Bucket|S3 ativo encontrado|Proxy S3\. Bucket):\s*(\S+)",
            text,
            re.I,
        )

        bucket_json_match = re.search(
            r'"bucket_name"\s*:\s*"([^"]+)"',
            text,
            re.I,
        )

        bucket_name = ""

        if bucket_match:
            bucket_name = bucket_match.group(1).strip()

        elif bucket_json_match:
            bucket_name = bucket_json_match.group(1).strip()

        rejeitado = re.search(
            r"Acesso rejeitado:\s*(.+)",
            text,
            re.I,
        )

        confiavel = re.search(
            r"Acesso confiável:\s*(.+)",
            text,
            re.I,
        )

        status_confianca = "desconhecido"
        motivo_confianca = ""

        if rejeitado:
            status_confianca = "rejeitado"
            motivo_confianca = normalize_motivo(rejeitado.group(1))

        elif confiavel:
            status_confianca = "confiavel"
            motivo_confianca = normalize_motivo(confiavel.group(1))

        elif origem_eventbridge:
            status_confianca = "nao_aplicavel"
            motivo_confianca = "origem_eventbridge"

        disparou_apply = (
            "Disparando workflow: apply.yml" in text
            or "Workflow apply.yml disparado com sucesso" in text
        )

        disparou_destroy = (
            "Disparando workflow: destroy.yml" in text
            or "Workflow destroy.yml disparado com sucesso" in text
        )

        ####---------------------------------------------####
        ####----  Indicadores de resposta do ambiente ----####
        ####---------------------------------------------####
        ####
        #### acordou_ambiente:
        ####   Primeiro acesso válido quando ainda não existe bucket ativo.
        ####   A Lambda cria o registro TEMPORARIO e dispara o workflow apply.
        ####
        #### aguardando_criacao:
        ####   Rechamada/refresh da página de espera enquanto o apply ainda
        ####   não terminou e o bucket definitivo ainda não foi registrado.
        ####
        #### carregou_site:
        ####   O bucket efêmero já existe e o HTML foi servido via proxy S3.
        ####

        acordou_ambiente = (
            "Nenhum TEMPORARIO encontrado. Criando e disparando apply." in text
            or "Criando registro TEMPORARIO no DynamoDB." in text
            or disparou_apply
        )

        aguardando_criacao = (
            "Registro TEMPORARIO encontrado: True" in text
            and "Ambiente já está em processo de criação." in text
        )

        carregou_site = (
            "S3 ativo encontrado:" in text
            or "Servindo site via proxy S3" in text
            or "Proxy S3. Bucket:" in text
        )

        if acordou_ambiente or aguardando_criacao:
            bucket_name = "TEMPORARIO"

        ####--------------------------------------####
        ####----  Estado final da resposta   ----####
        ####--------------------------------------####
        ####
        #### estado_resposta resume o resultado operacional da execução:
        ####
        ####   - criou_ambiente:
        ####       acesso válido disparou criação do ambiente.
        ####
        ####   - aguardando_criacao:
        ####       refresh da página enquanto o apply ainda está em andamento.
        ####
        ####   - site_servido:
        ####       site foi servido a partir do bucket efêmero.
        ####
        ####   - eventbridge_destroy:
        ####       execução EventBridge disparou destroy.
        ####
        ####   - eventbridge_timeout:
        ####       execução EventBridge avaliou timeout/reagendamento.
        ####
        ####   - indefinido:
        ####       execução não se encaixou nas regras conhecidas.
        ####

        if origem_eventbridge and disparou_destroy:
            estado_resposta = "eventbridge_destroy"

        elif origem_eventbridge:
            estado_resposta = "eventbridge_timeout"

        elif acordou_ambiente:
            estado_resposta = "criou_ambiente"

        elif aguardando_criacao:
            estado_resposta = "aguardando_criacao"

        elif carregou_site:
            estado_resposta = "site_servido"

        else:
            estado_resposta = "indefinido"

        ####-----------------------------------------####
        ####----  Marcação para análise posterior ----####
        ####-----------------------------------------####
        ####
        #### A Silver não descarta registros.
        ####
        #### O campo manter_para_analise apenas sinaliza se o evento deve
        #### entrar nas visões analíticas principais da Gold.
        ####
        #### Por enquanto, apenas aguardando_criacao é marcado como False,
        #### pois representa refresh técnico da página de espera.
        ####

        manter_para_analise = estado_resposta != "aguardando_criacao"

        if estado_resposta == "aguardando_criacao":
            motivo_analise = "refresh_aguardando_apply"

        elif estado_resposta == "criou_ambiente":
            motivo_analise = "primeiro_acesso_disparou_apply"

        elif estado_resposta == "site_servido":
            motivo_analise = "acesso_site_servido"

        elif estado_resposta == "eventbridge_destroy":
            motivo_analise = "destroy_automatico"

        elif estado_resposta == "eventbridge_timeout":
            motivo_analise = "validacao_timeout_eventbridge"

        else:
            motivo_analise = "classificacao_indefinida"

        result["origem_eventbridge"] = origem_eventbridge

        result["ambiente_ativo"] = bool(
            ambiente_match and int(ambiente_match.group(1)) > 0
        )

        result["bucket_name"] = bucket_name
        result["status_confianca"] = status_confianca
        result["motivo_confianca"] = motivo_confianca

        result["disparou_apply"] = disparou_apply
        result["disparou_destroy"] = disparou_destroy

        result["acordou_ambiente"] = acordou_ambiente
        result["aguardando_criacao"] = aguardando_criacao
        result["carregou_site"] = carregou_site

        result["estado_resposta"] = estado_resposta
        result["manter_para_analise"] = manter_para_analise
        result["motivo_analise"] = motivo_analise

        classified.append(result)

    return classified