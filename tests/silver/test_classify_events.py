####-----------------------------------------------------------------------------------------####
####----             TESTE DOCUMENTAL - SILVER 03 — classify_events.py                   ----#### 
####-----------------------------------------------------------------------------------------####
####----                                                                                 ----####
####----  Objetivo:                                                                      ----####
####----    Validar a classificação operacional dos eventos da Lambda controle.          ----####
####----                                                                                 ----####
####----  Regras:                                                                        ----####
####----    - Não acessa AWS.                                                            ----####
####----    - Não lê arquivos.                                                           ----####
####----    - Não grava arquivos.                                                        ----####
####----    - Usa JSONs reais extraídos dos logs da conversa.                            ----####
####----                                                                                 ----####
####----  Casos cobertos:                                                                ----####
####----    01. Primeiro acesso humano → criou_ambiente, disparou_apply, status trusted  ----####
####----    02. EventBridge com destroy → eventbridge_destroy, disparou_destroy          ----####
####----    03. EventBridge sem destroy → eventbridge_timeout, sem status_confianca      ----####
####----    04. Acesso rejeitado sem ambiente → untrusted, deploy_avoided = True         ----####
####----    05. Site servido a acesso confiável → site_servido, renovou_timeout = True   ----####
####----    06. Refresh durante criação (temp_item) → aguardando_criacao, manter=False   ----####
####----    07. status_confianca: "trusted" / "untrusted" / "" (nunca "confiavel"        ----####
####----        /"rejeitado")                                                            ----####
####----    08. motivo_confianca vem de access_decision.reason (não texto livre)         ----####
####----    09. decision_user_agent extraído de access_decision.user_agent               ----####
####----    10. bucket_name de destroy_triggered tem prioridade sobre lambda_execution   ----####
####----    11. bucket_name nunca é "TEMPORARIO"                                         ----####
####----    12. last_accessed_at e expiration_time de destroy_triggered                  ----####
####----    13. active_site_untrusted_access detectado pelo evento homônimo              ----####
####----    14. event_type segue a prioridade: destroy > deploy                          ----####
####----        > active_site_untrusted_access > access_decision > lambda_execution      ----####
####----    15. campos obsoletos removidos: acordou_ambiente, ambiente_ativo,            ----####
####----        carregou_site                                                            ----####
####----    16. temp_item e active_items não aparecem como colunas de saída              ----####
####-----------------------------------------------------------------------------------------####

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from scripts.silver.classify_events import classify_events


####----------------------------####
####----  Fábrica de blocos  ----####
####----------------------------####

def make_record(block_text: str, is_http_event: bool = True) -> dict:
    """Registro mínimo simulando saída do extract_events.py."""
    return {
        "request_id":    "req-test",
        "is_http_event": is_http_event,
        "block_text":    block_text,
    }


# ── Block texts realistas baseados nos logs reais ──────────────────────────

BT_PRIMEIRO_ACESSO = """
START RequestId: req-001 Version: $LATEST
=== Lambda controle iniciada ===
{"event": "lambda_execution", "timestamp": "2026-06-18T04:17:50Z", "origem_eventbridge": false, "temp_item": false, "active_items": 0, "bucket": null}
Nenhum S3 ativo encontrado.
{"event": "access_decision", "trusted": true, "reason": "all_checks_passed", "user_agent": "mozilla/5.0 (windows nt 10.0) applewebkit/537.36", "path": "/", "method": "GET"}
Criando registro TEMPORARIO no DynamoDB.
Disparando workflow: apply.yml
{"event": "deploy_triggered", "reason": "no_active_s3_no_temp_item", "timestamp": "2026-06-18T04:17:50Z"}
Retornando página HTML de carregamento.
END RequestId: req-001
"""

BT_EVENTBRIDGE_DESTROY = """
START RequestId: req-002 Version: $LATEST
{"event": "lambda_execution", "timestamp": "2026-06-18T04:48:28Z", "origem_eventbridge": true, "temp_item": false, "active_items": 1, "bucket": "website-s3-iac-cv-efemero-357d077d"}
Timeout expirado. Disparando workflow destroy.
Disparando workflow: destroy.yml
{"event": "destroy_triggered", "bucket": "website-s3-iac-cv-efemero-357d077d", "last_accessed_at": "2026-06-18T04:18:00+00:00", "expiration_time": "2026-06-18T04:48:00+00:00", "timestamp": "2026-06-18T04:48:00Z"}
END RequestId: req-002
"""

BT_EVENTBRIDGE_TIMEOUT = """
START RequestId: req-003 Version: $LATEST
{"event": "lambda_execution", "timestamp": "2026-06-18T04:18:51Z", "origem_eventbridge": true, "temp_item": false, "active_items": 1, "bucket": "website-s3-iac-cv-efemero-357d077d"}
Ainda dentro do timeout. Reagendando EventBridge.
END RequestId: req-003
"""

BT_ACESSO_REJEITADO = """
START RequestId: req-004 Version: $LATEST
{"event": "lambda_execution", "timestamp": "2026-06-18T16:30:19Z", "origem_eventbridge": false, "temp_item": false, "active_items": 0, "bucket": null}
{"event": "access_decision", "trusted": false, "reason": "missing_accept_header", "user_agent": "opera/9.80 (windows nt 6.1)", "path": "/", "method": "GET"}
{"event": "access_rejected", "reason": "scanner_or_untrusted", "timestamp": "2026-06-18T16:30:19Z"}
Acesso não confiável. Retornando página neutra sem acordar o ambiente.
END RequestId: req-004
"""

BT_SITE_SERVIDO = """
START RequestId: req-005 Version: $LATEST
{"event": "lambda_execution", "timestamp": "2026-06-18T05:00:00Z", "origem_eventbridge": false, "temp_item": false, "active_items": 1, "bucket": "website-s3-iac-cv-efemero-357d077d"}
{"event": "access_decision", "trusted": true, "reason": "all_checks_passed", "user_agent": "mozilla/5.0 (iphone; cpu iphone os 17_0)", "path": "/", "method": "GET"}
Servindo site via proxy S3
END RequestId: req-005
"""

BT_AGUARDANDO_CRIACAO = """
START RequestId: req-006 Version: $LATEST
{"event": "lambda_execution", "timestamp": "2026-06-18T04:18:00Z", "origem_eventbridge": false, "temp_item": true, "active_items": 0, "bucket": null}
{"event": "access_decision", "trusted": true, "reason": "all_checks_passed", "user_agent": "mozilla/5.0", "path": "/", "method": "GET"}
Registro TEMPORARIO encontrado: True
Retornando página de espera.
END RequestId: req-006
"""

BT_ACTIVE_SITE_UNTRUSTED = """
START RequestId: req-007 Version: $LATEST
{"event": "lambda_execution", "timestamp": "2026-06-18T06:00:00Z", "origem_eventbridge": false, "temp_item": false, "active_items": 1, "bucket": "website-s3-iac-cv-efemero-abc123"}
{"event": "access_decision", "trusted": false, "reason": "bot_user_agent_blocked", "user_agent": "sparixemailscraper/1.0", "path": "/", "method": "GET"}
{"event": "active_site_untrusted_access", "timestamp": "2026-06-18T06:00:00Z"}
END RequestId: req-007
"""


####-------------------####
####----  Testes  ----####
####-------------------####

def test_primeiro_acesso_humano():
    """Primeiro acesso humano → criou_ambiente, apply disparado, trusted."""
    r = classify_events([make_record(BT_PRIMEIRO_ACESSO)])[0]

    assert r["estado_resposta"]    == "criou_ambiente"
    assert r["disparou_apply"]     is True
    assert r["disparou_destroy"]   is False
    assert r["status_confianca"]   == "trusted"
    assert r["motivo_confianca"]   == "all_checks_passed"
    assert r["origem_eventbridge"] is False
    assert r["manter_para_analise"] is True
    assert r["motivo_analise"]     == "primeiro_acesso_disparou_apply"
    assert r["event_type"]         == "deploy_triggered"
    assert r["bucket_name"]        == ""  # bucket ainda não existe

    print("TESTE 01 OK: primeiro acesso humano → criou_ambiente")


def test_eventbridge_destroy():
    """EventBridge com timeout expirado → eventbridge_destroy."""
    r = classify_events([make_record(BT_EVENTBRIDGE_DESTROY, is_http_event=False)])[0]

    assert r["estado_resposta"]    == "eventbridge_destroy"
    assert r["disparou_destroy"]   is True
    assert r["disparou_apply"]     is False
    assert r["origem_eventbridge"] is True
    assert r["bucket_name"]        == "website-s3-iac-cv-efemero-357d077d"
    assert r["last_accessed_at"]   == "2026-06-18T04:18:00+00:00"
    assert r["expiration_time"]    == "2026-06-18T04:48:00+00:00"
    assert r["event_type"]         == "destroy_triggered"
    assert r["motivo_analise"]     == "destroy_automatico"

    print("TESTE 02 OK: EventBridge destroy → eventbridge_destroy")


def test_eventbridge_timeout():
    """EventBridge sem destroy → eventbridge_timeout, sem status_confianca."""
    r = classify_events([make_record(BT_EVENTBRIDGE_TIMEOUT, is_http_event=False)])[0]

    assert r["estado_resposta"]    == "eventbridge_timeout"
    assert r["disparou_destroy"]   is False
    assert r["origem_eventbridge"] is True
    assert r["status_confianca"]   == ""   # sem decisão de acesso
    assert r["motivo_confianca"]   == ""
    assert r["decision_user_agent"] == ""
    assert r["motivo_analise"]     == "validacao_timeout_eventbridge"

    print("TESTE 03 OK: EventBridge timeout → eventbridge_timeout")


def test_acesso_rejeitado_sem_ambiente():
    """Acesso rejeitado sem ambiente ativo → untrusted, deploy_avoided = True."""
    r = classify_events([make_record(BT_ACESSO_REJEITADO)])[0]

    assert r["status_confianca"]   == "untrusted"
    assert r["motivo_confianca"]   == "missing_accept_header"
    assert r["deploy_avoided"]     is True
    assert r["disparou_apply"]     is False
    assert r["bucket_name"]        == ""

    print("TESTE 04 OK: acesso rejeitado sem ambiente → deploy_avoided = True")


def test_status_confianca_valores_corretos():
    """status_confianca usa 'trusted'/'untrusted', nunca 'confiavel'/'rejeitado'."""
    r_trusted   = classify_events([make_record(BT_PRIMEIRO_ACESSO)])[0]
    r_untrusted = classify_events([make_record(BT_ACESSO_REJEITADO)])[0]
    r_eb        = classify_events([make_record(BT_EVENTBRIDGE_TIMEOUT, False)])[0]

    assert r_trusted["status_confianca"]   == "trusted"
    assert r_untrusted["status_confianca"] == "untrusted"
    assert r_eb["status_confianca"]        == ""

    for val in ("confiavel", "rejeitado", "desconhecido", "nao_aplicavel"):
        assert r_trusted["status_confianca"]   != val
        assert r_untrusted["status_confianca"] != val

    print("TESTE 05 OK: status_confianca = 'trusted' / 'untrusted' / ''")


def test_motivo_e_ua_de_json():
    """motivo_confianca e decision_user_agent vêm do JSON, não de texto livre."""
    r = classify_events([make_record(BT_ACESSO_REJEITADO)])[0]

    assert r["motivo_confianca"]    == "missing_accept_header"
    assert r["decision_user_agent"] == "opera/9.80 (windows nt 6.1)"

    print("TESTE 06 OK: motivo_confianca e decision_user_agent do JSON access_decision")


def test_site_servido_renovou_timeout():
    """Site servido a acesso confiável → site_servido, renovou_timeout = True."""
    r = classify_events([make_record(BT_SITE_SERVIDO)])[0]

    assert r["estado_resposta"]  == "site_servido"
    assert r["serviu_site"]      is True
    assert r["renovou_timeout"]  is True
    assert r["deploy_avoided"]   is False
    assert r["disparou_apply"]   is False
    assert r["motivo_analise"]   == "acesso_site_servido"

    print("TESTE 07 OK: site servido → renovou_timeout = True")


def test_aguardando_criacao():
    """Refresh com temp_item=True → aguardando_criacao, manter_para_analise=False."""
    r = classify_events([make_record(BT_AGUARDANDO_CRIACAO)])[0]

    assert r["aguardando_criacao"]  is True
    assert r["estado_resposta"]     == "aguardando_criacao"
    assert r["manter_para_analise"] is False
    assert r["motivo_analise"]      == "refresh_aguardando_apply"
    assert r["bucket_name"]         == ""

    print("TESTE 08 OK: aguardando_criacao → manter_para_analise = False")


def test_bucket_name_nunca_temporario():
    """bucket_name nunca recebe o valor 'TEMPORARIO'."""
    for bt in [BT_AGUARDANDO_CRIACAO, BT_PRIMEIRO_ACESSO]:
        r = classify_events([make_record(bt)])[0]
        assert r["bucket_name"] != "TEMPORARIO", \
            f"bucket_name não deve ser 'TEMPORARIO', veio: {r['bucket_name']!r}"

    print("TESTE 09 OK: bucket_name nunca é 'TEMPORARIO'")


def test_bucket_destroy_tem_prioridade():
    """bucket_name de destroy_triggered prevalece sobre lambda_execution.bucket."""
    r = classify_events([make_record(BT_EVENTBRIDGE_DESTROY, is_http_event=False)])[0]

    assert r["bucket_name"] == "website-s3-iac-cv-efemero-357d077d"

    print("TESTE 10 OK: bucket_name de destroy_triggered tem prioridade")


def test_last_accessed_at_expiration_time():
    """last_accessed_at e expiration_time extraídos de destroy_triggered."""
    r = classify_events([make_record(BT_EVENTBRIDGE_DESTROY, is_http_event=False)])[0]

    assert r["last_accessed_at"] == "2026-06-18T04:18:00+00:00"
    assert r["expiration_time"]  == "2026-06-18T04:48:00+00:00"

    print("TESTE 11 OK: last_accessed_at e expiration_time de destroy_triggered")


def test_active_site_untrusted_access():
    """active_site_untrusted_access → estado próprio, não site_servido."""
    r = classify_events([make_record(BT_ACTIVE_SITE_UNTRUSTED)])[0]

    assert r["active_site_untrusted_access"] is True
    assert r["status_confianca"]   == "untrusted"
    assert r["event_type"]         == "active_site_untrusted_access"
    assert r["estado_resposta"]    == "site_servido_nao_confiavel"
    assert r["estado_resposta"]    != "site_servido"
    assert r["renovou_timeout"]    is False
    assert r["manter_para_analise"] is True
    assert r["motivo_analise"]     == "bot_ou_preview_em_ambiente_ativo"

    print("TESTE 12 OK: active_site_untrusted_access → site_servido_nao_confiavel")


def test_event_type_prioridade():
    """event_type segue prioridade: destroy > deploy > access_decision > lambda_execution."""
    assert classify_events([make_record(BT_EVENTBRIDGE_DESTROY, False)])[0]["event_type"] == "destroy_triggered"
    assert classify_events([make_record(BT_PRIMEIRO_ACESSO)])[0]["event_type"]            == "deploy_triggered"
    assert classify_events([make_record(BT_ACTIVE_SITE_UNTRUSTED)])[0]["event_type"]       == "active_site_untrusted_access"
    assert classify_events([make_record(BT_ACTIVE_SITE_UNTRUSTED)])[0]["estado_resposta"]  == "site_servido_nao_confiavel"
    assert classify_events([make_record(BT_SITE_SERVIDO)])[0]["event_type"]               == "access_decision"
    assert classify_events([make_record(BT_EVENTBRIDGE_TIMEOUT, False)])[0]["event_type"] == "lambda_execution"

    print("TESTE 13 OK: event_type segue a prioridade correta")


def test_campos_obsoletos_removidos():
    """Campos não previstos no dicionário não devem existir no resultado."""
    r = classify_events([make_record(BT_PRIMEIRO_ACESSO)])[0]

    for campo in ("acordou_ambiente", "ambiente_ativo", "carregou_site"):
        assert campo not in r, f"campo obsoleto '{campo}' não deve existir"

    print("TESTE 14 OK: acordou_ambiente, ambiente_ativo e carregou_site removidos")


def test_temp_item_active_items_nao_propagam():
    """temp_item e active_items são insumos internos — não viram colunas."""
    r = classify_events([make_record(BT_AGUARDANDO_CRIACAO)])[0]

    assert "temp_item"    not in r, "temp_item não deve ser coluna de saída"
    assert "active_items" not in r, "active_items não deve ser coluna de saída"

    print("TESTE 15 OK: temp_item e active_items não propagam como colunas")


def test_site_servido_vs_nao_confiavel():
    """site_servido e site_servido_nao_confiavel são estados distintos."""
    r_trusted   = classify_events([make_record(BT_SITE_SERVIDO)])[0]
    r_untrusted = classify_events([make_record(BT_ACTIVE_SITE_UNTRUSTED)])[0]

    # Trusted → site_servido com timeout renovado
    assert r_trusted["estado_resposta"] == "site_servido"
    assert r_trusted["renovou_timeout"] is True

    # Untrusted → estado próprio, timeout NÃO renovado
    assert r_untrusted["estado_resposta"] == "site_servido_nao_confiavel"
    assert r_untrusted["renovou_timeout"] is False

    # Os dois nunca colapsam no mesmo estado
    assert r_trusted["estado_resposta"] != r_untrusted["estado_resposta"]

    print("TESTE 16 OK: site_servido ≠ site_servido_nao_confiavel")


####-----------------------####
####----  Entry point  ----####
####-----------------------####

def main():
    print("=" * 70)
    print("TESTE DOCUMENTAL — Silver 03: classify_events.py")
    print("=" * 70)

    testes = [
        test_primeiro_acesso_humano,
        test_eventbridge_destroy,
        test_eventbridge_timeout,
        test_acesso_rejeitado_sem_ambiente,
        test_status_confianca_valores_corretos,
        test_motivo_e_ua_de_json,
        test_site_servido_renovou_timeout,
        test_aguardando_criacao,
        test_bucket_name_nunca_temporario,
        test_bucket_destroy_tem_prioridade,
        test_last_accessed_at_expiration_time,
        test_active_site_untrusted_access,
        test_event_type_prioridade,
        test_campos_obsoletos_removidos,
        test_temp_item_active_items_nao_propagam,
        test_site_servido_vs_nao_confiavel,
    ]

    for teste in testes:
        teste()

    print()
    print("=" * 70)
    print(f"{len(testes)} testes passaram.")
    print("=" * 70)


if __name__ == "__main__":
    main()