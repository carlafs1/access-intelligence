####---------------------------------------------------------------------------------------####
####----              TESTE DOCUMENTAL - SILVER 04 — enrich_visitors.py                ----####
####---------------------------------------------------------------------------------------####
####----  Objetivo:                                                                    ----####
####----    Validar o enriquecimento de visitantes (identidade, navegador e            ----####
####----    agrupamento de ciclo de vida do ambiente efêmero).                         ----####
####----                                                                               ----####
####----  Regras:                                                                      ----####
####----    - Não acessa AWS.                                                          ----####
####----    - Não lê arquivos.                                                         ----####
####----    - Não grava arquivos.                                                      ----####
####----    - Serve como documentação executável do comportamento esperado.            ----####
####----                                                                               ----####
####----  Casos cobertos:                                                              ----####
####----    1. network_prefix extrai os 4 primeiros blocos do IPv6 (/64)              ----####
####----    1b. visitor_id permanece igual com IPv6 variando dentro do /64            ----####
####----    2. network_prefix extrai os 3 primeiros octetos do IPv4 (/24)             ----####
####----    2b. visitor_id permanece igual com IPv4 variando dentro do /24            ----####
####----    3. browser_family reconhece navegadores e bots conhecidos                  ----####
####----    4. device_type só retorna mobile/desktop (nunca "bot")                     ----####
####----    5. is_scanner_user_agent identifica UAs de scanner conhecidos              ----####
####----    6. is_scanner_user_agent = False para navegador real                       ----####
####----    7. is_social_preview identifica apps sociais (UA e/ou range)               ----####
####----    8. visitor_id é hash estável de network_prefix + user_agent                ----####
####----    9. visitor_id muda quando user_agent muda (mesma rede)                     ----####
####----   10. visitor_type mapeado a partir de motivo_confianca (tabela oficial)      ----####
####----   11. visitor_type vazio quando motivo_confianca vazio                        ----####
####----   12. cf_pop não é mais produzido pelo enriquecimento                         ----####
####----   13. propagação de visitor_id para evento de destroy                         ----####
####----   14. site_session_id agrupa todo o ciclo apply → destroy                     ----####
####----   15. site_session_id distingue eventos isolados (fora de ciclo), count == 1  ----####
####----   16. novo apply com ciclo ainda aberto abre um novo site_session_id          ----####
####---------------------------------------------------------------------------------------####

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from scripts.silver.enrich_visitors import (
    get_network_prefix,
    browser_family,
    device_type,
    is_scanner_user_agent,
    is_social_preview,
    visitor_type,
    enrich_visitors,
    propagate_visitor_id_to_operational_events,
    assign_site_session_id,
)


####-----------------------------####
####----  Fábrica de registros ----####
####-----------------------------####

# IP padrão usado por qualquer teste que não especifica ip= explicitamente.
# O main() roda a suíte inteira duas vezes, alternando este valor entre
# IPv6 e IPv4 — garante que os testes "neutros" (que não testam IP de
# propósito) também sejam exercitados nas duas famílias, sem precisar
# comentar/descomentar nada manualmente.
DEFAULT_IP = "2804:14c:65a0:430b:39a8:7451:bd4c:796f"


def make_record(
    request_id: str,
    timestamp_utc: str,
    ip: str | None = None,
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0) Chrome/120.0",
    range_header: str = "",
    motivo_confianca: str = "all_checks_passed",
    disparou_apply: bool = False,
    disparou_destroy: bool = False,
    bucket_name: str = "",
    estado_resposta: str = "",
) -> dict:
    """Constrói um registro Silver realista para uso nos testes.

    Se ip não for informado, usa DEFAULT_IP — que o main() alterna entre
    IPv6 e IPv4 a cada rodada completa da suíte.
    """
    if ip is None:
        ip = DEFAULT_IP

    return {
        "request_id":       request_id,
        "timestamp_utc":    timestamp_utc,
        "ip":                ip,
        "user_agent":        user_agent,
        "range":             range_header,
        "motivo_confianca":  motivo_confianca,
        "disparou_apply":    disparou_apply,
        "disparou_destroy":  disparou_destroy,
        "bucket_name":       bucket_name,
        "estado_resposta":   estado_resposta,
    }


####------------------####
####----  Testes  ----####
####------------------####

def test_network_prefix_ipv6():
    """network_prefix extrai os 4 primeiros blocos de um IPv6."""
    prefix = get_network_prefix("2804:14c:65a0:430b:39a8:7451:bd4c:796f")
    assert prefix == "2804:14c:65a0:430b", f"esperado prefixo /64, veio: {prefix}"
    print(f"TESTE 01 OK: network_prefix = {prefix}")


def test_visitor_id_recorrente_com_ip_variando_no_64():
    """visitor_id deve permanecer igual quando o IPv6 varia só no sufixo (após o /64)."""
    records = [
        make_record(
            "ipv6_1", "2026-06-18T10:00:00Z",
            ip="2804:14c:65a0:430b:39a8:7451:bd4c:796f",
        ),
        make_record(
            "ipv6_2", "2026-06-18T10:05:00Z",
            ip="2804:14c:65a0:430b:aaaa:bbbb:cccc:dddd",
        ),
    ]
    enrich_visitors(records)

    assert records[0]["network_prefix"] == records[1]["network_prefix"], \
        "mesmo /64 deve gerar o mesmo network_prefix"
    assert records[0]["visitor_id"] == records[1]["visitor_id"], \
        "IPv6 variando dentro do mesmo /64 deve manter o mesmo visitor_id"
    print("TESTE 01b OK: visitor_id recorrente com IPv6 variando dentro do /64")


def test_network_prefix_ipv4_24():
    """network_prefix extrai os 3 primeiros octetos de um IPv4 (/24)."""
    prefix = get_network_prefix("163.116.230.152")
    assert prefix == "163.116.230", f"esperado prefixo /24, veio: {prefix}"
    print(f"TESTE 02 OK: network_prefix (IPv4 /24) = {prefix}")


def test_visitor_id_recorrente_com_ip_variando_no_24():
    """visitor_id deve permanecer igual quando o IPv4 varia só no último octeto."""
    records = [
        make_record("ip1", "2026-06-18T10:00:00Z", ip="163.116.230.152"),
        make_record("ip2", "2026-06-18T10:05:00Z", ip="163.116.230.9"),
    ]
    enrich_visitors(records)

    assert records[0]["visitor_id"] == records[1]["visitor_id"], \
        "IPv4 variando dentro do mesmo /24 deve manter o mesmo visitor_id"
    print("TESTE 02b OK: visitor_id recorrente com IPv4 variando dentro do /24")


def test_browser_family_conhecidos():
    """browser_family reconhece navegadores e bots conhecidos."""
    assert browser_family("Mozilla/5.0 Chrome/120.0") == "chrome"
    assert browser_family("Mozilla/5.0 Chrome/120.0.0.0 Edg/120.0.0.0") == "edge", \
        "Edge é Chromium-based e contém 'chrome' no UA — precisa ser checado antes"
    assert browser_family("Mozilla/5.0 Chrome/120.0.0.0 OPR/106.0.0.0") == "opera", \
        "Opera é Chromium-based e contém 'chrome' no UA — precisa ser checado antes"
    assert browser_family("Mozilla/5.0 Opera/9.80") == "opera"
    assert browser_family("WhatsApp/2.0 Android") == "whatsapp"
    assert browser_family("LinkedInBot/1.0") == "linkedinbot"
    assert browser_family("curl/8.0") == "curl"
    assert browser_family("python-requests/2.31") == "python"
    assert browser_family("algo-totalmente-desconhecido/1.0") == "unknown"
    print("TESTE 03 OK: browser_family reconhece navegadores e bots conhecidos")


def test_device_type_nunca_bot():
    """device_type só deve retornar mobile/desktop, nunca 'bot'."""
    assert device_type("Mozilla/5.0 (iPhone; CPU iPhone OS)") == "mobile"
    assert device_type("Mozilla/5.0 (Linux; Android 13)") == "mobile"
    assert device_type("Mozilla/5.0 Windows NT 10.0") == "desktop"
    assert device_type("zgrab/0.1 crawler bot") == "desktop", \
        "device_type não deve classificar como 'bot' — isso é papel de is_scanner_user_agent"
    print("TESTE 04 OK: device_type retorna apenas mobile/desktop")


def test_is_scanner_user_agent_true():
    """is_scanner_user_agent identifica UAs de scanner conhecidos."""
    assert is_scanner_user_agent("curl/8.0") is True
    assert is_scanner_user_agent("zgrab/0.1") is True
    assert is_scanner_user_agent("python-requests/2.31") is True
    assert is_scanner_user_agent("Mozilla/5.0 PaloAlto/Scanner") is True
    print("TESTE 05 OK: is_scanner_user_agent = True para UAs de scanner conhecidos")


def test_is_scanner_user_agent_false_navegador_real():
    """is_scanner_user_agent = False para navegador real."""
    assert is_scanner_user_agent("Mozilla/5.0 (Windows NT 10.0) Chrome/120.0") is False
    print("TESTE 06 OK: is_scanner_user_agent = False para navegador real")


def test_is_social_preview():
    """is_social_preview identifica apps sociais via UA e/ou header range."""
    assert is_social_preview("WhatsApp/2.23", "") is True
    assert is_social_preview("LinkedInBot/1.0", "") is True
    assert is_social_preview("WhatsApp/2.23", "bytes=0-307199") is True
    assert is_social_preview("Mozilla/5.0 Chrome/120.0", "") is False
    print("TESTE 07 OK: is_social_preview identifica apps sociais corretamente")


def test_visitor_id_estavel():
    """visitor_id é hash estável de network_prefix + user_agent."""
    records = [
        make_record("v1", "2026-06-18T10:00:00Z"),
        make_record("v2", "2026-06-18T10:05:00Z"),
    ]
    enrich_visitors(records)

    assert records[0]["visitor_id"] == records[1]["visitor_id"], \
        "mesmo network_prefix + user_agent deve gerar o mesmo visitor_id"
    print(f"TESTE 08 OK: visitor_id estável = {records[0]['visitor_id']}")


def test_visitor_id_muda_com_user_agent():
    """visitor_id muda quando user_agent muda, mesmo na mesma rede."""
    records = [
        make_record("v3", "2026-06-18T10:00:00Z", user_agent="Mozilla/5.0 Chrome/120.0"),
        make_record("v4", "2026-06-18T10:05:00Z", user_agent="curl/8.0"),
    ]
    enrich_visitors(records)

    assert records[0]["visitor_id"] != records[1]["visitor_id"], \
        "user_agent diferente deve gerar visitor_id diferente"
    print("TESTE 09 OK: visitor_id muda quando user_agent muda")


def test_visitor_id_protegido_ip_invalido():
    """IP ausente/inválido não deve quebrar o cálculo nem colapsar redes
    diferentes sob o mesmo visitor_id — usa 'unknown_network' como fallback."""
    r1 = make_record("inv1", "2026-06-18T10:00:00Z", ip="não-é-um-ip", user_agent="curl/8.0")
    r2 = make_record("inv2", "2026-06-18T10:05:00Z", ip="", user_agent="curl/8.0")
    enrich_visitors([r1, r2])

    assert r1["network_prefix"] == "", "IP inválido deve gerar network_prefix vazio"
    assert r1["visitor_id"], "visitor_id deve ser gerado mesmo com IP inválido"
    assert r1["visitor_id"] == r2["visitor_id"], \
        "mesmo fallback 'unknown_network' + mesmo user_agent deve gerar o mesmo visitor_id"
    print(f"TESTE 09b OK: visitor_id protegido contra IP inválido = {r1['visitor_id']}")


def test_visitor_type_mapeamento_oficial():
    """visitor_type segue a tabela oficial de motivo_confianca (aba Domínios)."""
    assert visitor_type("all_checks_passed")              == "humano_provavel"
    assert visitor_type("missing_accept_language_allowed") == "humano_provavel"
    assert visitor_type("social_preview_bot")              == "social_preview"
    assert visitor_type("scanner_agent")                   == "scanner"
    assert visitor_type("suspicious_path")                 == "scanner"
    assert visitor_type("non_get_method")                  == "scanner"
    assert visitor_type("allowed_search_bot")              == "crawler_buscador"
    assert visitor_type("no_user_agent")                   == "bot_generico"
    assert visitor_type("missing_accept_header")           == "bot_generico"
    print("TESTE 10 OK: visitor_type segue o mapeamento oficial de motivo_confianca")


def test_visitor_type_vazio_sem_motivo():
    """visitor_type fica vazio quando motivo_confianca está vazio (ex.: EventBridge puro)."""
    r = make_record("v5", "2026-06-18T10:00:00Z", motivo_confianca="")
    enrich_visitors([r])

    assert r["visitor_type"] == "", \
        f"esperado vazio sem motivo_confianca, veio: {r['visitor_type']}"
    print("TESTE 11 OK: visitor_type vazio quando motivo_confianca vazio")


def test_cf_pop_nao_existe_mais():
    """cf_pop não deve ser produzido — é dispensável (cf_ray já embute o POP)."""
    r = make_record("v6", "2026-06-18T10:00:00Z")
    enrich_visitors([r])

    assert "cf_pop" not in r, "cf_pop não deve existir no registro enriquecido"
    print("TESTE 12 OK: cf_pop não é mais produzido pelo enriquecimento")


def test_propagacao_visitor_id_destroy():
    """visitor_id deve ser propagado para o evento de destroy do bucket."""
    records = [
        make_record(
            "v7", "2026-06-18T10:00:00Z",
            bucket_name="website-s3-iac-cv-efemero-xyz",
        ),
        make_record(
            "v8", "2026-06-18T10:30:00Z",
            bucket_name="website-s3-iac-cv-efemero-xyz",
            disparou_destroy=True,
            motivo_confianca="",
            user_agent="",
        ),
    ]
    enrich_visitors(records)
    propagate_visitor_id_to_operational_events(records)

    assert records[1]["visitor_id"] == records[0]["visitor_id"], \
        "destroy deve herdar o visitor_id do último acesso real do bucket"
    print("TESTE 13 OK: visitor_id propagado corretamente para o destroy")


def test_site_session_id_agrupa_ciclo_completo():
    """site_session_id deve ser igual em todos os eventos do mesmo ciclo apply → destroy."""
    records = [
        make_record("c1", "2026-06-18T10:00:00Z", disparou_apply=True, bucket_name=""),
        make_record("c2", "2026-06-18T10:00:05Z", bucket_name="TEMPORARIO", motivo_confianca=""),
        make_record("c3", "2026-06-18T10:00:54Z", bucket_name="website-s3-iac-cv-efemero-abc"),
        make_record(
            "c4", "2026-06-18T10:30:00Z",
            bucket_name="website-s3-iac-cv-efemero-abc",
            disparou_destroy=True,
            motivo_confianca="",
        ),
    ]
    enrich_visitors(records)
    assign_site_session_id(records)

    ids = {r["site_session_id"] for r in records}
    assert len(ids) == 1, f"todos os eventos do ciclo devem compartilhar o mesmo id, veio: {ids}"
    assert "" not in ids, "site_session_id do ciclo não deve ficar vazio"
    print(f"TESTE 14 OK: ciclo completo agrupado sob site_session_id = {ids.pop()}")


def test_site_session_id_eventos_isolados_count_um():
    """Eventos fora de qualquer ciclo recebem hash próprio, com contagem == 1."""
    records = [
        make_record(
            "iso1", "2026-06-18T11:00:00Z",
            user_agent="WhatsApp/2.23", range_header="bytes=0-100",
            motivo_confianca="social_preview_bot",
        ),
        make_record(
            "iso2", "2026-06-18T11:05:00Z",
            user_agent="zgrab/0.1", motivo_confianca="scanner_agent",
        ),
    ]
    enrich_visitors(records)
    assign_site_session_id(records)

    assert records[0]["site_session_id"] != "", "evento isolado não deve ficar vazio"
    assert records[0]["site_session_id"] != records[1]["site_session_id"], \
        "eventos isolados distintos devem ter site_session_id distintos"

    contagem = Counter(r["site_session_id"] for r in records)
    for r in records:
        assert contagem[r["site_session_id"]] == 1, \
            "evento isolado deve ter count == 1 — sinal de auditoria"

    print("TESTE 15 OK: eventos isolados com site_session_id próprio e count == 1")


def test_novo_apply_com_ciclo_aberto_abre_outro():
    """Novo disparou_apply com ciclo ainda aberto (anomalia) deve abrir um novo site_session_id."""
    records = [
        make_record("a1", "2026-06-18T09:00:00Z", disparou_apply=True),
        # Sem destroy do ciclo anterior — novo apply chega antes:
        make_record("a2", "2026-06-18T09:30:00Z", disparou_apply=True),
        make_record("a3", "2026-06-18T09:30:05Z"),
    ]
    enrich_visitors(records)
    assign_site_session_id(records)

    assert records[0]["site_session_id"] != records[1]["site_session_id"], \
        "novo apply deve abrir um site_session_id diferente do ciclo anterior"
    assert records[1]["site_session_id"] == records[2]["site_session_id"], \
        "evento após o novo apply deve herdar o novo ciclo"
    print("TESTE 16 OK: novo apply com ciclo aberto inicia um novo site_session_id")


####-----------------------####
####----  Entry point  ----####
####-----------------------####

TESTES = [
    test_network_prefix_ipv6,
    test_visitor_id_recorrente_com_ip_variando_no_64,
    test_network_prefix_ipv4_24,
    test_visitor_id_recorrente_com_ip_variando_no_24,
    test_browser_family_conhecidos,
    test_device_type_nunca_bot,
    test_is_scanner_user_agent_true,
    test_is_scanner_user_agent_false_navegador_real,
    test_is_social_preview,
    test_visitor_id_estavel,
    test_visitor_id_muda_com_user_agent,
    test_visitor_id_protegido_ip_invalido,
    test_visitor_type_mapeamento_oficial,
    test_visitor_type_vazio_sem_motivo,
    test_cf_pop_nao_existe_mais,
    test_propagacao_visitor_id_destroy,
    test_site_session_id_agrupa_ciclo_completo,
    test_site_session_id_eventos_isolados_count_um,
    test_novo_apply_com_ciclo_aberto_abre_outro,
]

# Famílias de IP usadas como DEFAULT_IP em cada rodada completa da suíte.
# Testes que já fixam o próprio ip= (ex.: os de /64 e /24) continuam
# verificando exatamente o que verificavam — a troca aqui só afeta os
# testes "neutros" que dependiam implicitamente do default da fábrica.
RODADAS_IP = [
    ("IPv6", "2804:14c:65a0:430b:39a8:7451:bd4c:796f"),
    ("IPv4", "163.116.230.152"),
]


def run_suite(label: str, ip_default: str) -> int:
    """Roda a suíte inteira uma vez, com DEFAULT_IP fixado para a rodada."""
    global DEFAULT_IP
    DEFAULT_IP = ip_default

    print("=" * 70)
    print(f"TESTE DOCUMENTAL — Silver 04: enrich_visitors.py  [rodada: {label}]")
    print("=" * 70)

    for teste in TESTES:
        teste()

    print()
    print(f"{'=' * 70}")
    print(f"{len(TESTES)} testes passaram. [rodada: {label}, DEFAULT_IP={ip_default}]")
    print(f"{'=' * 70}")
    print()

    return len(TESTES)


def main():
    total = 0

    for label, ip_default in RODADAS_IP:
        total += run_suite(label, ip_default)

    print(f"RESUMO FINAL: {len(RODADAS_IP)} rodadas completas "
          f"({' + '.join(l for l, _ in RODADAS_IP)}), "
          f"{total} execuções de teste no total.")


if __name__ == "__main__":
    main()