####---------------------------------------------------------------------------------------####
####----               TESTE DOCUMENTAL - SILVER 02 — extract_events.py                ----####
####---------------------------------------------------------------------------------------####
####----  Objetivo:                                                                    ----####
####----    Validar a extração de eventos HTTP dos blocos reconstruídos da Lambda.     ----####
####----                                                                               ----####
####----  Regras:                                                                      ----####
####----    - Não acessa AWS.                                                          ----####
####----    - Não lê arquivos.                                                         ----####
####----    - Não grava arquivos.                                                      ----####
####----    - Serve como documentação executável do comportamento esperado.            ----####
####----                                                                               ----####
####----  Casos cobertos:                                                              ----####
####----    1. timestamp_utc termina em Z (não +00:00)                                 ----####
####----    2. datetime ingênuo (tzinfo is None) é protegido — recebe UTC              ----####
####----    3. fallback block_closed → closed (chave "closed")                         ----####
####----    4. fallback block_closed → block_closed (chave "block_closed")             ----####
####----    5. sem nenhuma chave de fechamento → block_closed = False                  ----####
####----    6. parse_status = "ok" para evento HTTP válido                             ----####
####----    7. todos os 16 headers mapeados corretamente                               ----####
####----    8. campos de requisição HTTP corretos                                      ----####
####----    9. source_ip_cloudflare vem de requestContext.http.sourceIp                ----####
####----   10. EventBridge → is_http_event = False, parse_status = "erro"              ----####
####----   11. campos removidos não estão no registro                                  ----####
####----       (route_key, api_gateway_request_id, timestamp_bsb)                      ----####
####----   12. block_text preservado para classify_events                              ----####
####----   13. parse_error vazio quando parse bem-sucedido                             ----####
####----   14. timeEpoch tem prioridade sobre start_ts do CloudWatch                   ----####
####---------------------------------------------------------------------------------------####

from datetime import datetime, timezone
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from scripts.silver.extract_events import extract_events


####-----------------------------####
####----  Fábrica de blocos  ----####
####-----------------------------####

def make_block(
    request_id: str,
    start_ts: datetime,
    closed_key: str | None = None,
    closed_val: bool = False,
    with_epoch: bool = True,
    extra_headers: dict | None = None,
) -> dict:
    """
    Constrói um bloco de execução Lambda realista para uso nos testes.

    with_epoch=False simula o caso em que timeEpoch não está presente,
    forçando o uso do start_ts do CloudWatch como timestamp.
    """
    epoch_part   = '"timeEpoch": 1781756269258,' if with_epoch else ""
    extra        = extra_headers or {}
    merged_extra = ", ".join(f'"{k}": "{v}"' for k, v in extra.items())
    extra_comma  = ", " + merged_extra if merged_extra else ""

    block_text = (
        f"START RequestId: {request_id}\n"
        f"Evento recebido:\n"
        f'{{"version":"2.0","rawPath":"/","rawQueryString":"utm=linkedin",'
        f'"headers":{{'
        f'"cf-connecting-ip":"40.114.79.135","cf-ipcountry":"US",'
        f'"cf-ray":"a0d77c8a9a6bb114-IAD","host":"carlasampaio.com.br",'
        f'"user-agent":"Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36",'
        f'"accept":"text/html,application/xhtml+xml",'
        f'"accept-encoding":"gzip, br","accept-language":"en-US;q=1.0",'
        f'"range":"bytes=0-307199","referer":"https://wa.me/",'
        f'"sec-ch-ua":"\\"Chromium\\";v=\\"146\\"",'
        f'"sec-ch-ua-mobile":"?0","sec-ch-ua-platform":"\\"Windows\\"",'
        f'"sec-fetch-dest":"document","sec-fetch-mode":"navigate",'
        f'"sec-fetch-site":"none","sec-fetch-user":"?1",'
        f'"x-forwarded-for":"40.114.79.135, 104.22.101.251"'
        f'{extra_comma}}},'
        f'"requestContext":{{'
        f'"http":{{"method":"GET","sourceIp":"104.22.101.251"}},'
        f'{epoch_part}"requestId":"fI6pIjKAiYcEM5A="}},'
        f'"isBase64Encoded":false}}\n'
        f"END RequestId: {request_id}"
    )

    block = {
        "request_id": request_id,
        "block_text": block_text,
        "start_ts":   start_ts,
        "log_group":  "/aws/lambda/website-s3-iac-cv-controle",
        "log_stream": "2026/06/18/[$LATEST]abc123",
    }

    if closed_key:
        block[closed_key] = closed_val

    return block


def make_eventbridge_block(request_id: str, start_ts: datetime) -> dict:
    """Bloco de evento EventBridge (sem estrutura HTTP)."""
    return {
        "request_id": request_id,
        "block_text": (
            f"START RequestId: {request_id}\n"
            f"Origem: EventBridge.\n"
            f"END RequestId: {request_id}"
        ),
        "start_ts":   start_ts,
        "log_group":  "/aws/lambda/website-s3-iac-cv-controle",
        "log_stream": "stream1",
        "closed":     True,
    }


####------------------####
####----  Testes  ----####
####------------------####

def test_timestamp_formato_z():
    """timestamp_utc deve terminar em Z, nunca em +00:00."""
    r = extract_events([
        make_block("t1", datetime(2026, 6, 18, 4, 17, tzinfo=timezone.utc), "closed", True)
    ])[0]

    assert r["timestamp_utc"].endswith("Z"), \
        f"esperado Z, veio: {r['timestamp_utc']}"
    assert "+00:00" not in r["timestamp_utc"], \
        f"offset +00:00 não deve aparecer: {r['timestamp_utc']}"

    print(f"TESTE 01 OK: timestamp_utc = {r['timestamp_utc']}")


def test_datetime_ingenuo_protegido():
    """datetime sem tzinfo deve receber UTC e produzir timestamp com Z."""
    r = extract_events([
        make_block("t2", datetime(2026, 6, 18, 4, 17), "closed", True, with_epoch=False)
    ])[0]

    assert r["timestamp_utc"] is not None, "timestamp_utc não deve ser None"
    assert r["timestamp_utc"].endswith("Z"), \
        f"datetime ingênuo não foi protegido: {r['timestamp_utc']}"

    print(f"TESTE 02 OK: datetime ingênuo protegido → {r['timestamp_utc']}")


def test_block_closed_chave_closed():
    """Fallback: block_closed lê chave 'closed'."""
    r = extract_events([
        make_block("t3a", datetime(2026, 6, 18, tzinfo=timezone.utc), "closed", True)
    ])[0]

    assert r["block_closed"] is True
    print("TESTE 03 OK: block_closed leu chave 'closed' = True")


def test_block_closed_chave_block_closed():
    """Fallback: block_closed lê chave 'block_closed'."""
    r = extract_events([
        make_block("t3b", datetime(2026, 6, 18, tzinfo=timezone.utc), "block_closed", True)
    ])[0]

    assert r["block_closed"] is True
    print("TESTE 04 OK: block_closed leu chave 'block_closed' = True")


def test_block_closed_sem_chave():
    """Sem nenhuma chave de fechamento → block_closed = False."""
    r = extract_events([
        make_block("t3c", datetime(2026, 6, 18, tzinfo=timezone.utc))
    ])[0]

    assert r["block_closed"] is False
    print("TESTE 05 OK: sem chave → block_closed = False")


def test_parse_status_ok():
    """Evento HTTP válido → parse_status = 'ok', parse_error = ''."""
    r = extract_events([
        make_block("t4", datetime(2026, 6, 18, tzinfo=timezone.utc), "closed", True)
    ])[0]

    assert r["parse_status"] == "ok",    f"esperado 'ok', veio '{r['parse_status']}'"
    assert r["parse_error"]  == "",      f"parse_error deve ser vazio, veio '{r['parse_error']}'"
    print("TESTE 06 OK: parse_status = 'ok', parse_error = ''")


def test_headers_mapeados():
    """Todos os 16 headers do dicionário devem ser extraídos corretamente."""
    r = extract_events([
        make_block("t5", datetime(2026, 6, 18, tzinfo=timezone.utc), "closed", True)
    ])[0]

    assert r["ip"]                == "40.114.79.135"
    assert r["pais_cf"]           == "US"
    assert r["cf_ray"]            == "a0d77c8a9a6bb114-IAD"
    assert r["accept"]            == "text/html,application/xhtml+xml"
    assert r["accept_encoding"]   == "gzip, br"
    assert r["accept_language"]   == "en-US;q=1.0"
    assert r["range"]             == "bytes=0-307199"
    assert r["referer"]           == "https://wa.me/"
    assert r["sec_ch_ua"]         == '"Chromium";v="146"'
    assert r["sec_ch_ua_mobile"]  == "?0"
    assert r["sec_ch_ua_platform"]== '"Windows"'
    assert r["sec_fetch_dest"]    == "document"
    assert r["sec_fetch_mode"]    == "navigate"
    assert r["sec_fetch_site"]    == "none"
    assert r["sec_fetch_user"]    == "?1"
    assert r["x_forwarded_for"]   == "40.114.79.135, 104.22.101.251"
    assert r["user_agent"]        == "Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36"

    print("TESTE 07 OK: todos os 16 headers mapeados corretamente")


def test_campos_requisicao_http():
    """Campos de requisição HTTP extraídos corretamente."""
    r = extract_events([
        make_block("t6", datetime(2026, 6, 18, tzinfo=timezone.utc), "closed", True)
    ])[0]

    assert r["method"]           == "GET"
    assert r["raw_path"]         == "/"
    assert r["raw_query_string"] == "utm=linkedin"
    assert r["host"]             == "carlasampaio.com.br"
    print("TESTE 08 OK: campos de requisição HTTP corretos")


def test_source_ip_cloudflare():
    """source_ip_cloudflare vem de requestContext.http.sourceIp."""
    r = extract_events([
        make_block("t7", datetime(2026, 6, 18, tzinfo=timezone.utc), "closed", True)
    ])[0]

    assert r["source_ip_cloudflare"] == "104.22.101.251"
    print("TESTE 09 OK: source_ip_cloudflare = requestContext.http.sourceIp")


def test_eventbridge_nao_http():
    """Evento EventBridge → is_http_event = False, parse_status = 'erro'."""
    r = extract_events([
        make_eventbridge_block("eb-1", datetime(2026, 6, 18, 4, 18, tzinfo=timezone.utc))
    ])[0]

    assert r["is_http_event"] is False, \
        f"EventBridge não deve ser HTTP: {r['is_http_event']}"
    assert r["parse_status"] == "erro", \
        f"esperado 'erro', veio '{r['parse_status']}'"
    print("TESTE 10 OK: EventBridge → is_http_event=False, parse_status='erro'")


def test_campos_removidos():
    """Campos não previstos no dicionário não devem estar no registro."""
    r = extract_events([
        make_block("t8", datetime(2026, 6, 18, tzinfo=timezone.utc), "closed", True)
    ])[0]

    assert "route_key"              not in r, "route_key não deve existir"
    assert "api_gateway_request_id" not in r, "api_gateway_request_id não deve existir"
    assert "timestamp_bsb"          not in r, "timestamp_bsb não deve existir"
    print("TESTE 11 OK: route_key, api_gateway_request_id e timestamp_bsb removidos")


def test_block_text_preservado():
    """block_text deve ser preservado para classify_events.py."""
    r = extract_events([
        make_block("t9", datetime(2026, 6, 18, tzinfo=timezone.utc), "closed", True)
    ])[0]

    assert "block_text" in r,          "block_text deve existir no registro"
    assert "Evento recebido" in r["block_text"]
    print("TESTE 12 OK: block_text preservado para classify_events")


def test_timeepoch_prioridade():
    """timeEpoch tem prioridade sobre start_ts do CloudWatch."""
    start_ts_cw = datetime(2026, 6, 18, 0, 0, 0, tzinfo=timezone.utc)  # hora diferente
    expected_ts = datetime.fromtimestamp(
        1781756269258 / 1000, tz=timezone.utc
    ).isoformat().replace("+00:00", "Z")

    r = extract_events([
        make_block("t10", start_ts_cw, "closed", True, with_epoch=True)
    ])[0]

    assert r["timestamp_utc"] == expected_ts, \
        f"esperado {expected_ts}, veio {r['timestamp_utc']}"
    print(f"TESTE 13 OK: timeEpoch prevalece sobre start_ts → {r['timestamp_utc']}")


####-----------------------####
####----  Entry point  ----####
####-----------------------####

def main():
    print("=" * 70)
    print("TESTE DOCUMENTAL — Silver 02: extract_events.py")
    print("=" * 70)

    testes = [
        test_timestamp_formato_z,
        test_datetime_ingenuo_protegido,
        test_block_closed_chave_closed,
        test_block_closed_chave_block_closed,
        test_block_closed_sem_chave,
        test_parse_status_ok,
        test_headers_mapeados,
        test_campos_requisicao_http,
        test_source_ip_cloudflare,
        test_eventbridge_nao_http,
        test_campos_removidos,
        test_block_text_preservado,
        test_timeepoch_prioridade,
    ]

    for teste in testes:
        teste()

    print()
    print(f"{'=' * 70}")
    print(f"{len(testes)} testes passaram.")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()