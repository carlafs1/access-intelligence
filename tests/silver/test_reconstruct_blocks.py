####------------------------------------------------------------------------------------------####
####----                  TESTE DOCUMENTAL - SILVER 01 — reconstruct_blocks.py            ----####                   
####------------------------------------------------------------------------------------------####
####----  Objetivo:                                                                       ----####
####----    Validar a reconstrução de execuções da Lambda a partir dos eventos Bronze.    ----####
####----                                                                                  ----####
####----  Regras:                                                                         ----####
####----    - Não acessa AWS.                                                             ----####
####----    - Não lê arquivos reais.                                                      ----####
####----    - Não grava arquivos.                                                         ----####
####----    - Serve como documentação executável do comportamento esperado.               ----####
####----                                                                                  ----####
####----  Casos cobertos:                                                                 ----####
####----    01. Bloco completo START → END: block_closed = True, end_ts preenchido        ----####
####----    02. Bloco incompleto (sem END): block_closed = False, end_ts = None           ----####
####----    03. Novo START antes do END fecha bloco anterior como incompleto              ----####
####----    04. Dois log_streams produzem blocos independentes sem interferência          ----####
####----    05. _lines não vaza para o bloco final (campo interno de trabalho)            ----####
####----    06. Separador \\n entre linhas — linhas nunca ficam coladas                   ----####
####----    07. block_text contém todas as linhas do bloco                                ----####
####----    08. select_bronze_files retorna [] sem exceção quando não há pendentes        ----####
####----    09. DEFAULT_BRONZE_BASE aponta para data/bronze (não data/bronze/cloudwatch)  ----####
####----    10. reconstruct_blocks recebe DataFrame, não lista de dicts                   ----####
####----    11. END com request_id divergente é ignorado                                  ----####
####------------------------------------------------------------------------------------------####

from datetime import datetime, timezone
from pathlib import Path
import json
import sys
import tempfile

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from scripts.silver.reconstruct_blocks import (
    reconstruct_blocks,
    select_bronze_files,
    DEFAULT_BRONZE_BASE,
)


####----------------------------####
####----  Fábrica de dados  ----####
####----------------------------####

def ts(h: int, m: int, s: int = 0) -> pd.Timestamp:
    return pd.Timestamp(datetime(2026, 6, 18, h, m, s, tzinfo=timezone.utc))


def make_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def start_msg(req_id: str) -> str:
    return f"START RequestId: {req_id} Version: $LATEST"


def end_msg(req_id: str) -> str:
    return f"END RequestId: {req_id}"


####-------------------####
####----  Testes  ----####
####-------------------####

def test_bloco_completo():
    """Bloco START → END: block_closed = True, end_ts preenchido."""
    df = make_df([
        {"event_id": "e1", "log_stream": "s1", "log_group": "g1",
         "timestamp_utc": ts(4, 17, 49), "message": start_msg("req-001")},
        {"event_id": "e2", "log_stream": "s1", "log_group": "g1",
         "timestamp_utc": ts(4, 17, 50), "message": "Evento recebido: {...}"},
        {"event_id": "e3", "log_stream": "s1", "log_group": "g1",
         "timestamp_utc": ts(4, 17, 51), "message": end_msg("req-001")},
    ])

    blocks = reconstruct_blocks(df)

    assert len(blocks) == 1
    b = blocks[0]
    assert b["request_id"]   == "req-001"
    assert b["block_closed"] is True
    assert b["end_ts"]       == ts(4, 17, 51)
    assert b["start_ts"]     == ts(4, 17, 49)
    assert b["log_group"]    == "g1"
    assert b["log_stream"]   == "s1"

    print("TESTE 01 OK: bloco completo START → END")


def test_bloco_incompleto():
    """Bloco sem END: block_closed = False, end_ts = None."""
    df = make_df([
        {"event_id": "e4", "log_stream": "s2", "log_group": "g1",
         "timestamp_utc": ts(4, 18, 0), "message": start_msg("req-002")},
        {"event_id": "e5", "log_stream": "s2", "log_group": "g1",
         "timestamp_utc": ts(4, 18, 1), "message": "processando..."},
    ])

    blocks = reconstruct_blocks(df)

    assert len(blocks) == 1
    b = blocks[0]
    assert b["block_closed"] is False
    assert b["end_ts"]       is None

    print("TESTE 02 OK: bloco incompleto → block_closed = False, end_ts = None")


def test_novo_start_fecha_anterior():
    """Novo START antes do END fecha bloco anterior como incompleto."""
    df = make_df([
        {"event_id": "e6", "log_stream": "s3", "log_group": "g1",
         "timestamp_utc": ts(4, 19, 0), "message": start_msg("req-003")},
        {"event_id": "e7", "log_stream": "s3", "log_group": "g1",
         "timestamp_utc": ts(4, 19, 1), "message": "linha intermediária"},
        {"event_id": "e8", "log_stream": "s3", "log_group": "g1",
         "timestamp_utc": ts(4, 19, 2), "message": start_msg("req-004")},
        {"event_id": "e9", "log_stream": "s3", "log_group": "g1",
         "timestamp_utc": ts(4, 19, 3), "message": end_msg("req-004")},
    ])

    blocks = reconstruct_blocks(df)

    assert len(blocks)               == 2
    assert blocks[0]["request_id"]   == "req-003"
    assert blocks[0]["block_closed"] is False
    assert blocks[1]["request_id"]   == "req-004"
    assert blocks[1]["block_closed"] is True

    print("TESTE 03 OK: novo START fecha bloco anterior como incompleto")


def test_dois_log_streams_independentes():
    """Dois log_streams produzem blocos independentes sem interferência."""
    df = make_df([
        {"event_id": "ea", "log_stream": "s4", "log_group": "g1",
         "timestamp_utc": ts(4, 20, 0), "message": start_msg("req-A")},
        {"event_id": "eb", "log_stream": "s4", "log_group": "g1",
         "timestamp_utc": ts(4, 20, 1), "message": end_msg("req-A")},
        {"event_id": "ec", "log_stream": "s5", "log_group": "g1",
         "timestamp_utc": ts(4, 20, 2), "message": start_msg("req-B")},
        {"event_id": "ed", "log_stream": "s5", "log_group": "g1",
         "timestamp_utc": ts(4, 20, 3), "message": end_msg("req-B")},
    ])

    blocks = reconstruct_blocks(df)

    assert len(blocks) == 2
    assert {b["request_id"] for b in blocks} == {"req-A", "req-B"}
    assert all(b["block_closed"] for b in blocks)

    print("TESTE 04 OK: dois log_streams produzem blocos independentes")


def test_lines_nao_vaza():
    """_lines é campo interno e não deve aparecer no bloco final."""
    df = make_df([
        {"event_id": "e10", "log_stream": "s6", "log_group": "g1",
         "timestamp_utc": ts(4, 21, 0), "message": start_msg("req-005")},
        {"event_id": "e11", "log_stream": "s6", "log_group": "g1",
         "timestamp_utc": ts(4, 21, 1), "message": end_msg("req-005")},
    ])

    blocks = reconstruct_blocks(df)

    for b in blocks:
        assert "_lines" not in b, f"_lines vazou no bloco {b.get('request_id')}"

    print("TESTE 05 OK: _lines não vaza para o bloco final")


def test_separador_newline():
    """Linhas devem ser separadas por \\n — nunca coladas."""
    df = make_df([
        {"event_id": "e12", "log_stream": "s7", "log_group": "g1",
         "timestamp_utc": ts(4, 22, 0), "message": start_msg("req-006")},
        {"event_id": "e13", "log_stream": "s7", "log_group": "g1",
         "timestamp_utc": ts(4, 22, 1), "message": "Evento recebido: {...}"},
        {"event_id": "e14", "log_stream": "s7", "log_group": "g1",
         "timestamp_utc": ts(4, 22, 2), "message": end_msg("req-006")},
    ])

    blocks = reconstruct_blocks(df)
    lines = blocks[0]["block_text"].split("\n")

    assert len(lines) == 3, f"esperado 3 linhas, veio {len(lines)}: {lines}"
    assert "START" in lines[0]
    assert "Evento recebido" in lines[1]
    assert "END" in lines[2]

    print("TESTE 06 OK: separador \\n — linhas nunca coladas")


def test_block_text_conteudo():
    """block_text deve conter todas as linhas do bloco."""
    df = make_df([
        {"event_id": "e15", "log_stream": "s8", "log_group": "g1",
         "timestamp_utc": ts(4, 23, 0), "message": start_msg("req-007")},
        {"event_id": "e16", "log_stream": "s8", "log_group": "g1",
         "timestamp_utc": ts(4, 23, 1), "message": "linha A"},
        {"event_id": "e17", "log_stream": "s8", "log_group": "g1",
         "timestamp_utc": ts(4, 23, 2), "message": "linha B"},
        {"event_id": "e18", "log_stream": "s8", "log_group": "g1",
         "timestamp_utc": ts(4, 23, 3), "message": end_msg("req-007")},
    ])

    blocks = reconstruct_blocks(df)
    text = blocks[0]["block_text"]

    assert "START" in text
    assert "linha A" in text
    assert "linha B" in text
    assert "END" in text

    print("TESTE 07 OK: block_text contém todas as linhas do bloco")


def test_sem_pendentes_retorna_lista_vazia():
    """select_bronze_files retorna [] sem exceção quando não há pendentes."""
    with tempfile.TemporaryDirectory() as tmp:
        bronze = Path(tmp) / "bronze"
        bronze.mkdir()
        (bronze / "dummy.parquet").write_text("x")

        state_path = Path(tmp) / "state.json"
        state_path.write_text(json.dumps({
            "bronze_to_silver": {
                "last_processed_file": str(bronze / "zzz_futuro.parquet")
            }
        }))

        result = select_bronze_files(str(bronze), str(state_path))

        assert result == [], f"esperado [], veio {result}"

    print("TESTE 08 OK: select_bronze_files retorna [] sem exceção")


def test_default_bronze_base():
    """DEFAULT_BRONZE_BASE deve ser 'data/bronze', não 'data/bronze/cloudwatch'."""
    assert DEFAULT_BRONZE_BASE == "data/bronze", \
        f"esperado 'data/bronze', veio '{DEFAULT_BRONZE_BASE}'"

    print(f"TESTE 09 OK: DEFAULT_BRONZE_BASE = '{DEFAULT_BRONZE_BASE}'")


def test_end_request_id_divergente():
    """END com request_id diferente do bloco atual é ignorado — bloco fica aberto."""
    df = make_df([
        {"event_id": "e21", "log_stream": "s10", "log_group": "g1",
         "timestamp_utc": ts(4, 25, 0), "message": start_msg("req-009")},
        {"event_id": "e22", "log_stream": "s10", "log_group": "g1",
         "timestamp_utc": ts(4, 25, 1), "message": "linha intermediária"},
        # END com request_id diferente — deve ser ignorado
        {"event_id": "e23", "log_stream": "s10", "log_group": "g1",
         "timestamp_utc": ts(4, 25, 2), "message": end_msg("req-OUTRO")},
        # END correto chega logo depois
        {"event_id": "e24", "log_stream": "s10", "log_group": "g1",
         "timestamp_utc": ts(4, 25, 3), "message": end_msg("req-009")},
    ])

    blocks = reconstruct_blocks(df)

    assert len(blocks) == 1
    b = blocks[0]
    assert b["request_id"]   == "req-009"
    assert b["block_closed"] is True, \
        "bloco deve fechar quando END correto chega depois do END errado"
    assert b["end_ts"] == ts(4, 25, 3)

    print("TESTE 11 OK: END com request_id divergente é ignorado — bloco aguarda END correto")


def test_reconstruct_blocks_recebe_dataframe():
    """reconstruct_blocks recebe DataFrame — não lista de dicts."""
    df = make_df([
        {"event_id": "e19", "log_stream": "s9", "log_group": "g1",
         "timestamp_utc": ts(4, 24, 0), "message": start_msg("req-008")},
        {"event_id": "e20", "log_stream": "s9", "log_group": "g1",
         "timestamp_utc": ts(4, 24, 1), "message": end_msg("req-008")},
    ])

    assert isinstance(df, pd.DataFrame)
    blocks = reconstruct_blocks(df)
    assert len(blocks) == 1

    print("TESTE 10 OK: reconstruct_blocks recebe DataFrame corretamente")


####-----------------------####
####----  Entry point  ----####
####-----------------------####

def main():
    print("=" * 70)
    print("TESTE DOCUMENTAL — Silver 01: reconstruct_blocks.py")
    print("=" * 70)

    testes = [
        test_bloco_completo,
        test_bloco_incompleto,
        test_novo_start_fecha_anterior,
        test_dois_log_streams_independentes,
        test_lines_nao_vaza,
        test_separador_newline,
        test_block_text_conteudo,
        test_sem_pendentes_retorna_lista_vazia,
        test_default_bronze_base,
        test_reconstruct_blocks_recebe_dataframe,
        test_end_request_id_divergente,
    ]

    for teste in testes:
        teste()

    print()
    print("=" * 70)
    print(f"{len(testes)} testes passaram.")
    print("=" * 70)


if __name__ == "__main__":
    main()