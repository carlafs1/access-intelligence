####---------------------------------------------------------------------------------------####
####----        Controle incremental — leitura/escrita genérica no R2                  ----####
####---------------------------------------------------------------------------------------####
####----                                                                               ----####
####----  Objetivo:                                                                    ----####
####----    Centralizar a leitura/escrita dos arquivos de controle incremental que     ----####
####----    cada orquestrador usa para saber "de onde continuar" — em vez de cada um   ----####
####----    reescrever a mesma lógica de JSON + R2.                                    ----####
####----                                                                               ----####
####----  Usado por:                                                                   ----####
####----    - cloudwatch_to_bronze.py  → config.CONTROL_KEY_CLOUDWATCH_TO_BRONZE       ----####
####----    - run_silver.py            → config.CONTROL_KEY_BRONZE_TO_SILVER           ----####
####----    - run_gold.py (futuro)     → config.CONTROL_KEY_SILVER_TO_GOLD             ----####
####----                                                                               ----####
####----  As keys e o nome do bucket (access-intelligence-cache) ficam em              ----####
####----  scripts/config.py — não aqui. Este módulo só sabe LER/ESCREVER um            ----####
####----  controle JSON no R2, dado um client + bucket + key; não decide nomes.        ----####
####----                                                                               ----####
####----  Por que não Parquet: Parquet só compensa com muitas linhas repetidas         ----####
####----  (compressão colunar). Cada controle aqui é um único registro/estado — o      ----####
####----  overhead do formato Parquet pode até deixar o arquivo maior que o JSON       ----####
####----  equivalente, e perde a legibilidade humana (cat arquivo.json funciona;       ----####
####----  ler Parquet exige abrir Python).                                             ----####
####---------------------------------------------------------------------------------------####

from __future__ import annotations

import json


def load_control_state_from_r2(r2_client, bucket: str, key: str) -> dict:
    """
    Carrega um arquivo de controle JSON do R2.

    Retorna {} se o objeto ainda não existir (primeiro run) — estado normal,
    não é erro.
    """
    try:
        obj = r2_client.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read()
        return json.loads(body) if body else {}
    except r2_client.exceptions.NoSuchKey:
        print(f"Controle não encontrado em r2://{bucket}/{key}. Iniciando vazio.")
        return {}
    except Exception as exc:
        print(f"Falha ao carregar controle do R2 ({exc}). Iniciando vazio.")
        return {}


def save_control_state_to_r2(state: dict, r2_client, bucket: str, key: str) -> None:
    """Grava o estado de controle (dict) como JSON no R2, sobrescrevendo o anterior."""
    payload = json.dumps(state, ensure_ascii=False, indent=2).encode("utf-8")

    r2_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=payload,
        ContentType="application/json",
    )