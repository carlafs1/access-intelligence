####---------------------------------------------------------------------------------------####
####----                  Configuração — credenciais e buckets do R2                   ----####
####---------------------------------------------------------------------------------------####
####----                                                                               ----####
####----  Objetivo:                                                                    ----####
####----    Centralizar a leitura das variáveis de ambiente do R2 (credenciais e nomes ----####
####----    de bucket), em vez de espalhar os.environ[...] por todos os scripts.       ----####
####----                                                                               ----####
####----  Por que via variável de ambiente, e não hardcoded ou em arquivo de config    ----####
####----  versionado:                                                                  ----####
####----    Credenciais nunca devem ir para o controle de versão. O .env real fica só  ----####
####----    na máquina local (ou em "Secrets" do ambiente de execução, se rodar em     ----####
####----    CI/CD); o .env.example documenta o formato esperado, sem valores reais.    ----####
####----                                                                               ----####
####----  Uso típico no orquestrador (run_silver.py):                                  ----####
####----                                                                               ----####
####----    from dotenv import load_dotenv                                             ----####
####----    load_dotenv()  # lê o .env local para os.environ                           ----####
####----    from scripts.config import load_r2_config                                  ----####
####----    config = load_r2_config()                                                  ----####
####----                                                                               ----####
####---------------------------------------------------------------------------------------####

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class R2Config:
    account_id: str
    access_key_id: str
    secret_access_key: str
    bucket_bronze: str
    bucket_silver: str
    bucket_gold: str
    bucket_cache: str
    geoip_city_db_path: str
    geoip_asn_db_path: str


REQUIRED_ENV_VARS = [
    "R2_ACCOUNT_ID",
    "R2_ACCESS_KEY_ID",
    "R2_SECRET_ACCESS_KEY",
]

# Keys (caminhos dentro do bucket de cache) dos arquivos de controle
# incremental. Não são segredo nem variam por ambiente — por isso ficam
# como constantes fixas aqui, não como variável de .env.
CONTROL_KEY_CLOUDWATCH_TO_BRONZE = "control/cloudwatch_to_bronze.json"
CONTROL_KEY_BRONZE_TO_SILVER     = "control/bronze_to_silver.json"
CONTROL_KEY_SILVER_TO_GOLD       = "control/silver_to_gold.json"   # reservado, run_gold.py


def load_r2_config(env: dict | None = None) -> R2Config:
    """
    Lê e valida as variáveis de ambiente do R2.

    env permite injetar um dict fake nos testes, em vez de depender de
    os.environ real — mesmo princípio de injeção de dependência usado em
    enrich_geoip.py (geoip_lookup, r2_client).

    Levanta ValueError com uma mensagem clara se faltar alguma variável
    obrigatória — falha rápido e explícito, em vez de um client boto3
    quebrando mais adiante com um erro de autenticação confuso.
    """
    source = env if env is not None else os.environ

    faltando = [var for var in REQUIRED_ENV_VARS if not source.get(var)]

    if faltando:
        raise ValueError(
            "Variáveis de ambiente do R2 ausentes: "
            f"{', '.join(faltando)}. "
            "Copie .env.example para .env, preencha os valores reais e "
            "garanta que load_dotenv() foi chamado antes de load_r2_config()."
        )

    return R2Config(
        account_id=source["R2_ACCOUNT_ID"],
        access_key_id=source["R2_ACCESS_KEY_ID"],
        secret_access_key=source["R2_SECRET_ACCESS_KEY"],
        bucket_bronze=source.get("R2_BUCKET_BRONZE", "access-intelligence-bronze"),
        bucket_silver=source.get("R2_BUCKET_SILVER", "access-intelligence-silver"),
        bucket_gold=source.get("R2_BUCKET_GOLD", "access-intelligence-gold"),
        bucket_cache=source.get("R2_BUCKET_CACHE", "access-intelligence-cache"),
        geoip_city_db_path=source.get(
            "GEOIP_CITY_DB_PATH", "data/geoip/GeoLite2-City.mmdb",
        ),
        geoip_asn_db_path=source.get(
            "GEOIP_ASN_DB_PATH", "data/geoip/GeoLite2-ASN.mmdb",
        ),
    )