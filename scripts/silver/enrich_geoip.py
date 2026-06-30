####---------------------------------------------------------------------------------------####
####----            Silver 05 — Enriquece visitantes com geolocalização e rede.        ----####
####---------------------------------------------------------------------------------------####
####----                                                                               ----####
####----  Objetivo:                                                                    ----####
####----    Consultar a base GeoIP (MaxMind GeoLite2) a partir do campo ip e derivar   ----####
####----    atributos de localização e de classificação de rede — bloco "Rede e        ----####
####----    Geolocalização" da aba Final do dicionário de dados.                       ----####
####----                                                                               ----####
####----  Campos produzidos (aba Final):                                               ----####
####----    - geo_country_code, geo_country_name                                       ----####
####----    - geo_city, geo_region, geo_postal_code                                    ----####
####----    - geo_latitude, geo_longitude, geo_timezone                                ----####
####----    - geo_asn, geo_provider          (derivados de geo_org)                    ----####
####----    - is_cloud_provider, is_known_scanner_network, network_type                ----####
####----                                                                               ----####
####----  Campo insumo (usado internamente, não persiste como coluna):                 ----####
####----    geo_org — string bruta do GeoIP (ex.: "AS28573 Claro NXT Telecomunicacoes  ----####
####----    Ltda"), redundante com a concatenação de geo_asn + geo_provider. Usada só  ----####
####----    para derivar esses dois campos e as flags de rede.                         ----####
####----                                                                               ----####
####----  Cache de IPs (evita re-consultar a base externa):                            ----####
####----    Antes de consultar o MaxMind, o orquestrador carrega do R2 um cache        ----####
####----    em Parquet (bucket access-intelligence-cache) já consultado em runs        ----####
####----    anteriores. Só os IPs distintos ainda não vistos vão até a base externa;   ----####
####----    o cache atualizado volta para o R2 ao final do run. Ver build_cached_      ----####
####----    lookup, extract_distinct_ips, get_uncached_ips, load_geoip_cache_from_r2   ----####
####----    e save_geoip_cache_to_r2.                                                  ----####
####----                                                                               ----####
####----  Desenho de injeção de dependência:                                           ----####
####----    enrich_geoip() recebe um geoip_lookup(ip) -> dict | None como parâmetro,   ----####
####----    em vez de ler o banco MaxMind diretamente. Isso mantém a lógica de         ----####
####----    classificação 100% testável sem precisar do arquivo .mmdb real — o mesmo   ----####
####----    princípio de "não acessa AWS / não lê arquivo" já usado nos testes da      ----####
####----    Silver 01-04. A leitura real do banco fica isolada em                      ----####
####----    make_maxmind_lookup(), só usada pelo orquestrador (run_silver.py).         ----####
####----                                                                               ----####
####----  Observações importantes (aba Final):                                         ----####
####----    geo_latitude/geo_longitude têm baixa confiabilidade para decisão de        ----####
####----    segurança — servem só para visualização em mapas.                          ----####
####----    Cuidado com falso positivo em is_cloud_provider: VPN corporativa           ----####
####----    hospedada em cloud também acende essa flag.                                ----####
####---------------------------------------------------------------------------------------####

from __future__ import annotations

import re
from typing import Callable, Optional


####---------------------------------------------####
####----  Tipo da função de lookup injetada  ----####
####---------------------------------------------####

# geoip_lookup(ip) deve retornar um dict com as chaves abaixo (todas
# opcionais — preencha o que a base tiver) ou None/dict vazio quando o IP
# não for encontrado (ex.: IP privado, reservado, ou fora da base):
#   country_code, country_name, city, region, postal_code,
#   latitude, longitude, timezone, org
GeoIPLookup = Callable[[str], Optional[dict]]


####------------------------------####
####----  Parsing de geo_org  ----####
####------------------------------####

RE_ASN = re.compile(r"^(AS\d+)\s*(.*)$")


def parse_geo_org(geo_org: str) -> tuple[str, str]:
    """
    Separa geo_org em geo_asn + geo_provider.

    Exemplo:
      "AS28573 Claro NXT Telecomunicacoes Ltda"
      ↓
      geo_asn="AS28573", geo_provider="Claro NXT Telecomunicacoes Ltda"

    Se geo_org não seguir o padrão "AS<número> <nome>", geo_asn fica vazio
    e o texto inteiro vai para geo_provider (degradação segura).
    """
    geo_org = (geo_org or "").strip()

    if not geo_org:
        return "", ""

    match = RE_ASN.match(geo_org)

    if not match:
        return "", geo_org

    return match.group(1), match.group(2).strip()


####-------------------------------------------####
####----  Classificação de rede (geo_org)  ----####
####-------------------------------------------####

CLOUD_PROVIDER_TOKENS = [
    "amazon",
    "aws",
    "google cloud",
    "google llc",
    "microsoft",
    "azure",
    "digitalocean",
    "digital ocean",
    "oracle cloud",
    "alibaba",
    "tencent",
    "ovh",
    "hetzner",
    "linode",
    "akamai connected cloud",
    "vultr",
    "scaleway",
    "ibm cloud",
    "contabo",
]

SCANNER_NETWORK_TOKENS = [
    "palo alto networks",
    "censys",
    "shodan",
    "internet-wide scan",
    "internet wide scan",
    "stretchoid",
    "binaryedge",
    "leakix",
    "rapid7",
    "onyphe",
    "netsystemsresearch",
    "security research",
]


def is_cloud_provider(geo_provider: str) -> bool:
    """
    Indica se o provedor de rede é uma nuvem pública conhecida
    (AWS, Azure, GCP etc.).

    Cuidado (aba Final): VPN corporativa hospedada em cloud também acende
    essa flag — não é, isoladamente, prova de bot/scanner.
    """
    provider = geo_provider.lower()
    return any(token in provider for token in CLOUD_PROVIDER_TOKENS)


def is_known_scanner_network(geo_org: str) -> bool:
    """
    Indica se a rede de origem é uma rede conhecida de varredura/scanner
    (Palo Alto, Censys, Shodan etc.) — sinal de segurança forte, maior peso
    no suspicion_score.
    """
    org = geo_org.lower()
    return any(token in org for token in SCANNER_NETWORK_TOKENS)


def network_type(
    geo_org: str,
    geo_provider: str,
    has_geo_data: bool,
) -> str:
    """
    Classifica o tipo de rede: residencial, cloud ou scanner.

    Prioridade: scanner > cloud > residencial (default).
    Sem dado de GeoIP disponível, retorna vazio — não há base para opinar.
    """
    if not has_geo_data:
        return ""

    if is_known_scanner_network(geo_org):
        return "scanner"

    if is_cloud_provider(geo_provider):
        return "cloud"

    return "residencial"


####------------------------------------####
####----  Enriquecimento principal  ----####
####------------------------------------####

def enrich_geoip(records: list[dict], geoip_lookup: GeoIPLookup) -> list[dict]:
    """
    Adiciona campos de geolocalização e classificação de rede a cada registro.

    geoip_lookup é injetado pelo chamador — ver GeoIPLookup acima. Isso
    permite testar toda a lógica de derivação sem depender do banco MaxMind
    real.
    """
    for record in records:
        ip = record.get("ip", "")

        geo = geoip_lookup(ip) if ip else None
        geo = geo or {}
        has_geo_data = bool(geo)

        geo_org = geo.get("org", "") or ""
        geo_asn, geo_provider = parse_geo_org(geo_org)

        record["geo_country_code"] = geo.get("country_code", "") or ""
        record["geo_country_name"] = geo.get("country_name", "") or ""
        record["geo_city"]         = geo.get("city", "") or ""
        record["geo_region"]       = geo.get("region", "") or ""
        record["geo_postal_code"]  = geo.get("postal_code", "") or ""
        record["geo_latitude"]     = geo.get("latitude")
        record["geo_longitude"]    = geo.get("longitude")
        record["geo_timezone"]     = geo.get("timezone", "") or ""
        record["geo_asn"]          = geo_asn
        record["geo_provider"]     = geo_provider

        record["is_cloud_provider"]        = is_cloud_provider(geo_provider)
        record["is_known_scanner_network"] = is_known_scanner_network(geo_org)
        record["network_type"]             = network_type(
            geo_org, geo_provider, has_geo_data,
        )

    return records


####-----------------------------------------------------------####
####----  Implementação real da lookup (MaxMind GeoLite2)  ----####
####-----------------------------------------------------------####

def make_maxmind_lookup(city_db_path: str, asn_db_path: str) -> GeoIPLookup:
    """
    Constrói um geoip_lookup(ip) real, lendo as bases MaxMind GeoLite2
    City e ASN (.mmdb).

    Uso típico no orquestrador (run_silver.py):

      lookup = make_maxmind_lookup(
          "data/geoip/GeoLite2-City.mmdb",
          "data/geoip/GeoLite2-ASN.mmdb",
      )
      enrich_geoip(records, lookup)

    Import do pacote geoip2 é feito aqui dentro (lazy import) para que o
    restante do módulo — usado pelos testes — não exija a dependência
    instalada nem os arquivos .mmdb presentes.

    IPs privados/reservados ou ausentes da base levantam
    AddressNotFoundError; tratado retornando None (sem dado de GeoIP),
    em vez de propagar a exceção e quebrar o pipeline.
    """
    import geoip2.database
    import geoip2.errors

    city_reader = geoip2.database.Reader(city_db_path)
    asn_reader  = geoip2.database.Reader(asn_db_path)

    def lookup(ip: str) -> Optional[dict]:
        if not ip:
            return None

        result: dict = {}

        try:
            city = city_reader.city(ip)
            result["country_code"] = city.country.iso_code or ""
            result["country_name"] = city.country.name or ""
            result["city"]         = city.city.name or ""
            result["region"]       = (
                city.subdivisions.most_specific.name
                if city.subdivisions
                else ""
            ) or ""
            result["postal_code"]  = city.postal.code or ""
            result["latitude"]     = city.location.latitude
            result["longitude"]    = city.location.longitude
            result["timezone"]     = city.location.time_zone or ""
        except geoip2.errors.AddressNotFoundError:
            pass

        try:
            asn = asn_reader.asn(ip)
            number = asn.autonomous_system_number
            org    = asn.autonomous_system_organization or ""

            if number:
                result["org"] = f"AS{number} {org}".strip()
            else:
                result["org"] = org
        except geoip2.errors.AddressNotFoundError:
            pass

        return result or None

    return lookup


####---------------------------------------------------------------------------------------####
####----     Cache de IPs já consultados (evita re-consultar a base externa)           ----####
####---------------------------------------------------------------------------------------####
####----                                                                               ----####
####----  Estratégia:                                                                  ----####
####----    1. Carrega o cache (dict ip -> dado GeoIP) persistido no R2.               ----####
####----    2. Para cada IP distinto do arquivo, primeiro tenta o cache.               ----####
####----    3. Só consulta a base externa (MaxMind) para os IPs que não estavam        ----####
####----       no cache.                                                               ----####
####----    4. Ao final do run, o cache atualizado (antigo + novos IPs) é gravado      ----####
####----       de volta no R2 — orquestrador decide quando salvar.                     ----####
####----                                                                               ----####
####----  Por que um sentinel em vez de cache.get(ip, {}):                             ----####
####----    Um IP privado/reservado ou fora da base MaxMind gera resultado vazio       ----####
####----    ({} ou None) — isso PRECISA ser cacheado também (já sabemos que não tem    ----####
####----    geo dado), senão o pipeline re-consultaria esse IP eternamente em todo     ----####
####----    run futuro. Por isso o cache guarda {} para "consultado e sem dado",       ----####
####----    distinto de "nunca consultado" (chave ausente do dict).                    ----####
####---------------------------------------------------------------------------------------####

_CACHE_MISS = object()


def extract_distinct_ips(records: list[dict]) -> list[str]:
    """
    Extrai os IPs distintos presentes nos registros, na ordem de primeira
    aparição (determinístico, útil para logging/depuração).
    """
    seen: set[str] = set()
    distinct: list[str] = []

    for record in records:
        ip = record.get("ip", "")

        if ip and ip not in seen:
            seen.add(ip)
            distinct.append(ip)

    return distinct


def get_uncached_ips(distinct_ips: list[str], cache: dict) -> list[str]:
    """
    Filtra, dentre os IPs distintos, apenas os que ainda não estão no cache
    — são esses que efetivamente vão até a base externa.
    """
    return [ip for ip in distinct_ips if ip not in cache]


def build_cached_lookup(geoip_lookup: GeoIPLookup, cache: dict) -> GeoIPLookup:
    """
    Envolve um geoip_lookup real com uma camada de cache em memória.

    Comportamento:
      - IP já presente no cache (mesmo que com valor vazio) → retorna do
        cache, sem chamar geoip_lookup.
      - IP ausente do cache → chama geoip_lookup, grava o resultado no
        cache (inclusive quando o resultado é None/vazio) e retorna.

    O dict cache é mutado in-place — ao final do enrich_geoip, ele contém
    o cache antigo + todos os IPs novos consultados nesse run, pronto para
    ser persistido de volta no R2 pelo orquestrador.
    """

    def lookup(ip: str) -> Optional[dict]:
        if not ip:
            return None

        cached = cache.get(ip, _CACHE_MISS)

        if cached is not _CACHE_MISS:
            return cached or None

        result = geoip_lookup(ip)
        cache[ip] = result or {}
        return result

    return lookup


####---------------------------------------------------------------------------------------####
####----  Persistência do cache no Cloudflare R2 (Parquet)                             ----####
####---------------------------------------------------------------------------------------####
####----                                                                               ----####
####----  Bucket: access-intelligence-cache                                            ----####
####----  Key padrão: geoip/geoip_cache.parquet                                        ----####
####----                                                                               ----####
####----  Formato Parquet em vez de JSON: mais compacto (colunar + compressão) e já    ----####
####----  é o formato usado no restante do pipeline (Bronze/Silver/Gold).              ----####
####---------------------------------------------------------------------------------------####

CACHE_COLUMNS = [
    "ip",
    "country_code",
    "country_name",
    "city",
    "region",
    "postal_code",
    "latitude",
    "longitude",
    "timezone",
    "org",
]


def cache_dict_to_dataframe(cache: dict):
    """
    Converte o cache em memória (dict ip -> dado GeoIP) para um DataFrame
    no formato gravado em Parquet.

    IPs cacheados como "sem dado" ({}) geram uma linha só com o ip
    preenchido e o restante vazio — preserva a distinção entre "consultado,
    sem dado" e "nunca consultado" mesmo depois de ir para o Parquet.
    """
    import pandas as pd

    rows = []

    for ip, geo in cache.items():
        geo = geo or {}
        row = {"ip": ip}

        for col in CACHE_COLUMNS[1:]:
            row[col] = geo.get(col)

        rows.append(row)

    return pd.DataFrame(rows, columns=CACHE_COLUMNS)


def dataframe_to_cache_dict(df) -> dict:
    """
    Converte o DataFrame lido do Parquet de volta para o dict ip -> dado
    GeoIP usado por build_cached_lookup.

    Campos vazios (NaN/None) não entram no dict do registro — mantém o
    mesmo formato "enxuto" que geoip_lookup já produz (chaves ausentes em
    vez de None espalhado).
    """
    import pandas as pd

    cache: dict = {}

    for _, row in df.iterrows():
        ip = row["ip"]
        geo = {
            col: row[col]
            for col in CACHE_COLUMNS[1:]
            if pd.notna(row[col])
        }
        cache[ip] = geo

    return cache


def load_geoip_cache_from_r2(
    r2_client,
    bucket: str = "access-intelligence-cache",
    key: str = "geoip/geoip_cache.parquet",
) -> dict:
    """
    Carrega o cache de IPs já consultados a partir de um Parquet no R2.

    Retorna {} se o objeto ainda não existir (primeiro run) — não é erro,
    é o estado normal antes da primeira consulta.

    r2_client é um cliente boto3 ("s3") apontando para o endpoint do R2,
    injetado pelo chamador — mesma lógica de injeção de dependência usada
    em make_maxmind_lookup, para manter a função testável com um client
    fake.
    """
    import io
    import pandas as pd

    try:
        obj = r2_client.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read()
        df = pd.read_parquet(io.BytesIO(body))
        return dataframe_to_cache_dict(df)
    except r2_client.exceptions.NoSuchKey:
        print(f"Cache não encontrado em r2://{bucket}/{key}. Iniciando vazio.")
        return {}
    except Exception as exc:
        # Cobre ClientError genérico (ex.: 404 sem NoSuchKey tipado em
        # alguns clients) sem derrubar o pipeline por causa do cache.
        print(f"Falha ao carregar cache do R2 ({exc}). Iniciando vazio.")
        return {}


def save_geoip_cache_to_r2(
    cache: dict,
    r2_client,
    bucket: str = "access-intelligence-cache",
    key: str = "geoip/geoip_cache.parquet",
) -> None:
    """
    Grava o cache de IPs (antigo + novos IPs consultados neste run) de
    volta no R2 em formato Parquet, sobrescrevendo o objeto anterior.
    """
    import io

    df = cache_dict_to_dataframe(cache)
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    r2_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )


def make_r2_client(
    account_id: str,
    access_key_id: str,
    secret_access_key: str,
):
    """
    Constrói um client boto3 ("s3") apontando para o endpoint do
    Cloudflare R2 da conta.

    Import do boto3 feito aqui dentro (lazy import) pelo mesmo motivo do
    geoip2 em make_maxmind_lookup — o módulo principal não exige a
    dependência instalada para os testes.

    region_name="auto" é obrigatório: sem isso, o boto3 tenta inferir uma
    região AWS qualquer do ambiente local (ex.: de uma config AWS CLI
    pré-existente, como "us-east-2"), e o R2 rejeita com
    InvalidRegionName — ele só aceita "auto" ou os códigos de região
    próprios da Cloudflare (wnam, enam, weur, eeur, apac, oc).
    """
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name="auto",
    )


####---------------------------------------------------------------------------------------####
####----     Cross-validação com IPinfo (segunda fonte, só para os IPs novos)          ----####
####---------------------------------------------------------------------------------------####
####----                                                                               ----####
####----  Desenho (decisão de produto registrada):                                     ----####
####----    - MaxMind GeoLite2 continua sendo a fonte PRINCIPAL (geo_city etc. já      ----####
####----      vêm de lá, sem mudança).                                                 ----####
####----    - IPinfo entra só como segunda fonte de lat/long+país, para medir          ----####
####----      DIVERGÊNCIA — nunca para "corrigir" ou substituir o dado do MaxMind.     ----####
####----    - Consulta sem autenticação (https://ipinfo.io/{ip}/json) — gratuita, mas  ----####
####----      sem garantia de estabilidade. Por isso: timeout curto, nunca lança       ----####
####----      exceção, sempre degrada para None se falhar (pipeline nunca quebra por   ----####
####----      causa do IPinfo).                                                        ----####
####----    - Mesma camada de cache (build_cached_lookup) já usada para o MaxMind —    ----####
####----      cada IP só é consultado no IPinfo uma vez na vida.                       ----####
####----    - connection_type NÃO está disponível em nenhuma fonte gratuita (é um      ----####
####----      banco pago separado, GeoIP2-Connection-Type) — não tentamos simular.     ----####
####----                                                                               ----####
####----  Severidade da divergência (tabela de classificação):                         ----####
####----    ≤100 km                      → baixa                                       ----####
####----    100–500 km                    → média                                      ----####
####----    >500 km OU país diferente     → alta  (geo_sources_divergent = True)       ----####
####---------------------------------------------------------------------------------------####

import math


def haversine_distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Distância em km entre dois pontos (lat/long), usando a fórmula de
    Haversine — padrão para distância "em linha reta" sobre a superfície
    da Terra, suficiente para nosso propósito (não precisamos de precisão
    de rota, só de uma medida de "quão longe as duas fontes discordam").
    """
    R = 6371.0  # raio médio da Terra em km

    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = (
        math.sin(delta_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c


def classify_geo_divergence_severity(
    distance_km: float | None,
    country_differs: bool,
) -> str:
    """
    Classifica a severidade da divergência entre MaxMind e IPinfo.

    Tabela de classificação:
      país diferente       → alta  (sobrepõe a distância, mesmo que pequena)
      > 500 km             → alta
      100–500 km           → média
      <= 100 km            → baixa
      sem dado suficiente  → "" (não dá para opinar)
    """
    if country_differs:
        return "alta"

    if distance_km is None:
        return ""

    if distance_km > 500:
        return "alta"

    if distance_km > 100:
        return "media"

    return "baixa"


def make_ipinfo_lookup() -> GeoIPLookup:
    """
    Constrói um lookup(ip) -> dict|None real, consultando
    https://ipinfo.io/{ip}/json SEM autenticação (gratuito, sem token).

    Mesmo formato de retorno de make_maxmind_lookup (country_code, city,
    region, postal_code, latitude, longitude, timezone, org) — para que
    qualquer função que já trabalha com o resultado do MaxMind também
    funcione aqui sem adaptação.

    Nunca lança exceção: falha de rede, timeout, IP privado, ou resposta
    inesperada — tudo cai em None. O IPinfo é um sinal complementar
    opcional; uma instabilidade dele não pode derrubar o pipeline.
    """
    import requests

    def lookup(ip: str) -> Optional[dict]:
        if not ip:
            return None

        try:
            resp = requests.get(f"https://ipinfo.io/{ip}/json", timeout=5)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            print(f"[ipinfo] Falha ao consultar {ip} ({exc}). Seguindo sem esse dado.")
            return None

        latitude, longitude = None, None
        loc = data.get("loc", "")

        if loc and "," in loc:
            try:
                lat_str, lon_str = loc.split(",", 1)
                latitude, longitude = float(lat_str), float(lon_str)
            except ValueError:
                pass

        return {
            "country_code": data.get("country", "") or "",
            "city": data.get("city", "") or "",
            "region": data.get("region", "") or "",
            "postal_code": data.get("postal", "") or "",
            "latitude": latitude,
            "longitude": longitude,
            "timezone": data.get("timezone", "") or "",
            "org": data.get("org", "") or "",
        }

    return lookup


def cross_validate_with_ipinfo(records: list[dict], ipinfo_lookup: GeoIPLookup) -> list[dict]:
    """
    Para cada registro já enriquecido por enrich_geoip() (MaxMind), consulta
    o IPinfo como segunda fonte e calcula a divergência entre as duas.

    Não sobrescreve nenhum campo geo_* do MaxMind — só adiciona:
      - geo_distance_km        — distância em km entre as duas fontes
      - geo_divergence_severity — "baixa" | "media" | "alta" | ""
      - geo_sources_divergent   — True somente quando severity == "alta"

    Registros sem lat/long do MaxMind, ou sem dado do IPinfo, ficam com
    geo_distance_km = None e severity = "" — sem dado suficiente para
    opinar, não é tratado como divergência.
    """
    for record in records:
        ip = record.get("ip", "")

        mm_lat = record.get("geo_latitude")
        mm_lon = record.get("geo_longitude")
        mm_country = record.get("geo_country_code") or ""

        ipinfo_data = ipinfo_lookup(ip) if ip else None
        ipinfo_data = ipinfo_data or {}

        ii_lat = ipinfo_data.get("latitude")
        ii_lon = ipinfo_data.get("longitude")
        ii_country = ipinfo_data.get("country_code") or ""

        has_both_coords = None not in (mm_lat, mm_lon, ii_lat, ii_lon)

        distance_km = (
            haversine_distance_km(mm_lat, mm_lon, ii_lat, ii_lon)
            if has_both_coords
            else None
        )

        country_known = bool(mm_country) and bool(ii_country)
        country_differs = country_known and mm_country != ii_country

        severity = classify_geo_divergence_severity(distance_km, country_differs)

        record["geo_distance_km"] = distance_km
        record["geo_divergence_severity"] = severity
        record["geo_sources_divergent"] = severity == "alta"

    return records