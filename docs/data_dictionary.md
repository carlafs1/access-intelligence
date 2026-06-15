# Dicionário de Dados — Silver / access_events

## Metadados do Dataset

| Item | Valor |
|---|---|
| Camada | Silver |
| Dataset | access_events |
| Partição | year / month / day |
| Formato | Parquet |
| Chave primária | request_id |
| Corte inicial | timestamp_utc >= 2026-06-10T03:00:00Z |
| Total de campos | 34 |
| Versão | 1.1 |
| Atualizado em | 2026-06-12 |

## Campos

| Campo | Tipo | Nulo? | Definição | Origem |
|---|---|---|---|---|
| `request_id` | string | Não | Identificador técnico único da execução da Lambda controle. | Linha START RequestId do CloudWatch. |
| `api_gateway_request_id` | string | Sim | Identificador da requisição HTTP recebida pelo API Gateway. | requestContext.requestId. |
| `timestamp_utc` | timestamp | Sim | Momento da ocorrência em UTC. | requestContext.timeEpoch ou timestamp do bloco CloudWatch. |
| `ip` | string | Sim | IP real do visitante. | Header cf-connecting-ip. |
| `pais_cf` | string | Sim | País de origem informado pela Cloudflare. | Header cf-ipcountry. |
| `method` | string | Sim | Método HTTP utilizado. | requestContext.http.method. |
| `route_key` | string | Sim | Rota resolvida pelo API Gateway. | Campo routeKey. |
| `raw_path` | string | Sim | Caminho solicitado. | Campo rawPath. |
| `host` | string | Sim | Domínio acessado. | Header host. |
| `user_agent` | string | Sim | Identificação do navegador, bot, crawler ou scanner. | Header user-agent. |
| `referer` | string | Sim | Origem da navegação quando disponível. | Header referer ou referrer. |
| `accept_language` | string | Sim | Idiomas preferenciais do cliente. | Header accept-language. |
| `cf_ray` | string | Sim | Identificador da transação no Cloudflare. | Header cf-ray. |
| `x_forwarded_for` | string | Sim | Cadeia de IPs atravessados pela requisição. | Header x-forwarded-for. |
| `source_ip_cloudflare` | string | Sim | IP visto pelo API Gateway. | requestContext.http.sourceIp. |
| `log_group` | string | Sim | Grupo do CloudWatch Logs. | Metadado da Bronze. |
| `log_stream` | string | Sim | Stream do CloudWatch Logs. | Metadado da Bronze. |
| `block_closed` | boolean | Não | Indica se o bloco START/END foi fechado corretamente. | Reconstrução dos blocos da Lambda. |
| `is_http_event` | boolean | Não | Indica se o bloco contém evento HTTP estruturado. | Presença de headers e requestContext.http. |
| `origem_eventbridge` | boolean | Não | Indica se a execução foi iniciada pelo EventBridge. | Log Origem EventBridge: True. |
| `ambiente_ativo` | boolean | Não | Indica se existia ambiente S3 ativo. | Log Ambientes ativos encontrados. |
| `bucket_name` | string | Sim | Nome do bucket S3 ativo. | Log S3 ativo encontrado. |
| `status_confianca` | string | Não | Classificação operacional do acesso. | Mensagens Acesso confiável, Acesso rejeitado ou Origem EventBridge. |
| `motivo_confianca` | string | Sim | Motivo normalizado da classificação. | Mensagem de decisão da Lambda controle. |
| `disparou_apply` | boolean | Não | Indica se o workflow apply.yml foi acionado. | Logs de disparo do apply.yml. |
| `disparou_destroy` | boolean | Não | Indica se o workflow destroy.yml foi acionado. | Logs de disparo do destroy.yml. |
| `network_prefix` | string | Sim | Prefixo IPv6 /64 do visitante, quando aplicável. | Derivado do campo ip. |
| `browser_family` | string | Sim | Família do navegador, bot ou ferramenta. | Derivado do user_agent. Nulo se user_agent ausente. |
| `device_type` | string | Sim | Tipo provável de dispositivo. | Derivado do user_agent. Nulo se user_agent ausente. |
| `visitor_type` | string | Sim | Tipo analítico do visitante. | Derivado de user_agent e status_confianca. Nulo se ambos ausentes. |
| `cf_pop` | string | Sim | Ponto de presença Cloudflare. | Derivado do sufixo de cf_ray. |
| `visitor_id` | string | Sim | Assinatura técnica aproximada para recorrência provável. | Hash de prefixo/IP, navegador, dispositivo, idioma, país e POP. Nulo se campos base ausentes. |
| `parse_status` | string | Não | Resultado do parser. | Gerado no processo Bronze → Silver. |
| `parse_error` | string | Sim | Erro de parsing, se houver. | Exceção capturada pelo parser. |

## Domínios Controlados

### status_confianca

| Valor | Significado |
|---|---|
| `confiavel` | Acesso aprovado pelas regras atuais. |
| `rejeitado` | Acesso descartado pelas regras da Lambda controle. |
| `desconhecido` | Não houve decisão explícita de confiança no bloco. |
| `nao_aplicavel` | Execução operacional (ex: EventBridge) sem visitante HTTP. |

### parse_status

| Valor | Significado |
|---|---|
| `success` | Registro processado com sucesso. |
| `error` | Registro não processado por erro de parsing. |

### visitor_type

| Valor | Significado |
|---|---|
| `humano_provavel` | Acesso com características compatíveis com navegação humana. |
| `bot` | Acesso identificado como bot, crawler ou preview. |
| `suspeito` | Acesso rejeitado ou com sinais de risco. |

### browser_family (exemplos não exaustivos)

| Valor | Significado |
|---|---|
| `Chrome` | Google Chrome ou derivados. |
| `Firefox` | Mozilla Firefox. |
| `Safari` | Apple Safari. |
| `curl` | Requisição via curl. |
| `Python Requests` | Biblioteca requests do Python. |
| `Googlebot` | Crawler do Google. |
| `unknown` | User-agent ausente ou não reconhecido. |

### device_type

| Valor | Significado |
|---|---|
| `desktop` | Acesso por computador. |
| `mobile` | Acesso por dispositivo móvel. |
| `tablet` | Acesso por tablet. |
| `bot` | Ferramenta automatizada ou crawler. |
| `unknown` | Não identificado. |

## Observações

- `request_id` é a chave primária técnica da Silver.
- `api_gateway_request_id` identifica a requisição HTTP e não substitui `request_id`.
- `block_text` é usado internamente no parser, mas removido antes da gravação final.
- `timestamp_bsb` não está sendo gravado na Silver atual; incluir em `extract_events.py` se necessário.
- `carregou_site` e `acordou_ambiente` ainda não são gerados; devem entrar via `classify_events.py` futuramente.
- `visitor_id` não identifica pessoa real; é assinatura técnica para análise de recorrência.
- Campos derivados de `user_agent` (`browser_family`, `device_type`, `visitor_type`, `visitor_id`) são nulos quando o UA está ausente.

## Funil Operacional para a Gold

```text
is_http_event
      ↓
status_confianca = confiavel
      ↓
ambiente_ativo
      ↓
disparou_apply
      ↓
disparou_destroy
```
