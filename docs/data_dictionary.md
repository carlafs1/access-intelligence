# Dicionário de Dados — Access Intelligence

## Visão Geral da Linhagem

Este dicionário documenta os atributos em suas três camadas (arquitetura Lakehouse / Medalhão), validado contra a implementação real dos scripts de coleta e processamento.

```text
Bronze (ingestão crua)
   |  16 campos copiados sem transformação do CloudWatch Logs
   v
Silver (estruturado e enriquecido)
   |  67 campos persistidos, produzidos por 5 scripts
   |  + 4 campos de processamento interno (não persistem, mas alimentam a derivação)
   v
Gold (visão analítica)
   |  37 campos promovidos da Silver — métricas e indicadores prontos para consumo
   v
Analytics / ML / Assistente Generativo
```

> **Status**: Bronze está implementada e estável. A Silver está implementada, mas ainda em refinamento/teste — campos e regras de classificação podem mudar conforme o pipeline evolui (8 pontos já identificados, ver seção dedicada). A camada Gold ainda está em desenvolvimento — os campos descritos na seção Gold representam o destino analítico planejado, não código já implementado.

---

## Camada Bronze

Cópia fiel do evento do CloudWatch Logs, sem nenhuma interpretação. Toda a inteligência (parsing, classificação, enriquecimento) acontece a partir daqui, na Silver.

*Fonte: `scripts/collect_cloudwatch_bronze.py` (`BRONZE_COLUMNS`).*

| Campo | Motivo | Descrição |
|---|---|---|
| `event_id` | Deduplicação | ID único do evento no CloudWatch, atribuído pela própria AWS |
| `timestamp_ms` | Valor bruto AWS | Timestamp do evento em milissegundos, na precisão original da AWS |
| `timestamp_utc` | Linha do tempo | Timestamp do evento convertido para ISO 8601 UTC |
| `ingestion_time_ms` | Valor bruto AWS | Timestamp de quando o CloudWatch recebeu o log, em milissegundos |
| `ingestion_time` | Auditoria CloudWatch | Timestamp de ingestão no CloudWatch, convertido para ISO 8601 UTC |
| `log_group` | Origem | Log group de origem (sempre /aws/lambda/website-s3-iac-cv-controle) |
| `log_stream` | Reconstrução START/END — agrupar mensagens da mesma instância da Lambda e ajudar na reconstrução cronológica | Log stream específico — identifica a instância/execução do container Lambda |
| `message` | Log bruto completo | Mensagem bruta completa da Lambda (JSON estruturado ou texto livre) |
| `message_size` | Qualidade/auditoria | Tamanho em bytes de message — detecção de anomalias |
| `source_service` | cloudwatch_logs | Origem do log no pipeline (hoje sempre cloudwatch_logs) |
| `aws_region` | Região da coleta | Região AWS onde a coleta foi executada |
| `collected_at_utc` | Quando coletamos | Momento exato em que esta coleta específica foi executada |
| `collection_id` | Lote de coleta | Identificador único da execução de coleta (AAAAMMDDTHHMMSSZ) |
| `collection_type` | Distinguir coleta automática de reprocessamento manual (auditoria do pipeline) | incremental ou manual_reprocess |
| `collection_window_start` | Janela coletada | Início da janela de tempo coberta pela coleta |
| `collection_window_end` | Janela coletada | Fim da janela de tempo coberta pela coleta |

---

## Camada Silver (em refinamento/teste)

Campos estruturados, classificados e enriquecidos a partir da Bronze, organizados por script de origem. Pipeline implementado e funcional, mas ainda passando por ajustes de regras, nomenclatura e cobertura de casos.

### `reconstruct_blocks.py`

| Campo | Bloco | O que significa | Vai para Gold? |
|---|---|---|---|
| `request_id` | Requisição HTTP | ID único da execução da Lambda | Sim |
| `log_group` | — | Preservado da Bronze para rastreabilidade | Não |
| `log_stream` | — | Preservado da Bronze para rastreabilidade | Não |
| `block_closed` | Operacional do Ambiente | Se a execução teve START e END capturados | Não |

### `extract_events.py`

| Campo | Bloco | O que significa | Vai para Gold? |
|---|---|---|---|
| `timestamp_utc` | Requisição HTTP | Hora exata do acesso | Sim |
| `method` | Requisição HTTP | Método HTTP usado | Sim |
| `raw_path` | Requisição HTTP | Caminho exato acessado | Sim |
| `raw_query_string` | Requisição HTTP | Parâmetros da URL, sem processamento | Sim |
| `host` | Requisição HTTP | Domínio chamado na requisição | Sim |
| `ip` | Rede e Geolocalização | IP real do visitante, definido pela Cloudflare na borda | Sim |
| `pais_cf` | Rede e Geolocalização | País estimado pela Cloudflare | Sim |
| `cf_ray` | Rede e Geolocalização | ID único da requisição na Cloudflare, com POP embutido | Sim |
| `x_forwarded_for` | Rede e Geolocalização | Cadeia de IPs declarada no header | Sim |
| `source_ip_cloudflare` | Rede e Geolocalização | IP de origem da conexão TCP real que chegou ao API Gateway | Não |
| `user_agent` | Navegador e Comportamento | Identidade declarada do cliente (software) | Sim |
| `referer` | Navegador e Comportamento | De onde o visitante veio (canal de origem) | Sim |
| `accept_language` | Navegador e Comportamento | Idioma preferido do cliente | Sim |
| `accept` | Navegador e Comportamento | Tipos de conteúdo aceitos pelo cliente | Não |
| `accept_encoding` | Navegador e Comportamento | Compressão aceita pelo cliente | Não |
| `range` | Navegador e Comportamento | Solicitação de parte do conteúdo | Sim |
| `sec_ch_ua` | Navegador e Comportamento | Marca/versão do navegador | Não |
| `sec_ch_ua_mobile` | Navegador e Comportamento | Mobile ou não | Não |
| `sec_ch_ua_platform` | Navegador e Comportamento | Plataforma/SO declarado | Não |
| `sec_fetch_dest` | Navegador e Comportamento | O que está sendo buscado (document, image, etc.) | Não |
| `sec_fetch_mode` | Navegador e Comportamento | Tipo de navegação (navigate, no-cors, etc.) | Não |
| `sec_fetch_site` | Navegador e Comportamento | Relação da requisição com a origem | Sim |
| `sec_fetch_user` | Navegador e Comportamento | Se houve ação explícita do usuário | Sim |
| `is_http_event` | Operacional do Ambiente | Se o evento tem estrutura de requisição HTTP | Não |
| `parse_status` | Operacional do Ambiente | Status de extração daquela linha de log | Não |
| `parse_error` | Operacional do Ambiente | Detalhe do erro de leitura | Não |

### `classify_events.py`

| Campo | Bloco | O que significa | Vai para Gold? |
|---|---|---|---|
| `origem_eventbridge` | Operacional do Ambiente | Se a execução foi disparada por usuário ou por agenda (EventBridge) | Não |
| `ambiente_ativo` | — | True se havia ambiente S3 ativo no momento da execução | Não |
| `bucket_name` | Operacional do Ambiente | Nome do bucket S3 daquele ciclo de vida | Sim |
| `status_confianca` | Segurança e Confiança | Resultado da decisão do classificador online | Sim |
| `motivo_confianca` | Segurança e Confiança | Motivo específico da aceitação/rejeição | Sim |
| `disparou_apply` | Operacional do Ambiente | Se essa execução criou um novo ambiente | Sim |
| `disparou_destroy` | Operacional do Ambiente | Se essa execução destruiu o ambiente | Sim |
| `acordou_ambiente` | — | True se o acesso iniciou a criação do ambiente efêmero | Não |
| `aguardando_criacao` | Operacional do Ambiente | Se a requisição caiu na página de espera | Não |
| `carregou_site` | Operacional do Ambiente | Se o conteúdo foi efetivamente entregue | Não |
| `estado_resposta` | Operacional do Ambiente | Resultado final de como a Lambda processou o evento | Não |
| `manter_para_analise` | Operacional do Ambiente | Se a linha deve seguir para a próxima etapa | Não |
| `motivo_analise` | Operacional do Ambiente | Explica por que a linha foi retida ou descartada | Não |

### `enrich_visitors.py`

| Campo | Bloco | O que significa | Vai para Gold? |
|---|---|---|---|
| `network_prefix` | Rede e Geolocalização | Prefixo de rede, agrupando variação de IPv6 | Sim |
| `browser_family` | Navegador e Comportamento | Família do cliente, simplificada | Sim |
| `device_type` | Navegador e Comportamento | Tipo de dispositivo | Sim |
| `is_scanner_user_agent` | Navegador e Comportamento | Se o UA é de scanner conhecido | Sim |
| `is_social_preview` | Navegador e Comportamento | Se é preview de app social | Sim |
| `visitor_type` | Identidade do Visitante | Tradução amigável e categórica de motivo_confianca | Sim |
| `visitor_id` | Identidade do Visitante | Assinatura aproximada do visitante. | Sim |
| `site_session_id` | Identidade do Visitante | Identifica o ciclo completo do ambiente efêmero (bucket), desde o primeiro apply/start até o destroy, agrupando todos os eventos e visitantes que passaram por aquele mesmo ambiente. | Sim |

### `enrich_geoip.py`

| Campo | Bloco | O que significa | Vai para Gold? |
|---|---|---|---|
| `geo_country_code` | Rede e Geolocalização | País do IP segundo a base de GeoIP | Sim |
| `geo_country_name` | Rede e Geolocalização | Nome do país | Não |
| `geo_city` | Rede e Geolocalização | Cidade aproximada | Sim |
| `geo_region` | Rede e Geolocalização | Estado/região aproximada | Sim |
| `geo_postal_code` | Rede e Geolocalização | CEP aproximado | Não |
| `geo_latitude` | Rede e Geolocalização | Latitude aproximada do bloco de IP | Não |
| `geo_longitude` | Rede e Geolocalização | Longitude aproximada do bloco de IP | Não |
| `geo_timezone` | Rede e Geolocalização | Fuso horário provável | Não |
| `geo_asn` | Rede e Geolocalização | Identificador técnico da rede | Sim |
| `geo_provider` | Rede e Geolocalização | Nome legível do provedor | Sim |
| `is_cloud_provider` | Rede e Geolocalização | Indica provedor cloud (AWS, Azure, GCP etc.) | Sim |
| `is_known_scanner_network` | Rede e Geolocalização | Rede conhecida de scanner | Sim |
| `network_type` | Rede e Geolocalização | Classificação do tipo de rede | Sim |
| `geo_distance_km` | — | Distância (Haversine) entre coordenadas MaxMind e IPinfo | Não |
| `geo_divergence_severity` | — | baixa | media | alta | Não |
| `geo_sources_divergent` | — | True quando severity == alta | Não |

---

## Campos de Processamento Interno (não persistem)

Existem só durante o pipeline (dedup, insumo de derivação) e nunca viram coluna final.

| Script | Campo | Papel |
|---|---|---|
| `reconstruct_blocks.py` | `start_ts` | Datetime do evento START; consumido por extract_events.py para montar timestamp_utc |
| `reconstruct_blocks.py` | `end_ts` | Datetime do evento END (None se bloco incompleto); não persiste |
| `reconstruct_blocks.py` | `block_text` | Texto completo do bloco; necessário para classify_events.py, removido antes da gravação final |
| `enrich_geoip.py` | `geo_org` | Organização dona do IP |

---

## Camada Gold (planejada)

Subconjunto de 37 campos da Silver com destino definido para a camada analítica. **Estes campos ainda não têm pipeline de Gold implementado** — listados aqui para documentar a intenção de uso.

### Originados em `reconstruct_blocks.py`

| Campo | Utilidade prática | Como usar para validar acessos |
|---|---|---|
| `request_id` | Chave primária da Silver/Gold — agrupa todas as mensagens da mesma execução | Reconstruir START → END |

### Originados em `extract_events.py`

| Campo | Utilidade prática | Como usar para validar acessos |
|---|---|---|
| `timestamp_utc` | Montar linha do tempo de todo o sistema | Medir refresh, deploy, destroy e sessões |
| `method` | Saber a ação solicitada | GET é esperado; POST, PUT, DELETE seriam suspeitos |
| `raw_path` | Detectar tentativa de exploração | Paths como .env, .git, wp-admin indicam scanner |
| `raw_query_string` | Identificar origem de campanha; detectar tentativa de injeção via querystring | Parâmetros estranhos podem indicar exploração |
| `host` | Verificação de integridade pós-bloqueio do endpoint default | Qualquer valor diferente de carlasampaio.com.br é forte indício de falha na blindagem |
| `ip` | Identificar origem, agrupar, consultar GeoIP | Base de todo agrupamento de rede e geolocalização |
| `pais_cf` | Localização rápida, comparação cruzada com geo_country_code | Detectar países incomuns |
| `cf_ray` | Rastreabilidade e correlação de refreshes/retries | Correlacionar requisições da mesma sessão de borda |
| `x_forwarded_for` | Detector de tentativa de injeção/manipulação de header, não fonte primária de IP | Mais de um IP ou primeiro valor diferente de 'ip' sugere scanner testando bypass de controle por IP |
| `user_agent` | Base para classificação de bots, navegadores e scanners | WhatsApp, Chrome, Safari, curl, Palo Alto etc. |
| `referer` | Origem do acesso, distinto de quem é o cliente (user_agent) | Ver WhatsApp, LinkedIn, refresh interno |
| `accept_language` | Forte sinal comportamental humano | Ausente em bots/curl; avaliar exceções de VPN/proxy |
| `range` | Identificar preview social | WhatsApp usa range para gerar prévia |
| `sec_fetch_site` | Entender se é navegação nova ou refresh interno | same-origin em refresh; cross-site vindo de fora |
| `sec_fetch_user` | Diferenciar clique humano de automação | Sinal humano forte |

### Originados em `classify_events.py`

| Campo | Utilidade prática | Como usar para validar acessos |
|---|---|---|
| `bucket_name` | Ligar acesso ao ambiente físico, complementar ao site_session_id | Agrupar todos os eventos de um mesmo recurso físico |
| `status_confianca` | Separar acessos aceitos e rejeitados | Base de toda a validação de acesso |
| `motivo_confianca` | Explicabilidade da decisão; base de visitor_type | Entender qual regra foi acionada |
| `disparou_apply` | Medir quantos ambientes foram de fato provisionados | Primeiro acesso humano real, sem ambiente já ativo |
| `disparou_destroy` | Medir encerramentos por timeout | Validar que o timeout foi respeitado |

### Originados em `enrich_visitors.py`

| Campo | Utilidade prática | Como usar para validar acessos |
|---|---|---|
| `network_prefix` | Unir IPv6 variável da mesma rede/dispositivo | Identificar mesma rede mesmo com IPv6 mudando por sessão |
| `browser_family` | Agrupar navegadores e apps em dashboards | Leitura direta sem reprocessar headers brutos |
| `device_type` | Entender padrão de uso | Celular x desktop |
| `is_scanner_user_agent` | Sinal de segurança central | curl, zgrab, Palo Alto, python etc. |
| `is_social_preview` | Evitar contar como visualização real / evitar acordar ambiente | WhatsApp/LinkedIn/Facebook não deveriam acordar o ambiente |
| `visitor_type` | Segmentação rápida em dashboard | Separar humanos/bots/previews/crawlers sem repetir a lógica de motivo_confianca a cada consulta |
| `visitor_id` | Seguir recorrência entre acessos, mesmo em ambientes/buckets diferentes | Base para detectar visitante recorrente (via GROUP BY na Gold) |
| `site_session_id` | Permite calcular duração e custo do ciclo completo, atravessando múltiplas execuções Lambda | Cruzar com disparou_apply, disparou_destroy e refresh_count |

### Originados em `enrich_geoip.py`

| Campo | Utilidade prática | Como usar para validar acessos |
|---|---|---|
| `geo_country_code` | Validação cruzada com pais_cf | Comparar com pais_cf |
| `geo_city` | Análise geográfica | Validar se o acesso parece esperado |
| `geo_region` | Investigar origem geográfica | Detectar mudanças improváveis de região |
| `geo_asn` | Agrupar provedores, detectar redes recorrentes | Chave técnica compacta para agregação |
| `geo_provider` | Leitura humana imediata em relatórios | Saber se é residencial, cloud, etc. |
| `is_cloud_provider` | Sinal de segurança e de humanidade (entra nos dois scores) | Acesso cloud pode ser scanner, bot, teste ou VPN corporativa |
| `is_known_scanner_network` | Sinal de segurança forte | Palo Alto, Censys, Shodan etc. |
| `network_type` | Classificação analítica central | Cloud tende a bot/teste; residencial tende a humano |

---

## Em Refinamento

Este dicionário e a linhagem dos campos estão em evolução ativa junto com o código. Pontos já identificados e ainda não fechados:

| Campo | Situação |
|---|---|
| `log_group` | Adicionar à planilha: bloco sugerido 'Operacional do Ambiente' |
| `log_stream` | Adicionar à planilha: bloco sugerido 'Operacional do Ambiente' |
| `ambiente_ativo` | Candidato a insumo intermediário, mesma categoria de active_items/temp_item |
| `acordou_ambiente` | Candidato a insumo intermediário, mesma categoria de active_items/temp_item |
| `carregou_site` | Mesmo campo que 'serviu_site' na planilha original — conteúdo abaixo herdado dessa linha |
| `geo_distance_km` | Cross-validação com IPinfo — implementada, sem entrada na planilha original |
| `geo_divergence_severity` | Cross-validação com IPinfo — implementada, sem entrada na planilha original |
| `geo_sources_divergent` | Cross-validação com IPinfo — implementada, sem entrada na planilha original |

---

## Domínios e Scores

Como os campos da Silver alimentam os indicadores planejados para a Gold.

### `suspicion_score` (0 a 100, soma cumulativa, satura em 100)

Mede risco/ameaça.

| Sinal | Peso |
|---|---|
| is_known_scanner_network = true | 50 |
| is_scanner_user_agent = true | 40 |
| raw_path bate padrão de scanner (.env, .git, wp-admin etc.) | 30 |
| method != GET | 25 |
| host diferente de carlasampaio.com.br (canário) | 50 |
| is_cloud_provider = true (isolado, sem outros sinais) | 35 |
| accept_language e sec_fetch_* todos ausentes | 25 |
| is_social_preview = true | score travado em ≤10 (é esperado, não ameaça) |

### `human_probability` (0 a 1, base neutra 0.5, satura entre 0 e 1)

Mede a probabilidade de tráfego humano.

| Sinal | Efeito |
|---|---|
| is_scanner_user_agent = true ou is_known_scanner_network = true | probabilidade = 0 (força, sem cálculo) |
| is_social_preview = true | probabilidade = 0.05 (força, sem cálculo) |
| accept_language presente | +0.20 |
| sec_fetch_user = ?1 | +0.15 |
| network_type = residencial | +0.15 |
| browser_family é navegador real (Chrome/Safari/Firefox) | +0.20 |
| is_cloud_provider = true (e não é preview/scanner já capturado) | −0.30 |
| accept_language ausente | −0.20 |
| sec_fetch_* totalmente ausentes (sinal de script simples) | −0.15 |

### `visitor_type` × `motivo_confianca`

| motivo_confianca | visitor_type |
|---|---|
| `allowed_search_bot` | `crawler_buscador` |
| `social_preview_bot` | `social_preview` |
| `scanner_agent` | `scanner` |
| `suspicious_path` | `scanner` |
| `non_get_method` | `scanner` |
| `no_user_agent` | `bot_generico` |
| `missing_accept_header` | `bot_generico` |
| `missing_accept_language_allowed` | `humano_provavel` |
| `all_checks_passed` | `humano_provavel` |

### `sec-fetch-site`

| Valor | Significado | Exemplo |
|---|---|---|
| `none` | Navegação direta, sem página de origem | URL digitada, favorito, histórico, nova aba |
| `same-origin` | Mesma origem | Página do seu site → outra página do seu site |
| `same-site` | Mesmo site, origens diferentes | blog.exemplo.com → www.exemplo.com |
| `cross-site` | Outro site | LinkedIn → seu portfólio |
---

## Metadados do Dataset

| Item | Valor |
|---|---|
| Total de campos implementados | 83 (Bronze + Silver) |
| Campos planejados para Gold | 37 |
| Campos de processamento interno | 4 |
| Em refinamento | 8 |
| Fonte | `scripts/` (código real) + planilha de design do projeto |
| Versão | 3.1 |
