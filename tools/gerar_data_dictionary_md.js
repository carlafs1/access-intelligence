const fs = require("fs");

fs.mkdirSync("docs", { recursive: true });

// ===================================================================================
// Dicionário de Dados - Access Intelligence
//
// Gerado a partir da validação do código real: leitura linha a linha de
// collect_cloudwatch_bronze.py (Bronze) e dos 5 scripts da Silver
// (reconstruct_blocks, extract_events, classify_events, enrich_visitors,
// enrich_geoip), cruzados com a planilha de design do projeto.
//
// A camada Gold e a seção de Domínios vêm da planilha original (abas Gold
// e Domínios), que documentam o destino analítico desejado para os dados.
// ===================================================================================

const bronzeFields = [
  {
    "campo": "event_id",
    "motivo": "Deduplicação",
    "descricao": "ID único do evento no CloudWatch, atribuído pela própria AWS",
    "ja_documentado": "Sim",
    "obs": ""
  },
  {
    "campo": "timestamp_ms",
    "motivo": "Valor bruto AWS",
    "descricao": "Timestamp do evento em milissegundos, na precisão original da AWS",
    "ja_documentado": "Sim",
    "obs": ""
  },
  {
    "campo": "timestamp_utc",
    "motivo": "Linha do tempo",
    "descricao": "Timestamp do evento convertido para ISO 8601 UTC",
    "ja_documentado": "Sim",
    "obs": ""
  },
  {
    "campo": "ingestion_time_ms",
    "motivo": "Valor bruto AWS",
    "descricao": "Timestamp de quando o CloudWatch recebeu o log, em milissegundos",
    "ja_documentado": "Sim",
    "obs": ""
  },
  {
    "campo": "ingestion_time",
    "motivo": "Auditoria CloudWatch",
    "descricao": "Timestamp de ingestão no CloudWatch, convertido para ISO 8601 UTC",
    "ja_documentado": "Sim",
    "obs": ""
  },
  {
    "campo": "log_group",
    "motivo": "Origem",
    "descricao": "Log group de origem (sempre /aws/lambda/website-s3-iac-cv-controle)",
    "ja_documentado": "Sim",
    "obs": ""
  },
  {
    "campo": "log_stream",
    "motivo": "Reconstrução START/END — agrupar mensagens da mesma instância da Lambda e ajudar na reconstrução cronológica",
    "descricao": "Log stream específico — identifica a instância/execução do container Lambda",
    "ja_documentado": "Sim",
    "obs": ""
  },
  {
    "campo": "message",
    "motivo": "Log bruto completo",
    "descricao": "Mensagem bruta completa da Lambda (JSON estruturado ou texto livre)",
    "ja_documentado": "Sim",
    "obs": ""
  },
  {
    "campo": "message_size",
    "motivo": "Qualidade/auditoria",
    "descricao": "Tamanho em bytes de message — detecção de anomalias",
    "ja_documentado": "Sim",
    "obs": ""
  },
  {
    "campo": "source_service",
    "motivo": "cloudwatch_logs",
    "descricao": "Origem do log no pipeline (hoje sempre cloudwatch_logs)",
    "ja_documentado": "Sim",
    "obs": ""
  },
  {
    "campo": "aws_region",
    "motivo": "Região da coleta",
    "descricao": "Região AWS onde a coleta foi executada",
    "ja_documentado": "Sim",
    "obs": ""
  },
  {
    "campo": "collected_at_utc",
    "motivo": "Quando coletamos",
    "descricao": "Momento exato em que esta coleta específica foi executada",
    "ja_documentado": "Sim",
    "obs": ""
  },
  {
    "campo": "collection_id",
    "motivo": "Lote de coleta",
    "descricao": "Identificador único da execução de coleta (AAAAMMDDTHHMMSSZ)",
    "ja_documentado": "Sim",
    "obs": ""
  },
  {
    "campo": "collection_type",
    "motivo": "Distinguir coleta automática de reprocessamento manual (auditoria do pipeline)",
    "descricao": "incremental ou manual_reprocess",
    "ja_documentado": "Não",
    "obs": "Planilha original tinha 'pipeline_stage' nessa posição — não existe em BRONZE_COLUMNS no código atual"
  },
  {
    "campo": "collection_window_start",
    "motivo": "Janela coletada",
    "descricao": "Início da janela de tempo coberta pela coleta",
    "ja_documentado": "Sim",
    "obs": ""
  },
  {
    "campo": "collection_window_end",
    "motivo": "Janela coletada",
    "descricao": "Fim da janela de tempo coberta pela coleta",
    "ja_documentado": "Sim",
    "obs": ""
  }
];

const silverFields = [
  {
    "script": "reconstruct_blocks.py",
    "campo": "request_id",
    "bloco": "Requisição HTTP",
    "exemplo": "5e09a1e7-81b9-41b9-8c78-10fb7a7d5056",
    "origem": "Linha START RequestId",
    "obrigatorio": "Sempre",
    "significado": "ID único da execução da Lambda",
    "utilidade": "Chave primária da Silver/Gold — agrupa todas as mensagens da mesma execução",
    "como_validar": "Reconstruir START → END",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": true
  },
  {
    "script": "reconstruct_blocks.py",
    "campo": "log_group",
    "bloco": "",
    "exemplo": "",
    "origem": "",
    "obrigatorio": "",
    "significado": "Preservado da Bronze para rastreabilidade",
    "utilidade": "",
    "como_validar": "",
    "persiste": "Sim",
    "status": "Não documentado na planilha (estava em 'Excluídas', desatualizado — confirmado fundamental)",
    "obs": "Adicionar à planilha: bloco sugerido 'Operacional do Ambiente'",
    "promovido_gold": false
  },
  {
    "script": "reconstruct_blocks.py",
    "campo": "log_stream",
    "bloco": "",
    "exemplo": "",
    "origem": "",
    "obrigatorio": "",
    "significado": "Preservado da Bronze para rastreabilidade",
    "utilidade": "",
    "como_validar": "",
    "persiste": "Sim",
    "status": "Não documentado na planilha (estava em 'Excluídas', desatualizado — confirmado fundamental)",
    "obs": "Adicionar à planilha: bloco sugerido 'Operacional do Ambiente'",
    "promovido_gold": false
  },
  {
    "script": "reconstruct_blocks.py",
    "campo": "block_closed",
    "bloco": "Operacional do Ambiente",
    "exemplo": "true",
    "origem": "Reconstrução START/END",
    "obrigatorio": "Derivado",
    "significado": "Se a execução teve START e END capturados",
    "utilidade": "Garantir que o bloco de log está completo antes de analisar",
    "como_validar": "Evitar analisar execução incompleta",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": false
  },
  {
    "script": "reconstruct_blocks.py",
    "campo": "start_ts",
    "bloco": "",
    "exemplo": "",
    "origem": "",
    "obrigatorio": "",
    "significado": "Datetime do evento START; consumido por extract_events.py para montar timestamp_utc",
    "utilidade": "",
    "como_validar": "",
    "persiste": "Não — insumo",
    "status": "Não persiste como coluna própria",
    "obs": "",
    "promovido_gold": false
  },
  {
    "script": "reconstruct_blocks.py",
    "campo": "end_ts",
    "bloco": "",
    "exemplo": "",
    "origem": "",
    "obrigatorio": "",
    "significado": "Datetime do evento END (None se bloco incompleto); não persiste",
    "utilidade": "",
    "como_validar": "",
    "persiste": "Não — insumo",
    "status": "Não persiste como coluna própria",
    "obs": "",
    "promovido_gold": false
  },
  {
    "script": "reconstruct_blocks.py",
    "campo": "block_text",
    "bloco": "",
    "exemplo": "",
    "origem": "",
    "obrigatorio": "",
    "significado": "Texto completo do bloco; necessário para classify_events.py, removido antes da gravação final",
    "utilidade": "",
    "como_validar": "",
    "persiste": "Não — insumo",
    "status": "Confere com a planilha (insumo)",
    "obs": "",
    "promovido_gold": false
  },
  {
    "script": "extract_events.py",
    "campo": "timestamp_utc",
    "bloco": "Requisição HTTP",
    "exemplo": "2026-06-17T14:20:42Z",
    "origem": "CloudWatch / timeEpoch",
    "obrigatorio": "Sempre",
    "significado": "Hora exata do acesso",
    "utilidade": "Montar linha do tempo de todo o sistema",
    "como_validar": "Medir refresh, deploy, destroy e sessões",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": true
  },
  {
    "script": "extract_events.py",
    "campo": "method",
    "bloco": "Requisição HTTP",
    "exemplo": "GET",
    "origem": "requestContext.http.method",
    "obrigatorio": "Sempre em HTTP",
    "significado": "Método HTTP usado",
    "utilidade": "Saber a ação solicitada",
    "como_validar": "GET é esperado; POST, PUT, DELETE seriam suspeitos",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": true
  },
  {
    "script": "extract_events.py",
    "campo": "raw_path",
    "bloco": "Requisição HTTP",
    "exemplo": "/, /.env, /wp-admin",
    "origem": "rawPath",
    "obrigatorio": "Sempre em HTTP",
    "significado": "Caminho exato acessado",
    "utilidade": "Detectar tentativa de exploração",
    "como_validar": "Paths como .env, .git, wp-admin indicam scanner",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": true
  },
  {
    "script": "extract_events.py",
    "campo": "raw_query_string",
    "bloco": "Requisição HTTP",
    "exemplo": "utm=linkedin",
    "origem": "rawQueryString",
    "obrigatorio": "Opcional",
    "significado": "Parâmetros da URL, sem processamento",
    "utilidade": "Identificar origem de campanha; detectar tentativa de injeção via querystring",
    "como_validar": "Parâmetros estranhos podem indicar exploração",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": true
  },
  {
    "script": "extract_events.py",
    "campo": "host",
    "bloco": "Requisição HTTP",
    "exemplo": "carlasampaio.com.br",
    "origem": "Header host",
    "obrigatorio": "Sempre em HTTP",
    "significado": "Domínio chamado na requisição",
    "utilidade": "Verificação de integridade pós-bloqueio do endpoint default",
    "como_validar": "Qualquer valor diferente de carlasampaio.com.br é forte indício de falha na blindagem",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": true
  },
  {
    "script": "extract_events.py",
    "campo": "ip",
    "bloco": "Rede e Geolocalização",
    "exemplo": "2804:14c:65a0:430b:...",
    "origem": "headers.cf-connecting-ip",
    "obrigatorio": "Quase sempre",
    "significado": "IP real do visitante, definido pela Cloudflare na borda",
    "utilidade": "Identificar origem, agrupar, consultar GeoIP",
    "como_validar": "Base de todo agrupamento de rede e geolocalização",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": true
  },
  {
    "script": "extract_events.py",
    "campo": "pais_cf",
    "bloco": "Rede e Geolocalização",
    "exemplo": "BR, US, TW",
    "origem": "headers.cf-ipcountry",
    "obrigatorio": "Quase sempre",
    "significado": "País estimado pela Cloudflare",
    "utilidade": "Localização rápida, comparação cruzada com geo_country_code",
    "como_validar": "Detectar países incomuns",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": true
  },
  {
    "script": "extract_events.py",
    "campo": "cf_ray",
    "bloco": "Rede e Geolocalização",
    "exemplo": "a0d2b24f6af1ecc9-MIA",
    "origem": "headers.cf-ray",
    "obrigatorio": "Sempre via Cloudflare",
    "significado": "ID único da requisição na Cloudflare, com POP embutido",
    "utilidade": "Rastreabilidade e correlação de refreshes/retries",
    "como_validar": "Correlacionar requisições da mesma sessão de borda",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": true
  },
  {
    "script": "extract_events.py",
    "campo": "x_forwarded_for",
    "bloco": "Rede e Geolocalização",
    "exemplo": "IP real, IP Cloudflare",
    "origem": "headers.x-forwarded-for",
    "obrigatorio": "Quase sempre",
    "significado": "Cadeia de IPs declarada no header",
    "utilidade": "Detector de tentativa de injeção/manipulação de header, não fonte primária de IP",
    "como_validar": "Mais de um IP ou primeiro valor diferente de 'ip' sugere scanner testando bypass de controle por IP",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": true
  },
  {
    "script": "extract_events.py",
    "campo": "source_ip_cloudflare",
    "bloco": "Rede e Geolocalização",
    "exemplo": "172.68.7.16",
    "origem": "requestContext.http.sourceIp",
    "obrigatorio": "Sempre em HTTP",
    "significado": "IP de origem da conexão TCP real que chegou ao API Gateway",
    "utilidade": "Evidência de integridade da cadeia Cloudflare → API Gateway, não falsificável por header",
    "como_validar": "Confirmar que está dentro das faixas de IP publicadas pela Cloudflare",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": false
  },
  {
    "script": "extract_events.py",
    "campo": "user_agent",
    "bloco": "Navegador e Comportamento",
    "exemplo": "Mozilla/5.0 ... Safari/604.1",
    "origem": "headers.user-agent",
    "obrigatorio": "Quase sempre",
    "significado": "Identidade declarada do cliente (software)",
    "utilidade": "Base para classificação de bots, navegadores e scanners",
    "como_validar": "WhatsApp, Chrome, Safari, curl, Palo Alto etc.",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": true
  },
  {
    "script": "extract_events.py",
    "campo": "referer",
    "bloco": "Navegador e Comportamento",
    "exemplo": "https://carlasampaio.com.br/",
    "origem": "headers.referer",
    "obrigatorio": "Opcional",
    "significado": "De onde o visitante veio (canal de origem)",
    "utilidade": "Origem do acesso, distinto de quem é o cliente (user_agent)",
    "como_validar": "Ver WhatsApp, LinkedIn, refresh interno",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": true
  },
  {
    "script": "extract_events.py",
    "campo": "accept_language",
    "bloco": "Navegador e Comportamento",
    "exemplo": "pt-BR,pt;q=0.9",
    "origem": "headers.accept-language",
    "obrigatorio": "Opcional",
    "significado": "Idioma preferido do cliente",
    "utilidade": "Forte sinal comportamental humano",
    "como_validar": "Ausente em bots/curl; avaliar exceções de VPN/proxy",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": true
  },
  {
    "script": "extract_events.py",
    "campo": "accept",
    "bloco": "Navegador e Comportamento",
    "exemplo": "text/html,...",
    "origem": "headers.accept",
    "obrigatorio": "Quase sempre",
    "significado": "Tipos de conteúdo aceitos pelo cliente",
    "utilidade": "Sinal auxiliar de navegador real; ausência pode indicar scanner",
    "como_validar": "Cruzar com accept_encoding e accept_language",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": false
  },
  {
    "script": "extract_events.py",
    "campo": "accept_encoding",
    "bloco": "Navegador e Comportamento",
    "exemplo": "gzip, br",
    "origem": "headers.accept-encoding",
    "obrigatorio": "Quase sempre",
    "significado": "Compressão aceita pelo cliente",
    "utilidade": "Sinal técnico auxiliar",
    "como_validar": "Ajuda a diferenciar cliente real de script simples",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": false
  },
  {
    "script": "extract_events.py",
    "campo": "range",
    "bloco": "Navegador e Comportamento",
    "exemplo": "bytes=0-307199",
    "origem": "headers.range",
    "obrigatorio": "Raramente",
    "significado": "Solicitação de parte do conteúdo",
    "utilidade": "Identificar preview social",
    "como_validar": "WhatsApp usa range para gerar prévia",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": true
  },
  {
    "script": "extract_events.py",
    "campo": "sec_ch_ua",
    "bloco": "Navegador e Comportamento",
    "exemplo": "\"Google Chrome\";v=\"149\"",
    "origem": "headers.sec-ch-ua",
    "obrigatorio": "Opcional",
    "significado": "Marca/versão do navegador",
    "utilidade": "Validação cruzada com user_agent",
    "como_validar": "Checar coerência com user_agent",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": false
  },
  {
    "script": "extract_events.py",
    "campo": "sec_ch_ua_mobile",
    "bloco": "Navegador e Comportamento",
    "exemplo": "?0",
    "origem": "headers.sec-ch-ua-mobile",
    "obrigatorio": "Opcional",
    "significado": "Mobile ou não",
    "utilidade": "Validação cruzada com device_type",
    "como_validar": "Comparar com device_type já derivado",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": false
  },
  {
    "script": "extract_events.py",
    "campo": "sec_ch_ua_platform",
    "bloco": "Navegador e Comportamento",
    "exemplo": "\"Windows\"",
    "origem": "headers.sec-ch-ua-platform",
    "obrigatorio": "Opcional",
    "significado": "Plataforma/SO declarado",
    "utilidade": "Validação cruzada com user_agent",
    "como_validar": "Incoerência pode indicar spoofing",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": false
  },
  {
    "script": "extract_events.py",
    "campo": "sec_fetch_dest",
    "bloco": "Navegador e Comportamento",
    "exemplo": "document",
    "origem": "headers.sec-fetch-dest",
    "obrigatorio": "Opcional",
    "significado": "O que está sendo buscado (document, image, etc.)",
    "utilidade": "Sinal humano para investigação pontual",
    "como_validar": "document indica abertura de página HTML",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": false
  },
  {
    "script": "extract_events.py",
    "campo": "sec_fetch_mode",
    "bloco": "Navegador e Comportamento",
    "exemplo": "navigate",
    "origem": "headers.sec-fetch-mode",
    "obrigatorio": "Opcional",
    "significado": "Tipo de navegação (navigate, no-cors, etc.)",
    "utilidade": "Sinal humano para investigação pontual",
    "como_validar": "navigate sugere abertura de página por ação do usuário",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": false
  },
  {
    "script": "extract_events.py",
    "campo": "sec_fetch_site",
    "bloco": "Navegador e Comportamento",
    "exemplo": "none, same-origin, cross-site",
    "origem": "headers.sec-fetch-site",
    "obrigatorio": "Opcional",
    "significado": "Relação da requisição com a origem",
    "utilidade": "Entender se é navegação nova ou refresh interno",
    "como_validar": "same-origin em refresh; cross-site vindo de fora",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": true
  },
  {
    "script": "extract_events.py",
    "campo": "sec_fetch_user",
    "bloco": "Navegador e Comportamento",
    "exemplo": "?1",
    "origem": "headers.sec-fetch-user",
    "obrigatorio": "Opcional",
    "significado": "Se houve ação explícita do usuário",
    "utilidade": "Diferenciar clique humano de automação",
    "como_validar": "Sinal humano forte",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": true
  },
  {
    "script": "extract_events.py",
    "campo": "is_http_event",
    "bloco": "Operacional do Ambiente",
    "exemplo": "true",
    "origem": "Extração evento recebido",
    "obrigatorio": "Derivado",
    "significado": "Se o evento tem estrutura de requisição HTTP",
    "utilidade": "Par de verificação de qualidade com origem_eventbridge",
    "como_validar": "Calculado de forma independente; divergência = anomalia estrutural genuína",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": false
  },
  {
    "script": "extract_events.py",
    "campo": "parse_status",
    "bloco": "Operacional do Ambiente",
    "exemplo": "ok, erro",
    "origem": "Parser Silver",
    "obrigatorio": "Derivado",
    "significado": "Status de extração daquela linha de log",
    "utilidade": "Base de confiança de toda a camada Silver",
    "como_validar": "Identificar logs que não foram parseados corretamente",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": false
  },
  {
    "script": "extract_events.py",
    "campo": "parse_error",
    "bloco": "Operacional do Ambiente",
    "exemplo": "json_decode_error",
    "origem": "Parser Silver",
    "obrigatorio": "Derivado",
    "significado": "Detalhe do erro de leitura",
    "utilidade": "Debug do parser",
    "como_validar": "Corrigir lógica de parsing",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": false
  },
  {
    "script": "classify_events.py",
    "campo": "origem_eventbridge",
    "bloco": "Operacional do Ambiente",
    "exemplo": "false",
    "origem": "lambda_execution.origem_eventbridge",
    "obrigatorio": "Sempre no evento",
    "significado": "Se a execução foi disparada por usuário ou por agenda (EventBridge)",
    "utilidade": "Par de verificação de qualidade com is_http_event",
    "como_validar": "Divergência entre os dois indica evento HTTP malformado",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": false
  },
  {
    "script": "classify_events.py",
    "campo": "ambiente_ativo",
    "bloco": "",
    "exemplo": "",
    "origem": "",
    "obrigatorio": "",
    "significado": "True se havia ambiente S3 ativo no momento da execução",
    "utilidade": "",
    "como_validar": "",
    "persiste": "Sim",
    "status": "Não documentado na planilha",
    "obs": "Candidato a insumo intermediário, mesma categoria de active_items/temp_item",
    "promovido_gold": false
  },
  {
    "script": "classify_events.py",
    "campo": "bucket_name",
    "bloco": "Operacional do Ambiente",
    "exemplo": "website-s3-iac-cv-efemero-386d4497",
    "origem": "DynamoDB / eventos",
    "obrigatorio": "Obrigatório a partir da criação",
    "significado": "Nome do bucket S3 daquele ciclo de vida",
    "utilidade": "Ligar acesso ao ambiente físico, complementar ao site_session_id",
    "como_validar": "Agrupar todos os eventos de um mesmo recurso físico",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": true
  },
  {
    "script": "classify_events.py",
    "campo": "status_confianca",
    "bloco": "Segurança e Confiança",
    "exemplo": "trusted, untrusted",
    "origem": "access_decision.trusted",
    "obrigatorio": "Derivado",
    "significado": "Resultado da decisão do classificador online",
    "utilidade": "Separar acessos aceitos e rejeitados",
    "como_validar": "Base de toda a validação de acesso",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": true
  },
  {
    "script": "classify_events.py",
    "campo": "motivo_confianca",
    "bloco": "Segurança e Confiança",
    "exemplo": "all_checks_passed",
    "origem": "access_decision.reason",
    "obrigatorio": "Derivado",
    "significado": "Motivo específico da aceitação/rejeição",
    "utilidade": "Explicabilidade da decisão; base de visitor_type",
    "como_validar": "Entender qual regra foi acionada",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": true
  },
  {
    "script": "classify_events.py",
    "campo": "disparou_apply",
    "bloco": "Operacional do Ambiente",
    "exemplo": "true",
    "origem": "deploy_triggered",
    "obrigatorio": "Derivado",
    "significado": "Se essa execução criou um novo ambiente",
    "utilidade": "Medir quantos ambientes foram de fato provisionados",
    "como_validar": "Primeiro acesso humano real, sem ambiente já ativo",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": true
  },
  {
    "script": "classify_events.py",
    "campo": "disparou_destroy",
    "bloco": "Operacional do Ambiente",
    "exemplo": "true",
    "origem": "destroy_triggered",
    "obrigatorio": "Derivado",
    "significado": "Se essa execução destruiu o ambiente",
    "utilidade": "Medir encerramentos por timeout",
    "como_validar": "Validar que o timeout foi respeitado",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": true
  },
  {
    "script": "classify_events.py",
    "campo": "acordou_ambiente",
    "bloco": "",
    "exemplo": "",
    "origem": "",
    "obrigatorio": "",
    "significado": "True se o acesso iniciou a criação do ambiente efêmero",
    "utilidade": "",
    "como_validar": "",
    "persiste": "Sim",
    "status": "Não documentado na planilha",
    "obs": "Candidato a insumo intermediário, mesma categoria de active_items/temp_item",
    "promovido_gold": false
  },
  {
    "script": "classify_events.py",
    "campo": "aguardando_criacao",
    "bloco": "Operacional do Ambiente",
    "exemplo": "true",
    "origem": "TEMPORARIO + HTTP",
    "obrigatorio": "Derivado",
    "significado": "Se a requisição caiu na página de espera",
    "utilidade": "Medir UX de espera durante provisionamento",
    "como_validar": "Contar refresh durante o provisionamento",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": false
  },
  {
    "script": "classify_events.py",
    "campo": "carregou_site",
    "bloco": "Operacional do Ambiente",
    "exemplo": "true",
    "origem": "Log 'Servindo site via proxy S3'",
    "obrigatorio": "Derivado",
    "significado": "Se o conteúdo foi efetivamente entregue",
    "utilidade": "Controle operacional de entrega",
    "como_validar": "Saber quem efetivamente viu o site",
    "persiste": "Sim",
    "status": "Nome divergente",
    "obs": "Mesmo campo que 'serviu_site' na planilha original — conteúdo abaixo herdado dessa linha",
    "promovido_gold": false
  },
  {
    "script": "classify_events.py",
    "campo": "estado_resposta",
    "bloco": "Operacional do Ambiente",
    "exemplo": "criou_ambiente, aguardando_criacao, site_servido",
    "origem": "Regras Silver",
    "obrigatorio": "Derivado",
    "significado": "Resultado final de como a Lambda processou o evento",
    "utilidade": "Principal leitura operacional para depuração do sistema",
    "como_validar": "Entender o que a Lambda fez naquela execução",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": false
  },
  {
    "script": "classify_events.py",
    "campo": "manter_para_analise",
    "bloco": "Operacional do Ambiente",
    "exemplo": "true",
    "origem": "Regras Silver",
    "obrigatorio": "Derivado",
    "significado": "Se a linha deve seguir para a próxima etapa",
    "utilidade": "Reduzir ruído (ex: remover refreshes)",
    "como_validar": "Decisão de filtro do próprio ETL",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": false
  },
  {
    "script": "classify_events.py",
    "campo": "motivo_analise",
    "bloco": "Operacional do Ambiente",
    "exemplo": "primeiro_acesso_disparou_apply",
    "origem": "Regras Silver",
    "obrigatorio": "Derivado",
    "significado": "Explica por que a linha foi retida ou descartada",
    "utilidade": "Auditoria das próprias decisões de filtro do pipeline",
    "como_validar": "Justificar por que uma linha específica foi mantida ou não",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": false
  },
  {
    "script": "enrich_visitors.py",
    "campo": "network_prefix",
    "bloco": "Rede e Geolocalização",
    "exemplo": "2804:14c:65a0:430b",
    "origem": "Derivado de ip",
    "obrigatorio": "Derivado",
    "significado": "Prefixo de rede, agrupando variação de IPv6",
    "utilidade": "Unir IPv6 variável da mesma rede/dispositivo",
    "como_validar": "Identificar mesma rede mesmo com IPv6 mudando por sessão",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": true
  },
  {
    "script": "enrich_visitors.py",
    "campo": "browser_family",
    "bloco": "Navegador e Comportamento",
    "exemplo": "Safari, Chrome, WhatsApp",
    "origem": "Derivado de user_agent",
    "obrigatorio": "Derivado",
    "significado": "Família do cliente, simplificada",
    "utilidade": "Agrupar navegadores e apps em dashboards",
    "como_validar": "Leitura direta sem reprocessar headers brutos",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": true
  },
  {
    "script": "enrich_visitors.py",
    "campo": "device_type",
    "bloco": "Navegador e Comportamento",
    "exemplo": "mobile, desktop",
    "origem": "Derivado de user_agent",
    "obrigatorio": "Derivado",
    "significado": "Tipo de dispositivo",
    "utilidade": "Entender padrão de uso",
    "como_validar": "Celular x desktop",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": true
  },
  {
    "script": "enrich_visitors.py",
    "campo": "is_scanner_user_agent",
    "bloco": "Navegador e Comportamento",
    "exemplo": "true",
    "origem": "Regras sobre UA",
    "obrigatorio": "Derivado",
    "significado": "Se o UA é de scanner conhecido",
    "utilidade": "Sinal de segurança central",
    "como_validar": "curl, zgrab, Palo Alto, python etc.",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": true
  },
  {
    "script": "enrich_visitors.py",
    "campo": "is_social_preview",
    "bloco": "Navegador e Comportamento",
    "exemplo": "true",
    "origem": "Regras sobre UA/range",
    "obrigatorio": "Derivado",
    "significado": "Se é preview de app social",
    "utilidade": "Evitar contar como visualização real / evitar acordar ambiente",
    "como_validar": "WhatsApp/LinkedIn/Facebook não deveriam acordar o ambiente",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": true
  },
  {
    "script": "enrich_visitors.py",
    "campo": "visitor_type",
    "bloco": "Identidade do Visitante",
    "exemplo": "humano_provavel, social_preview, scanner, crawler_buscador, bot_generico",
    "origem": "Mapeamento direto de motivo_confianca",
    "obrigatorio": "Derivado",
    "significado": "Tradução amigável e categórica de motivo_confianca",
    "utilidade": "Segmentação rápida em dashboard",
    "como_validar": "Separar humanos/bots/previews/crawlers sem repetir a lógica de motivo_confianca a cada consulta",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": true
  },
  {
    "script": "enrich_visitors.py",
    "campo": "visitor_id",
    "bloco": "Identidade do Visitante",
    "exemplo": "9a7862c8c741a2cd",
    "origem": "Hash de network_prefix + user_agent",
    "obrigatorio": "Derivado",
    "significado": "Assinatura aproximada do visitante.",
    "utilidade": "Seguir recorrência entre acessos, mesmo em ambientes/buckets diferentes",
    "como_validar": "Base para detectar visitante recorrente (via GROUP BY na Gold)",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": true
  },
  {
    "script": "enrich_visitors.py",
    "campo": "site_session_id",
    "bloco": "Identidade do Visitante",
    "exemplo": "9a7862c8c741a2cd",
    "origem": "Hash gerado a partir de um event_id semente, no momento da criação do item TEMPORARIO",
    "obrigatorio": "Derivado",
    "significado": "Identifica o ciclo completo do ambiente efêmero (bucket), desde o primeiro apply/start até o destroy, agrupando todos os eventos e visitantes que passaram por aquele mesmo ambiente.",
    "utilidade": "Permite calcular duração e custo do ciclo completo, atravessando múltiplas execuções Lambda",
    "como_validar": "Cruzar com disparou_apply, disparou_destroy e refresh_count",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": true
  },
  {
    "script": "enrich_geoip.py",
    "campo": "geo_country_code",
    "bloco": "Rede e Geolocalização",
    "exemplo": "BR",
    "origem": "GeoIP country",
    "obrigatorio": "Opcional",
    "significado": "País do IP segundo a base de GeoIP",
    "utilidade": "Validação cruzada com pais_cf",
    "como_validar": "Comparar com pais_cf",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": true
  },
  {
    "script": "enrich_geoip.py",
    "campo": "geo_country_name",
    "bloco": "Rede e Geolocalização",
    "exemplo": "Brazil",
    "origem": "GeoIP",
    "obrigatorio": "Opcional",
    "significado": "Nome do país",
    "utilidade": "Leitura amigável em relatórios internos",
    "como_validar": "Facilitar leitura, comparar com geo_country_code",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": false
  },
  {
    "script": "enrich_geoip.py",
    "campo": "geo_city",
    "bloco": "Rede e Geolocalização",
    "exemplo": "Brasília",
    "origem": "GeoIP city",
    "obrigatorio": "Opcional",
    "significado": "Cidade aproximada",
    "utilidade": "Análise geográfica",
    "como_validar": "Validar se o acesso parece esperado",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": true
  },
  {
    "script": "enrich_geoip.py",
    "campo": "geo_region",
    "bloco": "Rede e Geolocalização",
    "exemplo": "Federal District",
    "origem": "GeoIP region",
    "obrigatorio": "Opcional",
    "significado": "Estado/região aproximada",
    "utilidade": "Investigar origem geográfica",
    "como_validar": "Detectar mudanças improváveis de região",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": true
  },
  {
    "script": "enrich_geoip.py",
    "campo": "geo_postal_code",
    "bloco": "Rede e Geolocalização",
    "exemplo": "70000-000",
    "origem": "GeoIP postal",
    "obrigatorio": "Opcional",
    "significado": "CEP aproximado",
    "utilidade": "Baixa utilidade",
    "como_validar": "Pouco confiável para validação",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": false
  },
  {
    "script": "enrich_geoip.py",
    "campo": "geo_latitude",
    "bloco": "Rede e Geolocalização",
    "exemplo": "-15.7997",
    "origem": "GeoIP loc",
    "obrigatorio": "Opcional",
    "significado": "Latitude aproximada do bloco de IP",
    "utilidade": "Plotagem em mapas internos",
    "como_validar": "Calcular distância entre acessos",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": false
  },
  {
    "script": "enrich_geoip.py",
    "campo": "geo_longitude",
    "bloco": "Rede e Geolocalização",
    "exemplo": "-47.8645",
    "origem": "GeoIP loc",
    "obrigatorio": "Opcional",
    "significado": "Longitude aproximada do bloco de IP",
    "utilidade": "Plotagem em mapas internos",
    "como_validar": "Calcular distância entre acessos",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": false
  },
  {
    "script": "enrich_geoip.py",
    "campo": "geo_timezone",
    "bloco": "Rede e Geolocalização",
    "exemplo": "America/Sao_Paulo",
    "origem": "GeoIP timezone",
    "obrigatorio": "Opcional",
    "significado": "Fuso horário provável",
    "utilidade": "Detectar incoerência entre horário do acesso e fuso esperado",
    "como_validar": "Comparar timestamp_utc convertido com geo_timezone",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": false
  },
  {
    "script": "enrich_geoip.py",
    "campo": "geo_asn",
    "bloco": "Rede e Geolocalização",
    "exemplo": "AS28573",
    "origem": "Derivado de geo_org",
    "obrigatorio": "Derivado",
    "significado": "Identificador técnico da rede",
    "utilidade": "Agrupar provedores, detectar redes recorrentes",
    "como_validar": "Chave técnica compacta para agregação",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": true
  },
  {
    "script": "enrich_geoip.py",
    "campo": "geo_provider",
    "bloco": "Rede e Geolocalização",
    "exemplo": "Claro NXT Telecomunicacoes Ltda",
    "origem": "Derivado de geo_org",
    "obrigatorio": "Derivado",
    "significado": "Nome legível do provedor",
    "utilidade": "Leitura humana imediata em relatórios",
    "como_validar": "Saber se é residencial, cloud, etc.",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": true
  },
  {
    "script": "enrich_geoip.py",
    "campo": "is_cloud_provider",
    "bloco": "Rede e Geolocalização",
    "exemplo": "true",
    "origem": "Derivado de geo_provider",
    "obrigatorio": "Derivado",
    "significado": "Indica provedor cloud (AWS, Azure, GCP etc.)",
    "utilidade": "Sinal de segurança e de humanidade (entra nos dois scores)",
    "como_validar": "Acesso cloud pode ser scanner, bot, teste ou VPN corporativa",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": true
  },
  {
    "script": "enrich_geoip.py",
    "campo": "is_known_scanner_network",
    "bloco": "Rede e Geolocalização",
    "exemplo": "true",
    "origem": "Derivado de geo_org",
    "obrigatorio": "Derivado",
    "significado": "Rede conhecida de scanner",
    "utilidade": "Sinal de segurança forte",
    "como_validar": "Palo Alto, Censys, Shodan etc.",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": true
  },
  {
    "script": "enrich_geoip.py",
    "campo": "network_type",
    "bloco": "Rede e Geolocalização",
    "exemplo": "residencial, cloud, scanner",
    "origem": "Regras sobre geo_org",
    "obrigatorio": "Derivado",
    "significado": "Classificação do tipo de rede",
    "utilidade": "Classificação analítica central",
    "como_validar": "Cloud tende a bot/teste; residencial tende a humano",
    "persiste": "Sim",
    "status": "Confere com a planilha",
    "obs": "",
    "promovido_gold": true
  },
  {
    "script": "enrich_geoip.py",
    "campo": "geo_distance_km",
    "bloco": "",
    "exemplo": "",
    "origem": "",
    "obrigatorio": "",
    "significado": "Distância (Haversine) entre coordenadas MaxMind e IPinfo",
    "utilidade": "",
    "como_validar": "",
    "persiste": "Sim",
    "status": "Não documentado na planilha",
    "obs": "Cross-validação com IPinfo — implementada, sem entrada na planilha original",
    "promovido_gold": false
  },
  {
    "script": "enrich_geoip.py",
    "campo": "geo_divergence_severity",
    "bloco": "",
    "exemplo": "",
    "origem": "",
    "obrigatorio": "",
    "significado": "baixa | media | alta",
    "utilidade": "",
    "como_validar": "",
    "persiste": "Sim",
    "status": "Não documentado na planilha",
    "obs": "Cross-validação com IPinfo — implementada, sem entrada na planilha original",
    "promovido_gold": false
  },
  {
    "script": "enrich_geoip.py",
    "campo": "geo_sources_divergent",
    "bloco": "",
    "exemplo": "",
    "origem": "",
    "obrigatorio": "",
    "significado": "True quando severity == alta",
    "utilidade": "",
    "como_validar": "",
    "persiste": "Sim",
    "status": "Não documentado na planilha",
    "obs": "Cross-validação com IPinfo — implementada, sem entrada na planilha original",
    "promovido_gold": false
  },
  {
    "script": "enrich_geoip.py",
    "campo": "geo_org",
    "bloco": "Rede e Geolocalização",
    "exemplo": "AS28573 Claro NXT Telecomunicacoes Ltda",
    "origem": "GeoIP org",
    "obrigatorio": "Opcional",
    "significado": "Organização dona do IP",
    "utilidade": "Distinguir residencial, cloud e scanner",
    "como_validar": "Identificar AWS, Claro, Palo Alto etc.",
    "persiste": "Não — insumo",
    "status": "Confere com a planilha (insumo)",
    "obs": "",
    "promovido_gold": false
  }
];

const suspicionScoreSignals = [
  {
    "sinal": "is_known_scanner_network = true",
    "peso": 50
  },
  {
    "sinal": "is_scanner_user_agent = true",
    "peso": 40
  },
  {
    "sinal": "raw_path bate padrão de scanner (.env, .git, wp-admin etc.)",
    "peso": 30
  },
  {
    "sinal": "method != GET",
    "peso": 25
  },
  {
    "sinal": "host diferente de carlasampaio.com.br (canário)",
    "peso": 50
  },
  {
    "sinal": "is_cloud_provider = true (isolado, sem outros sinais)",
    "peso": 35
  },
  {
    "sinal": "accept_language e sec_fetch_* todos ausentes",
    "peso": 25
  },
  {
    "sinal": "is_social_preview = true",
    "peso": "score travado em ≤10 (é esperado, não ameaça)"
  }
];
const humanProbabilitySignals = [
  {
    "sinal": "is_scanner_user_agent = true ou is_known_scanner_network = true",
    "efeito": "probabilidade = 0 (força, sem cálculo)"
  },
  {
    "sinal": "is_social_preview = true",
    "efeito": "probabilidade = 0.05 (força, sem cálculo)"
  },
  {
    "sinal": "accept_language presente",
    "efeito": "+0.20"
  },
  {
    "sinal": "sec_fetch_user = ?1",
    "efeito": "+0.15"
  },
  {
    "sinal": "network_type = residencial",
    "efeito": "+0.15"
  },
  {
    "sinal": "browser_family é navegador real (Chrome/Safari/Firefox)",
    "efeito": "+0.20"
  },
  {
    "sinal": "is_cloud_provider = true (e não é preview/scanner já capturado)",
    "efeito": "−0.30"
  },
  {
    "sinal": "accept_language ausente",
    "efeito": "−0.20"
  },
  {
    "sinal": "sec_fetch_* totalmente ausentes (sinal de script simples)",
    "efeito": "−0.15"
  }
];
const visitorTypeMap = [
  {
    "motivo_confianca": "allowed_search_bot",
    "visitor_type": "crawler_buscador"
  },
  {
    "motivo_confianca": "social_preview_bot",
    "visitor_type": "social_preview"
  },
  {
    "motivo_confianca": "scanner_agent",
    "visitor_type": "scanner"
  },
  {
    "motivo_confianca": "suspicious_path",
    "visitor_type": "scanner"
  },
  {
    "motivo_confianca": "non_get_method",
    "visitor_type": "scanner"
  },
  {
    "motivo_confianca": "no_user_agent",
    "visitor_type": "bot_generico"
  },
  {
    "motivo_confianca": "missing_accept_header",
    "visitor_type": "bot_generico"
  },
  {
    "motivo_confianca": "missing_accept_language_allowed",
    "visitor_type": "humano_provavel"
  },
  {
    "motivo_confianca": "all_checks_passed",
    "visitor_type": "humano_provavel"
  }
];
const secFetchSiteDomain = [
  {
    "valor": "none",
    "significado": "Navegação direta, sem página de origem",
    "exemplo": "URL digitada, favorito, histórico, nova aba"
  },
  {
    "valor": "same-origin",
    "significado": "Mesma origem",
    "exemplo": "Página do seu site → outra página do seu site"
  },
  {
    "valor": "same-site",
    "significado": "Mesmo site, origens diferentes",
    "exemplo": "blog.exemplo.com → www.exemplo.com"
  },
  {
    "valor": "cross-site",
    "significado": "Outro site",
    "exemplo": "LinkedIn → seu portfólio"
  }
];

const FENCE = "```";
const silverPersistido = silverFields.filter(f => f.persiste === "Sim");
const silverInsumo = silverFields.filter(f => f.persiste !== "Sim");
const goldFields = silverPersistido.filter(f => f.promovido_gold);
const emRefinamento = silverFields.filter(f => f.status.includes("documentado") || f.status.includes("divergente"));
const totalCampos = bronzeFields.length + silverPersistido.length;

function agruparPorScript(fields) {
  const grupos = {};
  for (const f of fields) {
    grupos[f.script] = grupos[f.script] || [];
    grupos[f.script].push(f);
  }
  return grupos;
}

const blocosSilver = agruparPorScript(silverPersistido);
const blocosGold = agruparPorScript(goldFields);

let md = "# Dicionário de Dados — Access Intelligence\n\n";

md += "## Visão Geral da Linhagem\n\n";
md += "Este dicionário documenta os atributos em suas três camadas (arquitetura Lakehouse / Medalhão), validado contra a implementação real dos scripts de coleta e processamento.\n\n";

md += FENCE + "text\n";
md += "Bronze (ingestão crua)\n";
md += "   |  " + bronzeFields.length + " campos copiados sem transformação do CloudWatch Logs\n";
md += "   v\n";
md += "Silver (estruturado e enriquecido)\n";
md += "   |  " + silverPersistido.length + " campos persistidos, produzidos por " + Object.keys(blocosSilver).length + " scripts\n";
md += "   |  + " + silverInsumo.length + " campos de processamento interno (não persistem, mas alimentam a derivação)\n";
md += "   v\n";
md += "Gold (visão analítica)\n";
md += "   |  " + goldFields.length + " campos promovidos da Silver — métricas e indicadores prontos para consumo\n";
md += "   v\n";
md += "Analytics / ML / Assistente Generativo\n";
md += FENCE + "\n\n";

md += "> **Status**: Bronze está implementada e estável. A Silver está implementada, mas ainda em " +
      "refinamento/teste — campos e regras de classificação podem mudar conforme o pipeline evolui " +
      "(" + emRefinamento.length + " pontos já identificados, ver seção dedicada). A camada Gold ainda " +
      "está em desenvolvimento — os campos descritos na seção Gold representam o destino analítico " +
      "planejado, não código já implementado.\n\n";

md += "---\n\n## Camada Bronze\n\n";
md += "Cópia fiel do evento do CloudWatch Logs, sem nenhuma interpretação. Toda a inteligência (parsing, classificação, enriquecimento) acontece a partir daqui, na Silver.\n\n";
md += "*Fonte: `scripts/collect_cloudwatch_bronze.py` (`BRONZE_COLUMNS`).*\n\n";
md += "| Campo | Motivo | Descrição |\n|---|---|---|\n";
for (const b of bronzeFields) {
  md += "| `" + b.campo + "` | " + b.motivo + " | " + b.descricao + " |\n";
}

md += "\n---\n\n## Camada Silver (em refinamento/teste)\n\n";
md += "Campos estruturados, classificados e enriquecidos a partir da Bronze, organizados por script de origem. Pipeline implementado e funcional, mas ainda passando por ajustes de regras, nomenclatura e cobertura de casos.\n\n";

for (const [script, campos] of Object.entries(blocosSilver)) {
  md += "### `" + script + "`\n\n";
  md += "| Campo | Bloco | O que significa | Vai para Gold? |\n";
  md += "|---|---|---|---|\n";
  for (const c of campos) {
    md += "| `" + c.campo + "` | " + (c.bloco || "—") + " | " + c.significado + " | " + (c.promovido_gold ? "Sim" : "Não") + " |\n";
  }
  md += "\n";
}

md += "---\n\n## Campos de Processamento Interno (não persistem)\n\n";
md += "Existem só durante o pipeline (dedup, insumo de derivação) e nunca viram coluna final.\n\n";
md += "| Script | Campo | Papel |\n|---|---|---|\n";
for (const i of silverInsumo) {
  md += "| `" + i.script + "` | `" + i.campo + "` | " + i.significado + " |\n";
}

md += "\n---\n\n## Camada Gold (planejada)\n\n";
md += "Subconjunto de " + goldFields.length + " campos da Silver com destino definido para a camada analítica. " +
      "**Estes campos ainda não têm pipeline de Gold implementado** — listados aqui para documentar a intenção de uso.\n\n";
for (const [script, campos] of Object.entries(blocosGold)) {
  md += "### Originados em `" + script + "`\n\n";
  md += "| Campo | Utilidade prática | Como usar para validar acessos |\n|---|---|---|\n";
  for (const c of campos) {
    md += "| `" + c.campo + "` | " + (c.utilidade || "—") + " | " + (c.como_validar || "—") + " |\n";
  }
  md += "\n";
}

md += "---\n\n## Em Refinamento\n\n";
md += "Este dicionário e a linhagem dos campos estão em evolução ativa junto com o código. Pontos já identificados e ainda " +
      "não fechados:\n\n";
md += "| Campo | Situação |\n|---|---|\n";
for (const c of emRefinamento) {
  md += "| `" + c.campo + "` | " + c.obs + " |\n";
}

md += "\n---\n\n## Domínios e Scores\n\n";
md += "Como os campos da Silver alimentam os indicadores planejados para a Gold.\n\n";

md += "### `suspicion_score` (0 a 100, soma cumulativa, satura em 100)\n\n";
md += "Mede risco/ameaça.\n\n";
md += "| Sinal | Peso |\n|---|---|\n";
for (const s of suspicionScoreSignals) { md += "| " + s.sinal + " | " + s.peso + " |\n"; }

md += "\n### `human_probability` (0 a 1, base neutra 0.5, satura entre 0 e 1)\n\n";
md += "Mede a probabilidade de tráfego humano.\n\n";
md += "| Sinal | Efeito |\n|---|---|\n";
for (const h of humanProbabilitySignals) { md += "| " + h.sinal + " | " + h.efeito + " |\n"; }

md += "\n### `visitor_type` × `motivo_confianca`\n\n";
md += "| motivo_confianca | visitor_type |\n|---|---|\n";
for (const v of visitorTypeMap) { md += "| `" + v.motivo_confianca + "` | `" + v.visitor_type + "` |\n"; }

md += "\n### `sec-fetch-site`\n\n";
md += "| Valor | Significado | Exemplo |\n|---|---|---|\n";
for (const sf of secFetchSiteDomain) { md += "| `" + sf.valor + "` | " + sf.significado + " | " + sf.exemplo + " |\n"; }

md += "---\n\n## Metadados do Dataset\n\n";
md += "| Item | Valor |\n|---|---|\n";
md += "| Total de campos implementados | " + totalCampos + " (Bronze + Silver) |\n";
md += "| Campos planejados para Gold | " + goldFields.length + " |\n";
md += "| Campos de processamento interno | " + silverInsumo.length + " |\n";
md += "| Em refinamento | " + emRefinamento.length + " |\n";
md += "| Fonte | `scripts/` (código real) + planilha de design do projeto |\n";
md += "| Versão | 3.1 |\n";

fs.writeFileSync("docs/data_dictionary.md", md, "utf8");
console.log("Gerado: docs/data_dictionary.md");
console.log("Bronze: " + bronzeFields.length + " | Silver: " + silverPersistido.length + " | Insumo: " + silverInsumo.length + " | Gold: " + goldFields.length + " | Em refinamento: " + emRefinamento.length);