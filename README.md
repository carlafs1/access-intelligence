# Access Intelligence

O **Access Intelligence** é uma plataforma de observabilidade, análise comportamental e detecção de anomalias construída sobre os eventos operacionais gerados pelo projeto **website-s3-iac-cv**.

Seu objetivo é transformar logs de infraestrutura em inteligência operacional, permitindo compreender padrões de acesso, monitorar o comportamento do ambiente, identificar eventos incomuns e explorar aplicações de Inteligência Artificial aplicadas à segurança e observabilidade.

---

## Arquitetura Conceitual

O projeto adota os princípios da arquitetura **Lakehouse (Medalhão)**, organizando os dados em camadas progressivas de valor analítico.

```text
AWS CloudWatch Logs
(Lambda controle)
         │
         ▼
┌─────────────────┐
│ Bronze          │
│ Logs Brutos     │
└─────────────────┘
         │
         ▼
┌─────────────────┐
│ Silver          │
│ Dados           │
│ Estruturados    │
└─────────────────┘
         │
         ▼
┌─────────────────┐
│ Gold            │
│ Métricas e      │
│ Indicadores     │
└─────────────────┘
         │
 ┌───────┼────────┐
 ▼       ▼        ▼
Analytics ML/IA  Assistente
e Dashboards     Generativo
```

---

## Fonte dos Dados

Fonte oficial de dados:

- AWS CloudWatch Logs da AWS Lambda `controle`

Os eventos registrados incluem informações de acesso, origem, comportamento dos usuários, execução da infraestrutura e eventos relacionados ao ciclo de vida do ambiente monitorado.

---

## Estrutura do Projeto

```text
access-intelligence/
├── scripts/
│   ├── collect_cloudwatch_bronze.py   # Fase 1 — coleta CloudWatch → Bronze
│   ├── control.py                     # Estado de coleta (R2: cloudwatch_to_bronze.json)
│   ├── config.py                      # Configuração do projeto (R2, janelas, etc.)
│   ├── test_r2_connection.py
│   └── silver/
│       ├── reconstruct_blocks.py      # Reconstrução das execuções da Lambda
│       ├── extract_events.py          # Parsing dos eventos HTTP
│       ├── classify_events.py         # Classificação operacional do ciclo de vida
│       ├── enrich_visitors.py         # Fingerprint e classificação de visitantes
│       ├── enrich_geoip.py            # Enriquecimento geográfico + cross-validação
│       └── run_silver.py              # Orquestrador da camada Silver
├── tests/
│   ├── test_cloudwatch_to_bronze.py
│   └── silver/                        # Um teste por módulo da Silver
├── tools/
│   └── gerar_data_dictionary_md.js    # Gera docs/data_dictionary.md
├── docs/
│   └── data_dictionary.md             # Dicionário de dados e linhagem (Bronze → Silver → Gold)
└── inspecionar_divergentes.py
```

---

## Dicionário de Dados

A linhagem completa dos campos — onde cada um nasce (Bronze), como é transformado (Silver) e o que é planejado para análise (Gold) — está documentada em [`docs/data_dictionary.md`](docs/data_dictionary.md).

O dicionário é gerado a partir da validação do código real (não só do design), cruzando os scripts de `scripts/` e `scripts/silver/` com a planilha de design do projeto. Para regenerar após qualquer mudança nos campos:

```bash
node tools/gerar_data_dictionary_md.js
```

---

## 🚀 Roadmap

### [x] Fase 0 — Estruturação

- Definição da arquitetura Lakehouse.
- Planejamento das camadas Bronze, Silver e Gold.
- Organização inicial do repositório.
- Documentação do projeto.

### [x] Fase 1 — Coleta

- Extração automatizada dos logs do CloudWatch.
- Persistência dos eventos brutos na camada Bronze.
- Armazenamento em Apache Parquet.
- Particionamento por data.
- Controle de reprocessamento através de Collection ID.
- Construção do histórico operacional.

### [x] Fase 2 — Camada Silver

#### [x] 2.1 Normalização

- Reconstrução das execuções da Lambda a partir dos logs.
- Parsing dos eventos brutos.
- Extração do JSON dos eventos HTTP.
- Estruturação dos eventos em formato tabular.
- Construção da camada Silver.

#### [x] 2.2 Classificação Operacional

- Classificação dos eventos do ciclo de vida do ambiente.
- Identificação de criação, acesso e destruição do ambiente.
- Consolidação dos estados operacionais.
- Extração de indicadores de comportamento operacional.

#### [~] 2.3 Enriquecimento

- Classificação de User Agents.
- Identificação de acessos humanos, bots e scanners.
- Enriquecimento geográfico via MaxMind GeoLite2 (país, cidade, ASN, provedor de rede).
- Cross-validação geográfica com IPinfo como segunda fonte (distância Haversine entre as duas estimativas, classificação de severidade de divergência).
- Detecção de provedor de nuvem/datacenter e redes de scanner conhecidas.
- Avaliação e consolidação dos atributos analíticos da camada Silver.
- Geração de identificadores analíticos de visitantes.
- Regras de correlação de visitantes em teste e refinamento.

### [ ] Fase 3 — Analytics (Gold)

- Construção da camada Gold.
- Métricas operacionais.
- Indicadores comportamentais.
- Agregações para consumo analítico.

### [ ] Fase 4 — Observabilidade

- Dashboards operacionais.
- Relatórios automatizados.
- Monitoramento contínuo do ambiente.
- Alertas operacionais.

### [ ] Fase 5 — Inteligência

- Detecção de anomalias.
- Identificação de padrões incomuns.
- Perfis comportamentais de acesso.
- Classificação automática de eventos.

### [ ] Fase 6 — IA Generativa

- Investigação de eventos em linguagem natural.
- Resumos automáticos de ocorrências.
- Assistente para análise operacional e segurança.
- Exploração de arquiteturas RAG.

---

## 🛠️ Tecnologias

- Python
- AWS CloudWatch Logs
- Apache Parquet
- Cloudflare R2
- DuckDB
- Streamlit
- Scikit-Learn
- APIs de LLM

---

## Status

Projeto em desenvolvimento.

### Fase Atual

**Camada Silver (normalização, classificação e enriquecimento).**

### Concluído

- Estruturação da arquitetura Lakehouse.
- Implementação da camada Bronze.
- Coleta automatizada de logs do CloudWatch.
- Persistência em Apache Parquet.
- Reconstrução das execuções da Lambda.
- Extração e normalização dos eventos HTTP.
- Classificação operacional dos eventos.
- Implementação inicial do enriquecimento analítico.

### Em Teste / Refinamento

- Estratégias de identificação de visitantes (`visitor_id`).
- Avaliação da utilidade dos atributos analíticos da Silver.
- Consolidação do modelo de enriquecimento.
- Correlação de eventos ao longo do ciclo de vida do ambiente.
- Indicadores comportamentais avançados.
- Regras de enriquecimento analítico.

### Próxima Etapa

**Construção da camada Gold com métricas, indicadores e agregações analíticas.**