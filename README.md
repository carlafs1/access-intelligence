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
- Enriquecimento geográfico e contextual dos eventos.
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