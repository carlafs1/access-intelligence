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

### [ ] Fase 1 — Coleta

- Extração automatizada dos logs do CloudWatch.
- Persistência dos eventos brutos na camada Bronze.
- Construção do histórico operacional.

### [ ] Fase 2 — Normalização

- Estruturação dos eventos em formato tabular.
- Conversão para Apache Parquet.
- Construção da camada Silver.

### [ ] Fase 3 — Enriquecimento

- Classificação de User Agents.
- Identificação de acessos humanos, bots e scanners.
- Enriquecimento geográfico e contextual dos eventos.
- Consolidação da camada Silver enriquecida.

### [ ] Fase 4 — Analytics

- Construção da camada Gold.
- Métricas operacionais.
- Indicadores comportamentais.
- Agregações para consumo analítico.

### [ ] Fase 5 — Observabilidade

- Dashboards operacionais.
- Relatórios automatizados.
- Monitoramento contínuo do ambiente.
- Alertas operacionais.

### [ ] Fase 6 — Inteligência

- Detecção de anomalias.
- Identificação de padrões incomuns.
- Perfis comportamentais de acesso.
- Classificação automática de eventos.

### [ ] Fase 7 — IA Generativa

- Investigação de eventos em linguagem natural.
- Resumos automáticos de ocorrências.
- Assistente para análise operacional e segurança.
- Exploração de arquiteturas RAG.

---

## 🛠️ Tecnologias

- Python
- DuckDB
- Apache Parquet
- AWS CloudWatch Logs
- Streamlit
- Scikit-Learn
- APIs de LLM

---

## Status

Projeto em fase inicial de estruturação.

**Fase atual:** Estruturação (Fase 0)

**Próxima etapa:** implementação da camada Bronze e automação da coleta dos logs do CloudWatch.