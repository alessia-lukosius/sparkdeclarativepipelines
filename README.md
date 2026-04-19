# ✈️ AlessiaAirlines — Databricks Data Engineering Pipeline

Pipeline de engenharia de dados construído com **Spark Declarative Pipelines (SDP)** no Databricks, implementando arquitetura medallion completa (Landing → Bronze → Silver → Gold) com Unity Catalog.

---

## 📐 Arquitetura

```
Landing Volume
    ├── documents/          ← CSVs diários de vendas de passagens
    └── agencies/           ← CSVs mensais de agências de viagem
         │
         ▼
    [Auto Loader + Temporary View]
         │
         ▼
      Bronze (Streaming Tables + Auto CDC)
    ├── documents_bronze    ← SCD Type 1 (upsert por chave de negócio)
    └── agencies_bronze     ← SCD Type 2 (histórico completo de mudanças)
         │
         ▼
      Silver (Materialized Views)
    ├── documents_silver    ← Dados limpos e tipados de vendas
    └── agencies_silver     ← Snapshot atual das agências
         │
         ▼
      Gold (Materialized Views)
    └── sales_consolidated  ← Visão analítica consolidada com normalização monetária (BRL)
```

**Catálogo Unity Catalog:** `alessiaairlines`  
**Schemas:** `landing` (volumes de ingestão) · `sdp` (todas as camadas do pipeline)

---

## 🗂️ Estrutura dos Notebooks

| Notebook | Descrição |
|---|---|
| `00_project_setup.py` | Criação do catálogo, schemas (`landing`, `sdp`) e volumes |
| `01_bronze_silver_documents_daily.py` | Pipeline SDP — vendas diárias (Bronze SCD1 + Silver) |
| `02_bronze_silver_agencies.py` | Pipeline SDP — agências mensais (Bronze SCD2 + Silver) |
| `03_gold_consolidated.py` | Pipeline SDP — camada Gold consolidada |

---

## 🔧 Tecnologias

- **Databricks** com **Unity Catalog**
- **Spark Declarative Pipelines** (`from pyspark import pipelines as dp`) — API atual (Lakeflow)
- **Auto Loader** (`cloudFiles`) para ingestão incremental dos volumes
- **Delta Lake** como formato de armazenamento em todas as camadas
- **Python / PySpark**

---

## 📦 Fontes de Dados

### `documents_daily` — Vendas de Passagens
- **Frequência:** diária
- **Formato de arquivo:** `documents_YYYYMMDD.csv`
- **Volume:** `/Volumes/alessiaairlines/landing/documents/`
- **Padrão CDC:** SCD Type 1 (substitui o registro mais recente por chave de negócio)

### `agencies` — Agências de Viagem
- **Frequência:** mensal
- **Formato de arquivo:** `agencies_YYYYMM.csv`
- **Volume:** `/Volumes/alessiaairlines/landing/agencies/`
- **Padrão CDC:** SCD Type 2 (preserva histórico completo de alterações)
- **Sequenciador SCD2:** `data_referencia` extraída do nome do arquivo via `regexp_extract` + `to_date`

---

## 🏅 Camadas da Arquitetura Medallion

### 🟫 Bronze — Fidelidade à Fonte
- Ingestão via `@dp.temporary_view` com Auto Loader (sem tabela física intermediária — o volume Landing preserva os arquivos originais)
- CDC gerenciado por `dp.create_auto_cdc_flow` em Streaming Tables
- Filosofia: **sem transformações de negócio**, apenas pré-requisitos técnicos (ex: filtro de chave nula)

### 🥈 Silver — Qualidade e Tipagem
- `@dp.materialized_view` lendo da Bronze via `dp.read` (batch)
- Aplicação de expectations (qualidade de dados), cast de tipos, renomeação de colunas
- Lógica de negócio aplicada aqui, não no Bronze

### 🥇 Gold — Camada Analítica
- `@dp.materialized_view` consolidando Silver de documentos e agências
- Normalização monetária: conversão de USD e EUR para BRL via `CASE WHEN` + `F.round(..., 2)` com taxas fixas (placeholder)
- Granularidade pronta para consumo analítico e dashboards

---

## ⚙️ Decisões Arquiteturais

| Decisão | Escolha | Justificativa |
|---|---|---|
| Tabela Raw Bronze | ❌ Removida | Volume Landing preserva os arquivos originais — redundância desnecessária |
| CDC Bronze | `dp.create_auto_cdc_flow` | Gerenciamento nativo de SCD1/SCD2 pelo SDP |
| Silver/Gold | `@dp.materialized_view` | Leitura batch da Bronze; CDC feed desnecessário |
| Schema único por pipeline | `sdp` | Restrição do SDP: um target schema por pipeline garante grafo de dependências correto |
| Sequenciador SCD2 | `data_referencia` (do filename) | Mais estável e semanticamente significativo que `file_modification_time` |
| Checkpoints | Gerenciado pelo SDP | Não é necessário configurar `checkpointLocation` manualmente |
| Configurações de pipeline | `spark.conf.get(...)` | Variáveis definidas em Pipeline Settings → Advanced → Configuration |

---

## 📊 Dados de Demonstração

O projeto inclui 4 CSVs de amostra para demonstração do comportamento dos pipelines:

| Arquivo | Propósito |
|---|---|
| `documents_20260601.csv` | Lote inicial de vendas |
| `documents_20260602.csv` | Demonstra SCD1: atualização de status de registros existentes |
| `agencies_202605.csv` | Snapshot inicial de agências |
| `agencies_202606.csv` | Demonstra SCD2: AG002 com alteração de nome e cidade (histórico preservado) |

---

## 🚀 Como Executar

1. **Setup inicial:** execute `00_project_setup.py` para criar catálogo, schemas e volumes
2. **Upload dos dados:** faça upload dos CSVs de amostra nos volumes correspondentes:
   - Vendas → `/Volumes/alessiaairlines/landing/documents/`
   - Agências → `/Volumes/alessiaairlines/landing/agencies/`
3. **Configuração dos pipelines:** em cada pipeline SDP, configure em *Settings → Advanced → Configuration*:
   ```
   catalog = alessiaairlines
   target_schema = sdp
   ```
4. **Execução:** acione os pipelines na ordem:
   - `01_bronze_silver_documents_daily` 
   - `02_bronze_silver_agencies`
   - `03_gold_consolidated`

---

## 📁 Estrutura do Repositório

```
alessiaairlines/
├── notebooks/
│   ├── 00_project_setup.py
│   ├── 01_bronze_silver_documents_daily.py
│   ├── 02_bronze_silver_agencies.py
│   └── 03_gold_consolidated.py
├── sample_data/
│   ├── documents_20260601.csv
│   ├── documents_20260602.csv
│   ├── agencies_202605.csv
│   └── agencies_202606.csv
└── README.md
```

---

*Projeto desenvolvido como demonstração técnica de pipelines de engenharia de dados modernos com Databricks Spark Declarative Pipelines e Unity Catalog.*
