# FinOps — Otimização de Custos

Como a arquitetura foi desenhada para eficiência de custos em nuvem e quais
decisões reduzem o custo operacional.

## 1. Armazenamento eficiente

- **Parquet colunar + compressão ZSTD**: reduz o volume armazenado em ~5–10x
  vs CSV/JSON e permite ler apenas as colunas necessárias.
- **Particionamento Hive** (`ano=`, e por UF quando aplicável): habilita
  *partition pruning* — queries que filtram por ano leem só as partições
  relevantes, cortando bytes escaneados (custo direto no BigQuery/Athena).
- **Lifecycle no GCS** (Terraform): Bronze expira após 90 dias; Silver migra
  para *Nearline* após 30 dias. Dados quentes ficam em Standard; frios, mais baratos.

## 2. Otimização de queries

- Agregações pesadas são feitas **uma vez** na camada Gold e materializadas;
  dashboards leem tabelas pequenas e pré-agregadas em vez de varrer fatos.
- A camada Gold é o **grão certo** para cada consumidor (município/UF/Brasil),
  evitando recomputações repetidas.

## 3. Recursos computacionais

- **DuckDB/Polars em vez de Spark**: para o volume deste indicador (milhões de
  linhas, não bilhões), um motor *single-node* vetorizado processa tudo em
  segundos, **sem cluster**. Isso elimina o custo de um cluster Dataproc/EMR
  ocioso e o overhead de JVM. Spark só se justificaria em escala muito maior.
- **Pub/Sub com retenção curta** (1 dia) e streaming em **micro-batches**:
  menos custo de retenção e processamento sob demanda.

## 4. Estimativa de custo (ordem de grandeza, GCP)

Cenário: ~5 GB Bronze, execução diária, Gold consultada por dashboards.

| Serviço | Uso estimado | Custo mensal aprox. (USD) |
|---|---|---|
| GCS (Standard/Nearline) | ~10 GB com lifecycle | < 1 |
| BigQuery armazenamento | ~2 GB Gold | < 1 |
| BigQuery consultas | ~50 GB scan/mês (com pruning) | ~0,30 |
| Pub/Sub | < 1 GB mensagens/mês | tier gratuito |
| Cloud Run (batch diário) | poucos minutos/dia | < 1 |
| **Total** | | **~3–5 USD/mês** |

> A escolha DuckDB/Polars + Parquet particionado + Gold materializada é o que
> mantém o custo em poucos dólares/mês — a mesma carga em um cluster Spark
> permanentemente ligado custaria dezenas a centenas de dólares.

## 5. Como monitorar custo

- Rótulos (labels) por camada/ambiente nos recursos GCP para *cost allocation*.
- Alertas de orçamento (Budgets & Alerts) no billing.
- Métrica de **bytes escaneados** por query como proxy de custo no BigQuery.
