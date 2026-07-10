# Execução do pipeline na nuvem (Cloud Run Job)

O pipeline roda **inteiro dentro da GCP** como um Cloud Run Job serverless:
o container baixa a fonte pública (BigQuery/Base dos Dados) para o disco
efêmero, processa Bronze→Silver→Gold **direto nos buckets GCS** (`--cloud`)
e carrega os datasets `alfabetizacao_gold` + `alfabetizacao_silver` no
BigQuery a partir das URIs `gs://`.

**FinOps**: job serverless — paga somente pelos minutos de execução
(~2 vCPU / 4 GiB por poucos minutos ≈ centavos por execução; coberto pelo
free trial). Sem cluster, sem VM ociosa, sem chave de service account na
imagem (autenticação via identidade do job / ADC).

## Deploy (uma vez)

```bash
gcloud auth login
gcloud config set project fiap-fase2

# APIs necessárias (uma vez por projeto)
gcloud services enable run.googleapis.com cloudbuild.googleapis.com artifactregistry.googleapis.com

# Build da imagem (Cloud Build) + criação do job
gcloud run jobs deploy alfabetizacao-pipeline \
  --source . \
  --region us-central1 \
  --tasks 1 --max-retries 0 \
  --cpu 2 --memory 4Gi --task-timeout 30m \
  --set-env-vars GCP_PROJECT_ID=fiap-fase2 \
  --service-account id-bigquery-download@fiap-fase2.iam.gserviceaccount.com
```

> A service account reutiliza a mesma do download (`id-bigquery-download@...`),
> que já tem as roles **BigQuery User** e **Storage Admin** — suficientes para
> consultar a fonte, escrever no GCS e carregar o BigQuery.

## Executar

```bash
gcloud run jobs execute alfabetizacao-pipeline --region us-central1 --wait
```

Logs em tempo real: console → Cloud Run → Jobs → `alfabetizacao-pipeline` →
execução → Logs (ou `gcloud beta run jobs executions logs`).

## Agendamento (opcional — ingestão batch periódica)

O Cloud Scheduler dispara o job no cron desejado (ex.: mensal, novo ciclo Saeb):

```bash
gcloud services enable cloudscheduler.googleapis.com

# permite ao scheduler invocar o job
gcloud run jobs add-iam-policy-binding alfabetizacao-pipeline \
  --region us-central1 \
  --member "serviceAccount:id-bigquery-download@fiap-fase2.iam.gserviceaccount.com" \
  --role roles/run.invoker

gcloud scheduler jobs create http alfabetizacao-mensal \
  --location us-central1 \
  --schedule "0 6 1 * *" \
  --http-method POST \
  --uri "https://us-central1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/fiap-fase2/jobs/alfabetizacao-pipeline:run" \
  --oauth-service-account-email id-bigquery-download@fiap-fase2.iam.gserviceaccount.com
```

## Arquivos envolvidos

| Arquivo | Papel |
|---|---|
| `Dockerfile` | imagem Python 3.14 slim com o código do pipeline |
| `scripts/cloud_run_job.sh` | entrypoint: download → `--cloud run-all` → publish BigQuery |
| `.gcloudignore` / `.dockerignore` | mantém `data/`, credenciais e docs fora da imagem |
