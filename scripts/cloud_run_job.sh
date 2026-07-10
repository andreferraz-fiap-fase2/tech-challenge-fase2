#!/usr/bin/env bash
# Execução completa do pipeline dentro do Cloud Run Job.
#
# Autenticação: Application Default Credentials da service account do job
# (--service-account no deploy) — nenhuma chave é copiada para a imagem.
# A landing (data/real) e o tópico de streaming usam o disco efêmero do
# container; o data lake (Bronze/Silver/Gold) vai direto para o GCS (--cloud).
set -euo pipefail

: "${GCP_PROJECT_ID:?defina a variável de ambiente GCP_PROJECT_ID no job}"

echo "=== 1/3 download: fonte pública (Base dos Dados) -> landing local do container ==="
python -m src.pipeline download --project "$GCP_PROJECT_ID"

echo "=== 2/3 pipeline cloud-native: Bronze -> Silver -> Gold direto no GCS ==="
python -m src.pipeline --cloud run-all

echo "=== 3/3 BigQuery: datasets gold + silver carregados das URIs gs:// ==="
python -m src.publish.gcp --cloud --project "$GCP_PROJECT_ID" --only bigquery

echo "=== job concluído ==="
