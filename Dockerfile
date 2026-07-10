# Imagem do pipeline para execução serverless na GCP (Cloud Run Job).
# O job roda: download (fonte pública BigQuery) → pipeline cloud-native
# (Bronze→Silver→Gold direto no GCS) → carga no BigQuery.
FROM python:3.14-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config/ config/
COPY scripts/ scripts/
COPY src/ src/

# Logs sem buffer (aparecem em tempo real no Cloud Logging)
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["bash", "scripts/cloud_run_job.sh"]
