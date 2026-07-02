output "lake_buckets" {
  value       = { for k, b in google_storage_bucket.lake : k => "gs://${b.name}" }
  description = "URIs dos buckets do data lake por camada"
}

output "bigquery_dataset" {
  value       = google_bigquery_dataset.gold.dataset_id
  description = "Dataset BigQuery da camada Gold"
}

output "pubsub_topic" {
  value       = google_pubsub_topic.indicador_updates.name
  description = "Tópico Pub/Sub da ingestão streaming"
}
