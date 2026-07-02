# ============================================================================
# Infraestrutura GCP para o pipeline híbrido de alfabetização (Tech Challenge F2)
# Provisiona o alvo de nuvem espelhado pela execução local:
#   - Data lake medalhão em GCS (bronze / silver / gold)
#   - Data warehouse analítico no BigQuery (camada Gold consultável)
#   - Pub/Sub para a ingestão streaming
# ============================================================================

terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ------------------------------------------------------------------ #
# Data lake — Arquitetura Medalhão (um bucket por camada)
# ------------------------------------------------------------------ #
locals {
  layers = ["bronze", "silver", "gold"]
}

resource "google_storage_bucket" "lake" {
  for_each                    = toset(local.layers)
  name                        = "${var.prefix}-${each.key}"
  location                    = var.region
  storage_class               = "STANDARD"
  force_destroy               = true
  uniform_bucket_level_access = true

  # FinOps: expira dados brutos antigos da Bronze e move Silver p/ nearline.
  dynamic "lifecycle_rule" {
    for_each = each.key == "bronze" ? [1] : []
    content {
      condition { age = var.bronze_retention_days }
      action { type = "Delete" }
    }
  }

  dynamic "lifecycle_rule" {
    for_each = each.key == "silver" ? [1] : []
    content {
      condition { age = 30 }
      action {
        type          = "SetStorageClass"
        storage_class = "NEARLINE"
      }
    }
  }
}

# ------------------------------------------------------------------ #
# BigQuery — camada analítica (Gold) consultável por dashboards/ML
# ------------------------------------------------------------------ #
resource "google_bigquery_dataset" "gold" {
  dataset_id                  = var.bigquery_dataset
  location                    = var.region
  description                 = "Camada Gold — indicadores de alfabetização prontos para análise"
  default_table_expiration_ms = null
}

# ------------------------------------------------------------------ #
# Pub/Sub — ingestão streaming (eventos de novas medições/atualizações)
# ------------------------------------------------------------------ #
resource "google_pubsub_topic" "indicador_updates" {
  name                       = var.pubsub_topic
  message_retention_duration = "86400s" # 1 dia (FinOps: retenção curta)
}

resource "google_pubsub_subscription" "indicador_consumer" {
  name                 = "${var.pubsub_topic}-sub"
  topic                = google_pubsub_topic.indicador_updates.id
  ack_deadline_seconds = 20

  expiration_policy {
    ttl = "" # não expira
  }
  retry_policy {
    minimum_backoff = "10s"
  }
}
