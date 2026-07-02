variable "project_id" {
  type        = string
  description = "ID do projeto GCP"
}

variable "region" {
  type        = string
  default     = "us-central1"
  description = "Região dos recursos"
}

variable "prefix" {
  type        = string
  default     = "tc2-alfabetizacao"
  description = "Prefixo dos nomes de buckets (deve ser globalmente único)"
}

variable "bigquery_dataset" {
  type    = string
  default = "alfabetizacao"
}

variable "pubsub_topic" {
  type    = string
  default = "indicador-updates"
}

variable "bronze_retention_days" {
  type        = number
  default     = 90
  description = "Dias de retenção dos dados brutos na Bronze (FinOps)"
}
