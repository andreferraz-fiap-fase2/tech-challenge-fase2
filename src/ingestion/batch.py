"""Ingestão BATCH → camada Bronze.

Lê os arquivos brutos da landing zone (Parquet reais baixados do BigQuery via
`scripts/bq_download.py`) e os grava na camada Bronze **sem transformações**
(princípio da Arquitetura Medalhão). Adiciona apenas metadados de ingestão:
`_ingested_at`, `_source`, `_ingestion_type`.

Em produção (GCP) esta etapa seria um job agendado (Cloud Run/Dataproc + Cloud
Scheduler) lendo o dataset da Base dos Dados (BigQuery) para o bucket Bronze (GCS).
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from ..common.config import REPO_ROOT, Config, load_config
from ..common.lake_io import write_table
from ..common.logging_setup import get_logger
from ..monitoring.metrics import MetricsCollector

log = get_logger("ingestion.batch")

# Fontes batch reais (nome lógico na Bronze -> arquivo Parquet na landing)
BATCH_SOURCES = [
    "alunos",
    "municipio",
    "uf",
    "meta_alfabetizacao_brasil",
    "meta_alfabetizacao_uf",
    "meta_alfabetizacao_municipio",
    "dicionario",
    "diretorio_uf",
    "diretorio_municipio",
]


def _landing_dir(cfg: Config) -> Path:
    return REPO_ROOT / cfg.get("lake.landing", "data/real")


def ingest_batch(cfg: Config | None = None, metrics: MetricsCollector | None = None) -> None:
    """Ingere todas as fontes batch da landing (Parquet real) para a Bronze."""
    cfg = cfg or load_config()
    metrics = metrics or MetricsCollector(cfg)
    landing = _landing_dir(cfg)

    if not landing.exists():
        raise FileNotFoundError(
            f"Landing zone não encontrada em {landing}. "
            "Baixe os dados reais: python -m scripts.bq_download --project SEU_PROJETO"
        )

    for name in BATCH_SOURCES:
        with metrics.stage(f"bronze_batch:{name}", "bronze") as m:
            src = landing / f"{name}.parquet"
            if not src.exists():
                raise FileNotFoundError(f"Arquivo de origem ausente: {src}")

            # Bronze preserva o dado bruto (mesmos tipos da fonte).
            df = pl.read_parquet(src)
            m.rows_in = df.height

            now = datetime.now(timezone.utc).isoformat()
            df = df.with_columns(
                pl.lit(now).alias("_ingested_at"),
                pl.lit(f"bigquery:br_inep_avaliacao_alfabetizacao.{name}").alias("_source"),
                pl.lit("batch").alias("_ingestion_type"),
            )
            write_table(df, "bronze", name, cfg=cfg)
            m.rows_out = df.height


if __name__ == "__main__":
    mc = MetricsCollector()
    ingest_batch(metrics=mc)
    mc.flush()
    print(mc.summary())
