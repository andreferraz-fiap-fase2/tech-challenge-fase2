"""Ingestão BATCH → camada Bronze.

Lê os arquivos brutos da landing zone (CSV) e os grava na camada Bronze em
Parquet, **sem transformações significativas** (princípio da Arquitetura
Medalhão). Adiciona apenas metadados de ingestão para rastreabilidade:
`_ingested_at`, `_source_file`, `_ingestion_type`.

Em produção (GCP) esta etapa seria um job agendado (Cloud Scheduler +
Cloud Run/Dataproc) lendo do dataset da Base dos Dados (BigQuery) para o
bucket Bronze (GCS).
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

# Fontes batch: (nome lógico, arquivo na landing, chave para partição opcional)
BATCH_SOURCES = [
    ("uf", "uf.csv", None),
    ("municipio", "municipio.csv", None),
    ("meta_brasil", "meta_brasil.csv", None),
    ("meta_uf", "meta_uf.csv", None),
    ("meta_municipio", "meta_municipio.csv", None),
    ("indicador_aluno", "indicador_aluno.csv", None),
]


def _landing_dir(cfg: Config) -> Path:
    return REPO_ROOT / "data" / "landing"


def ingest_batch(cfg: Config | None = None, metrics: MetricsCollector | None = None) -> None:
    """Ingere todas as fontes batch da landing para a Bronze."""
    cfg = cfg or load_config()
    metrics = metrics or MetricsCollector(cfg)
    landing = _landing_dir(cfg)

    if not landing.exists():
        raise FileNotFoundError(
            f"Landing zone não encontrada em {landing}. "
            "Rode: python -m scripts.generate_sample_data"
        )

    for name, filename, _part in BATCH_SOURCES:
        with metrics.stage(f"bronze_batch:{name}", "bronze") as m:
            src = landing / filename
            if not src.exists():
                raise FileNotFoundError(f"Arquivo de origem ausente: {src}")

            # Lê tudo como string na Bronze (raw fidelity); tipagem ocorre na Silver.
            df = pl.read_csv(src, infer_schema_length=0)
            m.rows_in = df.height

            now = datetime.now(timezone.utc).isoformat()
            df = df.with_columns(
                pl.lit(now).alias("_ingested_at"),
                pl.lit(filename).alias("_source_file"),
                pl.lit("batch").alias("_ingestion_type"),
            )
            write_table(df, "bronze", name, cfg=cfg)
            m.rows_out = df.height


if __name__ == "__main__":
    mc = MetricsCollector()
    ingest_batch(metrics=mc)
    mc.flush()
    print(mc.summary())
