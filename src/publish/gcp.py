"""Publicação das camadas na nuvem (GCP): data lake no GCS + Gold no BigQuery.

- publish_bigquery(): cria o dataset e carrega as tabelas Gold no BigQuery
  (data warehouse consultável por dashboards, ex.: Looker Studio).
- publish_gcs(): cria os buckets bronze/silver/gold e sobe os Parquet
  (data lake na nuvem). Requer a role Storage Admin na conta de serviço.

Autenticação via GOOGLE_APPLICATION_CREDENTIALS (Service Account key) ou ADC.

Uso:
    export GOOGLE_APPLICATION_CREDENTIALS=data/gcp-key.json
    python -m src.publish.gcp --project fiap-fase2            # BigQuery + GCS
    python -m src.publish.gcp --project fiap-fase2 --only bigquery
"""

from __future__ import annotations

import argparse
import os
import sys

from ..common.config import Config, load_config
from ..common.lake_io import read_table
from ..common.logging_setup import configure_utf8_stdio, get_logger

log = get_logger("publish.gcp")

GOLD_TABLES = [
    "indicador_municipio", "indicador_uf", "indicador_brasil",
    "evolucao_temporal", "validacao_microdado", "ml_features",
]
LAYER_TABLES = {
    "bronze": ["alunos", "alunos_stream", "municipio", "uf", "meta_alfabetizacao_brasil",
               "meta_alfabetizacao_uf", "meta_alfabetizacao_municipio", "dicionario",
               "diretorio_uf", "diretorio_municipio"],
    "silver": ["dim_uf", "dim_municipio", "fato_aluno", "fato_municipio", "fato_uf",
               "meta_brasil_long", "meta_uf_long", "meta_municipio_long", "taxa_brasil"],
    "gold": GOLD_TABLES,
}


def publish_bigquery(project: str, cfg: Config, location: str = "US") -> None:
    """Cria o dataset Gold e carrega as tabelas (WRITE_TRUNCATE)."""
    from google.cloud import bigquery

    dataset_id = cfg.get("gcp.bigquery_dataset", "alfabetizacao_gold")
    client = bigquery.Client(project=project)
    ds_ref = bigquery.Dataset(f"{project}.{dataset_id}")
    ds_ref.location = location
    client.create_dataset(ds_ref, exists_ok=True)
    log.info("dataset BigQuery pronto: %s.%s (%s)", project, dataset_id, location)

    for name in GOLD_TABLES:
        pdf = read_table("gold", name, cfg=cfg).to_pandas()
        table_id = f"{project}.{dataset_id}.{name}"
        job = client.load_table_from_dataframe(
            pdf, table_id,
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
        )
        job.result()
        n = client.get_table(table_id).num_rows
        log.info("carregado %s — %d linhas", table_id, n)
    log.info("BigQuery: camada Gold publicada em %s.%s", project, dataset_id)


def publish_gcs(project: str, cfg: Config, location: str = "us-central1") -> None:
    """Cria os buckets bronze/silver/gold e sobe os Parquet do lake."""
    from google.cloud import storage
    from ..common.config import REPO_ROOT

    client = storage.Client(project=project)
    for layer, tables in LAYER_TABLES.items():
        bucket_name = cfg.get(f"gcp.buckets.{layer}")
        bucket = client.bucket(bucket_name)
        if not bucket.exists():
            client.create_bucket(bucket, location=location)
            log.info("bucket criado: gs://%s", bucket_name)
        # sobe todos os arquivos Parquet da camada preservando a estrutura
        layer_root = cfg.lake_path(layer)
        count = 0
        for path in layer_root.rglob("*.parquet"):
            blob_name = str(path.relative_to(layer_root)).replace("\\", "/")
            bucket.blob(blob_name).upload_from_filename(str(path))
            count += 1
        log.info("gs://%s: %d arquivos enviados", bucket_name, count)
    log.info("GCS: data lake publicado")


def main(argv: list[str] | None = None) -> None:
    configure_utf8_stdio()
    ap = argparse.ArgumentParser(description="Publica as camadas na GCP (GCS + BigQuery).")
    ap.add_argument("--project", default=os.getenv("GCP_PROJECT_ID"))
    ap.add_argument("--only", choices=["bigquery", "gcs"], help="publica só uma parte")
    args = ap.parse_args(argv)

    cfg = load_config()
    project = args.project or cfg.gcp_project_id
    if not project:
        sys.exit("Informe --project SEU_ID (ou defina GCP_PROJECT_ID)")

    if args.only in (None, "bigquery"):
        publish_bigquery(project, cfg)
    if args.only in (None, "gcs"):
        try:
            publish_gcs(project, cfg)
        except Exception as exc:
            log.error("GCS falhou (%s). Verifique a role 'Storage Admin' na conta de serviço.", exc)
            if args.only == "gcs":
                raise


if __name__ == "__main__":
    main()
