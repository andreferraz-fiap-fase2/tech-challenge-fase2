"""Publicação das camadas na nuvem (GCP): data lake no GCS + Silver/Gold no BigQuery.

- publish_bigquery(): cria o dataset da camada (gold e/ou silver) e carrega as
  tabelas no BigQuery (data warehouse consultável por dashboards/SQL).
  * lake em modo `gcs` (cloud-native): carrega DIRETO das URIs `gs://` do lake
    (load_table_from_uri) — sem re-upload nem tráfego local (FinOps).
  * lake em modo `local`: carrega via DataFrame a partir de `data/`.
- publish_gcs(): cria os buckets bronze/silver/gold e sobe os Parquet locais.
  Desnecessário no modo cloud-native (o pipeline já grava direto no GCS).

Autenticação via GOOGLE_APPLICATION_CREDENTIALS (Service Account key) ou ADC.

Uso:
    export GOOGLE_APPLICATION_CREDENTIALS=data/gcp-key.json
    python -m src.publish.gcp --project fiap-fase2                    # GCS + BQ gold+silver
    python -m src.publish.gcp --project fiap-fase2 --only bigquery --layers silver
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
# camada -> chave do dataset BigQuery em settings.yaml
BQ_DATASET_KEYS = {
    "gold": ("gcp.bigquery_dataset", "alfabetizacao_gold"),
    "silver": ("gcp.bigquery_dataset_silver", "alfabetizacao_silver"),
}


def _load_from_uri(client, cfg: Config, layer: str, name: str, table_id: str):
    """Carrega uma tabela no BigQuery direto do GCS (modo cloud-native)."""
    from google.cloud import bigquery
    import fsspec

    root = cfg.lake_uri(layer)  # gs://bucket
    fs, _ = fsspec.core.url_to_fs(root)
    single = f"{root}/{name}.parquet"
    uri = single if fs.exists(single) else f"{root}/{name}/*"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition="WRITE_TRUNCATE",
    )
    return client.load_table_from_uri(uri, table_id, job_config=job_config), uri


def publish_bigquery(project: str, cfg: Config, layer: str = "gold", location: str = "US") -> None:
    """Cria o dataset da camada e carrega as tabelas (WRITE_TRUNCATE)."""
    from google.cloud import bigquery

    key, default = BQ_DATASET_KEYS[layer]
    dataset_id = cfg.get(key, default)
    client = bigquery.Client(project=project)
    ds_ref = bigquery.Dataset(f"{project}.{dataset_id}")
    ds_ref.location = location
    client.create_dataset(ds_ref, exists_ok=True)
    log.info("dataset BigQuery pronto: %s.%s (%s)", project, dataset_id, location)

    cloud_lake = cfg.lake_mode == "gcs"
    for name in LAYER_TABLES[layer]:
        table_id = f"{project}.{dataset_id}.{name}"
        if cloud_lake:
            job, uri = _load_from_uri(client, cfg, layer, name, table_id)
            job.result()
            log.info("carregado %s <- %s — %d linhas",
                     table_id, uri, client.get_table(table_id).num_rows)
        else:
            pdf = read_table(layer, name, cfg=cfg).to_pandas()
            job = client.load_table_from_dataframe(
                pdf, table_id,
                job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
            )
            job.result()
            log.info("carregado %s — %d linhas", table_id, client.get_table(table_id).num_rows)
    log.info("BigQuery: camada %s publicada em %s.%s", layer, project, dataset_id)


def publish_gcs(project: str, cfg: Config, location: str = "us-central1") -> None:
    """Cria os buckets bronze/silver/gold e sobe os Parquet do lake local."""
    from google.cloud import storage

    if cfg.lake_mode == "gcs":
        log.info("lake em modo cloud-native (gs://): upload desnecessário, pulando GCS")
        return

    client = storage.Client(project=project)
    for layer, _tables in LAYER_TABLES.items():
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
    ap.add_argument("--layers", default="gold,silver",
                    help="camadas a carregar no BigQuery (ex.: gold,silver)")
    ap.add_argument("--cloud", action="store_true",
                    help="lake em gs:// (equivale a LAKE_MODE=gcs): BQ carrega direto do GCS")
    args = ap.parse_args(argv)

    if args.cloud:
        os.environ["LAKE_MODE"] = "gcs"

    cfg = load_config()
    project = args.project or cfg.gcp_project_id
    if not project:
        sys.exit("Informe --project SEU_ID (ou defina GCP_PROJECT_ID)")

    if args.only in (None, "bigquery"):
        for layer in [x.strip() for x in args.layers.split(",") if x.strip()]:
            if layer not in BQ_DATASET_KEYS:
                sys.exit(f"Camada inválida para BigQuery: {layer} (use gold/silver)")
            publish_bigquery(project, cfg, layer=layer)
    if args.only in (None, "gcs"):
        try:
            publish_gcs(project, cfg)
        except Exception as exc:
            log.error("GCS falhou (%s). Verifique a role 'Storage Admin' na conta de serviço.", exc)
            if args.only == "gcs":
                raise


if __name__ == "__main__":
    main()
