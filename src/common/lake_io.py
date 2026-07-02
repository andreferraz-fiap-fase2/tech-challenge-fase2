"""Camada de IO do data lake (Parquet particionado).

Abstrai a leitura/escrita nas camadas Bronze/Silver/Gold. Localmente escreve
no filesystem (`data/`); o mesmo código funciona apontando para `gs://` quando
executado no GCP, pois Polars/PyArrow suportam object storage via fsspec.

Decisão FinOps: Parquet + compressão ZSTD + particionamento reduzem volume
armazenado e custo de scan em queries (menos bytes lidos no BigQuery/Athena).
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from .config import Config, load_config
from .logging_setup import get_logger

log = get_logger("common.lake_io")


def _layer_root(cfg: Config, layer: str) -> Path:
    root = cfg.lake_path(layer)
    root.mkdir(parents=True, exist_ok=True)
    return root


def write_table(
    df: pl.DataFrame,
    layer: str,
    name: str,
    *,
    partition_by: list[str] | None = None,
    cfg: Config | None = None,
) -> Path:
    """Grava um DataFrame numa camada do lake em Parquet.

    Se `partition_by` for informado, escreve em diretórios estilo Hive
    (ex.: `sigla_uf=SP/`), o que permite *partition pruning* nas queries.
    """
    cfg = cfg or load_config()
    compression = cfg.get("lake.compression", "zstd")
    dest = _layer_root(cfg, layer) / name

    if partition_by:
        # Remove partição anterior para reescrita idempotente.
        _clear_dir(dest)
        df.write_parquet(
            dest,
            compression=compression,
            partition_by=partition_by,
        )
    else:
        dest = dest.with_suffix(".parquet")
        df.write_parquet(dest, compression=compression)

    log.info(
        "gravado [%s] %s | %d linhas | partição=%s",
        layer, name, df.height, partition_by or "-",
    )
    return dest


def read_table(layer: str, name: str, *, cfg: Config | None = None) -> pl.DataFrame:
    """Lê uma tabela de uma camada (arquivo único ou dataset particionado)."""
    cfg = cfg or load_config()
    root = cfg.lake_path(layer)
    single = root / f"{name}.parquet"
    partitioned = root / name

    if single.exists():
        return pl.read_parquet(single)
    if partitioned.is_dir():
        # `**/*.parquet` recupera todas as partições Hive.
        return pl.read_parquet(partitioned / "**" / "*.parquet")
    raise FileNotFoundError(f"Tabela '{name}' não encontrada na camada '{layer}'.")


def table_exists(layer: str, name: str, *, cfg: Config | None = None) -> bool:
    cfg = cfg or load_config()
    root = cfg.lake_path(layer)
    return (root / f"{name}.parquet").exists() or (root / name).is_dir()


def _clear_dir(path: Path) -> None:
    if not path.exists():
        return
    for child in sorted(path.rglob("*"), reverse=True):
        if child.is_file():
            child.unlink()
        elif child.is_dir():
            child.rmdir()
    if path.is_dir():
        path.rmdir()
