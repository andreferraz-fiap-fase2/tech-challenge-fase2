"""Camada de IO do data lake (Parquet particionado).

Abstrai a leitura/escrita nas camadas Bronze/Silver/Gold. O destino é
resolvido por `Config.lake_uri()` conforme `lake.mode`:

- ``local`` — filesystem (`data/`), leitura/escrita via Polars nativo;
- ``gcs``   — cloud-native: lê e grava DIRETO nos buckets GCS via fsspec/gcsfs
  (sem etapa de upload posterior); autenticação por GOOGLE_APPLICATION_CREDENTIALS;
- qualquer URI fsspec (ex.: ``memory://lake``) — usada nos testes, valida o
  caminho cloud sem custo de nuvem.

No modo remoto as colunas de partição são mantidas TAMBÉM dentro dos arquivos
(além do layout Hive `col=valor/`): os tipos ficam exatos na releitura e o
BigQuery consegue carregar as tabelas direto das URIs `gs://` sem opções de
partição.

Decisão FinOps: Parquet + compressão ZSTD + particionamento reduzem volume
armazenado e custo de scan em queries (menos bytes lidos no BigQuery/Athena).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from .config import Config, load_config
from .logging_setup import get_logger

log = get_logger("common.lake_io")


def _is_local(cfg: Config) -> bool:
    return cfg.lake_mode == "local"


def _fs_and_root(cfg: Config, layer: str) -> tuple[Any, str]:
    """Resolve (filesystem fsspec, caminho-raiz da camada) para modo remoto."""
    import fsspec

    fs, root = fsspec.core.url_to_fs(cfg.lake_uri(layer))
    return fs, root.rstrip("/")


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
) -> str:
    """Grava um DataFrame numa camada do lake em Parquet.

    Se `partition_by` for informado, escreve em diretórios estilo Hive
    (ex.: `ano=2024/`), o que permite *partition pruning* nas queries.
    """
    cfg = cfg or load_config()
    compression = cfg.get("lake.compression", "zstd")

    if _is_local(cfg):
        dest = _layer_root(cfg, layer) / name
        if partition_by:
            _clear_dir(dest)  # reescrita idempotente
            df.write_parquet(dest, compression=compression, partition_by=partition_by)
        else:
            dest = dest.with_suffix(".parquet")
            df.write_parquet(dest, compression=compression)
        out = str(dest)
    else:
        out = _write_remote(df, layer, name, partition_by, cfg, compression)

    log.info(
        "gravado [%s] %s | %d linhas | partição=%s",
        layer, name, df.height, partition_by or "-",
    )
    return out


def _write_remote(
    df: pl.DataFrame,
    layer: str,
    name: str,
    partition_by: list[str] | None,
    cfg: Config,
    compression: str,
) -> str:
    fs, root = _fs_and_root(cfg, layer)

    if not partition_by:
        path = f"{root}/{name}.parquet"
        with fs.open(path, "wb") as fh:
            df.write_parquet(fh, compression=compression)
        return path

    dest = f"{root}/{name}"
    if fs.exists(dest):  # reescrita idempotente
        fs.rm(dest, recursive=True)
    for keys, part in df.partition_by(partition_by, as_dict=True, include_key=True).items():
        hive = "/".join(f"{col}={val}" for col, val in zip(partition_by, keys))
        path = f"{dest}/{hive}/part-0000.parquet"
        with fs.open(path, "wb") as fh:
            part.write_parquet(fh, compression=compression)
    return dest


def read_table(layer: str, name: str, *, cfg: Config | None = None) -> pl.DataFrame:
    """Lê uma tabela de uma camada (arquivo único ou dataset particionado)."""
    cfg = cfg or load_config()

    if _is_local(cfg):
        root = cfg.lake_path(layer)
        single = root / f"{name}.parquet"
        partitioned = root / name
        if single.exists():
            return pl.read_parquet(single)
        if partitioned.is_dir():
            # `**/*.parquet` recupera todas as partições Hive.
            return pl.read_parquet(partitioned / "**" / "*.parquet")
        raise FileNotFoundError(f"Tabela '{name}' não encontrada na camada '{layer}'.")

    fs, root = _fs_and_root(cfg, layer)
    single = f"{root}/{name}.parquet"
    if fs.exists(single):
        with fs.open(single, "rb") as fh:
            return pl.read_parquet(fh)
    parts = sorted(fs.glob(f"{root}/{name}/**/*.parquet"))
    if parts:
        frames = []
        for p in parts:
            with fs.open(p, "rb") as fh:
                frames.append(pl.read_parquet(fh))
        return pl.concat(frames)
    raise FileNotFoundError(
        f"Tabela '{name}' não encontrada na camada '{layer}' ({cfg.lake_uri(layer)})."
    )


def table_exists(layer: str, name: str, *, cfg: Config | None = None) -> bool:
    cfg = cfg or load_config()
    if _is_local(cfg):
        root = cfg.lake_path(layer)
        return (root / f"{name}.parquet").exists() or (root / name).is_dir()
    fs, root = _fs_and_root(cfg, layer)
    return fs.exists(f"{root}/{name}.parquet") or bool(fs.glob(f"{root}/{name}/**/*.parquet"))


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
