"""Transformação BRONZE → SILVER (dados tratados + integração + qualidade).

Responsabilidades (conforme o desafio):
  - Limpeza de dados e tratamento de valores ausentes
  - Padronização de nomes e tipos (casting)
  - Normalização de chaves (id_municipio 7 dígitos, sigla_uf maiúscula)
  - Validação de consistência e de chaves de relacionamento (FK)
  - Deduplicação
  - Integração das bases (batch + streaming; fatos + dimensões)

Também executa as regras de qualidade e persiste um relatório por tabela.
Falhas bloqueantes de qualidade interrompem o pipeline (fail-fast).
"""

from __future__ import annotations

import json

import polars as pl

from ..common.config import REPO_ROOT, Config, load_config
from ..common.lake_io import read_table, table_exists, write_table
from ..common.logging_setup import get_logger
from ..monitoring.metrics import MetricsCollector
from ..quality import (
    QualityReport,
    check_foreign_key,
    check_no_duplicates,
    check_not_null,
    check_range,
    check_value_set,
)

log = get_logger("transform.silver")

REGIOES = {"Norte", "Nordeste", "Sudeste", "Sul", "Centro-Oeste"}


def _save_quality_report(report: QualityReport) -> None:
    out_dir = REPO_ROOT / "monitoring" / "quality"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / f"{report.table}.json").write_text(
        json.dumps(report.as_dict(), indent=2, ensure_ascii=False), encoding="utf-8"
    )


def _enforce(report: QualityReport) -> None:
    _save_quality_report(report)
    if report.has_blocking_failures:
        falhas = [r.name for r in report.results if not r.passed and r.severity == "error"]
        raise ValueError(f"Qualidade reprovada em '{report.table}': {falhas}")


def build_silver(cfg: Config | None = None, metrics: MetricsCollector | None = None) -> None:
    cfg = cfg or load_config()
    metrics = metrics or MetricsCollector(cfg)
    max_null = float(cfg.get("quality.max_null_ratio", 0.05))
    ponto_corte = float(cfg.get("quality.ponto_corte_saeb", 743))
    prof_min = float(cfg.get("quality.proficiencia_min", 0))
    prof_max = float(cfg.get("quality.proficiencia_max", 1000))

    # ------------------------------------------------------------------ #
    # Dimensão UF
    # ------------------------------------------------------------------ #
    with metrics.stage("silver:dim_uf", "silver") as m:
        uf = read_table("bronze", "uf", cfg=cfg)
        m.rows_in = uf.height
        uf = (
            uf.select("sigla_uf", "nome_uf", "regiao")
            .with_columns(
                pl.col("sigla_uf").str.to_uppercase().str.strip_chars(),
                pl.col("nome_uf").str.strip_chars(),
                pl.col("regiao").str.strip_chars(),
            )
            .unique(subset=["sigla_uf"])
        )
        rep = QualityReport("dim_uf")
        rep.add(check_no_duplicates(uf, ["sigla_uf"]))
        rep.add(check_not_null(uf, "sigla_uf"))
        rep.add(check_value_set(uf, "regiao", REGIOES))
        _enforce(rep)
        write_table(uf, "silver", "dim_uf", cfg=cfg)
        m.rows_out = uf.height

    # ------------------------------------------------------------------ #
    # Dimensão Município (normaliza chave; valida FK -> UF)
    # ------------------------------------------------------------------ #
    with metrics.stage("silver:dim_municipio", "silver") as m:
        mun = read_table("bronze", "municipio", cfg=cfg)
        m.rows_in = mun.height
        mun = (
            mun.select("id_municipio", "nome_municipio", "sigla_uf")
            .with_columns(
                pl.col("id_municipio").str.strip_chars().str.zfill(7),
                pl.col("nome_municipio").str.strip_chars(),
                pl.col("sigla_uf").str.to_uppercase().str.strip_chars(),
            )
            .unique(subset=["id_municipio"])   # remove duplicata injetada
        )
        rep = QualityReport("dim_municipio")
        rep.add(check_no_duplicates(mun, ["id_municipio"]))
        rep.add(check_not_null(mun, "id_municipio"))
        rep.add(check_foreign_key(mun, "sigla_uf", uf, "sigla_uf"))
        _enforce(rep)
        write_table(mun, "silver", "dim_municipio", cfg=cfg)
        m.rows_out = mun.height

    # ------------------------------------------------------------------ #
    # Metas (Brasil / UF / Município) — casting + validação de faixa
    # ------------------------------------------------------------------ #
    with metrics.stage("silver:metas", "silver") as m:
        rows = 0
        rows += _build_meta_brasil(cfg)
        rows += _build_meta_uf(cfg, uf)
        rows += _build_meta_municipio(cfg, mun)
        m.rows_out = rows

    # ------------------------------------------------------------------ #
    # Fato Indicador (INTEGRAÇÃO batch + streaming)
    # ------------------------------------------------------------------ #
    with metrics.stage("silver:fato_indicador", "silver") as m:
        frames = []
        batch = read_table("bronze", "indicador_aluno", cfg=cfg).select(
            "ano", "id_municipio", "id_aluno", "proficiencia"
        ).with_columns(pl.lit("batch").alias("origem"))
        frames.append(batch)

        if table_exists("bronze", "indicador_stream", cfg=cfg):
            stream = read_table("bronze", "indicador_stream", cfg=cfg).select(
                "ano", "id_municipio", "id_aluno", "proficiencia"
            ).with_columns(pl.lit("streaming").alias("origem"))
            frames.append(stream)

        fato = pl.concat(frames, how="vertical_relaxed")
        m.rows_in = fato.height

        # padronização de tipos + normalização de chaves
        fato = fato.with_columns(
            pl.col("ano").cast(pl.Int32, strict=False),
            pl.col("id_municipio").str.strip_chars().str.zfill(7),
            pl.col("id_aluno").str.strip_chars(),
            # proficiência vazia ("") vira null antes do cast
            pl.when(pl.col("proficiencia").cast(pl.Utf8).str.strip_chars() == "")
            .then(None)
            .otherwise(pl.col("proficiencia"))
            .cast(pl.Float64, strict=False)
            .alias("proficiencia"),
        )

        n0 = fato.height
        # tratamento de valores ausentes: descarta proficiência/ano nulos
        fato = fato.filter(pl.col("proficiencia").is_not_null() & pl.col("ano").is_not_null())
        n_nulos = n0 - fato.height

        # deduplicação por (id_aluno, ano) — mantém a última medição
        n1 = fato.height
        fato = fato.unique(subset=["id_aluno", "ano"], keep="last")
        n_dup = n1 - fato.height

        # integridade referencial: remove alunos com município órfão
        n2 = fato.height
        fato = fato.join(mun.select("id_municipio"), on="id_municipio", how="semi")
        n_orfaos = n2 - fato.height

        # regra de negócio: alfabetizado se proficiência >= ponto de corte (743)
        fato = fato.with_columns(
            (pl.col("proficiencia") >= ponto_corte).alias("alfabetizado")
        )

        log.info(
            "limpeza fato_indicador: -%d nulos, -%d duplicatas, -%d órfãos",
            n_nulos, n_dup, n_orfaos,
        )

        rep = QualityReport("fato_indicador")
        rep.add(check_no_duplicates(fato, ["id_aluno", "ano"]))
        rep.add(check_not_null(fato, "proficiencia", max_null_ratio=0.0))
        rep.add(check_range(fato, "proficiencia", prof_min, prof_max))
        rep.add(check_foreign_key(fato, "id_municipio", mun, "id_municipio"))
        _enforce(rep)

        write_table(fato, "silver", "fato_indicador", partition_by=["ano"], cfg=cfg)
        m.rows_out = fato.height


def _build_meta_brasil(cfg: Config) -> int:
    df = read_table("bronze", "meta_brasil", cfg=cfg).select("ano", "meta_alfabetizacao")
    df = df.with_columns(
        pl.col("ano").cast(pl.Int32, strict=False),
        pl.col("meta_alfabetizacao").cast(pl.Float64, strict=False),
    ).unique(subset=["ano"])
    rep = QualityReport("meta_brasil")
    rep.add(check_no_duplicates(df, ["ano"]))
    rep.add(check_range(df, "meta_alfabetizacao", 0.0, 1.0))
    _enforce(rep)
    write_table(df, "silver", "meta_brasil", cfg=cfg)
    return df.height


def _build_meta_uf(cfg: Config, uf: pl.DataFrame) -> int:
    df = read_table("bronze", "meta_uf", cfg=cfg).select("ano", "sigla_uf", "meta_alfabetizacao")
    df = df.with_columns(
        pl.col("ano").cast(pl.Int32, strict=False),
        pl.col("sigla_uf").str.to_uppercase().str.strip_chars(),
        pl.col("meta_alfabetizacao").cast(pl.Float64, strict=False),
    ).unique(subset=["ano", "sigla_uf"])
    rep = QualityReport("meta_uf")
    rep.add(check_no_duplicates(df, ["ano", "sigla_uf"]))
    rep.add(check_foreign_key(df, "sigla_uf", uf, "sigla_uf"))
    rep.add(check_range(df, "meta_alfabetizacao", 0.0, 1.0))
    _enforce(rep)
    write_table(df, "silver", "meta_uf", cfg=cfg)
    return df.height


def _build_meta_municipio(cfg: Config, mun: pl.DataFrame) -> int:
    df = read_table("bronze", "meta_municipio", cfg=cfg).select("ano", "id_municipio", "meta_alfabetizacao")
    df = df.with_columns(
        pl.col("ano").cast(pl.Int32, strict=False),
        pl.col("id_municipio").str.strip_chars().str.zfill(7),
        pl.col("meta_alfabetizacao").cast(pl.Float64, strict=False),
    ).unique(subset=["ano", "id_municipio"])
    rep = QualityReport("meta_municipio")
    rep.add(check_no_duplicates(df, ["ano", "id_municipio"]))
    rep.add(check_foreign_key(df, "id_municipio", mun, "id_municipio"))
    rep.add(check_range(df, "meta_alfabetizacao", 0.0, 1.0))
    _enforce(rep)
    write_table(df, "silver", "meta_municipio", cfg=cfg)
    return df.height


if __name__ == "__main__":
    mc = MetricsCollector()
    build_silver(metrics=mc)
    mc.flush()
    print(mc.summary())
