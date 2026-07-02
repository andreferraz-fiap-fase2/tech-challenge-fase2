"""Transformação BRONZE → SILVER (dados tratados + integração + qualidade).

Trabalha sobre o schema REAL do dataset `br_inep_avaliacao_alfabetizacao`.

Responsabilidades (conforme o desafio):
  - Limpeza e tratamento de valores ausentes
  - Padronização de nomes e tipos (casting) e decodificação de códigos (dicionário)
  - Normalização de chaves (id_municipio 7 dígitos, sigla_uf maiúscula)
  - Validação de consistência e de chaves de relacionamento (FK)
  - Deduplicação
  - Integração das bases (microdados de alunos batch+streaming; fatos + dimensões;
    metas "largas" convertidas para formato longo)

Executa as regras de qualidade e persiste um relatório por tabela; falhas
bloqueantes interrompem o pipeline (fail-fast).
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

# Decodificação de `rede` (fonte: tabela `dicionario` do dataset)
REDE_MAP = {
    "0": "Total", "1": "Federal", "2": "Estadual", "3": "Municipal",
    "4": "Privada", "5": "Pública (Est+Mun)", "6": "Pública (Fed+Est+Mun)",
}
META_COLS = [f"meta_alfabetizacao_{a}" for a in range(2024, 2031)]


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


def _decode_rede(col: str = "rede") -> pl.Expr:
    return pl.col(col).cast(pl.Utf8).str.strip_chars().replace(REDE_MAP).alias("rede_nome")


def build_silver(cfg: Config | None = None, metrics: MetricsCollector | None = None) -> None:
    cfg = cfg or load_config()
    metrics = metrics or MetricsCollector(cfg)
    prof_min = float(cfg.get("quality.proficiencia_min", 0))
    prof_max = float(cfg.get("quality.proficiencia_max", 1000))
    taxa_min = float(cfg.get("quality.taxa_min", 0))
    taxa_max = float(cfg.get("quality.taxa_max", 100))

    uf_dim = _build_dim_uf(cfg, metrics)
    mun_dim = _build_dim_municipio(cfg, metrics, uf_dim)
    _build_fato_aluno(cfg, metrics, mun_dim, prof_min, prof_max)
    _build_fato_local(cfg, metrics, "municipio", "fato_municipio", ["id_municipio"], taxa_min, taxa_max)
    _build_fato_local(cfg, metrics, "uf", "fato_uf", ["sigla_uf"], taxa_min, taxa_max)
    _build_metas(cfg, metrics)


# --------------------------------------------------------------------------- #
# Dimensões
# --------------------------------------------------------------------------- #
def _build_dim_uf(cfg, metrics, ) -> pl.DataFrame:
    with metrics.stage("silver:dim_uf", "silver") as m:
        uf = read_table("bronze", "diretorio_uf", cfg=cfg)
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
        return uf


def _build_dim_municipio(cfg, metrics, uf_dim) -> pl.DataFrame:
    with metrics.stage("silver:dim_municipio", "silver") as m:
        mun = read_table("bronze", "diretorio_municipio", cfg=cfg)
        m.rows_in = mun.height
        mun = (
            mun.select("id_municipio", "nome_municipio", "sigla_uf")
            .with_columns(
                pl.col("id_municipio").cast(pl.Utf8).str.strip_chars().str.zfill(7),
                pl.col("nome_municipio").str.strip_chars(),
                pl.col("sigla_uf").str.to_uppercase().str.strip_chars(),
            )
            .unique(subset=["id_municipio"])
        )
        rep = QualityReport("dim_municipio")
        rep.add(check_no_duplicates(mun, ["id_municipio"]))
        rep.add(check_not_null(mun, "id_municipio"))
        rep.add(check_foreign_key(mun, "sigla_uf", uf_dim, "sigla_uf"))
        _enforce(rep)
        write_table(mun, "silver", "dim_municipio", cfg=cfg)
        m.rows_out = mun.height
        return mun


# --------------------------------------------------------------------------- #
# Fato: microdados de alunos (INTEGRA batch + streaming)
# --------------------------------------------------------------------------- #
def _build_fato_aluno(cfg, metrics, mun_dim, prof_min, prof_max) -> None:
    cols = ["ano", "id_municipio", "id_escola", "id_aluno", "serie", "rede",
            "presenca", "alfabetizado", "proficiencia", "peso_aluno"]
    with metrics.stage("silver:fato_aluno", "silver") as m:
        batch = read_table("bronze", "alunos", cfg=cfg).select(cols).with_columns(
            pl.lit("batch").alias("origem"))
        frames = [batch]
        if table_exists("bronze", "alunos_stream", cfg=cfg):
            stream = read_table("bronze", "alunos_stream", cfg=cfg).select(cols).with_columns(
                pl.lit("streaming").alias("origem"))
            frames.append(stream)
        fato = pl.concat(frames, how="vertical_relaxed")
        m.rows_in = fato.height

        fato = fato.with_columns(
            pl.col("ano").cast(pl.Int32, strict=False),
            pl.col("id_municipio").cast(pl.Utf8).str.strip_chars().str.zfill(7),
            pl.col("id_aluno").cast(pl.Utf8).str.strip_chars(),
            (pl.col("presenca").cast(pl.Utf8) == "1").alias("presente"),
            (pl.col("alfabetizado").cast(pl.Utf8) == "1").alias("alfabetizado"),
            pl.col("proficiencia").cast(pl.Float64, strict=False),
            pl.col("peso_aluno").cast(pl.Float64, strict=False),
            _decode_rede(),
        ).drop("presenca")

        # deduplicação por (id_aluno, ano)
        n0 = fato.height
        fato = fato.unique(subset=["id_aluno", "ano"], keep="last")
        n_dup = n0 - fato.height
        # integridade referencial: remove alunos com município fora do diretório
        n1 = fato.height
        fato = fato.join(mun_dim.select("id_municipio"), on="id_municipio", how="semi")
        n_orfaos = n1 - fato.height
        log.info("limpeza fato_aluno: -%d duplicatas, -%d órfãos (município)", n_dup, n_orfaos)

        rep = QualityReport("fato_aluno")
        rep.add(check_no_duplicates(fato, ["id_aluno", "ano"]))
        rep.add(check_not_null(fato, "id_municipio"))
        rep.add(check_foreign_key(fato, "id_municipio", mun_dim, "id_municipio"))
        # proficiência é nula para ausentes (esperado); valida só a faixa dos presentes
        rep.add(check_range(fato, "proficiencia", prof_min, prof_max))
        _enforce(rep)

        write_table(fato, "silver", "fato_aluno", partition_by=["ano"], cfg=cfg)
        m.rows_out = fato.height


# --------------------------------------------------------------------------- #
# Fato: indicador oficial já agregado (municipio / uf)
# --------------------------------------------------------------------------- #
def _build_fato_local(cfg, metrics, source, target, key_cols, taxa_min, taxa_max) -> None:
    with metrics.stage(f"silver:{target}", "silver") as m:
        df = read_table("bronze", source, cfg=cfg)
        m.rows_in = df.height
        key = key_cols[0]
        df = df.with_columns(
            pl.col("ano").cast(pl.Int32, strict=False),
            pl.col("serie").cast(pl.Utf8).str.strip_chars(),
            pl.col("rede").cast(pl.Utf8).str.strip_chars(),
            pl.col("taxa_alfabetizacao").cast(pl.Float64, strict=False),
            pl.col("media_portugues").cast(pl.Float64, strict=False),
            _decode_rede(),
        )
        if key == "id_municipio":
            df = df.with_columns(pl.col("id_municipio").cast(pl.Utf8).str.strip_chars().str.zfill(7))
        else:
            df = df.with_columns(pl.col("sigla_uf").str.to_uppercase().str.strip_chars())

        pk = ["ano", key, "serie", "rede"]
        df = df.unique(subset=pk)
        rep = QualityReport(target)
        rep.add(check_no_duplicates(df, pk))
        rep.add(check_range(df, "taxa_alfabetizacao", taxa_min, taxa_max))
        _enforce(rep)
        write_table(df, "silver", target, partition_by=["ano"], cfg=cfg)
        m.rows_out = df.height


# --------------------------------------------------------------------------- #
# Metas: converte formato "largo" (meta_2024..2030) para longo
# --------------------------------------------------------------------------- #
def _unpivot_metas(df: pl.DataFrame, id_cols: list[str]) -> pl.DataFrame:
    present = [c for c in META_COLS if c in df.columns]
    long = (
        df.select(id_cols + present)
        .unpivot(index=id_cols, on=present, variable_name="ano_meta", value_name="meta_alfabetizacao")
        .with_columns(
            pl.col("ano_meta").str.extract(r"(\d{4})").cast(pl.Int32),
            pl.col("meta_alfabetizacao").cast(pl.Float64, strict=False),
        )
        .drop_nulls("meta_alfabetizacao")
    )
    return long


def _build_metas(cfg, metrics) -> None:
    with metrics.stage("silver:metas", "silver") as m:
        rows = 0

        # Brasil: 1 linha por ano-obs; usamos a última observação para a trajetória de metas
        mb = read_table("bronze", "meta_alfabetizacao_brasil", cfg=cfg)
        mb_long = _unpivot_metas(mb.filter(pl.col("ano") == mb["ano"].max()), []).unique("ano_meta")
        write_table(mb_long, "silver", "meta_brasil_long", cfg=cfg)
        rows += mb_long.height

        # Também guardamos a taxa observada nacional por ano (indicador oficial Brasil)
        mb_obs = mb.select(
            pl.col("ano").cast(pl.Int32),
            pl.col("taxa_alfabetizacao").cast(pl.Float64),
        ).unique("ano")
        write_table(mb_obs, "silver", "taxa_brasil", cfg=cfg)
        rows += mb_obs.height

        # UF: metas por sigla_uf (última observação por UF)
        mu = read_table("bronze", "meta_alfabetizacao_uf", cfg=cfg).with_columns(
            pl.col("sigla_uf").str.to_uppercase().str.strip_chars())
        mu_last = mu.sort("ano").group_by("sigla_uf").last()
        mu_long = _unpivot_metas(mu_last, ["sigla_uf"])
        rep = QualityReport("meta_uf")
        rep.add(check_no_duplicates(mu_long, ["sigla_uf", "ano_meta"]))
        rep.add(check_range(mu_long, "meta_alfabetizacao", 0.0, 100.0))
        _enforce(rep)
        write_table(mu_long, "silver", "meta_uf_long", cfg=cfg)
        rows += mu_long.height

        # Município: metas por id_municipio (última observação por município)
        mm = read_table("bronze", "meta_alfabetizacao_municipio", cfg=cfg).with_columns(
            pl.col("id_municipio").cast(pl.Utf8).str.strip_chars().str.zfill(7))
        mm_last = mm.sort("ano").group_by("id_municipio").last()
        mm_long = _unpivot_metas(mm_last, ["id_municipio"])
        rep = QualityReport("meta_municipio")
        rep.add(check_no_duplicates(mm_long, ["id_municipio", "ano_meta"]))
        rep.add(check_range(mm_long, "meta_alfabetizacao", 0.0, 100.0))
        _enforce(rep)
        write_table(mm_long, "silver", "meta_municipio_long", cfg=cfg)
        rows += mm_long.height

        m.rows_out = rows


if __name__ == "__main__":
    mc = MetricsCollector()
    build_silver(metrics=mc)
    mc.flush()
    print(mc.summary())
