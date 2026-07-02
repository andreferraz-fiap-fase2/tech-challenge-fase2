"""Transformação SILVER → GOLD (camada analítica).

Cria datasets prontos para dashboards, análises estatísticas e ML:
  - gold_indicador_municipio : indicador de alfabetização por município/ano + gap vs meta
  - gold_indicador_uf        : agregado por UF/ano + gap vs meta
  - gold_indicador_brasil    : agregado nacional por ano + gap vs meta
  - gold_evolucao_temporal   : evolução YoY do indicador por UF
  - gold_ml_features         : tabela larga (uma linha por município/ano) para ML

Todas as métricas derivam do ponto de corte oficial do Saeb (743): um aluno é
`alfabetizado` quando proficiência >= 743, e o indicador é o % de alfabetizados.
"""

from __future__ import annotations

import polars as pl

from ..common.config import Config, load_config
from ..common.lake_io import read_table, write_table
from ..common.logging_setup import get_logger
from ..monitoring.metrics import MetricsCollector

log = get_logger("transform.gold")


def build_gold(cfg: Config | None = None, metrics: MetricsCollector | None = None) -> None:
    cfg = cfg or load_config()
    metrics = metrics or MetricsCollector(cfg)

    fato = read_table("silver", "fato_indicador", cfg=cfg)
    dim_mun = read_table("silver", "dim_municipio", cfg=cfg)
    dim_uf = read_table("silver", "dim_uf", cfg=cfg)
    meta_mun = read_table("silver", "meta_municipio", cfg=cfg)
    meta_uf = read_table("silver", "meta_uf", cfg=cfg)
    meta_br = read_table("silver", "meta_brasil", cfg=cfg)

    # ------------------------------------------------------------------ #
    # 1) Indicador por município/ano
    # ------------------------------------------------------------------ #
    with metrics.stage("gold:indicador_municipio", "gold") as m:
        m.rows_in = fato.height
        ind_mun = (
            fato.group_by("id_municipio", "ano")
            .agg(
                pl.len().alias("total_alunos"),
                pl.col("alfabetizado").sum().alias("total_alfabetizados"),
                pl.col("proficiencia").mean().round(1).alias("proficiencia_media"),
            )
            .with_columns(
                (pl.col("total_alfabetizados") / pl.col("total_alunos"))
                .round(4).alias("pct_alfabetizado")
            )
            # enriquecimento com dimensões
            .join(dim_mun, on="id_municipio", how="left")
            .join(dim_uf.select("sigla_uf", "nome_uf", "regiao"), on="sigla_uf", how="left")
            # comparação meta vs resultado
            .join(meta_mun, on=["id_municipio", "ano"], how="left")
            .with_columns(
                (pl.col("pct_alfabetizado") - pl.col("meta_alfabetizacao"))
                .round(4).alias("gap_meta"),
                (pl.col("pct_alfabetizado") >= pl.col("meta_alfabetizacao"))
                .alias("atingiu_meta"),
            )
            .sort("ano", "sigla_uf", "id_municipio")
        )
        write_table(ind_mun, "gold", "indicador_municipio", partition_by=["ano"], cfg=cfg)
        m.rows_out = ind_mun.height

    # ------------------------------------------------------------------ #
    # 2) Indicador por UF/ano
    # ------------------------------------------------------------------ #
    with metrics.stage("gold:indicador_uf", "gold") as m:
        fato_uf = fato.join(dim_mun.select("id_municipio", "sigla_uf"), on="id_municipio", how="left")
        ind_uf = (
            fato_uf.group_by("sigla_uf", "ano")
            .agg(
                pl.len().alias("total_alunos"),
                pl.col("alfabetizado").sum().alias("total_alfabetizados"),
                pl.col("proficiencia").mean().round(1).alias("proficiencia_media"),
            )
            .with_columns(
                (pl.col("total_alfabetizados") / pl.col("total_alunos"))
                .round(4).alias("pct_alfabetizado")
            )
            .join(dim_uf, on="sigla_uf", how="left")
            .join(meta_uf, on=["sigla_uf", "ano"], how="left")
            .with_columns(
                (pl.col("pct_alfabetizado") - pl.col("meta_alfabetizacao"))
                .round(4).alias("gap_meta"),
                (pl.col("pct_alfabetizado") >= pl.col("meta_alfabetizacao"))
                .alias("atingiu_meta"),
            )
            .sort("ano", "sigla_uf")
        )
        write_table(ind_uf, "gold", "indicador_uf", cfg=cfg)
        m.rows_out = ind_uf.height

    # ------------------------------------------------------------------ #
    # 3) Indicador nacional por ano
    # ------------------------------------------------------------------ #
    with metrics.stage("gold:indicador_brasil", "gold") as m:
        ind_br = (
            fato.group_by("ano")
            .agg(
                pl.len().alias("total_alunos"),
                pl.col("alfabetizado").sum().alias("total_alfabetizados"),
                pl.col("proficiencia").mean().round(1).alias("proficiencia_media"),
            )
            .with_columns(
                (pl.col("total_alfabetizados") / pl.col("total_alunos"))
                .round(4).alias("pct_alfabetizado")
            )
            .join(meta_br, on="ano", how="left")
            .with_columns(
                (pl.col("pct_alfabetizado") - pl.col("meta_alfabetizacao"))
                .round(4).alias("gap_meta"),
                (pl.col("pct_alfabetizado") >= pl.col("meta_alfabetizacao"))
                .alias("atingiu_meta"),
            )
            .sort("ano")
        )
        write_table(ind_br, "gold", "indicador_brasil", cfg=cfg)
        m.rows_out = ind_br.height

    # ------------------------------------------------------------------ #
    # 4) Evolução temporal (YoY) por UF
    # ------------------------------------------------------------------ #
    with metrics.stage("gold:evolucao_temporal", "gold") as m:
        evo = (
            ind_uf.select("sigla_uf", "nome_uf", "regiao", "ano", "pct_alfabetizado")
            .sort("sigla_uf", "ano")
            .with_columns(
                pl.col("pct_alfabetizado").diff().over("sigla_uf").round(4).alias("variacao_yoy")
            )
        )
        write_table(evo, "gold", "evolucao_temporal", cfg=cfg)
        m.rows_out = evo.height

    # ------------------------------------------------------------------ #
    # 5) Features para ML (uma linha por município/ano)
    # ------------------------------------------------------------------ #
    with metrics.stage("gold:ml_features", "gold") as m:
        feats = (
            ind_mun.select(
                "id_municipio", "nome_municipio", "sigla_uf", "regiao", "ano",
                "total_alunos", "proficiencia_media", "pct_alfabetizado",
                "meta_alfabetizacao", "gap_meta", "atingiu_meta",
            )
            .sort("id_municipio", "ano")
            .with_columns(
                pl.col("pct_alfabetizado").shift().over("id_municipio").alias("pct_alfabetizado_ano_anterior"),
            )
        )
        write_table(feats, "gold", "ml_features", cfg=cfg)
        m.rows_out = feats.height


if __name__ == "__main__":
    mc = MetricsCollector()
    build_gold(metrics=mc)
    mc.flush()
    print(mc.summary())
