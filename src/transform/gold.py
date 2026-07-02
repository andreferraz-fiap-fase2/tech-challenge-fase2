"""Transformação SILVER → GOLD (camada analítica) sobre os dados reais.

Datasets prontos para dashboards, estatística e ML:
  - gold_indicador_municipio : indicador oficial por município/ano + meta + gap
  - gold_indicador_uf        : idem por UF
  - gold_indicador_brasil    : taxa nacional por ano + meta + gap
  - gold_evolucao_temporal   : evolução da taxa por UF (2023→2024)
  - gold_validacao_microdado : taxa recalculada dos microdados de alunos vs oficial
  - gold_ml_features         : tabela larga (município × ano) para ML

Indicador = `taxa_alfabetizacao` (% de alunos do 2º ano com proficiência >= 743,
o ponto de corte do Saeb). Escala 0–100. Comparação usa a meta do respectivo ano.
"""

from __future__ import annotations

import polars as pl

from ..common.config import Config, load_config
from ..common.lake_io import read_table
from ..common.logging_setup import get_logger
from ..monitoring.metrics import MetricsCollector
from ..common.lake_io import write_table

log = get_logger("transform.gold")


# A meta nacional é definida para a rede Pública; a rede 'Total' (0) quase não é
# preenchida nos dados. Usamos 'Pública (Est+Mun)' como indicador oficial consolidado.
REDE_INDICADOR = "Pública (Est+Mun)"
REDES_PUBLICAS = {"Estadual", "Municipal"}  # microdados equivalentes à rede pública


def _publica(df: pl.DataFrame) -> pl.DataFrame:
    """Mantém a rede Pública (Estadual+Municipal) — o indicador oficial consolidado."""
    return df.filter(pl.col("rede_nome") == REDE_INDICADOR)


def build_gold(cfg: Config | None = None, metrics: MetricsCollector | None = None) -> None:
    cfg = cfg or load_config()
    metrics = metrics or MetricsCollector(cfg)

    dim_mun = read_table("silver", "dim_municipio", cfg=cfg)
    dim_uf = read_table("silver", "dim_uf", cfg=cfg)

    # ------------------------------------------------------------------ #
    # 1) Indicador por município/ano (oficial) + meta + gap
    # ------------------------------------------------------------------ #
    with metrics.stage("gold:indicador_municipio", "gold") as m:
        fm = _publica(read_table("silver", "fato_municipio", cfg=cfg))
        meta = read_table("silver", "meta_municipio_long", cfg=cfg)
        m.rows_in = fm.height
        ind_mun = (
            fm.select("ano", "id_municipio", "taxa_alfabetizacao", "media_portugues")
            .join(dim_mun, on="id_municipio", how="left")
            .join(dim_uf.select("sigla_uf", "nome_uf", "regiao"), on="sigla_uf", how="left")
            .join(meta, left_on=["id_municipio", "ano"], right_on=["id_municipio", "ano_meta"], how="left")
            .with_columns(
                (pl.col("taxa_alfabetizacao") - pl.col("meta_alfabetizacao")).round(2).alias("gap_meta"),
                (pl.col("taxa_alfabetizacao") >= pl.col("meta_alfabetizacao")).alias("atingiu_meta"),
            )
            .sort("ano", "sigla_uf", "id_municipio")
        )
        write_table(ind_mun, "gold", "indicador_municipio", partition_by=["ano"], cfg=cfg)
        m.rows_out = ind_mun.height

    # ------------------------------------------------------------------ #
    # 2) Indicador por UF/ano
    # ------------------------------------------------------------------ #
    with metrics.stage("gold:indicador_uf", "gold") as m:
        fu = _publica(read_table("silver", "fato_uf", cfg=cfg))
        meta_uf = read_table("silver", "meta_uf_long", cfg=cfg)
        m.rows_in = fu.height
        ind_uf = (
            fu.select("ano", "sigla_uf", "taxa_alfabetizacao", "media_portugues")
            .join(dim_uf, on="sigla_uf", how="left")
            .join(meta_uf, left_on=["sigla_uf", "ano"], right_on=["sigla_uf", "ano_meta"], how="left")
            .with_columns(
                (pl.col("taxa_alfabetizacao") - pl.col("meta_alfabetizacao")).round(2).alias("gap_meta"),
                (pl.col("taxa_alfabetizacao") >= pl.col("meta_alfabetizacao")).alias("atingiu_meta"),
            )
            .sort("ano", "sigla_uf")
        )
        write_table(ind_uf, "gold", "indicador_uf", cfg=cfg)
        m.rows_out = ind_uf.height

    # ------------------------------------------------------------------ #
    # 3) Indicador nacional por ano
    # ------------------------------------------------------------------ #
    with metrics.stage("gold:indicador_brasil", "gold") as m:
        taxa_br = read_table("silver", "taxa_brasil", cfg=cfg)
        meta_br = read_table("silver", "meta_brasil_long", cfg=cfg)
        ind_br = (
            taxa_br.join(meta_br, left_on="ano", right_on="ano_meta", how="left")
            .with_columns(
                (pl.col("taxa_alfabetizacao") - pl.col("meta_alfabetizacao")).round(2).alias("gap_meta"),
                (pl.col("taxa_alfabetizacao") >= pl.col("meta_alfabetizacao")).alias("atingiu_meta"),
            )
            .sort("ano")
        )
        write_table(ind_br, "gold", "indicador_brasil", cfg=cfg)
        m.rows_out = ind_br.height

    # ------------------------------------------------------------------ #
    # 4) Evolução temporal por UF (variação YoY da taxa)
    # ------------------------------------------------------------------ #
    with metrics.stage("gold:evolucao_temporal", "gold") as m:
        evo = (
            ind_uf.select("sigla_uf", "nome_uf", "regiao", "ano", "taxa_alfabetizacao")
            .sort("sigla_uf", "ano")
            .with_columns(
                pl.col("taxa_alfabetizacao").diff().over("sigla_uf").round(2).alias("variacao_yoy")
            )
        )
        write_table(evo, "gold", "evolucao_temporal", cfg=cfg)
        m.rows_out = evo.height

    # ------------------------------------------------------------------ #
    # 5) Validação: taxa recalculada dos microdados de alunos vs oficial
    # ------------------------------------------------------------------ #
    with metrics.stage("gold:validacao_microdado", "gold") as m:
        aluno = read_table("silver", "fato_aluno", cfg=cfg).filter(
            pl.col("presente") & pl.col("rede_nome").is_in(REDES_PUBLICAS))
        m.rows_in = aluno.height
        peso = pl.col("peso_aluno").fill_null(1.0)
        micro = (
            aluno.group_by("id_municipio", "ano")
            .agg(
                pl.len().alias("alunos_avaliados"),
                (100 * (pl.when(pl.col("alfabetizado")).then(peso).otherwise(0.0)).sum()
                 / peso.sum()).round(2).alias("taxa_microdado"),
                pl.col("proficiencia").mean().round(1).alias("proficiencia_media"),
            )
        )
        oficial = _publica(read_table("silver", "fato_municipio", cfg=cfg)).select(
            "id_municipio", "ano", pl.col("taxa_alfabetizacao").alias("taxa_oficial"))
        valida = (
            micro.join(oficial, on=["id_municipio", "ano"], how="inner")
            .with_columns((pl.col("taxa_microdado") - pl.col("taxa_oficial")).round(2).alias("diferenca"))
            .sort("ano", "id_municipio")
        )
        write_table(valida, "gold", "validacao_microdado", partition_by=["ano"], cfg=cfg)
        m.rows_out = valida.height

    # ------------------------------------------------------------------ #
    # 6) Features para ML (município × ano)
    # ------------------------------------------------------------------ #
    with metrics.stage("gold:ml_features", "gold") as m:
        feats = (
            ind_mun.select(
                "id_municipio", "nome_municipio", "sigla_uf", "regiao", "ano",
                "taxa_alfabetizacao", "media_portugues", "meta_alfabetizacao",
                "gap_meta", "atingiu_meta",
            )
            .sort("id_municipio", "ano")
            .with_columns(
                pl.col("taxa_alfabetizacao").shift().over("id_municipio").alias("taxa_ano_anterior"),
            )
        )
        write_table(feats, "gold", "ml_features", cfg=cfg)
        m.rows_out = feats.height


if __name__ == "__main__":
    mc = MetricsCollector()
    build_gold(metrics=mc)
    mc.flush()
    print(mc.summary())
