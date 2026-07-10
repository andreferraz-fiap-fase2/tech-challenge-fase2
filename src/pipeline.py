"""Orquestrador do pipeline híbrido (CLI) — dados reais da Base dos Dados.

Subcomandos:
  download      baixa as tabelas reais do BigQuery para data/real (requer credencial GCP)
  ingest-batch  data/real (Parquet) -> Bronze (batch)
  stream        produz + consome eventos de alunos -> Bronze (streaming)
  silver        Bronze -> Silver (limpeza, integração, qualidade)
  gold          Silver -> Gold (camada analítica)
  run-all       executa o pipeline completo (ingestão -> gold) e imprime resumo
  report        imprime um resumo dos datasets Gold

Uso:
  python -m src.pipeline download --project fiap-fase2   # 1x (baixa os dados)
  python -m src.pipeline run-all
  python -m src.pipeline --cloud run-all    # cloud-native: lake direto no GCS
"""

from __future__ import annotations

import argparse
import os
import sys

import polars as pl

from .common.config import load_config
from .common.lake_io import read_table
from .common.logging_setup import configure_utf8_stdio, get_logger
from .ingestion.batch import ingest_batch
from .ingestion.streaming import consume_stream, produce_events
from .monitoring.metrics import MetricsCollector
from .transform.gold import build_gold
from .transform.silver import build_silver

log = get_logger("pipeline")


def cmd_download(args: argparse.Namespace) -> None:
    from scripts import bq_download
    argv = ["bq_download"] + (["--project", args.project] if args.project else [])
    old, sys.argv = sys.argv, argv
    try:
        bq_download.main()
    finally:
        sys.argv = old


def cmd_ingest_batch(_args: argparse.Namespace) -> None:
    mc = MetricsCollector()
    ingest_batch(metrics=mc)
    mc.flush()
    print(mc.summary())


def cmd_stream(args: argparse.Namespace) -> None:
    mc = MetricsCollector()
    produce_events(n_events=args.events)
    consume_stream(metrics=mc)
    mc.flush()
    print(mc.summary())


def cmd_silver(_args: argparse.Namespace) -> None:
    mc = MetricsCollector()
    build_silver(metrics=mc)
    mc.flush()
    print(mc.summary())


def cmd_gold(_args: argparse.Namespace) -> None:
    mc = MetricsCollector()
    build_gold(metrics=mc)
    mc.flush()
    print(mc.summary())


def cmd_run_all(args: argparse.Namespace) -> None:
    """Executa o pipeline completo com uma única coleta de métricas."""
    cfg = load_config()
    mc = MetricsCollector(cfg)
    log.info("========== PIPELINE HÍBRIDO — EXECUÇÃO COMPLETA (dados reais) ==========")

    # Ingestão híbrida
    ingest_batch(cfg=cfg, metrics=mc)
    if not args.skip_stream:
        # tópico limpo a cada execução completa (reprodutibilidade)
        topic = load_config().get("streaming.topic_file", "data/streaming/topic_alunos.jsonl")
        from pathlib import Path
        from .common.config import REPO_ROOT
        p = REPO_ROOT / topic
        if p.exists():
            p.unlink()
        produce_events(n_events=args.events, cfg=cfg)
        consume_stream(cfg=cfg, metrics=mc)

    # Medalhão
    build_silver(cfg=cfg, metrics=mc)
    build_gold(cfg=cfg, metrics=mc)

    mc.flush()
    print(mc.summary())
    if mc.failed:
        sys.exit(1)
    cmd_report(args)


def cmd_report(_args: argparse.Namespace) -> None:
    cfg = load_config()
    print("\n" + "=" * 70)
    print(" AMOSTRA DA CAMADA GOLD (dados reais — Indicador Criança Alfabetizada)")
    print("=" * 70)
    try:
        br = read_table("gold", "indicador_brasil", cfg=cfg).sort("ano")
        print("\n▶ Indicador nacional de alfabetização (Brasil) por ano [% escala 0-100]:")
        print(br)

        uf = read_table("gold", "indicador_uf", cfg=cfg)
        ult = uf["ano"].max()
        print(f"\n▶ Top 5 UFs por taxa de alfabetização em {ult}:")
        print(
            uf.filter(pl.col("ano") == ult)
            .sort("taxa_alfabetizacao", descending=True)
            .select("sigla_uf", "nome_uf", "regiao", "taxa_alfabetizacao", "meta_alfabetizacao", "atingiu_meta")
            .head(5)
        )

        val = read_table("gold", "validacao_microdado", cfg=cfg)
        d = val["diferenca"].abs().mean()
        print(f"\n▶ Validação microdado×oficial: diferença média absoluta = {d:.2f} p.p. "
              f"({val.height} municípios×ano)")
    except FileNotFoundError:
        print("Camada Gold ainda não construída. Rode: python -m src.pipeline run-all")
    print("=" * 70)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="pipeline", description="Pipeline híbrido de alfabetização (FIAP Fase 2).")
    p.add_argument("--cloud", action="store_true",
                   help="modo cloud-native: lê/grava o lake direto no GCS (equivale a LAKE_MODE=gcs)")
    sub = p.add_subparsers(dest="command", required=True)

    d = sub.add_parser("download", help="baixa as tabelas reais do BigQuery")
    d.add_argument("--project", default=None, help="ID do projeto GCP (billing)")
    d.set_defaults(func=cmd_download)

    sub.add_parser("ingest-batch", help="data/real -> Bronze").set_defaults(func=cmd_ingest_batch)

    s = sub.add_parser("stream", help="produz + consome eventos de alunos -> Bronze")
    s.add_argument("--events", type=int, default=2000)
    s.set_defaults(func=cmd_stream)

    sub.add_parser("silver", help="Bronze -> Silver").set_defaults(func=cmd_silver)
    sub.add_parser("gold", help="Silver -> Gold").set_defaults(func=cmd_gold)
    sub.add_parser("report", help="resumo da camada Gold").set_defaults(func=cmd_report)

    r = sub.add_parser("run-all", help="pipeline completo (ingestão -> gold)")
    r.add_argument("--events", type=int, default=2000)
    r.add_argument("--skip-stream", action="store_true", help="não gerar/consumir streaming")
    r.set_defaults(func=cmd_run_all)

    return p


def main(argv: list[str] | None = None) -> None:
    configure_utf8_stdio()
    args = build_parser().parse_args(argv)
    if args.cloud:
        os.environ["LAKE_MODE"] = "gcs"
        log.info("modo cloud-native: lake em gs:// (LAKE_MODE=gcs)")
    args.func(args)


if __name__ == "__main__":
    main()
