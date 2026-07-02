"""Orquestrador do pipeline híbrido (CLI).

Subcomandos:
  generate      gera dados sintéticos na landing zone
  ingest-batch  landing -> Bronze (batch)
  stream        produz + consome eventos -> Bronze (streaming)
  silver        Bronze -> Silver (limpeza, integração, qualidade)
  gold          Silver -> Gold (camada analítica)
  run-all       executa o pipeline completo fim-a-fim
  report        imprime um resumo dos datasets Gold

Uso:
  python -m src.pipeline run-all
  python -m src.pipeline run-all --students 20000 --events 500
"""

from __future__ import annotations

import argparse
import sys

from .common.config import load_config
from .common.lake_io import read_table
from .common.logging_setup import configure_utf8_stdio, get_logger
from .ingestion.batch import ingest_batch
from .ingestion.streaming import consume_stream, produce_events
from .monitoring.metrics import MetricsCollector
from .transform.gold import build_gold
from .transform.silver import build_silver

log = get_logger("pipeline")


def cmd_generate(args: argparse.Namespace) -> None:
    from scripts.generate_sample_data import generate
    generate(students=args.students, per_uf=args.per_uf, seed=args.seed)


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
    from scripts.generate_sample_data import generate

    mc = MetricsCollector()
    log.info("========== PIPELINE HÍBRIDO — EXECUÇÃO COMPLETA ==========")

    if not args.skip_generate:
        generate(students=args.students, per_uf=args.per_uf, seed=args.seed)

    # Ingestão híbrida
    ingest_batch(metrics=mc)
    produce_events(n_events=args.events)
    consume_stream(metrics=mc)

    # Medalhão
    build_silver(metrics=mc)
    build_gold(metrics=mc)

    mc.flush()
    print(mc.summary())
    if mc.failed:
        sys.exit(1)
    cmd_report(args)


def cmd_report(_args: argparse.Namespace) -> None:
    cfg = load_config()
    print("\n" + "=" * 66)
    print(" AMOSTRA DA CAMADA GOLD")
    print("=" * 66)
    try:
        br = read_table("gold", "indicador_brasil", cfg=cfg).sort("ano")
        print("\n▶ Indicador nacional de alfabetização (Brasil) por ano:")
        with_cols = ["ano", "total_alunos", "pct_alfabetizado", "meta_alfabetizacao", "gap_meta", "atingiu_meta"]
        print(br.select([c for c in with_cols if c in br.columns]))

        import polars as pl
        uf = read_table("gold", "indicador_uf", cfg=cfg)
        ult = uf["ano"].max()
        print(f"\n▶ Top 5 UFs por % alfabetizado em {ult}:")
        print(
            uf.filter(pl.col("ano") == ult)
            .sort("pct_alfabetizado", descending=True)
            .select("sigla_uf", "nome_uf", "regiao", "pct_alfabetizado", "atingiu_meta")
            .head(5)
        )
    except FileNotFoundError:
        print("Camada Gold ainda não construída. Rode: python -m src.pipeline run-all")
    print("=" * 66)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="pipeline", description="Pipeline híbrido de alfabetização (FIAP Fase 2).")
    sub = p.add_subparsers(dest="command", required=True)

    def add_gen_args(sp):
        sp.add_argument("--students", type=int, default=8000)
        sp.add_argument("--per-uf", type=int, default=3)
        sp.add_argument("--seed", type=int, default=42)

    g = sub.add_parser("generate", help="gera dados sintéticos")
    add_gen_args(g)
    g.set_defaults(func=cmd_generate)

    sub.add_parser("ingest-batch", help="landing -> Bronze").set_defaults(func=cmd_ingest_batch)

    s = sub.add_parser("stream", help="produz + consome eventos -> Bronze")
    s.add_argument("--events", type=int, default=200)
    s.set_defaults(func=cmd_stream)

    sub.add_parser("silver", help="Bronze -> Silver").set_defaults(func=cmd_silver)
    sub.add_parser("gold", help="Silver -> Gold").set_defaults(func=cmd_gold)
    sub.add_parser("report", help="resumo da camada Gold").set_defaults(func=cmd_report)

    r = sub.add_parser("run-all", help="pipeline completo fim-a-fim")
    add_gen_args(r)
    r.add_argument("--events", type=int, default=200)
    r.add_argument("--skip-generate", action="store_true", help="não regerar dados")
    r.set_defaults(func=cmd_run_all)

    return p


def main(argv: list[str] | None = None) -> None:
    configure_utf8_stdio()
    args = build_parser().parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
