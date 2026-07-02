"""Ingestão STREAMING (simulada) → camada Bronze.

Simula um tópico Pub/Sub usando um arquivo JSONL (`topic_indicador.jsonl`):

  producer  → publica eventos (novas medições de proficiência / atualizações)
  consumer  → lê em micro-batches e persiste na Bronze (`indicador_stream`),
              medindo latência por micro-batch.

Mapeamento para produção (GCP):
  producer  ≈ serviço que publica em um tópico Pub/Sub
  consumer  ≈ Dataflow / Cloud Function assinando o tópico e escrevendo no GCS

Mantém near-real-time sem depender de infra de nuvem para rodar localmente.
"""

from __future__ import annotations

import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from ..common.config import REPO_ROOT, Config, load_config
from ..common.lake_io import read_table, write_table
from ..common.logging_setup import get_logger
from ..monitoring.metrics import MetricsCollector

log = get_logger("ingestion.streaming")

YEARS = [2021, 2022, 2023, 2024]


def _topic_path(cfg: Config) -> Path:
    return REPO_ROOT / cfg.get("streaming.topic_file", "data/streaming/topic_indicador.jsonl")


def produce_events(n_events: int = 200, cfg: Config | None = None, seed: int = 7) -> Path:
    """Publica `n_events` eventos no tópico (append), simulando produtores externos."""
    cfg = cfg or load_config()
    rng = random.Random(seed)
    topic = _topic_path(cfg)
    topic.parent.mkdir(parents=True, exist_ok=True)

    # Usa municípios reais já ingeridos na Bronze para gerar FKs válidas.
    try:
        muni_ids = read_table("bronze", "municipio", cfg=cfg)["id_municipio"].unique().to_list()
    except FileNotFoundError:
        muni_ids = ["3500001", "3300001", "2900001"]

    with open(topic, "a", encoding="utf-8") as fh:
        for i in range(n_events):
            ano = rng.choice(YEARS)
            evt = {
                "event_id": f"EVT-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{rng.randint(0, 10**9):09d}",
                "event_ts": datetime.now(timezone.utc).isoformat(),
                "event_type": rng.choice(["nova_medicao", "atualizacao_indicador"]),
                "ano": ano,
                "id_municipio": rng.choice(muni_ids),
                "id_aluno": f"S{rng.randint(0, 10**7):07d}",
                "proficiencia": round(max(400.0, min(950.0, rng.gauss(725 + (ano - 2021) * 12, 55))), 1),
            }
            fh.write(json.dumps(evt, ensure_ascii=False) + "\n")
    log.info("publicados %d eventos no tópico %s", n_events, topic.name)
    return topic


def consume_stream(cfg: Config | None = None, metrics: MetricsCollector | None = None) -> None:
    """Consome o tópico em micro-batches e persiste na Bronze (`indicador_stream`)."""
    cfg = cfg or load_config()
    metrics = metrics or MetricsCollector(cfg)
    topic = _topic_path(cfg)
    batch_size = int(cfg.get("streaming.micro_batch_size", 50))

    with metrics.stage("bronze_stream:consume", "bronze") as m:
        if not topic.exists():
            log.warning("nenhum evento no tópico; pulando streaming")
            m.rows_out = 0
            return

        events: list[dict] = []
        with open(topic, "r", encoding="utf-8") as fh:
            lines = [ln for ln in fh if ln.strip()]
        m.rows_in = len(lines)

        # Processa em micro-batches para simular consumo contínuo e medir latência.
        n_batches = 0
        for start in range(0, len(lines), batch_size):
            chunk = lines[start:start + batch_size]
            t0 = time.time()
            for ln in chunk:
                events.append(json.loads(ln))
            n_batches += 1
            log.info(
                "micro-batch %d: %d eventos processados em %.1f ms",
                n_batches, len(chunk), (time.time() - t0) * 1000,
            )

        if not events:
            m.rows_out = 0
            return

        df = pl.DataFrame(events).with_columns(
            pl.lit(datetime.now(timezone.utc).isoformat()).alias("_ingested_at"),
            pl.lit("topic_indicador.jsonl").alias("_source_file"),
            pl.lit("streaming").alias("_ingestion_type"),
        )
        write_table(df, "bronze", "indicador_stream", cfg=cfg)
        m.rows_out = df.height
        log.info("streaming consolidado: %d eventos em %d micro-batches", df.height, n_batches)


if __name__ == "__main__":
    mc = MetricsCollector()
    produce_events(200)
    consume_stream(metrics=mc)
    mc.flush()
    print(mc.summary())
