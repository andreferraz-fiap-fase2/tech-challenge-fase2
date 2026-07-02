"""Ingestão STREAMING (simulada) → camada Bronze.

Simula um tópico Pub/Sub (arquivo JSONL) com **novos eventos de medição de
alunos** — novas provas aplicadas / resultados chegando em tempo quase real,
no mesmo schema da tabela `alunos`.

  producer  → publica eventos (novas medições de proficiência)
  consumer  → lê em micro-batches e persiste na Bronze (`alunos_stream`),
              medindo latência por micro-batch.

Mapeamento para produção (GCP): producer ≈ publish em tópico Pub/Sub;
consumer ≈ Dataflow/Cloud Function assinando o tópico e gravando no GCS.
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

ANOS = [2023, 2024]
REDES = ["2", "3", "4"]  # Estadual, Municipal, Privada (como na base real)


def _topic_path(cfg: Config) -> Path:
    return REPO_ROOT / cfg.get("streaming.topic_file", "data/streaming/topic_alunos.jsonl")


def produce_events(n_events: int = 2000, cfg: Config | None = None, seed: int = 7) -> Path:
    """Publica `n_events` eventos de medição de alunos no tópico (append)."""
    cfg = cfg or load_config()
    rng = random.Random(seed)
    topic = _topic_path(cfg)
    topic.parent.mkdir(parents=True, exist_ok=True)

    # Usa municípios reais já ingeridos na Bronze para gerar FKs válidas.
    try:
        muni_ids = read_table("bronze", "diretorio_municipio", cfg=cfg)["id_municipio"].to_list()
    except FileNotFoundError:
        muni_ids = ["3550308", "3304557", "2927408"]

    with open(topic, "a", encoding="utf-8") as fh:
        for _ in range(n_events):
            ano = rng.choice(ANOS)
            presente = rng.random() > 0.12          # ~12% ausentes (como na base real)
            # proficiência sobe levemente entre 2023 e 2024
            prof = round(rng.gauss(735 + (ano - 2023) * 10, 60), 2) if presente else None
            if prof is not None:
                prof = max(400.0, min(950.0, prof))
            alfabetizado = "1" if (prof is not None and prof >= 743) else "0"
            evt = {
                "event_id": f"EVT-{datetime.now(timezone.utc):%Y%m%d}-{rng.randint(0, 10**9):09d}",
                "event_ts": datetime.now(timezone.utc).isoformat(),
                "ano": ano,
                "id_municipio": rng.choice(muni_ids),
                "id_escola": f"S{rng.randint(0, 10**8):08d}",
                "id_aluno": f"STREAM{rng.randint(0, 10**9):09d}",
                "caderno": str(rng.randint(1, 8)),
                "serie": "2",
                "rede": rng.choice(REDES),
                "presenca": "1" if presente else "0",
                "preenchimento_caderno": "1" if presente else "0",
                "alfabetizado": alfabetizado,
                "proficiencia": prof,
                "peso_aluno": round(rng.uniform(0.8, 1.5), 4) if presente else None,
            }
            fh.write(json.dumps(evt, ensure_ascii=False) + "\n")
    log.info("publicados %d eventos no tópico %s", n_events, topic.name)
    return topic


def consume_stream(cfg: Config | None = None, metrics: MetricsCollector | None = None) -> None:
    """Consome o tópico em micro-batches e persiste na Bronze (`alunos_stream`)."""
    cfg = cfg or load_config()
    metrics = metrics or MetricsCollector(cfg)
    topic = _topic_path(cfg)
    batch_size = int(cfg.get("streaming.micro_batch_size", 500))

    with metrics.stage("bronze_stream:consume", "bronze") as m:
        if not topic.exists():
            log.warning("nenhum evento no tópico; pulando streaming")
            m.rows_out = 0
            return

        with open(topic, "r", encoding="utf-8") as fh:
            lines = [ln for ln in fh if ln.strip()]
        m.rows_in = len(lines)

        events: list[dict] = []
        n_batches = 0
        for start in range(0, len(lines), batch_size):
            chunk = lines[start:start + batch_size]
            t0 = time.time()
            events.extend(json.loads(ln) for ln in chunk)
            n_batches += 1
            log.info("micro-batch %d: %d eventos em %.1f ms",
                     n_batches, len(chunk), (time.time() - t0) * 1000)

        if not events:
            m.rows_out = 0
            return

        df = pl.DataFrame(events).with_columns(
            pl.lit(datetime.now(timezone.utc).isoformat()).alias("_ingested_at"),
            pl.lit("pubsub:topic_alunos").alias("_source"),
            pl.lit("streaming").alias("_ingestion_type"),
        )
        write_table(df, "bronze", "alunos_stream", cfg=cfg)
        m.rows_out = df.height
        log.info("streaming consolidado: %d eventos em %d micro-batches", df.height, n_batches)


if __name__ == "__main__":
    mc = MetricsCollector()
    produce_events(2000)
    consume_stream(metrics=mc)
    mc.flush()
    print(mc.summary())
