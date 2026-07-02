"""Teste de fumaça end-to-end: gera dados, roda o pipeline e valida a Gold.

Usa um data lake temporário para não sujar `data/`.
"""

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import polars as pl


class TestPipelineEndToEnd(unittest.TestCase):
    def test_run_all_produces_valid_gold(self):
        from src.common.config import load_config
        from src.ingestion.batch import ingest_batch
        from src.ingestion.streaming import consume_stream, produce_events
        from src.monitoring.metrics import MetricsCollector
        from src.transform.gold import build_gold
        from src.transform.silver import build_silver
        from scripts.generate_sample_data import generate

        cfg = load_config()
        with TemporaryDirectory() as tmp:
            # Redireciona o lake para um diretório temporário.
            for layer in ("bronze", "silver", "gold"):
                cfg.data["lake"][layer] = str(Path(tmp) / layer)

            generate(students=500, per_uf=2, seed=1)
            mc = MetricsCollector(cfg)
            ingest_batch(cfg=cfg, metrics=mc)
            produce_events(n_events=50, cfg=cfg)
            consume_stream(cfg=cfg, metrics=mc)
            build_silver(cfg=cfg, metrics=mc)
            build_gold(cfg=cfg, metrics=mc)

            self.assertFalse(mc.failed, "pipeline não deveria falhar")

            gold_br = pl.read_parquet(Path(tmp) / "gold" / "indicador_brasil.parquet")
            self.assertGreater(gold_br.height, 0)
            # pct_alfabetizado deve estar no intervalo [0, 1]
            pct = gold_br["pct_alfabetizado"]
            self.assertGreaterEqual(pct.min(), 0.0)
            self.assertLessEqual(pct.max(), 1.0)


if __name__ == "__main__":
    unittest.main()
