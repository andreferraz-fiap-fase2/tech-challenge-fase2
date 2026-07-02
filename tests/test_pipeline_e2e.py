"""Teste de fumaça end-to-end sobre os dados REAIS (Bronze→Silver→Gold).

Requer os Parquet reais em data/real (baixados via scripts/bq_download.py).
Se não existirem, o teste é pulado (ex.: ambiente de CI sem credencial GCP).
"""

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import polars as pl

from src.common.config import REPO_ROOT, load_config

REAL_DATA = REPO_ROOT / "data" / "real" / "alunos.parquet"


@unittest.skipUnless(REAL_DATA.exists(), "dados reais ausentes em data/real")
class TestPipelineEndToEnd(unittest.TestCase):
    def test_run_produces_valid_gold(self):
        from src.ingestion.batch import ingest_batch
        from src.monitoring.metrics import MetricsCollector
        from src.transform.gold import build_gold
        from src.transform.silver import build_silver

        cfg = load_config()
        with TemporaryDirectory() as tmp:
            for layer in ("bronze", "silver", "gold"):
                cfg.data["lake"][layer] = str(Path(tmp) / layer)

            mc = MetricsCollector(cfg)
            ingest_batch(cfg=cfg, metrics=mc)        # data/real -> Bronze
            build_silver(cfg=cfg, metrics=mc)        # Bronze -> Silver (+ qualidade)
            build_gold(cfg=cfg, metrics=mc)          # Silver -> Gold

            self.assertFalse(mc.failed, "pipeline não deveria falhar")

            br = pl.read_parquet(Path(tmp) / "gold" / "indicador_brasil.parquet")
            self.assertGreater(br.height, 0)
            taxa = br["taxa_alfabetizacao"]
            self.assertGreaterEqual(taxa.min(), 0.0)
            self.assertLessEqual(taxa.max(), 100.0)   # escala 0-100

            # a validação microdado×oficial deve estar próxima (< 5 p.p. em média)
            val = pl.read_parquet(Path(tmp) / "gold" / "validacao_microdado" / "**" / "*.parquet")
            self.assertLess(val["diferenca"].abs().mean(), 5.0)


if __name__ == "__main__":
    unittest.main()
