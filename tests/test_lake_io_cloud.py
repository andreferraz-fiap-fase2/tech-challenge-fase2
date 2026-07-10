"""Testes do modo cloud-native do lake (lake.mode = URI fsspec).

Usa o filesystem em memória do fsspec (`memory://`) para exercitar o mesmo
caminho de código do GCS (gs://) sem credencial nem custo de nuvem.
"""

import unittest

import fsspec
import polars as pl

from src.common.config import Config
from src.common.lake_io import read_table, table_exists, write_table


def _cfg() -> Config:
    return Config({"lake": {"mode": "memory://lake", "compression": "zstd"}})


class TestConfigLakeUri(unittest.TestCase):
    def test_local_mode_resolve_para_filesystem(self):
        cfg = Config({"lake": {"mode": "local", "gold": "data/gold"}})
        self.assertTrue(cfg.lake_uri("gold").endswith("data/gold"))

    def test_gcs_mode_resolve_para_bucket(self):
        cfg = Config({"lake": {"mode": "gcs"},
                      "gcp": {"buckets": {"silver": "meu-bucket-silver"}}})
        self.assertEqual(cfg.lake_uri("silver"), "gs://meu-bucket-silver")

    def test_uri_explicita_anexa_camada(self):
        cfg = _cfg()
        self.assertEqual(cfg.lake_uri("bronze"), "memory://lake/bronze")

    def test_modo_invalido_levanta_erro(self):
        cfg = Config({"lake": {"mode": "ftp"}})
        with self.assertRaises(ValueError):
            cfg.lake_uri("gold")


class TestLakeIoCloud(unittest.TestCase):
    def setUp(self):
        self.cfg = _cfg()
        self.fs = fsspec.filesystem("memory")
        if self.fs.exists("/lake"):
            self.fs.rm("/lake", recursive=True)

    def test_roundtrip_arquivo_unico(self):
        df = pl.DataFrame({"id": [1, 2, 3], "taxa": [51.2, 60.0, 47.9]})
        self.assertFalse(table_exists("gold", "indicador", cfg=self.cfg))
        write_table(df, "gold", "indicador", cfg=self.cfg)
        self.assertTrue(table_exists("gold", "indicador", cfg=self.cfg))
        out = read_table("gold", "indicador", cfg=self.cfg)
        self.assertTrue(out.equals(df))

    def test_roundtrip_particionado_preserva_tipos(self):
        df = pl.DataFrame({
            "ano": [2023, 2023, 2024],
            "id_municipio": ["3550308", "3304557", "3550308"],
            "taxa": [50.0, 55.0, 60.0],
        })
        write_table(df, "silver", "fato", partition_by=["ano"], cfg=self.cfg)

        # layout Hive no destino (permite partition pruning)
        paths = self.fs.glob("/lake/silver/fato/**/*.parquet")
        self.assertTrue(any("ano=2023" in p for p in paths))
        self.assertTrue(any("ano=2024" in p for p in paths))

        out = read_table("silver", "fato", cfg=self.cfg).sort("taxa")
        self.assertEqual(out.height, 3)
        # coluna de partição mantida DENTRO do arquivo: dtype exato na releitura
        self.assertEqual(out["ano"].dtype, pl.Int64)
        self.assertEqual(sorted(out["ano"].to_list()), [2023, 2023, 2024])

    def test_reescrita_particionada_e_idempotente(self):
        df1 = pl.DataFrame({"ano": [2023, 2024], "v": [1, 2]})
        df2 = pl.DataFrame({"ano": [2024], "v": [9]})
        write_table(df1, "silver", "fato", partition_by=["ano"], cfg=self.cfg)
        write_table(df2, "silver", "fato", partition_by=["ano"], cfg=self.cfg)
        out = read_table("silver", "fato", cfg=self.cfg)
        # a partição antiga (2023) não pode sobrar da primeira escrita
        self.assertEqual(out.height, 1)
        self.assertEqual(out["ano"].to_list(), [2024])

    def test_tabela_inexistente_levanta_filenotfound(self):
        with self.assertRaises(FileNotFoundError):
            read_table("gold", "nao_existe", cfg=self.cfg)


if __name__ == "__main__":
    unittest.main()
