"""Testes das regras de qualidade de dados.

Rode com:  python -m pytest -q     (ou)     python -m unittest
"""

import unittest

import polars as pl

from src.quality import (
    check_foreign_key,
    check_no_duplicates,
    check_not_null,
    check_range,
    check_value_set,
)


class TestQualityChecks(unittest.TestCase):
    def test_no_duplicates_detects_repeated_keys(self):
        df = pl.DataFrame({"id": [1, 2, 2, 3]})
        self.assertFalse(check_no_duplicates(df, ["id"]).passed)
        self.assertEqual(check_no_duplicates(df, ["id"]).offending_rows, 1)

    def test_no_duplicates_passes_when_unique(self):
        df = pl.DataFrame({"id": [1, 2, 3]})
        self.assertTrue(check_no_duplicates(df, ["id"]).passed)

    def test_not_null_blocks_when_over_threshold(self):
        df = pl.DataFrame({"v": [1.0, None, 3.0, 4.0]})
        res = check_not_null(df, "v", max_null_ratio=0.0)
        self.assertFalse(res.passed)
        self.assertEqual(res.offending_rows, 1)

    def test_not_null_allows_within_threshold(self):
        df = pl.DataFrame({"v": [1.0, None, 3.0, 4.0]})
        self.assertTrue(check_not_null(df, "v", max_null_ratio=0.5).passed)

    def test_foreign_key_detects_orphans(self):
        child = pl.DataFrame({"uf": ["SP", "RJ", "ZZ"]})
        parent = pl.DataFrame({"sigla_uf": ["SP", "RJ", "MG"]})
        res = check_foreign_key(child, "uf", parent, "sigla_uf")
        self.assertFalse(res.passed)
        self.assertEqual(res.offending_rows, 1)

    def test_range_flags_out_of_bounds(self):
        df = pl.DataFrame({"p": [500.0, 743.0, 1200.0]})
        res = check_range(df, "p", 0.0, 1000.0)
        self.assertFalse(res.passed)
        self.assertEqual(res.offending_rows, 1)

    def test_value_set_flags_invalid_category(self):
        df = pl.DataFrame({"regiao": ["Sul", "Norte", "Lua"]})
        res = check_value_set(df, "regiao", {"Sul", "Norte", "Nordeste", "Sudeste", "Centro-Oeste"})
        self.assertFalse(res.passed)


if __name__ == "__main__":
    unittest.main()
