"""Regras de qualidade de dados: duplicidade, nulos, chaves e consistência."""

from .checks import (
    CheckResult,
    QualityReport,
    check_foreign_key,
    check_no_duplicates,
    check_not_null,
    check_range,
    check_value_set,
)

__all__ = [
    "CheckResult",
    "QualityReport",
    "check_foreign_key",
    "check_no_duplicates",
    "check_not_null",
    "check_range",
    "check_value_set",
]
