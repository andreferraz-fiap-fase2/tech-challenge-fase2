"""Verificações de qualidade de dados reutilizáveis (baseadas em Polars).

Cobre as regras exigidas pelo desafio:
  - Verificação de duplicidade (check_no_duplicates)
  - Detecção de valores ausentes (check_not_null)
  - Validação de chaves de relacionamento (check_foreign_key)
  - Consistência entre tabelas / domínios de valores (check_value_set, check_range)

Cada verificação retorna um `CheckResult`; um `QualityReport` agrega os
resultados de uma tabela e decide se o pipeline deve prosseguir ou falhar.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import polars as pl

from ..common.logging_setup import get_logger

log = get_logger("quality.checks")


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str
    severity: str = "error"          # error | warning
    offending_rows: int = 0


@dataclass
class QualityReport:
    table: str
    results: list[CheckResult] = field(default_factory=list)

    def add(self, result: CheckResult) -> None:
        self.results.append(result)
        icon = "✓" if result.passed else ("⚠" if result.severity == "warning" else "✗")
        log.info("[%s] %s %s — %s", self.table, icon, result.name, result.detail)

    @property
    def has_blocking_failures(self) -> bool:
        return any(not r.passed and r.severity == "error" for r in self.results)

    @property
    def warnings(self) -> list[CheckResult]:
        return [r for r in self.results if not r.passed and r.severity == "warning"]

    def as_dict(self) -> dict:
        return {
            "table": self.table,
            "passed": not self.has_blocking_failures,
            "checks": [r.__dict__ for r in self.results],
        }


# --------------------------------------------------------------------------- #
# Verificações individuais
# --------------------------------------------------------------------------- #
def check_no_duplicates(df: pl.DataFrame, keys: list[str]) -> CheckResult:
    """Nenhuma combinação de chave primária pode se repetir."""
    dup = df.height - df.select(keys).n_unique()
    return CheckResult(
        name=f"sem_duplicatas({','.join(keys)})",
        passed=dup == 0,
        detail=f"{dup} linhas duplicadas nas chaves {keys}",
        offending_rows=dup,
    )


def check_not_null(df: pl.DataFrame, column: str, max_null_ratio: float = 0.0) -> CheckResult:
    """Coluna crítica não pode ultrapassar o ratio de nulos permitido."""
    n_null = df[column].null_count()
    ratio = n_null / df.height if df.height else 0.0
    passed = ratio <= max_null_ratio
    return CheckResult(
        name=f"nao_nulo({column})",
        passed=passed,
        detail=f"{n_null} nulos ({ratio:.2%}); limite {max_null_ratio:.2%}",
        severity="error" if max_null_ratio == 0.0 else "warning",
        offending_rows=n_null,
    )


def check_foreign_key(
    df: pl.DataFrame, fk_column: str, parent: pl.DataFrame, parent_key: str
) -> CheckResult:
    """Todo valor de FK deve existir na tabela pai (integridade referencial)."""
    valid = parent.select(pl.col(parent_key).unique())
    orphans = (
        df.filter(pl.col(fk_column).is_not_null())
        .join(valid, left_on=fk_column, right_on=parent_key, how="anti")
    )
    n = orphans.height
    return CheckResult(
        name=f"fk({fk_column}->{parent_key})",
        passed=n == 0,
        detail=f"{n} valores de '{fk_column}' sem correspondência na tabela pai",
        offending_rows=n,
    )


def check_range(
    df: pl.DataFrame, column: str, min_value: float, max_value: float
) -> CheckResult:
    """Valores numéricos devem estar dentro do domínio esperado."""
    out = df.filter(
        (pl.col(column) < min_value) | (pl.col(column) > max_value)
    )
    n = out.height
    return CheckResult(
        name=f"faixa({column}∈[{min_value},{max_value}])",
        passed=n == 0,
        detail=f"{n} valores fora da faixa em '{column}'",
        offending_rows=n,
    )


def check_value_set(df: pl.DataFrame, column: str, allowed: set) -> CheckResult:
    """Coluna categórica só pode conter valores de um conjunto permitido."""
    present = set(df[column].drop_nulls().unique().to_list())
    invalid = present - allowed
    return CheckResult(
        name=f"dominio({column})",
        passed=not invalid,
        detail=f"valores inválidos: {sorted(invalid)[:10]}" if invalid else "domínio ok",
        offending_rows=len(invalid),
    )
