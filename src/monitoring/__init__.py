"""Observabilidade do pipeline: métricas, latência, volume e alertas."""

from .metrics import MetricsCollector, StageMetric

__all__ = ["MetricsCollector", "StageMetric"]
