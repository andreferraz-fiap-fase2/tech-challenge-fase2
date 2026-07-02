"""Coleta de métricas operacionais do pipeline.

Registra, por etapa (stage): volume processado, latência, falhas e alertas.
Persiste um JSON por execução em `monitoring/metrics/`, simulando o envio de
métricas ao Cloud Monitoring. Em produção, cada `StageMetric` viraria um
custom metric / log estruturado consumido por dashboards e políticas de alerta.
"""

from __future__ import annotations

import json
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from ..common.config import REPO_ROOT, Config, load_config
from ..common.logging_setup import get_logger

log = get_logger("monitoring.metrics")


@dataclass
class StageMetric:
    stage: str
    layer: str
    rows_in: int = 0
    rows_out: int = 0
    duration_seconds: float = 0.0
    status: str = "success"          # success | failed
    error: str | None = None
    alerts: list[str] = field(default_factory=list)


class MetricsCollector:
    """Acumula métricas de todas as etapas de uma execução do pipeline."""

    def __init__(self, cfg: Config | None = None) -> None:
        self.cfg = cfg or load_config()
        self.run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.started_at = time.time()
        self.stages: list[StageMetric] = []
        self._latency_warn = float(self.cfg.get("monitoring.latency_warning_seconds", 30))

    @contextmanager
    def stage(self, stage: str, layer: str, rows_in: int = 0) -> Iterator[StageMetric]:
        """Cronometra uma etapa e captura falhas/latência automaticamente."""
        metric = StageMetric(stage=stage, layer=layer, rows_in=rows_in)
        start = time.time()
        log.info("→ iniciando etapa '%s' [%s]", stage, layer)
        try:
            yield metric
        except Exception as exc:  # falha de ingestão/transformação
            metric.status = "failed"
            metric.error = f"{type(exc).__name__}: {exc}"
            metric.alerts.append(f"FALHA na etapa '{stage}': {metric.error}")
            log.error("✗ etapa '%s' FALHOU: %s", stage, metric.error)
            self._finalize(metric, start)
            raise
        else:
            self._finalize(metric, start)

    def _finalize(self, metric: StageMetric, start: float) -> None:
        metric.duration_seconds = round(time.time() - start, 3)
        if metric.duration_seconds > self._latency_warn:
            alert = (
                f"LATÊNCIA alta na etapa '{metric.stage}': "
                f"{metric.duration_seconds}s (limite {self._latency_warn}s)"
            )
            metric.alerts.append(alert)
            log.warning(alert)
        if metric.status == "success":
            log.info(
                "✓ etapa '%s' ok | %d→%d linhas | %.3fs",
                metric.stage, metric.rows_in, metric.rows_out, metric.duration_seconds,
            )
        self.stages.append(metric)

    # ---- Agregações para dashboards ----
    @property
    def total_rows_processed(self) -> int:
        return sum(s.rows_out for s in self.stages)

    @property
    def total_duration(self) -> float:
        return round(time.time() - self.started_at, 3)

    @property
    def all_alerts(self) -> list[str]:
        return [a for s in self.stages for a in s.alerts]

    @property
    def failed(self) -> bool:
        return any(s.status == "failed" for s in self.stages)

    def flush(self) -> Path:
        metrics_dir = REPO_ROOT / self.cfg.get("monitoring.metrics_dir", "monitoring/metrics")
        metrics_dir.mkdir(parents=True, exist_ok=True)
        out = metrics_dir / f"run_{self.run_id}.json"
        payload = {
            "run_id": self.run_id,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_duration_seconds": self.total_duration,
            "total_rows_processed": self.total_rows_processed,
            "failed": self.failed,
            "alerts": self.all_alerts,
            "stages": [asdict(s) for s in self.stages],
        }
        out.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        log.info("métricas da execução salvas em %s", out)
        return out

    def summary(self) -> str:
        lines = [
            "",
            "=" * 66,
            f" RESUMO DA EXECUÇÃO {self.run_id}",
            "=" * 66,
            f" Etapas............: {len(self.stages)}",
            f" Linhas processadas: {self.total_rows_processed}",
            f" Duração total.....: {self.total_duration}s",
            f" Status............: {'FALHOU' if self.failed else 'SUCESSO'}",
            f" Alertas...........: {len(self.all_alerts)}",
        ]
        for a in self.all_alerts:
            lines.append(f"   ⚠  {a}")
        lines.append("=" * 66)
        return "\n".join(lines)
