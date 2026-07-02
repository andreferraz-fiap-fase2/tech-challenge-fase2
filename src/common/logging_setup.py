"""Logging estruturado para o pipeline.

Emite logs legíveis no console. Em produção (GCP) o mesmo formato é
capturado pelo Cloud Logging, permitindo filtros por `stage` e `layer`.
"""

from __future__ import annotations

import logging
import sys

_CONFIGURED = False


def configure_utf8_stdio() -> None:
    """Garante saída UTF-8 no console (Windows usa cp1252 por padrão)."""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
        except (AttributeError, ValueError):
            pass


def get_logger(name: str) -> logging.Logger:
    global _CONFIGURED
    if not _CONFIGURED:
        configure_utf8_stdio()
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s | %(levelname)-7s | %(name)-28s | %(message)s",
                datefmt="%H:%M:%S",
            )
        )
        root = logging.getLogger("pipeline")
        root.setLevel(logging.INFO)
        root.addHandler(handler)
        root.propagate = False
        _CONFIGURED = True
    return logging.getLogger(f"pipeline.{name}")
