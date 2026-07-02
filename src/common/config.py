"""Carregamento da configuração central (config/settings.yaml).

Permite sobrescrever valores sensíveis via variáveis de ambiente
(ex.: GCP_PROJECT_ID), mantendo o YAML livre de segredos.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

# Raiz do repositório = dois níveis acima deste arquivo (src/common/config.py)
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = REPO_ROOT / "config" / "settings.yaml"


class Config:
    """Wrapper de acesso por caminho pontilhado sobre o dicionário de configuração."""

    def __init__(self, data: dict[str, Any]) -> None:
        self._data = data

    def get(self, dotted_key: str, default: Any = None) -> Any:
        node: Any = self._data
        for part in dotted_key.split("."):
            if not isinstance(node, dict) or part not in node:
                return default
            node = node[part]
        return node

    def __getitem__(self, dotted_key: str) -> Any:
        value = self.get(dotted_key, _MISSING)
        if value is _MISSING:
            raise KeyError(dotted_key)
        return value

    @property
    def data(self) -> dict[str, Any]:
        return self._data

    # ---- Caminhos absolutos do data lake (resolvidos a partir da raiz do repo) ----
    def lake_path(self, layer: str) -> Path:
        rel = self.get(f"lake.{layer}", f"data/{layer}")
        return REPO_ROOT / rel

    @property
    def gcp_project_id(self) -> str:
        return os.getenv("GCP_PROJECT_ID") or self.get("gcp.project_id", "")


_MISSING = object()


@lru_cache(maxsize=1)
def load_config(path: str | os.PathLike[str] | None = None) -> Config:
    cfg_path = Path(path) if path else DEFAULT_CONFIG_PATH
    with open(cfg_path, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    return Config(data)
