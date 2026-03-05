"""
Externalized configuration loader.
Loads config.yaml and .env. No hardcoded paths in jobs.
"""

import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from dotenv import load_dotenv

# Load .env from project root. override=True ensures values from .env
# overwrite empty env vars (e.g. OPENAI_API_KEY="" from docker-compose).
load_dotenv(override=True)

_CONFIG: Optional[Dict[str, Any]] = None


def _get_project_root() -> Path:
    return Path(__file__).resolve().parent.parent.parent


def _load_config() -> Dict[str, Any]:
    global _CONFIG
    if _CONFIG is not None:
        return _CONFIG
    root = _get_project_root()
    config_path = root / "config" / "config.yaml"
    if not config_path.exists():
        raise FileNotFoundError(f"Config not found: {config_path}")
    with open(config_path) as f:
        _CONFIG = yaml.safe_load(f)
    return _CONFIG


def get_paths() -> Dict[str, str]:
    cfg = _load_config()
    paths = cfg.get("paths", {})
    data_root = os.getenv("DATA_ROOT")
    if data_root:
        base = Path(data_root)
        return {k: str(base / Path(v).name) for k, v in paths.items()}
    return paths


def get_spark_config() -> Dict[str, Any]:
    cfg = _load_config()
    return cfg.get("spark", {})


def get_binance_config() -> Dict[str, str]:
    cfg = _load_config()
    return cfg.get("binance", {})


def get_gx_config() -> Dict[str, str]:
    cfg = _load_config()
    return cfg.get("gx", {})


def get_gold_window_seconds() -> int:
    """Fallback window in seconds when Silver has no intervals to infer from."""
    cfg = _load_config()
    gold = cfg.get("gold", {})
    return int(gold.get("window_seconds", 300))
