"""AetherGrid v2.0 - File-based Config & Secrets Loader.

Supports YAML/JSON config files, env var overrides, secrets from files.
Graceful handling of missing files, validation.
"""

import os
import json
try:
    import yaml
except ImportError:
    yaml = None
from pathlib import Path
from typing import Any, Dict, Optional


class ConfigLoader:
    """Loads config from files + env, with secrets support."""

    def __init__(self, config_dir: str = "config", secrets_dir: str = "secrets"):
        self.config_dir = Path(config_dir)
        self.secrets_dir = Path(secrets_dir)
        self._cache: Dict[str, Any] = {}
    
    def load(self, name: str = "default") -> Dict[str, Any]:
        """Load config file (yaml or json)."""
        for ext in (".yaml", ".yml", ".json"):
            path = self.config_dir / f"{name}{ext}"
            if path.exists():
                with open(path) as f:
                    if ext == ".json":
                        data = json.load(f)
                    else:
                        data = yaml.safe_load(f) or {} if yaml else {}
                # Env overrides
                for k, v in os.environ.items():
                    if k.startswith("AETHER_"):
                        data[k[7:].lower()] = v
                self._cache[name] = data
                return data
        return {}
    
    def get_secret(self, name: str) -> Optional[str]:
        """Load secret from secrets/ file (no logging)."""
        path = self.secrets_dir / name
        if path.exists():
            return path.read_text().strip()
        # Fallback env
        return os.environ.get(f"AETHER_SECRET_{name.upper()}")
    
    def get(self, key: str, default: Any = None, config_name: str = "default") -> Any:
        """Get value from loaded config."""
        cfg = self._cache.get(config_name, self.load(config_name))
        return cfg.get(key, default)


# Global instance
config = ConfigLoader()

def load_config(name: str = "default") -> Dict[str, Any]:
    return config.load(name)

def get_secret(name: str) -> Optional[str]:
    return config.get_secret(name)
