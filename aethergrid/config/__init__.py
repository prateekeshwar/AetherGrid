"""File-based config and secrets."""
from .loader import ConfigLoader, load_config, get_secret, config
__all__ = ["ConfigLoader", "load_config", "get_secret", "config"]
