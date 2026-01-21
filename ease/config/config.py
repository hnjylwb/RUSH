"""
Configuration management
"""

from typing import Dict, Any
from pathlib import Path


class Config:
    """
    Unified configuration manager

    Loads configuration from YAML files (if available) and provides easy access
    """

    def __init__(self, config_dir: str = "config"):
        """
        Initialize configuration

        Args:
            config_dir: Directory containing config files
        """
        self.config_dir = Path(config_dir)
        self._config = {}
        self._load_configs()

    def _load_configs(self):
        """Load all configuration files"""
        try:
            import yaml
            config_files = ['services.yaml', 'parameters.yaml']

            for filename in config_files:
                filepath = self.config_dir / filename
                if filepath.exists():
                    with open(filepath, 'r') as f:
                        config_data = yaml.safe_load(f)
                        if config_data:
                            self._config.update(config_data)
                else:
                    print(f"Warning: Configuration file '{filename}' not found in {self.config_dir}")

            if not self._config:
                raise RuntimeError(
                    f"No configuration files found in {self.config_dir}. "
                    f"Please create configuration files (services.yaml, parameters.yaml)"
                )

        except ImportError:
            raise RuntimeError(
                "pyyaml is not installed. Please install it with: pip install pyyaml"
            )

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value

        Args:
            key: Configuration key (supports nested keys like 'router.cost_weight')
            default: Default value if key not found

        Returns:
            Configuration value
        """
        keys = key.split('.')
        value = self._config

        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default

        return value

    def get_all(self) -> Dict:
        """Get all configuration"""
        return self._config.copy()
