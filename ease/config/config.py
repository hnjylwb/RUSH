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
        except ImportError:
            print("Warning: pyyaml not installed, using default configuration")
            # Use default config if yaml not available
            self._config = self._get_default_config()

    def _get_default_config(self) -> Dict:
        """Get default configuration"""
        return {
            'services': {
                'vm': [{
                    'name': 'vm-1',
                    'cpu_cores': 32,
                    'memory_gb': 128,
                    'io_bandwidth_mbps': 1250,
                    'network_bandwidth_mbps': 1250,
                    'cost_per_hour': 1.536
                }],
                'faas': [{
                    'name': 'faas-cluster-1',
                    'num_instances': 100,
                    'memory_per_instance_gb': 4,
                    'cpu_per_instance': 1.2,
                    'io_per_instance_mbps': 75,
                    'cost_per_gb_second': 0.0000002,
                    'total_cpu': 120,
                    'total_io': 7500
                }],
                'qaas': [{
                    'name': 'qaas-1',
                    'cost_per_tb': 5.0,
                    'max_concurrent_queries': 30,
                    'scan_speed_tb_per_sec': 0.5,
                    'base_latency': 5.0
                }]
            },
            'router': {
                'cost_weight': 1.0,
                'load_weights': {
                    'vm': 0.05,
                    'faas': 0.1,
                    'qaas': 0.15
                }
            },
            'inter_scheduler': {
                'wait_time_threshold': 1.0,
                'migration_cooldown': 5.0,
                'max_migrations': 3
            }
        }

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
