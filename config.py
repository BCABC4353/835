"""
Configuration Management for 835 EDI Parser
============================================

This module provides centralized configuration management with support for:
- Environment variables
- Configuration files (JSON, YAML, or .env)
- Programmatic defaults
- Runtime overrides

Configuration Priority (highest to lowest):
1. Environment variables
2. Config file values
3. Programmatic defaults
"""

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class Config:
    """Central configuration manager for 835 parser"""

    # Default configuration values
    DEFAULTS = {
        # Input file paths (None = not configured, user must set via Settings dialog)
        "trips_csv_path": None,
        "rates_xlsx_path": None,
        # Output file names (relative to processing folder)
        "output_csv_name": "835_consolidated_output.csv",
        "output_csv_compact_name": "835_consolidated_output_compact.csv",
        "validation_report_txt_name": "835_validation_report.txt",
        "validation_report_html_name": "835_validation_report.html",
        # Processing options
        "enable_fair_health_rates": True,
        "enable_trips_lookup": True,
        "enable_compact_csv": True,
        # Note: Validation is always run automatically (mandatory)
        # Logging configuration
        "log_level": "INFO",
        "log_file": None,  # None = console only
        "simple_log_format": False,
        # Memory optimization
        "chunk_size": 10000,  # Progress feedback interval
        # Validation options
        "validation_verbose": True,
    }

    def __init__(self, config_file: Optional[str] = None):
        """
        Initialize configuration.

        Args:
            config_file: Path to configuration file (JSON or .env format).
                        If None, looks for '835_config.json' in current directory,
                        then user's home directory.
        """
        self._config: Dict[str, Any] = self.DEFAULTS.copy()
        self._config_file_path: Optional[Path] = None

        # Try to load config file
        if config_file:
            self.load_config_file(config_file)
        else:
            # Auto-discover config file
            self._auto_discover_config()

        # Override with environment variables
        self._load_from_environment()

    def _auto_discover_config(self):
        """Auto-discover config file in standard locations"""
        search_paths = [
            self._get_default_config_path(),  # Check user AppData first
            Path.cwd() / "835_config.json",
            Path.home() / "835_config.json",
            Path.cwd() / ".835config",
            Path.home() / ".835config",
        ]

        for path in search_paths:
            if path.exists():
                logger.info("Found config file: %s", path)
                self.load_config_file(str(path))
                break

    def load_config_file(self, config_file: str):
        """
        Load configuration from JSON file.

        Args:
            config_file: Path to JSON config file
        """
        config_path = Path(config_file)

        if not config_path.exists():
            logger.warning("Config file not found: %s", config_file)
            return

        try:
            with open(config_path) as f:
                file_config = json.load(f)

            # Update config with file values
            self._config.update(file_config)
            self._config_file_path = config_path
            logger.info("Loaded configuration from: %s", config_path)

        except json.JSONDecodeError as e:
            logger.error("Invalid JSON in config file %s: %s", config_file, e)
        except Exception as e:
            logger.error("Error loading config file %s: %s", config_file, e)

    def _load_from_environment(self):
        """Load configuration from environment variables"""
        env_mapping = {
            "EDI_TRIPS_CSV_PATH": "trips_csv_path",
            "EDI_RATES_XLSX_PATH": "rates_xlsx_path",
            "EDI_OUTPUT_CSV_NAME": "output_csv_name",
            "EDI_OUTPUT_CSV_COMPACT_NAME": "output_csv_compact_name",
            "EDI_VALIDATION_REPORT_TXT": "validation_report_txt_name",
            "EDI_VALIDATION_REPORT_HTML": "validation_report_html_name",
            "EDI_ENABLE_FAIR_HEALTH": "enable_fair_health_rates",
            "EDI_ENABLE_TRIPS_LOOKUP": "enable_trips_lookup",
            "EDI_ENABLE_COMPACT_CSV": "enable_compact_csv",
            "EDI_LOG_LEVEL": "log_level",
            "EDI_LOG_FILE": "log_file",
            "EDI_CHUNK_SIZE": "chunk_size",
        }

        for env_var, config_key in env_mapping.items():
            value = os.getenv(env_var)
            if value is not None:
                # Type conversion for boolean and integer values
                if config_key in [
                    "enable_fair_health_rates",
                    "enable_trips_lookup",
                    "enable_compact_csv",
                    "validation_verbose",
                    "simple_log_format",
                ]:
                    value = value.lower() in ("true", "1", "yes", "on")
                elif config_key in ["chunk_size"]:
                    value = int(value)

                self._config[config_key] = value
                logger.debug("Config from env %s: %s = %s", env_var, config_key, value)

    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value"""
        return self._config.get(key, default)

    def set(self, key: str, value: Any):
        """Set configuration value at runtime"""
        self._config[key] = value

    def __getitem__(self, key: str) -> Any:
        """Dictionary-style access"""
        return self._config[key]

    def __setitem__(self, key: str, value: Any):
        """Dictionary-style setting"""
        self._config[key] = value

    @property
    def trips_csv_path(self) -> Optional[str]:
        """Path to Trips.csv file (None if not configured)"""
        return self._config["trips_csv_path"]

    @property
    def rates_xlsx_path(self) -> Optional[str]:
        """Path to RATES.xlsx file (None if not configured)"""
        return self._config["rates_xlsx_path"]

    @property
    def output_csv_name(self) -> str:
        """Output CSV file name"""
        return self._config["output_csv_name"]

    @property
    def output_csv_compact_name(self) -> str:
        """Compact output CSV file name"""
        return self._config["output_csv_compact_name"]

    @property
    def validation_report_txt_name(self) -> str:
        """Validation report text file name"""
        return self._config["validation_report_txt_name"]

    @property
    def validation_report_html_name(self) -> str:
        """Validation report HTML file name"""
        return self._config["validation_report_html_name"]

    def to_dict(self) -> Dict[str, Any]:
        """Export current configuration as dictionary"""
        return self._config.copy()

    def save(self, file_path: Optional[str] = None):
        """
        Save current configuration to JSON file.

        Args:
            file_path: Path to save config. If None, uses loaded config file path
                      or defaults to user-writable config location.
        """
        if file_path is None:
            file_path = self._config_file_path or self._get_default_config_path()
        else:
            file_path = Path(file_path)

        # Ensure parent directory exists
        file_path = Path(file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            with open(file_path, "w") as f:
                json.dump(self._config, f, indent=2)
            logger.info("Saved configuration to: %s", file_path)
        except Exception as e:
            logger.error("Error saving config to %s: %s", file_path, e)

    def _get_default_config_path(self) -> Path:
        """
        Get default config file path in user-writable location.

        Returns:
            Path to config file in AppData (Windows) or home directory (Unix)
        """
        # Try Windows AppData first
        appdata = os.getenv("APPDATA")
        if appdata:
            config_dir = Path(appdata) / "835-EDI-Parser"
            config_dir.mkdir(parents=True, exist_ok=True)
            return config_dir / "835_config.json"

        # Fallback to home directory
        config_dir = Path.home() / ".835-parser"
        config_dir.mkdir(parents=True, exist_ok=True)
        return config_dir / "835_config.json"

    def __repr__(self) -> str:
        return f"Config({len(self._config)} settings loaded)"


# Global configuration instance
_config: Optional[Config] = None


def get_config(config_file: Optional[str] = None, reload: bool = False) -> Config:
    """
    Get or create the global configuration instance.

    Args:
        config_file: Path to config file (only used on first call or if reload=True)
        reload: If True, reload configuration from file

    Returns:
        Config instance
    """
    global _config

    if _config is None or reload:
        _config = Config(config_file)

    return _config


def reset_config():
    """Reset global configuration to None (useful for testing)"""
    global _config
    _config = None
