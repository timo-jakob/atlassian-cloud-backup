"""
Configuration management for thinning module.
"""

import os
import json
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict

from .utils import human_readable_to_bytes, validate_thinning_config


@dataclass
class ThinningSettings:
    """Thinning configuration settings."""
    max_size_bytes: int
    backup_directory: str
    deletion_strategy: str = "oldest_first"
    warning_threshold: float = 0.8
    keep_count: int = 5  # For keep_newest strategy
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ThinningSettings":
        """Create ThinningSettings from dictionary."""
        return cls(
            max_size_bytes=data["max_size_bytes"],
            backup_directory=data["backup_directory"],
            deletion_strategy=data.get("deletion_strategy", "oldest_first"),
            warning_threshold=data.get("warning_threshold", 0.8),
            keep_count=data.get("keep_count", 5)
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert ThinningSettings to dictionary."""
        return {
            "max_size_bytes": self.max_size_bytes,
            "backup_directory": self.backup_directory,
            "deletion_strategy": self.deletion_strategy,
            "warning_threshold": self.warning_threshold
        }
    
    def validate(self) -> None:
        """Validate the thinning settings."""
        if self.max_size_bytes <= 0:
            raise ValueError("Max size must be positive")
        
        if not 0 <= self.warning_threshold <= 1:
            raise ValueError("Warning threshold must be between 0 and 1")
        
        valid_strategies = ["oldest_first", "keep_newest", "retention_ladder"]
        if self.deletion_strategy not in valid_strategies:
            raise ValueError(f"deletion_strategy must be one of: {valid_strategies}")
        
        if self.keep_count < 1:
            raise ValueError("keep_count must be at least 1")
        
        backup_path = Path(self.backup_directory)
        if not backup_path.is_absolute():
            raise ValueError("backup_directory must be an absolute path")


class ThinningConfig:
    """Manages thinning configuration loading and saving."""
    
    DEFAULT_CONFIG_NAME = "thinning_config.json"
    
    def __init__(self, config_file: Optional[Path] = None):
        """Initialize with optional config file path."""
        if config_file is None:
            self.config_file = Path.cwd() / self.DEFAULT_CONFIG_NAME
        elif config_file.is_dir():
            # If directory provided, use default filename in that directory
            self.config_file = config_file / self.DEFAULT_CONFIG_NAME
            self.config_directory = config_file  # Add this for backward compatibility
        else:
            # File path provided
            self.config_file = config_file
            self.config_directory = config_file.parent  # Add this for backward compatibility
        self._settings: Optional[ThinningSettings] = None
    
    def load(self) -> ThinningSettings:
        """Load thinning settings from configuration file."""
        if not self.config_file.exists():
            # Create default settings when file doesn't exist
            default_settings = self.create_default("/default/backups", "1TB")
            self._settings = default_settings
            return default_settings
        
        try:
            with open(self.config_file, 'r') as f:
                data = json.load(f)
            
            self._settings = ThinningSettings.from_dict(data)
            self._settings.validate()
            return self._settings
            
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in configuration file: {e}")
        except KeyError as e:
            raise ValueError(f"Missing required configuration key: {e}")
    
    def save(self, settings: ThinningSettings) -> None:
        """Save thinning settings to configuration file."""
        settings.validate()
        
        # Ensure directory exists
        self.config_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(self.config_file, 'w') as f:
            json.dump(settings.to_dict(), f, indent=2)
        
        self._settings = settings
    
    def create_default(self, backup_directory: str, max_size: str = "1TB") -> ThinningSettings:
        """Create default thinning configuration."""
        max_size_bytes, _ = validate_thinning_config(max_size, 0.8)
        
        settings = ThinningSettings(
            max_size_bytes=max_size_bytes,
            backup_directory=backup_directory,
            deletion_strategy="retention_ladder",
            warning_threshold=0.8,
            keep_count=5
        )
        
        return settings
    
    def get_settings(self) -> Optional[ThinningSettings]:
        """Get currently loaded settings."""
        return self._settings
    
    def exists(self) -> bool:
        """Check if configuration file exists."""
        return self.config_file.exists()
    
    @classmethod
    def from_environment(cls) -> "ThinningConfig":
        """Create ThinningConfig using environment variables."""
        config_file = os.getenv("THINNING_CONFIG_FILE")
        if config_file:
            return cls(Path(config_file))
        return cls()
    
    def update_setting(self, key: str, value: Any) -> None:
        """Update a specific setting."""
        if not self._settings:
            raise ValueError("No settings loaded. Call load() first.")
        
        if not hasattr(self._settings, key):
            raise ValueError(f"Unknown setting: {key}")
        
        # Create a new settings object with the updated value
        settings_dict = self._settings.to_dict()
        settings_dict[key] = value
        
        new_settings = ThinningSettings.from_dict(settings_dict)
        self.save(new_settings)


def create_sample_config(output_path: Path, backup_directory: str = "/var/backups/atlassian", max_size: str = "1TB") -> Path:
    """Create a sample thinning configuration file."""
    config = ThinningConfig(output_path)
    settings = config.create_default(backup_directory, max_size)
    config.save(settings)
    
    print(f"Sample thinning configuration created at: {output_path}")
    print("Configuration:")
    print(f"  Max Size: {max_size}")
    print(f"  Backup Directory: {backup_directory}")
    print(f"  Deletion Strategy: {settings.deletion_strategy}")
    print(f"  Warning Threshold: {settings.warning_threshold * 100}%")
    
    return config.config_file
