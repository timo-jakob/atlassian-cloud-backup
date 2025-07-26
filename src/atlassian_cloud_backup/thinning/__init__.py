"""
Thinning module for Atlassian Cloud Backup.

This module provides backup file thinning by deleting files based on strategy.
External systems trigger deletion of exactly one file based on:
- Strategy (retention_ladder or oldest_first) 
- Instance directory (where backups are stored)  
- Backup type (jira or confluence)

Supported filename patterns:
- JIRA:       <task_id>-jira-backup-<year>-<month>-<day>
- Confluence: confluence-backup-<year>-<month>-<day>

Default strategy: retention_ladder (intelligent time-based thinning)
- Recent (1 month): Keep latest per week
- Medium (1 year): Keep latest per month  
- Long term (1+ year): Keep latest per year

No size calculations or monitoring - just smart file thinning.
"""

from .manager import (
    BackupDeleter,
    DeletionConfig,
    BackupInfo,
    DeletionStrategy,
    OldestFirstStrategy,
    BackupRetentionLadder,
)
from .config import ThinningConfig, ThinningSettings, create_sample_config
from .utils import (
    bytes_to_human_readable,
    human_readable_to_bytes,
    calculate_percentage,
    format_usage_status,
    estimate_backup_size,
    validate_thinning_config,
)

__all__ = [
    # Main classes
    "BackupDeleter",
    "DeletionConfig", 
    "BackupInfo",
    
    # Deletion strategies
    "DeletionStrategy",
    "OldestFirstStrategy", 
    "BackupRetentionLadder",
    
    # Configuration
    "ThinningConfig",
    "ThinningSettings",
    "create_sample_config",
    
    # Utility functions
    "bytes_to_human_readable",
    "human_readable_to_bytes",
    "calculate_percentage",
    "format_usage_status",
    "estimate_backup_size",
    "validate_thinning_config",
]
