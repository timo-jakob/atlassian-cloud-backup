"""
Backup thinning manager for Atlassian Cloud Backup.

This module provides simple backup file thinning by deleting exactly one file
when triggered by external systems. No size calculations or monitoring.
"""

from pathlib import Path
from typing import List, Optional
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime


@dataclass
class BackupInfo:
    """Information about a backup file."""
    path: Path
    created_at: datetime
    task_id: str  # Backup task ID (for Jira) or "n/a" (for Confluence)
    
@dataclass
class DeletionConfig:
    """Simple configuration for deletion strategy."""
    deletion_strategy: str = "retention_ladder"
    
    
class DeletionStrategy(ABC):
    """Abstract base class for backup deletion strategies."""
    
    @abstractmethod
    def select_file_for_deletion(self, backups: List[BackupInfo]) -> Optional[BackupInfo]:
        """Select exactly one backup file for deletion from the given list."""
        pass


class OldestFirstStrategy(DeletionStrategy):
    """Delete the oldest backup file."""
    
    def select_file_for_deletion(self, backups: List[BackupInfo]) -> Optional[BackupInfo]:
        """Select the oldest backup file for deletion."""
        if not backups:
            return None
        
        # Return the backup with the earliest creation date
        return min(backups, key=lambda b: b.created_at)


class BackupRetentionLadder(DeletionStrategy):
    """
    Sophisticated retention strategy with time-based ladder:
    - Within 1 month: Keep latest backup of each week
    - Within 1 year: Keep latest backup of each month  
    - Beyond 1 year: Keep latest backup of each year
    
    This ensures we have fine-grained recent backups and coarse-grained old backups.
    """
    
    def select_file_for_deletion(self, backups: List[BackupInfo]) -> Optional[BackupInfo]:
        """Select a backup file for deletion based on retention ladder rules."""
        if not backups:
            return None
        
        from datetime import datetime, timedelta
        import calendar
        
        now = datetime.now()
        one_month_ago = now - timedelta(days=30)
        one_year_ago = now - timedelta(days=365)
        
        # Group backups by time periods
        recent_backups = []      # Within 1 month
        monthly_backups = []     # 1 month to 1 year ago
        yearly_backups = []      # Over 1 year ago
        
        for backup in backups:
            if backup.created_at >= one_month_ago:
                recent_backups.append(backup)
            elif backup.created_at >= one_year_ago:
                monthly_backups.append(backup)
            else:
                yearly_backups.append(backup)
        
        # Find candidates for deletion in each category
        candidates = []
        
        # Recent backups: keep latest per week
        candidates.extend(self._find_weekly_candidates(recent_backups))
        
        # Monthly backups: keep latest per month
        candidates.extend(self._find_monthly_candidates(monthly_backups))
        
        # Yearly backups: keep latest per year
        candidates.extend(self._find_yearly_candidates(yearly_backups))
        
        # Return the oldest candidate for deletion
        if candidates:
            return min(candidates, key=lambda b: b.created_at)
        
        return None
    
    def _find_weekly_candidates(self, backups: List[BackupInfo]) -> List[BackupInfo]:
        """Find backups that can be deleted from weekly retention (keep latest per week)."""
        if not backups:
            return []
        
        # Group by week (year, week_number)
        weekly_groups = {}
        for backup in backups:
            week_key = (backup.created_at.year, backup.created_at.isocalendar()[1])
            if week_key not in weekly_groups:
                weekly_groups[week_key] = []
            weekly_groups[week_key].append(backup)
        
        # Keep only the latest backup per week, mark others for deletion
        candidates = []
        for week_backups in weekly_groups.values():
            if len(week_backups) > 1:
                # Keep the latest, add others as candidates
                week_backups.sort(key=lambda b: b.created_at, reverse=True)
                candidates.extend(week_backups[1:])  # All except the latest
        
        return candidates
    
    def _find_monthly_candidates(self, backups: List[BackupInfo]) -> List[BackupInfo]:
        """Find backups that can be deleted from monthly retention (keep latest per month)."""
        if not backups:
            return []
        
        # Group by month (year, month)
        monthly_groups = {}
        for backup in backups:
            month_key = (backup.created_at.year, backup.created_at.month)
            if month_key not in monthly_groups:
                monthly_groups[month_key] = []
            monthly_groups[month_key].append(backup)
        
        # Keep only the latest backup per month
        candidates = []
        for month_backups in monthly_groups.values():
            if len(month_backups) > 1:
                # Keep the latest, add others as candidates
                month_backups.sort(key=lambda b: b.created_at, reverse=True)
                candidates.extend(month_backups[1:])  # All except the latest
        
        return candidates
    
    def _find_yearly_candidates(self, backups: List[BackupInfo]) -> List[BackupInfo]:
        """Find backups that can be deleted from yearly retention (keep latest per year)."""
        if not backups:
            return []
        
        # Group by year
        yearly_groups = {}
        for backup in backups:
            year_key = backup.created_at.year
            if year_key not in yearly_groups:
                yearly_groups[year_key] = []
            yearly_groups[year_key].append(backup)
        
        # Keep only the latest backup per year
        candidates = []
        for year_backups in yearly_groups.values():
            if len(year_backups) > 1:
                # Keep the latest, add others as candidates
                year_backups.sort(key=lambda b: b.created_at, reverse=True)
                candidates.extend(year_backups[1:])  # All except the latest
        
        return candidates


class BackupDeleter:
    """Simple backup deletion trigger."""
    
    STRATEGIES = {
        "oldest_first": OldestFirstStrategy,
        "retention_ladder": BackupRetentionLadder,
    }
    
    def __init__(self, config: DeletionConfig):
        self.config = config
        self.strategy = self._create_strategy(config.deletion_strategy)
    
    def _create_strategy(self, strategy_name: str) -> DeletionStrategy:
        """Create a deletion strategy instance."""
        if strategy_name not in self.STRATEGIES:
            raise ValueError(f"Unknown deletion strategy: {strategy_name}")
        
        strategy_class = self.STRATEGIES[strategy_name]
        return strategy_class()
    
    def scan_backups_in_directory(self, instance_directory: Path, backup_type: str) -> List[BackupInfo]:
        """Scan a specific instance directory for backups of a specific type."""
        backups = []
        
        if not instance_directory.exists() or not instance_directory.is_dir():
            return backups
        
        # Pattern: <task_id>-<type>-backup-<year>-<month>-<day>
        # Example: 665805-jira-backup-2025-07-19
        
        for item in instance_directory.iterdir():
            if item.is_file() and self._is_backup_file(item.name, backup_type):
                try:
                    created_at = datetime.fromtimestamp(item.stat().st_mtime)
                    task_id = self._extract_task_id(item.name, backup_type)
                    
                    backups.append(BackupInfo(
                        path=item,
                        created_at=created_at,
                        task_id=task_id
                    ))
                except OSError:
                    # Skip files we can't access
                    continue
        
        return backups
    
    def _is_backup_file(self, filename: str, backup_type: str) -> bool:
        """Check if a filename matches the backup pattern for the given type."""
        filename_lower = filename.lower()
        backup_type_lower = backup_type.lower()
        
        if backup_type_lower == "jira":
            # Pattern: <task_id>-jira-backup-<year>-<month>-<day>
            return "-jira-backup-" in filename_lower
        elif backup_type_lower == "confluence":
            # Pattern: confluence-backup-<year>-<month>-<day>
            return filename_lower.startswith("confluence-backup-")
        
        return False
    
    def _extract_task_id(self, filename: str, backup_type: str) -> str:
        """Extract task ID from filename (only applicable for Jira backups)."""
        backup_type_lower = backup_type.lower()
        
        if backup_type_lower == "jira":
            # Pattern: <task_id>-jira-backup-<year>-<month>-<day>
            parts = filename.split('-')
            if len(parts) >= 3 and parts[1].lower() == "jira" and parts[2].lower() == "backup":
                return parts[0]
        elif backup_type_lower == "confluence":
            # Confluence backups don't have task IDs
            return "n/a"
        
        return "unknown"
    
    def delete_one_backup(self, instance_directory: Path, backup_type: str) -> Optional[Path]:
        """
        Delete exactly one backup file from the specified directory and type.
        
        This is the main trigger method. External systems call this to delete
        exactly one backup file based on the configured strategy.
        
        Args:
            instance_directory: Path to the instance directory (e.g., /backups/abc)
            backup_type: Type of backup ('jira' or 'confluence')
            
        Returns:
            Path of the deleted file, or None if no file was deleted
        """
        # Scan for backups of the specified type in the instance directory
        backups = self.scan_backups_in_directory(instance_directory, backup_type)
        
        if not backups:
            return None  # No backups found
        
        # Use strategy to select which file to delete
        backup_to_delete = self.strategy.select_file_for_deletion(backups)
        
        if backup_to_delete is None:
            return None  # Strategy decided not to delete anything
        
        # Delete the selected file
        try:
            backup_to_delete.path.unlink()
            return backup_to_delete.path
        except OSError as e:
            print(f"Failed to delete {backup_to_delete.path}: {e}")
            return None
