"""
Test cases for the thinning module manager.

This module tests all core functionality of the backup thinning system including:
- Filename pattern recognition for JIRA and Confluence
- Deletion strategies (oldest_first and retention_ladder)
- BackupDeleter main functionality
- BackupInfo and DeletionConfig classes
"""

import pytest
import tempfile
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

# Import the modules under test
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from atlassian_cloud_backup.thinning.manager import (
    BackupInfo,
    DeletionConfig,
    OldestFirstStrategy,
    BackupRetentionLadder,
    BackupDeleter
)


class TestBackupInfo:
    """Test the BackupInfo dataclass."""
    
    def test_backup_info_creation(self):
        """Test creating a BackupInfo instance."""
        path = Path("/test/backup.jira")
        created_at = datetime.now()
        task_id = "123456"
        
        backup_info = BackupInfo(path=path, created_at=created_at, task_id=task_id)
        
        assert backup_info.path == path
        assert backup_info.created_at == created_at
        assert backup_info.task_id == task_id
    
    def test_backup_info_jira_task_id(self):
        """Test BackupInfo with JIRA task ID."""
        backup_info = BackupInfo(
            path=Path("665805-jira-backup-2025-07-19"),
            created_at=datetime.now(),
            task_id="665805"
        )
        assert backup_info.task_id == "665805"
    
    def test_backup_info_confluence_no_task_id(self):
        """Test BackupInfo with Confluence (no task ID)."""
        backup_info = BackupInfo(
            path=Path("confluence-backup-2025-07-19"),
            created_at=datetime.now(),
            task_id="n/a"
        )
        assert backup_info.task_id == "n/a"


class TestDeletionConfig:
    """Test the DeletionConfig dataclass."""
    
    def test_deletion_config_default(self):
        """Test default deletion configuration."""
        config = DeletionConfig()
        assert config.deletion_strategy == "retention_ladder"
    
    def test_deletion_config_custom(self):
        """Test custom deletion configuration."""
        config = DeletionConfig(deletion_strategy="oldest_first")
        assert config.deletion_strategy == "oldest_first"


class TestOldestFirstStrategy:
    """Test the OldestFirstStrategy."""
    
    def test_empty_backup_list(self):
        """Test strategy with empty backup list."""
        strategy = OldestFirstStrategy()
        result = strategy.select_file_for_deletion([])
        assert result is None
    
    def test_single_backup(self):
        """Test strategy with single backup."""
        strategy = OldestFirstStrategy()
        backup = BackupInfo(
            path=Path("test.jira"),
            created_at=datetime.now(),
            task_id="123"
        )
        
        result = strategy.select_file_for_deletion([backup])
        assert result == backup
    
    def test_multiple_backups_oldest_selected(self):
        """Test that oldest backup is selected."""
        strategy = OldestFirstStrategy()
        now = datetime.now()
        
        backups = [
            BackupInfo(Path("new.jira"), now, "123"),
            BackupInfo(Path("old.jira"), now - timedelta(days=5), "124"),
            BackupInfo(Path("medium.jira"), now - timedelta(days=2), "125"),
        ]
        
        result = strategy.select_file_for_deletion(backups)
        assert result.path.name == "old.jira"


class TestBackupRetentionLadder:
    """Test the BackupRetentionLadder strategy."""
    
    def test_empty_backup_list(self):
        """Test retention ladder with empty backup list."""
        strategy = BackupRetentionLadder()
        result = strategy.select_file_for_deletion([])
        assert result is None
    
    def test_single_backup_not_deleted(self):
        """Test that single backup is not deleted."""
        strategy = BackupRetentionLadder()
        backup = BackupInfo(
            path=Path("test.jira"),
            created_at=datetime.now(),
            task_id="123"
        )
        
        result = strategy.select_file_for_deletion([backup])
        assert result is None
    
    def test_weekly_retention_logic(self):
        """Test weekly retention logic within recent period."""
        strategy = BackupRetentionLadder()
        now = datetime.now()
        
        # Create multiple backups within the same week
        backups = [
            BackupInfo(Path("day1.jira"), now - timedelta(days=1), "123"),
            BackupInfo(Path("day2.jira"), now - timedelta(days=2), "124"),
            BackupInfo(Path("day3.jira"), now - timedelta(days=3), "125"),
        ]
        
        # Should select one for deletion (keeping the latest of the week)
        result = strategy.select_file_for_deletion(backups)
        assert result is not None
        # Should delete the oldest within the week
        assert result.path.name == "day3.jira"
    
    def test_monthly_retention_logic(self):
        """Test monthly retention logic for medium-term backups."""
        strategy = BackupRetentionLadder()
        now = datetime.now()
        
        # Create backups from same month but over 30 days ago
        base_date = now - timedelta(days=60)  # 2 months ago
        backups = [
            BackupInfo(Path("month1.jira"), base_date, "123"),
            BackupInfo(Path("month2.jira"), base_date - timedelta(days=5), "124"),
            BackupInfo(Path("month3.jira"), base_date - timedelta(days=10), "125"),
        ]
        
        result = strategy.select_file_for_deletion(backups)
        assert result is not None
        # Should delete the oldest within the month
        assert result.path.name == "month3.jira"
    
    def test_yearly_retention_logic(self):
        """Test yearly retention logic for long-term backups."""
        strategy = BackupRetentionLadder()
        now = datetime.now()
        
        # Create backups from same year but over 1 year ago
        base_date = now - timedelta(days=400)  # Over 1 year ago
        backups = [
            BackupInfo(Path("year1.jira"), base_date, "123"),
            BackupInfo(Path("year2.jira"), base_date - timedelta(days=30), "124"),
            BackupInfo(Path("year3.jira"), base_date - timedelta(days=60), "125"),
        ]
        
        result = strategy.select_file_for_deletion(backups)
        assert result is not None
        # Should delete the oldest within the year
        assert result.path.name == "year3.jira"
    
    def test_mixed_time_periods(self):
        """Test retention ladder with backups across all time periods."""
        strategy = BackupRetentionLadder()
        now = datetime.now()
        
        backups = [
            # Recent (within 1 month)
            BackupInfo(Path("recent1.jira"), now - timedelta(days=1), "1"),
            BackupInfo(Path("recent2.jira"), now - timedelta(days=2), "2"),
            
            # Medium term (1 month to 1 year)
            BackupInfo(Path("medium1.jira"), now - timedelta(days=60), "3"),
            BackupInfo(Path("medium2.jira"), now - timedelta(days=65), "4"),
            
            # Long term (over 1 year)
            BackupInfo(Path("long1.jira"), now - timedelta(days=400), "5"),
            BackupInfo(Path("long2.jira"), now - timedelta(days=405), "6"),
        ]
        
        result = strategy.select_file_for_deletion(backups)
        assert result is not None
        # Should prioritize deleting the oldest candidate
        assert result.path.name in ["recent2.jira", "medium2.jira", "long2.jira"]


class TestBackupDeleter:
    """Test the BackupDeleter main class."""
    
    def test_backup_deleter_creation_default(self):
        """Test creating BackupDeleter with default config."""
        config = DeletionConfig()
        deleter = BackupDeleter(config)
        
        assert deleter.config.deletion_strategy == "retention_ladder"
        assert isinstance(deleter.strategy, BackupRetentionLadder)
    
    def test_backup_deleter_creation_oldest_first(self):
        """Test creating BackupDeleter with oldest_first strategy."""
        config = DeletionConfig(deletion_strategy="oldest_first")
        deleter = BackupDeleter(config)
        
        assert deleter.config.deletion_strategy == "oldest_first"
        assert isinstance(deleter.strategy, OldestFirstStrategy)
    
    def test_unknown_strategy_raises_error(self):
        """Test that unknown strategy raises ValueError."""
        config = DeletionConfig(deletion_strategy="unknown_strategy")
        
        with pytest.raises(ValueError, match="Unknown deletion strategy"):
            BackupDeleter(config)
    
    def test_jira_filename_pattern_recognition(self):
        """Test JIRA filename pattern recognition."""
        config = DeletionConfig()
        deleter = BackupDeleter(config)
        
        # Valid JIRA patterns
        assert deleter._is_backup_file("665805-jira-backup-2025-07-19", "jira")
        assert deleter._is_backup_file("123456-JIRA-BACKUP-2025-07-20", "jira")  # Case insensitive
        
        # Invalid patterns
        assert not deleter._is_backup_file("confluence-backup-2025-07-19", "jira")
        assert not deleter._is_backup_file("invalid-filename.zip", "jira")
        assert not deleter._is_backup_file("665805-confluence-backup-2025-07-19", "jira")
    
    def test_confluence_filename_pattern_recognition(self):
        """Test Confluence filename pattern recognition."""
        config = DeletionConfig()
        deleter = BackupDeleter(config)
        
        # Valid Confluence patterns
        assert deleter._is_backup_file("confluence-backup-2025-07-19", "confluence")
        assert deleter._is_backup_file("CONFLUENCE-BACKUP-2025-07-20", "confluence")  # Case insensitive
        
        # Invalid patterns
        assert not deleter._is_backup_file("665805-jira-backup-2025-07-19", "confluence")
        assert not deleter._is_backup_file("invalid-filename.zip", "confluence")
        assert not deleter._is_backup_file("123456-confluence-backup-2025-07-19", "confluence")  # Has task ID
    
    def test_jira_task_id_extraction(self):
        """Test task ID extraction from JIRA filenames."""
        config = DeletionConfig()
        deleter = BackupDeleter(config)
        
        assert deleter._extract_task_id("665805-jira-backup-2025-07-19", "jira") == "665805"
        assert deleter._extract_task_id("123456-jira-backup-2025-07-20", "jira") == "123456"
        assert deleter._extract_task_id("invalid-filename.zip", "jira") == "unknown"
    
    def test_confluence_task_id_extraction(self):
        """Test task ID extraction from Confluence filenames."""
        config = DeletionConfig()
        deleter = BackupDeleter(config)
        
        assert deleter._extract_task_id("confluence-backup-2025-07-19", "confluence") == "n/a"
        assert deleter._extract_task_id("confluence-backup-2025-07-20", "confluence") == "n/a"
    
    def test_scan_nonexistent_directory(self):
        """Test scanning a non-existent directory."""
        config = DeletionConfig()
        deleter = BackupDeleter(config)
        
        result = deleter.scan_backups_in_directory(Path("/nonexistent/path"), "jira")
        assert result == []
    
    def test_scan_empty_directory(self):
        """Test scanning an empty directory."""
        config = DeletionConfig()
        deleter = BackupDeleter(config)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            result = deleter.scan_backups_in_directory(temp_path, "jira")
            assert result == []
    
    def test_scan_directory_with_jira_backups(self):
        """Test scanning directory with JIRA backup files."""
        config = DeletionConfig()
        deleter = BackupDeleter(config)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create test JIRA backup files
            jira_files = [
                "665805-jira-backup-2025-07-19",
                "665806-jira-backup-2025-07-20",
                "invalid-file.txt"  # Should be ignored
            ]
            
            for filename in jira_files:
                (temp_path / filename).touch()
            
            result = deleter.scan_backups_in_directory(temp_path, "jira")
            
            # Should find 2 JIRA backups, ignore the invalid file
            assert len(result) == 2
            backup_names = [backup.path.name for backup in result]
            assert "665805-jira-backup-2025-07-19" in backup_names
            assert "665806-jira-backup-2025-07-20" in backup_names
            assert "invalid-file.txt" not in backup_names
            
            # Check task IDs are extracted correctly
            for backup in result:
                if backup.path.name == "665805-jira-backup-2025-07-19":
                    assert backup.task_id == "665805"
                elif backup.path.name == "665806-jira-backup-2025-07-20":
                    assert backup.task_id == "665806"
    
    def test_scan_directory_with_confluence_backups(self):
        """Test scanning directory with Confluence backup files."""
        config = DeletionConfig()
        deleter = BackupDeleter(config)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create test Confluence backup files
            confluence_files = [
                "confluence-backup-2025-07-19",
                "confluence-backup-2025-07-20",
                "665805-jira-backup-2025-07-19"  # Should be ignored for confluence scan
            ]
            
            for filename in confluence_files:
                (temp_path / filename).touch()
            
            result = deleter.scan_backups_in_directory(temp_path, "confluence")
            
            # Should find 2 Confluence backups, ignore the JIRA file
            assert len(result) == 2
            backup_names = [backup.path.name for backup in result]
            assert "confluence-backup-2025-07-19" in backup_names
            assert "confluence-backup-2025-07-20" in backup_names
            assert "665805-jira-backup-2025-07-19" not in backup_names
            
            # Check task IDs are "n/a" for Confluence
            for backup in result:
                assert backup.task_id == "n/a"
    
    def test_delete_one_backup_no_files(self):
        """Test delete_one_backup with no backup files."""
        config = DeletionConfig()
        deleter = BackupDeleter(config)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            result = deleter.delete_one_backup(temp_path, "jira")
            assert result is None
    
    def test_delete_one_backup_strategy_decides_no_deletion(self):
        """Test delete_one_backup when strategy decides not to delete."""
        config = DeletionConfig()
        deleter = BackupDeleter(config)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create a single backup file
            (temp_path / "665805-jira-backup-2025-07-19").touch()
            
            # Retention ladder should not delete a single file
            result = deleter.delete_one_backup(temp_path, "jira")
            assert result is None
            
            # File should still exist
            assert (temp_path / "665805-jira-backup-2025-07-19").exists()
    
    def test_delete_one_backup_successful_deletion(self):
        """Test successful deletion of one backup file."""
        config = DeletionConfig(deletion_strategy="oldest_first")  # Use oldest_first for predictable behavior
        deleter = BackupDeleter(config)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create multiple backup files with different timestamps
            old_file = temp_path / "665805-jira-backup-2025-07-18"
            new_file = temp_path / "665806-jira-backup-2025-07-19"
            
            old_file.touch()
            new_file.touch()
            
            # Modify the creation time of the old file
            import time
            old_time = time.time() - 86400  # 1 day ago
            os.utime(old_file, (old_time, old_time))
            
            result = deleter.delete_one_backup(temp_path, "jira")
            
            # Should return the path of the deleted file
            assert result is not None
            assert result.name == "665805-jira-backup-2025-07-18"
            
            # Old file should be deleted, new file should remain
            assert not old_file.exists()
            assert new_file.exists()
    
    @patch('pathlib.Path.unlink')
    def test_delete_one_backup_deletion_failure(self, mock_unlink):
        """Test handling of deletion failure."""
        mock_unlink.side_effect = OSError("Permission denied")
        
        config = DeletionConfig(deletion_strategy="oldest_first")
        deleter = BackupDeleter(config)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create a backup file
            test_file = temp_path / "665805-jira-backup-2025-07-19"
            test_file.touch()
            
            # Capture print output
            with patch('builtins.print') as mock_print:
                result = deleter.delete_one_backup(temp_path, "jira")
                
                # Should return None on failure
                assert result is None
                
                # Should print error message
                mock_print.assert_called_once()
                error_message = mock_print.call_args[0][0]
                assert "Failed to delete" in error_message
                assert "Permission denied" in error_message


class TestIntegration:
    """Integration tests for the complete thinning workflow."""
    
    def test_complete_jira_thinning_workflow(self):
        """Test complete workflow with JIRA backups."""
        config = DeletionConfig(deletion_strategy="oldest_first")
        deleter = BackupDeleter(config)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create multiple JIRA backup files
            files = [
                "665801-jira-backup-2025-07-15",
                "665802-jira-backup-2025-07-16", 
                "665803-jira-backup-2025-07-17",
                "665804-jira-backup-2025-07-18",
                "665805-jira-backup-2025-07-19"
            ]
            
            for filename in files:
                (temp_path / filename).touch()
            
            # Set different creation times
            import time
            base_time = time.time()
            for i, filename in enumerate(files):
                file_time = base_time - (len(files) - i) * 86400  # Each file 1 day older
                file_path = temp_path / filename
                os.utime(file_path, (file_time, file_time))
            
            # Delete one backup (should be the oldest)
            deleted_path = deleter.delete_one_backup(temp_path, "jira")
            
            assert deleted_path is not None
            assert deleted_path.name == "665801-jira-backup-2025-07-15"
            
            # Verify only 4 files remain
            remaining_files = list(temp_path.glob("*-jira-backup-*"))
            assert len(remaining_files) == 4
            assert not (temp_path / "665801-jira-backup-2025-07-15").exists()
    
    def test_complete_confluence_thinning_workflow(self):
        """Test complete workflow with Confluence backups."""
        config = DeletionConfig(deletion_strategy="oldest_first")
        deleter = BackupDeleter(config)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create multiple Confluence backup files
            files = [
                "confluence-backup-2025-07-15",
                "confluence-backup-2025-07-16",
                "confluence-backup-2025-07-17",
                "confluence-backup-2025-07-18",
                "confluence-backup-2025-07-19"
            ]
            
            for filename in files:
                (temp_path / filename).touch()
            
            # Set different creation times
            import time
            base_time = time.time()
            for i, filename in enumerate(files):
                file_time = base_time - (len(files) - i) * 86400  # Each file 1 day older
                file_path = temp_path / filename
                os.utime(file_path, (file_time, file_time))
            
            # Delete one backup (should be the oldest)
            deleted_path = deleter.delete_one_backup(temp_path, "confluence")
            
            assert deleted_path is not None
            assert deleted_path.name == "confluence-backup-2025-07-15"
            
            # Verify only 4 files remain
            remaining_files = list(temp_path.glob("confluence-backup-*"))
            assert len(remaining_files) == 4
            assert not (temp_path / "confluence-backup-2025-07-15").exists()
    
    def test_mixed_backup_types_isolation(self):
        """Test that JIRA and Confluence backups are handled separately."""
        config = DeletionConfig(deletion_strategy="oldest_first")
        deleter = BackupDeleter(config)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create mixed backup files
            jira_files = [
                "665801-jira-backup-2025-07-15",
                "665802-jira-backup-2025-07-16"
            ]
            confluence_files = [
                "confluence-backup-2025-07-15",
                "confluence-backup-2025-07-16"
            ]
            
            for filename in jira_files + confluence_files:
                (temp_path / filename).touch()
            
            # Delete JIRA backup - should only affect JIRA files
            jira_deleted = deleter.delete_one_backup(temp_path, "jira")
            assert jira_deleted is not None
            assert "jira" in jira_deleted.name
            
            # Delete Confluence backup - should only affect Confluence files
            confluence_deleted = deleter.delete_one_backup(temp_path, "confluence")
            assert confluence_deleted is not None
            assert "confluence" in confluence_deleted.name
            
            # Verify we still have one of each type
            remaining_jira = list(temp_path.glob("*-jira-backup-*"))
            remaining_confluence = list(temp_path.glob("confluence-backup-*"))
            
            assert len(remaining_jira) == 1
            assert len(remaining_confluence) == 1


class TestLongTermRetentionScenarios:
    """Test realistic long-term backup scenarios."""

    def test_three_years_daily_backups_both_strategies(self):
        """
        Test scenario: Daily backups for 3 years starting February 3, 2022.
        Both strategies should delete the oldest file (February 3, 2022).
        """
        # Generate 3 years of daily backups starting Feb 3, 2022
        start_date = datetime(2022, 2, 3)
        backups = []
        
        for i in range(3 * 365):  # 3 years of daily backups
            backup_date = start_date + timedelta(days=i)
            backup_info = BackupInfo(
                path=Path(f"backup-{backup_date.strftime('%Y-%m-%d')}.jira"),
                created_at=backup_date,
                task_id=f"task-{i+1}"
            )
            backups.append(backup_info)
        
        # Test OldestFirstStrategy - should select February 3, 2022
        oldest_strategy = OldestFirstStrategy()
        selected_oldest = oldest_strategy.select_file_for_deletion(backups)
        
        assert selected_oldest is not None
        assert selected_oldest.created_at == start_date
        assert selected_oldest.path.name == "backup-2022-02-03.jira"
        assert selected_oldest.task_id == "task-1"
        
        # Test BackupRetentionLadder - should also select February 3, 2022
        # (oldest file when many candidates exist)
        ladder_strategy = BackupRetentionLadder()
        selected_ladder = ladder_strategy.select_file_for_deletion(backups)
        
        assert selected_ladder is not None
        assert selected_ladder.created_at == start_date
        assert selected_ladder.path.name == "backup-2022-02-03.jira"
        assert selected_ladder.task_id == "task-1"

    def test_daily_backups_ending_december_2022(self):
        """
        Test scenario: Daily backups ending December 31, 2022.
        - retention_ladder: Should delete January 1, 2022 (oldest in yearly group)
        - oldest_first: Should delete January 1, 2022 (absolute oldest)
        """
        # Generate daily backups for 2022 (Jan 1 - Dec 31)
        start_date = datetime(2022, 1, 1)
        end_date = datetime(2022, 12, 31)
        backups = []
        
        current_date = start_date
        task_counter = 1
        while current_date <= end_date:
            backup_info = BackupInfo(
                path=Path(f"backup-{current_date.strftime('%Y-%m-%d')}.jira"),
                created_at=current_date,
                task_id=f"task-{task_counter}"
            )
            backups.append(backup_info)
            current_date += timedelta(days=1)
            task_counter += 1
        
        # Should have 365 backups for 2022
        assert len(backups) == 365
        
        # Test OldestFirstStrategy - should select January 1, 2022
        oldest_strategy = OldestFirstStrategy()
        selected_oldest = oldest_strategy.select_file_for_deletion(backups)
        
        assert selected_oldest is not None
        assert selected_oldest.created_at == datetime(2022, 1, 1)
        assert selected_oldest.path.name == "backup-2022-01-01.jira"
        assert selected_oldest.task_id == "task-1"
        
        # Test BackupRetentionLadder - should select January 1, 2022
        # (all backups are > 1 year old, so yearly retention applies)
        # Should keep newest per year, delete others starting from oldest
        ladder_strategy = BackupRetentionLadder()
        selected_ladder = ladder_strategy.select_file_for_deletion(backups)
        
        assert selected_ladder is not None
        assert selected_ladder.created_at == datetime(2022, 1, 1)
        assert selected_ladder.path.name == "backup-2022-01-01.jira"
        assert selected_ladder.task_id == "task-1"

    def test_complex_retention_ladder_with_gaps(self):
        """
        Test retention ladder with realistic backup patterns including gaps.
        """
        now = datetime.now()
        backups = []
        
        # Recent backups (last 4 weeks) - daily
        for i in range(28):
            backup_date = now - timedelta(days=i)
            backup_info = BackupInfo(
                path=Path(f"recent-{backup_date.strftime('%Y-%m-%d')}.jira"),
                created_at=backup_date,
                task_id=f"recent-{i+1}"
            )
            backups.append(backup_info)
        
        # Medium-term backups (2-12 months old) - every 3 days
        for i in range(30, 365, 3):
            backup_date = now - timedelta(days=i)
            backup_info = BackupInfo(
                path=Path(f"medium-{backup_date.strftime('%Y-%m-%d')}.jira"),
                created_at=backup_date,
                task_id=f"medium-{i}"
            )
            backups.append(backup_info)
        
        # Long-term backups (1+ years old) - weekly
        for i in range(365, 3*365, 7):
            backup_date = now - timedelta(days=i)
            backup_info = BackupInfo(
                path=Path(f"longterm-{backup_date.strftime('%Y-%m-%d')}.jira"),
                created_at=backup_date,
                task_id=f"longterm-{i}"
            )
            backups.append(backup_info)
        
        # Test that retention ladder selects appropriate candidates
        ladder_strategy = BackupRetentionLadder()
        selected = ladder_strategy.select_file_for_deletion(backups)
        
        assert selected is not None
        # Should select from candidates that violate retention rules
        # (e.g., multiple backups in same week/month/year)
        
        print(f"Selected for deletion: {selected.path.name} from {selected.created_at}")
        
        # Verify it's a valid candidate (not the newest in its time group)
        age = now - selected.created_at
        if age <= timedelta(days=28):
            # Recent backup - should not be newest in its week
            week_start = selected.created_at - timedelta(days=selected.created_at.weekday())
            week_backups = [b for b in backups 
                          if week_start <= b.created_at < week_start + timedelta(days=7)]
            newest_in_week = max(week_backups, key=lambda b: b.created_at)
            assert selected != newest_in_week
        
    def test_retention_ladder_preserves_newest_in_groups(self):
        """
        Test that retention ladder preserves the newest backup in each time group.
        """
        now = datetime.now()
        backups = []
        
        # Create multiple backups in the same week (recent period)
        base_date = now - timedelta(days=7)  # One week ago
        for i in range(5):  # 5 backups in same week
            backup_date = base_date + timedelta(days=i)
            backup_info = BackupInfo(
                path=Path(f"week-backup-{i+1}.jira"),
                created_at=backup_date,
                task_id=f"week-{i+1}"
            )
            backups.append(backup_info)
        
        # Create multiple backups in the same month (medium period)
        month_base = now - timedelta(days=60)  # 2 months ago
        for i in range(3):  # 3 backups in same month
            backup_date = month_base + timedelta(days=i*10)
            backup_info = BackupInfo(
                path=Path(f"month-backup-{i+1}.jira"),
                created_at=backup_date,
                task_id=f"month-{i+1}"
            )
            backups.append(backup_info)
        
        ladder_strategy = BackupRetentionLadder()
        
        # Run deletion multiple times to see pattern
        deletion_candidates = []
        temp_backups = backups.copy()
        
        for _ in range(5):  # Try up to 5 deletions
            selected = ladder_strategy.select_file_for_deletion(temp_backups)
            if selected:
                deletion_candidates.append(selected)
                temp_backups.remove(selected)
            else:
                break
        
        # Verify that newest backups in each group are preserved
        if deletion_candidates:
            # Should not delete the newest backup from the week group
            week_backups = [b for b in backups if "week-backup" in b.path.name]
            newest_week = max(week_backups, key=lambda b: b.created_at)
            assert newest_week not in deletion_candidates
            
            # Should not delete the newest backup from the month group
            month_backups = [b for b in backups if "month-backup" in b.path.name]
            newest_month = max(month_backups, key=lambda b: b.created_at)
            assert newest_month not in deletion_candidates

    def test_edge_case_single_backup_per_period(self):
        """
        Test edge case where there's only one backup per time period.
        Retention ladder should not delete anything.
        """
        now = datetime.now()
        backups = []
        
        # One backup per week for the last 4 weeks
        for i in range(4):
            backup_date = now - timedelta(weeks=i)
            backup_info = BackupInfo(
                path=Path(f"weekly-{i+1}.jira"),
                created_at=backup_date,
                task_id=f"weekly-{i+1}"
            )
            backups.append(backup_info)
        
        # One backup per month for medium term
        for i in range(4, 12):
            backup_date = now - timedelta(days=30*i)
            backup_info = BackupInfo(
                path=Path(f"monthly-{i+1}.jira"),
                created_at=backup_date,
                task_id=f"monthly-{i+1}"
            )
            backups.append(backup_info)
        
        ladder_strategy = BackupRetentionLadder()
        selected = ladder_strategy.select_file_for_deletion(backups)
        
        # Should not delete anything since each backup is the only one in its period
        assert selected is None
