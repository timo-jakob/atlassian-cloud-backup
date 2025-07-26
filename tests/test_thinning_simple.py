#!/usr/bin/env python3
"""
Simple test runner for the thinning module.

This script runs basic tests on the thinning module without requiring
external dependencies like pytest or the main application dependencies.
"""

import sys
import os
import tempfile
from pathlib import Path
from datetime import datetime, timedelta

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

# Import directly from thinning module
from atlassian_cloud_backup.thinning.manager import (
    BackupInfo,
    DeletionConfig,
    OldestFirstStrategy,
    BackupRetentionLadder,
    BackupDeleter
)


def test_backup_info():
    """Test BackupInfo creation."""
    print("🧪 Testing BackupInfo...")
    
    path = Path("/test/backup.jira")
    created_at = datetime.now()
    task_id = "123456"
    
    backup_info = BackupInfo(path=path, created_at=created_at, task_id=task_id)
    
    assert backup_info.path == path
    assert backup_info.created_at == created_at
    assert backup_info.task_id == task_id
    
    print("✅ BackupInfo tests passed")


def test_deletion_config():
    """Test DeletionConfig."""
    print("🧪 Testing DeletionConfig...")
    
    # Test default config
    config = DeletionConfig()
    assert config.deletion_strategy == "retention_ladder"
    
    # Test custom config
    config = DeletionConfig(deletion_strategy="oldest_first")
    assert config.deletion_strategy == "oldest_first"
    
    print("✅ DeletionConfig tests passed")


def test_oldest_first_strategy():
    """Test OldestFirstStrategy."""
    print("🧪 Testing OldestFirstStrategy...")
    
    strategy = OldestFirstStrategy()
    
    # Test empty list
    result = strategy.select_file_for_deletion([])
    assert result is None
    
    # Test single backup
    backup = BackupInfo(Path("test.jira"), datetime.now(), "123")
    result = strategy.select_file_for_deletion([backup])
    assert result == backup
    
    # Test multiple backups - should select oldest
    now = datetime.now()
    backups = [
        BackupInfo(Path("new.jira"), now, "123"),
        BackupInfo(Path("old.jira"), now - timedelta(days=5), "124"),
        BackupInfo(Path("medium.jira"), now - timedelta(days=2), "125"),
    ]
    
    result = strategy.select_file_for_deletion(backups)
    assert result.path.name == "old.jira"
    
    print("✅ OldestFirstStrategy tests passed")


def test_retention_ladder_strategy():
    """Test BackupRetentionLadder strategy."""
    print("🧪 Testing BackupRetentionLadder...")
    
    strategy = BackupRetentionLadder()
    
    # Test empty list
    result = strategy.select_file_for_deletion([])
    assert result is None
    
    # Test single backup - should not delete
    backup = BackupInfo(Path("test.jira"), datetime.now(), "123")
    result = strategy.select_file_for_deletion([backup])
    assert result is None
    
    # Test weekly retention logic
    now = datetime.now()
    backups = [
        BackupInfo(Path("day1.jira"), now - timedelta(days=1), "123"),
        BackupInfo(Path("day2.jira"), now - timedelta(days=2), "124"),
        BackupInfo(Path("day3.jira"), now - timedelta(days=3), "125"),
    ]
    
    result = strategy.select_file_for_deletion(backups)
    assert result is not None  # Should select one for deletion
    
    print("✅ BackupRetentionLadder tests passed")


def test_backup_deleter():
    """Test BackupDeleter main functionality."""
    print("🧪 Testing BackupDeleter...")
    
    # Test creation with default config
    config = DeletionConfig()
    deleter = BackupDeleter(config)
    assert isinstance(deleter.strategy, BackupRetentionLadder)
    
    # Test creation with oldest_first
    config = DeletionConfig(deletion_strategy="oldest_first")
    deleter = BackupDeleter(config)
    assert isinstance(deleter.strategy, OldestFirstStrategy)
    
    # Test unknown strategy raises error
    try:
        config = DeletionConfig(deletion_strategy="unknown")
        BackupDeleter(config)
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "Unknown deletion strategy" in str(e)
    
    print("✅ BackupDeleter creation tests passed")


def test_filename_patterns():
    """Test filename pattern recognition."""
    print("🧪 Testing filename patterns...")
    
    config = DeletionConfig()
    deleter = BackupDeleter(config)
    
    # Test JIRA patterns
    assert deleter._is_backup_file("665805-jira-backup-2025-07-19", "jira")
    assert deleter._is_backup_file("123456-JIRA-BACKUP-2025-07-20", "jira")  # Case insensitive
    assert not deleter._is_backup_file("confluence-backup-2025-07-19", "jira")
    assert not deleter._is_backup_file("invalid-filename.zip", "jira")
    
    # Test Confluence patterns
    assert deleter._is_backup_file("confluence-backup-2025-07-19", "confluence")
    assert deleter._is_backup_file("CONFLUENCE-BACKUP-2025-07-20", "confluence")  # Case insensitive
    assert not deleter._is_backup_file("665805-jira-backup-2025-07-19", "confluence")
    assert not deleter._is_backup_file("123456-confluence-backup-2025-07-19", "confluence")  # Has task ID
    
    print("✅ Filename pattern tests passed")


def test_task_id_extraction():
    """Test task ID extraction."""
    print("🧪 Testing task ID extraction...")
    
    config = DeletionConfig()
    deleter = BackupDeleter(config)
    
    # Test JIRA task ID extraction
    assert deleter._extract_task_id("665805-jira-backup-2025-07-19", "jira") == "665805"
    assert deleter._extract_task_id("123456-jira-backup-2025-07-20", "jira") == "123456"
    assert deleter._extract_task_id("invalid-filename.zip", "jira") == "unknown"
    
    # Test Confluence task ID (should be "n/a")
    assert deleter._extract_task_id("confluence-backup-2025-07-19", "confluence") == "n/a"
    assert deleter._extract_task_id("confluence-backup-2025-07-20", "confluence") == "n/a"
    
    print("✅ Task ID extraction tests passed")


def test_directory_scanning():
    """Test directory scanning functionality."""
    print("🧪 Testing directory scanning...")
    
    config = DeletionConfig()
    deleter = BackupDeleter(config)
    
    # Test non-existent directory
    result = deleter.scan_backups_in_directory(Path("/nonexistent/path"), "jira")
    assert result == []
    
    # Test with real directory
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Test empty directory
        result = deleter.scan_backups_in_directory(temp_path, "jira")
        assert result == []
        
        # Create JIRA backup files
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
        
        # Test Confluence backups
        confluence_files = [
            "confluence-backup-2025-07-19",
            "confluence-backup-2025-07-20",
        ]
        
        for filename in confluence_files:
            (temp_path / filename).touch()
        
        result = deleter.scan_backups_in_directory(temp_path, "confluence")
        
        # Should find 2 Confluence backups, ignore JIRA files
        assert len(result) == 2
        backup_names = [backup.path.name for backup in result]
        assert "confluence-backup-2025-07-19" in backup_names
        assert "confluence-backup-2025-07-20" in backup_names
        
        # Check task IDs
        for backup in result:
            assert backup.task_id == "n/a"  # Confluence backups have no task ID
    
    print("✅ Directory scanning tests passed")


def test_end_to_end_workflow():
    """Test complete end-to-end workflow."""
    print("🧪 Testing end-to-end workflow...")
    
    config = DeletionConfig(deletion_strategy="oldest_first")  # Use predictable strategy
    deleter = BackupDeleter(config)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Create multiple backup files
        files = [
            "665801-jira-backup-2025-07-15",
            "665802-jira-backup-2025-07-16",
            "665803-jira-backup-2025-07-17",
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
        
        # Verify only 2 files remain
        remaining_files = list(temp_path.glob("*-jira-backup-*"))
        assert len(remaining_files) == 2
        assert not (temp_path / "665801-jira-backup-2025-07-15").exists()
    
    print("✅ End-to-end workflow tests passed")


def run_all_tests():
    """Run all tests."""
    print("🚀 Running Thinning Module Tests")
    print("=" * 50)
    
    try:
        test_backup_info()
        test_deletion_config()
        test_oldest_first_strategy()
        test_retention_ladder_strategy()
        test_backup_deleter()
        test_filename_patterns()
        test_task_id_extraction()
        test_directory_scanning()
        test_end_to_end_workflow()
        
        print("\n" + "=" * 50)
        print("🎉 All tests passed! The thinning module is working correctly.")
        print("\n📊 Test Coverage Summary:")
        print("✅ BackupInfo dataclass")
        print("✅ DeletionConfig configuration")
        print("✅ OldestFirstStrategy deletion logic")
        print("✅ BackupRetentionLadder sophisticated strategy")
        print("✅ BackupDeleter main functionality")
        print("✅ JIRA filename pattern recognition")
        print("✅ Confluence filename pattern recognition")
        print("✅ Task ID extraction")
        print("✅ Directory scanning")
        print("✅ End-to-end backup deletion workflow")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
