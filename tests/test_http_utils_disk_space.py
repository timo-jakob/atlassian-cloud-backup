"""
Tests for disk space management functionality in http_utils.

This module tests the new disk space monitoring and backup deletion
functionality added to the file download process.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, patch, call
import io

from atlassian_cloud_backup.utils.http_utils import (
    _detect_backup_type_from_filename,
    _ensure_disk_space_available,
    _stream_response_to_file
)
from atlassian_cloud_backup.thinning.manager import BackupDeleter, DeletionConfig


class TestDetectBackupType:
    """Test backup type detection from filename."""
    
    def test_detect_jira_backup(self):
        """Test detection of JIRA backup files."""
        assert _detect_backup_type_from_filename("/path/to/jira-backup-2025-01-01.zip") == "jira"
        assert _detect_backup_type_from_filename("12345-JIRA-backup-2025-07-27.zip") == "jira"
        assert _detect_backup_type_from_filename("mysite-jira-backup-task-67890.zip") == "jira"
    
    def test_detect_confluence_backup(self):
        """Test detection of Confluence backup files."""
        assert _detect_backup_type_from_filename("/path/to/confluence-backup-2025-01-01.zip") == "confluence"
        assert _detect_backup_type_from_filename("CONFLUENCE-backup-2025-07-27.zip") == "confluence"
        assert _detect_backup_type_from_filename("mysite-confluence-backup.zip") == "confluence"
    
    def test_detect_unknown_defaults_to_jira(self):
        """Test that unknown backup types default to JIRA."""
        assert _detect_backup_type_from_filename("/path/to/unknown-backup.zip") == "jira"
        assert _detect_backup_type_from_filename("backup-file.zip") == "jira"
        assert _detect_backup_type_from_filename("some-file.txt") == "jira"


class TestEnsureDiskSpaceAvailable:
    """Test disk space management functionality."""
    
    def test_sufficient_space_no_deletion(self):
        """Test that no deletion occurs when sufficient space is available."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            backup_deleter = Mock()
            
            # Mock disk_usage to return plenty of free space
            with patch('atlassian_cloud_backup.utils.http_utils.shutil.disk_usage') as mock_disk_usage:
                mock_disk_usage.return_value = (100_000_000, 50_000_000, 50_000_000)  # 50MB free
                
                _ensure_disk_space_available(
                    temp_path, 1_000_000, "jira", backup_deleter, "test-service"  # Need 1MB
                )
                
                # Should not attempt any deletions
                backup_deleter.delete_one_backup.assert_not_called()
    
    def test_insufficient_space_triggers_deletion(self):
        """Test that file deletion is triggered when space is insufficient."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            backup_deleter = Mock()
            
            # Create a fake old backup file to be "deleted"
            old_backup = temp_path / "old-jira-backup-2024-01-01.zip"
            old_backup.write_text("old backup content")
            
            # Mock successful deletion
            backup_deleter.delete_one_backup.return_value = old_backup
            
            # Mock disk_usage to show insufficient space initially, then sufficient after deletion
            with patch('atlassian_cloud_backup.utils.http_utils.shutil.disk_usage') as mock_disk_usage:
                mock_disk_usage.side_effect = [
                    (100_000_000, 50_000_000, 500_000),   # First call: insufficient space (500KB free)
                    (100_000_000, 50_000_000, 50_000_000) # After deletion: plenty of space (50MB free)
                ]
                
                _ensure_disk_space_available(
                    temp_path, 1_000_000, "jira", backup_deleter, "test-service"  # Need 1MB
                )
                
                # Should attempt deletion once
                backup_deleter.delete_one_backup.assert_called_once_with(temp_path, "jira")
    
    def test_multiple_deletions_if_needed(self):
        """Test that multiple files are deleted if needed to free enough space."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            backup_deleter = Mock()
            
            # Create fake old backup files to be "deleted"
            old_backup1 = temp_path / "old-jira-backup-2024-01-01.zip"
            old_backup2 = temp_path / "old-jira-backup-2024-01-02.zip"
            
            # Mock successful deletions
            backup_deleter.delete_one_backup.side_effect = [old_backup1, old_backup2]
            
            # Mock disk_usage to show insufficient space for first two calls, then sufficient
            with patch('atlassian_cloud_backup.utils.http_utils.shutil.disk_usage') as mock_disk_usage:
                mock_disk_usage.side_effect = [
                    (100_000_000, 50_000_000, 200_000),   # Initial: very low space
                    (100_000_000, 50_000_000, 600_000),   # After 1st deletion: still low
                    (100_000_000, 50_000_000, 2_000_000)  # After 2nd deletion: sufficient
                ]
                
                _ensure_disk_space_available(
                    temp_path, 1_000_000, "jira", backup_deleter, "test-service"  # Need 1MB
                )
                
                # Should attempt deletion twice
                assert backup_deleter.delete_one_backup.call_count == 2
                backup_deleter.delete_one_backup.assert_has_calls([
                    call(temp_path, "jira"),
                    call(temp_path, "jira")
                ])
    
    def test_no_more_files_to_delete(self):
        """Test behavior when no more files can be deleted."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            backup_deleter = Mock()
            
            # Mock that no files are available for deletion
            backup_deleter.delete_one_backup.return_value = None
            
            # Mock disk_usage to show insufficient space
            with patch('atlassian_cloud_backup.utils.http_utils.shutil.disk_usage') as mock_disk_usage:
                mock_disk_usage.return_value = (100_000_000, 50_000_000, 500_000)  # 500KB free
                
                # Should not raise an exception, just log warnings
                _ensure_disk_space_available(
                    temp_path, 1_000_000, "jira", backup_deleter, "test-service"  # Need 1MB
                )
                
                # Should attempt deletion once, then stop
                backup_deleter.delete_one_backup.assert_called_once_with(temp_path, "jira")
    
    def test_max_deletion_attempts_limit(self):
        """Test that deletion attempts are limited to prevent infinite loops."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            backup_deleter = Mock()
            
            # Mock that files are always available for deletion
            fake_files = [temp_path / f"backup-{i}.zip" for i in range(10)]
            backup_deleter.delete_one_backup.side_effect = fake_files
            
            # Mock disk_usage to always show insufficient space
            with patch('atlassian_cloud_backup.utils.http_utils.shutil.disk_usage') as mock_disk_usage:
                mock_disk_usage.return_value = (100_000_000, 50_000_000, 500_000)  # Always 500KB free
                
                _ensure_disk_space_available(
                    temp_path, 1_000_000, "jira", backup_deleter, "test-service"  # Need 1MB
                )
                
                # Should limit attempts to 5 (max_deletion_attempts)
                assert backup_deleter.delete_one_backup.call_count == 5
    
    def test_error_handling_in_space_check(self):
        """Test error handling when disk space check fails."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            backup_deleter = Mock()
            
            # Mock disk_usage to raise an exception
            with patch('atlassian_cloud_backup.utils.http_utils.shutil.disk_usage') as mock_disk_usage:
                mock_disk_usage.side_effect = OSError("Disk access error")
                
                # Should not raise an exception, just log error and continue
                _ensure_disk_space_available(
                    temp_path, 1_000_000, "jira", backup_deleter, "test-service"
                )
                
                # Should not attempt any deletions due to error
                backup_deleter.delete_one_backup.assert_not_called()


class TestStreamResponseWithDiskManagement:
    """Test the enhanced _stream_response_to_file function."""
    
    def test_stream_with_sufficient_space(self):
        """Test streaming when disk space is sufficient throughout."""
        from atlassian_cloud_backup.thinning.manager import BackupDeleter, DeletionConfig
        
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            filename = temp_path / "jira-backup-2025-07-27.zip"

            # Mock response with test content
            mock_response = Mock()
            test_content = [b"chunk1", b"chunk2", b"chunk3"]
            mock_response.iter_content.return_value = test_content

            # Create backup deleter instance for test
            deletion_config = DeletionConfig()
            backup_deleter = BackupDeleter(deletion_config)

            # Mock sufficient disk space
            with patch('atlassian_cloud_backup.utils.http_utils.shutil.disk_usage') as mock_disk_usage:
                mock_disk_usage.return_value = (100_000_000, 50_000_000, 50_000_000)  # 50MB free

                bytes_written = _stream_response_to_file(
                    mock_response, str(filename), 'wb', 0,
                    chunk_size=1024, log_chunk_size=1024*1024,
                    service_name="test-service", overall_start_time=0,
                    backup_type="jira", backup_deleter=backup_deleter
                )                # Should write all content
                assert bytes_written == len(b"chunk1chunk2chunk3")
                
                # Verify file was written
                assert filename.exists()
                assert filename.read_bytes() == b"chunk1chunk2chunk3"
    
    def test_stream_triggers_space_management(self):
        """Test that space management is triggered during streaming."""
        from atlassian_cloud_backup.thinning.manager import BackupDeleter, DeletionConfig
        
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            filename = temp_path / "jira-backup-2025-07-27.zip"

            # Create an old backup file to be deleted
            old_backup = temp_path / "old-jira-backup-2024-01-01.zip"
            old_backup.write_text("old backup content")

            # Mock response with test content
            mock_response = Mock()
            test_content = [b"chunk1", b"chunk2"]
            mock_response.iter_content.return_value = test_content

            # Create backup deleter instance for test
            deletion_config = DeletionConfig()
            backup_deleter = BackupDeleter(deletion_config)

            # Mock the delete_one_backup method on our actual instance
            with patch.object(backup_deleter, 'delete_one_backup', return_value=str(old_backup)) as mock_delete:

                # Mock disk space - insufficient initially, then sufficient after deletion
                with patch('atlassian_cloud_backup.utils.http_utils.shutil.disk_usage') as mock_disk_usage:
                    mock_disk_usage.side_effect = [
                        (100_000_000, 50_000_000, 5_000),    # First chunk: insufficient space
                        (100_000_000, 50_000_000, 50_000_000), # After deletion: sufficient
                        (100_000_000, 50_000_000, 50_000_000), # Second chunk: sufficient
                        (100_000_000, 50_000_000, 50_000_000)  # After deletion: sufficient
                    ]

                    bytes_written = _stream_response_to_file(
                        mock_response, str(filename), 'wb', 0,
                        chunk_size=1024, log_chunk_size=1024*1024,
                        service_name="test-service", overall_start_time=0,
                        backup_type="jira", backup_deleter=backup_deleter
                    )                    # Should still write all content
                    assert bytes_written == len(b"chunk1chunk2")
                    
                    # Should have triggered space management
                    mock_delete.assert_called_with(temp_path, "jira")
                    
                    # Verify file was written
                    assert filename.exists()
                    assert filename.read_bytes() == b"chunk1chunk2"
