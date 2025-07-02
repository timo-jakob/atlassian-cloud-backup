"""Tests for audit logging functionality."""

import os
import sys
import pytest
import tempfile
from unittest.mock import Mock, patch, call, mock_open
from datetime import datetime, timezone

# Add the src directory to Python path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from atlassian_cloud_backup.utils.audit_utils import AuditLogger


class TestAuditLogger:
    """Test audit logging functionality."""

    def test_format_filesize_bytes(self):
        """Test file size formatting for bytes."""
        assert AuditLogger._format_filesize(0) == "0 B"
        assert AuditLogger._format_filesize(500) == "500 B"
        assert AuditLogger._format_filesize(1023) == "1023 B"

    def test_format_filesize_kb(self):
        """Test file size formatting for kilobytes."""
        assert AuditLogger._format_filesize(1024) == "1.0 KB"
        assert AuditLogger._format_filesize(1536) == "1.5 KB"
        assert AuditLogger._format_filesize(1048575) == "1024.0 KB"

    def test_format_filesize_mb(self):
        """Test file size formatting for megabytes."""
        assert AuditLogger._format_filesize(1048576) == "1.0 MB"
        assert AuditLogger._format_filesize(15728640) == "15.0 MB"
        assert AuditLogger._format_filesize(16106127) == "15.4 MB"

    def test_format_filesize_gb(self):
        """Test file size formatting for gigabytes."""
        assert AuditLogger._format_filesize(1073741824) == "1.0 GB"
        assert AuditLogger._format_filesize(5368709120) == "5.0 GB"
        assert AuditLogger._format_filesize(16106127360) == "15.0 GB"

    def test_format_filesize_tb(self):
        """Test file size formatting for terabytes."""
        # 1 TB
        assert AuditLogger._format_filesize(1099511627776) == "1.0 TB"
        # 2.5 TB
        assert AuditLogger._format_filesize(2748779069440) == "2.5 TB"

    def test_format_filesize_pb(self):
        """Test file size formatting for petabytes."""
        # 1 PB
        assert AuditLogger._format_filesize(1125899906842624) == "1.0 PB"

    @patch('atlassian_cloud_backup.utils.audit_utils.logging')
    @patch('atlassian_cloud_backup.utils.audit_utils.datetime')
    def test_log_success_with_file(self, mock_datetime, mock_logging):
        """Test logging successful backup with file."""
        # Mock datetime
        mock_now = Mock()
        mock_now.strftime.return_value = "2025-05-25 14:30:00"
        mock_datetime.now.return_value = mock_now
        
        AuditLogger.log(
            service_name="Jira",
            site_url="https://example.atlassian.net",
            status="SUCCESS",
            filename="/tmp/backups/jira-backup-2025-05-25.zip",
            filesize=16106127360  # 15.0 GB
        )
        
        expected_message = (
            "AUDIT: 2025-05-25 14:30:00 | "
            "Site: https://example.atlassian.net | "
            "Service: Jira | "
            "Status: SUCCESS | "
            "Filename: jira-backup-2025-05-25.zip | "
            "Filesize: 15.0 GB"
        )
        
        mock_logging.info.assert_called_once_with(expected_message)

    @patch('atlassian_cloud_backup.utils.audit_utils.logging')
    @patch('atlassian_cloud_backup.utils.audit_utils.datetime')
    def test_log_skipped_with_reason(self, mock_datetime, mock_logging):
        """Test logging skipped backup with reason."""
        # Mock datetime
        mock_now = Mock()
        mock_now.strftime.return_value = "2025-05-25 14:30:00"
        mock_datetime.now.return_value = mock_now
        
        AuditLogger.log(
            service_name="Confluence",
            site_url="https://example.atlassian.net",
            status="SKIPPED",
            filename="/tmp/backups/confluence-backup-2025-05-20.zip",
            filesize=1048576,  # 1.0 MB
            reason="Recent backup exists"
        )
        
        expected_message = (
            "AUDIT: 2025-05-25 14:30:00 | "
            "Site: https://example.atlassian.net | "
            "Service: Confluence | "
            "Status: SKIPPED | "
            "Filename: confluence-backup-2025-05-20.zip | "
            "Filesize: 1.0 MB | "
            "Reason: Recent backup exists"
        )
        
        mock_logging.info.assert_called_once_with(expected_message)

    @patch('atlassian_cloud_backup.utils.audit_utils.logging')
    @patch('atlassian_cloud_backup.utils.audit_utils.datetime')
    def test_log_failed_without_file(self, mock_datetime, mock_logging):
        """Test logging failed backup without file."""
        # Mock datetime
        mock_now = Mock()
        mock_now.strftime.return_value = "2025-05-25 14:30:00"
        mock_datetime.now.return_value = mock_now
        
        AuditLogger.log(
            service_name="Jira",
            site_url="https://example.atlassian.net",
            status="FAILED",
            reason="Connection timeout"
        )
        
        expected_message = (
            "AUDIT: 2025-05-25 14:30:00 | "
            "Site: https://example.atlassian.net | "
            "Service: Jira | "
            "Status: FAILED | "
            "Filename: N/A | "
            "Filesize: N/A | "
            "Reason: Connection timeout"
        )
        
        mock_logging.info.assert_called_once_with(expected_message)

    @patch('atlassian_cloud_backup.utils.audit_utils.logging')
    @patch('atlassian_cloud_backup.utils.audit_utils.datetime')
    def test_log_minimal_info(self, mock_datetime, mock_logging):
        """Test logging with minimal information."""
        # Mock datetime
        mock_now = Mock()
        mock_now.strftime.return_value = "2025-05-25 14:30:00"
        mock_datetime.now.return_value = mock_now
        
        AuditLogger.log(
            service_name="Confluence",
            site_url="https://test.atlassian.net",
            status="SUCCESS"
        )
        
        expected_message = (
            "AUDIT: 2025-05-25 14:30:00 | "
            "Site: https://test.atlassian.net | "
            "Service: Confluence | "
            "Status: SUCCESS | "
            "Filename: N/A | "
            "Filesize: N/A"
        )
        
        mock_logging.info.assert_called_once_with(expected_message)

    def test_filename_extraction(self):
        """Test that only the basename is used for filenames."""
        with patch('atlassian_cloud_backup.utils.audit_utils.logging') as mock_logging, \
             patch('atlassian_cloud_backup.utils.audit_utils.datetime') as mock_datetime:
            
            mock_now = Mock()
            mock_now.strftime.return_value = "2025-05-25 14:30:00"
            mock_datetime.now.return_value = mock_now
            
            AuditLogger.log(
                service_name="Jira",
                site_url="https://example.atlassian.net",
                status="SUCCESS",
                filename="/very/long/path/to/backup/jira-backup-2025-05-25.zip"
            )
            
            # Verify only basename is used in log message
            call_args = mock_logging.info.call_args[0][0]
            assert "jira-backup-2025-05-25.zip" in call_args
            assert "/very/long/path" not in call_args

    @patch('atlassian_cloud_backup.utils.audit_utils.os.makedirs')
    @patch('builtins.open', new_callable=mock_open)
    def test_write_to_audit_file(self, mock_file, mock_makedirs):
        """Test writing to audit log file."""
        audit_log_path = "/tmp/audit/atlassian-backup.log"
        audit_message = "AUDIT: 2025-05-25 14:30:00 | Site: https://example.com | Status: SUCCESS"
        
        AuditLogger._write_to_audit_file(audit_log_path, audit_message)
        
        # Check directory was created
        mock_makedirs.assert_called_once_with("/tmp/audit", exist_ok=True)
        
        # Check file was opened correctly
        mock_file.assert_called_once_with(audit_log_path, 'a', encoding='utf-8')
        
        # Check message was written correctly
        mock_file().write.assert_called_once_with(audit_message + '\n')
        
    @patch('atlassian_cloud_backup.utils.audit_utils.os.makedirs')
    @patch('atlassian_cloud_backup.utils.audit_utils.logging')
    def test_write_to_audit_file_error_handling(self, mock_logging, mock_makedirs):
        """Test error handling when writing to audit log file fails."""
        audit_log_path = "/tmp/audit/atlassian-backup.log"
        audit_message = "AUDIT: 2025-05-25 14:30:00 | Site: https://example.com | Status: SUCCESS"
        
        # Simulate error when creating directory
        mock_makedirs.side_effect = PermissionError("Permission denied")
        
        AuditLogger._write_to_audit_file(audit_log_path, audit_message)
        
        # Check error was logged
        mock_logging.error.assert_called_once()
        assert "Failed to write to audit log file" in mock_logging.error.call_args[0][0]
        assert "Permission denied" in mock_logging.error.call_args[0][0]
        
        # Check fallback to standard logger
        mock_logging.info.assert_called_once_with(audit_message)

    @patch('atlassian_cloud_backup.utils.audit_utils.datetime')
    def test_utc_timezone(self, mock_datetime):
        """Test that the timestamp uses UTC timezone."""
        # Setup mock with timezone awareness
        mock_now = Mock()
        mock_now.strftime.return_value = "2025-05-25 14:30:00 UTC"
        mock_datetime.now.return_value = mock_now
        
        with patch('atlassian_cloud_backup.utils.audit_utils.logging') as mock_logging:
            AuditLogger.log(
                service_name="Jira",
                site_url="https://example.com",
                status="SUCCESS"
            )
            
            # Check UTC is correctly passed to now() function
            mock_datetime.now.assert_called_once_with(timezone.utc)
            
            # Check UTC is in the log message
            call_args = mock_logging.info.call_args[0][0]
            assert "UTC" in call_args

    @patch('atlassian_cloud_backup.utils.audit_utils.logging')
    def test_log_with_custom_audit_file(self, mock_logging):
        """Test logging with custom audit file path."""
        custom_audit_path = "/tmp/custom/audit.log"
        
        with patch.object(AuditLogger, '_write_to_audit_file') as mock_write:
            AuditLogger.log(
                service_name="Jira",
                site_url="https://example.com",
                status="SUCCESS",
                audit_log_path=custom_audit_path
            )
            
            # Check _write_to_audit_file was called with correct parameters
            assert mock_write.called
            assert custom_audit_path in mock_write.call_args[0]
            
            # Check standard logging was not used
            assert not mock_logging.info.called


class TestAuditLoggerIntegration:
    """Integration tests for audit logging."""

    def test_audit_log_format_consistency(self):
        """Test that audit log format is consistent across different scenarios."""
        test_cases = [
            {
                'service': 'Jira',
                'status': 'SUCCESS',
                'filename': '/tmp/jira.zip',
                'filesize': 1024,
                'reason': None
            },
            {
                'service': 'Confluence',
                'status': 'SKIPPED',
                'filename': None,
                'filesize': None,
                'reason': 'Service unavailable'
            },
            {
                'service': 'Jira',
                'status': 'FAILED',
                'filename': '/tmp/partial.zip',
                'filesize': 512,
                'reason': 'Download interrupted'
            }
        ]
        
        with patch('atlassian_cloud_backup.utils.audit_utils.logging') as mock_logging, \
             patch('atlassian_cloud_backup.utils.audit_utils.datetime') as mock_datetime:
            
            mock_now = Mock()
            mock_now.strftime.return_value = "2025-05-25 14:30:00"
            mock_datetime.now.return_value = mock_now
            
            for case in test_cases:
                AuditLogger.log(
                    service_name=case['service'],
                    site_url="https://example.atlassian.net",
                    status=case['status'],
                    filename=case['filename'],
                    filesize=case['filesize'],
                    reason=case['reason']
                )
        
        # Verify all calls have the expected format structure
        assert mock_logging.info.call_count == 3
        
        for call in mock_logging.info.call_args_list:
            message = call[0][0]
            # Each audit message should start with AUDIT: and contain all required fields
            assert message.startswith("AUDIT: 2025-05-25 14:30:00")
            assert "Site: https://example.atlassian.net" in message
            assert "Service:" in message
            assert "Status:" in message
            assert "Filename:" in message
            assert "Filesize:" in message
            
    def test_actual_file_writing(self):
        """Integration test for writing to an actual file."""
        # Create temporary directory and file
        with tempfile.TemporaryDirectory() as temp_dir:
            audit_file_path = os.path.join(temp_dir, "audit.log")
            
            # Log an audit entry to the file
            AuditLogger.log(
                service_name="TestService",
                site_url="https://test.example.com",
                status="SUCCESS",
                filename="/tmp/test.zip",
                filesize=2048,
                audit_log_path=audit_file_path
            )
            
            # Verify file exists and contains the expected content
            assert os.path.exists(audit_file_path)
            
            with open(audit_file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                
                # Check basic content structure
                assert "AUDIT:" in content
                assert "Site: https://test.example.com" in content
                assert "Service: TestService" in content
                assert "Status: SUCCESS" in content
                assert "Filename: test.zip" in content
                assert "Filesize: 2.0 KB" in content
                
            # Log another entry to the same file
            AuditLogger.log(
                service_name="AnotherService",
                site_url="https://test.example.com",
                status="FAILED",
                reason="Testing failure",
                audit_log_path=audit_file_path
            )
            
            # Check that both entries are in the file
            with open(audit_file_path, 'r', encoding='utf-8') as f:
                content = f.read().splitlines()
                assert len(content) == 2
                assert "Service: TestService" in content[0]
                assert "Service: AnotherService" in content[1]
                assert "Reason: Testing failure" in content[1]
