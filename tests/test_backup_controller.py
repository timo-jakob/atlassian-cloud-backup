"""Tests for the BackupController module."""

import os
import pytest
from unittest.mock import patch, Mock, MagicMock
from datetime import datetime, timezone

from atlassian_cloud_backup.backup_controller import BackupController
from atlassian_cloud_backup.jira.client import JiraClient
from atlassian_cloud_backup.confluence.client import ConfluenceClient
from atlassian_cloud_backup.utils.file_utils import FileManager
from atlassian_cloud_backup.utils.audit_utils import AuditLogger

class TestBackupController:
    """Test suite for the BackupController class."""
    
    def setup_method(self):
        """Set up test fixtures before each test."""
        self.url = "https://example.atlassian.net"
        self.username = "test_user"
        self.api_token = "test_token"
        self.poll_interval = 15
        self.backup_dir = "/tmp/test_backups"
        
        # Create backup controller with mock clients
        with patch('atlassian_cloud_backup.backup_controller.JiraClient') as mock_jira_client, \
             patch('atlassian_cloud_backup.backup_controller.ConfluenceClient') as mock_confluence_client, \
             patch('atlassian_cloud_backup.backup_controller.FileManager') as mock_file_manager:
            
            self.mock_jira_client = Mock()
            self.mock_confluence_client = Mock()
            self.mock_file_manager = Mock()
            
            mock_jira_client.return_value = self.mock_jira_client
            mock_confluence_client.return_value = self.mock_confluence_client
            mock_file_manager.return_value = self.mock_file_manager
            
            # Configure mock file manager
            self.mock_file_manager.get_backup_folder.return_value = self.backup_dir
            self.mock_file_manager.get_audit_log_path.return_value = os.path.join(self.backup_dir, "audit.log")
            
            # Create the controller
            self.controller = BackupController(
                self.url, 
                self.username, 
                self.api_token, 
                self.poll_interval, 
                self.backup_dir
            )

    def test_initialization(self):
        """Test controller initialization with correct parameters."""
        assert self.controller.url == self.url
        assert self.controller.username == self.username
        assert self.controller.api_token == self.api_token
        assert self.controller.poll_interval == self.poll_interval
        assert self.controller.backup_target_directory == self.backup_dir
        
        # Verify the clients were initialized with expected parameters
        assert self.controller.jira_client == self.mock_jira_client
        assert self.controller.confluence_client == self.mock_confluence_client
        assert self.controller.file_manager == self.mock_file_manager

    def test_orchestrate_success_both(self):
        """Test successful orchestration with both Jira and Confluence backups."""
        # Mock the process methods
        jira_result = {'jira_action': 'CREATED_NEW', 'jira_file': '/tmp/jira_backup.zip'}
        confluence_result = {'confluence_action': 'CREATED_NEW', 'confluence_file': '/tmp/confluence_backup.zip'}
        
        self.controller._process_jira_backup = Mock(return_value=jira_result)
        self.controller._process_confluence_backup = Mock(return_value=confluence_result)
        
        # Run the orchestration
        result = self.controller.orchestrate()
        
        # Verify results
        assert result is True
        self.controller._process_jira_backup.assert_called_once()
        self.controller._process_confluence_backup.assert_called_once()

    def test_orchestrate_no_updates(self):
        """Test orchestration when no backups are performed."""
        # Mock the process methods to return empty updates
        self.controller._process_jira_backup = Mock(return_value={})
        self.controller._process_confluence_backup = Mock(return_value={})
        
        # Run the orchestration
        result = self.controller.orchestrate()
        
        # Verify results
        assert result is False
        self.controller._process_jira_backup.assert_called_once()
        self.controller._process_confluence_backup.assert_called_once()

    def test_process_jira_backup_success(self):
        """Test successful Jira backup processing."""
        # Mock the Jira client's process_backup method
        jira_result = {'jira_action': 'CREATED_NEW', 'jira_file': '/tmp/jira_backup.zip'}
        self.mock_jira_client.process_backup.return_value = jira_result
        
        # Mock the audit logging method
        self.controller._handle_jira_audit_logging = Mock()
        
        # Call the method
        now = datetime.now(timezone.utc)
        status = {}
        result = self.controller._process_jira_backup(status, now)
        
        # Verify results
        assert result == jira_result
        self.mock_jira_client.process_backup.assert_called_once_with(status, now)
        self.controller._handle_jira_audit_logging.assert_called_once_with(jira_result)

    def test_process_jira_backup_exception(self):
        """Test Jira backup processing with exception."""
        # Mock the Jira client's process_backup method to raise an exception
        self.mock_jira_client.process_backup.side_effect = Exception("Test error")
        
        # Mock the audit logging method
        self.controller._log_jira_audit = Mock()
        
        # Call the method
        now = datetime.now(timezone.utc)
        status = {}
        result = self.controller._process_jira_backup(status, now)
        
        # Verify results
        assert result == {}
        self.mock_jira_client.process_backup.assert_called_once_with(status, now)
        self.controller._log_jira_audit.assert_called_once_with('FAILED', None, None, "Test error")

    def test_process_confluence_backup_success(self):
        """Test successful Confluence backup processing."""
        # Mock the Confluence client's process_backup method
        confluence_result = {'confluence_action': 'CREATED_NEW', 'confluence_file': '/tmp/confluence_backup.zip'}
        self.mock_confluence_client.process_backup.return_value = confluence_result
        
        # Mock the audit logging method
        self.controller._handle_confluence_audit_logging = Mock()
        
        # Call the method
        now = datetime.now(timezone.utc)
        status = {}
        result = self.controller._process_confluence_backup(status, now)
        
        # Verify results
        assert result == confluence_result
        self.mock_confluence_client.process_backup.assert_called_once_with(status, now)
        self.controller._handle_confluence_audit_logging.assert_called_once_with(confluence_result)

    def test_process_confluence_backup_exception(self):
        """Test Confluence backup processing with exception."""
        # Mock the Confluence client's process_backup method to raise an exception
        self.mock_confluence_client.process_backup.side_effect = Exception("Test error")
        
        # Mock the audit logging method
        self.controller._log_confluence_audit = Mock()
        
        # Call the method
        now = datetime.now(timezone.utc)
        status = {}
        result = self.controller._process_confluence_backup(status, now)
        
        # Verify results
        assert result == {}
        self.mock_confluence_client.process_backup.assert_called_once_with(status, now)
        self.controller._log_confluence_audit.assert_called_once_with('FAILED', None, None, "Test error")

    def test_handle_jira_audit_logging_created_new(self):
        """Test Jira audit logging for CREATED_NEW action."""
        # Mock the file size method and audit logging
        jira_file = '/tmp/jira_backup.zip'
        self.controller._get_file_size = Mock(return_value=1024)
        self.controller._log_jira_audit = Mock()
        
        # Call the method
        jira_updates = {'jira_action': 'CREATED_NEW', 'jira_file': jira_file}
        self.controller._handle_jira_audit_logging(jira_updates)
        
        # Verify the correct audit log was called
        self.controller._log_jira_audit.assert_called_once_with(
            'SUCCESS', jira_file, 1024, self.controller.BACKUP_NEW_REASON
        )

    def test_handle_jira_audit_logging_reused_existing(self):
        """Test Jira audit logging for REUSED_EXISTING action."""
        # Mock the file size method and audit logging
        jira_file = '/tmp/jira_backup.zip'
        self.controller._get_file_size = Mock(return_value=1024)
        self.controller._log_jira_audit = Mock()
        
        # Call the method
        jira_updates = {'jira_action': 'REUSED_EXISTING', 'jira_file': jira_file}
        self.controller._handle_jira_audit_logging(jira_updates)
        
        # Verify the correct audit log was called
        self.controller._log_jira_audit.assert_called_once_with(
            'SUCCESS', jira_file, 1024, self.controller.BACKUP_REUSED_LAST_SERVER_BACKUP
        )

    def test_handle_jira_audit_logging_no_update(self):
        """Test Jira audit logging for NO_UPDATE_NEEDED action."""
        self.controller._log_jira_audit = Mock()
        
        # Call the method
        jira_updates = {'jira_action': 'NO_UPDATE_NEEDED'}
        self.controller._handle_jira_audit_logging(jira_updates)
        
        # Verify the correct audit log was called
        self.controller._log_jira_audit.assert_called_once_with(
            'SKIPPED', None, None, self.controller.BACKUP_SKIP_REASON_FREQUENCY_LIMIT
        )

    def test_handle_jira_audit_logging_failed(self):
        """Test Jira audit logging for FAILED action."""
        self.controller._log_jira_audit = Mock()
        
        # Call the method
        jira_updates = {'jira_action': 'FAILED'}
        self.controller._handle_jira_audit_logging(jira_updates)
        
        # Verify the correct audit log was called
        self.controller._log_jira_audit.assert_called_once_with(
            'FAILED', None, None, self.controller.BACKUP_FAILED_REASON
        )

    def test_handle_jira_audit_logging_no_action(self):
        """Test Jira audit logging when no action is provided."""
        self.controller._log_jira_audit = Mock()
        
        # Call the method with empty updates
        jira_updates = {}
        self.controller._handle_jira_audit_logging(jira_updates)
        
        # Verify no logging occurred
        self.controller._log_jira_audit.assert_not_called()

    def test_handle_confluence_audit_logging_created_new(self):
        """Test Confluence audit logging for CREATED_NEW action."""
        # Mock the file size method and audit logging
        confluence_file = '/tmp/confluence_backup.zip'
        self.controller._get_file_size = Mock(return_value=1024)
        self.controller._log_confluence_audit = Mock()
        
        # Call the method
        confluence_updates = {'confluence_action': 'CREATED_NEW', 'confluence_file': confluence_file}
        self.controller._handle_confluence_audit_logging(confluence_updates)
        
        # Verify the correct audit log was called
        self.controller._log_confluence_audit.assert_called_once_with(
            'SUCCESS', confluence_file, 1024, self.controller.BACKUP_NEW_REASON
        )

    def test_handle_confluence_audit_logging_waited_for_existing(self):
        """Test Confluence audit logging for WAITED_FOR_EXISTING action."""
        # Mock the file size method and audit logging
        confluence_file = '/tmp/confluence_backup.zip'
        self.controller._get_file_size = Mock(return_value=1024)
        self.controller._log_confluence_audit = Mock()
        
        # Call the method
        confluence_updates = {'confluence_action': 'WAITED_FOR_EXISTING', 'confluence_file': confluence_file}
        self.controller._handle_confluence_audit_logging(confluence_updates)
        
        # Verify the correct audit log was called
        self.controller._log_confluence_audit.assert_called_once_with(
            'SUCCESS', confluence_file, 1024, self.controller.BACKUP_REUSED_LAST_SERVER_BACKUP
        )

    def test_handle_confluence_audit_logging_skipped_frequency(self):
        """Test Confluence audit logging for SKIPPED_FREQUENCY_LIMIT action."""
        # Mock the file size method and audit logging
        confluence_file = '/tmp/confluence_backup.zip'
        self.controller._get_file_size = Mock(return_value=1024)
        self.controller._log_confluence_audit = Mock()
        
        # Call the method
        confluence_updates = {'confluence_action': 'SKIPPED_FREQUENCY_LIMIT', 'confluence_file': confluence_file}
        self.controller._handle_confluence_audit_logging(confluence_updates)
        
        # Verify the correct audit log was called
        self.controller._log_confluence_audit.assert_called_once_with(
            'SKIPPED', confluence_file, 1024, self.controller.BACKUP_SKIP_REASON_FREQUENCY_LIMIT
        )

    def test_handle_confluence_audit_logging_skipped_no_update(self):
        """Test Confluence audit logging for SKIPPED_NO_UPDATE_NEEDED action."""
        # Mock the file size method and audit logging
        confluence_file = '/tmp/confluence_backup.zip'
        self.controller._get_file_size = Mock(return_value=1024)
        self.controller._log_confluence_audit = Mock()
        
        # Call the method
        confluence_updates = {'confluence_action': 'SKIPPED_NO_UPDATE_NEEDED', 'confluence_file': confluence_file}
        self.controller._handle_confluence_audit_logging(confluence_updates)
        
        # Verify the correct audit log was called
        self.controller._log_confluence_audit.assert_called_once_with(
            'SKIPPED', confluence_file, 1024, self.controller.BACKUP_SKIP_REASON_FREQUENCY_LIMIT_1_DAY
        )

    def test_handle_confluence_audit_logging_skipped_unavailable(self):
        """Test Confluence audit logging for SKIPPED_UNAVAILABLE action."""
        self.controller._log_confluence_audit = Mock()
        
        # Call the method
        confluence_updates = {'confluence_action': 'SKIPPED_UNAVAILABLE'}
        self.controller._handle_confluence_audit_logging(confluence_updates)
        
        # Verify the correct audit log was called
        self.controller._log_confluence_audit.assert_called_once_with(
            'SKIPPED', None, None, self.controller.CONFLUENCE_BACKUP_SKIPPED_UNLICENSED_REASON
        )

    def test_handle_confluence_audit_logging_failed(self):
        """Test Confluence audit logging for FAILED action."""
        self.controller._log_confluence_audit = Mock()
        
        # Call the method
        confluence_updates = {'confluence_action': 'FAILED'}
        self.controller._handle_confluence_audit_logging(confluence_updates)
        
        # Verify the correct audit log was called
        self.controller._log_confluence_audit.assert_called_once_with(
            'FAILED', None, None, self.controller.BACKUP_FAILED_REASON
        )

    def test_handle_confluence_audit_logging_no_action(self):
        """Test Confluence audit logging when no action is provided."""
        self.controller._log_confluence_audit = Mock()
        
        # Call the method with empty updates
        confluence_updates = {}
        self.controller._handle_confluence_audit_logging(confluence_updates)
        
        # Verify no logging occurred
        self.controller._log_confluence_audit.assert_not_called()

    def test_log_jira_audit(self):
        """Test Jira audit logging."""
        with patch('atlassian_cloud_backup.backup_controller.AuditLogger') as mock_audit_logger:
            # Call the method
            self.controller._log_jira_audit('SUCCESS', '/tmp/file.zip', 1024, 'Test reason')
            
            # Verify the audit logger was called correctly
            mock_audit_logger.log.assert_called_once_with(
                'Jira', self.url, 'SUCCESS', '/tmp/file.zip', 1024, 'Test reason', 
                self.mock_file_manager.get_audit_log_path.return_value
            )

    def test_log_confluence_audit(self):
        """Test Confluence audit logging."""
        with patch('atlassian_cloud_backup.backup_controller.AuditLogger') as mock_audit_logger:
            # Call the method
            self.controller._log_confluence_audit('SUCCESS', '/tmp/file.zip', 1024, 'Test reason')
            
            # Verify the audit logger was called correctly
            mock_audit_logger.log.assert_called_once_with(
                'Confluence', self.url, 'SUCCESS', '/tmp/file.zip', 1024, 'Test reason', 
                self.mock_file_manager.get_audit_log_path.return_value
            )

    def test_get_file_size_existing_file(self):
        """Test getting size of an existing file."""
        with patch('os.path.exists', return_value=True), \
             patch('os.path.getsize', return_value=2048):
            
            # Call the method
            size = self.controller._get_file_size('/tmp/file.zip')
            
            # Verify the result
            assert size == 2048

    def test_get_file_size_nonexistent_file(self):
        """Test getting size of a non-existent file."""
        with patch('os.path.exists', return_value=False):
            # Call the method
            size = self.controller._get_file_size('/tmp/nonexistent.zip')
            
            # Verify the result
            assert size is None

    def test_get_file_size_none_filename(self):
        """Test getting size when filename is None."""
        # Call the method
        size = self.controller._get_file_size(None)
        
        # Verify the result
        assert size is None
