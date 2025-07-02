"""Tests for Confluence client functionality."""

import os
import sys
import pytest
import requests
from unittest.mock import Mock, patch, call, ANY
from datetime import datetime, timedelta, timezone
import json

# Import the ConfluenceClient
from atlassian_cloud_backup.confluence.client import ConfluenceClient

class TestConfluenceClient:
    """Test Confluence client functionality."""

    def setup_method(self, method):
        """Set up test fixtures."""
        self.client = ConfluenceClient(
            url="https://example.atlassian.net",
            username="test@example.com",
            api_token="test-token",
            poll_interval=1,  # Short interval for faster tests
            backup_target_directory="/tmp/backups",
            backup_timeout_minutes=10
        )
        
        # Common mock responses used across tests
        self.mock_complete_status = {
            'currentStatus': 'COMPLETE',
            'alternativePercentage': '100%',
            'fileName': 'backup-123456.zip'
        }
        
        self.mock_in_progress_status = {
            'currentStatus': 'IN_PROGRESS',
            'alternativePercentage': '50%',
        }
        
        self.mock_failed_status = {
            'currentStatus': 'FAILED',
            'alternativePercentage': '0%',
        }

    @patch('atlassian_cloud_backup.confluence.client.make_authenticated_request')
    def test_get_backup_status_success(self, mock_request):
        """Test successful retrieval of backup status."""
        mock_response = Mock()
        mock_response.json.return_value = self.mock_complete_status
        mock_request.return_value = mock_response
        
        result = self.client.get_backup_status()
        
        assert result == self.mock_complete_status
        mock_request.assert_called_once_with(
            'GET',
            'https://example.atlassian.net/wiki/rest/obm/1.0/getprogress.json',
            'test@example.com',
            'test-token'
        )
        
    @patch('atlassian_cloud_backup.confluence.client.make_authenticated_request')
    def test_get_backup_status_unavailable(self, mock_request):
        """Test backup status when Confluence is unavailable (204 response)."""
        mock_response = Mock()
        mock_response.status_code = 204
        mock_request.return_value = mock_response
        
        result = self.client.get_backup_status()
        
        assert result is None
        mock_request.assert_called_once()

    @patch('atlassian_cloud_backup.confluence.client.make_authenticated_request')
    def test_get_backup_status_unauthorized(self, mock_request):
        """Test backup status when unauthorized (401 response)."""
        mock_error = Exception("HTTP Error")
        mock_error.response = Mock(status_code=401)
        mock_request.side_effect = mock_error
        
        result = self.client.get_backup_status()
        
        assert result is None
        mock_request.assert_called_once()

    @patch('atlassian_cloud_backup.confluence.client.make_authenticated_request')
    def test_get_backup_status_other_error(self, mock_request):
        """Test backup status with other error."""
        mock_request.side_effect = Exception("Network error")
        
        with pytest.raises(Exception, match="Network error"):
            self.client.get_backup_status()
        
        mock_request.assert_called_once()

    @patch('atlassian_cloud_backup.confluence.client.make_authenticated_request')
    def test_trigger_backup_success(self, mock_request):
        """Test successful backup triggering."""
        mock_response = Mock()
        mock_request.return_value = mock_response
        
        result = self.client.trigger_backup()
        
        assert result is True
        mock_request.assert_called_once_with(
            'POST', 
            'https://example.atlassian.net/wiki/rest/obm/1.0/runbackup',
            'test@example.com',
            'test-token',
            headers=ANY,
            json={'cbAttachments': True}
        )

    @patch('atlassian_cloud_backup.confluence.client.make_authenticated_request')
    def test_trigger_backup_already_in_progress(self, mock_request):
        """Test backup trigger when backup already in progress (406)."""
        http_error = requests.HTTPError("HTTP Error")
        http_error.response = Mock(status_code=406, text="Backup already in progress")
        mock_request.side_effect = http_error
        
        result = self.client.trigger_backup()
        
        assert result is False
        mock_request.assert_called_once()

    @patch('atlassian_cloud_backup.confluence.client.make_authenticated_request')
    def test_trigger_backup_frequency_limit(self, mock_request):
        """Test backup trigger with frequency limit exceeded (412)."""
        http_error = requests.HTTPError("HTTP Error")
        http_error.response = Mock(status_code=412, text="Backup frequency limit exceeded")
        mock_request.side_effect = http_error
        
        with pytest.raises(requests.HTTPError):
            self.client.trigger_backup()
        
        mock_request.assert_called_once()

    @patch('atlassian_cloud_backup.confluence.client.make_authenticated_request')
    def test_wait_for_completion_immediate_success(self, mock_request):
        """Test wait_for_completion when backup is already complete."""
        mock_response = Mock()
        mock_response.json.return_value = self.mock_complete_status
        mock_request.return_value = mock_response
        
        result = self.client.wait_for_completion()
        
        assert result is True
        mock_request.assert_called_once_with(
            'GET',
            'https://example.atlassian.net/wiki/rest/obm/1.0/getprogress.json',
            'test@example.com',
            'test-token'
        )

    @patch('atlassian_cloud_backup.confluence.client.make_authenticated_request')
    def test_wait_for_completion_with_wait(self, mock_request):
        """Test wait_for_completion when it needs to wait for completion."""
        # First call: in progress
        # Second call: complete
        mock_response_in_progress = Mock()
        mock_response_in_progress.json.return_value = self.mock_in_progress_status
        
        mock_response_complete = Mock()
        mock_response_complete.json.return_value = self.mock_complete_status
        
        mock_request.side_effect = [mock_response_in_progress, mock_response_complete]
        
        result = self.client.wait_for_completion()
        
        assert result is True
        assert mock_request.call_count == 2

    @patch('atlassian_cloud_backup.confluence.client.make_authenticated_request')
    def test_wait_for_completion_failure(self, mock_request):
        """Test wait_for_completion when backup fails."""
        mock_response = Mock()
        mock_response.json.return_value = self.mock_failed_status
        mock_request.return_value = mock_response
        
        result = self.client.wait_for_completion()
        
        assert result is False
        mock_request.assert_called_once()

    @patch('atlassian_cloud_backup.confluence.client.datetime')
    def test_wait_for_completion_timeout(self, mock_datetime):
        """Test wait_for_completion when timeout is exceeded."""
        # Setup datetime mocking
        start_time = datetime.now()
        mock_datetime.now.side_effect = [
            start_time,  # First call in _initialize_confluence_monitoring
            start_time + timedelta(minutes=20)  # Second call in _is_timeout_exceeded - force timeout
        ]
        
        # Call the method directly - we don't need to mock the API calls since we'll timeout immediately
        result = self.client.wait_for_completion()
        
        # Verify the result is False (timed out)
        assert result is False

    @patch('atlassian_cloud_backup.confluence.client.make_authenticated_request')
    def test_wait_for_completion_return_data(self, mock_request):
        """Test wait_for_completion with return_data=True."""
        mock_response = Mock()
        mock_response.json.return_value = self.mock_complete_status
        mock_request.return_value = mock_response
        
        result = self.client.wait_for_completion(return_data=True)
        
        assert result == self.mock_complete_status
        mock_request.assert_called_once()

    @patch('atlassian_cloud_backup.confluence.client.download_file')
    @patch('atlassian_cloud_backup.confluence.client.make_authenticated_request')
    def test_wait_for_file_success(self, mock_request, mock_download):
        """Test waiting for file and downloading it."""
        mock_response = Mock()
        mock_response.json.return_value = self.mock_complete_status
        mock_request.return_value = mock_response
        
        download_path = "/tmp/backups/example_atlassian_net/Confluence_2023-01-01.zip"
        mock_download.return_value = download_path
        
        result = self.client.wait_for_file()
        
        assert result == download_path
        mock_request.assert_called()
        mock_download.assert_called_once()
        
    @patch('atlassian_cloud_backup.confluence.client.ConfluenceClient.wait_for_completion')
    def test_wait_for_file_no_backup_data(self, mock_wait_for_completion):
        """Test wait_for_file when backup completion fails."""
        # Mock wait_for_completion to return False (backup failed)
        mock_wait_for_completion.return_value = False
        
        result = self.client.wait_for_file()
        
        assert result is None
        mock_wait_for_completion.assert_called_once_with(timeout_minutes=600, return_data=True)

    @patch('atlassian_cloud_backup.confluence.client.download_file')
    @patch('atlassian_cloud_backup.confluence.client.make_authenticated_request')
    def test_wait_for_file_no_filename(self, mock_request, mock_download):
        """Test wait_for_file when no filename in response."""
        mock_response = Mock()
        mock_response.json.return_value = {'currentStatus': 'COMPLETE', 'alternativePercentage': '100%'}
        mock_request.return_value = mock_response
        
        result = self.client.wait_for_file()
        
        assert result is None
        mock_request.assert_called()
        mock_download.assert_not_called()

    @patch('atlassian_cloud_backup.confluence.client.ConfluenceClient._get_download_details')
    @patch('atlassian_cloud_backup.confluence.client.ConfluenceClient._download_backup_file')
    @patch('atlassian_cloud_backup.confluence.client.ConfluenceClient.wait_for_completion')
    def test_wait_for_file_with_download(self, mock_wait_completion, mock_download_file, mock_get_details):
        """Test complete wait_for_file process."""
        # Setup mocks for the complete flow
        mock_wait_completion.return_value = {'fileName': 'backup-123456.zip', 'status': 'COMPLETE'}
        
        download_details = {
            'url': 'https://example.atlassian.net/wiki/download/backup-123456.zip',
            'filename': '/tmp/backups/example_atlassian_net/Confluence_2023-01-01.zip'
        }
        mock_get_details.return_value = download_details
        
        download_path = "/tmp/backups/example_atlassian_net/Confluence_2023-01-01.zip"
        mock_download_file.return_value = download_path
        
        # Execute test
        result = self.client.wait_for_file()
        
        # Verify results
        assert result == download_path
        mock_wait_completion.assert_called_once_with(timeout_minutes=600, return_data=True)
        mock_get_details.assert_called_once_with({'fileName': 'backup-123456.zip', 'status': 'COMPLETE'})
        mock_download_file.assert_called_once_with(download_details)

    @patch('atlassian_cloud_backup.confluence.client.FileManager')
    @patch('atlassian_cloud_backup.confluence.client.make_authenticated_request')
    def test_process_backup_confluence_unavailable(self, mock_request, mock_file_manager_class):
        """Test process_backup when Confluence is unavailable."""
        # Setup request mock to indicate Confluence is unavailable
        mock_response = Mock(status_code=204)
        mock_request.return_value = mock_response
        
        # Execute test
        now = datetime.now()
        result = self.client.process_backup({}, now)
        
        # Verify results
        assert result == {'confluence_action': 'SKIPPED_UNAVAILABLE'}
        mock_request.assert_called_once()

    @patch('atlassian_cloud_backup.confluence.client.FileManager')
    @patch('atlassian_cloud_backup.confluence.client.make_authenticated_request')
    def test_process_backup_no_update_needed(self, mock_request, mock_file_manager_class):
        """Test process_backup when no update is needed."""
        # Setup FileManager mock to indicate no backup is needed
        mock_file_manager = Mock()
        mock_file_manager.is_confluence_backup_needed.return_value = (False, "existing_backup.zip")
        mock_file_manager.extract_date_from_confluence_filename.return_value = datetime.now()
        mock_file_manager_class.return_value = mock_file_manager
        
        # Setup request mock for Confluence available
        mock_response = Mock()
        mock_response.json.return_value = self.mock_complete_status
        mock_request.return_value = mock_response
        
        # Execute test
        now = datetime.now()
        result = self.client.process_backup({}, now)
        
        # Verify results
        assert result == {'confluence_action': 'SKIPPED_NO_UPDATE_NEEDED', 'confluence_file': 'existing_backup.zip'}
        mock_request.assert_called_once()
        mock_file_manager.is_confluence_backup_needed.assert_called_once()

    @patch('atlassian_cloud_backup.confluence.client.ConfluenceClient._wait_and_download_backup')
    @patch('atlassian_cloud_backup.confluence.client.ConfluenceClient.trigger_backup')
    @patch('atlassian_cloud_backup.confluence.client.FileManager')
    @patch('atlassian_cloud_backup.confluence.client.make_authenticated_request')
    def test_process_backup_trigger_success(self, mock_request, mock_file_manager_class, mock_trigger, mock_wait_download):
        """Test process_backup with successful backup trigger and download."""
        # Setup FileManager mock to indicate backup is needed
        mock_file_manager = Mock()
        mock_file_manager.is_confluence_backup_needed.return_value = (True, None)
        mock_file_manager_class.return_value = mock_file_manager
        
        # Setup request mock for Confluence available
        mock_response = Mock()
        mock_response.json.return_value = self.mock_complete_status
        mock_request.return_value = mock_response
        
        # Setup trigger and wait_download mocks
        mock_trigger.return_value = True
        mock_wait_download.return_value = {'confluence_action': 'CREATED_NEW', 'confluence_file': 'new_backup.zip'}
        
        # Execute test
        now = datetime.now()
        result = self.client.process_backup({}, now)
        
        # Verify results
        assert result == {'confluence_action': 'CREATED_NEW', 'confluence_file': 'new_backup.zip'}
        mock_request.assert_called_once()
        mock_trigger.assert_called_once()
        mock_wait_download.assert_called_once_with(now)

    @patch('atlassian_cloud_backup.confluence.client.ConfluenceClient._wait_and_download_backup')
    @patch('atlassian_cloud_backup.confluence.client.ConfluenceClient.trigger_backup')
    @patch('atlassian_cloud_backup.confluence.client.FileManager')
    @patch('atlassian_cloud_backup.confluence.client.make_authenticated_request')
    def test_process_backup_backup_in_progress(self, mock_request, mock_file_manager_class, mock_trigger, mock_wait_download):
        """Test process_backup when backup is already in progress."""
        # Setup FileManager mock to indicate backup is needed
        mock_file_manager = Mock()
        mock_file_manager.is_confluence_backup_needed.return_value = (True, None)
        mock_file_manager_class.return_value = mock_file_manager
        
        # Setup request mock for Confluence available
        mock_response = Mock()
        mock_response.json.return_value = self.mock_complete_status
        mock_request.return_value = mock_response
        
        # Setup trigger to indicate backup already in progress (returns False)
        mock_trigger.return_value = False
        mock_wait_download.return_value = {'confluence_action': 'WAITED_FOR_EXISTING', 'confluence_file': 'new_backup.zip'}
        
        # Execute test
        now = datetime.now()
        result = self.client.process_backup({}, now)
        
        # Verify results
        assert result == {'confluence_action': 'WAITED_FOR_EXISTING', 'confluence_file': 'new_backup.zip'}
        mock_request.assert_called_once()
        mock_trigger.assert_called_once()
        mock_wait_download.assert_called_once_with(now, use_existing=True)

    @patch('atlassian_cloud_backup.confluence.client.ConfluenceClient._handle_http_error')
    @patch('atlassian_cloud_backup.confluence.client.ConfluenceClient.trigger_backup')
    @patch('atlassian_cloud_backup.confluence.client.FileManager')
    @patch('atlassian_cloud_backup.confluence.client.make_authenticated_request')
    def test_process_backup_frequency_limit(self, mock_request, mock_file_manager_class, mock_trigger, mock_handle_error):
        """Test process_backup with frequency limit HTTP error."""
        # Setup FileManager mock to indicate backup is needed
        mock_file_manager = Mock()
        mock_file_manager.is_confluence_backup_needed.return_value = (True, None)
        mock_file_manager_class.return_value = mock_file_manager
        
        # Setup request mock for Confluence available
        mock_response = Mock()
        mock_response.json.return_value = self.mock_complete_status
        mock_request.return_value = mock_response
        
        # Setup trigger to raise HTTP 412 error (use requests.HTTPError)
        http_error = requests.HTTPError("HTTP Error")
        http_error.response = Mock(status_code=412)
        mock_trigger.side_effect = http_error
        
        # Setup error handler
        mock_handle_error.return_value = {'confluence_action': 'SKIPPED_FREQUENCY_LIMIT'}
        
        # Execute test
        now = datetime.now()
        result = self.client.process_backup({}, now)
        
        # Verify results
        assert result == {'confluence_action': 'SKIPPED_FREQUENCY_LIMIT'}
        mock_handle_error.assert_called_once_with(http_error)
        mock_request.assert_called_once()
        mock_trigger.assert_called_once()
        mock_handle_error.assert_called_once_with(http_error)

    @patch('atlassian_cloud_backup.confluence.client.FileManager')
    def test_handle_frequency_limit_error_with_existing_backup(self, mock_file_manager_class):
        """Test _handle_frequency_limit_error with existing backup file."""
        # Setup mock error with JSON response
        mock_error = Mock()
        mock_error.response = Mock()
        mock_error.response.json.return_value = {'error': 'You can only create one backup every 24 hours'}
        
        # Setup FileManager mock
        mock_file_manager = Mock()
        mock_file_manager.is_confluence_backup_needed.return_value = (False, "existing_backup.zip")
        mock_file_manager_class.return_value = mock_file_manager
        
        # Execute test
        result = self.client._handle_frequency_limit_error(mock_error)
        
        # Verify results
        assert result == {'confluence_action': 'SKIPPED_FREQUENCY_LIMIT', 'confluence_file': 'existing_backup.zip'}
        mock_file_manager.is_confluence_backup_needed.assert_called_once()

    @patch('atlassian_cloud_backup.confluence.client.FileManager')
    def test_handle_frequency_limit_error_no_existing_backup(self, mock_file_manager_class):
        """Test _handle_frequency_limit_error without existing backup file."""
        # Setup mock error with text response (not JSON)
        mock_error = Mock()
        mock_error.response = Mock()
        mock_error.response.json.side_effect = ValueError("Not JSON")
        mock_error.response.text = "Backup frequency limit exceeded"
        
        # Setup FileManager mock
        mock_file_manager = Mock()
        mock_file_manager.is_confluence_backup_needed.return_value = (True, None)
        mock_file_manager_class.return_value = mock_file_manager
        
        # Execute test
        result = self.client._handle_frequency_limit_error(mock_error)
        
        # Verify results
        assert result == {'confluence_action': 'SKIPPED_FREQUENCY_LIMIT'}
        mock_file_manager.is_confluence_backup_needed.assert_called_once()

    @patch('atlassian_cloud_backup.confluence.client.ConfluenceClient.wait_for_file')
    @patch('atlassian_cloud_backup.confluence.client.ConfluenceClient.wait_for_completion')
    def test_wait_and_download_backup_success(self, mock_wait_completion, mock_wait_file):
        """Test _wait_and_download_backup with successful download."""
        # Setup mocks
        mock_wait_completion.return_value = True
        mock_wait_file.return_value = "/tmp/backups/example_atlassian_net/Confluence_2023-01-01.zip"
        
        # Execute test
        now = datetime.now()
        result = self.client._wait_and_download_backup(now)
        
        # Verify results
        expected = {
            'last_confluence_backup': now,
            'confluence_file': "/tmp/backups/example_atlassian_net/Confluence_2023-01-01.zip",
            'confluence_action': 'CREATED_NEW'
        }
        assert result == expected
        mock_wait_completion.assert_called_once()
        mock_wait_file.assert_called_once()

    @patch('atlassian_cloud_backup.confluence.client.ConfluenceClient.wait_for_file')
    @patch('atlassian_cloud_backup.confluence.client.ConfluenceClient.wait_for_completion')
    def test_wait_and_download_backup_existing(self, mock_wait_completion, mock_wait_file):
        """Test _wait_and_download_backup with existing backup."""
        # Setup mocks
        mock_wait_completion.return_value = True
        mock_wait_file.return_value = "/tmp/backups/example_atlassian_net/Confluence_2023-01-01.zip"
        
        # Execute test
        now = datetime.now()
        result = self.client._wait_and_download_backup(now, use_existing=True)
        
        # Verify results
        expected = {
            'last_confluence_backup': now,
            'confluence_file': "/tmp/backups/example_atlassian_net/Confluence_2023-01-01.zip",
            'confluence_action': 'WAITED_FOR_EXISTING'
        }
        assert result == expected
        mock_wait_completion.assert_called_once()
        mock_wait_file.assert_called_once()

    @patch('atlassian_cloud_backup.confluence.client.ConfluenceClient.wait_for_file')
    @patch('atlassian_cloud_backup.confluence.client.ConfluenceClient.wait_for_completion')
    def test_wait_and_download_backup_completion_failed(self, mock_wait_completion, mock_wait_file):
        """Test _wait_and_download_backup when wait_for_completion fails."""
        # Setup mocks
        mock_wait_completion.return_value = False
        
        # Execute test
        now = datetime.now()
        result = self.client._wait_and_download_backup(now)
        
        # Verify results
        assert result == {'confluence_action': 'FAILED'}
        mock_wait_completion.assert_called_once()
        mock_wait_file.assert_not_called()

    @patch('atlassian_cloud_backup.confluence.client.ConfluenceClient.wait_for_file')
    @patch('atlassian_cloud_backup.confluence.client.ConfluenceClient.wait_for_completion')
    def test_wait_and_download_backup_download_failed(self, mock_wait_completion, mock_wait_file):
        """Test _wait_and_download_backup when wait_for_file fails."""
        # Setup mocks
        mock_wait_completion.return_value = True
        mock_wait_file.return_value = None
        
        # Execute test
        now = datetime.now()
        result = self.client._wait_and_download_backup(now)
        
        # Verify results
        assert result == {'confluence_action': 'FAILED'}
        mock_wait_completion.assert_called_once()
        mock_wait_file.assert_called_once()
        
    @patch('atlassian_cloud_backup.confluence.client.FileManager')
    @patch('atlassian_cloud_backup.confluence.client.ConfluenceClient._is_confluence_available')
    def test_process_backup_generic_exception(self, mock_is_available, mock_file_manager_class):
        """Test process_backup when a generic exception occurs during backup."""
        # Setup mock for confluence availability
        mock_is_available.return_value = True
        
        # Setup FileManager mock to indicate backup is needed
        mock_file_manager = Mock()
        mock_file_manager.is_confluence_backup_needed.return_value = (True, None)
        mock_file_manager_class.return_value = mock_file_manager
        
        # Patch the _attempt_backup_trigger method to raise a generic exception
        with patch.object(self.client, '_attempt_backup_trigger', side_effect=Exception("Unexpected error")):
            # Execute test
            now = datetime.now()
            result = self.client.process_backup({}, now)
        
        # Verify results
        assert result == {'confluence_action': 'FAILED'}
        
    def test_handle_http_error_other_error(self):
        """Test _handle_http_error with an HTTP error other than 412."""
        # Replace the client's _handle_http_error method temporarily for the test
        original_method = self.client._handle_http_error
        
        # Create a wrapper that captures if the raise was executed
        raise_executed = [False]
        def mock_handle_http_error(e):
            try:
                return original_method(e)
            except Exception:
                raise_executed[0] = True
                raise
            
        self.client._handle_http_error = mock_handle_http_error
        
        try:
            # Create an HTTP error with a non-412 status code
            http_error = requests.HTTPError("HTTP Error")
            http_error.response = Mock(status_code=500)
            
            # We need to actually raise the error to test the re-raise
            with patch('logging.error') as mock_log:
                try:
                    raise http_error
                except requests.HTTPError as e:
                    with pytest.raises(requests.HTTPError):
                        self.client._handle_http_error(e)
                
                # Assert that the error was logged
                mock_log.assert_called_once_with('Unexpected error triggering Confluence backup: %s', "HTTP Error")
            
            # Assert that the raise was actually executed
            assert raise_executed[0] is True
            
        finally:
            # Restore the original method
            self.client._handle_http_error = original_method
            
    def test_log_backup_progress(self):
        """Test _log_backup_progress with various status values."""
        # Test with progress available
        with patch('logging.info') as mock_log:
            self.client._log_backup_progress("STARTING", "50%", False)
            mock_log.assert_called_with('Confluence server-side backup progress: %s%%, status: %s', "50%", "STARTING")
            
        # Test without progress
        with patch('logging.info') as mock_log:
            self.client._log_backup_progress("STARTING", None, False)
            mock_log.assert_called_with('Confluence serverside backup status: %s', "STARTING")
            
        # Test with COMPLETE status (should not log)
        with patch('logging.info') as mock_log:
            self.client._log_backup_progress("COMPLETE", "100%", False)
            mock_log.assert_not_called()
