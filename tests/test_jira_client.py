"""Tests for Jira client functionality."""

import os
import sys
import pytest
from unittest.mock import Mock, patch, call
from datetime import datetime, timezone

# Add the src directory to Python path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

# Mock the atlassian library since it might not be available in test environment
sys.modules['atlassian'] = Mock()

from atlassian_cloud_backup.jira.client import JiraClient


class TestJiraClient:
    """Test Jira client functionality."""

    def setup_method(self, method):
        """Set up test fixtures."""
        with patch('atlassian_cloud_backup.jira.client.Jira') as mock_jira_class:
            mock_jira_instance = Mock()
            mock_jira_class.return_value = mock_jira_instance
            
            self.client = JiraClient(
                url="https://example.atlassian.net",
                username="test@example.com",
                api_token="test-token",
                poll_interval=1,  # Short interval for faster tests
                backup_target_directory="/tmp/backups",
                jira_backup_timeout_minutes=10
            )

    @patch('atlassian_cloud_backup.jira.client.make_authenticated_request')
    def test_get_download_url_immediate_success(self, mock_request):
        """Test get_download_url when result is immediately available."""
        # Mock successful response with download URL
        mock_response = Mock()
        mock_response.json.return_value = {
            'result': 'backup/download/12345.zip',
            'status': 'COMPLETE',
            'progress': 100
        }
        mock_request.return_value = mock_response
        
        result = self.client.get_download_url(12345)
        
        assert result == "https://example.atlassian.net/plugins/servlet/backup/download/12345.zip"
        mock_request.assert_called_once_with(
            'GET',
            'https://example.atlassian.net/rest/backup/1/export/getProgress',
            'test@example.com',
            'test-token',
            params={'taskId': 12345}
        )

    @patch('atlassian_cloud_backup.jira.client.make_authenticated_request')
    def test_get_download_url_waits_for_completion(self, mock_request):
        """Test get_download_url when it needs to wait for file creation to complete."""
        # First response: no result (file still being created)
        mock_response_no_result = Mock()
        mock_response_no_result.json.return_value = {
            'status': 'IN_PROGRESS',
            'progress': 90
        }
        
        # Progress responses during wait_for_completion
        mock_response_progress = Mock()
        mock_response_progress.json.return_value = {
            'status': 'COMPLETE',
            'progress': 100,
            'result': 'backup/download/12345.zip'
        }
        
        # Final response: result available
        mock_response_with_result = Mock()
        mock_response_with_result.json.return_value = {
            'result': 'backup/download/12345.zip',
            'status': 'COMPLETE',
            'progress': 100
        }
        
        # Setup mock responses: first call has no result, then progress calls, then final success
        mock_request.side_effect = [
            mock_response_no_result,  # Initial call - no result
            mock_response_progress,   # wait_for_completion call - now complete
            mock_response_with_result # Retry call - now has result
        ]
        
        result = self.client.get_download_url(12345)
        
        assert result == "https://example.atlassian.net/plugins/servlet/backup/download/12345.zip"
        # Should have been called 3 times: initial, during wait, and retry
        assert mock_request.call_count == 3

    @patch('atlassian_cloud_backup.jira.client.make_authenticated_request')
    def test_get_download_url_fails_after_wait(self, mock_request):
        """Test get_download_url when backup fails during wait."""
        # First response: no result
        mock_response_no_result = Mock()
        mock_response_no_result.json.return_value = {
            'status': 'IN_PROGRESS',
            'progress': 50
        }
        
        # Progress response showing failure
        mock_response_failed = Mock()
        mock_response_failed.json.return_value = {
            'status': 'FAILED',
            'progress': 50
        }
        
        mock_request.side_effect = [
            mock_response_no_result,  # Initial call - no result
            mock_response_failed      # wait_for_completion call - shows failure
        ]
        
        with pytest.raises(RuntimeError, match="failed to complete or timed out"):
            self.client.get_download_url(12345)

    @patch('atlassian_cloud_backup.jira.client.make_authenticated_request')
    def test_get_download_url_no_result_after_wait(self, mock_request):
        """Test get_download_url when no result is available even after waiting."""
        # First response: no result
        mock_response_no_result = Mock()
        mock_response_no_result.json.return_value = {
            'status': 'IN_PROGRESS',
            'progress': 90
        }
        
        # Progress response showing completion but still no result
        mock_response_complete_no_result = Mock()
        mock_response_complete_no_result.json.return_value = {
            'status': 'COMPLETE',
            'progress': 100
            # Still no 'result' field
        }
        
        mock_request.side_effect = [
            mock_response_no_result,           # Initial call - no result
            mock_response_complete_no_result,  # wait_for_completion call - complete but no result
            mock_response_complete_no_result   # Retry call - still no result
        ]
        
        with pytest.raises(RuntimeError, match="No download URL found.*even after waiting"):
            self.client.get_download_url(12345)

    @patch('atlassian_cloud_backup.jira.client.make_authenticated_request')
    def test_fetch_last_task_id_success(self, mock_request):
        """Test successful fetching of last task ID."""
        mock_response = Mock()
        mock_response.text = "12345"
        mock_request.return_value = mock_response
        
        result = self.client.fetch_last_task_id()
        
        assert result == 12345
        mock_request.assert_called_once_with(
            'GET',
            'https://example.atlassian.net/rest/backup/1/export/lastTaskId',
            'test@example.com',
            'test-token'
        )

    @patch('atlassian_cloud_backup.jira.client.make_authenticated_request')
    def test_fetch_last_task_id_empty_response(self, mock_request):
        """Test fetch_last_task_id with empty response."""
        mock_response = Mock()
        mock_response.text = ""
        mock_request.return_value = mock_response
        
        result = self.client.fetch_last_task_id()
        
        assert result is None

    @patch('atlassian_cloud_backup.jira.client.make_authenticated_request')
    def test_fetch_last_task_id_exception(self, mock_request):
        """Test fetch_last_task_id when request raises exception."""
        mock_request.side_effect = Exception("Network error")
        
        result = self.client.fetch_last_task_id()
        
        assert result is None
