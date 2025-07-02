"""Extended tests for the Jira client."""

import os
import time
import pytest
from unittest.mock import patch, Mock, MagicMock
from requests.exceptions import HTTPError, RequestException
from datetime import datetime, timezone

from atlassian_cloud_backup.utils.http_utils import DownloadError

# We need to add these tests to the existing TestJiraClientExtended
# so let's import it from test_jira_client_extended.py
from test_jira_client_extended import TestJiraClientExtended

class TestJiraClientFinalCoverage(TestJiraClientExtended):
    """Additional tests to complete coverage for JiraClient."""

    def test_fetch_last_task_id_value_error(self):
        """Test handling of invalid (non-integer) response from lastTaskId."""
        with patch('atlassian_cloud_backup.jira.client.make_authenticated_request') as mock_request:
            # Mock response with non-integer value
            mock_response = Mock()
            mock_response.text = "not-an-integer"
            mock_request.return_value = mock_response
            
            # It seems the implementation logs the error and returns None instead of raising
            result = self.client.fetch_last_task_id()
            assert result is None

    def test_fetch_task_info_fallback(self):
        """Test fallback when jira.get fails in fetch_task_info."""
        # Mock the jira client to raise an exception
        self.client.jira.get = Mock(side_effect=Exception("API error"))
        
        # Mock the direct API call that's used as fallback
        with patch('atlassian_cloud_backup.jira.client.make_authenticated_request') as mock_request:
            mock_response = Mock()
            mock_response.json.return_value = {"status": "COMPLETE"}
            mock_request.return_value = mock_response
            
            # Execute the test
            result = self.client.fetch_task_info(12345)
            
            # Verify the result
            assert result == {"status": "COMPLETE"}
            # Verify the direct API was called
            mock_request.assert_called_once()

    def test_establish_session_cookies_no_cookies(self):
        """Test case when no cookies are obtained from lastTaskId endpoint."""
        mock_session = Mock()
        mock_session.cookies = []
        mock_session.get.return_value.text = "12345"
        
        with patch('atlassian_cloud_backup.jira.client.logging.warning') as mock_warning:
            self.client._establish_session_cookies(mock_session)
            mock_warning.assert_called_with('No cookies obtained from lastTaskId endpoint')

    def test_post_backup_request_no_response_exception(self):
        """Test handling of RequestException with no response attribute."""
        mock_session = Mock()
        exception = RequestException("Connection error")
        # Create an exception without response attribute
        mock_session.post.side_effect = exception
        
        with pytest.raises(HTTPError, match="Request failed:"):
            self.client._post_backup_request(mock_session, None)

    def test_handle_http_error_else_branch(self):
        """Test the else branch of _handle_http_error which re-raises the error."""
        # Create a mock HTTPError with a non-412 status code
        mock_response = Mock()
        mock_response.status_code = 404
        error = HTTPError("Not Found error", response=mock_response)
        
        # The method uses "raise" which requires an active exception context
        # So we need to actually raise the exception first
        try:
            raise error
        except HTTPError:
            with pytest.raises(HTTPError):
                self.client._handle_http_error(error, 12345)

    def test_determine_timeout_with_parameter(self):
        """Test _determine_timeout when a parameter is provided."""
        # Override the instance timeout
        self.client.backup_timeout_minutes = 600
        
        # Call with a specific timeout
        result = self.client._determine_timeout(300)
        
        # Verify it returns the provided timeout, not the instance one
        assert result == 300

    def test_download_existing_backup_remove_file_exception(self):
        """Test exception handling when removing invalid zip file."""
        with patch('atlassian_cloud_backup.jira.client.JiraClient.download_backup_file') as mock_download, \
             patch('atlassian_cloud_backup.jira.client.JiraClient._verify_zip_file') as mock_verify, \
             patch('atlassian_cloud_backup.jira.client.JiraClient.fetch_task_info') as mock_fetch_info, \
             patch('atlassian_cloud_backup.jira.client.os.remove') as mock_remove, \
             patch('atlassian_cloud_backup.jira.client.logging.warning') as mock_warning:
            
            # Set up mocks
            mock_fetch_info.return_value = {'submitted': 1609459200000}  # 2021-01-01
            mock_download.return_value = "/tmp/backups/jira_backup.zip"
            mock_verify.return_value = False
            mock_remove.side_effect = OSError("Permission denied")
            
            # Execute the test
            result = self.client._download_existing_backup(12345, datetime.now(timezone.utc))
            
            # Verify the result
            assert result == {}
            mock_warning.assert_called_once()
            mock_remove.assert_called_once_with("/tmp/backups/jira_backup.zip")
