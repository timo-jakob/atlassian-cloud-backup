"""Final coverage tests for the Jira client."""

import os
import time
import pytest
from unittest.mock import patch, Mock, MagicMock
from requests.exceptions import HTTPError, RequestException
from datetime import datetime, timezone

# Import from existing test file
from test_jira_client_extended import TestJiraClientExtended

class TestJiraClientFinalCoverage2(TestJiraClientExtended):
    """Final tests to maximize coverage for JiraClient."""

    def test_post_backup_request_exception_with_response(self):
        """Test exception handling with a response object."""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.text = "Bad request"
        exception = RequestException("Request failed", response=mock_response)
        mock_session.post.side_effect = exception
        
        with pytest.raises(HTTPError, match="HTTP 400:"):
            self.client._post_backup_request(mock_session, None)

    def test_handle_http_error_server_id_is_none(self):
        """Test handling HTTP error when server task ID is None."""
        mock_response = Mock()
        mock_response.status_code = 412
        error = HTTPError("Frequency limit", response=mock_response)
        
        # Mock fetch_last_task_id to return None
        with patch.object(self.client, 'fetch_last_task_id', return_value=None):
            result = self.client._handle_http_error(error, 12345)
            
        assert result == {'action': 'no_server_backup'}

    def test_handle_http_error_local_task_id_newer(self):
        """Test handling HTTP error when local task ID is newer than server task ID."""
        mock_response = Mock()
        mock_response.status_code = 412
        error = HTTPError("Frequency limit", response=mock_response)
        
        # Mock fetch_last_task_id to return a smaller ID than local
        with patch.object(self.client, 'fetch_last_task_id', return_value=10000):
            result = self.client._handle_http_error(error, 12345)
            
        assert result == {'action': 'skipped', 'reason': 'local_current'}

    def test_handle_backup_trigger_error_other_status(self):
        """Test handling of non-500 status code errors."""
        mock_response = Mock()
        mock_response.status_code = 400
        error = HTTPError("Bad request", response=mock_response)
        
        try:
            raise error
        except HTTPError:
            with pytest.raises(HTTPError):
                self.client._handle_backup_trigger_error(error)

    def test_handle_new_backup_failure(self):
        """Test the failure branch in _handle_new_backup."""
        with patch.object(self.client, '_wait_and_download_backup', return_value={}):
            result = self.client._handle_new_backup(12345, datetime.now(timezone.utc))
            
        assert result == {}
