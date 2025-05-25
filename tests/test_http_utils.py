"""Tests for HTTP utilities with focus on HTTP 416 error handling."""

import os
import sys
import tempfile
import pytest
import requests
from unittest.mock import Mock, patch, call

# Add the src directory to Python path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from atlassian_cloud_backup.utils.http_utils import (
    download_file,
    _attempt_download,
    _handle_range_response,
    DownloadError
)


class TestHTTP416ErrorHandling:
    """Test HTTP 416 Range Not Satisfiable error handling."""

    def test_handle_range_response_with_416(self):
        """Test that _handle_range_response handles 416 status correctly."""
        mock_response = Mock()
        mock_response.status_code = 416
        
        file_open_mode, start_bytes = _handle_range_response(mock_response, 1000)
        
        assert file_open_mode == 'wb'
        assert start_bytes == 0

    @patch('atlassian_cloud_backup.utils.http_utils.make_authenticated_request')
    @patch('atlassian_cloud_backup.utils.http_utils._stream_response_to_file')
    @patch('os.path.exists')
    @patch('os.remove')
    def test_attempt_download_handles_416_error(self, mock_remove, mock_exists, 
                                                mock_stream, mock_request):
        """Test that _attempt_download handles HTTP 416 errors by deleting partial file."""
        # Setup mocks
        mock_exists.return_value = True
        mock_stream.return_value = 1024
        
        # Create HTTP 416 error response
        mock_416_response = Mock()
        mock_416_response.status_code = 416
        http_416_error = requests.exceptions.HTTPError(response=mock_416_response)
        
        # Create successful retry response
        mock_success_response = Mock()
        mock_success_response.status_code = 200
        
        # First call raises 416, second call succeeds
        mock_request.side_effect = [http_416_error, mock_success_response]
        
        # Execute
        result = _attempt_download(
            'http://example.com/file.zip',
            '/tmp/test_file.zip',
            'user',
            'token',
            'Test Service',
            8192,
            100*1024*1024,
            1000,  # current_expected_on_disk > 0
            0,
            0,
            3
        )
        
        # Verify partial file was deleted
        mock_exists.assert_called_once_with('/tmp/test_file.zip')
        mock_remove.assert_called_once_with('/tmp/test_file.zip')
        
        # Verify two requests were made
        assert mock_request.call_count == 2
        
        # First call with range headers
        first_call = mock_request.call_args_list[0]
        assert 'headers' in first_call[1]
        assert 'Range' in first_call[1]['headers']
        
        # Second call without range headers
        second_call = mock_request.call_args_list[1]
        assert 'headers' not in second_call[1] or 'Range' not in second_call[1].get('headers', {})
        
        # Verify stream was called with correct parameters
        mock_stream.assert_called_once_with(
            mock_success_response, '/tmp/test_file.zip', 'wb', 0,
            8192, 100*1024*1024, 'Test Service', 0
        )
        
        assert result == 1024

    @patch('atlassian_cloud_backup.utils.http_utils.make_authenticated_request')
    @patch('atlassian_cloud_backup.utils.http_utils._stream_response_to_file')
    @patch('os.path.exists')
    @patch('os.remove')
    def test_attempt_download_416_no_partial_file(self, mock_remove, mock_exists,
                                                   mock_stream, mock_request):
        """Test that HTTP 416 handling works when no partial file exists."""
        # Setup mocks
        mock_exists.return_value = False
        mock_stream.return_value = 1024
        
        # Create HTTP 416 error response
        mock_416_response = Mock()
        mock_416_response.status_code = 416
        http_416_error = requests.exceptions.HTTPError(response=mock_416_response)
        
        # Create successful retry response
        mock_success_response = Mock()
        mock_success_response.status_code = 200
        
        # First call raises 416, second call succeeds
        mock_request.side_effect = [http_416_error, mock_success_response]
        
        # Execute
        result = _attempt_download(
            'http://example.com/file.zip',
            '/tmp/test_file.zip',
            'user',
            'token',
            'Test Service',
            8192,
            100*1024*1024,
            1000,  # current_expected_on_disk > 0
            0,
            0,
            3
        )
        
        # Verify file existence was checked but no removal attempted
        mock_exists.assert_called_once_with('/tmp/test_file.zip')
        mock_remove.assert_not_called()
        
        # Verify success
        assert result == 1024

    @patch('atlassian_cloud_backup.utils.http_utils.make_authenticated_request')
    def test_attempt_download_reraises_other_http_errors(self, mock_request):
        """Test that non-416 HTTP errors are re-raised."""
        # Create HTTP 500 error response
        mock_500_response = Mock()
        mock_500_response.status_code = 500
        http_500_error = requests.exceptions.HTTPError(response=mock_500_response)
        
        mock_request.side_effect = http_500_error
        
        # Execute and verify exception is re-raised
        with pytest.raises(requests.exceptions.HTTPError) as exc_info:
            _attempt_download(
                'http://example.com/file.zip',
                '/tmp/test_file.zip',
                'user',
                'token',
                'Test Service',
                8192,
                100*1024*1024,
                1000,
                0,
                0,
                3
            )
        
        assert exc_info.value.response.status_code == 500


class TestDownloadFileIntegration:
    """Integration tests for download_file function."""

    @patch('atlassian_cloud_backup.utils.http_utils._retry_download')
    @patch('atlassian_cloud_backup.utils.http_utils._log_download_complete')
    def test_download_file_success(self, mock_log_complete, mock_retry):
        """Test successful download file execution."""
        mock_retry.return_value = 1024
        
        result = download_file(
            'http://example.com/file.zip',
            '/tmp/test_file.zip',
            'user',
            'token',
            'Test Service'
        )
        
        assert result == '/tmp/test_file.zip'
        mock_retry.assert_called_once()
        mock_log_complete.assert_called_once()


class TestHTTP416Integration:
    """Integration test for HTTP 416 error handling in the complete download workflow."""

    @patch('atlassian_cloud_backup.utils.http_utils.make_authenticated_request')
    @patch('atlassian_cloud_backup.utils.http_utils._log_download_complete')
    @patch('os.path.exists')
    @patch('os.remove')
    @patch('os.path.getsize')
    def test_download_file_handles_416_and_recovers(self, mock_getsize, mock_remove, 
                                                    mock_exists, mock_log_complete, 
                                                    mock_request):
        """Test that download_file can recover from HTTP 416 errors during partial resumption."""
        # Setup: simulate partial file exists
        mock_exists.return_value = True
        mock_getsize.side_effect = [1000, 0]  # Initial size 1000, then 0 after deletion
        
        # Create HTTP 416 error response
        mock_416_response = Mock()
        mock_416_response.status_code = 416
        http_416_error = requests.exceptions.HTTPError(response=mock_416_response)
        
        # Create successful response that streams 2048 bytes
        mock_success_response = Mock()
        mock_success_response.status_code = 200
        mock_success_response.iter_content.return_value = [b'x' * 1024, b'y' * 1024]
        
        # First call (with range) raises 416, second call (without range) succeeds
        mock_request.side_effect = [http_416_error, mock_success_response]
        
        # Execute download
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_filename = temp_file.name
        
        try:
            result = download_file(
                'http://example.com/backup.zip',
                temp_filename,
                'user',
                'token',
                'Jira'
            )
            
            # Verify success
            assert result == temp_filename
            
            # Verify partial file was deleted when 416 occurred
            mock_remove.assert_called_once_with(temp_filename)
            
            # Verify two requests were made
            assert mock_request.call_count == 2
            
            # Verify completion was logged
            mock_log_complete.assert_called_once()
            
        finally:
            # Clean up temp file
            if os.path.exists(temp_filename):
                os.remove(temp_filename)
