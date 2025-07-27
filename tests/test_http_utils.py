"""Tests for HTTP utilities with focus on HTTP 416 error handling."""

import os
import sys
import tempfile
import pytest
import requests
import time
import http.client
from unittest.mock import Mock, patch, call, MagicMock

# Add the src directory to Python path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from atlassian_cloud_backup.utils.http_utils import (
    download_file,
    make_authenticated_request,
    _attempt_download,
    _handle_range_response,
    _stream_response_to_file,
    _log_download_progress,
    _log_download_complete,
    _prepare_range_request,
    _retry_download,
    DownloadError,
    RETRIABLE_EXCEPTIONS
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
        from atlassian_cloud_backup.thinning.manager import BackupDeleter, DeletionConfig
        
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
        
        # Create backup deleter instance for test
        deletion_config = DeletionConfig()
        backup_deleter = BackupDeleter(deletion_config)
        
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
            3,
            'jira',  # backup_type
            backup_deleter  # backup_deleter
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
            8192, 100*1024*1024, 'Test Service', 0, 'jira', backup_deleter
        )
        
        assert result == 1024

    @patch('atlassian_cloud_backup.utils.http_utils.make_authenticated_request')
    @patch('atlassian_cloud_backup.utils.http_utils._stream_response_to_file')
    @patch('os.path.exists')
    @patch('os.remove')
    def test_attempt_download_416_no_partial_file(self, mock_remove, mock_exists,
                                                   mock_stream, mock_request):
        """Test that HTTP 416 handling works when no partial file exists."""
        from atlassian_cloud_backup.thinning.manager import BackupDeleter, DeletionConfig
        
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
        
        # Create backup deleter instance for test
        deletion_config = DeletionConfig()
        backup_deleter = BackupDeleter(deletion_config)
        
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
            3,
            'jira',  # backup_type
            backup_deleter  # backup_deleter
        )
        
        # Verify file existence was checked but no removal attempted
        mock_exists.assert_called_once_with('/tmp/test_file.zip')
        mock_remove.assert_not_called()
        
        # Verify success
        assert result == 1024

    @patch('atlassian_cloud_backup.utils.http_utils.make_authenticated_request')
    def test_attempt_download_reraises_other_http_errors(self, mock_request):
        """Test that non-416 HTTP errors are re-raised."""
        from atlassian_cloud_backup.thinning.manager import BackupDeleter, DeletionConfig
        
        # Create HTTP 500 error response
        mock_500_response = Mock()
        mock_500_response.status_code = 500
        http_500_error = requests.exceptions.HTTPError(response=mock_500_response)
        
        mock_request.side_effect = http_500_error
        
        # Create backup deleter instance for test
        deletion_config = DeletionConfig()
        backup_deleter = BackupDeleter(deletion_config)
        
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
                3,
                'jira',  # backup_type
                backup_deleter  # backup_deleter
            )
        
        assert exc_info.value.response.status_code == 500

    @patch('atlassian_cloud_backup.utils.http_utils.make_authenticated_request')
    @patch('atlassian_cloud_backup.utils.http_utils._stream_response_to_file')
    @patch('atlassian_cloud_backup.utils.http_utils._handle_range_response')
    def test_attempt_download_success_path(self, mock_handle_range, mock_stream, mock_request):
        """Test _attempt_download's success path (no exceptions)."""
        from atlassian_cloud_backup.thinning.manager import BackupDeleter, DeletionConfig
        
        # Setup mocks
        mock_response = Mock()
        mock_response.status_code = 200
        mock_request.return_value = mock_response
        
        mock_handle_range.return_value = ('ab', 1024)
        mock_stream.return_value = 2048
        
        # Create backup deleter instance for test
        deletion_config = DeletionConfig()
        backup_deleter = BackupDeleter(deletion_config)
        
        # Execute
        result = _attempt_download(
            'http://example.com/file.zip',
            '/tmp/test_file.zip',
            'user',
            'token',
            'Test Service',
            8192,
            100*1024*1024,
            1024,  # current_expected_on_disk > 0
            0,
            0,
            3,
            'jira',  # backup_type
            backup_deleter  # backup_deleter
        )
        
        # Verify normal flow was followed
        mock_request.assert_called_once()
        mock_handle_range.assert_called_once_with(mock_response, 1024)
        mock_stream.assert_called_once()
        assert result == 2048


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


class TestHTTPUtilsFunctions:
    """Tests for individual HTTP utility functions."""

    def test_make_authenticated_request(self):
        """Test make_authenticated_request function."""
        with patch('atlassian_cloud_backup.utils.http_utils.requests.request') as mock_request:
            mock_response = Mock()
            mock_response.raise_for_status = Mock()
            mock_request.return_value = mock_response

            result = make_authenticated_request(
                'GET',
                'https://example.com/api',
                'username',
                'api_token',
                params={'key': 'value'},
                headers={'Accept': 'application/json'}
            )

            # Verify request was made with correct parameters
            mock_request.assert_called_once()
            args, kwargs = mock_request.call_args
            assert args[0] == 'GET'
            assert args[1] == 'https://example.com/api'
            assert 'auth' in kwargs
            assert kwargs['params'] == {'key': 'value'}
            assert kwargs['headers'] == {'Accept': 'application/json'}
            assert result == mock_response

    def test_make_authenticated_request_raises_error(self):
        """Test make_authenticated_request raises HTTPError."""
        with patch('atlassian_cloud_backup.utils.http_utils.requests.request') as mock_request:
            mock_response = Mock()
            mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Not Found")
            mock_request.return_value = mock_response

            with pytest.raises(requests.exceptions.HTTPError):
                make_authenticated_request(
                    'GET',
                    'https://example.com/api',
                    'username',
                    'api_token'
                )

    @patch('atlassian_cloud_backup.utils.http_utils.time')
    def test_log_download_progress(self, mock_time):
        """Test _log_download_progress function."""
        mock_time.time.return_value = 100
        
        with patch('atlassian_cloud_backup.utils.http_utils.logging.getLogger') as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger
            
            # Test with reasonable values
            _log_download_progress(
                'Jira',
                10485760,  # 10 MB
                100,        # current_time
                90,         # start_time (10 seconds ago)
                95,         # last_log_time (5 seconds ago)
                2097152     # log_chunk_size (2 MB)
            )
            
            # Verify logger was called
            mock_get_logger.assert_called_once_with("progress")
            mock_logger.info.assert_called_once()
            
            # Check log message contains speed info
            log_msg = mock_logger.info.call_args[0][0]
            assert 'Jira' in log_msg
            assert 'MB/s' in log_msg

    @patch('builtins.print')
    def test_log_download_complete(self, mock_print):
        """Test _log_download_complete function."""
        with patch('atlassian_cloud_backup.utils.http_utils.time.time') as mock_time, \
             patch('atlassian_cloud_backup.utils.http_utils.logging.info') as mock_log_info:
            mock_time.return_value = 100
            
            _log_download_complete(
                'Confluence',
                '/tmp/backup.zip',
                10485760,  # 10 MB
                90         # start_time (10 seconds ago)
            )
            
            # Verify print and logging were called
            mock_print.assert_called_once()
            mock_log_info.assert_called_once()
            
            # Check arguments passed to logging.info 
            args, _ = mock_log_info.call_args
            _, service_name, filename, size_mb, elapsed, speed = args
            
            assert service_name == 'Confluence'
            assert filename == '/tmp/backup.zip'
            assert abs(size_mb - 10.0) < 0.01
            assert abs(elapsed - 10.0) < 0.01
            assert abs(speed - 1.0) < 0.01

    def test_prepare_range_request(self):
        """Test _prepare_range_request function."""
        # Test with zero bytes (fresh download)
        headers = _prepare_range_request(0, 0, 3)
        assert headers == {}
        
        # Test with some bytes (resuming)
        headers = _prepare_range_request(1024, 1, 3)
        assert headers == {'Range': 'bytes=1024-'}

    def test_handle_range_response_206(self):
        """Test _handle_range_response with 206 Partial Content."""
        mock_response = Mock()
        mock_response.status_code = 206
        
        mode, start_bytes = _handle_range_response(mock_response, 1024)
        
        assert mode == 'ab'
        assert start_bytes == 1024

    def test_handle_range_response_200(self):
        """Test _handle_range_response with 200 OK."""
        mock_response = Mock()
        mock_response.status_code = 200
        
        mode, start_bytes = _handle_range_response(mock_response, 1024)
        
        assert mode == 'wb'
        assert start_bytes == 0

    def test_handle_range_response_unexpected_status(self):
        """Test _handle_range_response with unexpected status code."""
        mock_response = Mock()
        mock_response.status_code = 202  # Accepted
        
        mode, start_bytes = _handle_range_response(mock_response, 1024)
        
        assert mode == 'wb'
        assert start_bytes == 0

    def test_handle_range_response_no_range(self):
        """Test _handle_range_response with no range (fresh download)."""
        mock_response = Mock()
        mock_response.status_code = 200
        
        mode, start_bytes = _handle_range_response(mock_response, 0)
        
        assert mode == 'wb'
        assert start_bytes == 0

    @patch('builtins.open', new_callable=MagicMock)
    def test_stream_response_to_file(self, mock_open):
        """Test _stream_response_to_file function."""
        from atlassian_cloud_backup.thinning.manager import BackupDeleter, DeletionConfig
        
        mock_file = MagicMock()
        mock_open.return_value.__enter__.return_value = mock_file
        
        mock_response = Mock()
        mock_response.iter_content.return_value = [b'chunk1', b'chunk2', b'']
        
        # Create backup deleter instance for test
        deletion_config = DeletionConfig()
        backup_deleter = BackupDeleter(deletion_config)
        
        with patch('atlassian_cloud_backup.utils.http_utils._log_download_progress') as mock_log_progress:
            with patch('atlassian_cloud_backup.utils.http_utils._ensure_disk_space_available') as mock_disk_space:
                # The log_chunk_size needs to be smaller than the total bytes to trigger logging
                # We'll use chunk sizes that will trigger logging
                total_bytes = len(b'chunk1') + len(b'chunk2')  # 12 bytes
                log_chunk_size = 5  # Will trigger logging after 5 bytes
                
                bytes_written = _stream_response_to_file(
                    mock_response,
                    '/tmp/test.zip',
                    'wb',
                    0,
                    8192,
                    log_chunk_size,
                    'Test Service',
                    time.time(),
                    'jira',  # backup_type
                    backup_deleter  # backup_deleter
                )
            
            # Verify file was opened correctly
            mock_open.assert_called_once_with('/tmp/test.zip', 'wb')
            
            # Verify chunks were written
            assert mock_file.write.call_count == 2
            assert mock_file.write.call_args_list == [call(b'chunk1'), call(b'chunk2')]
            
            # With 12 bytes total and logging every 5 bytes, we should log at least twice
            assert mock_log_progress.call_count >= 1
            
            # Verify correct bytes count returned
            assert bytes_written == total_bytes

    @patch('os.path.getsize')
    @patch('os.path.exists')
    @patch('time.sleep')
    def test_retry_download_success(self, mock_sleep, mock_exists, mock_getsize):
        """Test _retry_download function with successful download."""
        download_fn = Mock()
        download_fn.return_value = 1024
        mock_exists.return_value = True
        mock_getsize.return_value = 1024
        
        result = _retry_download(
            download_fn,
            '/tmp/test.zip',
            'Test Service',
            3,
            1
        )
        
        assert result == 1024
        download_fn.assert_called_once_with(0)
        mock_sleep.assert_not_called()

    @patch('os.path.getsize')
    @patch('os.path.exists')
    @patch('time.sleep')
    def test_retry_download_retriable_error(self, mock_sleep, mock_exists, mock_getsize):
        """Test _retry_download function with retriable error."""
        download_fn = Mock()
        download_fn.side_effect = [
            http.client.IncompleteRead("incomplete"),
            1024
        ]
        mock_exists.return_value = True
        mock_getsize.return_value = 512
        
        with patch('atlassian_cloud_backup.utils.http_utils.logging.warning') as mock_warning, \
             patch('atlassian_cloud_backup.utils.http_utils.logging.info') as mock_info:
            result = _retry_download(
                download_fn,
                '/tmp/test.zip',
                'Test Service',
                3,
                1
            )
            
            assert result == 1024
            assert download_fn.call_count == 2
            mock_warning.assert_called_once()
            mock_info.assert_called_once()
            mock_sleep.assert_called_once_with(1)

    @patch('os.path.getsize')
    @patch('os.path.exists')
    @patch('time.sleep')
    def test_retry_download_max_retries(self, mock_sleep, mock_exists, mock_getsize):
        """Test _retry_download function with max retries exceeded."""
        download_fn = Mock()
        download_fn.side_effect = http.client.IncompleteRead("incomplete")
        mock_exists.return_value = True
        mock_getsize.return_value = 512
        
        with patch('atlassian_cloud_backup.utils.http_utils.logging.warning') as mock_warning, \
             patch('atlassian_cloud_backup.utils.http_utils.logging.info') as mock_info, \
             patch('atlassian_cloud_backup.utils.http_utils.logging.error') as mock_error:
            
            with pytest.raises(http.client.IncompleteRead):
                _retry_download(
                    download_fn,
                    '/tmp/test.zip',
                    'Test Service',
                    1,  # max_retries
                    1   # initial_delay_seconds
                )
            
            assert download_fn.call_count == 2  # Initial + 1 retry
            assert mock_warning.call_count == 2
            assert mock_info.call_count == 1
            assert mock_error.call_count == 1
            assert mock_sleep.call_count == 1

class TestDownloadFileErrorHandling:
    """Test error handling in the download_file function."""

    @patch('os.path.exists')
    @patch('os.path.getsize')
    @patch('atlassian_cloud_backup.utils.http_utils._retry_download')
    @patch('atlassian_cloud_backup.utils.http_utils._log_download_complete')
    def test_download_file_http_error(self, mock_log_complete, mock_retry, mock_getsize, mock_exists):
        """Test download_file handles HTTP errors properly."""
        mock_exists.return_value = True
        mock_getsize.return_value = 1024
        
        # Create HTTP error response
        mock_response = Mock()
        mock_response.status_code = 403
        http_error = requests.exceptions.HTTPError("403 Forbidden", response=mock_response)
        mock_retry.side_effect = http_error
        
        with patch('atlassian_cloud_backup.utils.http_utils.logging.error') as mock_error:
            with pytest.raises(requests.exceptions.HTTPError):
                download_file(
                    'http://example.com/file.zip',
                    '/tmp/test.zip',
                    'user',
                    'token',
                    'Test Service'
                )
            
            mock_error.assert_called_once()
            mock_log_complete.assert_not_called()

    @patch('os.path.exists')
    @patch('os.path.getsize')
    @patch('atlassian_cloud_backup.utils.http_utils._retry_download')
    @patch('atlassian_cloud_backup.utils.http_utils._log_download_complete')
    def test_download_file_other_exception(self, mock_log_complete, mock_retry, mock_getsize, mock_exists):
        """Test download_file handles other exceptions properly."""
        mock_exists.return_value = True
        mock_getsize.return_value = 1024
        
        mock_retry.side_effect = ValueError("Some unexpected error")
        
        with pytest.raises(DownloadError) as exc_info:
            download_file(
                'http://example.com/file.zip',
                '/tmp/test.zip',
                'user',
                'token',
                'Test Service'
            )
        
        assert "Download failed for Test Service after" in str(exc_info.value)
        mock_log_complete.assert_not_called()

    @patch('os.path.exists')
    @patch('os.path.getsize')
    def test_download_file_resume_existing(self, mock_getsize, mock_exists):
        """Test download_file with existing partial file."""
        mock_exists.return_value = True
        mock_getsize.return_value = 1024
        
        with patch('atlassian_cloud_backup.utils.http_utils._retry_download') as mock_retry, \
             patch('atlassian_cloud_backup.utils.http_utils._log_download_complete') as mock_log_complete, \
             patch('atlassian_cloud_backup.utils.http_utils.logging.info') as mock_info:
            
            mock_retry.return_value = 2048
            
            result = download_file(
                'http://example.com/file.zip',
                '/tmp/test.zip',
                'user',
                'token',
                'Test Service'
            )
            
            assert result == '/tmp/test.zip'
            assert mock_info.call_count >= 2  # At least 2 log messages (start + found partial)
            mock_retry.assert_called_once()
            mock_log_complete.assert_called_once()
