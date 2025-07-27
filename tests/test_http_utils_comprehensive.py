"""
Additional comprehensive tests for HTTP utilities with edge cases and error handling.

This module adds more sophisticated tests to ensure robust coverage of
all code paths, especially error handling and edge cases.
"""

import pytest
import tempfile
import os
import sys
import time
from pathlib import Path
from unittest.mock import Mock, patch, call, MagicMock
import requests

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
    _detect_backup_type_from_filename,
    _ensure_disk_space_available,
    DownloadError,
    RETRIABLE_EXCEPTIONS,
    MAX_DOWNLOAD_RETRIES,
    INITIAL_RETRY_DELAY_SECONDS,
    RETRY_DELAY_MULTIPLIER
)


class TestHTTPUtilsEdgeCases:
    """Test edge cases and error handling in HTTP utilities."""
    
    def test_make_authenticated_request_success(self):
        """Test successful authenticated request."""
        with patch('requests.request') as mock_request:
            mock_response = Mock()
            mock_response.raise_for_status.return_value = None
            mock_request.return_value = mock_response
            
            result = make_authenticated_request(
                'GET', 'http://example.com', 'user', 'token'
            )
            
            assert result == mock_response
            mock_request.assert_called_once()
            mock_response.raise_for_status.assert_called_once()
    
    def test_make_authenticated_request_with_kwargs(self):
        """Test authenticated request with additional kwargs."""
        with patch('requests.request') as mock_request:
            mock_response = Mock()
            mock_response.raise_for_status.return_value = None
            mock_request.return_value = mock_response
            
            result = make_authenticated_request(
                'POST', 'http://example.com', 'user', 'token',
                timeout=30, headers={'Custom': 'Header'}
            )
            
            assert result == mock_response
            # Verify auth was added and kwargs passed through
            call_args = mock_request.call_args
            assert 'auth' in call_args[1]
            assert call_args[1]['timeout'] == 30
            assert call_args[1]['headers']['Custom'] == 'Header'
    
    def test_make_authenticated_request_raises_error(self):
        """Test authenticated request that raises HTTP error."""
        with patch('requests.request') as mock_request:
            mock_response = Mock()
            mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Not Found")
            mock_request.return_value = mock_response
            
            with pytest.raises(requests.exceptions.HTTPError):
                make_authenticated_request('GET', 'http://example.com', 'user', 'token')


class TestDownloadFileEdgeCases:
    """Test edge cases in download_file function."""
    
    @patch('atlassian_cloud_backup.utils.http_utils._retry_download')
    @patch('atlassian_cloud_backup.utils.http_utils._log_download_complete')
    @patch('os.path.exists')
    @patch('os.path.getsize')
    def test_download_file_no_existing_file(self, mock_getsize, mock_exists, 
                                           mock_log_complete, mock_retry):
        """Test download when no existing file exists."""
        mock_exists.return_value = False
        mock_retry.return_value = 1024
        
        result = download_file(
            'http://example.com/file.zip', '/tmp/test.zip',
            'user', 'token', 'test-service'
        )
        
        assert result == '/tmp/test.zip'
        mock_retry.assert_called_once()
        mock_log_complete.assert_called_once_with('test-service', '/tmp/test.zip', 1024, pytest.approx(time.time(), abs=1))
    
    @patch('atlassian_cloud_backup.utils.http_utils._retry_download')
    @patch('os.path.exists')
    @patch('os.path.getsize')
    def test_download_file_with_existing_partial_file(self, mock_getsize, mock_exists, mock_retry):
        """Test download with existing partial file."""
        mock_exists.return_value = True
        mock_getsize.return_value = 512  # Partial file exists
        mock_retry.return_value = 1024
        
        result = download_file(
            'http://example.com/file.zip', '/tmp/test.zip',
            'user', 'token', 'test-service'
        )
        
        assert result == '/tmp/test.zip'
        mock_retry.assert_called_once()
    
    @patch('atlassian_cloud_backup.utils.http_utils._retry_download')
    def test_download_file_http_error_propagation(self, mock_retry):
        """Test that HTTP errors are propagated correctly."""
        http_error = requests.exceptions.HTTPError("500 Server Error")
        http_error.response = Mock()
        http_error.response.status_code = 500
        mock_retry.side_effect = http_error
        
        with pytest.raises(requests.exceptions.HTTPError):
            download_file(
                'http://example.com/file.zip', '/tmp/test.zip',
                'user', 'token', 'test-service'
            )
    
    @patch('atlassian_cloud_backup.utils.http_utils._retry_download')
    def test_download_file_other_exception_wrapped(self, mock_retry):
        """Test that other exceptions are wrapped in DownloadError."""
        mock_retry.side_effect = ConnectionError("Network error")
        
        with pytest.raises(DownloadError) as exc_info:
            download_file(
                'http://example.com/file.zip', '/tmp/test.zip',
                'user', 'token', 'test-service'
            )
        
        assert "Network error" in str(exc_info.value)
        assert f"after {MAX_DOWNLOAD_RETRIES + 1} attempts" in str(exc_info.value)


class TestRetryDownloadEdgeCases:
    """Test edge cases in retry download functionality."""
    
    @patch('time.sleep')
    @patch('os.path.getsize')
    @patch('os.path.exists')
    def test_retry_download_all_attempts_fail(self, mock_exists, mock_getsize, mock_sleep):
        """Test retry when all attempts fail."""
        mock_exists.return_value = True
        mock_getsize.return_value = 100
        
        def failing_download(attempt):
            raise requests.exceptions.ConnectionError("Network error")
        
        with pytest.raises(requests.exceptions.ConnectionError):
            _retry_download(failing_download, '/tmp/test.zip', 'test-service', 2, 1)
        
        # Should sleep between retries but not after the last failed attempt
        assert mock_sleep.call_count == 2  # 2 retries means 2 sleep calls
        
        # Verify exponential backoff
        sleep_calls = [call.args[0] for call in mock_sleep.call_args_list]
        assert sleep_calls[0] == 1  # First retry delay
        assert sleep_calls[1] == 2  # Second retry delay (multiplied by 2)
    
    def test_retry_download_success_on_first_attempt(self):
        """Test retry when first attempt succeeds."""
        def successful_download(attempt):
            return 1024
        
        result = _retry_download(successful_download, '/tmp/test.zip', 'test-service', 3, 1)
        assert result == 1024
    
    @patch('time.sleep')
    @patch('os.path.getsize')
    @patch('os.path.exists')
    def test_retry_download_success_after_failures(self, mock_exists, mock_getsize, mock_sleep):
        """Test retry when later attempt succeeds."""
        mock_exists.return_value = True
        mock_getsize.return_value = 100
        
        call_count = 0
        def sometimes_failing_download(attempt):
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise requests.exceptions.ConnectionError("Network error")
            return 1024
        
        result = _retry_download(sometimes_failing_download, '/tmp/test.zip', 'test-service', 5, 1)
        assert result == 1024
        assert mock_sleep.call_count == 2  # Two failed attempts before success


class TestPrepareRangeRequestEdgeCases:
    """Test edge cases in range request preparation."""
    
    def test_prepare_range_request_zero_bytes(self):
        """Test range request with zero bytes on disk."""
        headers = _prepare_range_request(0, 0, 5)
        assert headers == {}
    
    def test_prepare_range_request_with_bytes(self):
        """Test range request with existing bytes."""
        headers = _prepare_range_request(1024, 1, 5)
        assert headers == {'Range': 'bytes=1024-'}
    
    def test_prepare_range_request_large_values(self):
        """Test range request with large byte values."""
        headers = _prepare_range_request(1_000_000_000, 10, 15)
        assert headers == {'Range': 'bytes=1000000000-'}


class TestLogFunctionsEdgeCases:
    """Test edge cases in logging functions."""
    
    @patch('logging.getLogger')
    def test_log_download_progress_zero_elapsed(self, mock_get_logger):
        """Test log progress with zero elapsed time."""
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        
        current_time = 1000.0
        start_time = 1000.0  # Same as current time
        last_log_time = 999.0
        
        _log_download_progress(
            'test-service', 1024*1024, current_time, start_time, last_log_time, 1024*1024
        )
        
        # Should handle zero elapsed time gracefully
        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args[0]
        assert "0.00 MB/s" in call_args[0] or "inf" not in call_args[0]  # No infinite values
    
    @patch('builtins.print')
    @patch('logging.info')
    def test_log_download_complete_zero_elapsed(self, mock_log_info, mock_print):
        """Test log complete with zero elapsed time."""
        start_time = time.time()
        end_time = start_time  # Same time
        
        _log_download_complete('test-service', '/tmp/test.zip', 1024*1024, start_time)
        
        mock_print.assert_called_once()  # Should print newline
        mock_log_info.assert_called_once()
        
        # Check that we don't get infinite speeds
        call_args = mock_log_info.call_args[0]
        assert "inf" not in str(call_args)


class TestHandleRangeResponseEdgeCases:
    """Test edge cases in range response handling."""
    
    def test_handle_range_response_206_with_no_existing_bytes(self):
        """Test 206 response when no bytes expected on disk."""
        mock_response = Mock()
        mock_response.status_code = 206
        
        file_open_mode, start_bytes = _handle_range_response(mock_response, 0)
        
        # Should treat as fresh download when no existing bytes expected
        assert file_open_mode == 'wb'
        assert start_bytes == 0
    
    def test_handle_range_response_unknown_status_with_range(self):
        """Test unknown status code with range request."""
        mock_response = Mock()
        mock_response.status_code = 418  # I'm a teapot
        
        file_open_mode, start_bytes = _handle_range_response(mock_response, 1000)
        
        assert file_open_mode == 'wb'
        assert start_bytes == 0


class TestDetectBackupTypeEdgeCases:
    """Test edge cases in backup type detection."""
    
    def test_detect_backup_type_case_insensitive(self):
        """Test that detection is case insensitive."""
        test_cases = [
            ("JIRA-backup.zip", "jira"),
            ("jira-BACKUP.ZIP", "jira"),
            ("JiRa-backup.zip", "jira"),
            ("CONFLUENCE-backup.zip", "confluence"),
            ("confluence-BACKUP.ZIP", "confluence"),
            ("CoNfLuEnCe-backup.zip", "confluence"),
        ]
        
        for filename, expected_type in test_cases:
            assert _detect_backup_type_from_filename(filename) == expected_type
    
    def test_detect_backup_type_with_paths(self):
        """Test detection with full file paths."""
        assert _detect_backup_type_from_filename("/long/path/to/jira-backup.zip") == "jira"
        assert _detect_backup_type_from_filename("C:\\Windows\\Path\\confluence-backup.zip") == "confluence"
        assert _detect_backup_type_from_filename("/tmp/unknown-file.zip") == "jira"  # Default
    
    def test_detect_backup_type_edge_cases(self):
        """Test detection with edge case filenames."""
        test_cases = [
            ("jira", "jira"),  # No extension
            ("confluence", "confluence"),  # No extension
            ("", "jira"),  # Empty string
            ("backup.zip", "jira"),  # No service type
            ("jira-confluence-backup.zip", "jira"),  # Both keywords (jira comes first in check)
        ]
        
        for filename, expected_type in test_cases:
            assert _detect_backup_type_from_filename(filename) == expected_type


class TestConstantsAndExceptions:
    """Test constants and exception classes."""
    
    def test_download_error_exception(self):
        """Test DownloadError exception."""
        error = DownloadError("Test error message")
        assert str(error) == "Test error message"
        assert isinstance(error, Exception)
    
    def test_retriable_exceptions_tuple(self):
        """Test that RETRIABLE_EXCEPTIONS contains expected exception types."""
        import http.client
        import requests.exceptions
        
        expected_exceptions = (
            http.client.IncompleteRead,
            requests.exceptions.ChunkedEncodingError,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        )
        
        assert RETRIABLE_EXCEPTIONS == expected_exceptions
    
    def test_retry_constants(self):
        """Test retry configuration constants."""
        assert MAX_DOWNLOAD_RETRIES == 15
        assert INITIAL_RETRY_DELAY_SECONDS == 1
        assert RETRY_DELAY_MULTIPLIER == 2


class TestStreamResponseEdgeCases:
    """Test edge cases in stream response functionality."""
    
    def test_stream_response_empty_chunks(self):
        """Test streaming with empty chunks in response."""
        from atlassian_cloud_backup.thinning.manager import BackupDeleter, DeletionConfig
        
        with tempfile.TemporaryDirectory() as temp_dir:
            filename = os.path.join(temp_dir, "test.zip")

            # Mock response with empty chunks
            mock_response = Mock()
            mock_response.iter_content.return_value = [b"data", b"", b"more", b"", b"data"]

            # Create backup deleter instance for test
            deletion_config = DeletionConfig()
            backup_deleter = BackupDeleter(deletion_config)

            # Mock disk space functions
            with patch('atlassian_cloud_backup.utils.http_utils._ensure_disk_space_available'):
                bytes_written = _stream_response_to_file(
                    mock_response, filename, 'wb', 0,
                    chunk_size=1024, log_chunk_size=1024*1024,
                    service_name="test-service", overall_start_time=time.time(),
                    backup_type="jira", backup_deleter=backup_deleter
                )            # Should only count non-empty chunks
            assert bytes_written == len(b"datamore" + b"data")
            
            # Verify file content
            with open(filename, 'rb') as f:
                assert f.read() == b"datamoredata"
    
    def test_stream_response_progress_logging_threshold(self):
        """Test that progress logging happens at correct thresholds."""
        from atlassian_cloud_backup.thinning.manager import BackupDeleter, DeletionConfig
        
        with tempfile.TemporaryDirectory() as temp_dir:
            filename = os.path.join(temp_dir, "test.zip")

            # Create chunks that will cross the logging threshold
            chunk_size = 1024
            log_chunk_size = 2048  # Small threshold for testing
            chunks = [b"x" * chunk_size] * 5  # 5KB total

            mock_response = Mock()
            mock_response.iter_content.return_value = chunks

            # Create backup deleter instance for test
            deletion_config = DeletionConfig()
            backup_deleter = BackupDeleter(deletion_config)

            with patch('atlassian_cloud_backup.utils.http_utils._ensure_disk_space_available'), \
                 patch('atlassian_cloud_backup.utils.http_utils._log_download_progress') as mock_log:

                bytes_written = _stream_response_to_file(
                    mock_response, filename, 'wb', 0,
                    chunk_size=chunk_size, log_chunk_size=log_chunk_size,
                    service_name="test-service", overall_start_time=time.time(),
                    backup_type="jira", backup_deleter=backup_deleter
                )
            
            assert bytes_written == 5 * chunk_size
            
            # Should log progress when crossing thresholds
            # With 2KB threshold and 1KB chunks: should log at 2KB and 4KB
            assert mock_log.call_count >= 2
