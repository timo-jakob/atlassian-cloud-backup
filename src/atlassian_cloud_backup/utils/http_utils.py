"""HTTP utilities for Atlassian Cloud Backup."""

import os
import time
import logging
import requests
import shutil
from pathlib import Path
from requests.auth import HTTPBasicAuth
import http.client # For IncompleteRead
import sys

from atlassian_cloud_backup.thinning.manager import BackupDeleter, DeletionConfig

class DownloadError(Exception):
    """Raised when a download fails after all retry attempts."""

# Exceptions considered retriable for download logic
RETRIABLE_EXCEPTIONS = (
    http.client.IncompleteRead,
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
)

# Download retry configuration
MAX_DOWNLOAD_RETRIES = 15
INITIAL_RETRY_DELAY_SECONDS = 1
RETRY_DELAY_MULTIPLIER = 2  # Exponential backoff multiplier (delay sequence: 1s, 2s, 4s, 8s, 16s, 32s, ... up to ~9 hours total)

def make_authenticated_request(method, url, username, api_token, **kwargs):
    """Make an authenticated HTTP request to Atlassian API.
    
    Args:
        method (str): HTTP method ('GET', 'POST', etc.)
        url (str): URL to request
        username (str): Username for authentication
        api_token (str): API token for authentication
        **kwargs: Additional arguments to pass to requests.request
        
    Returns:
        requests.Response: Response object
        
    Raises:
        requests.exceptions.HTTPError: If the HTTP request returns an error
    """
    auth = HTTPBasicAuth(username, api_token)
    response = requests.request(method, url, auth=auth, **kwargs)
    response.raise_for_status()
    return response

def download_file(url, filename, username, api_token, service_name, chunk_size=8192, log_chunk_size=100*1024*1024, deletion_strategy="oldest_first"):
    """Download a file with progress tracking and retry/resume capabilities.
    
    Args:
        url (str): URL to download from
        filename (str): Path to save the file to
        username (str): Username for authentication
        api_token (str): API token for authentication
        service_name (str): Name of the service for logging
        chunk_size (int): Size of chunks to download
        log_chunk_size (int): Size threshold for logging progress
        deletion_strategy (str): Strategy for managing disk space when storage is low
        
    Returns:
        str: The filename of the downloaded file
        
    Raises:
        DownloadError: If the download fails after all retries.
        requests.exceptions.HTTPError: If a non-retriable HTTP error occurs during the download.
    """
    logging.info(f'Starting download for {service_name} backup from: {url} to {filename}')
    
    overall_start_time = time.time()

    bytes_successfully_written_to_disk = 0
    if os.path.exists(filename):
        bytes_successfully_written_to_disk = os.path.getsize(filename)
        if bytes_successfully_written_to_disk > 0:
            logging.info(f"Found existing partial file: {filename}, size: {bytes_successfully_written_to_disk} bytes. Will attempt to resume.")

    # Initialize backup deleter once for the entire download
    backup_type = _detect_backup_type_from_filename(filename)
    deletion_config = DeletionConfig()
    deletion_config.deletion_strategy = deletion_strategy  # Use configured strategy for space management
    backup_deleter = BackupDeleter(deletion_config)

    # Define the actual download attempt function
    def _do_attempt(attempt):
        return _attempt_download(
            url, filename, username, api_token, service_name,
            chunk_size, log_chunk_size,
            os.path.getsize(filename) if os.path.exists(filename) else 0,
            overall_start_time,
            attempt, MAX_DOWNLOAD_RETRIES, backup_type, backup_deleter
        )
    try:
        bytes_written = _retry_download(
            _do_attempt, filename, service_name, MAX_DOWNLOAD_RETRIES, INITIAL_RETRY_DELAY_SECONDS
        )
    except requests.exceptions.HTTPError as e:
        # Non-retriable HTTP error
        logging.error(
            f"HTTP error during download for {service_name}: "
            f"{e.response.status_code} - {e}"
        )
        raise
    except Exception as e:
        # Wrap any other exception as DownloadError
        raise DownloadError(
            f"Download failed for {service_name} after {MAX_DOWNLOAD_RETRIES + 1} attempts: {e}"
        )
    logging.info("Download completed successfully.")
    _log_download_complete(service_name, filename, bytes_written, overall_start_time)
    return filename

def _retry_download(download_fn, filename, service_name, max_retries, initial_delay_seconds):
    """Retry a download function with exponential backoff and progress updates."""
    delay = initial_delay_seconds
    for attempt in range(max_retries + 1):
        try:
            return download_fn(attempt)
        except RETRIABLE_EXCEPTIONS as e:
            logging.warning(
                f"Download attempt {attempt + 1}/{max_retries + 1} for {service_name} failed: {type(e).__name__} - {e}"
            )
            # refresh current progress
            bytes_on_disk = os.path.getsize(filename) if os.path.exists(filename) else 0
            if attempt < max_retries:
                logging.info(
                    f"Retrying in {delay} seconds... Current progress: {bytes_on_disk} bytes."
                )
                time.sleep(delay)
                delay *= RETRY_DELAY_MULTIPLIER
            else:
                logging.error(
                    f"Max retries reached for {service_name} download. "
                    f"Failed after {max_retries + 1} attempts. Final progress: {bytes_on_disk} bytes."
                )
                raise

def _attempt_download(url, filename, username, api_token, service_name,
                      chunk_size, log_chunk_size,
                      current_expected_on_disk, overall_start_time,
                      attempt, max_retries, backup_type, backup_deleter):
    """Perform a single download attempt, handling range and streaming."""
    headers = _prepare_range_request(current_expected_on_disk, attempt, max_retries)
    
    try:
        response = make_authenticated_request(
            'GET', url, username, api_token,
            stream=True, headers=headers, timeout=30
        )
    except requests.exceptions.HTTPError as e:
        # Handle HTTP 416 Range Not Satisfiable error
        if e.response.status_code == 416:
            logging.warning(
                f"HTTP 416 Range Not Satisfiable error for {service_name} download. "
                f"Deleting partial file and restarting from beginning."
            )
            # Delete the partial file and restart
            if os.path.exists(filename):
                os.remove(filename)
                logging.info(f"Deleted partial file: {filename}")
            
            # Retry without range headers (full download)
            response = make_authenticated_request(
                'GET', url, username, api_token,
                stream=True, timeout=30
            )
            file_open_mode, start_bytes = 'wb', 0
        else:
            # Re-raise other HTTP errors
            raise
    else:
        # Normal response handling
        file_open_mode, start_bytes = _handle_range_response(
            response, current_expected_on_disk
        )
    
    return _stream_response_to_file(
        response, filename, file_open_mode, start_bytes,
        chunk_size, log_chunk_size, service_name, overall_start_time,
        backup_type, backup_deleter
    )

def _handle_range_response(response, current_expected_on_disk):
    """Determine file open mode and adjusted start bytes based on response."""
    if current_expected_on_disk > 0:
        if response.status_code == 206:
            logging.info("Server responded with 206 Partial Content. Appending to existing file.")
            return 'ab', current_expected_on_disk
        elif response.status_code == 200:
            logging.warning("Server sent 200 OK despite Range request. Restarting download from beginning.")
            return 'wb', 0
        elif response.status_code == 416:
            logging.warning("Server returned 416 Range Not Satisfiable. The partial file may be corrupted or the server doesn't support resuming. Restarting download from beginning.")
            return 'wb', 0
        else:
            logging.warning(f"Unexpected status {response.status_code} with Range request. Restarting download.")
            return 'wb', 0
    # fresh download
    return 'wb', 0

def _stream_response_to_file(response, filename, file_open_mode, initial_bytes, chunk_size, log_chunk_size, service_name, overall_start_time, backup_type, backup_deleter):
    """Stream response content to file with progress logging and disk space management, return total bytes written."""
    bytes_written = initial_bytes
    last_log_time = time.time()
    next_log_threshold = bytes_written + log_chunk_size
    
    # Set up disk space monitoring
    file_path = Path(filename)
    backup_directory = file_path.parent
    minimum_free_space = chunk_size * 10  # Risk buffer: 10 times chunk size

    with open(filename, file_open_mode) as f:
        for chunk in response.iter_content(chunk_size=chunk_size):
            if not chunk:
                continue
                
            # Check disk space before writing the chunk
            _ensure_disk_space_available(
                backup_directory, minimum_free_space, backup_type, 
                backup_deleter, service_name
            )
            
            f.write(chunk)
            bytes_written += len(chunk)
            current_time = time.time()
            if bytes_written >= next_log_threshold:
                _log_download_progress(
                    service_name,
                    bytes_written,
                    current_time,
                    overall_start_time,
                    last_log_time,
                    log_chunk_size
                )
                next_log_threshold += log_chunk_size
                last_log_time = current_time
    return bytes_written

def _log_download_progress(service_name, bytes_downloaded, current_time, start_time, last_log_time, log_chunk_size):
    """Log download progress with speed metrics."""
    mb = bytes_downloaded / (1024 * 1024)
    elapsed = current_time - start_time
    speed = mb / elapsed if elapsed > 0 else 0
    
    # Calculate recent speed (since last log)
    recent_elapsed = current_time - last_log_time
    recent_bytes = log_chunk_size / (1024 * 1024)  # Convert to MB
    recent_speed = recent_bytes / recent_elapsed if recent_elapsed > 0 else 0
    
    msg = f"Downloaded {mb:.2f} MB of {service_name} backup ({speed:.2f} MB/s, current: {recent_speed:.2f} MB/s)"
    progress_logger = logging.getLogger("progress")
    progress_logger.info(msg, extra={"progress": True})

def _log_download_complete(service_name, filename, bytes_downloaded, start_time):
    """Log completion of download with final statistics."""
    total_elapsed = time.time() - start_time
    total_mb = bytes_downloaded / (1024 * 1024)
    avg_speed = total_mb / total_elapsed if total_elapsed > 0 else 0
    # Move to new line after inline progress
    print()
    logging.info('Downloaded %s backup to %s (%.2f MB in %.1f seconds, avg: %.2f MB/s)', 
                service_name, filename, total_mb, total_elapsed, avg_speed)

def _prepare_range_request(current_expected_on_disk, attempt, max_retries):
    """Return headers dict for HTTP Range requests when resuming downloads."""
    if current_expected_on_disk > 0:
        logging.debug(
            f"Resuming download attempt {attempt + 1}/{max_retries + 1}, starting at byte {current_expected_on_disk}"
        )
        return {'Range': f'bytes={current_expected_on_disk}-'}
    return {}

def _detect_backup_type_from_filename(filename):
    """Detect backup type (jira or confluence) from filename."""
    filename_lower = Path(filename).name.lower()
    if "jira" in filename_lower:
        return "jira"
    elif "confluence" in filename_lower:
        return "confluence"
    else:
        # Default to jira if unclear
        return "jira"

def _ensure_disk_space_available(backup_directory, minimum_free_space, backup_type, backup_deleter, service_name):
    """Ensure sufficient disk space is available, delete old backups if necessary."""
    try:
        # Get available disk space
        _, _, free_bytes = shutil.disk_usage(backup_directory)
        
        # Check if we have enough space
        if free_bytes >= minimum_free_space:
            return  # Sufficient space available
        
        logging.warning(
            f"Low disk space detected during {service_name} download. "
            f"Free space: {free_bytes / (1024*1024):.1f} MB, "
            f"Required: {minimum_free_space / (1024*1024):.1f} MB. "
            f"Attempting to free space by deleting old {backup_type} backups."
        )
        
        # Try to delete old backups to free space
        deleted_files = 0
        max_deletion_attempts = 5  # Prevent infinite loop
        
        while free_bytes < minimum_free_space and deleted_files < max_deletion_attempts:
            # Use the backup deleter to remove one old backup
            deleted_file = backup_deleter.delete_one_backup(backup_directory, backup_type)
            
            if deleted_file is None:
                # No more files to delete
                logging.warning(
                    f"No more {backup_type} backup files to delete in {backup_directory}. "
                    f"Free space: {free_bytes / (1024*1024):.1f} MB"
                )
                break
            
            deleted_files += 1
            logging.info(f"Deleted old backup file: {deleted_file}")
            
            # Refresh free space after deletion
            _, _, free_bytes = shutil.disk_usage(backup_directory)
            
        if free_bytes >= minimum_free_space:
            logging.info(
                f"Successfully freed disk space. "
                f"Free space: {free_bytes / (1024*1024):.1f} MB, "
                f"Deleted {deleted_files} old backup files."
            )
        else:
            logging.warning(
                f"Still insufficient disk space after deleting {deleted_files} files. "
                f"Free space: {free_bytes / (1024*1024):.1f} MB, "
                f"Required: {minimum_free_space / (1024*1024):.1f} MB. "
                f"Download may fail due to insufficient disk space."
            )
            
    except Exception as e:
        logging.error(f"Error checking or managing disk space: {e}")
        # Continue download attempt even if space management fails