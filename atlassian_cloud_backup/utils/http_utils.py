"""HTTP utilities for Atlassian Cloud Backup."""

import os
import time
import logging
import requests
from requests.auth import HTTPBasicAuth
import http.client # For IncompleteRead

class DownloadError(Exception):
    """Raised when a download fails after all retry attempts."""

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

def download_file(url, filename, username, api_token, service_name, chunk_size=8192, log_chunk_size=100*1024*1024):
    """Download a file with progress tracking and retry/resume capabilities.
    
    Args:
        url (str): URL to download from
        filename (str): Path to save the file to
        username (str): Username for authentication
        api_token (str): API token for authentication
        service_name (str): Name of the service for logging
        chunk_size (int): Size of chunks to download
        log_chunk_size (int): Size threshold for logging progress
        
    Returns:
        str: The filename of the downloaded file
        
    Raises:
        Exception: If download fails after all retries or due to a non-retriable HTTP error.
    """
    logging.info(f'Starting download for {service_name} backup from: {url} to {filename}')
    
    max_retries = 5
    initial_delay_seconds = 1
    overall_start_time = time.time()

    bytes_successfully_written_to_disk = 0
    if os.path.exists(filename):
        bytes_successfully_written_to_disk = os.path.getsize(filename)
        if bytes_successfully_written_to_disk > 0:
            logging.info(f"Found existing partial file: {filename}, size: {bytes_successfully_written_to_disk} bytes. Will attempt to resume.")

    for attempt in range(max_retries + 1):
        try:
            # attempt download and streaming
            bytes_successfully_written_to_disk = _attempt_download(
                url, filename, username, api_token, service_name,
                chunk_size, log_chunk_size,
                bytes_successfully_written_to_disk, overall_start_time,
                attempt, max_retries
            )
            logging.info(f"Download attempt {attempt + 1} completed successfully.")
            _log_download_complete(
                service_name, filename, bytes_successfully_written_to_disk, overall_start_time
            )
            return filename

        except (http.client.IncompleteRead, requests.exceptions.ChunkedEncodingError, requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            logging.warning(f"Download attempt {attempt + 1}/{max_retries + 1} for {service_name} failed: {type(e).__name__} - {str(e)}")
            
            # Update bytes_successfully_written_to_disk from actual file size before next attempt
            if os.path.exists(filename):
                bytes_successfully_written_to_disk = os.path.getsize(filename)
            else:
                # This case (file doesn't exist after trying to write) is unlikely but reset to be safe
                bytes_successfully_written_to_disk = 0
            
            if attempt < max_retries:
                delay = initial_delay_seconds * (2 ** attempt)
                logging.info(f"Retrying in {delay} seconds... Current progress: {bytes_successfully_written_to_disk} bytes.")
                time.sleep(delay)
            else:
                logging.error(f"Max retries reached for {service_name} download. Failed after {max_retries + 1} attempts. Final progress: {bytes_successfully_written_to_disk} bytes.")
                raise # Re-raise the last caught exception
        
        except requests.exceptions.HTTPError as e:
            # Non-retriable HTTP errors (e.g., 403, 404, 500)
            logging.error(f"HTTP error during download for {service_name} (Attempt {attempt + 1}): {e.response.status_code} - {e}")
            raise # Re-raise immediately

    # This part should ideally not be reached if logic is correct (either success or re-raise)
    logging.error(f"Download for {service_name} failed definitively after all attempts.")
    raise DownloadError(f"Download failed for {service_name} after {max_retries + 1} attempts.")

def _attempt_download(url, filename, username, api_token, service_name,
                      chunk_size, log_chunk_size,
                      current_expected_on_disk, overall_start_time,
                      attempt, max_retries):
    """Perform a single download attempt, handling range and streaming."""
    headers = _prepare_range_request(current_expected_on_disk, attempt, max_retries)
    response = make_authenticated_request(
        'GET', url, username, api_token,
        stream=True, headers=headers, timeout=30
    )
    file_open_mode, start_bytes = _handle_range_response(
        response, current_expected_on_disk
    )
    return _stream_response_to_file(
        response, filename, file_open_mode, start_bytes,
        chunk_size, log_chunk_size, service_name, overall_start_time
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
        else:
            logging.warning(f"Unexpected status {response.status_code} with Range request. Restarting download.")
            return 'wb', 0
    # fresh download
    return 'wb', 0

def _stream_response_to_file(response, filename, file_open_mode, initial_bytes, chunk_size, log_chunk_size, service_name, overall_start_time):
    """Stream response content to file with progress logging, return total bytes written."""
    bytes_written = initial_bytes
    last_log_time = time.time()
    next_log_threshold = bytes_written + log_chunk_size

    with open(filename, file_open_mode) as f:
        for chunk in response.iter_content(chunk_size=chunk_size):
            if not chunk:
                continue
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
    
    logging.info('Downloaded %.2f MB of %s backup (%.2f MB/s, current: %.2f MB/s)...', 
                mb, service_name, speed, recent_speed)

def _log_download_complete(service_name, filename, bytes_downloaded, start_time):
    """Log completion of download with final statistics."""
    total_elapsed = time.time() - start_time
    total_mb = bytes_downloaded / (1024 * 1024)
    avg_speed = total_mb / total_elapsed if total_elapsed > 0 else 0
    logging.info('Downloaded %s backup to %s (%.2f MB in %.1f seconds, avg: %.2f MB/s)', 
                service_name, filename, total_mb, total_elapsed, avg_speed)