"""HTTP utilities for Atlassian Cloud Backup."""

import os
import time
import logging
import requests
from requests.auth import HTTPBasicAuth
import http.client # For IncompleteRead

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
        current_attempt_start_time = time.time()
        last_log_time_for_recent_speed = current_attempt_start_time
        
        # In case of retry, bytes_successfully_written_to_disk is already updated from previous failed attempt's except block
        # or from the initial check before the loop.
        
        headers = {}
        file_open_mode = 'wb' # Default to overwrite for a new segment or full download
        
        # Effective starting position for this attempt's download stream
        # This is also the amount of data we expect to be on disk if resuming.
        current_expected_on_disk = bytes_successfully_written_to_disk

        headers = _prepare_range_request(current_expected_on_disk, attempt, max_retries)

        try:
            response = make_authenticated_request(
                'GET', url, username, api_token, 
                stream=True, headers=headers, timeout=30 # timeout for connect and initial response
            )

            # Check how the server responded to our Range request
            if current_expected_on_disk > 0: # We sent a Range header
                if response.status_code == 206: # Partial Content - server supports resume
                    logging.info("Server responded with 206 Partial Content. Appending to existing file.")
                    file_open_mode = 'ab'
                elif response.status_code == 200: # OK - server sent the whole file
                    logging.warning("Server sent 200 OK despite Range request. Restarting download from beginning.")
                    bytes_successfully_written_to_disk = 0 # Reset, as we are overwriting
                    current_expected_on_disk = 0
                    file_open_mode = 'wb'
                else:
                    # For other status codes with Range request, make_authenticated_request's raise_for_status would likely have triggered.
                    # If not, this is an unexpected state. Default to overwrite.
                    logging.warning(f"Unexpected status {response.status_code} with Range request. Restarting download.")
                    bytes_successfully_written_to_disk = 0
                    current_expected_on_disk = 0
                    file_open_mode = 'wb'
            else: # Not a range request, so expect 200 OK for a full download
                file_open_mode = 'wb' # Should already be 'wb'
                bytes_successfully_written_to_disk = 0 # Ensure this is 0 for a fresh full download

            # bytes_downloaded_this_stream tracks progress for the current response stream
            bytes_downloaded_this_stream = 0
            # next_log_threshold_abs is based on total bytes expected on disk
            next_log_threshold_abs = current_expected_on_disk + log_chunk_size
            
            with open(filename, file_open_mode) as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if not chunk:
                        continue
                    
                    f.write(chunk)
                    bytes_written_in_chunk = len(chunk)
                    
                    if file_open_mode == 'wb' and current_expected_on_disk == 0:
                        # If overwriting, bytes_successfully_written_to_disk tracks from 0 for this stream
                        bytes_successfully_written_to_disk += bytes_written_in_chunk
                    elif file_open_mode == 'ab':
                        # If appending, add to the existing total
                        bytes_successfully_written_to_disk += bytes_written_in_chunk
                    # If 'wb' but current_expected_on_disk was >0 (due to server 200 OK), it was reset, so first branch applies.

                    bytes_downloaded_this_stream += bytes_written_in_chunk
                    current_time = time.time()
                    
                    if bytes_successfully_written_to_disk >= next_log_threshold_abs:
                        _log_download_progress(
                            service_name,
                            bytes_successfully_written_to_disk, # Total bytes on disk
                            current_time,
                            overall_start_time, # For overall average speed
                            last_log_time_for_recent_speed, # For recent speed
                            log_chunk_size # Expected amount for recent speed calculation
                        )
                        next_log_threshold_abs += log_chunk_size
                        last_log_time_for_recent_speed = current_time
            
            # If loop completes, this attempt was successful
            logging.info(f"Download attempt {attempt + 1} completed successfully.")
            _log_download_complete(service_name, filename, bytes_successfully_written_to_disk, overall_start_time)
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
    raise Exception(f"Download failed for {service_name} after {max_retries + 1} attempts.")

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