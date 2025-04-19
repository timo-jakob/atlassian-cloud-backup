"""HTTP utilities for Atlassian Cloud Backup."""

import time
import logging
import requests
from requests.auth import HTTPBasicAuth

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
    """Download a file with progress tracking.
    
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
    """
    logging.info(f'Downloading {service_name} backup from: {url}')
    
    response = make_authenticated_request('GET', url, username, api_token, stream=True)
    
    bytes_downloaded = 0
    next_log_threshold = log_chunk_size
    start_time = time.time()
    last_log_time = start_time
    
    with open(filename, 'wb') as f:
        for chunk in response.iter_content(chunk_size=chunk_size):
            if not chunk:
                continue
                
            f.write(chunk)
            bytes_downloaded += len(chunk)
            current_time = time.time()
            
            if bytes_downloaded >= next_log_threshold:
                _log_download_progress(
                    service_name, 
                    bytes_downloaded, 
                    current_time, 
                    start_time, 
                    last_log_time,
                    log_chunk_size
                )
                next_log_threshold += log_chunk_size
                last_log_time = current_time
    
    _log_download_complete(service_name, filename, bytes_downloaded, start_time)
    return filename

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