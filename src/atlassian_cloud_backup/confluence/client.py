"""Confluence backup functionality."""

import os
import time
import logging
import requests  # for HTTPError handling
from datetime import datetime, timedelta, timezone
from requests.exceptions import HTTPError

from atlassian_cloud_backup.utils.http_utils import make_authenticated_request, download_file

# Default timeout of 6 hours (360 minutes), can be overridden with environment variable
DEFAULT_TIMEOUT_MINUTES = int(os.getenv('CONFLUENCE_BACKUP_TIMEOUT_MINUTES', 480))

class ConfluenceClient:
    """Client for handling Confluence backup operations."""
    
    def __init__(self, url, username, api_token, poll_interval=30, include_attachments=True, backup_target_directory=None):
        """
        Initialize Confluence client.
        
        Args:
            url (str): Confluence instance URL
            username (str): Username for authentication
            api_token (str): API token for authentication
            poll_interval (int): Seconds to wait between polling requests
            include_attachments (bool): Whether to include attachments in backups
        """
        self.url = url
        self.username = username
        self.api_token = api_token
        self.poll_interval = poll_interval
        self.include_attachments = include_attachments
        self.backup_target_directory = backup_target_directory
        
        # Log the URL being used
        logging.info('Connecting to Confluence instance at %s', self.url)
        
    def process_backup(self, status, now):
        """Handle Confluence backup process and return updated status.
        
        Args:
            status (dict): Current backup status
            now (datetime): Current datetime
            
        Returns:
            dict: Updated backup status with 'confluence_action' key indicating the action taken
        """
        logging.info('Starting Confluence backup process - always attempting to trigger new backup')
        
    def process_backup(self, status, now):
        """Handle Confluence backup process and return updated status.
        
        Args:
            status (dict): Current backup status
            now (datetime): Current datetime
            
        Returns:
            dict: Updated backup status with 'confluence_action' key indicating the action taken
        """
        logging.info('Starting Confluence backup process - always attempting to trigger new backup')
        
        # Check if Confluence is available first
        if not self._is_confluence_available():
            return {'confluence_action': 'SKIPPED_UNAVAILABLE'}
        
        # Always try to trigger a new backup first
        try:
            return self._attempt_backup_trigger(now)
        except HTTPError as e:
            return self._handle_http_error(e)
        except Exception as e:
            logging.error('Unexpected error during Confluence backup trigger: %s', str(e))
            return {'confluence_action': 'FAILED'}

    def _is_confluence_available(self):
        """Check if Confluence service is available."""
        conf_status = self.get_backup_status()
        if conf_status is None:
            logging.info('Skipping Confluence backup for %s - service unavailable', self.url)
            return False
        return True

    def _attempt_backup_trigger(self, now):
        """Attempt to trigger a new backup."""
        success = self.trigger_backup()
        if success:
            # HTTP 200 - backup triggered successfully
            logging.info('Confluence backup triggered successfully')
            return self._wait_and_download_backup(now)
        else:
            # This handles the 406 case (backup already in progress)
            logging.info('Confluence backup already in progress, waiting for completion')
            return self._wait_and_download_backup(now, use_existing=True)

    def _handle_http_error(self, e):
        """Handle HTTP errors during backup trigger."""
        if getattr(e, 'response', None) and e.response.status_code == 412:
            return self._handle_frequency_limit_error(e)
        else:
            # Other HTTP errors, re-raise
            logging.error('Unexpected error triggering Confluence backup: %s', str(e))
            raise

    def _handle_frequency_limit_error(self, e):
        """Handle HTTP 412 - backup frequency limit."""
        try:
            error_data = e.response.json()
            error_message = error_data.get('error', 'Backup frequency limit exceeded')
        except (ValueError, AttributeError):
            # Fallback if response is not JSON
            error_message = e.response.text if hasattr(e.response, 'text') else str(e)
        
        print(f"\n⚠️  Confluence Backup Limitation: {error_message}")
        logging.info('Confluence backup denied (HTTP 412): %s', error_message)
        return {'confluence_action': 'SKIPPED_FREQUENCY_LIMIT'}
    
    def get_backup_status(self):
        """Check if a Confluence backup exists and get its status.
        
        Returns:
            dict or None: Backup status data, or None if Confluence is unavailable
        """
        logging.info('Checking Confluence backup status')
        url = f"{self.url.rstrip('/')}/wiki/rest/obm/1.0/getprogress.json"
        try:
            response = make_authenticated_request('GET', url, self.username, self.api_token)
            if response.status_code == 204:
                logging.info('Confluence appears to be unavailable or unlicensed for this instance. Skipping Confluence backup.')
                return None
            return response.json()
        except Exception as e:
            if hasattr(e, 'response') and hasattr(e.response, 'status_code') and e.response.status_code in (401, 403, 404):
                logging.info('Confluence appears to be unavailable or unlicensed for this instance. Skipping Confluence backup.')
                return None
            else:
                # For other errors, still raise the exception
                logging.error('Error checking Confluence status: %s', str(e))
                raise

    def trigger_backup(self):
        """Start a new Confluence backup.
        
        Returns:
            bool: True if the backup was triggered successfully, False if skipped due to 406.
            
        Raises:
            HTTPError: For HTTP errors other than 406 (including 412 which should be handled by caller)
        """
        logging.info('Triggering Confluence backup...')
        endpoint = f"{self.url.rstrip('/')}/wiki/rest/obm/1.0/runbackup"
        logging.info('Confluence backup endpoint: %s', endpoint)
        
        headers = {
            'Content-Type': 'application/json',
            'X-Atlassian-Token': 'no-check'
        }
        payload = {'cbAttachments': self.include_attachments}
        try:
            make_authenticated_request(
                'POST', endpoint, self.username, self.api_token,
                headers=headers, json=payload
            )
        except HTTPError as e:
            if e.response is not None and e.response.status_code == 406:
                logging.info('Confluence backup skipped (406 Not Acceptable - backup already in progress): %s', e.response.text)
                return False
            # Let other HTTP errors (including 412) bubble up to be handled by caller
            raise
        
        logging.info('Confluence backup triggered.')
        return True

    def wait_for_completion(self, timeout_minutes=None, return_data=False):
        """Wait until a Confluence backup completes, with timeout.
        
        Args:
            timeout_minutes (int): Maximum time to wait in minutes before timing out
            return_data (bool): If True, return the backup data dict; if False, return bool
            
        Returns:
            bool or dict: Success status (bool) or backup data (dict) if return_data=True
        """
        timeout_minutes = timeout_minutes or DEFAULT_TIMEOUT_MINUTES
        logging.info('Monitoring Confluence backup progress (timeout: %d minutes)...', timeout_minutes)
        
        monitor_context = self._initialize_confluence_monitoring(timeout_minutes)
        url = f"{self.url.rstrip('/')}/wiki/rest/obm/1.0/getprogress.json"
        
        while True:
            if self._is_timeout_exceeded(monitor_context['start_time'], monitor_context['timeout_delta']):
                logging.error(f'Confluence backup timed out after {timeout_minutes} minutes')
                return self._get_timeout_return_value(return_data)
                
            backup_result = self._check_and_evaluate_backup_status(url, return_data)
            if backup_result is not None:
                return backup_result
                
            time.sleep(self.poll_interval)

    def _initialize_confluence_monitoring(self, timeout_minutes):
        """Initialize monitoring context for Confluence backup."""
        return {
            'start_time': datetime.now(),
            'timeout_delta': timedelta(minutes=timeout_minutes)
        }

    def _check_and_evaluate_backup_status(self, url, return_data):
        """Check backup status and evaluate if monitoring should continue."""
        data = self._get_backup_status_data(url)
        status = data.get('currentStatus', '')
        progress = data.get('alternativePercentage', 0)
        
        self._log_backup_progress(status, progress, return_data)
        
        return self._evaluate_backup_status(status, progress, data, return_data)
    
    def _is_timeout_exceeded(self, start_time, timeout_delta):
        """Check if the backup operation has timed out.
        
        Args:
            start_time (datetime): When the backup monitoring started
            timeout_delta (timedelta): Maximum allowed duration
            
        Returns:
            bool: True if timeout has been exceeded
        """
        return datetime.now() - start_time > timeout_delta
    
    def _get_timeout_return_value(self, return_data):
        """Get the appropriate return value when timeout occurs.
        
        Args:
            return_data (bool): Whether to return data or boolean
            
        Returns:
            None or False: Appropriate timeout return value
        """
        return None if return_data else False
    
    def _get_backup_status_data(self, url):
        """Fetch backup status data from the API.
        
        Args:
            url (str): API endpoint URL
            
        Returns:
            dict: Backup status data
        """
        response = make_authenticated_request('GET', url, self.username, self.api_token)
        return response.json()
    
    def _log_backup_progress(self, status, progress, return_data):
        """Log backup progress information.
        
        Args:
            status (str): Current backup status
            progress (int): Backup progress percentage
            return_data (bool): Whether data will be returned
        """
        if status not in ('COMPLETE', 'FAILED', 'ERROR'):
            # Always show progress when available, regardless of return_data mode
            if progress and str(progress) != '0':
                logging.info('Confluence backup progress: %s%%, status: %s', progress, status)
            else:
                logging.info('Confluence backup status: %s', status)
    
    def _evaluate_backup_status(self, status, progress, data, return_data):
        """Evaluate backup status and determine if monitoring should continue.
        
        Args:
            status (str): Current backup status
            progress (int): Backup progress percentage
            data (dict): Full backup status data
            return_data (bool): Whether to return data or boolean
            
        Returns:
            bool, dict, None, or None: Return value if status is final, None if should continue
        """
        if self._check_complete_status(status, progress):
            if not return_data:
                logging.info('Confluence backup completed.')
                logging.info('Waiting 5 minutes to ensure backup file is available for download...')
                # time.sleep(5 * 60)
            return data if return_data else True
        elif status in ('FAILED', 'ERROR'):
            logging.error('Confluence backup failed with status: %s', status)
            return None if return_data else False
        
        # Continue monitoring
        return None

    def _check_complete_status(self, status, progress):
        return status == 'COMPLETE' or (status == 'Archiving attachments.' and progress == '100%')
    
    def wait_for_file(self):
        """Wait for Confluence backup to complete and download the file.
        
        Returns:
            str or None: Path to downloaded file, or None if download failed
        """
        logging.info('Waiting for Confluence backup file...')
        
        # Wait for the backup to be ready and get its data
        backup_data = self.wait_for_completion(timeout_minutes=DEFAULT_TIMEOUT_MINUTES, return_data=True)
        if not backup_data:
            return None
            
        # Get download details from the backup data
        download_details = self._get_download_details(backup_data)
        if not download_details:
            return None
            
        # Download the file
        return self._download_backup_file(download_details)
    
    def _get_download_details(self, data):
        """Extract download URL and local filename from backup data.
        
        Args:
            data (dict): Backup data from API response
            
        Returns:
            dict or None: Download details (url and filename) or None if invalid
        """
        remote_filename = data.get('fileName')
        if not remote_filename:
            logging.error("No filename found in Confluence backup response")
            return None
        
        logging.info("Found Confluence backup filename: %s", remote_filename)
        download_url = f"{self.url.rstrip('/')}/wiki/download/{remote_filename}"
        
        from atlassian_cloud_backup.utils.file_utils import FileManager
        file_manager = FileManager(self.url, backup_target_directory=self.backup_target_directory)
        local_filename = file_manager.prepare_backup_path("Confluence")
        
        return {
            'url': download_url,
            'filename': local_filename
        }
    
    def _download_backup_file(self, download_details):
        """Download the Confluence backup file.
        
        Args:
            download_details (dict): Download URL and local filename
            
        Returns:
            str: Path to the downloaded file
        """
        url = download_details['url']
        local_filename = download_details['filename']
        
        logging.info('Downloading Confluence backup from: %s', url)
        return download_file(url, local_filename, self.username, self.api_token, "Confluence")
    
    def _wait_and_download_backup(self, now, use_existing=False):
        """Wait for backup completion and download the file.
        
        Args:
            now (datetime): Current datetime
            use_existing (bool): Whether we're using an existing backup in progress
            
        Returns:
            dict: Updated backup status
        """
        action = 'WAITED_FOR_EXISTING' if use_existing else 'CREATED_NEW'
        
        logging.info('Waiting for Confluence backup to complete...')
        if not self.wait_for_completion():
            logging.error('Confluence backup failed to complete or timed out')
            return {'confluence_action': 'FAILED'}
        
        # Download the backup file
        conf_file = self.wait_for_file()
        if conf_file:
            logging.info('Successfully downloaded Confluence backup: %s', conf_file)
            return {
                'last_confluence_backup': now,
                'confluence_file': conf_file,
                'confluence_action': action
            }
        else:
            logging.error('Failed to download Confluence backup file')
            return {'confluence_action': 'FAILED'}