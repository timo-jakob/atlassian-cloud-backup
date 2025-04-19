"""Confluence backup functionality."""

import time
import logging
from datetime import datetime, timedelta, timezone

from atlassian_cloud_backup.utils.http_utils import make_authenticated_request, download_file

class ConfluenceClient:
    """Client for handling Confluence backup operations."""
    
    def __init__(self, url, username, api_token, poll_interval=30, include_attachments=True):
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
        
        # Log the URL being used
        logging.info('Connecting to Confluence instance at %s', self.url)
        
    def process_backup(self, status, now):
        """Handle Confluence backup process and return updated status.
        
        Args:
            status (dict): Current backup status
            now (datetime): Current datetime
            
        Returns:
            dict: Updated backup status
        """
        updated = {}
        conf_status = self.get_backup_status()

        # Skip if Confluence is not available or unlicensed
        if conf_status is None:
            logging.info('Skipping Confluence backup for %s', self.url)
            return updated
        
        if self._can_use_existing_backup(conf_status, now):
            return self._use_existing_backup(conf_status)
        
        return self._create_new_backup(now)
    
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
            bool: True if the backup was triggered successfully
        """
        logging.info('Triggering Confluence backup...')
        endpoint = f"{self.url.rstrip('/')}/wiki/rest/obm/1.0/runbackup"
        logging.info('Confluence backup endpoint: %s', endpoint)
        
        headers = {
            'Content-Type': 'application/json',
            'X-Atlassian-Token': 'no-check',
            'X-Requested-With': 'XMLHttpRequest'
        }
        payload = {'cbAttachments': self.include_attachments}
        
        response = make_authenticated_request(
            'POST', endpoint, self.username, self.api_token,
            headers=headers, json=payload
        )
        
        logging.info('Confluence backup triggered.')
        return True

    def wait_for_completion(self):
        """Wait until a Confluence backup completes.
        
        Returns:
            bool: True if backup completed successfully, False otherwise
        """
        logging.info('Monitoring Confluence backup progress...')
        url = f"{self.url.rstrip('/')}/wiki/rest/obm/1.0/getprogress.json"
        
        while True:
            response = make_authenticated_request('GET', url, self.username, self.api_token)
            data = response.json()
            
            status = data.get('currentStatus', '')
            progress = data.get('alternativePercentage', 0)
            logging.info('Confluence backup progress: %s%%, status: %s', progress, status)
            
            if status == 'COMPLETE':
                logging.info('Confluence backup completed.')
                return True
            elif status in ('FAILED', 'ERROR'):
                logging.error('Confluence backup failed with status: %s', status)
                return False
                
            time.sleep(self.poll_interval)
    
    def wait_for_file(self):
        """Wait for Confluence backup to complete and download the file.
        
        Returns:
            str or None: Path to downloaded file, or None if download failed
        """
        logging.info('Waiting for Confluence backup file...')
        
        # Wait for the backup to be ready and get its data
        backup_data = self._wait_for_complete_status()
        if not backup_data:
            return None
            
        # Get download details from the backup data
        download_details = self._get_download_details(backup_data)
        if not download_details:
            return None
            
        # Download the file
        return self._download_backup_file(download_details)
    
    def _wait_for_complete_status(self):
        """Wait until the Confluence backup status is complete.
        
        Returns:
            dict or None: Backup data if complete, None if failed
        """
        url = f"{self.url.rstrip('/')}/wiki/rest/obm/1.0/getprogress.json"
        
        while True:
            response = make_authenticated_request('GET', url, self.username, self.api_token)
            data = response.json()
            
            status = data.get('currentStatus', '')
            
            if status == 'COMPLETE':
                return data
            
            if status in ('FAILED', 'ERROR'):
                logging.error('Confluence backup failed with status: %s', status)
                return None
            
            logging.info('Backup not yet complete (status: %s), waiting...', status)
            time.sleep(self.poll_interval)
    
    def _get_download_details(self, data):
        """Extract download URL and local filename from backup data.
        
        Args:
            data (dict): Backup data from API response
            
        Returns:
            dict or None: Download details (url and filename) or None if invalid
        """
        remote_filename = data.get('filename')
        if not remote_filename:
            logging.error("No filename found in Confluence backup response")
            return None
        
        logging.info("Found Confluence backup filename: %s", remote_filename)
        download_url = f"{self.url.rstrip('/')}/{remote_filename}"
        
        from atlassian_cloud_backup.utils.file_utils import FileManager
        file_manager = FileManager(self.url)
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
    
    def _can_use_existing_backup(self, conf_status, now):
        """Check if an existing Confluence backup can be used.
        
        Args:
            conf_status (dict): Current backup status
            now (datetime): Current datetime
            
        Returns:
            bool: True if the existing backup can be used
        """
        conf_time = conf_status.get('time')
        if not conf_time:
            return False

        try:
            conf_timestamp = datetime.fromtimestamp(conf_time / 1000, tz=timezone.utc)
            one_week_ago = now - timedelta(days=7)
            is_outdated = conf_status.get('isOutdated', True)
            
            return (not is_outdated and 
                    conf_timestamp > one_week_ago and 
                    conf_status.get('currentStatus') == 'COMPLETE')
        except (ValueError, TypeError):
            logging.warning('Invalid timestamp in Confluence backup status: %s', conf_time)
            return False

    def _use_existing_backup(self, conf_status):
        """Use and download an existing Confluence backup.
        
        Args:
            conf_status (dict): Current backup status
            
        Returns:
            dict: Updated backup status
        """
        updated = {}
        conf_time = conf_status.get('time')
        conf_timestamp = datetime.fromtimestamp(conf_time / 1000, tz=timezone.utc)
        
        logging.info('Using existing Confluence backup from %s', 
                    conf_timestamp.astimezone().strftime('%Y-%m-%d %H:%M:%S %Z'))
        
        conf_file = self.wait_for_file()
        if conf_file:
            updated['last_confluence_backup'] = conf_timestamp
            updated['confluence_file'] = conf_file
            logging.info('Downloaded existing Confluence backup')
            
        return updated

    def _create_new_backup(self, now):
        """Create a new Confluence backup.
        
        Args:
            now (datetime): Current datetime
            
        Returns:
            dict: Updated backup status
        """
        updated = {}
        logging.info('Creating new Confluence backup')
        
        self.trigger_backup()
        if self.wait_for_completion():
            conf_file = self.wait_for_file()
            if conf_file:
                updated['last_confluence_backup'] = now
                updated['confluence_file'] = conf_file
                
        return updated