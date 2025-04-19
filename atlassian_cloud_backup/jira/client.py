"""Jira backup functionality."""

import time
import logging
from datetime import datetime, timedelta, timezone

from atlassian import Jira
from atlassian_cloud_backup.utils.http_utils import make_authenticated_request, download_file

class JiraClient:
    """Client for handling Jira backup operations."""
    
    def __init__(self, url, username, api_token, poll_interval=30):
        """
        Initialize Jira client.
        
        Args:
            url (str): Jira instance URL
            username (str): Username for authentication
            api_token (str): API token for authentication
            poll_interval (int): Seconds to wait between polling requests
        """
        self.url = url
        self.username = username
        self.api_token = api_token
        self.poll_interval = poll_interval
        
        # Log the URL being used
        logging.info('Connecting to Jira instance at %s', self.url)
        self.jira = Jira(url=self.url, username=self.username, password=self.api_token)
    
    def process_backup(self, status, now):
        """Handle Jira backup process and return updated status.
        
        Args:
            status (dict): Current backup status
            now (datetime): Current datetime
            
        Returns:
            dict: Updated backup status
        """
        updated = {}
        
        # Fetch and compare Jira task IDs
        server_task_id = self.fetch_last_task_id()
        local_task_id = status.get('jira_task_id')
        local_file = status.get('jira_file')

        # Skip if we've already processed this task ID
        if server_task_id is not None and server_task_id == local_task_id and local_file:
            logging.info('Jira task ID %d already processed in previous run. Skipping Jira backup for %s', 
                        server_task_id, self.url)
            return updated
            
        if server_task_id != local_task_id:
            logging.info('Using server task ID %d (local was %s)', server_task_id, local_task_id)
        
        # Try to use existing task if it's recent enough
        if server_task_id:
            updated.update(self._check_existing_task(server_task_id, now))
            
        # Create new backup if needed
        if not updated:
            updated.update(self._create_new_backup(now))
            
        return updated
        
    def fetch_last_task_id(self):
        """Get the ID of the last backup task.
        
        Returns:
            int or None: Task ID or None if no tasks exist
        """
        logging.info('Fetching last Jira backup task ID from server')
        url = f"{self.url.rstrip('/')}/rest/backup/1/export/lastTaskId"
        try:
            response = make_authenticated_request('GET', url, self.username, self.api_token)
            
            # Handle empty response
            response_text = response.text.strip()
            if not response_text:
                logging.info('Server returned empty lastTaskId, no previous backup exists')
                return None
            
            try:
                task_id = int(response_text)
                logging.info('Server lastTaskId: %d', task_id)
                return task_id
            except ValueError:
                # Only raise if it's not empty but also not a valid integer
                raise RuntimeError(f"Unexpected response for lastTaskId: '{response_text}'")
        except Exception as e:
            logging.error(f"Error fetching last task ID: {str(e)}")
            return None
    
    def fetch_task_info(self, task_id):
        """Get information about a specific backup task.
        
        Args:
            task_id (int): Task ID to fetch information for
            
        Returns:
            dict: Task information
        """
        logging.info('Fetching Jira task info for ID %d', task_id)
        try:
            return self.jira.get(f'/rest/api/3/task/{task_id}')
        except Exception:
            # Fall back to direct API call if the Jira library fails
            url = f"{self.url.rstrip('/')}/rest/api/3/task/{task_id}"
            response = make_authenticated_request('GET', url, self.username, self.api_token)
            return response.json()
    
    def trigger_backup(self):
        """Start a new Jira backup.
        
        Returns:
            int: Task ID of the new backup
        """
        logging.info('Triggering Jira backup via POST /rest/backup/1/export/runbackup')
        url = f"{self.url.rstrip('/')}/rest/backup/1/export/runbackup"
        headers = {'Content-Type': 'application/json'}
        payload = {"cbAttachments": "true", "exportToCloud": "true"}
        
        response = make_authenticated_request(
            'POST', url, self.username, self.api_token, 
            headers=headers, json=payload
        )
        data = response.json()
        
        task_id = data.get('taskId') or data.get('task_id')
        if not task_id:
            raise RuntimeError('No taskId returned from Jira backup runbackup.')
            
        logging.info('Jira backup triggered, task ID: %s', task_id)
        return int(task_id)
        
    def wait_for_completion(self, task_id):
        """Wait until a Jira backup task completes.
        
        Args:
            task_id (int): Task ID to monitor
            
        Returns:
            bool: True if backup completed successfully, False otherwise
        """
        logging.info('Waiting for Jira backup to complete (task %d)...', task_id)
        endpoint = '/rest/backup/1/export/getProgress'
        url = f"{self.url.rstrip('/')}{endpoint}"
        
        while True:
            response = make_authenticated_request(
                'GET', url, self.username, self.api_token, 
                params={'taskId': task_id}
            )
            resp = response.json()
            
            percent = resp.get('progress', 0)
            status = resp.get('status', '').upper()
            logging.info('Progress: %s%%, status: %s', percent, status)
            
            if status in ('COMPLETE', 'DONE', 'SUCCESSFUL') or percent == 100:
                logging.info('Jira backup in the Atlassian Cloud completed.')
                return True
                
            if status in ('FAILED', 'ERROR'):
                logging.error('Jira backup in the Atlassian Cloud failed with status: %s', status)
                return False
                
            time.sleep(self.poll_interval)
    
    def get_download_url(self, task_id):
        """Get the download URL for a completed backup.
        
        Args:
            task_id (int): Task ID of the backup
            
        Returns:
            str: Download URL
        """
        logging.info('Retrieving download URL for Jira backup task %d', task_id)
        endpoint = '/rest/backup/1/export/getProgress'
        url = f"{self.url.rstrip('/')}{endpoint}"
        
        response = make_authenticated_request(
            'GET', url, self.username, self.api_token, 
            params={'taskId': task_id}
        )
        
        data = response.json()
        result = data.get('result')
        if not result:
            raise RuntimeError('No result found in Jira backup response.')
        
        download_url = f"{self.url.rstrip('/')}/plugins/servlet/{result}"
        logging.info('Found Jira backup download URL: %s', download_url)
        return download_url
    
    def download_backup_file(self, task_id, filename):
        """Download the backup file for a completed task.
        
        Args:
            task_id (int): Task ID to download
            filename (str): File path to save the backup to
            
        Returns:
            str: Path to the downloaded file
        """
        download_url = self.get_download_url(task_id)
        return download_file(download_url, filename, self.username, self.api_token, "Jira")
    
    def _check_existing_task(self, task_id, now):
        """Check if an existing task can be used for backup.
        
        Args:
            task_id (int): Task ID to check
            now (datetime): Current datetime
            
        Returns:
            dict: Updated backup status if task is usable, empty dict otherwise
        """
        updated = {}
        task_info = self.fetch_task_info(task_id)
        submitted_ms = task_info.get('submitted')
        
        if not submitted_ms:
            logging.error('Missing "submitted" timestamp in Jira task %d response: %s', task_id, task_info)
            raise ValueError(f'Missing "submitted" timestamp in Jira task {task_id} response: {task_info}')
            
        # Convert milliseconds timestamp to datetime
        created = datetime.fromtimestamp(submitted_ms / 1000, tz=timezone.utc)
        created_str = created.astimezone().strftime('%Y-%m-%d %H:%M:%S %Z')
        
        if now - created <= timedelta(hours=24):
            logging.info('Reusing Jira task %d from %s (local time %s)', 
                        task_id, created_str, created.astimezone().strftime('%Y-%m-%d %H:%M:%S %Z'))
                        
            if self.wait_for_completion(task_id):
                # Wait successful, now download the file
                from atlassian_cloud_backup.utils.file_utils import FileManager
                file_manager = FileManager(self.url)
                filename = file_manager.prepare_backup_path("Jira")
                
                download_filename = self.download_backup_file(task_id, filename)
                updated['last_jira_backup'] = created
                updated['jira_task_id'] = task_id
                updated['jira_file'] = download_filename
        else:
            logging.info('Existing Jira task %d is older than 24 h (%s), triggering new.', 
                        task_id, created_str)
                        
        return updated
    
    def _create_new_backup(self, now):
        """Create a new backup.
        
        Args:
            now (datetime): Current datetime
            
        Returns:
            dict: Updated backup status
        """
        updated = {}
        new_task_id = self.trigger_backup()
        
        if self.wait_for_completion(new_task_id):
            # Wait successful, now download the file
            from atlassian_cloud_backup.utils.file_utils import FileManager
            file_manager = FileManager(self.url)
            filename = file_manager.prepare_backup_path("Jira")
            
            download_filename = self.download_backup_file(new_task_id, filename)
            updated['last_jira_backup'] = now
            updated['jira_task_id'] = new_task_id
            updated['jira_file'] = download_filename
            
        return updated