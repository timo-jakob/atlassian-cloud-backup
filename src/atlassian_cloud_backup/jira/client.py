"""Jira backup functionality."""

import os
import time
import logging
from datetime import datetime, timedelta, timezone
from requests.exceptions import HTTPError

from atlassian import Jira
from atlassian_cloud_backup.utils.http_utils import make_authenticated_request, download_file, DownloadError
from atlassian_cloud_backup.utils.file_utils import FileManager # Ensure FileManager is imported

# Default timeout of 6 hours (360 minutes), can be overridden with environment variable
DEFAULT_TIMEOUT_MINUTES = int(os.getenv('JIRA_BACKUP_TIMEOUT_MINUTES', 480))
DATEIME_FORMAT_STR = '%Y-%m-%d %H:%M:%S %Z'

class JiraClient:
    """Client for handling Jira backup operations."""
    
    def __init__(self, url, username, api_token, poll_interval=30, backup_target_directory=None, jira_backup_timeout_minutes=None):
        """
        Initialize Jira client.
        
        Args:
            url (str): Jira instance URL
            username (str): Username for authentication
            api_token (str): API token for authentication
            poll_interval (int): Seconds to wait between polling requests
            backup_target_directory (str, optional): Base directory for backups.
            jira_backup_timeout_minutes (int, optional): Timeout in minutes for Jira backup.
        """
        self.url = url
        self.username = username
        self.api_token = api_token
        self.poll_interval = poll_interval
        self.backup_target_directory = backup_target_directory # Store the directory
        self.jira_backup_timeout_minutes = jira_backup_timeout_minutes # Store the timeout
        
        # Log the URL being used
        logging.info('Connecting to Jira instance at %s', self.url)
        self.jira = Jira(url=self.url, username=self.username, password=self.api_token)
    
    def process_backup(self, status, now):
        """Handle Jira backup process and return updated status.
        
        Args:
            status (dict): Current backup status
            now (datetime): Current datetime
            
        Returns:
            dict: Updated backup status with 'jira_action' key indicating the action taken
        """
        # Fetch and compare Jira task IDs
        server_task_id = self.fetch_last_task_id()
        local_task_id = status.get('jira_task_id')

        # Check if an existing task can be reused. The `_check_existing_task` method evaluates
        # the task's age and determines whether it is still valid. If the task is too old or
        # otherwise invalid, a new backup will be triggered instead.
        if server_task_id is not None:
            if server_task_id == local_task_id:
                last_backup_time = status.get('last_jira_backup')
                if last_backup_time and (now - last_backup_time <= timedelta(hours=168)):
                    
                    logging.info(
                        'Jira backup from %s (task %d) is new enough. Skipping new backup.',
                        last_backup_time.strftime(DATEIME_FORMAT_STR),
                        local_task_id
                    )
                    result = dict(status)
                    result['jira_action'] = 'REUSED_EXISTING'
                    return result
                else:
                    logging.info(
                        'Local backup for task %d is older than 168 hours or timestamp missing, proceeding to check server task.',
                        local_task_id
                    )

            logging.info('Using server task ID %d (local was %s)', server_task_id, local_task_id)
            existing = self._check_existing_task(server_task_id, now, status) # Pass status to check_existing_task
            if existing:
                existing['jira_action'] = 'REUSED_EXISTING'
                return existing

        # Create new backup
        new_backup = self._create_new_backup(now)
        if new_backup:
            new_backup['jira_action'] = 'CREATED_NEW'
        return new_backup
        
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
        headers = {
            'Content-Type': 'application/json',
            'X-Requested-With': 'XMLHttpRequest'
        }
        payload = {"cbAttachments": "true", "exportToCloud": "true"}
        
        try:
            response = make_authenticated_request(
                'POST', url, self.username, self.api_token,
                headers=headers, json=payload
            )
        except HTTPError as e:
            # Fallback on server error 500: use lastTaskId instead
            if getattr(e, 'response', None) and e.response.status_code == 500:
                time.sleep(5)
                fallback_task_id = self.fetch_last_task_id()
                if fallback_task_id is not None:
                    logging.warning('Triggering Jira backup returned HTTP 500, falling back to lastTaskId: %s', fallback_task_id)
                    return fallback_task_id
            raise
        
        data = response.json()
        
        task_id = data.get('taskId') or data.get('task_id')
        if not task_id:
            raise RuntimeError('No taskId returned from Jira backup runbackup.')
            
        logging.info('Jira backup triggered, task ID: %s', task_id)
        return int(task_id)
        
    def wait_for_completion(self, task_id, timeout_minutes=None):
        """Wait until a Jira backup task completes.
        
        Args:
            task_id (int): Task ID to monitor
            timeout_minutes (int, optional): Maximum time to wait in minutes before timing out.
                                         If None, uses instance's configured timeout or default.
            
        Returns:
            bool: True if backup completed successfully, False otherwise
        """
        # Prioritize timeout passed to this method, then instance config, then global default
        current_timeout = timeout_minutes if timeout_minutes is not None else self.jira_backup_timeout_minutes
        current_timeout = current_timeout if current_timeout is not None else DEFAULT_TIMEOUT_MINUTES
        
        logging.info('Waiting for Jira backup to complete (task %d, timeout: %d minutes)...', task_id, current_timeout)
        endpoint = '/rest/backup/1/export/getProgress'
        url = f"{self.url.rstrip('/')}{endpoint}"
        
        start_time = datetime.now()
        timeout_delta = timedelta(minutes=current_timeout)
        
        while True:
            # Check if timeout has been exceeded
            if datetime.now() - start_time > timeout_delta:
                logging.error(f'Jira backup timed out after {current_timeout} minutes')
                return False
                
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
        
        # If no result is available yet, the file creation might still be ongoing
        # even though lastTaskId indicated completion. Wait for actual completion.
        if not result:
            logging.warning(
                'No download URL available for task %d yet. File creation may still be ongoing. '
                'Waiting for backup to complete...', task_id
            )
            
            # Wait for the backup to truly complete with a download URL available
            if not self.wait_for_completion(task_id):
                raise RuntimeError(f'Jira backup task {task_id} failed to complete or timed out while waiting for download URL.')
            
            # Retry getting the download URL after waiting for completion
            response = make_authenticated_request(
                'GET', url, self.username, self.api_token, 
                params={'taskId': task_id}
            )
            
            data = response.json()
            result = data.get('result')
            
            if not result:
                raise RuntimeError(f'No download URL found for Jira backup task {task_id} even after waiting for completion.')
        
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
        try:
            return download_file(download_url, filename, self.username, self.api_token, "Jira")
        except DownloadError as e:
            logging.error("Jira backup download failed for task %d: %s", task_id, e)
            raise RuntimeError(f"Failed to download Jira backup for task {task_id}") from e
    
    def _check_existing_task(self, task_id, now, status): # Added status parameter
        """
        Check if an existing Jira backup task (and its local file) can be reused.
        Args:
            task_id (int): The server's last task ID (which matches local task ID).
            now (datetime): Current UTC datetime.
            status (dict): Current local backup status.
        Returns:
            dict: Backup details if reusable, else empty dict.
        """
        task_info = self.fetch_task_info(task_id)
        if not task_info:
            logging.warning(f"Jira backup check: Could not fetch info for task_id {task_id} to check for reuse.")
            return {}

        if not task_info.get('result'): 
            logging.info(f"Jira backup check: Task {task_id} on server is not a completed downloadable backup according to task_info. Status: {task_info.get('status', 'N/A')}, Description: {task_info.get('description', 'N/A')}")
            return {}

        submitted_ms = task_info.get('submitted')
        if not submitted_ms:
            logging.warning(f"Jira backup check: Task {task_id} has no submission time in its info.")
            return {}
        
        server_backup_datetime = datetime.fromtimestamp(submitted_ms / 1000, tz=timezone.utc)

        local_jira_file = status.get('jira_file')
        # Condition for reuse: backup is recent (e.g., within 7 days) AND local file exists
        if local_jira_file and os.path.exists(local_jira_file) and \
           (now - server_backup_datetime) <= timedelta(days=7): 
            
            logging.info(f"Jira backup check: Conditions met for reusing existing file {local_jira_file} for task ID {task_id} (created at {server_backup_datetime}).")
            # Even if reusing, we might want to ensure the filename reflects the server_backup_datetime if it differs
            # For now, we assume the existing local_jira_file is correctly named or its name is acceptable.
            return {
                'last_jira_backup': server_backup_datetime, # Use server_backup_datetime as the authoritative time
                'jira_file': local_jira_file,
                'jira_task_id': task_id
            }
        
        # If we are not reusing an existing *file*, but the task on server is recent and complete,
        # we might still want to download it with the correct date.
        # This logic branch implies we will re-download if local file is missing or too old, even if task ID matches.
        if (now - server_backup_datetime) <= timedelta(days=7): # Still within acceptable age to download
            logging.info(f"Jira backup check: Task {task_id} (created at {server_backup_datetime}) is recent. Will attempt to download.")
            try:
                file_manager = FileManager(self.url, backup_target_directory=self.backup_target_directory)
                # Use server_backup_datetime for the filename
                filename = file_manager.prepare_backup_path("Jira", backup_datetime=server_backup_datetime)
                
                downloaded_file = self.download_backup_file(task_id, filename)
                if downloaded_file:
                    return {
                        'last_jira_backup': server_backup_datetime,
                        'jira_file': downloaded_file,
                        'jira_task_id': task_id
                    }
                else:
                    logging.warning(f"Jira backup check: Failed to download file for recent task {task_id}.")
            except Exception as e:
                logging.error(f"Jira backup check: Error downloading for recent task {task_id}: {e}", exc_info=True)
        
        logging.info(f"Jira backup check: Not reusing or re-downloading task {task_id}. Local file: '{local_jira_file}' (exists: {os.path.exists(local_jira_file if local_jira_file else '')}). Server backup time: {server_backup_datetime}. Age: {now - server_backup_datetime}.")
        return {}
    
    def _create_new_backup(self, now):
        """
        Triggers a new Jira backup, waits for completion, and downloads the file.
        Returns:
            dict: Details of the new backup if successful, else empty dict.
        """
        try:
            logging.info("Jira backup: Triggering new backup process.")
            task_id = self.trigger_backup()
            
            if not self.wait_for_completion(task_id):
                logging.error(f"Jira backup: Task {task_id} did not complete successfully or timed out.")
                return {}

            # Fetch task info to get the submitted date for the filename
            task_info = self.fetch_task_info(task_id)
            submitted_ms = task_info.get('submitted')
            if not submitted_ms:
                logging.warning(f"Jira backup: New task {task_id} completed but missing 'submitted' timestamp. Using current time for filename.")
                backup_datetime_for_filename = now # Fallback to current time
            else:
                backup_datetime_for_filename = datetime.fromtimestamp(submitted_ms / 1000, tz=timezone.utc)

            file_manager = FileManager(self.url, backup_target_directory=self.backup_target_directory)
            # Use the task's submitted datetime for the filename
            backup_filepath = file_manager.prepare_backup_path("Jira", backup_datetime=backup_datetime_for_filename)
            
            logging.info(f"Jira backup: Task {task_id} completed. Attempting download to {backup_filepath}.")
            downloaded_file = self.download_backup_file(task_id, backup_filepath)

            if not downloaded_file:
                logging.error(f"Jira backup: Failed to download file for task {task_id}.")
                return {}

            logging.info(f"Jira backup: Successfully downloaded {downloaded_file} for task {task_id}.")
            return {
                'last_jira_backup': backup_datetime_for_filename, # Use task's submitted time as backup time
                'jira_file': downloaded_file,
                'jira_task_id': task_id
            }
        except HTTPError as e: 
            logging.error(f"Jira backup: HTTPError during new backup creation: {e.response.status_code if e.response else 'N/A'} - {e.response.text if e.response else str(e)}", exc_info=True)
            return {}
        except DownloadError as e: 
            logging.error(f"Jira backup: DownloadError during new backup creation: {e}", exc_info=True)
            return {}
        except Exception as e:
            logging.error(f"Jira backup: Unexpected error during new backup creation: {e}", exc_info=True)
            return {}