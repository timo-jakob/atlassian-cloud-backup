"""Jira backup functionality."""

import os
import time
import logging
import zipfile
import requests
from datetime import datetime, timedelta, timezone
from requests.exceptions import HTTPError

from atlassian import Jira
from atlassian_cloud_backup.utils.http_utils import make_authenticated_request, download_file, DownloadError
from atlassian_cloud_backup.utils.file_utils import FileManager # Ensure FileManager is imported

# Fixed timeout of 600 minutes (10 hours) for Jira backups
DEFAULT_TIMEOUT_MINUTES = 600
DATEIME_FORMAT_STR = '%Y-%m-%d %H:%M:%S %Z'
APPLICATION_JSON = 'application/json'
NEW_JIRA_TRIGGERED = 'New Jira backup triggered successfully with task ID: %d'

class JiraClient:
    """Client for handling Jira backup operations."""
    
    def __init__(self, url, username, api_token, poll_interval=30, backup_target_directory=None, backup_timeout_minutes=DEFAULT_TIMEOUT_MINUTES):
        """
        Initialize Jira client.
        
        Args:
            url (str): Jira instance URL
            username (str): Username for authentication
            api_token (str): API token for authentication
            poll_interval (int): Seconds to wait between polling requests
            backup_target_directory (str, optional): Base directory for backups.
            backup_timeout_minutes (int, optional): Timeout in minutes for Jira backup.
        """
        self.url = url
        self.username = username
        self.api_token = api_token
        self.poll_interval = poll_interval
        self.backup_target_directory = backup_target_directory # Store the directory
        self.backup_timeout_minutes = backup_timeout_minutes # Store the timeout
        
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
        logging.info('Starting Jira backup process - always attempting to trigger new backup first')
        
        try:
            result = self.trigger_backup()
            
            # Handle dictionary result with action
            if isinstance(result, dict) and 'action' in result:
                action = result['action']
                
                # Use a dispatch table to handle different actions
                action_handlers = {
                    'new_backup': lambda: self._handle_new_backup(result['task_id'], now),
                    'downloaded': lambda: self._handle_downloaded_backup(result['backup_data']),
                    'skipped': lambda: self._handle_skipped_backup(),
                    'no_server_backup': lambda: self._handle_no_server_backup(),
                    'download_failed': lambda: self._handle_download_failed()
                }
                
                # Execute the appropriate handler if action is recognized
                if action in action_handlers:
                    return action_handlers[action]()
                else:
                    logging.error('Unknown action from trigger_backup: %s', action)
                    return {}
            
            # Handle legacy integer result (task ID)
            elif isinstance(result, int):
                return self._handle_new_backup(result, now)
            
            # Handle unexpected result
            else:
                logging.error('Unexpected result from trigger_backup: %s', result)
                return {}
                
        except Exception as e:
            logging.error('Unexpected error during backup trigger: %s', str(e))
            return {}
        
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
            int or dict: Task ID of the new backup, or backup result dict if 412 handled
        """
        logging.info('Triggering Jira backup via POST /rest/backup/1/export/runbackup')
        
        # Get local task ID from existing backup files
        file_manager = FileManager(self.url, backup_target_directory=self.backup_target_directory)
        local_task_id = file_manager.get_latest_jira_task_id_from_files()
        
        try:
            # Use session-based authentication for backup trigger
            result = self._trigger_backup_with_session(local_task_id)
            return result
        except HTTPError as e:
            return self._handle_backup_trigger_error(e)
    
    def _trigger_backup_with_session(self, local_task_id=None):
        """Trigger backup using lastTaskId endpoint to get cookies.

        This method uses a different approach to get session cookies:
        1. First calls /rest/backup/1/export/lastTaskId to establish a session and get cookies
        2. Uses those cookies to trigger the backup via /rest/backup/1/export/runbackup

        Returns:
            Response: HTTP response from backup trigger or None if skipped
        """
        session = self._initialize_session()

        try:
            self._establish_session_cookies(session)
            return self._post_backup_request(session, local_task_id)
        finally:
            session.close()

    def _initialize_session(self):
        """Initialize a new session."""
        import requests
        return requests.Session()

    def _establish_session_cookies(self, session):
        """Establish session cookies by calling the lastTaskId endpoint."""
        lasttask_url = f"{self.url.rstrip('/')}/rest/backup/1/export/lastTaskId"
        logging.debug('Getting cookies from lastTaskId endpoint for backup trigger')

        try:
            lasttask_response = session.get(
                lasttask_url,
                auth=(self.username, self.api_token),
                headers={'Accept': APPLICATION_JSON},
                timeout=30
            )
            lasttask_response.raise_for_status()
            logging.debug('Successfully called lastTaskId endpoint and obtained session cookies')

            if session.cookies:
                cookie_names = [cookie.name for cookie in session.cookies]
                logging.debug('Obtained cookies: %s', ', '.join(cookie_names))
            else:
                logging.warning('No cookies obtained from lastTaskId endpoint')
        except requests.exceptions.RequestException as e:
            logging.error('Failed to get cookies from lastTaskId endpoint: %s', str(e))
            raise HTTPError(f"Failed to get session cookies: {str(e)}")

    def _post_backup_request(self, session, local_task_id):
        """Send a POST request to trigger the backup."""
        backup_url = f"{self.url.rstrip('/')}/rest/backup/1/export/runbackup"
        backup_payload = {
            "cbAttachments": "true",
            "exportToCloud": "true"
        }

        try:
            response = session.post(
                backup_url,
                json=backup_payload,
                auth=(self.username, self.api_token),
                headers={
                    'Accept': APPLICATION_JSON,
                    'Content-Type': APPLICATION_JSON
                },
                timeout=60
            )
            response.raise_for_status()
            logging.info('Jira backup triggered successfully using cookies from lastTaskId')
            task_id = self._extract_task_id_from_response(response)
            return {'action': 'new_backup', 'task_id': task_id}

        except requests.exceptions.HTTPError as e:
            result = self._handle_http_error(e, local_task_id)
            return result

        except requests.exceptions.RequestException as e:
            logging.error('Failed to trigger Jira backup with cookies: %s', str(e))
            if hasattr(e, 'response') and e.response is not None:
                raise HTTPError(f"HTTP {e.response.status_code}: {e.response.text}", response=e.response)
            else:
                raise HTTPError(f"Request failed: {str(e)}")

    def _handle_http_error(self, error, local_task_id):
        """Handle HTTP errors during the backup trigger."""
        if error.response.status_code == 412:
            logging.info('Backup trigger denied due to frequency limit (HTTP 412). Fetching lastTaskId.')
            server_task_id = self.fetch_last_task_id()

            if server_task_id is None:
                logging.warning('No task ID found on server despite frequency limit error')
                return {'action': 'no_server_backup'}

            logging.info('Server task ID: %d, Local task ID: %s', server_task_id, local_task_id)

            if local_task_id is not None and server_task_id <= local_task_id:
                logging.info('Server task ID %d is not newer than local task ID %d, skipping backup', 
                             server_task_id, local_task_id)
                return {'action': 'skipped', 'reason': 'local_current'}

            logging.info('Server has newer backup (task %d), attempting download', server_task_id)
            backup_result = self._download_existing_backup(server_task_id, datetime.now(timezone.utc))
            if backup_result:
                return {'action': 'downloaded', 'backup_data': backup_result}
            else:
                return {'action': 'download_failed'}

        raise

    def wait_for_completion(self, task_id, timeout_minutes=None):
        """Wait until a Jira backup task completes.
        
        Args:
            task_id (int): Task ID to monitor
            timeout_minutes (int, optional): Maximum time to wait in minutes before timing out.
                                         If None, uses instance's configured timeout or default.
            
        Returns:
            bool: True if backup completed successfully, False otherwise
        """
        current_timeout = self._determine_timeout(timeout_minutes)
        logging.info('Waiting for Jira backup to complete (task %d, timeout: %d minutes)...', task_id, current_timeout)
        
        monitor_context = self._initialize_monitoring_context(current_timeout)
        url = f"{self.url.rstrip('/')}/rest/backup/1/export/getProgress"
        
        while True:
            if self._is_timeout_exceeded(monitor_context):
                logging.error(f'Jira backup timed out after {current_timeout} minutes')
                return False
                
            status_result = self._check_backup_status(url, task_id)
            if status_result is not None:
                return status_result
                
            time.sleep(self.poll_interval)

    def _determine_timeout(self, timeout_minutes):
        """Determine the appropriate timeout value."""
        if timeout_minutes is not None:
            return timeout_minutes
        return self.backup_timeout_minutes

    def _initialize_monitoring_context(self, timeout_minutes):
        """Initialize context for backup monitoring."""
        return {
            'start_time': datetime.now(),
            'timeout_delta': timedelta(minutes=timeout_minutes)
        }

    def _is_timeout_exceeded(self, monitor_context):
        """Check if monitoring timeout has been exceeded."""
        return datetime.now() - monitor_context['start_time'] > monitor_context['timeout_delta']

    def _check_backup_status(self, url, task_id):
        """Check backup status and return result if final, None if should continue."""
        response = make_authenticated_request(
            'GET', url, self.username, self.api_token, 
            params={'taskId': task_id}
        )
        resp = response.json()
        
        percent = resp.get('progress', 0)
        status = resp.get('status', '').upper()
        logging.info('Progress: %s%%, status: %s', percent, status)
        
        if self._is_completed_status(status, percent):
            logging.info('Jira backup in the Atlassian Cloud completed.')
            return True
            
        if self._is_failed_status(status):
            logging.error('Jira backup in the Atlassian Cloud failed with status: %s', status)
            return False
            
        return None  # Continue monitoring

    def _is_completed_status(self, status, percent):
        """Check if status indicates completion."""
        return status in ('COMPLETE', 'DONE', 'SUCCESSFUL') or percent == 100

    def _is_failed_status(self, status):
        """Check if status indicates failure."""
        return status in ('FAILED', 'ERROR')
    
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
    
    def _wait_and_download_backup(self, task_id, now):
        """Wait for backup completion and download the file.
        
        Args:
            task_id (int): Task ID to wait for and download
            now (datetime): Current datetime
            
        Returns:
            dict: Backup details if successful, else empty dict
        """
        if not self.wait_for_completion(task_id):
            logging.error('Jira backup task %d did not complete successfully or timed out', task_id)
            return {}
        
        return self._download_existing_backup(task_id, now)
    
    def _download_existing_backup(self, task_id, now):
        """Download and verify an existing backup.
        
        Args:
            task_id (int): Task ID to download
            now (datetime): Current datetime
            
        Returns:
            dict: Backup details if successful, else empty dict
        """
        try:
            # Fetch task info to get the submitted date for the filename
            task_info = self.fetch_task_info(task_id)
            submitted_ms = task_info.get('submitted')
            if not submitted_ms:
                logging.warning('Task %d completed but missing submitted timestamp, using current time for filename', task_id)
                backup_datetime_for_filename = now
            else:
                backup_datetime_for_filename = datetime.fromtimestamp(submitted_ms / 1000, tz=timezone.utc)

            file_manager = FileManager(self.url, backup_target_directory=self.backup_target_directory)
            backup_filepath = file_manager.prepare_jira_backup_path(task_id, backup_datetime=backup_datetime_for_filename)
            
            logging.info('Downloading Jira backup for task %d to %s', task_id, backup_filepath)
            downloaded_file = self.download_backup_file(task_id, backup_filepath)

            if not downloaded_file:
                logging.error('Failed to download file for task %d', task_id)
                return {}

            # Verify the downloaded file is a valid ZIP
            if not self._verify_zip_file(downloaded_file):
                logging.error('Downloaded file %s is not a valid ZIP file', downloaded_file)
                # Clean up invalid file
                try:
                    os.remove(downloaded_file)
                    logging.info('Removed invalid file %s', downloaded_file)
                except OSError as e:
                    logging.warning('Failed to remove invalid file %s: %s', downloaded_file, e)
                return {}

            logging.info('Successfully downloaded and verified Jira backup: %s (task %d)', downloaded_file, task_id)
            return {
                'last_jira_backup': backup_datetime_for_filename,
                'jira_file': downloaded_file
            }
            
        except Exception as e:
            logging.error('Error downloading backup for task %d: %s', task_id, str(e))
            return {}
    
    def _verify_zip_file(self, filepath):
        """Verify that a file is a valid ZIP archive.
        
        Args:
            filepath (str): Path to the file to verify
            
        Returns:
            bool: True if file is a valid ZIP, False otherwise
        """
        try:
            with zipfile.ZipFile(filepath, 'r') as zip_file:
                # Test the ZIP file integrity
                bad_file = zip_file.testzip()
                if bad_file:
                    logging.error('ZIP file %s contains corrupted file: %s', filepath, bad_file)
                    return False
                
                # Check if ZIP has any files
                if len(zip_file.namelist()) == 0:
                    logging.error('ZIP file %s is empty', filepath)
                    return False
                
                logging.info('ZIP file %s verified successfully (%d files)', filepath, len(zip_file.namelist()))
                return True
                
        except zipfile.BadZipFile:
            logging.error('File %s is not a valid ZIP file', filepath)
            return False
        except Exception as e:
            logging.error('Error verifying ZIP file %s: %s', filepath, str(e))
            return False
    
    def _handle_backup_trigger_error(self, error):
        """Handle errors that occur during backup triggering.
        
        Args:
            error (HTTPError): The HTTP error that occurred
            
        Returns:
            int: Task ID if fallback is successful
            
        Raises:
            HTTPError: For HTTP errors
        """
        if not getattr(error, 'response', None):
            raise error
        
        status_code = error.response.status_code
        
        if status_code == 500:
            return self._handle_server_error()
        else:
            raise error
    
    def _handle_server_error(self):
        """Handle HTTP 500 server errors with fallback to lastTaskId.
        
        Returns:
            int: Task ID if fallback is successful
            
        Raises:
            HTTPError: If fallback fails
        """
        logging.warning('Triggering Jira backup returned HTTP 500, attempting fallback to lastTaskId')
        time.sleep(5)
        fallback_task_id = self.fetch_last_task_id()
        
        if fallback_task_id is not None:
            logging.warning('Using existing task ID %s as fallback for HTTP 500 error', fallback_task_id)
            return fallback_task_id
        else:
            logging.error('No existing task ID available for fallback after HTTP 500 error')
            raise HTTPError("500 Server Error with no fallback available")
    
    def _extract_task_id_from_response(self, response):
        """Extract and validate task ID from backup trigger response.
        
        Args:
            response: HTTP response from backup trigger
            
        Returns:
            int: The task ID
            
        Raises:
            RuntimeError: If no task ID is found in response
        """
        data = response.json()
        task_id = data.get('taskId') or data.get('task_id')
        
        if not task_id:
            raise RuntimeError('No taskId returned from Jira backup runbackup.')
        
        logging.info('Jira backup triggered, task ID: %s', task_id)
        return int(task_id)

    def _handle_new_backup(self, task_id, now):
        """Handle new backup process.
        
        Args:
            task_id: The task ID to process
            now: Current datetime
            
        Returns:
            dict: Backup information or empty dict if failed
        """
        logging.info(NEW_JIRA_TRIGGERED, task_id)
        new_backup = self._wait_and_download_backup(task_id, now)
        if new_backup:
            new_backup['jira_action'] = 'CREATED_NEW'
            return new_backup
        else:
            logging.error('Failed to complete new backup process')
            return {}
            
    def _handle_downloaded_backup(self, backup_data):
        """Handle downloaded backup data.
        
        Args:
            backup_data: The downloaded backup data
            
        Returns:
            dict: Backup information
        """
        backup_data['jira_action'] = 'REUSED_EXISTING'
        return backup_data
        
    def _handle_skipped_backup(self):
        """Handle skipped backup scenario.
        
        Returns:
            dict: Action information
        """
        logging.info("Backup skipped as local version is current.")
        return {'jira_action': 'NO_UPDATE_NEEDED'}
        
    def _handle_no_server_backup(self):
        """Handle no server backup available scenario.
        
        Returns:
            dict: Empty dict
        """
        logging.warning("No server backup available despite frequency limit.")
        return {}
        
    def _handle_download_failed(self):
        """Handle download failure scenario.
        
        Returns:
            dict: Empty dict
        """
        logging.warning("Backup download failed.")
        return {}
    
    # The process_backup method has been moved to the top of the class
    # and refactored to reduce cognitive complexity