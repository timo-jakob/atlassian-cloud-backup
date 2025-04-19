#!/usr/bin/env python3
import os
import sys
import json
import time
from datetime import datetime, timedelta, timezone
import logging
import click
import requests
from requests.auth import HTTPBasicAuth
from atlassian import Jira

# Configure logging to stdout
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stdout
)

# Constants
STATUS_FILE = os.getenv('STATUS_FILE', 'backup_status.json')
POLL_INTERVAL = int(os.getenv('POLL_INTERVAL_SECONDS', 30))  # seconds
CONFLUENCE_ATTACHMENT_BACKUP = os.getenv('CB_ATTACHMENTS', 'true').lower() == 'true'
LOG_CHUNK_SIZE = 100 * 1024 * 1024  # 100 MB
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S %Z'  # Common datetime format for display

class BackupController:
    def __init__(self, url, username, api_token):
        # Store provided credentials
        self.url = url
        self.username = username
        self.api_token = api_token
        
        # Log the URL being used
        logging.info('Using Atlassian Cloud URL: %s', self.url)
        logging.info('Connecting to Jira instance at %s', self.url)
        self.jira = Jira(url=self.url, username=self.username, password=self.api_token)

    def load_status(self):
        # Create a URL-specific status file
        status_file = self._get_status_filename()
        
        if not os.path.isfile(status_file):
            return {}
        
        with open(status_file, 'r') as f:
            data = json.load(f)
        
        for key in ['last_jira_backup', 'last_confluence_backup']:
            if key in data:
                try:
                    data[key] = datetime.fromisoformat(data[key])
                except ValueError:
                    logging.warning('Invalid datetime format in status for %s: %s', key, data[key])
        return data

    def save_status(self, status):
        to_save = {}
        if 'last_jira_backup' in status:
            to_save['last_jira_backup'] = status['last_jira_backup'].isoformat()
            to_save['jira_task_id'] = status.get('jira_task_id')
            to_save['jira_file'] = status.get('jira_file')
        if 'last_confluence_backup' in status:
            to_save['last_confluence_backup'] = status['last_confluence_backup'].isoformat()
            to_save['confluence_file'] = status.get('confluence_file')
        
        # Use URL-specific status file
        status_file = self._get_status_filename()
        
        with open(status_file, 'w') as f:
            json.dump(to_save, f, indent=2)
        logging.info('Status file updated: %s', status_file)

    def _get_status_filename(self):
        """Create a URL-specific status filename."""
        # Use the folder name based on URL as part of the status filename
        folder_name = self._get_folder_name()
        
        # Get the base status filename from environment or use default
        base_status_file = os.path.basename(STATUS_FILE)
        
        # Create URL-specific status file by inserting folder name before extension
        name, ext = os.path.splitext(base_status_file)
        return f"{name}_{folder_name}{ext}"

    def should_backup(self, last_time):
        return (datetime.now(timezone.utc) - last_time) > timedelta(hours=24)

    # --- Jira methods ---
    def fetch_last_jira_task_id(self):
        logging.info('Fetching last Jira backup task ID from server')
        url = f"{self.url.rstrip('/')}/rest/backup/1/export/lastTaskId"
        r = requests.get(url, auth=HTTPBasicAuth(self.username, self.api_token))
        r.raise_for_status()
        
        # Handle empty response
        response_text = r.text.strip()
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

    def fetch_jira_task_info(self, task_id):
        logging.info('Fetching Jira task info for ID %d', task_id)
        try:
            return self.jira.get(f'/rest/api/3/task/{task_id}')
        except Exception:
            url = f"{self.url.rstrip('/')}/rest/api/3/task/{task_id}"
            r = requests.get(url, auth=HTTPBasicAuth(self.username, self.api_token))
            r.raise_for_status()
            return r.json()

    def trigger_jira_backup(self):
        logging.info('Triggering Jira backup via POST /rest/backup/1/export/runbackup')
        url = f"{self.url.rstrip('/')}/rest/backup/1/export/runbackup"
        headers = {'Content-Type': 'application/json'}
        payload = {"cbAttachments": "true", "exportToCloud": "true"}
        r = requests.post(url, auth=HTTPBasicAuth(self.username, self.api_token), headers=headers, json=payload)
        r.raise_for_status()
        data = r.json()
        task_id = data.get('taskId') or data.get('task_id')
        if not task_id:
            raise RuntimeError('No taskId returned from Jira backup runbackup.')
        logging.info('Jira backup triggered, task ID: %s', task_id)
        return int(task_id)

    def wait_for_jira_completion(self, task_id):
        logging.info('Waiting for Jira backup to complete (task %d)...', task_id)
        endpoint = '/rest/backup/1/export/getProgress'
        url = f"{self.url.rstrip('/')}{endpoint}"
        while True:
            r = requests.get(url, auth=HTTPBasicAuth(self.username, self.api_token), params={'taskId': task_id})
            r.raise_for_status()
            resp = r.json()
            percent = resp.get('progress', 0)
            status = resp.get('status', '').upper()
            logging.info('Progress: %s%%, status: %s', percent, status)
            if status in ('COMPLETE', 'DONE', 'SUCCESSFUL') or percent == 100:
                logging.info('Jira backup in the Atlassian Cloud completed.')
                return True
            if status in ('FAILED', 'ERROR'):
                logging.error('Jira backup in the Atlassian Cloud failed with status: %s', status)
                return False
            time.sleep(POLL_INTERVAL)

    def download_jira_file(self, task_id):
        """Download Jira backup file for the given task ID."""
        download_url = self._get_jira_download_url(task_id)
        filename = self._prepare_jira_backup_path()
        
        return self._download_file(download_url, filename, "Jira")

    def _get_jira_download_url(self, task_id):
        """Get the download URL for a Jira backup task."""
        logging.info('Retrieving download URL for Jira backup task %d', task_id)
        endpoint = '/rest/backup/1/export/getProgress'
        url = f"{self.url.rstrip('/')}{endpoint}"
        r = requests.get(url, auth=HTTPBasicAuth(self.username, self.api_token), params={'taskId': task_id})
        r.raise_for_status()
        
        data = r.json()
        result = data.get('result')
        if not result:
            raise RuntimeError('No result found in Jira backup response.')
        
        download_url = f"{self.url.rstrip('/')}/plugins/servlet/{result}"
        logging.info('Found Jira backup download URL: %s', download_url)
        return download_url

    def _prepare_jira_backup_path(self):
        """Create folder and return the full backup file path."""
        folder_name = self._get_folder_name()
        os.makedirs(folder_name, exist_ok=True)
        
        filename = os.path.join(
            folder_name, 
            f"jira-backup-{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.zip"
        )
        return filename

    def _download_file(self, url, filename, service_name):
        """Generic file download with progress tracking."""
        r = requests.get(url, auth=HTTPBasicAuth(self.username, self.api_token), stream=True)
        r.raise_for_status()

        bytes_downloaded = 0
        next_log_threshold = LOG_CHUNK_SIZE
        start_time = time.time()
        last_log_time = start_time
        
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                if not chunk:
                    continue
                    
                f.write(chunk)
                bytes_downloaded += len(chunk)
                current_time = time.time()
                
                if bytes_downloaded >= next_log_threshold:
                    self._log_download_progress(
                        service_name, 
                        bytes_downloaded, 
                        current_time, 
                        start_time, 
                        last_log_time
                    )
                    next_log_threshold += LOG_CHUNK_SIZE
                    last_log_time = current_time
        
        self._log_download_complete(service_name, filename, bytes_downloaded, start_time)
        return filename

    def _log_download_progress(self, service_name, bytes_downloaded, current_time, start_time, last_log_time):
        """Log download progress with speed metrics."""
        mb = bytes_downloaded / (1024 * 1024)
        elapsed = current_time - start_time
        speed = mb / elapsed if elapsed > 0 else 0
        
        # Calculate recent speed (since last log)
        recent_elapsed = current_time - last_log_time
        recent_bytes = LOG_CHUNK_SIZE / (1024 * 1024)  # Convert to MB
        recent_speed = recent_bytes / recent_elapsed if recent_elapsed > 0 else 0
        
        logging.info('Downloaded %.2f MB of %s backup (%.2f MB/s, current: %.2f MB/s)...', 
                    mb, service_name, speed, recent_speed)

    def _log_download_complete(self, service_name, filename, bytes_downloaded, start_time):
        """Log completion of download with final statistics."""
        total_elapsed = time.time() - start_time
        total_mb = bytes_downloaded / (1024 * 1024)
        avg_speed = total_mb / total_elapsed if total_elapsed > 0 else 0
        logging.info('Downloaded %s backup to %s (%.2f MB in %.1f seconds, avg: %.2f MB/s)', 
                    service_name, filename, total_mb, total_elapsed, avg_speed)

    def _get_folder_name(self):
        """Create a sanitized folder name from the Atlassian URL."""
        # Remove protocol prefix and any special characters
        import re
        folder = re.sub(r'^https?://', '', self.url)
        folder = re.sub(r'[/\\:*?"<>|]', '_', folder)
        return folder.strip('_')

    # --- Confluence Cloud methods ---
    def trigger_confluence_backup(self):
        logging.info('Triggering Confluence backup...')
        endpoint = f"{self.url.rstrip('/')}/wiki/rest/obm/1.0/runbackup"
        logging.info('Confluence backup endpoint: %s', endpoint)
        headers = {
            'Content-Type': 'application/json',
            'X-Atlassian-Token': 'no-check',
            'X-Requested-With': 'XMLHttpRequest'
        }
        payload = {'cbAttachments': CONFLUENCE_ATTACHMENT_BACKUP}
        resp = requests.post(endpoint, auth=(self.username, self.api_token), headers=headers, json=payload)
        resp.raise_for_status()
        logging.info('Confluence backup triggered.')

    def wait_for_confluence_file(self):
        logging.info('Waiting for Confluence backup file...')
        
        # Create folder based on URL
        folder_name = self._get_folder_name()
        os.makedirs(folder_name, exist_ok=True)
        
        # Get the download URL from the progress API
        url = f"{self.url.rstrip('/')}/wiki/rest/obm/1.0/getprogress.json"
        
        while True:
            r = requests.get(url, auth=(self.username, self.api_token))
            r.raise_for_status()
            data = r.json()
            
            status = data.get('currentStatus', '')
            
            if status == 'COMPLETE':
                # Get the filename from the API response
                remote_filename = data.get('filename')
                if not remote_filename:
                    logging.error("No filename found in Confluence backup response")
                    return None
                
                logging.info(f"Found Confluence backup filename: {remote_filename}")
                download_url = f"{self.url.rstrip('/')}/{remote_filename}"
                
                # Local filename (with folder)
                local_filename = os.path.join(folder_name, os.path.basename(remote_filename))
                
                logging.info('Downloading Confluence backup from: %s', download_url)
                dl = requests.get(download_url, auth=(self.username, self.api_token), stream=True)
                dl.raise_for_status()
                
                bytes_downloaded = 0
                next_log_threshold = LOG_CHUNK_SIZE
                with open(local_filename, 'wb') as f:
                    for chunk in dl.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            bytes_downloaded += len(chunk)
                            if bytes_downloaded >= next_log_threshold:
                                mb = bytes_downloaded / (1024 * 1024)
                                logging.info('Downloaded %.2f MB of Confluence backup so far...', mb)
                                next_log_threshold += LOG_CHUNK_SIZE
                
                logging.info('Downloaded Confluence backup to %s', local_filename)
                return local_filename
            
            elif status in ('FAILED', 'ERROR'):
                logging.error('Confluence backup failed with status: %s', status)
                return None
            
            logging.info('Backup not yet complete (status: %s), waiting...', status)
            time.sleep(POLL_INTERVAL)

    def get_confluence_backup_status(self):
        """Check if a Confluence backup exists and get its status"""
        logging.info('Checking Confluence backup status')
        url = f"{self.url.rstrip('/')}/wiki/rest/obm/1.0/getprogress.json"
        try:
            r = requests.get(url, auth=(self.username, self.api_token))
            r.raise_for_status()
            if r.status_code == 204:
                logging.info('Confluence appears to be unavailable or unlicensed for this instance. Skipping Confluence backup.')
                return None
            return r.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code in (401, 403, 404):
                logging.info('Confluence appears to be unavailable or unlicensed for this instance. Skipping Confluence backup.')
                return None
            else:
                # For other HTTP errors, still raise the exception
                logging.error('Error checking Confluence status: %s', str(e))
                raise

    def wait_for_confluence_completion(self):
        """Poll until Confluence backup is complete"""
        logging.info('Monitoring Confluence backup progress...')
        url = f"{self.url.rstrip('/')}/wiki/rest/obm/1.0/getprogress.json"
        
        while True:
            r = requests.get(url, auth=(self.username, self.api_token))
            r.raise_for_status()
            data = r.json()
            
            status = data.get('currentStatus', '')
            progress = data.get('alternativePercentage', 0)
            logging.info('Confluence backup progress: %s%%, status: %s', progress, status)
            
            if status == 'COMPLETE':
                logging.info('Confluence backup completed.')
                return True
            elif status in ('FAILED', 'ERROR'):
                logging.error('Confluence backup failed with status: %s', status)
                return False
                
            time.sleep(POLL_INTERVAL)

    def orchestrate(self):
        """Main controller method to coordinate backup operations."""
        status = self.load_status()
        now = datetime.now(timezone.utc)
        updated = {}

        # Log last backup times in local timezone
        self._log_last_backup_times(status)

        # Process Jira backup
        jira_updated = self._handle_jira_backup(status, now)
        updated.update(jira_updated)

        # Process Confluence backup
        confluence_updated = self._handle_confluence_backup()
        updated.update(confluence_updated)

        # Save updates
        if updated:
            merged = {**status, **updated}
            self.save_status(merged)

    def _log_last_backup_times(self, status):
        """Log the last backup times in local timezone."""
        last_jira = status.get('last_jira_backup')
        if last_jira:
            local_jira = last_jira.astimezone()
            logging.info('Last Jira backup was at %s (local time)', 
                         local_jira.strftime(DATETIME_FORMAT))
        
        last_conf = status.get('last_confluence_backup')
        if last_conf:
            local_conf = last_conf.astimezone()
            logging.info('Last Confluence backup was at %s (local time)', 
                         local_conf.strftime(DATETIME_FORMAT))

    def _handle_jira_backup(self, status, now):
        """Handle Jira backup process and return updated status."""
        updated = {}
        
        # Fetch and compare Jira task IDs
        server_task_id = self.fetch_last_jira_task_id()
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
            updated.update(self._check_existing_jira_task(server_task_id, now))
            
        # Create new backup if needed
        if not updated:
            updated.update(self._create_new_jira_backup(now))
            
        return updated

    def _check_existing_jira_task(self, task_id, now):
        """Check if existing Jira task can be used and process it."""
        updated = {}
        task_info = self.fetch_jira_task_info(task_id)
        submitted_ms = task_info.get('submitted')
        
        if not submitted_ms:
            logging.error('Missing "submitted" timestamp in Jira task %d response: %s', task_id, task_info)
            sys.exit(1)
            
        # Convert milliseconds timestamp to datetime
        created = datetime.fromtimestamp(submitted_ms / 1000, tz=timezone.utc)
        created_str = created.astimezone().strftime(DATETIME_FORMAT)
        
        if now - created <= timedelta(hours=24):
            logging.info('Reusing Jira task %d from %s (local time %s)', 
                         task_id, created_str, created.astimezone().strftime(DATETIME_FORMAT))
            if self.wait_for_jira_completion(task_id):
                filename = self.download_jira_file(task_id)
                updated['last_jira_backup'] = created
                updated['jira_task_id'] = task_id
                updated['jira_file'] = filename
        else:
            logging.info('Existing Jira task %d is older than 24 h (%s), triggering new.', 
                         task_id, created_str)
                         
        return updated

    def _create_new_jira_backup(self, now):
        """Create a new Jira backup and return updated status."""
        updated = {}
        new_task_id = self.trigger_jira_backup()
        if self.wait_for_jira_completion(new_task_id):
            filename = self.download_jira_file(new_task_id)
            updated['last_jira_backup'] = now
            updated['jira_task_id'] = new_task_id
            updated['jira_file'] = filename
        return updated

    def _handle_confluence_backup(self):
        """Handle Confluence backup process and return updated status."""
        updated = {}
        conf_status = self.get_confluence_backup_status()

        if conf_status is None:
            logging.info('Skipping Confluence backup for %s', self.url)
            return updated
        
        if self._can_use_existing_confluence_backup(conf_status, now):
            return self._use_existing_confluence_backup(conf_status)
        
        return self._create_new_confluence_backup(now)

    def _can_use_existing_confluence_backup(self, conf_status, now):
        """Check if an existing Confluence backup can be used."""
        conf_time = conf_status.get('time')
        if not conf_time:
            return False

        try:
            conf_timestamp = datetime.fromtimestamp(conf_time / 1000, tz=timezone.utc)
            one_week_ago = now - timedelta(days=7)
            is_outdated = conf_status.get('isOutdated', True)
            
            return not is_outdated and conf_timestamp > one_week_ago and conf_status.get('currentStatus') == 'COMPLETE'
        except (ValueError, TypeError):
            logging.warning('Invalid timestamp in Confluence backup status: %s', conf_time)
            return False

    def _use_existing_confluence_backup(self, conf_status):
        """Use and download an existing Confluence backup."""
        updated = {}
        conf_time = conf_status.get('time')
        conf_timestamp = datetime.fromtimestamp(conf_time / 1000, tz=timezone.utc)
        
        logging.info('Using existing Confluence backup from %s', 
                    conf_timestamp.astimezone().strftime(DATETIME_FORMAT))
        
        conf_file = self.wait_for_confluence_file()
        if conf_file:
            updated['last_confluence_backup'] = conf_timestamp
            updated['confluence_file'] = conf_file
            logging.info('Downloaded existing Confluence backup')
            
        return updated

    def _create_new_confluence_backup(self, now):
        """Create a new Confluence backup and return updated status."""
        updated = {}
        logging.info('Creating new Confluence backup')
        self.trigger_confluence_backup()
        if self.wait_for_confluence_completion():
            conf_file = self.wait_for_confluence_file()
            if conf_file:
                updated['last_confluence_backup'] = now
                updated['confluence_file'] = conf_file
        return updated

@click.command()
def main():
    """
    CLI tool to backup Atlassian Cloud instances (Jira & Confluence).
    
    Environment variables:
    - ATLASSIAN_INSTANCES: Comma-separated list of instance names (e.g., "company1,company2")
      Script will convert these to https://<name>.atlassian.net URLs
    - ATLASSIAN_USERNAME: Username for Atlassian authentication
    - ATLASSIAN_API_TOKEN: API token for Atlassian authentication
    """
    # Get instance names from environment
    instance_names = os.getenv('ATLASSIAN_INSTANCES', '')
    
    # Process instance names into standard Atlassian URLs
    urls = []
    if instance_names:
        names = [name.strip() for name in instance_names.split(',') if name.strip()]
        urls = [f"https://{name}.atlassian.net" for name in names]
    
    if not urls:
        logging.error('No valid Atlassian instances provided. Set ATLASSIAN_INSTANCES environment variable.')
        sys.exit(1)
        
    username = os.getenv('ATLASSIAN_USERNAME')
    api_token = os.getenv('ATLASSIAN_API_TOKEN')
    
    if not all([username, api_token]):
        logging.error(
            'Missing one of ATLASSIAN_USERNAME or ATLASSIAN_API_TOKEN environment variables.'
        )
        sys.exit(1)
    
    logging.info('Will process %d Atlassian instances: %s', len(urls), ', '.join(urls))
    
    success_count = 0
    for url in urls:
        try:
            logging.info('Starting backup for Atlassian instance: %s', url)
            controller = BackupController(url=url, username=username, api_token=api_token)
            controller.orchestrate()
            success_count += 1
            logging.info('Completed backup for %s', url)
        except Exception as e:
            logging.error('Failed to backup %s: %s', url, str(e))
            
    logging.info('Backup completed for %d of %d Atlassian instances', success_count, len(urls))

if __name__ == '__main__':
    main()
