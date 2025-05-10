"""Main controller for Atlassian Cloud backup operations."""

import logging
from datetime import datetime, timezone

from atlassian_cloud_backup.jira.client import JiraClient
from atlassian_cloud_backup.confluence.client import ConfluenceClient
from atlassian_cloud_backup.utils.file_utils import FileManager

class BackupController:
    """Controller for orchestrating backups of Atlassian Cloud instances."""
    
    def __init__(self, url, username, api_token, poll_interval=30, backup_target_directory=None):
        """
        Initialize backup controller with credentials.
        
        Args:
            url (str): Atlassian instance URL
            username (str): Username for authentication
            api_token (str): API token for authentication
            poll_interval (int): Seconds to wait between polling requests
            backup_target_directory (str, optional): Base directory for backups.
        """
        # Store provided credentials and parameters
        self.url = url
        self.username = username
        self.api_token = api_token
        self.poll_interval = poll_interval
        self.backup_target_directory = backup_target_directory
        
        # Log the URL being used
        logging.info('Using Atlassian Cloud URL: %s', self.url)
        
        # Initialize components
        self.jira_client = JiraClient(url, username, api_token, poll_interval, self.backup_target_directory)
        self.confluence_client = ConfluenceClient(url, username, api_token, poll_interval)
        self.file_manager = FileManager(url, backup_target_directory=self.backup_target_directory)

        # Log the target directory for backups
        logging.info('Backup target directory: %s', self.file_manager.get_backup_folder())
        
    def orchestrate(self):
        """
        Main controller method to coordinate backup operations.
        
        Orchestrates Jira and Confluence backup processes, loading and saving
        status information along the way.
        
        Returns:
            bool: True if at least one backup was performed
        """
        # Load current backup status
        status = self.file_manager.load_status()
        now = datetime.now(timezone.utc)
        updated = {}

        # Log last backup times in local timezone
        self._log_last_backup_times(status)

        # Process Jira backup
        jira_updated = self.jira_client.process_backup(status, now)
        updated.update(jira_updated)

        # Process Confluence backup
        confluence_updated = self.confluence_client.process_backup(status, now)
        updated.update(confluence_updated)

        # Save updates if any changes were made
        if updated:
            merged = {**status, **updated}
            self.file_manager.save_status(merged)
            return True
        
        return False
            
    def _log_last_backup_times(self, status):
        """Log the last backup times in local timezone.
        
        Args:
            status (dict): Current backup status
        """
        datetime_format = '%Y-%m-%d %H:%M:%S %Z'
        
        # Log Jira backup time if available
        last_jira = status.get('last_jira_backup')
        if last_jira:
            local_jira = last_jira.astimezone()
            logging.info('Last Jira backup was at %s (local time)', 
                         local_jira.strftime(datetime_format))
        
        # Log Confluence backup time if available
        last_conf = status.get('last_confluence_backup')
        if last_conf:
            local_conf = last_conf.astimezone()
            logging.info('Last Confluence backup was at %s (local time)', 
                         local_conf.strftime(datetime_format))