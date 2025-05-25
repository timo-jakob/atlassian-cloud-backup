"""Main controller for Atlassian Cloud backup operations."""

import os
import logging
from datetime import datetime, timezone

from atlassian_cloud_backup.jira.client import JiraClient
from atlassian_cloud_backup.confluence.client import ConfluenceClient
from atlassian_cloud_backup.utils.file_utils import FileManager
from atlassian_cloud_backup.utils.audit_utils import AuditLogger

class BackupController:
    """Controller for orchestrating backups of Atlassian Cloud instances."""
    
    def __init__(self, url, username, api_token, poll_interval=30, backup_target_directory=None, jira_backup_timeout_minutes=None):
        """
        Initialize backup controller with credentials.
        
        Args:
            url (str): Atlassian instance URL
            username (str): Username for authentication
            api_token (str): API token for authentication
            poll_interval (int): Seconds to wait between polling requests
            backup_target_directory (str, optional): Base directory for backups.
            jira_backup_timeout_minutes (int, optional): Timeout in minutes for Jira backup.
        """
        # Store provided credentials and parameters
        self.url = url
        self.username = username
        self.api_token = api_token
        self.poll_interval = poll_interval
        self.backup_target_directory = backup_target_directory
        self.jira_backup_timeout_minutes = jira_backup_timeout_minutes
        
        # Log the URL being used
        logging.info('Using Atlassian Cloud URL: %s', self.url)
        
        # Initialize components
        self.jira_client = JiraClient(url, username, api_token, poll_interval, self.backup_target_directory, self.jira_backup_timeout_minutes)
        self.confluence_client = ConfluenceClient(url, username, api_token, poll_interval, True, self.backup_target_directory)
        self.file_manager = FileManager(url, backup_target_directory=self.backup_target_directory)

        # Log the target directory for backups
        logging.info('Backup target directory: %s', self.file_manager.get_backup_folder())
        # Log the Jira backup timeout
        if self.jira_backup_timeout_minutes is not None:
            logging.info('Jira backup timeout: %d minutes', self.jira_backup_timeout_minutes)
        else:
            default_timeout = 480  # Default timeout value in minutes
            logging.info('Jira backup timeout: Using default value of %d minutes', default_timeout)
        
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

        # Process Jira backup (errors should not stop Confluence)
        jira_action = None
        try:
            jira_updated = self.jira_client.process_backup(status, now)
            updated.update(jira_updated)
            jira_action = jira_updated.get('jira_action')
        except Exception as e:
            logging.error('Jira backup failed: %s', e)
            self._log_jira_audit('FAILED', None, None, str(e))

        # Log Jira audit entry if action was determined
        if jira_action:
            jira_file = updated.get('jira_file')
            if jira_action == 'REUSED_EXISTING':
                self._log_jira_audit('SKIPPED', jira_file, self._get_file_size(jira_file), 'Reused existing backup')
            elif jira_action == 'CREATED_NEW':
                self._log_jira_audit('SUCCESS', jira_file, self._get_file_size(jira_file))

        # Process Confluence backup
        confluence_action = None
        try:
            confluence_updated = self.confluence_client.process_backup(status, now)
            updated.update(confluence_updated)
            confluence_action = confluence_updated.get('confluence_action')
        except Exception as e:
            logging.error('Confluence backup failed: %s', e)
            self._log_confluence_audit('FAILED', None, None, str(e))

        # Log Confluence audit entry if action was determined
        if confluence_action:
            confluence_file = updated.get('confluence_file')
            if confluence_action in ('SKIPPED_RECENT', 'SKIPPED_UNAVAILABLE'):
                reason = 'Recent backup exists' if confluence_action == 'SKIPPED_RECENT' else 'Service unavailable'
                self._log_confluence_audit('SKIPPED', confluence_file, self._get_file_size(confluence_file), reason)
            elif confluence_action == 'REUSED_EXISTING':
                self._log_confluence_audit('SKIPPED', confluence_file, self._get_file_size(confluence_file), 'Reused existing backup')
            elif confluence_action == 'CREATED_NEW':
                self._log_confluence_audit('SUCCESS', confluence_file, self._get_file_size(confluence_file))

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

    def _log_jira_audit(self, status, filename=None, filesize=None, reason=None):
        """Log audit entry for Jira backup operation."""
        AuditLogger.log('Jira', self.url, status, filename, filesize, reason)
    
    def _log_confluence_audit(self, status, filename=None, filesize=None, reason=None):
        """Log audit entry for Confluence backup operation."""
        AuditLogger.log('Confluence', self.url, status, filename, filesize, reason)
    
    def _get_file_size(self, filename):
        """Get file size in bytes if file exists."""
        if filename and os.path.exists(filename):
            return os.path.getsize(filename)
        return None