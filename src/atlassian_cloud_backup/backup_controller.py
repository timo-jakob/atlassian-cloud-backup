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
        
        Always attempts fresh backups without considering previous status or local files.
        
        Returns:
            bool: True if at least one backup was performed
        """
        now = datetime.now(timezone.utc)
        updated = {}

        logging.info('Starting fresh backup process for %s', self.url)

        # Process Jira backup (always fresh)
        jira_updates = self._process_jira_backup({}, now)
        updated.update(jira_updates)

        # Process Confluence backup (always fresh)
        confluence_updates = self._process_confluence_backup({}, now)
        updated.update(confluence_updates)

        # Save updates if any changes were made
        if updated:
            # Update consolidated status file
            self.file_manager.update_site_in_consolidated_status(updated)
            return True
        
        return False
    
    def _process_jira_backup(self, status, now):
        """
        Process Jira backup and handle audit logging.
        
        Args:
            status: Current backup status
            now: Current timestamp
            
        Returns:
            dict: Updates from Jira backup processing
        """
        try:
            jira_updated = self.jira_client.process_backup(status, now)
            self._handle_jira_audit_logging(jira_updated)
            return jira_updated
        except Exception as e:
            logging.error('Jira backup failed: %s', e)
            self._log_jira_audit('FAILED', None, None, str(e))
            return {}
    
    def _process_confluence_backup(self, status, now):
        """
        Process Confluence backup and handle audit logging.
        
        Args:
            status: Current backup status
            now: Current timestamp
            
        Returns:
            dict: Updates from Confluence backup processing
        """
        try:
            confluence_updated = self.confluence_client.process_backup(status, now)
            self._handle_confluence_audit_logging(confluence_updated)
            return confluence_updated
        except Exception as e:
            logging.error('Confluence backup failed: %s', e)
            self._log_confluence_audit('FAILED', None, None, str(e))
            return {}
    
    def _handle_jira_audit_logging(self, jira_updates):
        """
        Handle audit logging for Jira backup actions.
        
        Args:
            jira_updates: Dictionary containing Jira backup updates
        """
        jira_action = jira_updates.get('jira_action')
        if not jira_action:
            return
            
        jira_file = jira_updates.get('jira_file')
        
        if jira_action == 'CREATED_NEW':
            self._log_jira_audit('SUCCESS', jira_file, self._get_file_size(jira_file), 'New backup created')
        elif jira_action == 'REUSED_EXISTING':
            self._log_jira_audit('SUCCESS', jira_file, self._get_file_size(jira_file), 'Downloaded existing server backup (frequency limit)')
        elif jira_action == 'NO_UPDATE_NEEDED':
            self._log_jira_audit('SKIPPED', None, None, self.JIRA_SKIP_REASON_FREQUENCY_LIMIT)
        elif jira_action == 'FAILED':
            self._log_jira_audit('FAILED', None, None, 'Backup process failed')
    
    def _handle_confluence_audit_logging(self, confluence_updates):
        """
        Handle audit logging for Confluence backup actions.
        
        Args:
            confluence_updates: Dictionary containing Confluence backup updates
        """
        confluence_action = confluence_updates.get('confluence_action')
        if not confluence_action:
            return
            
        confluence_file = confluence_updates.get('confluence_file')
        
        if confluence_action == 'CREATED_NEW':
            self._log_confluence_audit('SUCCESS', confluence_file, self._get_file_size(confluence_file), 'New backup created')
        elif confluence_action == 'WAITED_FOR_EXISTING':
            self._log_confluence_audit('SUCCESS', confluence_file, self._get_file_size(confluence_file), 'Waited for existing backup in progress')
        elif confluence_action == 'SKIPPED_FREQUENCY_LIMIT':
            self._log_confluence_audit('SKIPPED', confluence_file, self._get_file_size(confluence_file), 'Backup skipped due to frequency limits')
        elif confluence_action == 'SKIPPED_NO_UPDATE_NEEDED':
            self._log_confluence_audit('SKIPPED', confluence_file, self._get_file_size(confluence_file), 'Backup skipped because newest version is less than one day old')
        elif confluence_action == 'SKIPPED_UNAVAILABLE':
            self._log_confluence_audit('SKIPPED', None, None, 'Service unavailable or unlicensed')
        elif confluence_action == 'FAILED':
            self._log_confluence_audit('FAILED', None, None, 'Backup process failed')

    def _log_jira_audit(self, status, filename=None, filesize=None, reason=None):
        """Log audit entry for Jira backup operation."""
        audit_log_path = self.file_manager.get_audit_log_path()
        AuditLogger.log('Jira', self.url, status, filename, filesize, reason, audit_log_path)
    
    def _log_confluence_audit(self, status, filename=None, filesize=None, reason=None):
        """Log audit entry for Confluence backup operation."""
        audit_log_path = self.file_manager.get_audit_log_path()
        AuditLogger.log('Confluence', self.url, status, filename, filesize, reason, audit_log_path)
    
    def _get_file_size(self, filename):
        """Get file size in bytes if file exists."""
        if filename and os.path.exists(filename):
            return os.path.getsize(filename)
        return None