"""File management utilities for Atlassian Cloud Backup."""

import os
import json
import re
import logging
from datetime import datetime

def sanitize_folder_name(url):
    """Create a sanitized folder name from an Atlassian URL."""
    folder = re.sub(r'^https?://', '', url)
    folder = re.sub(r'[/\\:*?"<>|]', '_', folder)
    return folder.strip('_')

class FileManager:
    """Handles file operations, path management, and status tracking."""
    
    def __init__(self, url, status_file='backup_status.json', backup_target_directory=None):
        """
        Initialize with URL and status file.
        
        Args:
            url (str): Atlassian instance URL
            status_file (str): Path to status file (default: 'backup_status.json')
            backup_target_directory (str, optional): Base directory for all backups.
        """
        self.url = url
        self.base_status_file = os.getenv('STATUS_FILE', status_file)
        self.folder_name = sanitize_folder_name(url)  # Instance-specific folder name, e.g., "mycompany.atlassian.net"
        self.backup_target_directory = backup_target_directory

    def get_backup_folder(self):
        """Get the absolute folder path for backups for the specific instance and ensure it exists."""
        if self.backup_target_directory:
            # Base backup directory is specified, create instance-specific subfolder there
            instance_backup_folder = os.path.join(os.path.abspath(self.backup_target_directory), self.folder_name)
        else:
            # No base backup directory, use folder_name directly (creates in CWD relative to script execution)
            instance_backup_folder = os.path.abspath(self.folder_name)
        
        os.makedirs(instance_backup_folder, exist_ok=True)
        return instance_backup_folder
        
    def get_status_filename(self):
        """Create a status filename, located within the instance's backup folder."""
        base_status_file_name = os.path.basename(self.base_status_file) # e.g., "backup_status.json"
        instance_folder_path = self.get_backup_folder() # This is an absolute path
        return os.path.join(instance_folder_path, base_status_file_name)
    
    def load_status(self):
        """Load backup status from JSON file."""
        status_file = self.get_status_filename()
        
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
        """Save backup status to JSON file."""
        to_save = {}
        if 'last_jira_backup' in status:
            to_save['last_jira_backup'] = status['last_jira_backup'].isoformat()
            to_save['jira_task_id'] = status.get('jira_task_id')
            to_save['jira_file'] = status.get('jira_file')
        if 'last_confluence_backup' in status:
            to_save['last_confluence_backup'] = status['last_confluence_backup'].isoformat()
            to_save['confluence_file'] = status.get('confluence_file')
        
        # Use URL-specific status file
        status_file = self.get_status_filename()
        
        with open(status_file, 'w') as f:
            json.dump(to_save, f, indent=2)
        logging.info('Status file updated: %s', status_file)
        
    def prepare_backup_path(self, service_name, extension='.zip'):
        """Create folder and return the full backup file path."""
        instance_folder = self.get_backup_folder() # This is an absolute path
        
        filename = os.path.join(
            instance_folder, 
            f"{service_name.lower()}-backup-{datetime.now().strftime('%Y-%m-%d')}{extension}"
        )
        return filename