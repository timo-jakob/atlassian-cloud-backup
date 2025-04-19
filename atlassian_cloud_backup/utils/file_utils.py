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
    
    def __init__(self, url, status_file='backup_status.json'):
        """
        Initialize with URL and status file.
        
        Args:
            url (str): Atlassian instance URL
            status_file (str): Path to status file (default: 'backup_status.json')
        """
        self.url = url
        self.base_status_file = os.getenv('STATUS_FILE', status_file)
        self.folder_name = sanitize_folder_name(url)
    
    def get_status_filename(self):
        """Create a URL-specific status filename."""
        base_status_file = os.path.basename(self.base_status_file)
        name, ext = os.path.splitext(base_status_file)
        return f"{name}_{self.folder_name}{ext}"
    
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
        
    def get_backup_folder(self):
        """Get the folder path for backups and ensure it exists."""
        folder_name = self.folder_name
        os.makedirs(folder_name, exist_ok=True)
        return folder_name
        
    def prepare_backup_path(self, service_name, extension='.zip'):
        """Create folder and return the full backup file path."""
        folder_name = self.get_backup_folder()
        
        filename = os.path.join(
            folder_name, 
            f"{service_name.lower()}-backup-{datetime.now().strftime('%Y-%m-%d')}{extension}"
        )
        return filename