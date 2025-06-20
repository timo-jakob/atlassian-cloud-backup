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
    
    def __init__(self, url, backup_target_directory=None):
        """
        Initialize with URL and backup target directory.
        
        Args:
            url (str): Atlassian instance URL
            backup_target_directory (str, optional): Base directory for all backups.
        """
        self.url = url
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
        
    def prepare_backup_path(self, service_name, extension='.zip', backup_datetime=None):
        """Create folder and return the full backup file path.
        
        Args:
            service_name (str): Name of the service (e.g., "Jira", "Confluence").
            extension (str): File extension for the backup file.
            backup_datetime (datetime, optional): Specific datetime to use for the filename.
                                                 If None, current datetime is used.
        """
        instance_folder = self.get_backup_folder() # This is an absolute path
        
        date_str = (backup_datetime or datetime.now()).strftime('%Y-%m-%d')
        
        filename = os.path.join(
            instance_folder, 
            f"{service_name.lower()}-backup-{date_str}{extension}"
        )
        return filename
    
    def prepare_jira_backup_path(self, task_id, extension='.zip', backup_datetime=None):
        """Create folder and return the full Jira backup file path with task ID prefix.
        
        Args:
            task_id (int): Jira task ID to include in filename
            extension (str): File extension for the backup file.
            backup_datetime (datetime, optional): Specific datetime to use for the filename.
                                                 If None, current datetime is used.
        """
        instance_folder = self.get_backup_folder() # This is an absolute path
        
        date_str = (backup_datetime or datetime.now()).strftime('%Y-%m-%d')
        
        filename = os.path.join(
            instance_folder, 
            f"{task_id}-jira-backup-{date_str}{extension}"
        )
        return filename
    
    def get_latest_jira_task_id_from_files(self):
        """Extract the latest task ID from existing Jira backup files.
        
        Returns:
            int or None: Latest task ID found in filenames, or None if no files exist
        """
        instance_folder = self.get_backup_folder()
        
        if not os.path.exists(instance_folder):
            return None
        
        max_task_id = None
        jira_pattern = re.compile(r'^(\d+)-jira-backup-.*\.zip$')
        
        try:
            for filename in os.listdir(instance_folder):
                match = jira_pattern.match(filename)
                if match:
                    task_id = int(match.group(1))
                    if max_task_id is None or task_id > max_task_id:
                        max_task_id = task_id
        except (OSError, ValueError) as e:
            logging.warning('Error reading backup directory %s: %s', instance_folder, e)
            return None
        
        return max_task_id
    
    # Methods related to backup_status.json removed - no longer used
    
    def get_audit_log_path(self):
        """Get the path to the audit log file in the target backup directory.
        
        Returns:
            str: Full path to the audit log file
        """
        if self.backup_target_directory:
            # Use the specified backup target directory
            audit_dir = os.path.abspath(self.backup_target_directory)
        else:
            # If no backup target directory, use current working directory
            audit_dir = os.getcwd()
        
        os.makedirs(audit_dir, exist_ok=True)
        return os.path.join(audit_dir, 'atlassian.backup.audit.log')
    
    def find_latest_confluence_backup_file(self):
        """Find the latest Confluence backup file based on filename pattern.
        
        Returns:
            str or None: Path to latest Confluence backup file, or None if none exists
        """
        instance_folder = self.get_backup_folder()
        
        if not os.path.exists(instance_folder):
            return None
        
        latest_file = None
        latest_date = None
        confluence_pattern = re.compile(r'^confluence-backup-(\d{4}-\d{2}-\d{2})\.zip$')
        
        try:
            for filename in os.listdir(instance_folder):
                match = confluence_pattern.match(filename)
                if match:
                    date_str = match.group(1)
                    try:
                        file_date = datetime.strptime(date_str, '%Y-%m-%d')
                        if latest_date is None or file_date > latest_date:
                            latest_date = file_date
                            latest_file = os.path.join(instance_folder, filename)
                    except ValueError:
                        logging.warning('Invalid date format in filename: %s', filename)
                        continue
        except OSError as e:
            logging.warning('Error reading backup directory %s: %s', instance_folder, e)
            return None
        
        return latest_file

    def extract_date_from_confluence_filename(self, filepath):
        """Extract the backup date from a Confluence backup filename.
        
        Args:
            filepath (str): Path to the Confluence backup file
            
        Returns:
            datetime or None: Date from the filename, or None if parsing fails
        """
        if not filepath:
            return None
            
        basename = os.path.basename(filepath)
        match = re.match(r'^confluence-backup-(\d{4}-\d{2}-\d{2})\.zip$', basename)
        
        if not match:
            return None
            
        try:
            date_str = match.group(1)
            # Always return a naive datetime (without timezone info)
            return datetime.strptime(date_str, '%Y-%m-%d').replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
        except ValueError:
            return None
    
    def is_confluence_backup_needed(self, now=None):
        """Check if a new Confluence backup is needed based on the date of the latest backup file.
        
        Args:
            now (datetime, optional): Current datetime. If None, current datetime is used.
            
        Returns:
            tuple: (bool, str or None) - (True if backup needed, latest backup file path or None)
        """
        if now is None:
            # Create a naive datetime (no timezone info)
            now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            # Ensure the datetime is naive by removing timezone info if present
            if now.tzinfo is not None:
                # Convert to naive by removing timezone info
                now = now.replace(tzinfo=None)
            # Normalize to start of day
            now = now.replace(hour=0, minute=0, second=0, microsecond=0)
            
        latest_backup_file = self.find_latest_confluence_backup_file()
        
        if not latest_backup_file:
            # No backup file exists
            return True, None
            
        backup_date = self.extract_date_from_confluence_filename(latest_backup_file)
        
        if not backup_date:
            # Couldn't parse date from filename
            return True, latest_backup_file
            
        # Ensure backup_date is also naive (it should be, but just to be safe)
        if backup_date.tzinfo is not None:
            backup_date = backup_date.replace(tzinfo=None)
            
        # Check if the backup date is at least 1 day older than the current date
        backup_needed = (now - backup_date).days >= 1
        
        return backup_needed, latest_backup_file