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
    
    def get_consolidated_status_file(self):
        """Get the path to the consolidated status file in the root backup directory."""
        if self.backup_target_directory:
            root_backup_dir = os.path.abspath(self.backup_target_directory)
        else:
            # If no backup target directory, use current working directory
            root_backup_dir = os.getcwd()
        
        os.makedirs(root_backup_dir, exist_ok=True)
        return os.path.join(root_backup_dir, 'consolidated_backup_status.json')
    
    def load_consolidated_status(self):
        """Load consolidated backup status from JSON file.
        
        Returns:
            dict: Consolidated status with site URLs as top-level keys
        """
        status_file = self.get_consolidated_status_file()
        
        if not os.path.isfile(status_file):
            return {}
        
        try:
            raw_data = self._load_status_file(status_file)
            return self._convert_datetime_strings_to_objects(raw_data)
        except Exception as e:
            logging.warning('Error loading consolidated status file %s: %s', status_file, e)
            return {}
    
    def _load_status_file(self, status_file):
        """Load raw JSON data from status file.
        
        Args:
            status_file (str): Path to the status file
            
        Returns:
            dict: Raw data from JSON file
        """
        with open(status_file, 'r') as f:
            return json.load(f)
    
    def _convert_datetime_strings_to_objects(self, data):
        """Convert datetime strings back to datetime objects for all sites.
        
        Args:
            data (dict): Raw data with datetime strings
            
        Returns:
            dict: Data with datetime objects
        """
        for site_url, site_data in data.items():
            if isinstance(site_data, dict):
                self._convert_site_datetime_fields(site_url, site_data)
        return data
    
    def _convert_site_datetime_fields(self, site_url, site_data):
        """Convert datetime fields for a single site.
        
        Args:
            site_url (str): URL of the site
            site_data (dict): Site data dictionary
        """
        datetime_fields = ['last_jira_backup', 'last_confluence_backup']
        for field in datetime_fields:
            if field in site_data:
                self._convert_single_datetime_field(site_url, site_data, field)
    
    def _convert_single_datetime_field(self, site_url, site_data, field):
        """Convert a single datetime field from string to datetime object.
        
        Args:
            site_url (str): URL of the site
            site_data (dict): Site data dictionary
            field (str): Name of the datetime field
        """
        try:
            site_data[field] = datetime.fromisoformat(site_data[field])
        except ValueError:
            logging.warning('Invalid datetime format in consolidated status for %s.%s: %s', 
                          site_url, field, site_data[field])
    
    def save_consolidated_status(self, all_sites_status):
        """Save consolidated backup status to JSON file.
        
        Args:
            all_sites_status (dict): Dictionary with site URLs as keys and their status as values
        """
        status_file = self.get_consolidated_status_file()
        
        # Prepare data for JSON serialization
        to_save = {}
        for site_url, site_status in all_sites_status.items():
            site_data = {}
            if 'last_jira_backup' in site_status:
                site_data['last_jira_backup'] = site_status['last_jira_backup'].isoformat()
                site_data['jira_task_id'] = site_status.get('jira_task_id')
                site_data['jira_file'] = site_status.get('jira_file')
            if 'last_confluence_backup' in site_status:
                site_data['last_confluence_backup'] = site_status['last_confluence_backup'].isoformat()
                site_data['confluence_file'] = site_status.get('confluence_file')
            
            to_save[site_url] = site_data
        
        with open(status_file, 'w') as f:
            json.dump(to_save, f, indent=2)
        logging.info('Consolidated status file updated: %s', status_file)
    
    def update_site_in_consolidated_status(self, site_status):
        """Update status for this specific site in the consolidated status file.
        
        Args:
            site_status (dict): Status data for the current site
        """
        # Load existing consolidated status
        consolidated = self.load_consolidated_status()
        
        # Update this site's status
        consolidated[self.url] = site_status
        
        # Save back to consolidated file
        self.save_consolidated_status(consolidated)