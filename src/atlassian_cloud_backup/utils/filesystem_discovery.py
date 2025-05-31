"""Filesystem discovery utilities for reconstructing backup status from existing files."""

import os
import re
import logging
import zipfile
import tarfile
from datetime import datetime, time
from atlassian_cloud_backup.utils.file_utils import sanitize_folder_name


class FilesystemDiscovery:
    """Discovers existing backup files and reconstructs consolidated status."""
    
    def __init__(self, backup_target_directory=None):
        """
        Initialize filesystem discovery.
        
        Args:
            backup_target_directory (str, optional): Base directory for all backups.
                                                   If None, uses current working directory.
        """
        if backup_target_directory:
            self.root_backup_dir = os.path.abspath(backup_target_directory)
        else:
            self.root_backup_dir = os.getcwd()
    
    def discover_sites_and_backups(self):
        """
        Scan the filesystem to discover existing sites and their backups.
        
        Returns:
            dict: Consolidated status dictionary with site URLs as keys
        """
        logging.info('Starting filesystem discovery in: %s', self.root_backup_dir)
        
        if not os.path.exists(self.root_backup_dir):
            logging.warning('Backup directory does not exist: %s', self.root_backup_dir)
            return {}
        
        discovered_status = {}
        
        # Get all subdirectories in the root backup directory
        try:
            entries = os.listdir(self.root_backup_dir)
        except OSError as e:
            logging.error('Error reading backup directory %s: %s', self.root_backup_dir, e)
            return {}
        
        for entry in entries:
            entry_path = os.path.join(self.root_backup_dir, entry)
            
            # Skip files and focus on directories
            if not os.path.isdir(entry_path):
                continue
            
            # Skip common non-site directories
            if entry.startswith('.') or entry.lower() in ('logs', 'temp', 'tmp'):
                continue
            
            # Try to reconstruct the site URL from the folder name
            site_url = self._reconstruct_url_from_folder_name(entry)
            if not site_url:
                logging.debug('Could not reconstruct URL from folder name: %s', entry)
                continue
            
            # Discover backups in this site's directory
            site_status = self._discover_site_backups(entry_path)
            if site_status:
                discovered_status[site_url] = site_status
                logging.info('Discovered site: %s with %d backup files', 
                           site_url, len([k for k in site_status.keys() if k.endswith('_file')]))
        
        logging.info('Filesystem discovery completed. Found %d sites.', len(discovered_status))
        return discovered_status
    
    def _reconstruct_url_from_folder_name(self, folder_name):
        """
        Reconstruct an Atlassian URL from a sanitized folder name.
        
        This reverses the sanitize_folder_name() function by converting underscores
        back to likely URL characters and adding the https:// prefix.
        
        Args:
            folder_name (str): Sanitized folder name
            
        Returns:
            str or None: Reconstructed URL, or None if it doesn't look like an Atlassian URL
        """
        # Replace underscores with dots for common domain patterns
        # This is a heuristic - we can't perfectly reverse the sanitization
        reconstructed = folder_name.replace('_', '.')
        
        # Check if it looks like an Atlassian Cloud URL pattern
        if '.atlassian.net' in reconstructed or '.atlassian.com' in reconstructed:
            return f'https://{reconstructed}'
        
        # Handle cases where the domain might have had different characters replaced
        # Try some common patterns
        if 'atlassian' in reconstructed.lower():
            # If it contains atlassian but no .net/.com, try adding .net
            if '.net' not in reconstructed and '.com' not in reconstructed:
                # Replace the last underscore group with .net if it makes sense
                parts = reconstructed.rsplit('.', 1)
                if len(parts) == 2 and parts[1] in ('net', 'com'):
                    return f'https://{reconstructed}'
                else:
                    # Try adding .atlassian.net
                    base = reconstructed.replace('.atlassian', '').replace('atlassian', '')
                    if base and not base.startswith('.') and not base.endswith('.'):
                        return f'https://{base}.atlassian.net'
        
        # If we still can't figure it out, try a different approach
        # Look for patterns that suggest it was a URL
        if re.match(r'^[a-zA-Z0-9._-]+$', folder_name) and '.' in folder_name:
            # Assume it was a domain and add https://
            return f'https://{reconstructed}'
        
        return None
    
    def _is_path_safe(self, file_path):
        """
        Check if a file path is safe (doesn't contain directory traversal attempts).
        
        Args:
            file_path (str): File path to check
            
        Returns:
            bool: True if the path is safe, False if it contains directory traversal attempts
        """
        # Normalize the path to resolve any ".." components
        normalized_path = os.path.normpath(file_path)
        
        # Check for absolute paths (security risk)
        if os.path.isabs(normalized_path):
            return False
        
        # Check if the normalized path tries to go outside the current directory
        if normalized_path.startswith('..') or '/..' in normalized_path or '\\..\\' in normalized_path:
            return False
        
        # Check for other suspicious patterns
        if normalized_path.startswith('/') or normalized_path.startswith('\\'):
            return False
            
        return True
    
    def _validate_backup_file(self, file_path, extension):
        """
        Validate that a backup file is not corrupted by attempting to list its contents.
        Also performs security checks to ensure the archive doesn't contain malicious paths.
        
        Args:
            file_path (str): Path to the backup file
            extension (str): File extension (zip or tar.gz)
            
        Returns:
            bool: True if the file appears to be valid and safe, False if corrupted or malicious
        """
        try:
            if extension == 'zip':
                with zipfile.ZipFile(file_path, 'r') as zip_file:
                    # List the contents - this will fail if the ZIP is corrupted
                    file_names = zip_file.namelist()
                    
                    # Security check: validate file paths to prevent directory traversal
                    for file_name in file_names:
                        if self._is_path_safe(file_name):
                            continue
                        else:
                            logging.warning('Backup file contains unsafe path and will be ignored: %s (unsafe path: %s)', 
                                          file_path, file_name)
                            return False
                    
                    return True
                    
            elif extension == 'tar.gz':
                with tarfile.open(file_path, 'r:gz') as tar_file:
                    # List the contents - this will fail if the tar.gz is corrupted
                    # Note: We only list contents, never extract, so this is safe from zip-slip attacks
                    file_names = tar_file.getnames()
                    
                    # Security check: validate file paths to prevent directory traversal
                    for file_name in file_names:
                        if self._is_path_safe(file_name):
                            continue
                        else:
                            logging.warning('Backup file contains unsafe path and will be ignored: %s (unsafe path: %s)', 
                                          file_path, file_name)
                            return False
                    
                    return True
            else:
                # Unknown extension, assume it's valid for now
                logging.warning('Unknown backup file extension: %s for file %s', extension, file_path)
                return True
                
        except (zipfile.BadZipFile, tarfile.TarError, OSError, EOFError) as e:
            logging.warning('Backup file appears to be corrupted and will be ignored: %s (error: %s)', 
                          file_path, str(e))
            return False
        except Exception as e:
            # Catch any other unexpected errors
            logging.warning('Error validating backup file %s: %s (treating as corrupted)', 
                          file_path, str(e))
            return False
    
    def _discover_site_backups(self, site_dir):
        """
        Discover backup files in a site's directory.
        
        Args:
            site_dir (str): Path to the site's backup directory
            
        Returns:
            dict: Site status dictionary with backup information
        """
        site_status = {}
        
        try:
            files = os.listdir(site_dir)
        except OSError as e:
            logging.warning('Error reading site directory %s: %s', site_dir, e)
            return {}
        
        # Pattern to match backup files: {service}-backup-{date}.{extension}
        backup_pattern = re.compile(r'^(jira|confluence)-backup-(\d{4}-\d{2}-\d{2})\.(zip|tar\.gz)$', re.IGNORECASE)
        
        jira_backups = []
        confluence_backups = []
        
        for filename in files:
            file_path = os.path.join(site_dir, filename)
            
            # Skip directories
            if os.path.isdir(file_path):
                continue
            
            match = backup_pattern.match(filename)
            if match:
                service = match.group(1).lower()
                date_str = match.group(2)
                extension = match.group(3)
                
                # Validate the backup file before processing it
                if not self._validate_backup_file(file_path, extension):
                    # File is corrupted, skip it (warning already logged in validation method)
                    continue
                
                try:
                    # Parse the date and create a datetime object with 00:00:00 time
                    backup_date = datetime.strptime(date_str, '%Y-%m-%d')
                    backup_datetime = datetime.combine(backup_date.date(), time(0, 0, 0))
                    
                    backup_info = {
                        'file_path': file_path,
                        'filename': filename,
                        'date': backup_datetime,
                        'extension': extension
                    }
                    
                    if service == 'jira':
                        jira_backups.append(backup_info)
                    elif service == 'confluence':
                        confluence_backups.append(backup_info)
                        
                except ValueError as e:
                    logging.warning('Could not parse date from backup filename %s: %s', filename, e)
                    continue
        
        # Find the most recent backup for each service
        if jira_backups:
            latest_jira = max(jira_backups, key=lambda x: x['date'])
            site_status['last_jira_backup'] = latest_jira['date']
            site_status['jira_file'] = latest_jira['file_path']
            # We don't have task_id from filesystem, so omit it
            logging.debug('Found Jira backup: %s (date: %s)', 
                         latest_jira['filename'], latest_jira['date'])
        
        if confluence_backups:
            latest_confluence = max(confluence_backups, key=lambda x: x['date'])
            site_status['last_confluence_backup'] = latest_confluence['date']
            site_status['confluence_file'] = latest_confluence['file_path']
            logging.debug('Found Confluence backup: %s (date: %s)', 
                         latest_confluence['filename'], latest_confluence['date'])
        
        return site_status
    
    def verify_discovered_site(self, discovered_url, folder_name):
        """
        Verify that a discovered URL correctly maps back to the folder name.
        
        This helps validate the URL reconstruction process.
        
        Args:
            discovered_url (str): The reconstructed URL
            folder_name (str): The original folder name
            
        Returns:
            bool: True if the URL correctly maps to the folder name
        """
        if not discovered_url:
            return False
        
        # Use the same sanitization function to verify round-trip consistency
        expected_folder = sanitize_folder_name(discovered_url)
        return expected_folder == folder_name
    
    def get_backup_statistics(self, discovered_status):
        """
        Generate statistics about discovered backups.
        
        Args:
            discovered_status (dict): Discovered status dictionary
            
        Returns:
            dict: Statistics about the discovered backups
        """
        stats = {
            'total_sites': len(discovered_status),
            'sites_with_jira': 0,
            'sites_with_confluence': 0,
            'sites_with_both': 0,
            'oldest_backup': None,
            'newest_backup': None,
            'total_backup_files': 0
        }
        
        all_backup_dates = []
        
        for site_url, site_status in discovered_status.items():
            has_jira = 'last_jira_backup' in site_status
            has_confluence = 'last_confluence_backup' in site_status
            
            if has_jira:
                stats['sites_with_jira'] += 1
                stats['total_backup_files'] += 1
                all_backup_dates.append(site_status['last_jira_backup'])
            
            if has_confluence:
                stats['sites_with_confluence'] += 1
                stats['total_backup_files'] += 1
                all_backup_dates.append(site_status['last_confluence_backup'])
            
            if has_jira and has_confluence:
                stats['sites_with_both'] += 1
        
        if all_backup_dates:
            stats['oldest_backup'] = min(all_backup_dates)
            stats['newest_backup'] = max(all_backup_dates)
        
        return stats
    
    def _is_path_safe(self, file_path):
        """
        Check if a file path from an archive is safe (doesn't contain directory traversal).
        
        Args:
            file_path (str): Path to check
            
        Returns:
            bool: True if the path is safe, False if it contains potential directory traversal
        """
        # Normalize the path to resolve any '..' or '.' components
        normalized_path = os.path.normpath(file_path)
        
        # Check for absolute paths (starting with /)
        if os.path.isabs(normalized_path):
            return False
        
        # Check for path traversal patterns
        if normalized_path.startswith('../') or '/../' in normalized_path or normalized_path == '..':
            return False
        
        # Check for drive letters on Windows (like C:)
        if ':' in normalized_path and os.name == 'nt':
            return False
        
        return True
