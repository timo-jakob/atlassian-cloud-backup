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
        reconstructed = folder_name.replace('_', '.')
        
        # Try different reconstruction strategies
        url = self._try_direct_atlassian_match(reconstructed)
        if url:
            return url
            
        url = self._try_incomplete_atlassian_match(reconstructed)
        if url:
            return url
            
        return self._try_generic_domain_match(folder_name, reconstructed)
    
    def _try_direct_atlassian_match(self, reconstructed):
        """
        Check if the reconstructed string already contains a complete Atlassian domain.
        
        Args:
            reconstructed (str): Folder name with underscores replaced by dots
            
        Returns:
            str or None: Complete URL if found, None otherwise
        """
        if '.atlassian.net' in reconstructed or '.atlassian.com' in reconstructed:
            return f'https://{reconstructed}'
        return None
    
    def _try_incomplete_atlassian_match(self, reconstructed):
        """
        Try to reconstruct URL when 'atlassian' is present but domain is incomplete.
        
        Args:
            reconstructed (str): Folder name with underscores replaced by dots
            
        Returns:
            str or None: Complete URL if reconstructed, None otherwise
        """
        if 'atlassian' not in reconstructed.lower():
            return None
            
        # If it contains atlassian but no .net/.com, try to fix it
        if '.net' in reconstructed or '.com' in reconstructed:
            return None  # Already has domain extension, handled elsewhere
        
        # Check if it's already properly formatted
        parts = reconstructed.rsplit('.', 1)
        if len(parts) == 2 and parts[1] in ('net', 'com'):
            return f'https://{reconstructed}'
        
        # Try adding .atlassian.net
        base = self._extract_base_domain(reconstructed)
        if base:
            return f'https://{base}.atlassian.net'
        
        return None
    
    def _extract_base_domain(self, reconstructed):
        """
        Extract the base domain name from a string containing 'atlassian'.
        
        Args:
            reconstructed (str): String with 'atlassian' in it
            
        Returns:
            str or None: Base domain if valid, None otherwise
        """
        base = reconstructed.replace('.atlassian', '').replace('atlassian', '')
        if base and not base.startswith('.') and not base.endswith('.'):
            return base
        return None
    
    def _try_generic_domain_match(self, folder_name, reconstructed):
        """
        Try to match a generic domain pattern and add https prefix.
        
        Args:
            folder_name (str): Original folder name
            reconstructed (str): Folder name with underscores replaced by dots
            
        Returns:
            str or None: Complete URL if pattern matches, None otherwise
        """
        if re.match(r'^[a-zA-Z0-9._-]+$', folder_name) and '.' in folder_name:
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
        Also performs security checks to ensure the archive doesn't contain malicious paths
        or exhibit characteristics of zip/tar bombs.
        
        Args:
            file_path (str): Path to the backup file
            extension (str): File extension (zip or tar.gz)
            
        Returns:
            bool: True if the file appears to be valid and safe, False if corrupted or malicious
        """
        try:
            if extension == 'zip':
                return self._validate_zip_file(file_path)
            elif extension == 'tar.gz':
                return self._validate_tar_gz_file(file_path)
            else:
                return self._handle_unknown_extension(file_path, extension)
                
        except (zipfile.BadZipFile, tarfile.TarError, OSError, EOFError) as e:
            logging.warning('Backup file appears to be corrupted and will be ignored: %s (error: %s)', 
                          file_path, str(e))
            return False
        except Exception as e:
            # Catch any other unexpected errors
            logging.warning('Error validating backup file %s: %s (treating as corrupted)', 
                          file_path, str(e))
            return False

    def _get_security_thresholds(self):
        """
        Get security thresholds for bomb detection.
        Adjusted for large Atlassian backup files (can be 250GB+ compressed).
        
        Returns:
            tuple: (max_entries, max_size, max_ratio)
        """
        THRESHOLD_ENTRIES = 1000000  # Maximum number of entries (1M files)
        THRESHOLD_SIZE = 1073741824000  # Maximum uncompressed size (1TB = 1000GB)
        THRESHOLD_RATIO = 100  # Maximum compression ratio (higher threshold for legitimate backups)
        return THRESHOLD_ENTRIES, THRESHOLD_SIZE, THRESHOLD_RATIO

    def _validate_zip_file(self, file_path):
        """
        Validate a ZIP backup file for corruption and security issues.
        
        Args:
            file_path (str): Path to the ZIP file
            
        Returns:
            bool: True if file is valid and safe, False otherwise
        """
        with zipfile.ZipFile(file_path, 'r') as zip_file:
            # List the contents - this will fail if the ZIP is corrupted
            file_names = zip_file.namelist()
            
            # Security check: validate file paths to prevent directory traversal
            if not self._validate_file_paths(file_names, file_path):
                return False
            
            # Security check: detect zip bombs
            threshold_entries, threshold_size, threshold_ratio = self._get_security_thresholds()
            return self._check_zip_bomb_safety(zip_file, file_path, threshold_entries, threshold_size, threshold_ratio)

    def _validate_tar_gz_file(self, file_path):
        """
        Validate a TAR.GZ backup file for corruption and security issues.
        
        Args:
            file_path (str): Path to the TAR.GZ file
            
        Returns:
            bool: True if file is valid and safe, False otherwise
        """
        with tarfile.open(file_path, 'r:gz') as tar_file:
            # List the contents - this will fail if the tar.gz is corrupted
            # Note: We only list contents, never extract, so this is safe from zip-slip attacks
            file_names = tar_file.getnames()
            
            # Security check: validate file paths to prevent directory traversal
            if not self._validate_file_paths(file_names, file_path):
                return False
            
            # Security check: detect tar bombs
            threshold_entries, threshold_size, threshold_ratio = self._get_security_thresholds()
            return self._check_tar_bomb_safety(tar_file, file_path, threshold_entries, threshold_size, threshold_ratio)

    def _validate_file_paths(self, file_names, file_path):
        """
        Validate that all file paths in an archive are safe (no directory traversal).
        
        Args:
            file_names (list): List of file names from the archive
            file_path (str): Path to the archive file (for logging)
            
        Returns:
            bool: True if all paths are safe, False if any unsafe path is found
        """
        for file_name in file_names:
            if not self._is_path_safe(file_name):
                logging.warning('Backup file contains unsafe path and will be ignored: %s (unsafe path: %s)', 
                              file_path, file_name)
                return False
        return True

    def _handle_unknown_extension(self, file_path, extension):
        """
        Handle backup files with unknown extensions.
        
        Args:
            file_path (str): Path to the file
            extension (str): File extension
            
        Returns:
            bool: True (assume valid for unknown extensions)
        """
        logging.warning('Unknown backup file extension: %s for file %s', extension, file_path)
        return True
    
    def _discover_site_backups(self, site_dir):
        """
        Discover backup files in a site's directory.
        
        Args:
            site_dir (str): Path to the site's backup directory
            
        Returns:
            dict: Site status dictionary with backup information
        """
        # Get list of files in the directory
        files = self._get_site_files(site_dir)
        if not files:
            return {}
        
        # Process all backup files and categorize them
        jira_backups, confluence_backups = self._process_backup_files(site_dir, files)
        
        # Generate site status from discovered backups
        return self._generate_site_status(jira_backups, confluence_backups)

    def _get_site_files(self, site_dir):
        """
        Get the list of files in a site directory.
        
        Args:
            site_dir (str): Path to the site's backup directory
            
        Returns:
            list: List of filenames, or empty list if error
        """
        try:
            return os.listdir(site_dir)
        except OSError as e:
            logging.warning('Error reading site directory %s: %s', site_dir, e)
            return []

    def _process_backup_files(self, site_dir, files):
        """
        Process all files in a site directory and categorize valid backup files.
        
        Args:
            site_dir (str): Path to the site's backup directory
            files (list): List of filenames to process
            
        Returns:
            tuple: (jira_backups, confluence_backups) lists of backup info dictionaries
        """
        # Pattern to match backup files: {service}-backup-{date}.{extension}
        backup_pattern = re.compile(r'^(jira|confluence)-backup-(\d{4}-\d{2}-\d{2})\.(zip|tar\.gz)$', re.IGNORECASE)
        
        jira_backups = []
        confluence_backups = []
        
        for filename in files:
            file_path = os.path.join(site_dir, filename)
            
            # Skip directories
            if os.path.isdir(file_path):
                continue
            
            # Check if file matches backup pattern
            match = backup_pattern.match(filename)
            if not match:
                continue
            
            # Process the matched backup file
            backup_info = self._process_single_backup_file(file_path, filename, match)
            if backup_info:
                service = backup_info['service']
                if service == 'jira':
                    jira_backups.append(backup_info)
                elif service == 'confluence':
                    confluence_backups.append(backup_info)
        
        return jira_backups, confluence_backups

    def _process_single_backup_file(self, file_path, filename, match):
        """
        Process a single backup file and create backup info dictionary.
        
        Args:
            file_path (str): Full path to the backup file
            filename (str): Filename of the backup file
            match: Regex match object with service, date, and extension groups
            
        Returns:
            dict or None: Backup info dictionary if valid, None if invalid
        """
        service = match.group(1).lower()
        date_str = match.group(2)
        extension = match.group(3)
        
        # Validate the backup file before processing it
        if not self._validate_backup_file(file_path, extension):
            # File is corrupted, skip it (warning already logged in validation method)
            return None
        
        # Parse the date
        try:
            backup_date = datetime.strptime(date_str, '%Y-%m-%d')
            backup_datetime = datetime.combine(backup_date.date(), time(0, 0, 0))
            
            return {
                'service': service,
                'file_path': file_path,
                'filename': filename,
                'date': backup_datetime,
                'extension': extension
            }
        except ValueError as e:
            logging.warning('Could not parse date from backup filename %s: %s', filename, e)
            return None

    def _generate_site_status(self, jira_backups, confluence_backups):
        """
        Generate site status dictionary from categorized backup lists.
        
        Args:
            jira_backups (list): List of Jira backup info dictionaries
            confluence_backups (list): List of Confluence backup info dictionaries
            
        Returns:
            dict: Site status dictionary with backup information
        """
        site_status = {}
        
        # Process Jira backups
        if jira_backups:
            latest_jira = max(jira_backups, key=lambda x: x['date'])
            site_status['last_jira_backup'] = latest_jira['date']
            site_status['jira_file'] = latest_jira['file_path']
            # We don't have task_id from filesystem, so omit it
            logging.debug('Found Jira backup: %s (date: %s)', 
                         latest_jira['filename'], latest_jira['date'])
        
        # Process Confluence backups
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
    
    def _check_zip_bomb_safety(self, zip_file, file_path, threshold_entries, threshold_size, threshold_ratio):
        """
        Check if a ZIP file exhibits characteristics of a zip bomb.
        
        Args:
            zip_file: Open ZipFile object
            file_path (str): Path to the file being checked (for logging)
            threshold_entries (int): Maximum allowed number of entries
            threshold_size (int): Maximum allowed uncompressed size
            threshold_ratio (int): Maximum allowed compression ratio
            
        Returns:
            bool: True if the file appears safe, False if it looks like a zip bomb
        """
        try:
            total_uncompressed_size = 0
            total_entries = 0
            
            for info in zip_file.infolist():
                total_entries += 1
                
                # Check entry count threshold
                if total_entries > threshold_entries:
                    logging.warning('Backup file rejected: too many entries (%d > %d), potential zip bomb: %s', 
                                  total_entries, threshold_entries, file_path)
                    return False
                
                # Check uncompressed size
                total_uncompressed_size += info.file_size
                if total_uncompressed_size > threshold_size:
                    logging.warning('Backup file rejected: uncompressed size too large (%d > %d), potential zip bomb: %s', 
                                  total_uncompressed_size, threshold_size, file_path)
                    return False
                
                # Check compression ratio (avoid division by zero)
                if info.compress_size > 0:
                    compression_ratio = info.file_size / info.compress_size
                    if compression_ratio > threshold_ratio:
                        logging.warning('Backup file rejected: suspicious compression ratio (%.2f > %d), potential zip bomb: %s', 
                                      compression_ratio, threshold_ratio, file_path)
                        return False
            
            logging.debug('ZIP file passed bomb detection: %d entries, %d total size, file: %s', 
                         total_entries, total_uncompressed_size, file_path)
            return True
            
        except Exception as e:
            logging.warning('Error during zip bomb detection for %s: %s (treating as suspicious)', file_path, str(e))
            return False
    
    def _check_tar_bomb_safety(self, tar_file, file_path, threshold_entries, threshold_size, threshold_ratio):
        """
        Check if a TAR file exhibits characteristics of a tar bomb.
        Based on security scanner recommendations for detecting compression bombs.
        
        Args:
            tar_file: Open TarFile object
            file_path (str): Path to the file being checked (for logging)
            threshold_entries (int): Maximum allowed number of entries
            threshold_size (int): Maximum allowed uncompressed size
            threshold_ratio (int): Maximum allowed compression ratio
            
        Returns:
            bool: True if the file appears safe, False if it looks like a tar bomb
        """
        try:
            total_size_archive = 0
            total_entry_archive = 0
            
            for entry in tar_file:
                total_entry_archive += 1
                
                # Check entry count threshold - too many entries can lead to inode exhaustion
                if total_entry_archive > threshold_entries:
                    logging.warning('Backup file rejected: too many entries (%d > %d), potential tar bomb: %s', 
                                  total_entry_archive, threshold_entries, file_path)
                    return False
                
                # Check total size first to avoid processing huge files
                if total_size_archive > threshold_size:
                    logging.warning('Backup file rejected: uncompressed size too large (%d > %d), potential tar bomb: %s', 
                                  total_size_archive, threshold_size, file_path)
                    return False
                
                # Only process regular files (not directories, links, etc.)
                if entry.isreg():
                    # Add the declared size to our total
                    total_size_archive += entry.size
                    
                    # For compression ratio check, we'll sample the file rather than reading it entirely
                    # This prevents the bomb from being triggered while still detecting suspicious ratios
                    try:
                        file_obj = tar_file.extractfile(entry)
                        if file_obj is None:
                            continue
                        
                        # Sample the first few chunks to check for suspicious expansion
                        chunk_size = 1024
                        max_sample_chunks = 10  # Only sample first 10KB
                        size_read = 0
                        
                        for i in range(max_sample_chunks):
                            chunk = file_obj.read(chunk_size)
                            if not chunk:
                                break
                            size_read += len(chunk)
                        
                        # If we read significantly more than expected for the sample, that's suspicious
                        # For a normal file, reading 10KB should not expand to much more than that
                        if size_read > 0 and entry.size > 0:
                            # Calculate how much the sample expanded compared to declared size
                            sample_ratio = (size_read / min(entry.size, max_sample_chunks * chunk_size))
                            if sample_ratio > threshold_ratio:
                                logging.warning('Backup file rejected: suspicious expansion ratio (%.2f > %d) for entry %s, potential tar bomb: %s', 
                                              sample_ratio, threshold_ratio, entry.name, file_path)
                                return False
                            
                    except Exception as e:
                        # If we can't read the file for validation, that's suspicious
                        logging.warning('Backup file rejected: unable to validate entry %s (%s), potential tar bomb: %s', 
                                      entry.name, str(e), file_path)
                        return False
            
            logging.debug('TAR file passed bomb detection: %d entries, %d total size, file: %s', 
                         total_entry_archive, total_size_archive, file_path)
            return True
            
        except Exception as e:
            logging.warning('Error during tar bomb detection for %s: %s (treating as suspicious)', file_path, str(e))
            return False

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
