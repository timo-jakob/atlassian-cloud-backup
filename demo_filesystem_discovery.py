#!/usr/bin/env python3
"""
Demonstration script for filesystem discovery functionality.

This script creates a sample backup directory structure and demonstrates
how the filesystem discovery can reconstruct the consolidated status.
"""

import os
import sys
import tempfile
import shutil
from datetime import datetime, time

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from atlassian_cloud_backup.utils.filesystem_discovery import FilesystemDiscovery
from atlassian_cloud_backup.utils.file_utils import FileManager, sanitize_folder_name


def create_sample_backup_structure(base_dir):
    """Create a sample backup directory structure for demonstration."""
    print(f"Creating sample backup structure in: {base_dir}")
    
    # Define some sample sites and their backups
    sites_and_backups = [
        {
            'url': 'https://mycompany.atlassian.net',
            'backups': [
                ('jira-backup-2024-12-15.zip', 'Jira backup content'),
                ('confluence-backup-2024-12-20.zip', 'Confluence backup content'),
                ('jira-backup-2024-11-30.zip', 'Older Jira backup')  # Should be ignored in favor of newer
            ]
        },
        {
            'url': 'https://testorg.atlassian.net',
            'backups': [
                ('jira-backup-2024-12-10.zip', 'Test org Jira backup'),
                # No Confluence backup for this site
            ]
        },
        {
            'url': 'https://dev-team.atlassian.net',
            'backups': [
                ('confluence-backup-2024-12-18.tar.gz', 'Dev team Confluence backup'),
                # No Jira backup for this site
            ]
        }
    ]
    
    for site_info in sites_and_backups:
        site_url = site_info['url']
        folder_name = sanitize_folder_name(site_url)
        site_dir = os.path.join(base_dir, folder_name)
        os.makedirs(site_dir, exist_ok=True)
        
        print(f"  Creating site folder: {folder_name} (for {site_url})")
        
        for backup_filename, content in site_info['backups']:
            backup_path = os.path.join(site_dir, backup_filename)
            with open(backup_path, 'w') as f:
                f.write(content)
            print(f"    Created backup: {backup_filename}")
    
    # Create some files that should be ignored
    ignored_files = [
        'README.txt',
        'logs/backup.log',
        'temp/temp_file.tmp'
    ]
    
    for ignored_file in ignored_files:
        file_path = os.path.join(base_dir, ignored_file)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w') as f:
            f.write('This file should be ignored')
        print(f"  Created file to ignore: {ignored_file}")


def demonstrate_filesystem_discovery(base_dir):
    """Demonstrate the filesystem discovery functionality."""
    print(f"\n{'='*60}")
    print("FILESYSTEM DISCOVERY DEMONSTRATION")
    print(f"{'='*60}")
    
    # Initialize discovery
    discovery = FilesystemDiscovery(base_dir)
    
    # Discover sites and backups
    print("\n1. Discovering sites and backups...")
    discovered_status = discovery.discover_sites_and_backups()
    
    # Display discovered sites
    print(f"\nDiscovered {len(discovered_status)} sites:")
    for site_url, site_status in discovered_status.items():
        print(f"\n  Site: {site_url}")
        
        if 'last_jira_backup' in site_status:
            print(f"    Jira backup: {site_status['last_jira_backup'].strftime('%Y-%m-%d')}")
            print(f"    Jira file: {os.path.basename(site_status['jira_file'])}")
        
        if 'last_confluence_backup' in site_status:
            print(f"    Confluence backup: {site_status['last_confluence_backup'].strftime('%Y-%m-%d')}")
            print(f"    Confluence file: {os.path.basename(site_status['confluence_file'])}")
    
    # Show statistics
    print("\n2. Backup statistics:")
    stats = discovery.get_backup_statistics(discovered_status)
    print(f"  Total sites: {stats['total_sites']}")
    print(f"  Sites with Jira: {stats['sites_with_jira']}")
    print(f"  Sites with Confluence: {stats['sites_with_confluence']}")
    print(f"  Sites with both: {stats['sites_with_both']}")
    print(f"  Total backup files: {stats['total_backup_files']}")
    
    if stats['oldest_backup'] and stats['newest_backup']:
        print(f"  Date range: {stats['oldest_backup'].strftime('%Y-%m-%d')} to {stats['newest_backup'].strftime('%Y-%m-%d')}")
    
    return discovered_status


def demonstrate_file_manager_integration(base_dir, discovered_status):
    """Demonstrate how FileManager integrates with filesystem discovery."""
    print(f"\n{'='*60}")
    print("FILE MANAGER INTEGRATION DEMONSTRATION")
    print(f"{'='*60}")
    
    # Create a FileManager instance (no consolidated status file exists yet)
    print("\n1. Creating FileManager instance...")
    file_manager = FileManager("https://newsite.atlassian.net", base_dir)
    
    # Load consolidated status - should trigger discovery
    print("\n2. Loading consolidated status (will trigger discovery)...")
    consolidated_status = file_manager.load_consolidated_status()
    
    print(f"FileManager discovered {len(consolidated_status)} sites")
    
    # Check if consolidated status file was created
    status_file = file_manager.get_consolidated_status_file()
    if os.path.exists(status_file):
        print(f"3. Consolidated status file created: {status_file}")
        
        # Show the structure of the saved file
        import json
        with open(status_file, 'r') as f:
            saved_data = json.load(f)
        
        print("\n4. Saved consolidated status structure:")
        for site_url in saved_data:
            print(f"  {site_url}:")
            for key, value in saved_data[site_url].items():
                if key.endswith('_backup'):
                    print(f"    {key}: {value}")
                elif key.endswith('_file'):
                    print(f"    {key}: {os.path.basename(value)}")
                else:
                    print(f"    {key}: {value}")
    
    return consolidated_status


def main():
    """Main demonstration function."""
    print("Atlassian Cloud Backup - Filesystem Discovery Demonstration")
    print("=" * 60)
    
    # Create temporary directory for demonstration
    temp_dir = tempfile.mkdtemp(prefix='atlassian_backup_demo_')
    
    try:
        # Create sample backup structure
        create_sample_backup_structure(temp_dir)
        
        # Demonstrate filesystem discovery
        discovered_status = demonstrate_filesystem_discovery(temp_dir)
        
        # Demonstrate FileManager integration
        consolidated_status = demonstrate_file_manager_integration(temp_dir, discovered_status)
        
        print(f"\n{'='*60}")
        print("DEMONSTRATION COMPLETE")
        print(f"{'='*60}")
        print(f"Temporary directory: {temp_dir}")
        print("You can examine the created files and directory structure.")
        print("The directory will be cleaned up when the script exits.")
        
        # Optional: Keep directory for inspection
        # Automatically clean up for demo purposes
        print("Automatically cleaning up for demonstration...")
    
    finally:
        # Clean up temporary directory
        shutil.rmtree(temp_dir, ignore_errors=True)
        print("Temporary directory cleaned up.")


if __name__ == '__main__':
    main()
