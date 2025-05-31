#!/usr/bin/env python3
"""
Demonstration of backup file validation in filesystem discovery.
Shows how corrupted files are detected and ignored while valid files are processed.
"""

import os
import sys
import tempfile
import zipfile
import tarfile
import shutil

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from atlassian_cloud_backup.utils.filesystem_discovery import FilesystemDiscovery
from atlassian_cloud_backup.utils.file_utils import sanitize_folder_name


def create_valid_zip(file_path, content="Valid backup content"):
    """Create a valid ZIP file."""
    with zipfile.ZipFile(file_path, 'w') as zip_file:
        zip_file.writestr("backup_data.txt", content)


def create_valid_tar_gz(file_path, content="Valid tar.gz backup content"):
    """Create a valid tar.gz file."""
    temp_file = file_path + ".temp"
    with open(temp_file, 'w') as f:
        f.write(content)
    
    with tarfile.open(file_path, 'w:gz') as tar_file:
        tar_file.add(temp_file, arcname="backup_data.txt")
    
    os.remove(temp_file)


def create_corrupted_file(file_path, content="This is not a valid backup file"):
    """Create a corrupted backup file (just text content)."""
    with open(file_path, 'w') as f:
        f.write(content)


def main():
    # Create temporary directory
    temp_dir = tempfile.mkdtemp(prefix="validation_demo_")
    print(f"Validation Demo - Creating test files in: {temp_dir}")
    print("=" * 60)
    
    try:
        # Create site directory
        site_url = "https://validation-demo.atlassian.net"
        site_folder = sanitize_folder_name(site_url)
        site_dir = os.path.join(temp_dir, site_folder)
        os.makedirs(site_dir)
        
        # Create mix of valid and corrupted backup files
        print("Creating backup files:")
        
        # Valid files
        valid_jira_zip = os.path.join(site_dir, "jira-backup-2024-12-01.zip")
        create_valid_zip(valid_jira_zip, "Valid Jira backup data")
        print(f"  ✓ Created valid ZIP: jira-backup-2024-12-01.zip")
        
        valid_confluence_tar = os.path.join(site_dir, "confluence-backup-2024-12-02.tar.gz")
        create_valid_tar_gz(valid_confluence_tar, "Valid Confluence backup data")
        print(f"  ✓ Created valid tar.gz: confluence-backup-2024-12-02.tar.gz")
        
        # Corrupted files (these will be detected and ignored)
        corrupted_jira = os.path.join(site_dir, "jira-backup-2024-12-03.zip")
        create_corrupted_file(corrupted_jira, "This is not a ZIP file!")
        print(f"  ✗ Created corrupted ZIP: jira-backup-2024-12-03.zip")
        
        corrupted_confluence = os.path.join(site_dir, "confluence-backup-2024-12-04.tar.gz")
        create_corrupted_file(corrupted_confluence, "This is not a tar.gz file!")
        print(f"  ✗ Created corrupted tar.gz: confluence-backup-2024-12-04.tar.gz")
        
        # Another valid file (should be newest and selected)
        newest_jira = os.path.join(site_dir, "jira-backup-2024-12-05.zip")
        create_valid_zip(newest_jira, "Newest valid Jira backup")
        print(f"  ✓ Created newest valid ZIP: jira-backup-2024-12-05.zip")
        
        print("\n" + "=" * 60)
        print("Running filesystem discovery with validation...")
        print("=" * 60)
        
        # Run discovery
        discovery = FilesystemDiscovery(temp_dir)
        result = discovery.discover_sites_and_backups()
        
        print(f"\nDiscovery Results:")
        print(f"  Sites found: {len(result)}")
        
        if result:
            for site_url, site_data in result.items():
                print(f"\nSite: {site_url}")
                if 'last_jira_backup' in site_data:
                    print(f"  Last Jira backup: {site_data['last_jira_backup']}")
                    print(f"  Jira file: {os.path.basename(site_data['jira_file'])}")
                if 'last_confluence_backup' in site_data:
                    print(f"  Last Confluence backup: {site_data['last_confluence_backup']}")
                    print(f"  Confluence file: {os.path.basename(site_data['confluence_file'])}")
        
        # Get statistics
        stats = discovery.get_backup_statistics(result)
        print(f"\nStatistics:")
        print(f"  Total sites: {stats['total_sites']}")
        print(f"  Sites with Jira: {stats['sites_with_jira']}")
        print(f"  Sites with Confluence: {stats['sites_with_confluence']}")
        print(f"  Total backup files: {stats['total_backup_files']}")
        
        print("\n" + "=" * 60)
        print("Summary:")
        print("- Valid backup files were processed and included in results")
        print("- Corrupted files were detected, logged as warnings, and ignored")
        print("- The newest valid backup for each service was selected")
        print("- Discovery completed successfully despite corrupted files")
        print("=" * 60)
        
    finally:
        # Cleanup
        print(f"\nCleaning up temporary directory: {temp_dir}")
        shutil.rmtree(temp_dir)
        print("Validation demo complete!")


if __name__ == "__main__":
    main()
