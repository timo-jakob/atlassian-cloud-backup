#!/usr/bin/env python3
"""Test script to verify individual status files are no longer created."""

import os
import sys
import tempfile
from datetime import datetime, timezone

# Add src to path for imports
sys.path.insert(0, 'src')

from atlassian_cloud_backup.utils.file_utils import FileManager

def test_no_individual_status_files():
    """Verify that individual status files are no longer created."""
    
    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"Testing in directory: {temp_dir}")
        
        # Create FileManager instance
        site_url = "https://test.atlassian.net"
        fm = FileManager(site_url, backup_target_directory=temp_dir)
        
        # Create test status data
        now = datetime.now(timezone.utc)
        status = {
            'last_jira_backup': now,
            'jira_task_id': 'task-123',
            'jira_file': '/path/to/jira-backup.zip'
        }
        
        # Update consolidated status
        fm.update_site_in_consolidated_status(status)
        
        # Check what files exist
        print(f"Files in root backup directory:")
        for file in os.listdir(temp_dir):
            print(f"  - {file}")
        
        # Check what files exist in site-specific folder (if any)
        site_folder = os.path.join(temp_dir, "test.atlassian.net")
        if os.path.exists(site_folder):
            print(f"Files in site-specific directory ({site_folder}):")
            for file in os.listdir(site_folder):
                print(f"  - {file}")
        else:
            print("No site-specific directory created (expected)")
        
        # Verify consolidated status file exists and individual status files don't
        consolidated_file = fm.get_consolidated_status_file()
        print(f"Consolidated status file: {consolidated_file}")
        print(f"Consolidated file exists: {os.path.exists(consolidated_file)}")
        
        # Try to look for any individual status files
        individual_status_files = []
        for root, dirs, files in os.walk(temp_dir):
            for file in files:
                if file.endswith('status.json') and file != 'backup_status.json':
                    individual_status_files.append(os.path.join(root, file))
        
        if individual_status_files:
            print(f"❌ Found unexpected individual status files: {individual_status_files}")
            assert not individual_status_files, "Unexpected individual status files found."
        else:
            print("✅ No individual status files found (expected)")
        
        # Verify consolidated status can be loaded
        loaded_status = fm.load_consolidated_status()
        assert site_url in loaded_status, "Failed to load consolidated status for the site."
        print("✅ Consolidated status loaded successfully")
        print(f"   Site data keys: {list(loaded_status[site_url].keys())}")

if __name__ == "__main__":
    success = test_no_individual_status_files()
    if success:
        print("✅ All tests passed! Individual status files successfully removed.")
    else:
        print("❌ Tests failed!")
        sys.exit(1)
