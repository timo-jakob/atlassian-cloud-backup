#!/usr/bin/env python3
"""Test script to demonstrate consolidated backup status functionality."""

import os
import sys
import tempfile
from datetime import datetime, timezone

# Add src to path for imports
sys.path.insert(0, 'src')

from atlassian_cloud_backup.utils.file_utils import FileManager

def test_consolidated_status():
    """Test the consolidated status file functionality."""
    
    # Create a temporary directory for testing
    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"Testing consolidated status in: {temp_dir}")
        
        # Create FileManager instances for two different sites
        site1_url = "https://company1.atlassian.net"
        site2_url = "https://company2.atlassian.net"
        
        fm1 = FileManager(site1_url, backup_target_directory=temp_dir)
        fm2 = FileManager(site2_url, backup_target_directory=temp_dir)
        
        # Create test status data for site 1
        now = datetime.now(timezone.utc)
        site1_status = {
            'last_jira_backup': now,
            'jira_task_id': 'task-123',
            'jira_file': '/path/to/jira-backup.zip',
            'last_confluence_backup': now,
            'confluence_file': '/path/to/confluence-backup.zip'
        }
        
        # Create test status data for site 2
        site2_status = {
            'last_jira_backup': now,
            'jira_task_id': 'task-456',
            'jira_file': '/path/to/jira-backup-2.zip'
        }
        
        # Update consolidated status for both sites
        fm1.update_site_in_consolidated_status(site1_status)
        fm2.update_site_in_consolidated_status(site2_status)
        
        # Load and verify consolidated status
        consolidated = fm1.load_consolidated_status()
        
        print("Consolidated status structure:")
        print(f"Sites in consolidated status: {list(consolidated.keys())}")
        print(f"Site 1 data keys: {list(consolidated[site1_url].keys()) if site1_url in consolidated else 'None'}")
        print(f"Site 2 data keys: {list(consolidated[site2_url].keys()) if site2_url in consolidated else 'None'}")
        
        # Check consolidated file location
        consolidated_file = fm1.get_consolidated_status_file()
        print(f"Consolidated status file location: {consolidated_file}")
        print(f"File exists: {os.path.exists(consolidated_file)}")
        
        # Verify both sites are in the consolidated status
        assert site1_url in consolidated, f"Site 1 not found in consolidated status"
        assert site2_url in consolidated, f"Site 2 not found in consolidated status"
        
        # Verify site 1 has all expected data
        site1_data = consolidated[site1_url]
        assert 'last_jira_backup' in site1_data, "Site 1 missing Jira backup data"
        assert 'last_confluence_backup' in site1_data, "Site 1 missing Confluence backup data"
        
        # Verify site 2 has expected data (no Confluence)
        site2_data = consolidated[site2_url]
        assert 'last_jira_backup' in site2_data, "Site 2 missing Jira backup data"
        assert 'last_confluence_backup' not in site2_data, "Site 2 should not have Confluence backup data"
        
        print("✅ All tests passed! Consolidated status functionality working correctly.")

if __name__ == '__main__':
    test_consolidated_status()
