#!/usr/bin/env python3
"""
Demo script to showcase the refactored _discover_site_backups functionality.

This demonstrates how the cognitive complexity has been reduced by breaking down
the site backup discovery into smaller, focused methods.
"""

import os
import sys
import tempfile
import zipfile
from datetime import datetime

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from atlassian_cloud_backup.utils.filesystem_discovery import FilesystemDiscovery

def create_demo_backup_structure():
    """Create a temporary directory structure with demo backup files."""
    temp_dir = tempfile.mkdtemp(prefix="demo_site_discovery_")
    
    # Create site directories
    site1_dir = os.path.join(temp_dir, "mycompany-atlassian-net")
    site2_dir = os.path.join(temp_dir, "testorg-atlassian-net")
    os.makedirs(site1_dir)
    os.makedirs(site2_dir)
    
    # Create backup files for site1
    create_demo_backup_file(os.path.join(site1_dir, "jira-backup-2024-01-15.zip"))
    create_demo_backup_file(os.path.join(site1_dir, "jira-backup-2024-01-20.zip"))
    create_demo_backup_file(os.path.join(site1_dir, "confluence-backup-2024-01-18.tar.gz"))
    
    # Create backup files for site2
    create_demo_backup_file(os.path.join(site2_dir, "jira-backup-2024-01-10.zip"))
    create_demo_backup_file(os.path.join(site2_dir, "confluence-backup-2024-01-12.tar.gz"))
    
    # Create some non-backup files to test filtering
    with open(os.path.join(site1_dir, "readme.txt"), 'w') as f:
        f.write("This is not a backup file")
    with open(os.path.join(site1_dir, "invalid-name.zip"), 'w') as f:
        f.write("Invalid backup filename")
    
    return temp_dir

def create_demo_backup_file(file_path):
    """Create a demo backup file (ZIP or TAR.GZ)."""
    if file_path.endswith('.zip'):
        with zipfile.ZipFile(file_path, 'w') as zf:
            zf.writestr("demo_file.txt", "This is a demo backup file")
    elif file_path.endswith('.tar.gz'):
        import tarfile
        import io
        with tarfile.open(file_path, 'w:gz') as tf:
            content = b"This is a demo backup file"
            info = tarfile.TarInfo("demo_file.txt")
            info.size = len(content)
            tf.addfile(info, io.BytesIO(content))

def demonstrate_refactored_methods():
    """Demonstrate the refactored _discover_site_backups and its helper methods."""
    print("🔧 Site Backup Discovery Refactoring Demo")
    print("=" * 50)
    
    # Create demo structure
    temp_dir = create_demo_backup_structure()
    discovery = FilesystemDiscovery()
    
    try:
        # Demonstrate the main refactored method
        site1_dir = os.path.join(temp_dir, "mycompany-atlassian-net")
        print(f"\n📁 Discovering backups in: {site1_dir}")
        
        # Show the refactored _discover_site_backups method workflow
        print("\n🔍 Step 1: Getting site files...")
        files = discovery._get_site_files(site1_dir)
        print(f"Found {len(files)} files: {files}")
        
        print("\n🔍 Step 2: Processing backup files...")
        jira_backups, confluence_backups = discovery._process_backup_files(site1_dir, files)
        print(f"Jira backups: {len(jira_backups)}")
        for backup in jira_backups:
            print(f"  - {backup['filename']} (date: {backup['date']})")
        print(f"Confluence backups: {len(confluence_backups)}")
        for backup in confluence_backups:
            print(f"  - {backup['filename']} (date: {backup['date']})")
        
        print("\n🔍 Step 3: Generating site status...")
        site_status = discovery._generate_site_status(jira_backups, confluence_backups)
        print("Site status:")
        for key, value in site_status.items():
            if isinstance(value, datetime):
                print(f"  {key}: {value.strftime('%Y-%m-%d %H:%M:%S')}")
            else:
                print(f"  {key}: {value}")
        
        print("\n📊 Complete discovery result:")
        complete_status = discovery._discover_site_backups(site1_dir)
        for key, value in complete_status.items():
            if isinstance(value, datetime):
                print(f"  {key}: {value.strftime('%Y-%m-%d %H:%M:%S')}")
            else:
                print(f"  {key}: {os.path.basename(value) if key.endswith('_file') else value}")
        
        # Demonstrate error handling
        print("\n❌ Testing error handling with non-existent directory:")
        empty_status = discovery._discover_site_backups("/nonexistent/directory")
        print(f"Result: {empty_status}")
        
    finally:
        # Clean up
        import shutil
        shutil.rmtree(temp_dir)

def show_refactoring_benefits():
    """Explain the benefits of the refactoring."""
    print("\n🎯 Refactoring Benefits")
    print("=" * 30)
    print("✅ Reduced cognitive complexity by decomposing into focused methods:")
    print("   - _get_site_files(): File listing with error handling")
    print("   - _process_backup_files(): File processing and categorization")
    print("   - _process_single_backup_file(): Individual file validation")
    print("   - _generate_site_status(): Status dictionary generation")
    print("\n✅ Improved readability and maintainability")
    print("✅ Better separation of concerns")
    print("✅ Easier to test individual components")
    print("✅ Eliminated nested conditionals")
    print("✅ Clear workflow in main method")

if __name__ == "__main__":
    demonstrate_refactored_methods()
    show_refactoring_benefits()
    print("\n✨ Demo completed successfully!")
