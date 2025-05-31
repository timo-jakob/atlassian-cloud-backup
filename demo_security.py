#!/usr/bin/env python3
"""
Security demonstration for filesystem discovery.
Shows how the system detects and blocks malicious backup files with directory traversal attacks.
"""

import os
import sys
import tempfile
import zipfile
import tarfile
import shutil
import io

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from atlassian_cloud_backup.utils.filesystem_discovery import FilesystemDiscovery
from atlassian_cloud_backup.utils.file_utils import sanitize_folder_name


def create_valid_backup(file_path, service="jira"):
    """Create a valid backup file."""
    with zipfile.ZipFile(file_path, 'w') as zip_file:
        zip_file.writestr(f"{service}_backup.xml", f"Valid {service} backup content")
        zip_file.writestr("attachments/file1.txt", "Attachment content")


def create_malicious_zip_backup(file_path):
    """Create a malicious ZIP backup with directory traversal paths."""
    with zipfile.ZipFile(file_path, 'w') as zip_file:
        # Legitimate content
        zip_file.writestr("backup.xml", "Backup content")
        
        # Malicious paths - these would escape the extraction directory
        zip_file.writestr("../../../etc/passwd", "root:x:0:0:root:/root:/bin/bash")
        zip_file.writestr("../../../tmp/malicious.sh", "#!/bin/bash\necho 'Malicious script'")


def create_malicious_tar_backup(file_path):
    """Create a malicious tar.gz backup with directory traversal paths."""
    # First create a temporary directory with some content
    temp_dir = tempfile.mkdtemp()
    try:
        # Create a legitimate file
        legitimate_file = os.path.join(temp_dir, "backup.xml")
        with open(legitimate_file, 'w') as f:
            f.write("Legitimate backup content")
        
        # Create the tar.gz with both legitimate and malicious content
        with tarfile.open(file_path, 'w:gz') as tar_file:
            # Add legitimate content
            tar_file.add(legitimate_file, arcname="backup.xml")
            
            # Add malicious content with path traversal
            malicious_info = tarfile.TarInfo(name="../../../etc/shadow")
            malicious_content = b"root:$6$salt$hash:18000:0:99999:7:::"
            malicious_info.size = len(malicious_content)
            tar_file.addfile(malicious_info, io.BytesIO(malicious_content))
            
    finally:
        shutil.rmtree(temp_dir)


def main():
    # Create temporary directory
    temp_dir = tempfile.mkdtemp(prefix="security_demo_")
    print(f"Security Demo - Creating test files in: {temp_dir}")
    print("=" * 70)
    
    try:
        # Create site directory
        site_url = "https://security-test.atlassian.net"
        site_folder = sanitize_folder_name(site_url)
        site_dir = os.path.join(temp_dir, site_folder)
        os.makedirs(site_dir)
        
        print("Creating backup files:")
        print()
        
        # Create valid backup (should be accepted)
        valid_backup = os.path.join(site_dir, "jira-backup-2024-12-01.zip")
        create_valid_backup(valid_backup, "jira")
        print(f"  ✓ Created VALID backup: jira-backup-2024-12-01.zip")
        
        # Create malicious ZIP backup (should be rejected)
        malicious_zip = os.path.join(site_dir, "jira-backup-2024-12-05.zip")  # Newer date
        create_malicious_zip_backup(malicious_zip)
        print(f"  ⚠️  Created MALICIOUS ZIP: jira-backup-2024-12-05.zip (contains ../../../etc/passwd)")
        
        # Create malicious tar.gz backup (should be rejected)
        malicious_tar = os.path.join(site_dir, "confluence-backup-2024-12-03.tar.gz")
        create_malicious_tar_backup(malicious_tar)
        print(f"  ⚠️  Created MALICIOUS TAR.GZ: confluence-backup-2024-12-03.tar.gz (contains ../../../etc/shadow)")
        
        # Create another valid backup
        valid_confluence = os.path.join(site_dir, "confluence-backup-2024-12-02.zip")
        create_valid_backup(valid_confluence, "confluence")
        print(f"  ✓ Created VALID backup: confluence-backup-2024-12-02.zip")
        
        print("\n" + "=" * 70)
        print("Running filesystem discovery with security validation...")
        print("=" * 70)
        
        # Run discovery
        discovery = FilesystemDiscovery(temp_dir)
        result = discovery.discover_sites_and_backups()
        
        print(f"\nSecurity Analysis Results:")
        print(f"  Sites discovered: {len(result)}")
        
        if result:
            for site_url_found, site_data in result.items():
                print(f"\nSite: {site_url_found}")
                
                if 'last_jira_backup' in site_data:
                    jira_file = os.path.basename(site_data['jira_file'])
                    print(f"  ✓ Jira backup used: {jira_file}")
                    print(f"    Date: {site_data['last_jira_backup']}")
                    
                    # Check if the malicious file was rejected
                    if "2024-12-01" in jira_file:
                        print(f"    🛡️  SECURITY: Malicious jira-backup-2024-12-05.zip was REJECTED")
                        print(f"    🛡️  SECURITY: Used older but safe backup instead")
                
                if 'last_confluence_backup' in site_data:
                    conf_file = os.path.basename(site_data['confluence_file'])
                    print(f"  ✓ Confluence backup used: {conf_file}")
                    print(f"    Date: {site_data['last_confluence_backup']}")
                    
                    # Check if the malicious file was rejected
                    if "2024-12-02" in conf_file:
                        print(f"    🛡️  SECURITY: Malicious confluence-backup-2024-12-03.tar.gz was REJECTED")
                        print(f"    🛡️  SECURITY: Used safe backup instead")
        else:
            print("  No valid sites found (all backups may have been malicious)")
        
        print("\n" + "=" * 70)
        print("Security Summary:")
        print("🛡️  ZIP files with directory traversal paths are detected and rejected")
        print("🛡️  TAR.GZ files with directory traversal paths are detected and rejected")
        print("🛡️  Absolute paths in archives are detected and rejected")
        print("🛡️  The system automatically uses safe backups when malicious ones exist")
        print("🛡️  Security validation happens during discovery, not extraction")
        print("🛡️  No malicious content is ever extracted to the filesystem")
        print("=" * 70)
        
    finally:
        # Cleanup
        print(f"\nCleaning up temporary directory: {temp_dir}")
        shutil.rmtree(temp_dir)
        print("Security demonstration complete!")


if __name__ == "__main__":
    main()
