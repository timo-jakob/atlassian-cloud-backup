#!/usr/bin/env python3
"""Demo script to showcase the audit logging functionality."""

import sys
import os
import tempfile
import logging

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from atlassian_cloud_backup.utils.audit_utils import AuditLogger

# Configure logging to show audit messages
logging.basicConfig(level=logging.INFO, format='%(message)s')

def demo_audit_logging():
    """Demonstrate various audit logging scenarios."""
    print("=== Audit Logging Demonstration ===\n")
    
    # Create a temporary backup file for realistic file size
    with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as tmp:
        tmp.write(b'X' * 1048576)  # 1MB of data
        backup_file = tmp.name
    
    try:
        # Scenario 1: Successful Jira backup
        print("1. Successful Jira backup:")
        AuditLogger.log(
            service_name="Jira",
            site_url="https://example.atlassian.net",
            status="SUCCESS",
            filename=backup_file,
            filesize=1048576
        )
        
        # Scenario 2: Skipped Confluence backup (recent backup exists)
        print("\n2. Skipped Confluence backup:")
        AuditLogger.log(
            service_name="Confluence",
            site_url="https://example.atlassian.net",
            status="SKIPPED",
            filename="/tmp/old-backup.zip",
            filesize=2097152,  # 2MB
            reason="Recent backup exists"
        )
        
        # Scenario 3: Failed backup (no file created)
        print("\n3. Failed backup attempt:")
        AuditLogger.log(
            service_name="Jira",
            site_url="https://test.atlassian.net",
            status="FAILED",
            reason="Connection timeout"
        )
        
        # Scenario 4: Large backup file
        print("\n4. Large Confluence backup:")
        AuditLogger.log(
            service_name="Confluence",
            site_url="https://large-org.atlassian.net",
            status="SUCCESS",
            filename="/backups/confluence-export-2025-05-25.zip",
            filesize=5368709120  # 5GB
        )
        
    finally:
        # Clean up
        os.unlink(backup_file)

if __name__ == "__main__":
    demo_audit_logging()
