"""Audit logging utilities for Atlassian Cloud Backup."""

import os
import logging
from datetime import datetime

class AuditLogger:
    """Handles audit logging for backup operations."""
    
    @staticmethod
    def log(service_name, site_url, status, filename=None, filesize=None, reason=None):
        """Log a single audit entry for a backup operation.
        
        Args:
            service_name (str): Name of the service (e.g., "Jira", "Confluence")
            site_url (str): URL of the Atlassian site
            status (str): Status of the operation (SUCCESS, SKIPPED, FAILED)
            filename (str, optional): Path to the backup file
            filesize (int, optional): Size of the backup file in bytes
            reason (str, optional): Reason for failure or skip
        """
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Format filesize as human-readable
        filesize_str = AuditLogger._format_filesize(filesize) if filesize is not None else "N/A"
        
        # Extract just the filename from full path
        file_basename = os.path.basename(filename) if filename else "N/A"
        
        # Build the audit log entry
        log_parts = [
            f"AUDIT: {timestamp}",
            f"Site: {site_url}",
            f"Service: {service_name}",
            f"Status: {status}",
            f"Filename: {file_basename}",
            f"Filesize: {filesize_str}"
        ]
        
        if reason:
            log_parts.append(f"Reason: {reason}")
        
        audit_message = " | ".join(log_parts)
        logging.info(audit_message)
    
    @staticmethod
    def _format_filesize(size_bytes):
        """Format file size in human-readable format.
        
        Args:
            size_bytes (int): Size in bytes
            
        Returns:
            str: Human-readable size (e.g., "15.2 MB")
        """
        if size_bytes == 0:
            return "0 B"
        
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                if unit == 'B':
                    return f"{int(size_bytes)} {unit}"
                else:
                    return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        
        return f"{size_bytes:.1f} PB"
