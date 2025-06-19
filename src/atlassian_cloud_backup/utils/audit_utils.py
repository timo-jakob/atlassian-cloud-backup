"""Audit logging utilities for Atlassian Cloud Backup."""

import os
import logging
from datetime import datetime, timezone

class AuditLogger:
    """Handles audit logging for backup operations."""
    
    @staticmethod
    def log(service_name, site_url, status, filename=None, filesize=None, reason=None, audit_log_path=None):
        """Log a single audit entry for a backup operation.
        
        Args:
            service_name (str): Name of the service (e.g., "Jira", "Confluence")
            site_url (str): URL of the Atlassian site
            status (str): Status of the operation (SUCCESS, SKIPPED, FAILED)
            filename (str, optional): Path to the backup file
            filesize (int, optional): Size of the backup file in bytes
            reason (str, optional): Reason for failure or skip
            audit_log_path (str, optional): Path to the audit log file. If None, logs to standard logger.
        """
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        
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
        
        # Write to audit file if path is provided, otherwise use standard logger
        if audit_log_path:
            AuditLogger._write_to_audit_file(audit_log_path, audit_message)
        else:
            logging.info(audit_message)
    
    @staticmethod
    def _write_to_audit_file(audit_log_path, message):
        """Write audit message to the specified audit log file.
        
        Args:
            audit_log_path (str): Path to the audit log file
            message (str): Audit message to write
        """
        try:
            # Ensure the directory exists
            audit_dir = os.path.dirname(audit_log_path)
            os.makedirs(audit_dir, exist_ok=True)
            
            # Append to the audit log file
            with open(audit_log_path, 'a', encoding='utf-8') as f:
                f.write(message + '\n')
                
        except Exception as e:
            # Fallback to standard logger if file writing fails
            logging.error(f"Failed to write to audit log file {audit_log_path}: {e}")
            logging.info(message)
    
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
