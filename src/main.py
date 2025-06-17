#!/usr/bin/env python3
"""
CLI tool to backup Atlassian Cloud instances (Jira & Confluence).
"""

import os
import sys
import logging
import click
from datetime import datetime
import configparser  # Added import
from pathlib import Path  # Added import

from atlassian_cloud_backup import BackupController
from atlassian_cloud_backup.utils.file_utils import FileManager

# Configure logging to stdout
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stdout
)

# Module-level config object to be initialized in main
config = configparser.ConfigParser()

# Module-level helper to retrieve configuration values
def get_config_value(env_var, prop_key, default=None):
    """Get a config value from environment or properties file."""
    value = os.getenv(env_var)
    if value:
        return value
    if 'atlassian' in config and prop_key in config['atlassian']:
        return config['atlassian'][prop_key]
    return default

def validate_credentials(username, api_token):
    """Validate that required credentials are provided."""
    if not all([username, api_token]):
        logging.error(
            'Missing ATLASSIAN_USERNAME/username or ATLASSIAN_API_TOKEN/api_token in environment variables or properties file.'
        )
        return False
    return True

def process_single_backup(url, username, api_token, poll_interval, backup_target_directory, jira_backup_timeout_minutes):
    """Process backup for a single Atlassian instance.
    
    Returns:
        bool: True if backup was successful, False otherwise
    """
    try:
        logging.info('Starting backup for Atlassian instance: %s', url)
        controller = BackupController(
            url=url,
            username=username,
            api_token=api_token,
            poll_interval=poll_interval,
            backup_target_directory=backup_target_directory,
            jira_backup_timeout_minutes=jira_backup_timeout_minutes
        )
        controller.orchestrate()
        logging.info('Completed backup for %s', url)
        return True
    except Exception as e:
        logging.error('Failed to backup %s: %s', url, str(e))
        return False

def get_runtime_configuration():
    """Get runtime configuration values.
    
    Returns:
        dict: Configuration dictionary with parsed values
    """
    return {
        'username': get_config_value('ATLASSIAN_USERNAME', 'username'),
        'api_token': get_config_value('ATLASSIAN_API_TOKEN', 'api_token'),
        'poll_interval': int(get_config_value('POLL_INTERVAL_SECONDS', 'poll_interval_seconds', '30')),
        'backup_target_directory': get_config_value('BACKUP_TARGET_DIRECTORY', 'backup_target_directory'),
        'jira_backup_timeout_minutes': int(get_config_value('JIRA_BACKUP_TIMEOUT_MINUTES', 'jira_backup_timeout_minutes', '480'))
    }

@click.command()
def main():
    """
    CLI tool to backup Atlassian Cloud instances (Jira & Confluence).
    
    Configuration can be provided via environment variables or a properties file
    located at ~/.atlassian-cloud-backup/backup.properties.
    Environment variables take precedence.

    Environment variables / Properties file keys:
    - ATLASSIAN_INSTANCES / instances: Comma-separated list of instance names (e.g., "company1,company2")
      Script will convert these to https://<name>.atlassian.net URLs
    - ATLASSIAN_USERNAME / username: Username for Atlassian authentication
    - ATLASSIAN_API_TOKEN / api_token: API token for Atlassian authentication
    - POLL_INTERVAL_SECONDS / poll_interval_seconds: Optional, seconds to wait between API polling requests (default: 30)
    - BACKUP_TARGET_DIRECTORY / backup_target_directory: Optional, the base directory where backup files will be stored.
      If not provided, backups will be stored in subdirectories named after the instance URL in the current working directory.
    - JIRA_BACKUP_TIMEOUT_MINUTES / jira_backup_timeout_minutes: Optional, timeout in minutes for Jira backup (default: 480)
    """
    # Initialize properties file
    properties_file_path = Path.home() / ".atlassian-cloud-backup" / "backup.properties"
    
    if properties_file_path.exists():
        config.read(properties_file_path)
        logging.info(f"Loaded configuration from {properties_file_path}")
    else:
        logging.info(f"Properties file not found at {properties_file_path}, using environment variables or defaults.")

    # Get instance names
    instance_names = get_config_value('ATLASSIAN_INSTANCES', 'instances', '')
    
    # Process instance names into standard Atlassian URLs
    urls = []
    if instance_names:
        names = [name.strip() for name in instance_names.split(',') if name.strip()]
        urls = [f"https://{name}.atlassian.net" for name in names]
    
    if not urls:
        logging.error('No valid Atlassian instances provided. Set ATLASSIAN_INSTANCES environment variable or "instances" in properties file.')
        sys.exit(1)
        
    # Get runtime configuration values
    runtime_config = get_runtime_configuration()
    username = runtime_config['username']
    api_token = runtime_config['api_token']
    poll_interval = runtime_config['poll_interval']
    backup_target_directory = runtime_config['backup_target_directory']
    jira_backup_timeout_minutes = runtime_config['jira_backup_timeout_minutes']

    # Validate credentials
    if not validate_credentials(username, api_token):
        sys.exit(1)
    
    logging.info('Will process %d Atlassian instances: %s', len(urls), ', '.join(urls))
    
    # Log information about the consolidated status file
    temp_fm = FileManager(urls[0], backup_target_directory=backup_target_directory)
    consolidated_status_file = temp_fm.get_consolidated_status_file()
    logging.info('Backup status will be saved to: %s', consolidated_status_file)
    
    success_count = 0
    for url in urls:
        # Process backup for each URL
        if process_single_backup(url, username, api_token, poll_interval, backup_target_directory, jira_backup_timeout_minutes):
            success_count += 1
            
    logging.info('Backup completed for %d of %d Atlassian instances', success_count, len(urls))

if __name__ == '__main__':
    main()