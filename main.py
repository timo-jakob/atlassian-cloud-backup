#!/usr/bin/env python3
"""
CLI tool to backup Atlassian Cloud instances (Jira & Confluence).
"""

import os
import sys
import logging
import click
from datetime import datetime
import configparser # Added import
from pathlib import Path # Added import

from atlassian_cloud_backup import BackupController

# Configure logging to stdout
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stdout
)

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
    """
    config = configparser.ConfigParser()
    properties_file_path = Path.home() / ".atlassian-cloud-backup" / "backup.properties"
    
    if properties_file_path.exists():
        config.read(properties_file_path)
        logging.info(f"Loaded configuration from {properties_file_path}")
    else:
        logging.info(f"Properties file not found at {properties_file_path}, using environment variables or defaults.")

    # Helper function to get config value
    def get_config_value(env_var, prop_key, default=None):
        value = os.getenv(env_var)
        if value:
            return value
        if 'atlassian' in config and prop_key in config['atlassian']:
            return config['atlassian'][prop_key]
        return default

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
        
    username = get_config_value('ATLASSIAN_USERNAME', 'username')
    api_token = get_config_value('ATLASSIAN_API_TOKEN', 'api_token')
    poll_interval_str = get_config_value('POLL_INTERVAL_SECONDS', 'poll_interval_seconds', '30')
    poll_interval = int(poll_interval_str)
    backup_target_directory = get_config_value('BACKUP_TARGET_DIRECTORY', 'backup_target_directory')
    
    if not all([username, api_token]):
        logging.error(
            'Missing ATLASSIAN_USERNAME/username or ATLASSIAN_API_TOKEN/api_token in environment variables or properties file.'
        )
        sys.exit(1)
    
    logging.info('Will process %d Atlassian instances: %s', len(urls), ', '.join(urls))
    
    success_count = 0
    for url in urls:
        try:
            logging.info('Starting backup for Atlassian instance: %s', url)
            controller = BackupController(
                url=url, 
                username=username, 
                api_token=api_token,
                poll_interval=poll_interval,
                backup_target_directory=backup_target_directory
            )
            controller.orchestrate()
            success_count += 1
            logging.info('Completed backup for %s', url)
        except Exception as e:
            logging.error('Failed to backup %s: %s', url, str(e))
            
    logging.info('Backup completed for %d of %d Atlassian instances', success_count, len(urls))

if __name__ == '__main__':
    main()