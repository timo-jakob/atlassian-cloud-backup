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
import select
import termios

import sys
import os

# Add the current directory to the path so imports work correctly
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from atlassian_cloud_backup.backup_controller import BackupController
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

def prompt_for_config(prompt, current_value=None, default=None):
    """Helper to prompt for a configuration value with a timeout."""
    if current_value:
        prompt_text = f"{prompt} [{current_value}]: "
    elif default:
        prompt_text = f"{prompt} [{default}]: "
    else:
        prompt_text = f"{prompt}: "
    
    sys.stdout.write(prompt_text)
    sys.stdout.flush()
    
    ready, _, _ = select.select([sys.stdin], [], [], 3600) # 1 hour timeout
    
    if ready:
        value = sys.stdin.readline().strip()
    else:
        logging.error("Timeout waiting for user input. Exiting.")
        termios.tcflush(sys.stdin.fileno(), termios.TCIFLUSH)
        sys.exit(1)

    if not value:
        return current_value or default
    return value

def handle_mandatory_field(key, env_var):
    """Handle a single mandatory configuration field."""
    current_value = get_config_value(env_var, key)
    if not current_value:
        if not sys.stdin.isatty():
            logging.error(f"Mandatory configuration '{key}' is missing and the application is running in a non-interactive mode. Exiting.")
            sys.exit(1)
        new_value = prompt_for_config(f"Enter {key.replace('_', ' ')}")
        config['atlassian'][key] = new_value
        return True
    return False

def handle_optional_field(key, env_var, default):
    """Handle a single optional configuration field."""
    current_value = get_config_value(env_var, key)
    if not sys.stdin.isatty():
        if not current_value:
            config['atlassian'][key] = default
            return True
    else:
        new_value = prompt_for_config(f"Enter {key.replace('_', ' ')}", current_value, default)
        if new_value != config.get('atlassian', key, fallback=None):
            config['atlassian'][key] = new_value
            return True
    return False

def ensure_configuration(properties_file_path):
    """Ensure all necessary configurations are set, prompting the user if needed."""
    if properties_file_path.exists():
        config.read(properties_file_path)
        logging.info(f"Loaded configuration from {properties_file_path}")

    if 'atlassian' not in config:
        config['atlassian'] = {}

    mandatory_fields = {
        'username': 'ATLASSIAN_USERNAME',
        'api_token': 'ATLASSIAN_API_TOKEN',
        'backup_target_directory': 'BACKUP_TARGET_DIRECTORY'
    }
    optional_fields = {
        'instances': ('ATLASSIAN_INSTANCES', ''),
        'poll_interval_seconds': ('POLL_INTERVAL_SECONDS', '30')
    }

    # Check if all mandatory fields are present
    all_mandatory_present = all(get_config_value(env_var, key) for key, env_var in mandatory_fields.items())

    # Only prompt for configuration if mandatory fields are missing
    if not all_mandatory_present:
        print("Welcome to the Atlassian Cloud Backup tool!")
        logging.info("Some mandatory configuration is missing. Let's configure it.")

        config_changed = False
        for key, env_var in mandatory_fields.items():
            if handle_mandatory_field(key, env_var):
                config_changed = True
        
        for key, (env_var, default) in optional_fields.items():
            if handle_optional_field(key, env_var, default):
                config_changed = True

        if config_changed:
            properties_file_path.parent.mkdir(exist_ok=True)
            with open(properties_file_path, 'w') as configfile:
                config.write(configfile)
            logging.info(f"Configuration saved to {properties_file_path}")

def validate_credentials(username, api_token, backup_target_directory):
    """Validate that required credentials are provided."""
    if not all([username, api_token, backup_target_directory]):
        logging.error(
            'Missing ATLASSIAN_USERNAME/username, ATLASSIAN_API_TOKEN/api_token, or BACKUP_TARGET_DIRECTORY/backup_target_directory in environment variables or properties file.'
        )
        return False
    return True

def process_single_backup(url, username, api_token, poll_interval, backup_target_directory):
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
            backup_target_directory=backup_target_directory
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
        'backup_target_directory': get_config_value('BACKUP_TARGET_DIRECTORY', 'backup_target_directory')
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
    """
    # Initialize properties file
    properties_file_path = Path.home() / ".atlassian-cloud-backup" / "backup.properties"
    properties_file_path.parent.mkdir(exist_ok=True)  # Ensure the directory exists

    ensure_configuration(properties_file_path)

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

    # Validate credentials
    if not validate_credentials(username, api_token, backup_target_directory):
        sys.exit(1)
    
    logging.info('Will process %d Atlassian instances: %s', len(urls), ', '.join(urls))
    
    # Log information about the audit log
    temp_fm = FileManager(urls[0], backup_target_directory=backup_target_directory)
    audit_log_file = temp_fm.get_audit_log_path()
    logging.info('Audit logs will be written to: %s', audit_log_file)
    
    success_count = 0
    for url in urls:
        # Process backup for each URL
        if process_single_backup(url, username, api_token, poll_interval, backup_target_directory):
            success_count += 1
            
    logging.info('Backup completed for %d of %d Atlassian instances', success_count, len(urls))

if __name__ == '__main__':
    main()