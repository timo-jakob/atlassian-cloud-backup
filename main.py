#!/usr/bin/env python3
"""
CLI tool to backup Atlassian Cloud instances (Jira & Confluence).
"""

import os
import sys
import logging
import click
from datetime import datetime

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
    
    Environment variables:
    - ATLASSIAN_INSTANCES: Comma-separated list of instance names (e.g., "company1,company2")
      Script will convert these to https://<name>.atlassian.net URLs
    - ATLASSIAN_USERNAME: Username for Atlassian authentication
    - ATLASSIAN_API_TOKEN: API token for Atlassian authentication
    - POLL_INTERVAL_SECONDS: Optional, seconds to wait between API polling requests (default: 30)
    """
    # Get instance names from environment
    instance_names = os.getenv('ATLASSIAN_INSTANCES', '')
    
    # Process instance names into standard Atlassian URLs
    urls = []
    if instance_names:
        names = [name.strip() for name in instance_names.split(',') if name.strip()]
        urls = [f"https://{name}.atlassian.net" for name in names]
    
    if not urls:
        logging.error('No valid Atlassian instances provided. Set ATLASSIAN_INSTANCES environment variable.')
        sys.exit(1)
        
    username = os.getenv('ATLASSIAN_USERNAME')
    api_token = os.getenv('ATLASSIAN_API_TOKEN')
    poll_interval = int(os.getenv('POLL_INTERVAL_SECONDS', 30))
    
    if not all([username, api_token]):
        logging.error(
            'Missing one of ATLASSIAN_USERNAME or ATLASSIAN_API_TOKEN environment variables.'
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
                poll_interval=poll_interval
            )
            controller.orchestrate()
            success_count += 1
            logging.info('Completed backup for %s', url)
        except Exception as e:
            logging.error('Failed to backup %s: %s', url, str(e))
            
    logging.info('Backup completed for %d of %d Atlassian instances', success_count, len(urls))

if __name__ == '__main__':
    main()