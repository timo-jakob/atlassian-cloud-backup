import os
import logging

from backup import AtlassianCloudBackup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("atlassian_cloud_backup.log"),
        logging.StreamHandler()]
    )

# Read environment variables from OS
backup = AtlassianCloudBackup(
    os.getenv('JIRA_URL'),
    os.getenv('JIRA_USERNAME'),
    os.getenv('JIRA_API_TOKEN'),
    logging
)

taskid = backup.get_backup_task_id()
backup.wait_for_backup_to_complete(taskid)
backup.download_backup_file(taskid)