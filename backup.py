import requests
import time
from requests.auth import HTTPBasicAuth

class AtlassianCloudBackup:

    def __init__(self, jira_url, username, api_token, logging):
        self.jira_url = jira_url
        self.username = username
        self.api_token = api_token
        self.logging = logging

        self.BACKUP_TASKID = "/rest/backup/1/export/lastTaskId" # the path of the endpoint to get the last task ID which is our backup task ID
        self.BACKUP_PROGRESS = "/rest/api/3/task/" # the path of the endpoint to get the progress of the backup task
        self.BACKUP_DOWNLOAD = "/rest/backup/1/export/getProgress?taskId=" # the path of the endpoint to get the download URL of the backup file
        self.BACKUP_TRIGGER = "/rest/backup/1/export/runbackup" # the path of the endpoint to trigger the backup

        if not all([self.jira_url, self.username, self.api_token]):
            raise EnvironmentError("Please set the JIRA_URL, JIRA_USERNAME, and JIRA_API_TOKEN environment variables.")
        
    def _get_json_response(self, url):
        response = requests.get(
            url,
            auth=HTTPBasicAuth(self.username, self.api_token),
            headers={'Content-Type': 'application/json'}
        )
        return response.json()
    
    def get_backup_task_id(self):
        url = f'{self.jira_url}{self.BACKUP_TASKID}'   # the full URL of the endpoint to use for the request
        return self._get_json_response(url)
    
    def _get_task_progress(self, task_id):
        url = f'{self.jira_url}{self.BACKUP_PROGRESS}{task_id}'
        return self._get_json_response(url)
    
    def _get_download_url(self, task_id):
        url = f'{self.jira_url}{self.BACKUP_DOWNLOAD}{task_id}'
        response = self._get_json_response(url)
        return f'{self.jira_url}/plugins/servlet/{response.get("result")}'
    
    def wait_for_backup_to_complete(self, task_id):
        while True:
            backup_status = self._get_task_progress(task_id)
        
            status = backup_status.get('status')
            progress = backup_status.get('progress')

            if status == 'COMPLETE':
                self.logging.info("Backup completed.")
                return True
            elif status == 'FAILED':
                self.logging.error("Backup failed.")
                return False
            elif status == 'RUNNING' or status == 'ENQUEUED':
                self.logging.info("Backup in progress..." + str(progress))
                time.sleep(60)
            else:
                self.logging.info(f"Backup in status {backup_status}!")
                return False

    def download_backup_file(self, task_id):
        download_url = self._get_download_url(task_id)
        local_file_path = f'jira_backup_{task_id}.zip'

        self.logging.info(f"Downloading backup file from {download_url}...")
        response = requests.get(
            download_url,
            auth=HTTPBasicAuth(self.username, self.api_token),
            stream=True
        )
        with open(local_file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024):
                f.write(chunk)

    def trigger_backup(self):
        # Endpoint to trigger backup
        backup_url = f'{self.jira_url}/rest/backup/1/export/runbackup'

        # Payload to trigger backup
        payload = {
            "cbAttachments": True,  # Set to True if you want to include attachments
            "exportToCloud": False  # Set to False to download to local disk
        }

        # Trigger the backup
        response = requests.post(
            backup_url,
            auth=HTTPBasicAuth(self.username, self.api_token),
            headers={'Content-Type': 'application/json'},
            json=payload
        )

        success = response.status_code == 200
        if success:
            self.logging.info("Backup triggered successfully.")
        else:
            self.logging.error(f"Failed to trigger backup: {response.status_code}")
            self.logging.error(response.text)

        return success