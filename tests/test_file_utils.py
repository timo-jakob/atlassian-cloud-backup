"""Tests for file management utilities."""

import os
import sys
import re
import json
import pytest
import tempfile
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch, mock_open

# Add the src directory to Python path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from atlassian_cloud_backup.utils.file_utils import FileManager, sanitize_folder_name


class TestSanitizeFolderName:
    """Tests for the sanitize_folder_name function."""
    
    def test_strip_protocol(self):
        """Test that http:// and https:// are removed."""
        assert sanitize_folder_name('http://example.com') == 'example.com'
        assert sanitize_folder_name('https://example.com') == 'example.com'
        
    def test_replace_invalid_chars(self):
        """Test that invalid characters are replaced with underscores."""
        assert sanitize_folder_name('example.com/path') == 'example.com_path'
        assert sanitize_folder_name('example.com\\path') == 'example.com_path'
        assert sanitize_folder_name('example.com:path') == 'example.com_path'
        assert sanitize_folder_name('example.com*path') == 'example.com_path'
        assert sanitize_folder_name('example.com?path') == 'example.com_path'
        assert sanitize_folder_name('example.com"path') == 'example.com_path'
        assert sanitize_folder_name('example.com<path') == 'example.com_path'
        assert sanitize_folder_name('example.com>path') == 'example.com_path'
        assert sanitize_folder_name('example.com|path') == 'example.com_path'
        
    def test_strip_trailing_underscores(self):
        """Test that trailing underscores are stripped."""
        assert sanitize_folder_name('example.com/') == 'example.com'
        assert sanitize_folder_name('example.com\\') == 'example.com'


class TestFileManager:
    """Tests for the FileManager class."""
    
    @pytest.fixture
    def file_manager(self):
        """Create a FileManager instance for testing."""
        return FileManager('https://example.atlassian.net')
    
    @pytest.fixture
    def file_manager_with_target(self):
        """Create a FileManager instance with a custom target directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield FileManager('https://example.atlassian.net', backup_target_directory=temp_dir)
    
    def test_init(self):
        """Test initialization of FileManager."""
        fm = FileManager('https://example.atlassian.net', backup_target_directory='/tmp/backups')
        assert fm.url == 'https://example.atlassian.net'
        assert fm.folder_name == 'example.atlassian.net'
        assert fm.backup_target_directory == '/tmp/backups'
        
    def test_init_no_target_dir(self):
        """Test initialization of FileManager without target directory."""
        fm = FileManager('https://example.atlassian.net')
        assert fm.url == 'https://example.atlassian.net'
        assert fm.folder_name == 'example.atlassian.net'
        assert fm.backup_target_directory is None
    
    @patch('os.makedirs')
    def test_get_backup_folder_with_target(self, mock_makedirs):
        """Test get_backup_folder with a target directory."""
        fm = FileManager('https://example.atlassian.net', backup_target_directory='/tmp/backups')
        folder = fm.get_backup_folder()
        
        # Check expected path
        expected_path = os.path.join('/tmp/backups', 'example.atlassian.net')
        assert folder == os.path.abspath(expected_path)
        
        # Check directory creation
        mock_makedirs.assert_called_once_with(os.path.abspath(expected_path), exist_ok=True)
    
    @patch('os.makedirs')
    def test_get_backup_folder_no_target(self, mock_makedirs):
        """Test get_backup_folder without a target directory."""
        fm = FileManager('https://example.atlassian.net')
        folder = fm.get_backup_folder()
        
        # Check expected path is in CWD
        expected_path = os.path.abspath('example.atlassian.net')
        assert folder == expected_path
        
        # Check directory creation
        mock_makedirs.assert_called_once_with(expected_path, exist_ok=True)
    
    @patch('os.makedirs')
    @patch('datetime.datetime')
    def test_prepare_backup_path(self, mock_datetime, mock_makedirs):
        """Test prepare_backup_path with default parameters."""
        mock_date = Mock()
        mock_date.strftime.return_value = '2025-07-02'
        mock_datetime.now.return_value = mock_date
        
        fm = FileManager('https://example.atlassian.net', backup_target_directory='/tmp/backups')
        
        # Mock get_backup_folder to avoid its implementation details
        with patch.object(fm, 'get_backup_folder', return_value='/tmp/backups/example.atlassian.net'):
            path = fm.prepare_backup_path('Jira')
            
            # Check expected path
            assert path == '/tmp/backups/example.atlassian.net/jira-backup-2025-07-02.zip'
    
    @patch('os.makedirs')
    def test_prepare_backup_path_custom_datetime(self, mock_makedirs):
        """Test prepare_backup_path with custom datetime."""
        custom_date = datetime(2025, 7, 1)
        
        fm = FileManager('https://example.atlassian.net', backup_target_directory='/tmp/backups')
        
        # Mock get_backup_folder to avoid its implementation details
        with patch.object(fm, 'get_backup_folder', return_value='/tmp/backups/example.atlassian.net'):
            path = fm.prepare_backup_path('Confluence', backup_datetime=custom_date)
            
            # Check expected path
            assert path == '/tmp/backups/example.atlassian.net/confluence-backup-2025-07-01.zip'
    
    @patch('os.makedirs')
    def test_prepare_backup_path_custom_extension(self, mock_makedirs):
        """Test prepare_backup_path with custom extension."""
        custom_date = datetime(2025, 7, 1)
        
        fm = FileManager('https://example.atlassian.net', backup_target_directory='/tmp/backups')
        
        # Mock get_backup_folder to avoid its implementation details
        with patch.object(fm, 'get_backup_folder', return_value='/tmp/backups/example.atlassian.net'):
            path = fm.prepare_backup_path('Confluence', extension='.tar.gz', backup_datetime=custom_date)
            
            # Check expected path
            assert path == '/tmp/backups/example.atlassian.net/confluence-backup-2025-07-01.tar.gz'
    
    @patch('os.makedirs')
    @patch('datetime.datetime')
    def test_prepare_jira_backup_path(self, mock_datetime, mock_makedirs):
        """Test prepare_jira_backup_path with default parameters."""
        mock_date = Mock()
        mock_date.strftime.return_value = '2025-07-02'
        mock_datetime.now.return_value = mock_date
        
        fm = FileManager('https://example.atlassian.net', backup_target_directory='/tmp/backups')
        
        # Mock get_backup_folder to avoid its implementation details
        with patch.object(fm, 'get_backup_folder', return_value='/tmp/backups/example.atlassian.net'):
            path = fm.prepare_jira_backup_path(12345)
            
            # Check expected path
            assert path == '/tmp/backups/example.atlassian.net/12345-jira-backup-2025-07-02.zip'
    
    @patch('os.makedirs')
    def test_prepare_jira_backup_path_custom_params(self, mock_makedirs):
        """Test prepare_jira_backup_path with custom parameters."""
        custom_date = datetime(2025, 7, 1)
        
        fm = FileManager('https://example.atlassian.net', backup_target_directory='/tmp/backups')
        
        # Mock get_backup_folder to avoid its implementation details
        with patch.object(fm, 'get_backup_folder', return_value='/tmp/backups/example.atlassian.net'):
            path = fm.prepare_jira_backup_path(54321, extension='.tar.gz', backup_datetime=custom_date)
            
            # Check expected path
            assert path == '/tmp/backups/example.atlassian.net/54321-jira-backup-2025-07-01.tar.gz'
    
    @patch('os.path.exists')
    @patch('os.listdir')
    def test_get_latest_jira_task_id_from_files(self, mock_listdir, mock_exists):
        """Test retrieving the latest Jira task ID from filenames."""
        mock_exists.return_value = True
        mock_listdir.return_value = [
            '12345-jira-backup-2025-06-01.zip',
            '12346-jira-backup-2025-06-02.zip',
            '12350-jira-backup-2025-06-03.zip',
            'confluence-backup-2025-06-01.zip',
            'random-file.txt'
        ]
        
        fm = FileManager('https://example.atlassian.net')
        
        # Mock get_backup_folder to avoid its implementation details
        with patch.object(fm, 'get_backup_folder', return_value='/tmp/backups/example.atlassian.net'):
            task_id = fm.get_latest_jira_task_id_from_files()
            
            # Check expected task ID
            assert task_id == 12350
    
    @patch('os.path.exists')
    @patch('os.listdir')
    def test_get_latest_jira_task_id_from_files_no_files(self, mock_listdir, mock_exists):
        """Test retrieving task ID when no matching files exist."""
        mock_exists.return_value = True
        mock_listdir.return_value = [
            'confluence-backup-2025-06-01.zip',
            'random-file.txt'
        ]
        
        fm = FileManager('https://example.atlassian.net')
        
        # Mock get_backup_folder to avoid its implementation details
        with patch.object(fm, 'get_backup_folder', return_value='/tmp/backups/example.atlassian.net'):
            task_id = fm.get_latest_jira_task_id_from_files()
            
            # Check no task ID found
            assert task_id is None
    
    @patch('os.path.exists')
    def test_get_latest_jira_task_id_from_files_no_folder(self, mock_exists):
        """Test retrieving task ID when folder doesn't exist."""
        mock_exists.return_value = False
        
        fm = FileManager('https://example.atlassian.net')
        
        # Mock get_backup_folder to avoid its implementation details
        with patch.object(fm, 'get_backup_folder', return_value='/tmp/backups/example.atlassian.net'):
            task_id = fm.get_latest_jira_task_id_from_files()
            
            # Check no task ID found
            assert task_id is None
    
    @patch('os.path.exists')
    @patch('os.listdir')
    @patch('logging.warning')
    def test_get_latest_jira_task_id_from_files_error(self, mock_logging, mock_listdir, mock_exists):
        """Test error handling when reading backup directory fails."""
        mock_exists.return_value = True
        mock_listdir.side_effect = OSError('Permission denied')
        
        fm = FileManager('https://example.atlassian.net')
        
        # Mock get_backup_folder to avoid its implementation details
        with patch.object(fm, 'get_backup_folder', return_value='/tmp/backups/example.atlassian.net'):
            task_id = fm.get_latest_jira_task_id_from_files()
            
            # Check error handling
            assert task_id is None
            mock_logging.assert_called_once()
            assert 'Error reading backup directory' in mock_logging.call_args[0][0]
    
    @patch('os.makedirs')
    @patch('os.getcwd')
    def test_get_audit_log_path_with_target(self, mock_getcwd, mock_makedirs):
        """Test get_audit_log_path with a target directory."""
        mock_getcwd.return_value = '/home/user'
        
        fm = FileManager('https://example.atlassian.net', backup_target_directory='/tmp/backups')
        audit_path = fm.get_audit_log_path()
        
        # Check expected path
        assert audit_path == os.path.join('/tmp/backups', 'atlassian.backup.audit.log')
        
        # Check directory creation
        mock_makedirs.assert_called_once_with(os.path.abspath('/tmp/backups'), exist_ok=True)
        
        # Check getcwd was not called
        mock_getcwd.assert_not_called()
    
    @patch('os.makedirs')
    @patch('os.getcwd')
    def test_get_audit_log_path_no_target(self, mock_getcwd, mock_makedirs):
        """Test get_audit_log_path without a target directory."""
        mock_getcwd.return_value = '/home/user'
        
        fm = FileManager('https://example.atlassian.net')
        audit_path = fm.get_audit_log_path()
        
        # Check expected path
        assert audit_path == os.path.join('/home/user', 'atlassian.backup.audit.log')
        
        # Check directory creation
        mock_makedirs.assert_called_once_with('/home/user', exist_ok=True)
        
        # Check getcwd was called
        mock_getcwd.assert_called_once()
    
    @patch('os.path.exists')
    @patch('os.listdir')
    def test_find_latest_confluence_backup_file(self, mock_listdir, mock_exists):
        """Test finding the latest Confluence backup file."""
        mock_exists.return_value = True
        mock_listdir.return_value = [
            'confluence-backup-2025-06-01.zip',
            'confluence-backup-2025-07-02.zip',
            'confluence-backup-2025-06-15.zip',
            '12345-jira-backup-2025-06-01.zip',
            'random-file.txt'
        ]
        
        fm = FileManager('https://example.atlassian.net')
        
        # Mock get_backup_folder to avoid its implementation details
        with patch.object(fm, 'get_backup_folder', return_value='/tmp/backups/example.atlassian.net'):
            latest_file = fm.find_latest_confluence_backup_file()
            
            # Check expected file path
            assert latest_file == '/tmp/backups/example.atlassian.net/confluence-backup-2025-07-02.zip'
    
    @patch('os.path.exists')
    @patch('os.listdir')
    def test_find_latest_confluence_backup_file_no_files(self, mock_listdir, mock_exists):
        """Test finding Confluence backup when no matching files exist."""
        mock_exists.return_value = True
        mock_listdir.return_value = [
            '12345-jira-backup-2025-06-01.zip',
            'random-file.txt'
        ]
        
        fm = FileManager('https://example.atlassian.net')
        
        # Mock get_backup_folder to avoid its implementation details
        with patch.object(fm, 'get_backup_folder', return_value='/tmp/backups/example.atlassian.net'):
            latest_file = fm.find_latest_confluence_backup_file()
            
            # Check no file found
            assert latest_file is None
    
    @patch('os.path.exists')
    def test_find_latest_confluence_backup_file_no_folder(self, mock_exists):
        """Test finding Confluence backup when folder doesn't exist."""
        mock_exists.return_value = False
        
        fm = FileManager('https://example.atlassian.net')
        
        # Mock get_backup_folder to avoid its implementation details
        with patch.object(fm, 'get_backup_folder', return_value='/tmp/backups/example.atlassian.net'):
            latest_file = fm.find_latest_confluence_backup_file()
            
            # Check no file found
            assert latest_file is None
    
    @patch('os.path.exists')
    @patch('os.listdir')
    @patch('logging.warning')
    def test_find_latest_confluence_backup_file_error(self, mock_logging, mock_listdir, mock_exists):
        """Test error handling when reading backup directory fails."""
        mock_exists.return_value = True
        mock_listdir.side_effect = OSError('Permission denied')
        
        fm = FileManager('https://example.atlassian.net')
        
        # Mock get_backup_folder to avoid its implementation details
        with patch.object(fm, 'get_backup_folder', return_value='/tmp/backups/example.atlassian.net'):
            latest_file = fm.find_latest_confluence_backup_file()
            
            # Check error handling
            assert latest_file is None
            mock_logging.assert_called_once()
            assert 'Error reading backup directory' in mock_logging.call_args[0][0]
    
    @patch('os.path.exists')
    @patch('os.listdir')
    @patch('logging.warning')
    def test_find_latest_confluence_backup_file_invalid_date(self, mock_logging, mock_listdir, mock_exists):
        """Test handling invalid dates in filenames."""
        mock_exists.return_value = True
        mock_listdir.return_value = [
            'confluence-backup-2025-13-01.zip',  # Invalid month
            'confluence-backup-2025-06-32.zip',  # Invalid day
            'confluence-backup-invalid-date.zip',  # Invalid format
            '12345-jira-backup-2025-06-01.zip'
        ]
        
        fm = FileManager('https://example.atlassian.net')
        
        # Mock get_backup_folder to avoid its implementation details
        with patch.object(fm, 'get_backup_folder', return_value='/tmp/backups/example.atlassian.net'):
            latest_file = fm.find_latest_confluence_backup_file()
            
            # Check no valid file found
            assert latest_file is None
            # Check warning was logged only for files that match the pattern but have invalid dates
            assert mock_logging.call_count == 2  # Only the first two items have the right pattern but invalid dates
    
    def test_extract_date_from_confluence_filename(self):
        """Test extracting date from Confluence backup filename."""
        fm = FileManager('https://example.atlassian.net')
        
        # Valid filename
        date = fm.extract_date_from_confluence_filename('/tmp/backups/confluence-backup-2025-07-02.zip')
        assert date == datetime(2025, 7, 2, 0, 0, 0)
        
        # Invalid filename format
        assert fm.extract_date_from_confluence_filename('/tmp/backups/invalid-filename.zip') is None
        
        # Invalid date in filename
        assert fm.extract_date_from_confluence_filename('/tmp/backups/confluence-backup-invalid-date.zip') is None
        
        # None input
        assert fm.extract_date_from_confluence_filename(None) is None
    
    def test_extract_date_from_confluence_filename_with_invalid_date_format(self):
        """Test extracting date with invalid date format."""
        fm = FileManager('https://example.atlassian.net')
        
        # Valid filename pattern but invalid date
        date = fm.extract_date_from_confluence_filename('/tmp/backups/confluence-backup-2025-13-32.zip')
        assert date is None
    
    def test_is_confluence_backup_needed_no_existing_backup(self):
        """Test backup needed when no existing backup."""
        fm = FileManager('https://example.atlassian.net')
        
        # Mock find_latest_confluence_backup_file to return None (no existing backup)
        with patch.object(fm, 'find_latest_confluence_backup_file', return_value=None):
            needed, latest_file = fm.is_confluence_backup_needed()
            
            # Check backup is needed
            assert needed is True
            assert latest_file is None
    
    def test_is_confluence_backup_needed_unparseable_filename(self):
        """Test backup needed when filename can't be parsed."""
        fm = FileManager('https://example.atlassian.net')
        
        # Mock functions to return invalid filename
        with patch.object(fm, 'find_latest_confluence_backup_file', return_value='/tmp/invalid-filename.zip'), \
             patch.object(fm, 'extract_date_from_confluence_filename', return_value=None):
            needed, latest_file = fm.is_confluence_backup_needed()
            
            # Check backup is needed
            assert needed is True
            assert latest_file == '/tmp/invalid-filename.zip'
    
    def test_is_confluence_backup_needed_same_day(self):
        """Test backup not needed when latest backup is from today."""
        fm = FileManager('https://example.atlassian.net')
        today = datetime(2025, 7, 2, 10, 0, 0)  # Using a fixed datetime for testing
        
        # Mock functions to return today's date
        with patch.object(fm, 'find_latest_confluence_backup_file', return_value='/tmp/confluence-backup-2025-07-02.zip'), \
             patch.object(fm, 'extract_date_from_confluence_filename', return_value=datetime(2025, 7, 2, 0, 0, 0)):
            needed, latest_file = fm.is_confluence_backup_needed(today)
            
            # Check backup is not needed
            assert needed is False
            assert latest_file == '/tmp/confluence-backup-2025-07-02.zip'
    
    def test_is_confluence_backup_needed_yesterday(self):
        """Test backup needed when latest backup is from yesterday."""
        fm = FileManager('https://example.atlassian.net')
        today = datetime(2025, 7, 2, 10, 0, 0)  # Using a fixed datetime for testing
        
        # Mock functions to return yesterday's date
        with patch.object(fm, 'find_latest_confluence_backup_file', return_value='/tmp/confluence-backup-2025-07-01.zip'), \
             patch.object(fm, 'extract_date_from_confluence_filename', return_value=datetime(2025, 7, 1, 0, 0, 0)):
            needed, latest_file = fm.is_confluence_backup_needed(today)
            
            # Check backup is needed (the implementation considers a backup needed after 1 day)
            assert needed is True
            assert latest_file == '/tmp/confluence-backup-2025-07-01.zip'
    
    def test_is_confluence_backup_needed_older(self):
        """Test backup needed when latest backup is older than 1 day."""
        fm = FileManager('https://example.atlassian.net')
        today = datetime(2025, 7, 2, 10, 0, 0)  # Using a fixed datetime for testing
        
        # Mock functions to return old date
        with patch.object(fm, 'find_latest_confluence_backup_file', return_value='/tmp/confluence-backup-2025-06-30.zip'), \
             patch.object(fm, 'extract_date_from_confluence_filename', return_value=datetime(2025, 6, 30, 0, 0, 0)):
            needed, latest_file = fm.is_confluence_backup_needed(today)
            
            # Check backup is needed (more than 1 full day)
            assert needed is True
            assert latest_file == '/tmp/confluence-backup-2025-06-30.zip'
    
    def test_is_confluence_backup_needed_timezone_handling(self):
        """Test that timezone info is properly handled."""
        from datetime import timezone as tz
        
        fm = FileManager('https://example.atlassian.net')
        # Create a datetime with timezone info
        today_with_tz = datetime(2025, 7, 2, 10, 0, 0, tzinfo=tz.utc)
        
        # Mock functions to return a date
        with patch.object(fm, 'find_latest_confluence_backup_file', return_value='/tmp/confluence-backup-2025-07-01.zip'), \
             patch.object(fm, 'extract_date_from_confluence_filename', return_value=datetime(2025, 7, 1, 0, 0, 0)):
            needed, latest_file = fm.is_confluence_backup_needed(today_with_tz)
            
            # Ensure the timezone info was properly handled
            # The implementation considers a backup needed after 1 day, which is the case here
            assert needed is True
            assert latest_file == '/tmp/confluence-backup-2025-07-01.zip'
    
    def test_is_confluence_backup_needed_with_tzinfo(self):
        """Test is_confluence_backup_needed with now parameter that has timezone info."""
        fm = FileManager('https://example.atlassian.net')
        
        # Create a now date with timezone info
        now = datetime(2025, 7, 2, 10, 0, 0, tzinfo=timezone.utc)
        
        # Mock find_latest_confluence_backup_file to return a file path
        # Mock extract_date_from_confluence_filename to return a date without timezone
        with patch.object(fm, 'find_latest_confluence_backup_file', return_value='/tmp/confluence-backup-2025-06-30.zip'), \
             patch.object(fm, 'extract_date_from_confluence_filename', return_value=datetime(2025, 6, 30, 0, 0, 0)):
            needed, latest_file = fm.is_confluence_backup_needed(now)
            
            # Check backup is needed
            assert needed is True
            assert latest_file == '/tmp/confluence-backup-2025-06-30.zip'

    def test_backup_date_with_tzinfo_handling(self):
        """Test handling of backup dates with timezone info (very specific test for line 218)."""
        fm = FileManager('https://example.atlassian.net')
        
        # Create a datetime object with timezone for testing
        backup_date_with_tz = datetime(2025, 6, 30, 0, 0, 0, tzinfo=timezone.utc)
        
        # This test directly targets the timezone handling code
        # We'll use a context manager to patch methods and test the specific behavior
        with patch.object(fm, 'find_latest_confluence_backup_file', return_value='/tmp/confluence-backup-2025-06-30.zip'), \
             patch.object(fm, 'extract_date_from_confluence_filename', return_value=backup_date_with_tz):
            
            # Call the method under test with a non-timezone aware datetime
            now = datetime(2025, 7, 2, 0, 0, 0)  # 2 days after the backup date
            needed, latest_file = fm.is_confluence_backup_needed(now)
            
            # Verify the timezone was properly stripped and comparison works
            assert needed is True
            assert latest_file == '/tmp/confluence-backup-2025-06-30.zip'
    
    def test_real_fs_operations(self):
        """Integration test with real filesystem operations."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a file manager with the temp directory as target
            fm = FileManager('https://example.atlassian.net', backup_target_directory=temp_dir)
            
            # Get the backup folder and verify it was created
            backup_folder = fm.get_backup_folder()
            assert os.path.exists(backup_folder)
            
            # Create a backup path and verify it's correct
            backup_path = fm.prepare_backup_path('Jira')
            assert backup_path.startswith(backup_folder)
            assert 'jira-backup-' in backup_path
            assert backup_path.endswith('.zip')
            
            # Get audit log path and verify it's correct
            audit_path = fm.get_audit_log_path()
            assert audit_path == os.path.join(temp_dir, 'atlassian.backup.audit.log')


if __name__ == '__main__':
    pytest.main(['-v', __file__])
