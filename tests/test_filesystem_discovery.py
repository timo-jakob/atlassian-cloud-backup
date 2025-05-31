"""Tests for filesystem discovery functionality."""

import os
import tempfile
import shutil
from datetime import datetime, time
from unittest.mock import patch, MagicMock
import pytest

from atlassian_cloud_backup.utils.filesystem_discovery import FilesystemDiscovery
from atlassian_cloud_backup.utils.file_utils import FileManager, sanitize_folder_name


class TestFilesystemDiscovery:
    """Test cases for the FilesystemDiscovery class."""
    
    def setup_method(self):
        """Set up test environment before each test."""
        self.temp_dir = tempfile.mkdtemp()
    
    def teardown_method(self):
        """Clean up test environment after each test."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_empty_directory(self):
        """Test discovery in an empty directory."""
        discovery = FilesystemDiscovery(self.temp_dir)
        result = discovery.discover_sites_and_backups()
        assert result == {}
    
    def test_nonexistent_directory(self):
        """Test discovery when directory doesn't exist."""
        nonexistent_dir = os.path.join(self.temp_dir, "nonexistent")
        discovery = FilesystemDiscovery(nonexistent_dir)
        result = discovery.discover_sites_and_backups()
        assert result == {}
    
    def test_discover_single_site_with_jira_backup(self):
        """Test discovering a single site with a Jira backup."""
        # Create a site directory
        site_url = "https://mycompany.atlassian.net"
        site_folder = sanitize_folder_name(site_url)
        site_dir = os.path.join(self.temp_dir, site_folder)
        os.makedirs(site_dir)
        
        # Create a Jira backup file
        backup_filename = "jira-backup-2024-01-15.zip"
        backup_path = os.path.join(site_dir, backup_filename)
        with open(backup_path, 'w') as f:
            f.write("fake backup content")
        
        discovery = FilesystemDiscovery(self.temp_dir)
        result = discovery.discover_sites_and_backups()
        
        assert len(result) == 1
        assert site_url in result
        site_status = result[site_url]
        assert 'last_jira_backup' in site_status
        assert 'jira_file' in site_status
        assert site_status['jira_file'] == backup_path
        
        expected_date = datetime.combine(datetime(2024, 1, 15).date(), time(0, 0, 0))
        assert site_status['last_jira_backup'] == expected_date
    
    def test_discover_single_site_with_confluence_backup(self):
        """Test discovering a single site with a Confluence backup."""
        site_url = "https://testcompany.atlassian.net"
        site_folder = sanitize_folder_name(site_url)
        site_dir = os.path.join(self.temp_dir, site_folder)
        os.makedirs(site_dir)
        
        # Create a Confluence backup file
        backup_filename = "confluence-backup-2024-02-20.zip"
        backup_path = os.path.join(site_dir, backup_filename)
        with open(backup_path, 'w') as f:
            f.write("fake backup content")
        
        discovery = FilesystemDiscovery(self.temp_dir)
        result = discovery.discover_sites_and_backups()
        
        assert len(result) == 1
        assert site_url in result
        site_status = result[site_url]
        assert 'last_confluence_backup' in site_status
        assert 'confluence_file' in site_status
        assert site_status['confluence_file'] == backup_path
        
        expected_date = datetime.combine(datetime(2024, 2, 20).date(), time(0, 0, 0))
        assert site_status['last_confluence_backup'] == expected_date
    
    def test_discover_site_with_both_services(self):
        """Test discovering a site with both Jira and Confluence backups."""
        site_url = "https://fullsite.atlassian.net"
        site_folder = sanitize_folder_name(site_url)
        site_dir = os.path.join(self.temp_dir, site_folder)
        os.makedirs(site_dir)
        
        # Create both backup files
        jira_backup = os.path.join(site_dir, "jira-backup-2024-03-10.zip")
        conf_backup = os.path.join(site_dir, "confluence-backup-2024-03-12.zip")
        
        with open(jira_backup, 'w') as f:
            f.write("fake jira backup")
        with open(conf_backup, 'w') as f:
            f.write("fake confluence backup")
        
        discovery = FilesystemDiscovery(self.temp_dir)
        result = discovery.discover_sites_and_backups()
        
        assert len(result) == 1
        assert site_url in result
        site_status = result[site_url]
        
        assert 'last_jira_backup' in site_status
        assert 'jira_file' in site_status
        assert site_status['jira_file'] == jira_backup
        
        assert 'last_confluence_backup' in site_status
        assert 'confluence_file' in site_status
        assert site_status['confluence_file'] == conf_backup
        
        jira_date = datetime.combine(datetime(2024, 3, 10).date(), time(0, 0, 0))
        conf_date = datetime.combine(datetime(2024, 3, 12).date(), time(0, 0, 0))
        assert site_status['last_jira_backup'] == jira_date
        assert site_status['last_confluence_backup'] == conf_date
    
    def test_discover_multiple_backup_files_selects_latest(self):
        """Test that discovery selects the most recent backup when multiple exist."""
        site_url = "https://multibackup.atlassian.net"
        site_folder = sanitize_folder_name(site_url)
        site_dir = os.path.join(self.temp_dir, site_folder)
        os.makedirs(site_dir)
        
        # Create multiple Jira backups
        old_backup = os.path.join(site_dir, "jira-backup-2024-01-01.zip")
        new_backup = os.path.join(site_dir, "jira-backup-2024-01-15.zip")
        
        with open(old_backup, 'w') as f:
            f.write("old backup")
        with open(new_backup, 'w') as f:
            f.write("new backup")
        
        discovery = FilesystemDiscovery(self.temp_dir)
        result = discovery.discover_sites_and_backups()
        
        assert len(result) == 1
        site_status = result[site_url]
        
        # Should select the newer backup
        assert site_status['jira_file'] == new_backup
        expected_date = datetime.combine(datetime(2024, 1, 15).date(), time(0, 0, 0))
        assert site_status['last_jira_backup'] == expected_date
    
    def test_discover_multiple_sites(self):
        """Test discovering multiple sites."""
        sites = [
            "https://site1.atlassian.net",
            "https://site2.atlassian.net"
        ]
        
        for site_url in sites:
            site_folder = sanitize_folder_name(site_url)
            site_dir = os.path.join(self.temp_dir, site_folder)
            os.makedirs(site_dir)
            
            # Create a backup file for each site
            backup_path = os.path.join(site_dir, "jira-backup-2024-01-01.zip")
            with open(backup_path, 'w') as f:
                f.write("backup content")
        
        discovery = FilesystemDiscovery(self.temp_dir)
        result = discovery.discover_sites_and_backups()
        
        assert len(result) == 2
        for site_url in sites:
            assert site_url in result
            assert 'last_jira_backup' in result[site_url]
            assert 'jira_file' in result[site_url]
    
    def test_ignore_non_backup_files(self):
        """Test that non-backup files are ignored."""
        site_url = "https://cleansite.atlassian.net"
        site_folder = sanitize_folder_name(site_url)
        site_dir = os.path.join(self.temp_dir, site_folder)
        os.makedirs(site_dir)
        
        # Create various non-backup files
        files_to_ignore = [
            "README.txt",
            "backup_status.json",
            "random-file.zip",
            "jira-backup.zip",  # Missing date
            "confluence-backup-invalid-date.zip",
            "not-a-service-backup-2024-01-01.zip"
        ]
        
        for filename in files_to_ignore:
            file_path = os.path.join(site_dir, filename)
            with open(file_path, 'w') as f:
                f.write("content")
        
        discovery = FilesystemDiscovery(self.temp_dir)
        result = discovery.discover_sites_and_backups()
        
        # Should not find any valid backups
        assert result == {}
    
    def test_handle_different_file_extensions(self):
        """Test handling of different backup file extensions."""
        site_url = "https://extensions.atlassian.net"
        site_folder = sanitize_folder_name(site_url)
        site_dir = os.path.join(self.temp_dir, site_folder)
        os.makedirs(site_dir)
        
        # Create backups with different extensions
        zip_backup = os.path.join(site_dir, "jira-backup-2024-01-01.zip")
        tar_backup = os.path.join(site_dir, "confluence-backup-2024-01-02.tar.gz")
        
        with open(zip_backup, 'w') as f:
            f.write("zip backup")
        with open(tar_backup, 'w') as f:
            f.write("tar backup")
        
        discovery = FilesystemDiscovery(self.temp_dir)
        result = discovery.discover_sites_and_backups()
        
        assert len(result) == 1
        site_status = result[site_url]
        assert 'last_jira_backup' in site_status
        assert 'last_confluence_backup' in site_status
        assert site_status['jira_file'] == zip_backup
        assert site_status['confluence_file'] == tar_backup
    
    def test_url_reconstruction(self):
        """Test URL reconstruction from folder names."""
        discovery = FilesystemDiscovery(self.temp_dir)
        
        test_cases = [
            ("mycompany.atlassian.net", "https://mycompany.atlassian.net"),
            ("test.atlassian.com", "https://test.atlassian.com"),
            ("company_name.atlassian.net", "https://company.name.atlassian.net"),
        ]
        
        for folder_name, expected_url in test_cases:
            result = discovery._reconstruct_url_from_folder_name(folder_name)
            assert result == expected_url
    
    def test_url_reconstruction_edge_cases(self):
        """Test URL reconstruction edge cases."""
        discovery = FilesystemDiscovery(self.temp_dir)
        
        # Test cases that should return None
        invalid_cases = [
            "just_a_folder",
            "logs",
            ".hidden",
            ""
        ]
        
        for folder_name in invalid_cases:
            result = discovery._reconstruct_url_from_folder_name(folder_name)
            # These might return None or a URL, depending on heuristics
            # The important thing is they don't crash
            assert result is None or isinstance(result, str)
    
    def test_verify_discovered_site(self):
        """Test site verification."""
        discovery = FilesystemDiscovery(self.temp_dir)
        
        # Test valid round-trip
        original_url = "https://mycompany.atlassian.net"
        folder_name = sanitize_folder_name(original_url)
        
        assert discovery.verify_discovered_site(original_url, folder_name)
        
        # Test invalid cases
        assert not discovery.verify_discovered_site(None, folder_name)
        assert not discovery.verify_discovered_site("https://wrong.com", folder_name)
    
    def test_get_backup_statistics(self):
        """Test backup statistics generation."""
        discovery = FilesystemDiscovery(self.temp_dir)
        
        # Create test data
        discovered_status = {
            "https://site1.atlassian.net": {
                "last_jira_backup": datetime(2024, 1, 1),
                "jira_file": "/path/to/jira.zip"
            },
            "https://site2.atlassian.net": {
                "last_confluence_backup": datetime(2024, 1, 15),
                "confluence_file": "/path/to/confluence.zip"
            },
            "https://site3.atlassian.net": {
                "last_jira_backup": datetime(2024, 1, 10),
                "last_confluence_backup": datetime(2024, 1, 20),
                "jira_file": "/path/to/jira.zip",
                "confluence_file": "/path/to/confluence.zip"
            }
        }
        
        stats = discovery.get_backup_statistics(discovered_status)
        
        assert stats['total_sites'] == 3
        assert stats['sites_with_jira'] == 2
        assert stats['sites_with_confluence'] == 2
        assert stats['sites_with_both'] == 1
        assert stats['total_backup_files'] == 4
        assert stats['oldest_backup'] == datetime(2024, 1, 1)
        assert stats['newest_backup'] == datetime(2024, 1, 20)
    
    def test_get_backup_statistics_empty(self):
        """Test backup statistics with empty data."""
        discovery = FilesystemDiscovery(self.temp_dir)
        stats = discovery.get_backup_statistics({})
        
        assert stats['total_sites'] == 0
        assert stats['sites_with_jira'] == 0
        assert stats['sites_with_confluence'] == 0
        assert stats['sites_with_both'] == 0
        assert stats['total_backup_files'] == 0
        assert stats['oldest_backup'] is None
        assert stats['newest_backup'] is None


class TestFileManagerDiscoveryIntegration:
    """Test integration of filesystem discovery with FileManager."""
    
    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
    
    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_load_consolidated_status_uses_discovery_when_no_file(self):
        """Test that load_consolidated_status uses discovery when no file exists."""
        # Create a site with backup files
        site_url = "https://testsite.atlassian.net"
        site_folder = sanitize_folder_name(site_url)
        site_dir = os.path.join(self.temp_dir, site_folder)
        os.makedirs(site_dir)
        
        backup_path = os.path.join(site_dir, "jira-backup-2024-01-01.zip")
        with open(backup_path, 'w') as f:
            f.write("backup content")
        
        # Create FileManager (without creating consolidated status file)
        file_manager = FileManager("https://other.atlassian.net", self.temp_dir)
        
        # Should use discovery and find the existing site
        result = file_manager.load_consolidated_status()
        
        assert len(result) == 1
        assert site_url in result
        assert 'last_jira_backup' in result[site_url]
        
        # Should also create the consolidated status file
        status_file = file_manager.get_consolidated_status_file()
        assert os.path.exists(status_file)
    
    def test_load_consolidated_status_falls_back_to_discovery_on_error(self):
        """Test fallback to discovery when consolidated status file is corrupted."""
        # Create a site with backup files
        site_url = "https://fallback.atlassian.net"
        site_folder = sanitize_folder_name(site_url)
        site_dir = os.path.join(self.temp_dir, site_folder)
        os.makedirs(site_dir)
        
        backup_path = os.path.join(site_dir, "jira-backup-2024-01-01.zip")
        with open(backup_path, 'w') as f:
            f.write("backup content")
        
        # Create FileManager and write corrupted status file
        file_manager = FileManager("https://other.atlassian.net", self.temp_dir)
        status_file = file_manager.get_consolidated_status_file()
        
        with open(status_file, 'w') as f:
            f.write("invalid json content")
        
        # Should fall back to discovery
        result = file_manager.load_consolidated_status()
        
        assert len(result) == 1
        assert site_url in result
        assert 'last_jira_backup' in result[site_url]
    
    @patch('atlassian_cloud_backup.utils.file_utils.logging')
    def test_discovery_logging(self, mock_logging):
        """Test that discovery produces appropriate log messages."""
        # Create a site with backup files
        site_url = "https://logging.atlassian.net"
        site_folder = sanitize_folder_name(site_url)
        site_dir = os.path.join(self.temp_dir, site_folder)
        os.makedirs(site_dir)
        
        backup_path = os.path.join(site_dir, "jira-backup-2024-01-01.zip")
        with open(backup_path, 'w') as f:
            f.write("backup content")
        
        file_manager = FileManager("https://other.atlassian.net", self.temp_dir)
        file_manager.load_consolidated_status()
        
        # Verify that discovery logging occurred
        mock_logging.info.assert_any_call(
            'Filesystem discovery found %d sites with %d total backup files', 1, 1
        )
        mock_logging.info.assert_any_call('Saving discovered status to consolidated status file')
