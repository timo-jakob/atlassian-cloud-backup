"""
Test cases for the thinning module configuration and utilities.

This module tests the configuration classes and utility functions
used throughout the thinning module.
"""

import pytest
import tempfile
import json
from pathlib import Path
from unittest.mock import patch, mock_open

# Import the modules under test
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from atlassian_cloud_backup.thinning.config import (
    ThinningSettings,
    ThinningConfig,
    create_sample_config
)
from atlassian_cloud_backup.thinning.utils import (
    bytes_to_human_readable,
    human_readable_to_bytes,
    calculate_percentage,
    format_usage_status,
    estimate_backup_size,
    validate_thinning_config
)


class TestThinningSettings:
    """Test the ThinningSettings dataclass."""
    
    def test_thinning_settings_creation(self):
        """Test creating ThinningSettings instance."""
        settings = ThinningSettings(
            max_size_bytes=1000000000,  # 1GB
            backup_directory="/backups",
            deletion_strategy="retention_ladder",
            warning_threshold=0.8
        )
        
        assert settings.max_size_bytes == 1000000000
        assert settings.backup_directory == "/backups"
        assert settings.deletion_strategy == "retention_ladder"
        assert settings.warning_threshold == 0.8
    
    def test_thinning_settings_from_dict(self):
        """Test creating ThinningSettings from dictionary."""
        data = {
            "max_size_bytes": 2000000000,
            "backup_directory": "/test/backups",
            "deletion_strategy": "oldest_first",
            "warning_threshold": 0.9
        }
        
        settings = ThinningSettings.from_dict(data)
        
        assert settings.max_size_bytes == 2000000000
        assert settings.backup_directory == "/test/backups"
        assert settings.deletion_strategy == "oldest_first"
        assert settings.warning_threshold == 0.9
    
    def test_thinning_settings_to_dict(self):
        """Test converting ThinningSettings to dictionary."""
        settings = ThinningSettings(
            max_size_bytes=1500000000,
            backup_directory="/data/backups",
            deletion_strategy="retention_ladder",
            warning_threshold=0.85
        )
        
        result = settings.to_dict()
        
        expected = {
            "max_size_bytes": 1500000000,
            "backup_directory": "/data/backups",
            "deletion_strategy": "retention_ladder",
            "warning_threshold": 0.85
        }
        
        assert result == expected
    
    def test_thinning_settings_validate_valid(self):
        """Test validation of valid ThinningSettings."""
        settings = ThinningSettings(
            max_size_bytes=1000000000,
            backup_directory="/backups",
            deletion_strategy="retention_ladder",
            warning_threshold=0.8
        )
        
        # Should not raise any exceptions
        settings.validate()
    
    def test_thinning_settings_validate_invalid_threshold(self):
        """Test validation with invalid warning threshold."""
        settings = ThinningSettings(
            max_size_bytes=1000000000,
            backup_directory="/backups",
            deletion_strategy="retention_ladder",
            warning_threshold=1.5  # Invalid: > 1.0
        )
        
        with pytest.raises(ValueError, match="Warning threshold must be between 0 and 1"):
            settings.validate()
    
    def test_thinning_settings_validate_invalid_size(self):
        """Test validation with invalid max size."""
        settings = ThinningSettings(
            max_size_bytes=-1000,  # Invalid: negative
            backup_directory="/backups",
            deletion_strategy="retention_ladder",
            warning_threshold=0.8
        )
        
        with pytest.raises(ValueError, match="Max size must be positive"):
            settings.validate()


class TestThinningConfig:
    """Test the ThinningConfig class."""
    
    def test_thinning_config_creation(self):
        """Test creating ThinningConfig instance."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir)
            thinning_config = ThinningConfig(config_path)
            
            assert thinning_config.config_directory == config_path
            assert thinning_config.config_file == config_path / "thinning_config.json"
    
    @patch("builtins.open", new_callable=mock_open, read_data='{"max_size_bytes": 1000000000, "backup_directory": "/test", "deletion_strategy": "retention_ladder", "warning_threshold": 0.8}')
    @patch("pathlib.Path.exists", return_value=True)
    def test_thinning_config_load_existing(self, mock_exists, mock_file):
        """Test loading existing configuration file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir)
            thinning_config = ThinningConfig(config_path)
            
            settings = thinning_config.load()
            
            assert settings.max_size_bytes == 1000000000
            assert settings.backup_directory == "/test"
            assert settings.deletion_strategy == "retention_ladder"
            assert settings.warning_threshold == 0.8
    
    @patch("pathlib.Path.exists", return_value=False)
    def test_thinning_config_load_nonexistent(self, mock_exists):
        """Test loading non-existent configuration file creates default."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir)
            thinning_config = ThinningConfig(config_path)
            
            settings = thinning_config.load()
            
            # Should return default settings
            assert settings.max_size_bytes > 0
            assert settings.deletion_strategy in ["retention_ladder", "oldest_first"]
            assert 0 <= settings.warning_threshold <= 1
    
    def test_thinning_config_save(self):
        """Test saving configuration file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir)
            thinning_config = ThinningConfig(config_path)
            
            settings = ThinningSettings(
                max_size_bytes=2000000000,
                backup_directory="/test/save",
                deletion_strategy="oldest_first",
                warning_threshold=0.75
            )
            
            thinning_config.save(settings)
            
            # Verify file was created and contains correct data
            config_file = config_path / "thinning_config.json"
            assert config_file.exists()
            
            with open(config_file, 'r') as f:
                saved_data = json.load(f)
            
            assert saved_data["max_size_bytes"] == 2000000000
            assert saved_data["backup_directory"] == "/test/save"
            assert saved_data["deletion_strategy"] == "oldest_first"
            assert saved_data["warning_threshold"] == 0.75
    
    def test_thinning_config_create_default(self):
        """Test creating default configuration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir)
            thinning_config = ThinningConfig(config_path)
            
            settings = thinning_config.create_default("/test/backups", "2TB")
            
            assert settings.backup_directory == "/test/backups"
            assert settings.max_size_bytes == 2 * 1024 * 1024 * 1024 * 1024  # 2TB in bytes
            assert settings.deletion_strategy == "retention_ladder"
            assert settings.warning_threshold == 0.8


class TestUtilityFunctions:
    """Test utility functions for the thinning module."""
    
    def test_bytes_to_human_readable(self):
        """Test converting bytes to human-readable format."""
        assert bytes_to_human_readable(1024) == "1.0 KB"
        assert bytes_to_human_readable(1048576) == "1.0 MB"
        assert bytes_to_human_readable(1073741824) == "1.0 GB"
        assert bytes_to_human_readable(1099511627776) == "1.0 TB"
        
        # Test with decimal places
        assert bytes_to_human_readable(1536) == "1.5 KB"  # 1.5 * 1024
        assert bytes_to_human_readable(2621440) == "2.5 MB"  # 2.5 * 1024^2
        
        # Test edge cases
        assert bytes_to_human_readable(0) == "0 bytes"
        assert bytes_to_human_readable(512) == "512 bytes"
    
    def test_human_readable_to_bytes(self):
        """Test converting human-readable format to bytes."""
        assert human_readable_to_bytes("1 KB") == 1024
        assert human_readable_to_bytes("1 MB") == 1048576
        assert human_readable_to_bytes("1 GB") == 1073741824
        assert human_readable_to_bytes("1 TB") == 1099511627776
        
        # Test with decimals
        assert human_readable_to_bytes("1.5 KB") == 1536
        assert human_readable_to_bytes("2.5 MB") == 2621440
        
        # Test case insensitive
        assert human_readable_to_bytes("1 kb") == 1024
        assert human_readable_to_bytes("1 Mb") == 1048576
        assert human_readable_to_bytes("1 gb") == 1073741824
        
        # Test without space
        assert human_readable_to_bytes("1KB") == 1024
        assert human_readable_to_bytes("1MB") == 1048576
        
        # Test bytes
        assert human_readable_to_bytes("512 bytes") == 512
        assert human_readable_to_bytes("512") == 512
    
    def test_human_readable_to_bytes_invalid(self):
        """Test human_readable_to_bytes with invalid input."""
        with pytest.raises(ValueError):
            human_readable_to_bytes("invalid")
        
        with pytest.raises(ValueError):
            human_readable_to_bytes("1 XB")  # Invalid unit
        
        with pytest.raises(ValueError):
            human_readable_to_bytes("")
    
    def test_calculate_percentage(self):
        """Test percentage calculation."""
        assert calculate_percentage(50, 100) == 50.0
        assert calculate_percentage(25, 100) == 25.0
        assert calculate_percentage(0, 100) == 0.0
        assert calculate_percentage(100, 100) == 100.0
        
        # Test with decimals
        assert calculate_percentage(33, 100) == 33.0
        assert calculate_percentage(1, 3) == pytest.approx(33.333, rel=1e-3)
    
    def test_calculate_percentage_zero_total(self):
        """Test percentage calculation with zero total."""
        assert calculate_percentage(0, 0) == 0.0
        assert calculate_percentage(10, 0) == 0.0  # Avoid division by zero
    
    def test_format_usage_status(self):
        """Test formatting quota status information."""
        usage_info = {
            "used_bytes": 536870912,  # 512 MB
            "total_bytes": 1073741824,  # 1 GB
            "usage_percentage": 50.0,
            "available_bytes": 536870912,  # 512 MB
        }
        
        result = format_usage_status(usage_info)
        
        assert "512.0 MB" in result  # Used
        assert "1.0 GB" in result    # Total
        assert "50.0%" in result     # Percentage
        assert "512.0 MB" in result  # Available
    
    def test_estimate_backup_size(self):
        """Test backup size estimation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create some test files
            (temp_path / "file1.txt").write_text("Hello, World!" * 100)
            (temp_path / "file2.txt").write_text("Test content" * 200)
            
            # Create subdirectory with files
            sub_dir = temp_path / "subdir"
            sub_dir.mkdir()
            (sub_dir / "file3.txt").write_text("Subdirectory content" * 50)
            
            estimated_size = estimate_backup_size(temp_path)
            
            # Should return a reasonable estimate (greater than 0)
            assert estimated_size > 0
            assert isinstance(estimated_size, int)
    
    def test_estimate_backup_size_nonexistent(self):
        """Test backup size estimation for non-existent directory."""
        result = estimate_backup_size(Path("/nonexistent/path"))
        assert result == 0
    
    def test_validate_thinning_config_valid(self):
        """Test validation of valid quota configuration."""
        max_size_bytes, is_valid = validate_thinning_config("1 GB", 0.8)
        
        assert is_valid is True
        assert max_size_bytes == 1073741824  # 1 GB in bytes
    
    def test_validate_thinning_config_invalid_size(self):
        """Test validation with invalid size format."""
        max_size_bytes, is_valid = validate_thinning_config("invalid size", 0.8)
        
        assert is_valid is False
        assert max_size_bytes == 0
    
    def test_validate_thinning_config_invalid_threshold(self):
        """Test validation with invalid threshold."""
        max_size_bytes, is_valid = validate_thinning_config("1 GB", 1.5)
        
        assert is_valid is False
        assert max_size_bytes == 1073741824  # Size is still parsed correctly
    
    def test_validate_thinning_config_boundary_thresholds(self):
        """Test validation with boundary threshold values."""
        # Valid boundaries
        _, is_valid = validate_thinning_config("1 GB", 0.0)
        assert is_valid is True
        
        _, is_valid = validate_thinning_config("1 GB", 1.0)
        assert is_valid is True
        
        # Invalid boundaries
        _, is_valid = validate_thinning_config("1 GB", -0.1)
        assert is_valid is False
        
        _, is_valid = validate_thinning_config("1 GB", 1.1)
        assert is_valid is False


class TestCreateSampleConfig:
    """Test the create_sample_config utility function."""
    
    def test_create_sample_config(self):
        """Test creating sample configuration file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir)
            
            result_path = create_sample_config(config_path, "/test/backups", "5 GB")
            
            # Should return the path to the created config file
            assert result_path == config_path / "thinning_config.json"
            assert result_path.exists()
            
            # Verify contents
            with open(result_path, 'r') as f:
                config_data = json.load(f)
            
            assert config_data["backup_directory"] == "/test/backups"
            assert config_data["max_size_bytes"] == 5 * 1024 * 1024 * 1024  # 5 GB
            assert config_data["deletion_strategy"] == "retention_ladder"
            assert config_data["warning_threshold"] == 0.8
    
    def test_create_sample_config_default_values(self):
        """Test creating sample configuration with default values."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir)
            
            result_path = create_sample_config(config_path)
            
            assert result_path.exists()
            
            # Verify default values
            with open(result_path, 'r') as f:
                config_data = json.load(f)
            
            assert config_data["backup_directory"] == "/var/backups/atlassian"
            assert config_data["max_size_bytes"] == 1099511627776  # 1 TB
            assert config_data["deletion_strategy"] == "retention_ladder"
            assert config_data["warning_threshold"] == 0.8
    
    def test_create_sample_config_overwrites_existing(self):
        """Test that creating sample config overwrites existing file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir)
            config_file = config_path / "thinning_config.json"
            
            # Create an existing config file
            with open(config_file, 'w') as f:
                json.dump({"old": "config"}, f)
            
            # Create new sample config
            result_path = create_sample_config(config_path, "/new/path", "10 GB")
            
            # Should overwrite the existing file
            with open(result_path, 'r') as f:
                config_data = json.load(f)
            
            assert "old" not in config_data
            assert config_data["backup_directory"] == "/new/path"
            assert config_data["max_size_bytes"] == 10 * 1024 * 1024 * 1024  # 10 GB


# Integration tests for the complete utility workflow
class TestUtilitiesIntegration:
    """Integration tests for utility functions working together."""
    
    def test_size_conversion_roundtrip(self):
        """Test that size conversion functions work correctly together."""
        original_sizes = [
            1024,  # 1 KB
            1048576,  # 1 MB
            1073741824,  # 1 GB
            1536,  # 1.5 KB
            2621440,  # 2.5 MB
        ]
        
        for size in original_sizes:
            # Convert to human readable and back
            human_readable = bytes_to_human_readable(size)
            converted_back = human_readable_to_bytes(human_readable)
            
            # Should be approximately equal (allowing for floating point precision)
            assert abs(converted_back - size) <= 1  # Within 1 byte
    
    def test_thinning_workflow_complete(self):
        """Test complete quota configuration workflow."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir)
            
            # Create sample configuration
            config_file = create_sample_config(config_path, temp_dir, "2 GB")
            
            # Load the configuration
            thinning_config = ThinningConfig(config_path)
            settings = thinning_config.load()
            
            # Verify the configuration
            assert settings.backup_directory == temp_dir
            assert settings.max_size_bytes == 2 * 1024 * 1024 * 1024
            
            # Validate the configuration
            max_size_bytes, is_valid = validate_thinning_config("2 GB", settings.warning_threshold)
            assert is_valid
            assert max_size_bytes == settings.max_size_bytes
            
            # Format status information
            usage_info = {
                "used_bytes": 1073741824,  # 1 GB
                "total_bytes": settings.max_size_bytes,  # 2 GB
                "usage_percentage": 50.0,
                "available_bytes": 1073741824,  # 1 GB
            }
            
            status = format_usage_status(usage_info)
            assert "1.0 GB" in status  # Used
            assert "2.0 GB" in status  # Total
            assert "50.0%" in status   # Usage percentage
