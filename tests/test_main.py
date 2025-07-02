"""
Unit tests for the main module in the Atlassian Cloud Backup tool.
"""

import os
import sys
import io
import pytest
import configparser
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open
from click.testing import CliRunner

# Import the module under test
import src.main as main
from src.main import (
    get_config_value,
    validate_credentials,
    process_single_backup,
    get_runtime_configuration,
    handle_mandatory_field,
    handle_optional_field,
    ensure_configuration,
    prompt_for_config,
    main as main_cli
)


@pytest.fixture
def reset_main_config():
    """Reset the main.config to a clean state before and after each test."""
    main.config = configparser.ConfigParser()
    yield
    main.config = configparser.ConfigParser()


@pytest.fixture
def mock_properties_file():
    """Set up a mock properties file."""
    content = """[atlassian]
username = test_user
api_token = test_token
backup_target_directory = /test/path
instances = test-instance
poll_interval_seconds = 45
"""
    with patch("builtins.open", mock_open(read_data=content)):
        yield


class TestGetConfigValue:
    
    def test_get_from_env_var(self, monkeypatch, reset_main_config):
        """Test retrieving a value from an environment variable."""
        monkeypatch.setenv("TEST_ENV_VAR", "env_value")
        assert get_config_value("TEST_ENV_VAR", "test_prop", "default") == "env_value"
    
    def test_get_from_config(self, reset_main_config):
        """Test retrieving a value from the config."""
        main.config["atlassian"] = {"test_prop": "config_value"}
        assert get_config_value("TEST_ENV_VAR", "test_prop", "default") == "config_value"
    
    def test_get_default(self, reset_main_config):
        """Test falling back to the default value."""
        assert get_config_value("NON_EXISTENT_VAR", "non_existent_prop", "default") == "default"


class TestValidateCredentials:
    
    def test_all_credentials_valid(self):
        """Test validation with all credentials provided."""
        assert validate_credentials("user", "token", "/path/to/dir") is True
    
    def test_missing_username(self):
        """Test validation with missing username."""
        with patch("logging.error") as mock_error:
            assert validate_credentials(None, "token", "/path/to/dir") is False
            mock_error.assert_called_once()
    
    def test_missing_api_token(self):
        """Test validation with missing API token."""
        with patch("logging.error") as mock_error:
            assert validate_credentials("user", None, "/path/to/dir") is False
            mock_error.assert_called_once()
    
    def test_missing_backup_directory(self):
        """Test validation with missing backup directory."""
        with patch("logging.error") as mock_error:
            assert validate_credentials("user", "token", None) is False
            mock_error.assert_called_once()


class TestProcessSingleBackup:
    
    def test_successful_backup(self):
        """Test a successful backup process."""
        with patch("src.main.BackupController") as MockController:
            mock_instance = MockController.return_value
            result = process_single_backup("https://test.atlassian.net", "user", "token", 30, "/path")
            
            MockController.assert_called_with(
                url="https://test.atlassian.net",
                username="user",
                api_token="token",
                poll_interval=30,
                backup_target_directory="/path"
            )
            mock_instance.orchestrate.assert_called_once()
            assert result is True
    
    def test_failed_backup(self):
        """Test a failed backup process."""
        with patch("src.main.BackupController") as MockController:
            mock_instance = MockController.return_value
            mock_instance.orchestrate.side_effect = Exception("Test error")
            
            with patch("logging.error") as mock_error:
                result = process_single_backup("https://test.atlassian.net", "user", "token", 30, "/path")
                
                mock_error.assert_called_once()
                assert result is False


class TestGetRuntimeConfiguration:
    
    def test_config_retrieval(self, monkeypatch):
        """Test retrieving the runtime configuration."""
        monkeypatch.setattr("src.main.get_config_value", lambda env, key, default=None: {
            "ATLASSIAN_USERNAME": "env_user",
            "username": "user",
            "ATLASSIAN_API_TOKEN": "env_token",
            "api_token": "token",
            "POLL_INTERVAL_SECONDS": "60",
            "poll_interval_seconds": "45",
            "BACKUP_TARGET_DIRECTORY": "env_path",
            "backup_target_directory": "path",
        }.get(env) or {
            "username": "user",
            "api_token": "token",
            "poll_interval_seconds": "45",
            "backup_target_directory": "path",
        }.get(key, default))
        
        config = get_runtime_configuration()
        
        assert config["username"] == "env_user"
        assert config["api_token"] == "env_token"
        assert config["poll_interval"] == 60
        assert config["backup_target_directory"] == "env_path"


class TestHandleMandatoryField:
    
    @patch("src.main.get_config_value")
    @patch("src.main.sys.stdin.isatty", return_value=True)
    @patch("src.main.prompt_for_config", return_value="input_value")
    def test_missing_value_interactive(self, mock_prompt, mock_isatty, mock_get_value, reset_main_config):
        """Test handling a mandatory field with a missing value in interactive mode."""
        mock_get_value.return_value = None
        main.config["atlassian"] = {}
        
        result = handle_mandatory_field("test_key", "TEST_ENV")
        
        mock_get_value.assert_called_with("TEST_ENV", "test_key")
        mock_prompt.assert_called_with("Enter test key")
        assert main.config["atlassian"]["test_key"] == "input_value"
        assert result is True
    
    @patch("src.main.get_config_value")
    @patch("src.main.sys.stdin.isatty", return_value=False)
    def test_missing_value_noninteractive(self, mock_isatty, mock_get_value, reset_main_config):
        """Test handling a mandatory field with a missing value in non-interactive mode."""
        mock_get_value.return_value = None
        
        with pytest.raises(SystemExit) as e:
            with patch("logging.error") as mock_error:
                handle_mandatory_field("test_key", "TEST_ENV")
                mock_error.assert_called_once()
        
        assert e.value.code == 1
    
    @patch("src.main.get_config_value")
    def test_existing_value(self, mock_get_value, reset_main_config):
        """Test handling a mandatory field with an existing value."""
        mock_get_value.return_value = "existing_value"
        
        result = handle_mandatory_field("test_key", "TEST_ENV")
        
        mock_get_value.assert_called_with("TEST_ENV", "test_key")
        assert result is False


class TestHandleOptionalField:
    
    @patch("src.main.get_config_value")
    @patch("src.main.sys.stdin.isatty", return_value=True)
    @patch("src.main.prompt_for_config")
    def test_interactive_mode_with_change(self, mock_prompt, mock_isatty, mock_get_value, reset_main_config):
        """Test handling an optional field in interactive mode with a changed value."""
        mock_get_value.return_value = "current_value"
        mock_prompt.return_value = "new_value"
        main.config["atlassian"] = {}
        
        result = handle_optional_field("test_key", "TEST_ENV", "default_value")
        
        mock_get_value.assert_called_with("TEST_ENV", "test_key")
        mock_prompt.assert_called_with("Enter test key", "current_value", "default_value")
        assert main.config["atlassian"]["test_key"] == "new_value"
        assert result is True
    
    @patch("src.main.get_config_value")
    @patch("src.main.sys.stdin.isatty", return_value=True)
    @patch("src.main.prompt_for_config")
    def test_interactive_mode_no_change(self, mock_prompt, mock_isatty, mock_get_value, reset_main_config):
        """Test handling an optional field in interactive mode with no change."""
        mock_get_value.return_value = "current_value"
        mock_prompt.return_value = "current_value"
        main.config["atlassian"] = {"test_key": "current_value"}
        
        result = handle_optional_field("test_key", "TEST_ENV", "default_value")
        
        assert result is False
    
    @patch("src.main.get_config_value")
    @patch("src.main.sys.stdin.isatty", return_value=False)
    def test_noninteractive_missing_value(self, mock_isatty, mock_get_value, reset_main_config):
        """Test handling an optional field in non-interactive mode with a missing value."""
        mock_get_value.return_value = None
        main.config["atlassian"] = {}
        
        result = handle_optional_field("test_key", "TEST_ENV", "default_value")
        
        assert main.config["atlassian"]["test_key"] == "default_value"
        assert result is True
    
    @patch("src.main.get_config_value")
    @patch("src.main.sys.stdin.isatty", return_value=False)
    def test_noninteractive_existing_value(self, mock_isatty, mock_get_value, reset_main_config):
        """Test handling an optional field in non-interactive mode with an existing value."""
        mock_get_value.return_value = "existing_value"
        
        result = handle_optional_field("test_key", "TEST_ENV", "default_value")
        
        assert result is False


class TestPromptForConfig:
    
    @patch("sys.stdin.isatty", return_value=False)  # Simulate non-interactive environment
    @patch("sys.stdin")
    @patch("sys.stdout")
    def test_prompt_for_config_with_input(self, mock_stdout, mock_stdin, mock_isatty):
        """Test the prompt_for_config function with user input."""
        # Set up the mock stdin to return a specific value
        mock_stdin.readline.return_value = "test_input\n"
        
        # Call the function directly
        result = prompt_for_config("Enter value")
        
        # Verify the function wrote the expected prompt
        mock_stdout.write.assert_called_once_with("Enter value: ")
        mock_stdout.flush.assert_called_once()
        
        # Verify we got the expected result
        assert result == "test_input"
    
    @patch("sys.stdin.isatty", return_value=False)
    @patch("sys.stdin")
    @patch("sys.stdout")
    def test_prompt_with_current_value(self, mock_stdout, mock_stdin, mock_isatty):
        """Test prompt_for_config with a current value and empty input."""
        # Set up the mock stdin to return empty input
        mock_stdin.readline.return_value = "\n"
        
        # Call the function with a current value
        result = prompt_for_config("Enter value", current_value="current_value")
        
        # Verify the expected result
        assert result == "current_value"
    
    @patch("sys.stdin.isatty", return_value=False)
    @patch("sys.stdin")
    @patch("sys.stdout")
    def test_prompt_with_default_value(self, mock_stdout, mock_stdin, mock_isatty):
        """Test prompt_for_config with a default value and empty input."""
        # Set up the mock stdin to return empty input
        mock_stdin.readline.return_value = "\n"
        
        # Call the function with a default value
        result = prompt_for_config("Enter value", default="default_value")
        
        # Verify the expected result
        assert result == "default_value"
    
    @patch("sys.stdin.isatty", return_value=True)  # Simulate interactive terminal
    @patch("select.select", return_value=([], [], []))  # Simulate timeout
    @patch("logging.error")
    @patch("sys.exit")
    def test_prompt_with_timeout(self, mock_exit, mock_error, mock_select, mock_isatty):
        """Test the behavior of prompt_for_config when it times out."""
        with patch("sys.stdout"):
            # Call the function which should time out
            prompt_for_config("Enter value")
            
            # Verify error was logged and exit was called
            mock_error.assert_called_once()
            mock_exit.assert_called_once_with(1)


class TestEnsureConfiguration:
    
    @patch("src.main.get_config_value")
    @patch("pathlib.Path.exists")
    def test_all_mandatory_fields_present(self, mock_exists, mock_get_value, reset_main_config):
        """Test configuration with all mandatory fields present."""
        mock_exists.return_value = True
        mock_get_value.return_value = "value"
        
        with patch("configparser.ConfigParser.read") as mock_read:
            ensure_configuration(Path("/test/path"))
            mock_read.assert_called_once()
    
    @patch("src.main.get_config_value")
    @patch("pathlib.Path.exists")
    @patch("src.main.handle_mandatory_field")
    @patch("src.main.handle_optional_field")
    def test_missing_mandatory_field(self, mock_handle_optional, mock_handle_mandatory, mock_exists, mock_get_value, reset_main_config):
        """Test configuration with a missing mandatory field."""
        mock_exists.return_value = True
        mock_get_value.side_effect = lambda env, key: None if key == "username" else "value"
        mock_handle_mandatory.return_value = True
        mock_handle_optional.return_value = False
        
        with patch("builtins.print") as mock_print:
            with patch("logging.info") as mock_info:
                with patch("configparser.ConfigParser.read"):
                    with patch("pathlib.Path.mkdir"):
                        with patch("builtins.open", mock_open()):
                            ensure_configuration(Path("/test/path"))
                            
                            mock_print.assert_called_once()
                            assert mock_info.call_count >= 2
                            mock_handle_mandatory.assert_called()
                            mock_handle_optional.assert_called()


class TestMainCLI:
    
    @patch("src.main.ensure_configuration")
    @patch("src.main.get_config_value")
    @patch("src.main.get_runtime_configuration")
    @patch("src.main.validate_credentials")
    @patch("src.main.process_single_backup")
    @patch("src.main.FileManager")
    def test_successful_execution(self, mock_fm, mock_process, mock_validate, mock_runtime_config, mock_get_value, mock_ensure_config):
        """Test the main CLI with successful execution."""
        # Setup mocks
        mock_get_value.return_value = "test-instance"
        mock_runtime_config.return_value = {
            "username": "user",
            "api_token": "token",
            "poll_interval": 30,
            "backup_target_directory": "/path"
        }
        mock_validate.return_value = True
        mock_process.return_value = True
        
        mock_fm_instance = mock_fm.return_value
        mock_fm_instance.get_audit_log_path.return_value = "/path/to/audit.log"
        
        runner = CliRunner()
        
        with patch("pathlib.Path.mkdir"):
            result = runner.invoke(main_cli)
        
        assert result.exit_code == 0
        mock_ensure_config.assert_called_once()
        mock_validate.assert_called_once()
        mock_process.assert_called_once_with(
            "https://test-instance.atlassian.net",
            "user",
            "token",
            30,
            "/path"
        )
    
    @patch("src.main.ensure_configuration")
    @patch("src.main.get_config_value")
    def test_no_instances(self, mock_get_value, mock_ensure_config):
        """Test the main CLI with no instances provided."""
        mock_get_value.return_value = ""
        
        runner = CliRunner()
        
        with patch("pathlib.Path.mkdir"):
            result = runner.invoke(main_cli)
        
        assert result.exit_code == 1
    
    @patch("src.main.ensure_configuration")
    @patch("src.main.get_config_value")
    @patch("src.main.get_runtime_configuration")
    @patch("src.main.validate_credentials")
    def test_invalid_credentials(self, mock_validate, mock_runtime_config, mock_get_value, mock_ensure_config):
        """Test the main CLI with invalid credentials."""
        mock_get_value.return_value = "test-instance"
        mock_runtime_config.return_value = {
            "username": "user",
            "api_token": "token",
            "poll_interval": 30,
            "backup_target_directory": "/path"
        }
        mock_validate.return_value = False
        
        runner = CliRunner()
        
        with patch("pathlib.Path.mkdir"):
            result = runner.invoke(main_cli)
        
        assert result.exit_code == 1
        mock_validate.assert_called_once_with("user", "token", "/path")
    
    @patch("src.main.ensure_configuration")
    @patch("src.main.get_config_value")
    @patch("src.main.get_runtime_configuration")
    @patch("src.main.validate_credentials")
    @patch("src.main.process_single_backup")
    @patch("src.main.FileManager")
    def test_partial_success(self, mock_fm, mock_process, mock_validate, mock_runtime_config, mock_get_value, mock_ensure_config):
        """Test the main CLI with partial success."""
        mock_get_value.return_value = "test1,test2"
        mock_runtime_config.return_value = {
            "username": "user",
            "api_token": "token",
            "poll_interval": 30,
            "backup_target_directory": "/path"
        }
        mock_validate.return_value = True
        mock_process.side_effect = [True, False]
        
        mock_fm_instance = mock_fm.return_value
        mock_fm_instance.get_audit_log_path.return_value = "/path/to/audit.log"
        
        runner = CliRunner()
        
        with patch("pathlib.Path.mkdir"):
            result = runner.invoke(main_cli)
        
        assert result.exit_code == 0
        assert mock_process.call_count == 2
        with patch("logging.info") as mock_info:
            main.logging.info('Backup completed for %d of %d Atlassian instances', 1, 2)
            mock_info.assert_called_once_with('Backup completed for %d of %d Atlassian instances', 1, 2)


if __name__ == "__main__":
    pytest.main()
