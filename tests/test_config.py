import os
import pytest
import configparser

import src.main as main_module

@pytest.fixture(autouse=True)
def clear_env_and_config(monkeypatch, tmp_path):
    # Clear environment variable for test
    monkeypatch.delenv('TEST_ENV', raising=False)
    # Reset and ensure empty config
    main_module.config = configparser.ConfigParser()
    yield


def test_env_var_overrides_file(monkeypatch):
    # Environment variable takes precedence
    monkeypatch.setenv('TEST_ENV', 'from_env')
    # Also set config file value
    main_module.config.read_dict({'atlassian': {'test_key': 'from_file'}})
    result = main_module.get_config_value('TEST_ENV', 'test_key', 'default')
    assert result == 'from_env'


def test_file_value_used_when_no_env(monkeypatch):
    # No env var
    monkeypatch.delenv('TEST_ENV', raising=False)
    # Set config file value
    main_module.config.read_dict({'atlassian': {'test_key': 'from_file'}})
    result = main_module.get_config_value('TEST_ENV', 'test_key', 'default')
    assert result == 'from_file'


def test_default_returned_when_missing(monkeypatch):
    # Ensure no env var and no config entry
    monkeypatch.delenv('TEST_ENV', raising=False)
    # Config is empty
    result = main_module.get_config_value('TEST_ENV', 'missing_key', 'default_value')
    assert result == 'default_value'


def test_default_none_when_no_default_specified(monkeypatch):
    monkeypatch.delenv('TEST_ENV', raising=False)
    result = main_module.get_config_value('TEST_ENV', 'missing_key')
    assert result is None
