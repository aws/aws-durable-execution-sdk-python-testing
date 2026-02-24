"""Tests for the emulator configuration"""

import os

import pytest
from aws_durable_execution_sdk_python_testing.stores.base import StoreType

from aws_lambda_durable_functions_emulator.config import EmulatorConfig


def test_execution_store_type_default():
    """Test that default execution store type is sqlite"""
    # Clean up any existing env vars
    if "EXECUTION_STORE_TYPE" in os.environ:
        del os.environ["EXECUTION_STORE_TYPE"]

    config = EmulatorConfig()
    assert config.execution_store_type == StoreType.SQLITE.value


def test_execution_store_type_filesystem():
    """Test filesystem execution store type"""
    os.environ["EXECUTION_STORE_TYPE"] = StoreType.FILESYSTEM.value

    config = EmulatorConfig()
    assert config.execution_store_type == StoreType.FILESYSTEM.value

    # Clean up
    del os.environ["EXECUTION_STORE_TYPE"]


def test_execution_store_type_sqlite():
    """Test SQLite execution store type"""
    os.environ["EXECUTION_STORE_TYPE"] = StoreType.SQLITE.value

    config = EmulatorConfig()
    assert config.execution_store_type == StoreType.SQLITE.value

    # Clean up
    del os.environ["EXECUTION_STORE_TYPE"]


def test_execution_store_type_case_insensitive():
    """Test that execution store type is case insensitive"""
    os.environ["EXECUTION_STORE_TYPE"] = "SQLITE"

    config = EmulatorConfig()
    assert config.execution_store_type == StoreType.SQLITE.value

    # Clean up
    del os.environ["EXECUTION_STORE_TYPE"]


def test_execution_store_type_invalid():
    """Test that invalid execution store type raises ValueError"""
    os.environ["EXECUTION_STORE_TYPE"] = "invalid"

    with pytest.raises(ValueError, match="Invalid execution store type"):
        EmulatorConfig()

    # Clean up
    del os.environ["EXECUTION_STORE_TYPE"]


def test_execution_store_type_validation():
    """Test that only valid store types are accepted"""
    valid_types = [StoreType.FILESYSTEM.value, StoreType.SQLITE.value]

    for store_type in valid_types:
        os.environ["EXECUTION_STORE_TYPE"] = store_type
        config = EmulatorConfig()
        assert config.execution_store_type == store_type
        del os.environ["EXECUTION_STORE_TYPE"]
