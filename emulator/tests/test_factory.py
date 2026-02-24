"""Tests for the component factory"""

import os
import tempfile
from pathlib import Path

from aws_durable_execution_sdk_python_testing.stores.base import StoreType

from aws_lambda_durable_functions_emulator.config import EmulatorConfig
from aws_lambda_durable_functions_emulator.factory import TestingLibraryComponentFactory


def test_create_store_filesystem():
    """Test that filesystem store can be created"""
    with tempfile.TemporaryDirectory() as temp_dir:
        os.environ["EXECUTION_STORE_TYPE"] = StoreType.FILESYSTEM.value
        os.environ["STORAGE_DIR"] = temp_dir

        config = EmulatorConfig()
        store = TestingLibraryComponentFactory.create_store(config)

        assert store is not None
        assert "FileSystemExecutionStore" in str(type(store))

        # Clean up
        del os.environ["EXECUTION_STORE_TYPE"]
        del os.environ["STORAGE_DIR"]


def test_create_store_sqlite():
    """Test that SQLite store can be created"""
    with tempfile.TemporaryDirectory() as temp_dir:
        os.environ["EXECUTION_STORE_TYPE"] = StoreType.SQLITE.value
        os.environ["STORAGE_DIR"] = temp_dir

        config = EmulatorConfig()
        store = TestingLibraryComponentFactory.create_store(config)

        assert store is not None
        assert "SQLiteExecutionStore" in str(type(store))

        # Verify database file was created
        db_path = Path(temp_dir) / "durable-executions.db"
        assert db_path.exists()

        # Clean up
        del os.environ["EXECUTION_STORE_TYPE"]
        del os.environ["STORAGE_DIR"]


def test_create_store_default():
    """Test that default store type is sqlite"""
    # Clean up any existing env vars
    for key in ["EXECUTION_STORE_TYPE", "STORAGE_DIR"]:
        if key in os.environ:
            del os.environ[key]

    config = EmulatorConfig()
    store = TestingLibraryComponentFactory.create_store(config)

    assert store is not None
    assert "SQLiteExecutionStore" in str(type(store))


def test_create_scheduler():
    """Test that scheduler can be created"""
    scheduler = TestingLibraryComponentFactory.create_scheduler()
    assert scheduler is not None


def test_create_invoker():
    """Test that invoker can be created"""
    config = EmulatorConfig()
    invoker = TestingLibraryComponentFactory.create_invoker(config)
    assert invoker is not None


def test_create_executor():
    """Test that executor can be created with all components"""
    config = EmulatorConfig()
    executor = TestingLibraryComponentFactory.create_executor(config)
    assert executor is not None
