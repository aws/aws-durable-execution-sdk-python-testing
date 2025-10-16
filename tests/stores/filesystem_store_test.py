"""Tests for FileSystemExecutionStore."""

import tempfile
from pathlib import Path

import pytest

from aws_durable_execution_sdk_python_testing.exceptions import (
    DurableFunctionsLocalRunnerError,
)
from aws_durable_execution_sdk_python_testing.execution import Execution
from aws_durable_execution_sdk_python_testing.model import StartDurableExecutionInput
from aws_durable_execution_sdk_python_testing.stores.filesystem import (
    FileSystemExecutionStore,
)


@pytest.fixture
def temp_storage_dir():
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def store(temp_storage_dir):
    """Create a FileSystemExecutionStore with temporary storage."""
    return FileSystemExecutionStore.create(temp_storage_dir)


@pytest.fixture
def sample_execution():
    """Create a sample execution for testing."""
    input_data = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    return Execution.new(input_data)


def test_filesystem_execution_store_save_and_load(store, sample_execution):
    """Test saving and loading an execution."""
    store.save(sample_execution)
    loaded_execution = store.load(sample_execution.durable_execution_arn)

    assert (
        loaded_execution.durable_execution_arn == sample_execution.durable_execution_arn
    )
    assert (
        loaded_execution.start_input.function_name
        == sample_execution.start_input.function_name
    )
    assert (
        loaded_execution.start_input.execution_name
        == sample_execution.start_input.execution_name
    )
    assert loaded_execution.token_sequence == sample_execution.token_sequence
    assert loaded_execution.is_complete == sample_execution.is_complete


def test_filesystem_execution_store_load_nonexistent(store):
    """Test loading a nonexistent execution raises DurableFunctionsLocalRunnerError."""
    with pytest.raises(
        DurableFunctionsLocalRunnerError, match="Execution nonexistent-arn not found"
    ):
        store.load("nonexistent-arn")


def test_filesystem_execution_store_update(store, sample_execution):
    """Test updating an execution."""
    store.save(sample_execution)

    sample_execution.is_complete = True
    for _ in range(5):
        sample_execution.get_new_checkpoint_token()
    store.update(sample_execution)

    loaded_execution = store.load(sample_execution.durable_execution_arn)
    assert loaded_execution.is_complete is True
    assert loaded_execution.token_sequence == 5


def test_filesystem_execution_store_update_overwrites(store, temp_storage_dir):
    """Test that update overwrites existing execution."""
    input_data = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
    )
    execution1 = Execution.new(input_data)
    execution2 = Execution.new(input_data)
    execution2.durable_execution_arn = execution1.durable_execution_arn
    for _ in range(10):
        execution2.get_new_checkpoint_token()

    store.save(execution1)
    store.update(execution2)

    loaded_execution = store.load(execution1.durable_execution_arn)
    assert loaded_execution.token_sequence == 10


def test_filesystem_execution_store_multiple_executions(store):
    """Test storing multiple executions."""
    input_data1 = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function-1",
        function_qualifier="$LATEST",
        execution_name="test-execution-1",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
    )
    input_data2 = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function-2",
        function_qualifier="$LATEST",
        execution_name="test-execution-2",
        execution_timeout_seconds=600,
        execution_retention_period_days=14,
    )

    execution1 = Execution.new(input_data1)
    execution2 = Execution.new(input_data2)

    store.save(execution1)
    store.save(execution2)

    loaded_execution1 = store.load(execution1.durable_execution_arn)
    loaded_execution2 = store.load(execution2.durable_execution_arn)

    assert loaded_execution1.durable_execution_arn == execution1.durable_execution_arn
    assert loaded_execution2.durable_execution_arn == execution2.durable_execution_arn
    assert loaded_execution1.start_input.function_name == "test-function-1"
    assert loaded_execution2.start_input.function_name == "test-function-2"


def test_filesystem_execution_store_list_all_empty(store):
    """Test list_all method with empty store."""
    result = store.list_all()
    assert result == []


def test_filesystem_execution_store_list_all_with_executions(store):
    """Test list_all method with multiple executions."""
    # Create test executions
    input_data1 = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function-1",
        function_qualifier="$LATEST",
        execution_name="test-execution-1",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
    )
    input_data2 = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function-2",
        function_qualifier="$LATEST",
        execution_name="test-execution-2",
        execution_timeout_seconds=600,
        execution_retention_period_days=14,
    )
    input_data3 = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function-3",
        function_qualifier="$LATEST",
        execution_name="test-execution-3",
        execution_timeout_seconds=900,
        execution_retention_period_days=21,
    )

    execution1 = Execution.new(input_data1)
    execution2 = Execution.new(input_data2)
    execution3 = Execution.new(input_data3)

    # Save executions
    store.save(execution1)
    store.save(execution2)
    store.save(execution3)

    # Test list_all
    result = store.list_all()

    assert len(result) == 3
    arns = {execution.durable_execution_arn for execution in result}
    assert execution1.durable_execution_arn in arns
    assert execution2.durable_execution_arn in arns
    assert execution3.durable_execution_arn in arns


def test_filesystem_execution_store_file_path_generation(
    store, sample_execution, temp_storage_dir
):
    """Test that file paths are generated correctly with safe filenames."""
    arn_with_colons = "arn:aws:lambda:us-east-1:123456789012:durable-execution:test"
    expected_filename = (
        "arn_aws_lambda_us-east-1_123456789012_durable-execution_test.json"
    )

    # Test by saving and checking the file exists with expected name
    sample_execution.durable_execution_arn = arn_with_colons
    store.save(sample_execution)

    expected_file = temp_storage_dir / expected_filename
    assert expected_file.exists()


def test_filesystem_execution_store_corrupted_file_handling(store, temp_storage_dir):
    """Test that corrupted files are skipped during list_all."""
    # Create a valid execution
    input_data = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
    )
    execution = Execution.new(input_data)
    store.save(execution)

    # Create a corrupted file
    corrupted_file = temp_storage_dir / "corrupted.json"
    with open(corrupted_file, "w") as f:
        f.write("invalid json content")

    # list_all should skip the corrupted file and return only valid executions
    result = store.list_all()
    assert len(result) == 1
    assert result[0].durable_execution_arn == execution.durable_execution_arn


def test_filesystem_execution_store_custom_storage_dir():
    """Test creating store with custom storage directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
        custom_dir = Path(temp_dir) / "custom_storage"
        FileSystemExecutionStore.create(custom_dir)

        # Directory should be created
        assert custom_dir.exists()
        assert custom_dir.is_dir()


def test_filesystem_execution_store_init_no_side_effects():
    """Test that __init__ doesn't create directories (no side effects)."""
    with tempfile.TemporaryDirectory() as temp_dir:
        nonexistent_dir = Path(temp_dir) / "nonexistent"

        # __init__ should not create the directory
        FileSystemExecutionStore(nonexistent_dir)
        assert not nonexistent_dir.exists()


def test_filesystem_execution_store_thread_safety_basic(store, sample_execution):
    """Basic test that operations work without locking (atomic file operations)."""
    # Test that basic operations work - atomic file operations provide thread safety
    store.save(sample_execution)
    loaded = store.load(sample_execution.durable_execution_arn)
    assert loaded.durable_execution_arn == sample_execution.durable_execution_arn
