"""Tests for store module."""

import pytest

from aws_durable_functions_sdk_python_testing.execution import Execution
from aws_durable_functions_sdk_python_testing.model import StartDurableExecutionInput
from aws_durable_functions_sdk_python_testing.store import InMemoryExecutionStore


def test_in_memory_execution_store_save_and_load():
    """Test saving and loading an execution."""
    store = InMemoryExecutionStore()
    input_data = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    execution = Execution.new(input_data)

    store.save(execution)
    loaded_execution = store.load(execution.durable_execution_arn)

    assert loaded_execution is execution


def test_in_memory_execution_store_load_nonexistent():
    """Test loading a nonexistent execution raises KeyError."""
    store = InMemoryExecutionStore()

    with pytest.raises(KeyError):
        store.load("nonexistent-arn")


def test_in_memory_execution_store_update():
    """Test updating an execution."""
    store = InMemoryExecutionStore()
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

    execution.is_complete = True
    store.update(execution)

    loaded_execution = store.load(execution.durable_execution_arn)
    assert loaded_execution.is_complete is True


def test_in_memory_execution_store_update_overwrites():
    """Test that update overwrites existing execution."""
    store = InMemoryExecutionStore()
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

    store.save(execution1)
    store.update(execution2)

    loaded_execution = store.load(execution1.durable_execution_arn)
    assert loaded_execution is execution2


def test_in_memory_execution_store_multiple_executions():
    """Test storing multiple executions."""
    store = InMemoryExecutionStore()
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

    assert loaded_execution1 is execution1
    assert loaded_execution2 is execution2
