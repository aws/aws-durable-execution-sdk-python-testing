"""Concurrent access tests for InMemoryExecutionStore."""

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from aws_durable_execution_sdk_python_testing.execution import Execution
from aws_durable_execution_sdk_python_testing.model import StartDurableExecutionInput
from aws_durable_execution_sdk_python_testing.stores.memory import (
    InMemoryExecutionStore,
)


def test_concurrent_save_load():
    """Test concurrent save and load operations."""
    store = InMemoryExecutionStore()
    results = []
    results_lock = threading.Lock()

    def save_execution(i: int):
        input_data = StartDurableExecutionInput(
            account_id="123456789012",
            function_name="test-function",
            function_qualifier="$LATEST",
            execution_name=f"test-{i}",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id=f"inv-{i}",
            input=f'{{"test": {i}}}',
        )
        execution = Execution.new(input_data)
        execution.durable_execution_arn = f"arn-{i}"
        store.save(execution)
        with results_lock:
            results.append(f"saved-{i}")

    def load_execution(i: int):
        try:
            execution = store.load(f"arn-{i}")
            with results_lock:
                results.append(f"loaded-{execution.start_input.execution_name}")
        except KeyError:
            with results_lock:
                results.append(f"not-found-{i}")

    with ThreadPoolExecutor(max_workers=10) as executor:
        # Submit save operations first
        futures = [executor.submit(save_execution, i) for i in range(5)]
        # Wait for saves to complete
        for future in as_completed(futures):
            future.result()

        # Then submit load operations
        futures = []
        for i in range(5):
            futures.append(executor.submit(load_execution, i))
        # Wait for loads to complete
        for future in as_completed(futures):
            future.result()

    assert len(results) == 10


def test_concurrent_update_list():
    """Test concurrent update and list operations."""
    store = InMemoryExecutionStore()
    results = []
    results_lock = threading.Lock()

    # Pre-populate store
    for i in range(3):
        input_data = StartDurableExecutionInput(
            account_id="123456789012",
            function_name="test-function",
            function_qualifier="$LATEST",
            execution_name=f"test-{i}",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id=f"inv-{i}",
            input=f'{{"test": {i}}}',
        )
        execution = Execution.new(input_data)
        execution.durable_execution_arn = f"arn-{i}"
        store.save(execution)

    def update_execution(i: int):
        execution = store.load(f"arn-{i}")
        execution.is_complete = True
        store.update(execution)
        with results_lock:
            results.append(f"updated-{i}")

    def list_executions():
        executions = store.list_all()
        with results_lock:
            results.append(f"listed-{len(executions)}")

    with ThreadPoolExecutor(max_workers=6) as executor:
        # Submit update operations
        futures = [executor.submit(update_execution, i) for i in range(3)]
        # Submit list operations
        futures.extend([executor.submit(list_executions) for _ in range(3)])

        # Wait for all operations to complete
        for future in as_completed(futures):
            future.result()

    assert len(results) == 6
    final_list = store.list_all()
    assert len(final_list) == 3
