"""Tests for parallel example."""

from aws_durable_execution_sdk_python.execution import InvocationStatus

from aws_durable_execution_sdk_python_testing.runner import (
    DurableFunctionTestResult,
    DurableFunctionTestRunner,
)
from src import parallel


def test_parallel():
    """Test parallel example."""
    with DurableFunctionTestRunner(handler=parallel.handler) as runner:
        result: DurableFunctionTestResult = runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result == "Results: Task 1 complete, Task 2 complete, Task 3 complete"

    # Verify all three step operations exist
    step_ops = [op for op in result.operations if op.operation_type.value == "STEP"]
    assert len(step_ops) == 3

    step_names = {op.name for op in step_ops}
    assert step_names == {"task1", "task2", "task3"}
