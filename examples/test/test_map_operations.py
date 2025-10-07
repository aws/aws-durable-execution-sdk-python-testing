"""Tests for map_operations example."""

from aws_durable_execution_sdk_python.execution import InvocationStatus

from aws_durable_execution_sdk_python_testing.runner import (
    DurableFunctionTestResult,
    DurableFunctionTestRunner,
)
from src import map_operations


def test_map_operations():
    """Test map_operations example."""
    with DurableFunctionTestRunner(handler=map_operations.handler) as runner:
        result: DurableFunctionTestResult = runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result == "Squared results: [1, 4, 9, 16, 25]"

    # Verify all five step operations exist
    step_ops = [op for op in result.operations if op.operation_type.value == "STEP"]
    assert len(step_ops) == 5

    step_names = {op.name for op in step_ops}
    expected_names = {f"square_{i}" for i in range(5)}
    assert step_names == expected_names
