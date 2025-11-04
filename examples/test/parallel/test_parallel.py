"""Tests for parallel example."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import OperationType

from src.parallel import parallel
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=parallel.handler,
    lambda_function_name="Parallel Operations",
)
def test_parallel(durable_runner):
    """Test parallel example."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result) == [
        "Task 1 complete",
        "Task 2 complete",
        "Task 3 complete",
    ]

    # Verify all three step operations exist
    step_ops = [
        op for op in result.operations if op.operation_type == OperationType.STEP
    ]
    assert len(step_ops) == 3

    step_names = {op.name for op in step_ops}
    assert step_names == {"task1", "task2", "task3"}
