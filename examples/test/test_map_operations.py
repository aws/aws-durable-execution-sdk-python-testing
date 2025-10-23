"""Tests for map_operations example."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import OperationType

from src import map_operations
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=map_operations.handler,
    lambda_function_name="map operations",
)
def test_map_operations(durable_runner):
    """Test map_operations example."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result) == [1, 4, 9, 16, 25]

    # Verify all five step operations exist
    step_ops = [
        op for op in result.operations if op.operation_type == OperationType.STEP
    ]
    assert len(step_ops) == 5

    step_names = {op.name for op in step_ops}
    expected_names = {f"square_{i}" for i in range(5)}
    assert step_names == expected_names
