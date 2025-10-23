"""Tests for wait_for_condition."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import OperationType

from src import wait_for_condition
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=wait_for_condition.handler,
    lambda_function_name="wait for condition",
)
def test_wait_for_condition(durable_runner):
    """Test wait_for_condition pattern."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=15)

    assert result.status is InvocationStatus.SUCCEEDED
    # Should reach state 3 after 3 increments
    assert deserialize_operation_payload(result.result) == 3

    # Verify step operations exist (should have 3 increment steps)
    step_ops = [
        op for op in result.operations if op.operation_type == OperationType.STEP
    ]
    assert len(step_ops) == 3

    # Verify wait operations exist (should have 2 waits before final state)
    wait_ops = [op for op in result.operations if op.operation_type.value == "WAIT"]
    assert len(wait_ops) == 2
