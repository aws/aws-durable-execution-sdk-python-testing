"""Tests for steps_with_retry."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import OperationType

from src import steps_with_retry
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=steps_with_retry.handler,
    lambda_function_name="steps with retry",
)
def test_steps_with_retry(durable_runner):
    """Test steps_with_retry pattern."""
    with durable_runner:
        result = durable_runner.run(input={"name": "test-item"}, timeout=30)

    assert result.status is InvocationStatus.SUCCEEDED

    # Result should be either success with item or error
    assert isinstance(deserialize_operation_payload(result.result), dict)
    assert "success" in deserialize_operation_payload(
        result.result
    ) or "error" in deserialize_operation_payload(result.result)

    # Verify step operations exist (polling steps)
    step_ops = [
        op for op in result.operations if op.operation_type == OperationType.STEP
    ]
    assert len(step_ops) >= 1
