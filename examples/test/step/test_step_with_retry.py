"""Tests for step_with_retry example."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import OperationType

from src.step import step_with_retry
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=step_with_retry.handler,
    lambda_function_name="step with retry",
)
def test_step_with_retry(durable_runner):
    """Test step with retry configuration."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=30)

    # The function uses random() so it may succeed or fail
    # We just verify it completes and has retry configuration
    assert result.status in [InvocationStatus.SUCCEEDED, InvocationStatus.FAILED]

    # Verify step operation exists
    step_ops = [
        op for op in result.operations if op.operation_type == OperationType.STEP
    ]
    assert len(step_ops) >= 1

    # If it succeeded, verify the result
    if result.status is InvocationStatus.SUCCEEDED:
        assert deserialize_operation_payload(result.result) == "Operation succeeded"
