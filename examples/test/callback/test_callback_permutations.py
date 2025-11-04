"""Tests for callback operation permutations."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

from src.callback import callback_with_timeout
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=callback_with_timeout.handler,
    lambda_function_name="callback with timeout",
)
def test_callback_with_timeout(durable_runner):
    """Test callback with custom timeout configuration."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result).startswith(
        "Callback created with 60s timeout:"
    )

    callback_ops = [
        op for op in result.operations if op.operation_type.value == "CALLBACK"
    ]
    assert len(callback_ops) == 1
    assert callback_ops[0].name == "timeout_callback"
    assert callback_ops[0].callback_id is not None
