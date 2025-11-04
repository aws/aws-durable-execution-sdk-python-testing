"""Tests for callback example."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

from src.callback import callback
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=callback.handler,
    lambda_function_name="callback",
)
def test_callback(durable_runner):
    """Test callback example."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result).startswith(
        "Callback created with ID:"
    )

    # Find the callback operation
    callback_ops = [
        op for op in result.operations if op.operation_type.value == "CALLBACK"
    ]
    assert len(callback_ops) == 1
    callback_op = callback_ops[0]
    assert callback_op.name == "example_callback"
    assert callback_op.callback_id is not None
