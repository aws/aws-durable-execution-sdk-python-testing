"""Tests for callback operation permutations."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

from src.callback import callback_with_timeout


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=callback_with_timeout.handler,
    lambda_function_name="Callback With Heartbeat Timeout",
)
def test_callback_with_heartbeat_timeout(durable_runner):
    """Test callback with custom timeout configuration."""
    test_payload = {"timeoutType": "heartbeat"}
    with durable_runner:
        result = durable_runner.run(input=test_payload, timeout=20)

    assert result.status is InvocationStatus.FAILED
    error = result.error
    assert error is not None
    assert error.message == "Callback timed out on heartbeat"
    assert error.type == "CallbackError"
    assert error.data is None
    assert error.stack_trace is None


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=callback_with_timeout.handler,
    lambda_function_name="Callback With General Timeout",
)
def test_callback_with_general_timeout(durable_runner):
    """Test callback with custom timeout configuration."""
    test_payload = {"timeoutType": "general"}
    with durable_runner:
        result = durable_runner.run(input=test_payload, timeout=20)

    assert result.status is InvocationStatus.FAILED
    error = result.error
    assert error is not None
    assert error.message == "Callback timed out"
    assert error.type == "CallbackError"
    assert error.data is None
    assert error.stack_trace is None
