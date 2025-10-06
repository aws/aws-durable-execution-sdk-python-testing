"""Tests for callback operation permutations."""

from aws_durable_execution_sdk_python.execution import InvocationStatus

from aws_durable_execution_sdk_python_testing.runner import (
    DurableFunctionTestResult,
    DurableFunctionTestRunner,
)
from src import callback_with_timeout


def test_callback_with_timeout():
    """Test callback with custom timeout configuration."""
    with DurableFunctionTestRunner(handler=callback_with_timeout.handler) as runner:
        result: DurableFunctionTestResult = runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result.startswith("Callback created with 60s timeout:")

    callback_ops = [
        op for op in result.operations if op.operation_type.value == "CALLBACK"
    ]
    assert len(callback_ops) == 1
    assert callback_ops[0].name == "timeout_callback"
    assert callback_ops[0].callback_id is not None
