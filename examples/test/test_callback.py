"""Tests for callback example."""

from aws_durable_execution_sdk_python.execution import InvocationStatus

from aws_durable_execution_sdk_python_testing.runner import (
    DurableFunctionTestResult,
    DurableFunctionTestRunner,
)
from src import callback


def test_callback():
    """Test callback example."""
    with DurableFunctionTestRunner(handler=callback.handler) as runner:
        result: DurableFunctionTestResult = runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result.startswith("Callback created with ID:")

    # Find the callback operation
    callback_ops = [
        op for op in result.operations if op.operation_type.value == "CALLBACK"
    ]
    assert len(callback_ops) == 1
    callback_op = callback_ops[0]
    assert callback_op.name == "example_callback"
    assert callback_op.callback_id is not None
