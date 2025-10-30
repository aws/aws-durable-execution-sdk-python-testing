"""Tests for wait example."""

from aws_durable_execution_sdk_python.execution import InvocationStatus

from aws_durable_execution_sdk_python_testing.runner import (
    DurableFunctionTestResult,
    DurableFunctionTestRunner,
)
from src import wait


def test_wait():
    """Test wait example."""
    with DurableFunctionTestRunner(handler=wait.handler) as runner:
        result: DurableFunctionTestResult = runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result == "Wait completed"

    # Find the wait operation (it should be the only non-execution operation)
    wait_ops = [op for op in result.operations if op.operation_type.value == "WAIT"]
    assert len(wait_ops) == 1
    wait_op = wait_ops[0]
    assert wait_op.scheduled_end_timestamp is not None
