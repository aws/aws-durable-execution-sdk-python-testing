"""Tests for run_in_child_context example."""

from aws_durable_execution_sdk_python.execution import InvocationStatus

from aws_durable_execution_sdk_python_testing.runner import (
    DurableFunctionTestResult,
    DurableFunctionTestRunner,
)
from src import run_in_child_context


def test_run_in_child_context():
    """Test run_in_child_context example."""
    with DurableFunctionTestRunner(handler=run_in_child_context.handler) as runner:
        result: DurableFunctionTestResult = runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result == "Child context result: 10"

    # Verify child context operation exists
    context_ops = [
        op for op in result.operations if op.operation_type.value == "CONTEXT"
    ]
    assert len(context_ops) >= 1
