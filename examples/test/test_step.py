"""Tests for step example."""

from aws_durable_execution_sdk_python.execution import InvocationStatus

from aws_durable_execution_sdk_python_testing.runner import (
    DurableFunctionTestResult,
    DurableFunctionTestRunner,
    StepOperation,
)
from src import step


def test_step():
    """Test basic step example."""
    with DurableFunctionTestRunner(handler=step.handler) as runner:
        result: DurableFunctionTestResult = runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result == 8

    step_result: StepOperation = result.get_step("add_numbers")
    assert step_result.result == 8
