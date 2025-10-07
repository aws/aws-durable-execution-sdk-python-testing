"""Tests for step operation permutations."""

from aws_durable_execution_sdk_python.execution import InvocationStatus

from aws_durable_execution_sdk_python_testing.runner import (
    DurableFunctionTestResult,
    DurableFunctionTestRunner,
)
from src import step_no_name, step_with_exponential_backoff, step_with_name


def test_step_no_name():
    """Test step without explicit name."""
    with DurableFunctionTestRunner(handler=step_no_name.handler) as runner:
        result: DurableFunctionTestResult = runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result == "Result: Step without name"

    step_ops = [op for op in result.operations if op.operation_type.value == "STEP"]
    assert len(step_ops) == 1
    # Should use function name when no name provided
    assert step_ops[0].name is None or step_ops[0].name == "<lambda>"


def test_step_with_name():
    """Test step with explicit name."""
    with DurableFunctionTestRunner(handler=step_with_name.handler) as runner:
        result: DurableFunctionTestResult = runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result == "Result: Step with explicit name"

    step_ops = [op for op in result.operations if op.operation_type.value == "STEP"]
    assert len(step_ops) == 1
    assert step_ops[0].name == "custom_step"


def test_step_with_exponential_backoff():
    """Test step with exponential backoff retry strategy."""
    with DurableFunctionTestRunner(
        handler=step_with_exponential_backoff.handler
    ) as runner:
        result: DurableFunctionTestResult = runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result == "Result: Step with exponential backoff"

    step_ops = [op for op in result.operations if op.operation_type.value == "STEP"]
    assert len(step_ops) == 1
    assert step_ops[0].name == "retry_step"
