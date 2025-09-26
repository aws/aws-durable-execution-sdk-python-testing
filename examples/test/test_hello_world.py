"""Integration tests for example durable functions."""

from aws_durable_execution_sdk_python.execution import InvocationStatus

from aws_durable_execution_sdk_python_testing.runner import (
    DurableFunctionTestResult,
    DurableFunctionTestRunner,
)
from src import hello_world


def test_hello_world():
    """Test hello world example."""
    with DurableFunctionTestRunner(handler=hello_world.handler) as runner:
        result: DurableFunctionTestResult = runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED  # noqa: S101
    assert result.result == "Hello World!"  # noqa: S101
