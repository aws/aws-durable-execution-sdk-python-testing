"""Tests for parallel with chained invoke example."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import OperationStatus

from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from src.chained_invoke import parallel_with_invoke
from test.conftest import deserialize_operation_payload


def test_parallel_with_invoke():
    """Test parallel operation where each branch invokes a child function."""
    with DurableFunctionTestRunner(handler=parallel_with_invoke.handler) as runner:
        runner.register_handler("greeter", parallel_with_invoke.greeter_handler)
        result = runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED

    parsed = deserialize_operation_payload(result.result)
    expected = [
        {"greeting": "Hello, Alice!"},
        {"greeting": "Hello, Bob!"},
        {"greeting": "Hello, Charlie!"},
    ]
    assert parsed == expected

    # Verify the parallel operation
    parallel_op = result.get_context("parallel_with_invoke")
    assert parallel_op is not None
    assert parallel_op.status is OperationStatus.SUCCEEDED
    assert len(parallel_op.child_operations) == 3
