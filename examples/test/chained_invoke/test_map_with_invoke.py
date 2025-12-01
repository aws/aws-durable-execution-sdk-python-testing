"""Tests for map with chained invoke example."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import OperationStatus

from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from src.chained_invoke import map_with_invoke
from test.conftest import deserialize_operation_payload


def test_map_with_invoke():
    """Test map operation where each item invokes a child function."""
    with DurableFunctionTestRunner(handler=map_with_invoke.handler) as runner:
        runner.register_handler("doubler", map_with_invoke.doubler_handler)
        result = runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED

    # Each item [1,2,3,4,5] is doubled, returning {"result": value*2}
    parsed = deserialize_operation_payload(result.result)
    expected = [{"result": 2}, {"result": 4}, {"result": 6}, {"result": 8}, {"result": 10}]
    assert parsed == expected

    # Verify the map operation
    map_op = result.get_context("map_with_invoke")
    assert map_op is not None
    assert map_op.status is OperationStatus.SUCCEEDED
