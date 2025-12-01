"""Tests for basic chained invoke example."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import OperationStatus

from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from src.chained_invoke import invoke_basic
from test.conftest import deserialize_operation_payload


def test_invoke_basic():
    """Test basic chained invoke example."""
    with DurableFunctionTestRunner(handler=invoke_basic.handler) as runner:
        runner.register_handler("calculator", invoke_basic.calculator_handler)
        result = runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    parsed = deserialize_operation_payload(result.result)
    assert parsed["calculation_result"]["sum"] == 15
    assert parsed["calculation_result"]["product"] == 50
    assert parsed["calculation_result"]["difference"] == 5

    # Verify the invoke operation
    invoke_op = result.get_invoke("invoke_calculator")
    assert invoke_op is not None
    assert invoke_op.status is OperationStatus.SUCCEEDED
