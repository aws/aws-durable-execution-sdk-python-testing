"""Tests for nested chained invoke example."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import OperationStatus

from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from src.chained_invoke import nested_invoke
from test.conftest import deserialize_operation_payload


def test_nested_invoke():
    """Test nested chained invokes (invoke calling invoke).

    Flow: handler -> orchestrator -> adder -> multiplier
    Value: 5 -> add 10 = 15 -> multiply 2 = 30
    """
    with DurableFunctionTestRunner(handler=nested_invoke.handler) as runner:
        # Register the orchestrator (which is also a durable function)
        runner.register_handler("orchestrator", nested_invoke.orchestrator_handler)
        # Register the leaf handlers
        runner.register_handler("adder", nested_invoke.adder_handler)
        runner.register_handler("multiplier", nested_invoke.multiplier_handler)

        result = runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED

    parsed = deserialize_operation_payload(result.result)
    # 5 + 10 = 15, 15 * 2 = 30
    assert parsed["final_result"]["result"] == 30
    assert parsed["final_result"]["steps"] == ["add_10", "multiply_2"]

    # Verify the top-level invoke operation
    invoke_op = result.get_invoke("invoke_orchestrator")
    assert invoke_op is not None
    assert invoke_op.status is OperationStatus.SUCCEEDED
