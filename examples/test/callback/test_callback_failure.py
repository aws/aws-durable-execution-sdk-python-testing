"""Tests for create_callback_failures."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import ErrorObject

from src.callback import callback_failure


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=callback_failure.handler,
    lambda_function_name="Create Callback Failures Uncaught",
)
def test_handle_callback_operations_with_failure_uncaught(durable_runner):
    """Test handling callback operations with failure."""
    test_payload = {"shouldCatchError": False}

    with durable_runner:
        execution_arn = durable_runner.run_async(input=test_payload, timeout=30)

        callback_id = durable_runner.wait_for_callback(execution_arn=execution_arn)

        durable_runner.send_callback_failure(
            callback_id=callback_id,
            error=ErrorObject.from_message("External API failure"),
        )

        result = durable_runner.wait_for_result(execution_arn=execution_arn)

    assert result.status is InvocationStatus.FAILED

    error = result.error
    assert error is not None
    assert "External API failure" in error.message
    assert error.type == "CallbackError"
    assert error.stack_trace is None


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=callback_failure.handler,
    lambda_function_name="Create Callback Failures Caught Error",
)
def test_handle_callback_operations_with_caught_error(durable_runner):
    """Test handling callback operations with caught error."""
    test_payload = {"shouldCatchError": True}

    with durable_runner:
        execution_arn = durable_runner.run_async(input=test_payload, timeout=30)
        callback_id = durable_runner.wait_for_callback(execution_arn=execution_arn)
        durable_runner.send_callback_failure(
            callback_id=callback_id,
            error=ErrorObject.from_message("External API failure"),
        )
        result = durable_runner.wait_for_result(execution_arn=execution_arn)

    assert result.status is InvocationStatus.SUCCEEDED

    from test.conftest import deserialize_operation_payload

    result_data = deserialize_operation_payload(result.result)
    assert result_data["success"] is False
    assert "External API failure" in result_data["error"]
