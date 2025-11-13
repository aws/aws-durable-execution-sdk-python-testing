"""Tests for callback failure example."""

from asyncio import sleep

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

from wait_for_callback import wait_for_callback_failure
from test.conftest import deserialize_operation_payload
from wait_for_callback.external_system import ExternalSystem


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=wait_for_callback_failure.handler,
    lambda_function_name="wait for callback failure",
)
def test_callback_failure(durable_runner):
    """Test callback failure handling."""

    with durable_runner:
        external_system = ExternalSystem()
        # Configure external system for local mode if needed
        if durable_runner.mode == "local":

            def failure_handler(callback_id: str, error: Exception):
                sleep(0.5)  # Simulate async work
                durable_runner.fail_callback(callback_id, str(error))

            def success_handler(callback_id: str, msg: bytes):
                durable_runner.succeed_callback(callback_id, msg)

            external_system.activate_local_mode(
                success_handler=success_handler, failure_handler=failure_handler
            )

        result = durable_runner.run(input="test", timeout=10)
        external_system.shutdown()

    # Should handle the failure gracefully
    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result != "OK"
    assert result.result == '"Callback failed"'
