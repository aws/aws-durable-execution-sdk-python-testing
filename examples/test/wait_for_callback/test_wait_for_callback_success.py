"""Tests for run_in_child_context example."""

from asyncio import sleep

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

from wait_for_callback import wait_for_callback_success
from test.conftest import deserialize_operation_payload
from wait_for_callback.external_system import ExternalSystem


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=wait_for_callback_success.handler,
    lambda_function_name="wait for callback success",
)
def test_callback_success(durable_runner):
    """Test run_in_child_context example."""
    with durable_runner:
        external_system = ExternalSystem()  # Singleton instance
        # Configure external system for local mode if needed
        if durable_runner.mode == "local":

            def success_handler(callback_id: str, msg: bytes):
                sleep(0.5)
                durable_runner.succeed_callback(callback_id, msg)

            external_system.activate_local_mode(success_handler=success_handler)

        result = durable_runner.run(input="test", timeout=10)
        external_system.shutdown()

    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result) == "OK"

    # Verify child context operation exists
    context_ops = [
        op for op in result.operations if op.operation_type.value == "CONTEXT"
    ]
    assert len(context_ops) >= 1
