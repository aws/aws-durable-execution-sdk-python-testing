"""Tests for callback heartbeat example."""

from asyncio import sleep

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

from wait_for_callback import wait_for_callback_heartbeat
from test.conftest import deserialize_operation_payload
from wait_for_callback.external_system import ExternalSystem


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=wait_for_callback_heartbeat.handler,
    lambda_function_name="wait for callback heartbeat",
)
def test_callback_heartbeat(durable_runner):
    """Test callback heartbeat functionality."""

    with durable_runner:
        external_system = ExternalSystem()
        # Configure external system for local mode if needed
        if durable_runner.mode == "local":

            def heartbeat_handler(callback_id: str):
                sleep(0.1)  # Simulate async work
                # durable_runner.heartbeat_callback(callback_id)

            def success_handler(callback_id: str, msg: bytes):
                sleep(0.5)
                durable_runner.succeed_callback(callback_id, msg)

            external_system.activate_local_mode(
                success_handler=success_handler, heartbeat_handler=heartbeat_handler
            )

        result = durable_runner.run(input="test", timeout=30)
        external_system.shutdown()

    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result) == "OK"
