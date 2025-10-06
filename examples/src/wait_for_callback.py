from typing import Any

from aws_durable_execution_sdk_python.config import WaitForCallbackConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_handler


def external_system_call(_callback_id: str) -> None:
    """Simulate calling an external system with callback ID."""
    # In real usage, this would make an API call to an external system
    # passing the callback_id for the system to call back when done


@durable_handler
def handler(_event: Any, context: DurableContext) -> str:
    config = WaitForCallbackConfig(timeout_seconds=120, heartbeat_timeout_seconds=60)

    result = context.wait_for_callback(
        external_system_call, name="external_call", config=config
    )

    return f"External system result: {result}"
