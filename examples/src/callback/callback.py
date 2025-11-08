from typing import TYPE_CHECKING, Any

from aws_durable_execution_sdk_python.config import CallbackConfig, Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.types import Callback


@durable_execution
def handler(_event: Any, context: DurableContext) -> str:
    callback_config = CallbackConfig(
        timeout=Duration.from_minutes(2), heartbeat_timeout=Duration.from_minutes(1)
    )

    callback: Callback[str] = context.create_callback(
        name="example_callback", config=callback_config
    )

    # In a real scenario, you would pass callback.callback_id to an external system
    # For this example, we'll just return the callback_id to show it was created
    return f"Callback created with ID: {callback.callback_id}"
