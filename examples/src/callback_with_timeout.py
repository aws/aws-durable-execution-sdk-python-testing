from typing import TYPE_CHECKING, Any

from aws_durable_execution_sdk_python.config import CallbackConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_handler


if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.types import Callback


@durable_handler
def handler(_event: Any, context: DurableContext) -> str:
    # Callback with custom timeout configuration
    config = CallbackConfig(timeout_seconds=60, heartbeat_timeout_seconds=30)

    callback: Callback[str] = context.create_callback(
        name="timeout_callback", config=config
    )

    return f"Callback created with 60s timeout: {callback.callback_id}"
