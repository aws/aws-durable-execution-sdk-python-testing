from typing import TYPE_CHECKING, Any

from aws_durable_execution_sdk_python.config import CallbackConfig, Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.types import Callback


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    timeout_type = event.get("timeoutType", "general")
    if timeout_type == "heartbeat":
        config = CallbackConfig(
            timeout=Duration.from_seconds(10),
            heartbeat_timeout=Duration.from_seconds(1),
        )
    else:
        config = CallbackConfig(timeout=Duration.from_seconds(1))

    callback: Callback[str] = context.create_callback(
        name="timeout_callback", config=config
    )

    return callback.result()
