"""Demonstrates callback failure scenarios where the error propagates and is handled by framework."""

from typing import Any

from aws_durable_execution_sdk_python.config import CallbackConfig, Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(event: dict[str, Any], context: DurableContext) -> dict[str, Any]:
    """Handler demonstrating callback failure scenarios."""
    should_catch_error = event.get("shouldCatchError", False)
    callback_config = CallbackConfig(timeout=Duration.from_seconds(60))

    if should_catch_error:
        # Pattern where error is caught and returned in result
        try:
            callback = context.create_callback(
                name="failing-operation",
                config=callback_config,
            )
            return callback.result()
        except Exception as error:
            return {
                "success": False,
                "error": str(error),
            }
    else:
        # Pattern where error propagates to framework (for basic failure case)
        callback = context.create_callback(
            name="failing-operation",
            config=callback_config,
        )
        return callback.result()
