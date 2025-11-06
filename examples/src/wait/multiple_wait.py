"""Example demonstrating multiple sequential wait operations."""

from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    """Handler demonstrating multiple sequential wait operations."""
    context.wait(seconds=5, name="wait-1")
    context.wait(seconds=5, name="wait-2")

    return {
        "completedWaits": 2,
        "finalStep": "done",
    }
