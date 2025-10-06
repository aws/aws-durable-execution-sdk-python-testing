from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_handler


@durable_handler
def handler(_event: Any, context: DurableContext) -> str:
    # Wait with explicit name
    context.wait(seconds=2, name="custom_wait")
    return "Wait with name completed"
