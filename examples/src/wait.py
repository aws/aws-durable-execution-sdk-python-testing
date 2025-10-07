from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_handler


@durable_handler
def handler(_event: Any, context: DurableContext) -> str:
    context.wait(seconds=5)
    return "Wait completed"
