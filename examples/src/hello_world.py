from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(_event: Any, _context: DurableContext) -> str:
    """Simple hello world durable function."""
    return "Hello World!"
