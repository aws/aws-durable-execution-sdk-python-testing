"""Example demonstrating basic chained invoke."""

from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict:
    """Parent function that invokes a child function."""
    result = context.invoke(
        function_name="calculator",
        payload={"a": 10, "b": 5},
        name="invoke_calculator",
    )
    return {"calculation_result": result}


def calculator_handler(event: dict, context: Any) -> dict:
    """Child handler that performs calculation."""
    a = event.get("a", 0)
    b = event.get("b", 0)
    return {
        "sum": a + b,
        "product": a * b,
        "difference": a - b,
    }
