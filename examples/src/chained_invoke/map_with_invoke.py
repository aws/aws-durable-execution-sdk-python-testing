"""Example demonstrating map operations that invoke child functions."""

from typing import Any

from aws_durable_execution_sdk_python.config import MapConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(_event: Any, context: DurableContext) -> list[int]:
    """Process items using map where each item invokes a child function."""
    items = [1, 2, 3, 4, 5]

    return context.map(
        inputs=items,
        func=lambda ctx, item, index, _: ctx.invoke(
            function_name="doubler",
            payload={"value": item},
            name=f"invoke_item_{index}",
        ),
        name="map_with_invoke",
        config=MapConfig(max_concurrency=2),
    ).get_results()


def doubler_handler(event: dict, context: Any) -> dict:
    """Child handler that doubles the input value."""
    value = event.get("value", 0)
    return {"result": value * 2}
