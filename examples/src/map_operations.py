from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


def square(x: int) -> int:
    return x * x


@durable_execution
def handler(_event: Any, context: DurableContext) -> str:
    # Process a list of items using map-like operations
    items = [1, 2, 3, 4, 5]

    # Process each item as a separate durable step
    results = []
    for i, item in enumerate(items):
        result = context.step(lambda _, x=item: square(x), name=f"square_{i}")
        results.append(result)

    return f"Squared results: {results}"
