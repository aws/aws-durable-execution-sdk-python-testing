"""Example demonstrating parallel operations that invoke child functions."""

from typing import Any

from aws_durable_execution_sdk_python.config import ParallelConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(_event: Any, context: DurableContext) -> list[str]:
    """Execute parallel branches where each invokes a different child function."""
    return context.parallel(
        functions=[
            lambda ctx: ctx.invoke(
                function_name="greeter",
                payload={"name": "Alice"},
                name="greet_alice",
            ),
            lambda ctx: ctx.invoke(
                function_name="greeter",
                payload={"name": "Bob"},
                name="greet_bob",
            ),
            lambda ctx: ctx.invoke(
                function_name="greeter",
                payload={"name": "Charlie"},
                name="greet_charlie",
            ),
        ],
        name="parallel_with_invoke",
        config=ParallelConfig(max_concurrency=3),
    ).get_results()


def greeter_handler(event: dict, context: Any) -> dict:
    """Child handler that creates a greeting."""
    name = event.get("name", "World")
    return {"greeting": f"Hello, {name}!"}
