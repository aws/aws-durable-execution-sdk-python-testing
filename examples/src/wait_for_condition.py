"""Example demonstrating wait-for-condition pattern."""

from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(_event: Any, context: DurableContext) -> int:
    """Handler demonstrating wait-for-condition pattern."""
    state = 0
    attempt = 0
    max_attempts = 5

    while attempt < max_attempts:
        attempt += 1

        # Execute step to update state
        state = context.step(
            lambda _, s=state: s + 1,
            name=f"increment_state_{attempt}",
        )

        # Check condition
        if state >= 3:
            # Condition met, stop
            break

        # Wait before next attempt
        context.wait(seconds=1)

    return state
