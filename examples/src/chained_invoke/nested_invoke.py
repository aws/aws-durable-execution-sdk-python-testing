"""Example demonstrating nested chained invokes (invoke calling invoke)."""

from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict:
    """Parent function that invokes a child which invokes another child."""
    result = context.invoke(
        function_name="orchestrator",
        payload={"value": 5},
        name="invoke_orchestrator",
    )
    return {"final_result": result}


@durable_execution
def orchestrator_handler(event: dict, context: DurableContext) -> dict:
    """Middle function that invokes the worker."""
    value = event.get("value", 0)

    # First invoke: add 10
    added = context.invoke(
        function_name="adder",
        payload={"value": value, "add": 10},
        name="invoke_adder",
    )

    # Second invoke: multiply by 2
    multiplied = context.invoke(
        function_name="multiplier",
        payload={"value": added["result"]},
        name="invoke_multiplier",
    )

    return {"result": multiplied["result"], "steps": ["add_10", "multiply_2"]}


def adder_handler(event: dict, context: Any) -> dict:
    """Leaf handler that adds values."""
    value = event.get("value", 0)
    add = event.get("add", 0)
    return {"result": value + add}


def multiplier_handler(event: dict, context: Any) -> dict:
    """Leaf handler that multiplies by 2."""
    value = event.get("value", 0)
    return {"result": value * 2}
