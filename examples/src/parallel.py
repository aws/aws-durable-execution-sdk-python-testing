from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(_event: Any, context: DurableContext) -> str:
    # Execute multiple operations in parallel
    task1 = context.step(lambda _: "Task 1 complete", name="task1")
    task2 = context.step(lambda _: "Task 2 complete", name="task2")
    task3 = context.step(lambda _: "Task 3 complete", name="task3")

    # All tasks execute concurrently and results are collected
    return f"Results: {task1}, {task2}, {task3}"
