from random import random
from typing import Any

from aws_durable_execution_sdk_python.config import StepConfig
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    durable_step,
)
from aws_durable_execution_sdk_python.execution import durable_handler
from aws_durable_execution_sdk_python.retries import (
    RetryStrategyConfig,
    create_retry_strategy,
)


@durable_step
def unreliable_operation() -> str:
    failure_threshold = 0.5
    if random() > failure_threshold:  # noqa: S311
        msg = "Random error occurred"
        raise RuntimeError(msg)
    return "Operation succeeded"


@durable_handler
def handler(_event: Any, context: DurableContext) -> str:
    retry_config = RetryStrategyConfig(
        max_attempts=3,
        retryable_error_types=[RuntimeError],
    )

    result: str = context.step(
        unreliable_operation(),
        config=StepConfig(create_retry_strategy(retry_config)),
    )

    return result
