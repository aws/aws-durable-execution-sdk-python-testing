"""Tests for map_completion."""

import json

import pytest

from src.map import map_completion
from test.conftest import deserialize_operation_payload
from aws_durable_execution_sdk_python.execution import InvocationStatus


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=map_completion.handler,
    lambda_function_name="Map Completion Config",
)
def test_reproduce_completion_config_behavior_with_detailed_logging(durable_runner):
    """Demonstrates map behavior with minSuccessful and concurrent execution."""
    with durable_runner:
        result = durable_runner.run(input=None, timeout=60)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = deserialize_operation_payload(result.result)

    # 4 or 5 items are processed despite min_successful=2, which is expected due to the concurrent executor nature.
    # When the completion requirements are met and 2 items succeed, a completion event is set and the main thread
    # continues to cancel remaining futures. However, background threads cannot be stopped immediately since they're
    # not in the critical section. There's a gap between setting the completion_event and all futures actually stopping,
    # during which concurrent threads continue processing and increment counters. With max_concurrency=3 and 5 items,
    # 4 or 5 items may complete before the cancellation takes effect. This means >= 4 items are processed as expected
    # due to concurrency, with 4 or 5 items being typical in practice.
    #
    # Additionally, failure_count shows 0 because failed items have retry strategies configured and are still retrying
    # when execution completes. Failures aren't finalized until retries complete, so they don't appear in the failure_count.

    assert result_data["totalItems"] >= 4
    assert result_data["successfulCount"] >= 2
    assert result_data["failedCount"] == 0
    assert result_data["hasFailures"] is False
    assert result_data["batchStatus"] == "BatchItemStatus.SUCCEEDED"
    assert result_data["completionReason"] == "CompletionReason.MIN_SUCCESSFUL_REACHED"
