"""Integration tests for hello world example."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

from src import hello_world
from test.conftest import deserialize_operation_payload
from test.event_helper import assert_events


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=hello_world.handler,
    lambda_function_name="hello world",
)
def test_hello_world(durable_runner):
    """Test hello world example."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=30)

    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result) == {
        "statusCode": 200,
        "body": "Hello from Durable Lambda! (status: 200)",
    }
    assert_events("examples/events/hello_world_events.json", result.events)
