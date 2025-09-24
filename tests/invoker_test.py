"""Tests for invoker module."""

import json
from unittest.mock import Mock, patch

import pytest
from aws_durable_execution_sdk_python.execution import (
    DurableExecutionInvocationInput,
    DurableExecutionInvocationInputWithClient,
    DurableExecutionInvocationOutput,
    InitialExecutionState,
    InvocationStatus,
)
from aws_durable_execution_sdk_python.lambda_context import LambdaContext

from aws_durable_execution_sdk_python_testing.execution import Execution
from aws_durable_execution_sdk_python_testing.invoker import (
    InProcessInvoker,
    LambdaInvoker,
    create_test_lambda_context,
)
from aws_durable_execution_sdk_python_testing.model import StartDurableExecutionInput


def test_create_test_lambda_context():
    """Test creating a test lambda context."""
    context = create_test_lambda_context()

    assert isinstance(context, LambdaContext)
    assert (
        context.invoked_function_arn
        == "arn:aws:lambda:us-west-2:123456789012:function:test-function"
    )
    assert context.tenant_id == "test-tenant-789"
    assert context.client_context is not None


def test_in_process_invoker_init():
    """Test InProcessInvoker initialization."""
    handler = Mock()
    service_client = Mock()

    invoker = InProcessInvoker(handler, service_client)

    assert invoker.handler is handler
    assert invoker.service_client is service_client


def test_in_process_invoker_create_invocation_input():
    """Test creating invocation input for in-process invoker."""
    handler = Mock()
    service_client = Mock()
    invoker = InProcessInvoker(handler, service_client)

    input_data = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
    )
    execution = Execution.new(input_data)

    invocation_input = invoker.create_invocation_input(execution)

    assert isinstance(invocation_input, DurableExecutionInvocationInputWithClient)
    assert invocation_input.durable_execution_arn == execution.durable_execution_arn
    assert invocation_input.checkpoint_token is not None
    assert isinstance(invocation_input.initial_execution_state, InitialExecutionState)
    assert invocation_input.is_local_runner is False
    assert invocation_input.service_client is service_client


def test_in_process_invoker_invoke():
    """Test invoking function with in-process invoker."""
    # Mock handler that returns a valid response
    handler = Mock()
    handler.return_value = {"Status": "SUCCEEDED", "Result": "test-result"}

    service_client = Mock()
    invoker = InProcessInvoker(handler, service_client)

    input_data = DurableExecutionInvocationInput(
        durable_execution_arn="test-arn",
        checkpoint_token="test-token",  # noqa: S106
        initial_execution_state=InitialExecutionState(operations=[], next_marker=""),
        is_local_runner=False,
    )

    result = invoker.invoke("test-function", input_data)

    assert isinstance(result, DurableExecutionInvocationOutput)
    assert result.status == InvocationStatus.SUCCEEDED
    assert result.result == "test-result"

    # Verify handler was called with correct arguments
    handler.assert_called_once()
    call_args = handler.call_args[0]
    assert isinstance(call_args[0], DurableExecutionInvocationInputWithClient)
    assert isinstance(call_args[1], LambdaContext)


def test_lambda_invoker_init():
    """Test LambdaInvoker initialization."""
    lambda_client = Mock()

    invoker = LambdaInvoker(lambda_client)

    assert invoker.lambda_client is lambda_client


def test_lambda_invoker_create():
    """Test creating LambdaInvoker with boto3 client."""
    with patch("aws_durable_execution_sdk_python_testing.invoker.boto3") as mock_boto3:
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client

        invoker = LambdaInvoker.create("test-function")

        assert isinstance(invoker, LambdaInvoker)
        assert invoker.lambda_client is mock_client
        mock_boto3.client.assert_called_once_with("lambdainternal")


def test_lambda_invoker_create_invocation_input():
    """Test creating invocation input for lambda invoker."""
    lambda_client = Mock()
    invoker = LambdaInvoker(lambda_client)

    input_data = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
    )
    execution = Execution.new(input_data)

    invocation_input = invoker.create_invocation_input(execution)

    assert isinstance(invocation_input, DurableExecutionInvocationInput)
    assert invocation_input.durable_execution_arn == execution.durable_execution_arn
    assert invocation_input.checkpoint_token is not None
    assert isinstance(invocation_input.initial_execution_state, InitialExecutionState)
    assert invocation_input.is_local_runner is False


def test_lambda_invoker_invoke_success():
    """Test successful lambda invocation."""
    lambda_client = Mock()

    # Mock successful response
    mock_payload = Mock()
    mock_payload.read.return_value = json.dumps(
        {"Status": "SUCCEEDED", "Result": "lambda-result"}
    ).encode("utf-8")

    lambda_client.invoke20150331.return_value = {
        "StatusCode": 200,
        "Payload": mock_payload,
    }

    invoker = LambdaInvoker(lambda_client)

    input_data = DurableExecutionInvocationInput(
        durable_execution_arn="test-arn",
        checkpoint_token="test-token",  # noqa: S106
        initial_execution_state=InitialExecutionState(operations=[], next_marker=""),
        is_local_runner=False,
    )

    result = invoker.invoke("test-function", input_data)

    assert isinstance(result, DurableExecutionInvocationOutput)
    assert result.status == InvocationStatus.SUCCEEDED
    assert result.result == "lambda-result"

    # Verify lambda client was called correctly
    lambda_client.invoke20150331.assert_called_once_with(
        FunctionName="test-function",
        InvocationType="RequestResponse",
        Payload=input_data.to_dict(),
    )


def test_lambda_invoker_invoke_failure():
    """Test lambda invocation failure."""
    lambda_client = Mock()

    # Mock failed response
    mock_payload = Mock()
    lambda_client.invoke20150331.return_value = {
        "StatusCode": 500,
        "Payload": mock_payload,
    }

    invoker = LambdaInvoker(lambda_client)

    input_data = DurableExecutionInvocationInput(
        durable_execution_arn="test-arn",
        checkpoint_token="test-token",  # noqa: S106
        initial_execution_state=InitialExecutionState(operations=[], next_marker=""),
        is_local_runner=False,
    )

    with pytest.raises(
        Exception, match="Lambda invocation failed with status code: 500"
    ):
        invoker.invoke("test-function", input_data)


def test_in_process_invoker_invoke_with_execution_operations():
    """Test in-process invoker with execution that has operations."""
    handler = Mock()
    handler.return_value = {"Status": "SUCCEEDED", "Result": None}

    service_client = Mock()
    invoker = InProcessInvoker(handler, service_client)

    input_data = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation",
    )
    execution = Execution.new(input_data)
    execution.start()  # This adds operations

    invocation_input = invoker.create_invocation_input(execution)
    result = invoker.invoke("test-function", invocation_input)

    assert isinstance(result, DurableExecutionInvocationOutput)
    assert result.status == InvocationStatus.SUCCEEDED
    assert len(invocation_input.initial_execution_state.operations) > 0


def test_lambda_invoker_create_invocation_input_with_operations():
    """Test lambda invoker creating input with execution operations."""
    lambda_client = Mock()
    invoker = LambdaInvoker(lambda_client)

    input_data = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation",
    )
    execution = Execution.new(input_data)
    execution.start()  # This adds operations

    invocation_input = invoker.create_invocation_input(execution)

    assert isinstance(invocation_input, DurableExecutionInvocationInput)
    assert len(invocation_input.initial_execution_state.operations) > 0
    assert invocation_input.initial_execution_state.next_marker == ""
