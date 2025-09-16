"""Unit tests for executor module."""

import asyncio
from unittest.mock import Mock, patch

import pytest
from aws_durable_functions_sdk_python.execution import (
    DurableExecutionInvocationOutput,
    InvocationStatus,
)
from aws_durable_functions_sdk_python.lambda_service import ErrorObject

from aws_durable_functions_sdk_python_testing.exceptions import (
    IllegalStateError,
    InvalidParameterError,
    ResourceNotFoundError,
)
from aws_durable_functions_sdk_python_testing.execution import Execution
from aws_durable_functions_sdk_python_testing.executor import Executor
from aws_durable_functions_sdk_python_testing.model import StartDurableExecutionInput


@pytest.fixture
def mock_store():
    return Mock()


@pytest.fixture
def mock_scheduler():
    return Mock()


@pytest.fixture
def mock_invoker():
    return Mock()


@pytest.fixture
def executor(mock_store, mock_scheduler, mock_invoker):
    return Executor(mock_store, mock_scheduler, mock_invoker)


@pytest.fixture
def start_input():
    return StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
    )


@pytest.fixture
def mock_execution():
    execution = Mock(spec=Execution)
    execution.durable_execution_arn = "arn:aws:lambda:us-east-1:123456789012:function:test-function:execution:test-execution"
    execution.is_complete = False
    execution.consecutive_failed_invocation_attempts = 0
    execution.start_input = Mock()
    execution.start_input.function_name = "test-function"
    return execution


def test_init(mock_store, mock_scheduler, mock_invoker):
    executor = Executor(mock_store, mock_scheduler, mock_invoker)
    assert executor._store == mock_store  # noqa: SLF001
    assert executor._scheduler == mock_scheduler  # noqa: SLF001
    assert executor._invoker == mock_invoker  # noqa: SLF001
    assert executor._completion_events == {}  # noqa: SLF001


@patch("aws_durable_functions_sdk_python_testing.executor.Execution")
def test_start_execution(
    mock_execution_class, executor, start_input, mock_store, mock_scheduler
):
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution_class.new.return_value = mock_execution
    mock_event = Mock()
    mock_scheduler.create_event.return_value = mock_event

    with patch.object(executor, "_invoke_execution") as mock_invoke:
        result = executor.start_execution(start_input)

    mock_execution_class.new.assert_called_once_with(input=start_input)
    mock_execution.start.assert_called_once()
    mock_store.save.assert_called_once_with(mock_execution)
    mock_scheduler.create_event.assert_called_once()
    mock_invoke.assert_called_once_with("test-arn")
    assert result.execution_arn == "test-arn"
    assert executor._completion_events["test-arn"] == mock_event  # noqa: SLF001


def test_get_execution(executor, mock_store):
    mock_execution = Mock()
    mock_store.load.return_value = mock_execution

    result = executor.get_execution("test-arn")

    mock_store.load.assert_called_once_with("test-arn")
    assert result == mock_execution


def test_validate_invocation_response_and_store_failed_status(
    executor, mock_execution, mock_store
):
    response = DurableExecutionInvocationOutput(
        status=InvocationStatus.FAILED, error=ErrorObject.from_message("Test error")
    )

    with patch.object(executor, "_complete_workflow") as mock_complete:
        executor._validate_invocation_response_and_store(  # noqa: SLF001
            "test-arn", response, mock_execution
        )

    mock_complete.assert_called_once_with("test-arn", result=None, error=response.error)
    mock_store.save.assert_called_once_with(mock_execution)


def test_validate_invocation_response_and_store_succeeded_status(
    executor, mock_execution, mock_store
):
    response = DurableExecutionInvocationOutput(
        status=InvocationStatus.SUCCEEDED, result="success result"
    )

    with patch.object(executor, "_complete_workflow") as mock_complete:
        executor._validate_invocation_response_and_store(  # noqa: SLF001
            "test-arn", response, mock_execution
        )

    mock_complete.assert_called_once_with(
        "test-arn", result="success result", error=None
    )
    mock_store.save.assert_called_once_with(mock_execution)


def test_validate_invocation_response_and_store_pending_status(
    executor, mock_execution
):
    response = DurableExecutionInvocationOutput(status=InvocationStatus.PENDING)
    mock_execution.has_pending_operations.return_value = True

    executor._validate_invocation_response_and_store(  # noqa: SLF001
        "test-arn", response, mock_execution
    )

    mock_execution.has_pending_operations.assert_called_once_with(mock_execution)


def test_validate_invocation_response_and_store_execution_already_complete(
    executor, mock_execution
):
    mock_execution.is_complete = True
    response = DurableExecutionInvocationOutput(status=InvocationStatus.SUCCEEDED)

    with pytest.raises(IllegalStateError, match="Execution already completed"):
        executor._validate_invocation_response_and_store(  # noqa: SLF001
            "test-arn", response, mock_execution
        )


def test_validate_invocation_response_and_store_no_status(executor, mock_execution):
    response = DurableExecutionInvocationOutput(status=None)

    with pytest.raises(InvalidParameterError, match="Response status is required"):
        executor._validate_invocation_response_and_store(  # noqa: SLF001
            "test-arn", response, mock_execution
        )


def test_validate_invocation_response_and_store_failed_with_result(
    executor, mock_execution
):
    response = DurableExecutionInvocationOutput(
        status=InvocationStatus.FAILED, result="should not have result"
    )

    with pytest.raises(
        InvalidParameterError, match="Cannot provide a Result for FAILED status"
    ):
        executor._validate_invocation_response_and_store(  # noqa: SLF001
            "test-arn", response, mock_execution
        )


def test_validate_invocation_response_and_store_succeeded_with_error(
    executor, mock_execution
):
    response = DurableExecutionInvocationOutput(
        status=InvocationStatus.SUCCEEDED,
        error=ErrorObject.from_message("should not have error"),
    )

    with pytest.raises(
        InvalidParameterError, match="Cannot provide an Error for SUCCEEDED status"
    ):
        executor._validate_invocation_response_and_store(  # noqa: SLF001
            "test-arn", response, mock_execution
        )


def test_validate_invocation_response_and_store_pending_no_operations(
    executor, mock_execution
):
    response = DurableExecutionInvocationOutput(status=InvocationStatus.PENDING)
    mock_execution.has_pending_operations.return_value = False

    with pytest.raises(
        InvalidParameterError,
        match="Cannot return PENDING status with no pending operations",
    ):
        executor._validate_invocation_response_and_store(  # noqa: SLF001
            "test-arn", response, mock_execution
        )


def test_invoke_handler_success(executor, mock_store, mock_invoker, mock_execution):
    mock_store.load.return_value = mock_execution
    mock_invocation_input = Mock()
    mock_invoker.create_invocation_input.return_value = mock_invocation_input
    mock_response = DurableExecutionInvocationOutput(
        status=InvocationStatus.SUCCEEDED, result="test"
    )
    mock_invoker.invoke.return_value = mock_response

    with patch.object(executor, "_validate_invocation_response_and_store"):
        handler = executor._invoke_handler("test-arn")  # noqa: SLF001
        # Test that the handler is created and is callable
        assert callable(handler)


def test_invoke_handler_execution_already_complete(
    executor, mock_store, mock_execution
):
    mock_execution.is_complete = True
    mock_store.load.return_value = mock_execution

    handler = executor._invoke_handler("test-arn")  # noqa: SLF001
    assert callable(handler)

    # Execute the handler synchronously using asyncio.run
    asyncio.run(handler())

    mock_store.load.assert_called_with("test-arn")


def test_invoke_handler_execution_completed_during_invocation(
    executor, mock_store, mock_invoker, mock_execution
):
    mock_store.load.side_effect = [mock_execution, mock_execution]
    mock_execution.is_complete = False
    mock_invocation_input = Mock()
    mock_invoker.create_invocation_input.return_value = mock_invocation_input
    mock_response = Mock()
    mock_invoker.invoke.return_value = mock_response

    # Simulate execution completing during invocation
    def complete_execution(*args):
        mock_execution.is_complete = True
        return mock_execution

    mock_store.load.side_effect = [mock_execution, complete_execution()]

    handler = executor._invoke_handler("test-arn")  # noqa: SLF001
    assert callable(handler)


def test_invoke_handler_validation_error(
    executor, mock_store, mock_invoker, mock_execution
):
    mock_store.load.return_value = mock_execution
    mock_invocation_input = Mock()
    mock_invoker.create_invocation_input.return_value = mock_invocation_input
    mock_response = Mock()
    mock_invoker.invoke.return_value = mock_response

    with patch.object(
        executor, "_validate_invocation_response_and_store"
    ) as mock_validate:
        with patch.object(executor, "_retry_invocation"):
            mock_validate.side_effect = InvalidParameterError("validation error")

            handler = executor._invoke_handler("test-arn")  # noqa: SLF001
            assert callable(handler)


def test_invoke_handler_resource_not_found(
    executor, mock_store, mock_invoker, mock_execution
):
    mock_store.load.return_value = mock_execution
    mock_invoker.create_invocation_input.side_effect = ResourceNotFoundError(
        "Function not found"
    )

    with patch.object(executor, "_fail_workflow"):
        handler = executor._invoke_handler("test-arn")  # noqa: SLF001
        assert callable(handler)


def test_invoke_handler_general_exception(
    executor, mock_store, mock_invoker, mock_execution
):
    mock_store.load.return_value = mock_execution
    mock_invoker.create_invocation_input.side_effect = Exception("General error")

    with patch.object(executor, "_retry_invocation"):
        handler = executor._invoke_handler("test-arn")  # noqa: SLF001
        assert callable(handler)


def test_invoke_execution(executor, mock_scheduler):
    executor._completion_events["test-arn"] = Mock()  # noqa: SLF001

    executor._invoke_execution("test-arn", delay=5)  # noqa: SLF001

    mock_scheduler.call_later.assert_called_once()
    args = mock_scheduler.call_later.call_args
    assert args[1]["delay"] == 5
    assert args[1]["completion_event"] == executor._completion_events["test-arn"]  # noqa: SLF001


def test_complete_workflow_success(executor, mock_store, mock_execution):
    mock_store.load.return_value = mock_execution

    with patch.object(executor, "complete_execution") as mock_complete:
        executor._complete_workflow("test-arn", "result", None)  # noqa: SLF001

    mock_complete.assert_called_once_with("test-arn", "result")


def test_complete_workflow_failure(executor, mock_store, mock_execution):
    mock_store.load.return_value = mock_execution
    error = ErrorObject.from_message("test error")

    with patch.object(executor, "fail_execution") as mock_fail:
        executor._complete_workflow("test-arn", None, error)  # noqa: SLF001

    mock_fail.assert_called_once_with("test-arn", error)


def test_complete_workflow_already_complete(executor, mock_store, mock_execution):
    mock_execution.is_complete = True
    mock_store.load.return_value = mock_execution

    with pytest.raises(
        IllegalStateError, match="Cannot make multiple close workflow decisions"
    ):
        executor._complete_workflow("test-arn", "result", None)  # noqa: SLF001


def test_fail_workflow(executor, mock_store, mock_execution):
    mock_store.load.return_value = mock_execution
    error = ErrorObject.from_message("test error")

    with patch.object(executor, "fail_execution") as mock_fail:
        executor._fail_workflow("test-arn", error)  # noqa: SLF001

    mock_fail.assert_called_once_with("test-arn", error)


def test_fail_workflow_already_complete(executor, mock_store, mock_execution):
    mock_execution.is_complete = True
    mock_store.load.return_value = mock_execution
    error = ErrorObject.from_message("test error")

    with pytest.raises(
        IllegalStateError, match="Cannot make multiple close workflow decisions"
    ):
        executor._fail_workflow("test-arn", error)  # noqa: SLF001


def test_retry_invocation_under_limit(executor, mock_execution, mock_store):
    mock_execution.consecutive_failed_invocation_attempts = 3
    error = ErrorObject.from_message("test error")

    with patch.object(executor, "_invoke_execution") as mock_invoke:
        executor._retry_invocation(mock_execution, error)  # noqa: SLF001

    assert mock_execution.consecutive_failed_invocation_attempts == 4
    mock_store.save.assert_called_once_with(mock_execution)
    mock_invoke.assert_called_once_with(
        execution_arn=mock_execution.durable_execution_arn,
        delay=Executor.RETRY_BACKOFF_SECONDS,
    )


def test_retry_invocation_over_limit(executor, mock_execution):
    mock_execution.consecutive_failed_invocation_attempts = 6
    error = ErrorObject.from_message("test error")

    with patch.object(executor, "_fail_workflow") as mock_fail:
        executor._retry_invocation(mock_execution, error)  # noqa: SLF001

    mock_fail.assert_called_once_with(
        execution_arn=mock_execution.durable_execution_arn, error=error
    )


def test_complete_events(executor):
    mock_event = Mock()
    executor._completion_events["test-arn"] = mock_event  # noqa: SLF001

    executor._complete_events("test-arn")  # noqa: SLF001

    mock_event.set.assert_called_once()


def test_complete_events_no_event(executor):
    # Should not raise exception when event doesn't exist
    executor._complete_events("nonexistent-arn")  # noqa: SLF001


def test_wait_until_complete_success(executor):
    mock_event = Mock()
    mock_event.wait.return_value = True
    executor._completion_events["test-arn"] = mock_event  # noqa: SLF001

    result = executor.wait_until_complete("test-arn", timeout=10)

    assert result is True
    mock_event.wait.assert_called_once_with(10)


def test_wait_until_complete_timeout(executor):
    mock_event = Mock()
    mock_event.wait.return_value = False
    executor._completion_events["test-arn"] = mock_event  # noqa: SLF001

    result = executor.wait_until_complete("test-arn", timeout=10)

    assert result is False


def test_wait_until_complete_no_event(executor):
    with pytest.raises(ValueError, match="execution does not exist"):
        executor.wait_until_complete("nonexistent-arn")


def test_complete_execution(executor, mock_store, mock_execution):
    mock_execution.result = "test result"
    mock_store.load.return_value = mock_execution

    with patch.object(executor, "_complete_events") as mock_complete_events:
        executor.complete_execution("test-arn", "result")

    mock_store.load.assert_called_once_with(execution_arn="test-arn")
    mock_execution.complete_success.assert_called_once_with(result="result")
    mock_store.update.assert_called_once_with(mock_execution)
    mock_complete_events.assert_called_once_with(execution_arn="test-arn")


def test_fail_execution(executor, mock_store, mock_execution):
    error = ErrorObject.from_message("test error")
    mock_execution.result = "error result"
    mock_store.load.return_value = mock_execution

    with patch.object(executor, "_complete_events") as mock_complete_events:
        executor.fail_execution("test-arn", error)

    mock_store.load.assert_called_once_with(execution_arn="test-arn")
    mock_execution.complete_fail.assert_called_once_with(error=error)
    mock_store.update.assert_called_once_with(mock_execution)
    mock_complete_events.assert_called_once_with(execution_arn="test-arn")


def test_on_wait_succeeded(executor, mock_store, mock_execution):
    mock_store.load.return_value = mock_execution

    executor._on_wait_succeeded("test-arn", "op-123")  # noqa: SLF001

    mock_store.load.assert_called_once_with("test-arn")
    mock_execution.complete_wait.assert_called_once_with(operation_id="op-123")
    mock_store.update.assert_called_once_with(mock_execution)


def test_on_wait_succeeded_execution_complete(executor, mock_store, mock_execution):
    mock_execution.is_complete = True
    mock_store.load.return_value = mock_execution

    executor._on_wait_succeeded("test-arn", "op-123")  # noqa: SLF001

    mock_execution.complete_wait.assert_not_called()
    mock_store.update.assert_not_called()


def test_on_wait_succeeded_exception(executor, mock_store, mock_execution):
    mock_store.load.return_value = mock_execution
    mock_execution.complete_wait.side_effect = Exception("test error")

    # Should not raise exception
    executor._on_wait_succeeded("test-arn", "op-123")  # noqa: SLF001


def test_on_retry_ready(executor, mock_store, mock_execution):
    mock_store.load.return_value = mock_execution

    executor._on_retry_ready("test-arn", "op-123")  # noqa: SLF001

    mock_store.load.assert_called_once_with("test-arn")
    mock_execution.complete_retry.assert_called_once_with(operation_id="op-123")
    mock_store.update.assert_called_once_with(mock_execution)


def test_on_retry_ready_execution_complete(executor, mock_store, mock_execution):
    mock_execution.is_complete = True
    mock_store.load.return_value = mock_execution

    executor._on_retry_ready("test-arn", "op-123")  # noqa: SLF001

    mock_execution.complete_retry.assert_not_called()
    mock_store.update.assert_not_called()


def test_on_retry_ready_exception(executor, mock_store, mock_execution):
    mock_store.load.return_value = mock_execution
    mock_execution.complete_retry.side_effect = Exception("test error")

    # Should not raise exception
    executor._on_retry_ready("test-arn", "op-123")  # noqa: SLF001


def test_on_completed(executor):
    with patch.object(executor, "complete_execution") as mock_complete:
        executor.on_completed("test-arn", "result")

    mock_complete.assert_called_once_with("test-arn", "result")


def test_on_failed(executor):
    error = ErrorObject.from_message("test error")

    with patch.object(executor, "fail_execution") as mock_fail:
        executor.on_failed("test-arn", error)

    mock_fail.assert_called_once_with("test-arn", error)


def test_on_wait_timer_scheduled(executor, mock_scheduler):
    executor._completion_events["test-arn"] = Mock()  # noqa: SLF001

    with patch.object(executor, "_on_wait_succeeded"):
        with patch.object(executor, "_invoke_execution"):
            executor.on_wait_timer_scheduled("test-arn", "op-123", 10.0)

    mock_scheduler.call_later.assert_called_once()
    args = mock_scheduler.call_later.call_args
    assert args[1]["delay"] == 10.0
    assert args[1]["completion_event"] == executor._completion_events["test-arn"]  # noqa: SLF001


def test_validate_invocation_response_and_store_unexpected_status(
    executor, mock_execution
):
    # Create a mock response with an unexpected status
    response = Mock()
    response.status = "UNKNOWN_STATUS"

    with pytest.raises(IllegalStateError, match="Unexpected invocation status"):
        executor._validate_invocation_response_and_store(  # noqa: SLF001
            "test-arn", response, mock_execution
        )


def test_invoke_handler_execution_completed_during_invocation_async(
    executor, mock_store, mock_invoker, mock_execution
):
    # First call returns incomplete execution, second call returns completed execution
    incomplete_execution = Mock(spec=Execution)
    incomplete_execution.is_complete = False
    incomplete_execution.start_input = Mock()
    incomplete_execution.start_input.function_name = "test-function"
    incomplete_execution.consecutive_failed_invocation_attempts = 0
    incomplete_execution.durable_execution_arn = "test-arn"

    completed_execution = Mock(spec=Execution)
    completed_execution.is_complete = True

    mock_store.load.side_effect = [incomplete_execution, completed_execution]

    mock_invocation_input = Mock()
    mock_invoker.create_invocation_input.return_value = mock_invocation_input
    mock_response = Mock()
    mock_invoker.invoke.return_value = mock_response

    handler = executor._invoke_handler("test-arn")  # noqa: SLF001

    # Execute the handler
    import asyncio

    asyncio.run(handler())

    # Verify the execution was loaded twice (before and after invocation)
    assert mock_store.load.call_count == 2


def test_invoke_handler_validation_error_async(
    executor, mock_store, mock_invoker, mock_execution
):
    mock_store.load.return_value = mock_execution
    mock_invocation_input = Mock()
    mock_invoker.create_invocation_input.return_value = mock_invocation_input
    mock_response = Mock()
    mock_invoker.invoke.return_value = mock_response

    with patch.object(
        executor, "_validate_invocation_response_and_store"
    ) as mock_validate:
        with patch.object(executor, "_retry_invocation") as mock_retry:
            mock_validate.side_effect = InvalidParameterError("validation error")

            handler = executor._invoke_handler("test-arn")  # noqa: SLF001

            # Execute the handler
            import asyncio

            asyncio.run(handler())

            mock_retry.assert_called_once()


def test_invoke_handler_resource_not_found_async(
    executor, mock_store, mock_invoker, mock_execution
):
    mock_store.load.return_value = mock_execution
    mock_invoker.create_invocation_input.side_effect = ResourceNotFoundError(
        "Function not found"
    )

    with patch.object(executor, "_fail_workflow") as mock_fail:
        handler = executor._invoke_handler("test-arn")  # noqa: SLF001

        # Execute the handler
        import asyncio

        asyncio.run(handler())

        mock_fail.assert_called_once()


def test_invoke_handler_general_exception_async(
    executor, mock_store, mock_invoker, mock_execution
):
    mock_store.load.return_value = mock_execution
    mock_invoker.create_invocation_input.side_effect = Exception("General error")

    with patch.object(executor, "_retry_invocation") as mock_retry:
        handler = executor._invoke_handler("test-arn")  # noqa: SLF001

        # Execute the handler
        import asyncio

        asyncio.run(handler())

        mock_retry.assert_called_once()


def test_invoke_execution_with_delay(executor, mock_scheduler):
    executor._completion_events["test-arn"] = Mock()  # noqa: SLF001

    executor._invoke_execution("test-arn", delay=10)  # noqa: SLF001

    mock_scheduler.call_later.assert_called_once()
    args = mock_scheduler.call_later.call_args
    assert args[1]["delay"] == 10


def test_invoke_execution_no_delay(executor, mock_scheduler):
    executor._completion_events["test-arn"] = Mock()  # noqa: SLF001

    executor._invoke_execution("test-arn")  # noqa: SLF001

    mock_scheduler.call_later.assert_called_once()
    args = mock_scheduler.call_later.call_args
    assert args[1]["delay"] == 0


def test_on_step_retry_scheduled(executor, mock_scheduler):
    executor._completion_events["test-arn"] = Mock()  # noqa: SLF001

    with patch.object(executor, "_on_retry_ready"):
        with patch.object(executor, "_invoke_execution"):
            executor.on_step_retry_scheduled("test-arn", "op-123", 10.0)

    mock_scheduler.call_later.assert_called_once()
    args = mock_scheduler.call_later.call_args
    assert args[1]["delay"] == 10.0
    assert args[1]["completion_event"] == executor._completion_events["test-arn"]  # noqa: SLF001


def test_wait_handler_execution(executor, mock_scheduler):
    executor._completion_events["test-arn"] = Mock()  # noqa: SLF001

    with patch.object(executor, "_on_wait_succeeded") as mock_wait:
        with patch.object(executor, "_invoke_execution") as mock_invoke:
            executor.on_wait_timer_scheduled("test-arn", "op-123", 10.0)

            # Get the handler that was passed to call_later
            call_args = mock_scheduler.call_later.call_args
            wait_handler = call_args[0][0]

            # Execute the handler to test the inner function
            wait_handler()

            mock_wait.assert_called_once_with("test-arn", "op-123")
            mock_invoke.assert_called_once_with("test-arn", delay=0)


def test_retry_handler_execution(executor, mock_scheduler):
    executor._completion_events["test-arn"] = Mock()  # noqa: SLF001

    with patch.object(executor, "_on_retry_ready") as mock_retry:
        with patch.object(executor, "_invoke_execution") as mock_invoke:
            executor.on_step_retry_scheduled("test-arn", "op-123", 10.0)

            # Get the handler that was passed to call_later
            call_args = mock_scheduler.call_later.call_args
            retry_handler = call_args[0][0]

            # Execute the handler to test the inner function
            retry_handler()

            mock_retry.assert_called_once_with("test-arn", "op-123")
            mock_invoke.assert_called_once_with("test-arn", delay=0)
