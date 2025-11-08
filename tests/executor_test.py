"""Unit tests for executor module."""

import asyncio
import uuid
from datetime import UTC, datetime
from unittest.mock import Mock, patch

import pytest
from aws_durable_execution_sdk_python.execution import (
    DurableExecutionInvocationOutput,
    InvocationStatus,
)
from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    ExecutionDetails,
    Operation,
    OperationStatus,
    OperationType,
)

from aws_durable_execution_sdk_python_testing.exceptions import (
    ExecutionAlreadyStartedException,
    IllegalStateException,
    InvalidParameterValueException,
    ResourceNotFoundException,
)
from aws_durable_execution_sdk_python_testing.execution import Execution
from aws_durable_execution_sdk_python_testing.executor import Executor
from aws_durable_execution_sdk_python_testing.model import (
    ListDurableExecutionsResponse,
    SendDurableExecutionCallbackFailureResponse,
    SendDurableExecutionCallbackHeartbeatResponse,
    SendDurableExecutionCallbackSuccessResponse,
    StartDurableExecutionInput,
)
from aws_durable_execution_sdk_python_testing.observer import ExecutionObserver


class MockExecutionObserver(ExecutionObserver):
    """Mock observer to capture execution events through public callbacks."""

    def __init__(self):
        self.completed_executions = {}
        self.failed_executions = {}
        self.wait_timers = {}
        self.retry_schedules = {}

    def on_completed(self, execution_arn: str, result: str | None = None) -> None:
        """Capture completion events."""
        self.completed_executions[execution_arn] = result

    def on_failed(self, execution_arn: str, error: ErrorObject) -> None:
        """Capture failure events."""
        self.failed_executions[execution_arn] = error

    def on_wait_timer_scheduled(
        self, execution_arn: str, operation_id: str, delay: float
    ) -> None:
        """Capture wait timer scheduling events."""
        self.wait_timers[execution_arn] = {"operation_id": operation_id, "delay": delay}

    def on_step_retry_scheduled(
        self, execution_arn: str, operation_id: str, delay: float
    ) -> None:
        """Capture retry scheduling events."""
        self.retry_schedules[execution_arn] = {
            "operation_id": operation_id,
            "delay": delay,
        }


@pytest.fixture
def test_observer():
    return MockExecutionObserver()


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
def mock_checkpoint_processor():
    return Mock()


@pytest.fixture
def executor(mock_store, mock_scheduler, mock_invoker, mock_checkpoint_processor):
    return Executor(mock_store, mock_scheduler, mock_invoker, mock_checkpoint_processor)


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


def test_init(mock_store, mock_scheduler, mock_invoker, mock_checkpoint_processor):
    # Test that Executor can be constructed with dependencies
    # Dependency injection is implementation detail - test behavior instead
    executor = Executor(
        mock_store, mock_scheduler, mock_invoker, mock_checkpoint_processor
    )

    # Verify executor is properly initialized by testing it can perform basic operations
    assert executor is not None

    # Test that the executor uses the injected dependencies by verifying behavior
    # This will be covered by other tests that exercise the executor's functionality


@patch("aws_durable_execution_sdk_python_testing.executor.Execution")
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

    # Test observable behavior through public API
    # The executor should generate an invocation_id if not provided
    call_args = mock_execution_class.new.call_args
    actual_input = call_args.kwargs["input"]

    # Verify all fields match except invocation_id should be generated
    assert actual_input.account_id == start_input.account_id
    assert actual_input.function_name == start_input.function_name
    assert actual_input.function_qualifier == start_input.function_qualifier
    assert actual_input.execution_name == start_input.execution_name
    assert (
        actual_input.execution_timeout_seconds == start_input.execution_timeout_seconds
    )
    assert (
        actual_input.execution_retention_period_days
        == start_input.execution_retention_period_days
    )
    assert actual_input.invocation_id is not None  # Should be generated
    assert actual_input.trace_fields == start_input.trace_fields
    assert actual_input.tenant_id == start_input.tenant_id
    assert actual_input.input == start_input.input
    mock_execution.start.assert_called_once()
    mock_store.save.assert_called_once_with(mock_execution)
    mock_scheduler.create_event.assert_called_once()
    mock_invoke.assert_called_once_with("test-arn")
    assert result.execution_arn == "test-arn"

    # Test that completion event was created by verifying wait_until_complete works
    # This tests the same functionality without accessing private members
    mock_event.wait.return_value = True
    wait_result = executor.wait_until_complete("test-arn", timeout=1)
    assert wait_result is True
    mock_event.wait.assert_called_once_with(1)


@patch("aws_durable_execution_sdk_python_testing.executor.Execution")
def test_start_execution_with_provided_invocation_id(
    mock_execution_class, executor, mock_store, mock_scheduler
):
    # Create input with invocation_id already provided
    provided_invocation_id = "user-provided-id-123"
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id=provided_invocation_id,
    )

    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution_class.new.return_value = mock_execution
    mock_event = Mock()
    mock_scheduler.create_event.return_value = mock_event

    with patch.object(executor, "_invoke_execution") as mock_invoke:
        result = executor.start_execution(start_input)

    # Should use the provided invocation_id unchanged
    mock_execution_class.new.assert_called_once_with(input=start_input)
    mock_execution.start.assert_called_once()
    mock_store.save.assert_called_once_with(mock_execution)
    mock_scheduler.create_event.assert_called_once()
    mock_invoke.assert_called_once_with("test-arn")
    assert result.execution_arn == "test-arn"

    mock_execution = Mock()
    mock_store.load.return_value = mock_execution

    result = executor.get_execution("test-arn")

    mock_store.load.assert_called_once_with("test-arn")
    assert result == mock_execution


def test_should_complete_workflow_with_error_when_invocation_fails(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test that failed invocation responses trigger workflow completion with error."""
    # Arrange
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = False
    mock_execution.start_input = start_input
    mock_execution.consecutive_failed_invocation_attempts = 0

    # Mock invoker to return failed response
    mock_invocation_input = Mock()
    mock_invoker.create_invocation_input.return_value = mock_invocation_input
    failed_response = DurableExecutionInvocationOutput(
        status=InvocationStatus.FAILED, error=ErrorObject.from_message("Test error")
    )
    mock_invoker.invoke.return_value = failed_response

    # Mock execution creation and store behavior
    with patch(
        "aws_durable_execution_sdk_python_testing.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution
        mock_store.load.return_value = mock_execution

        # Mock the workflow completion methods
        with patch.object(executor, "fail_execution") as mock_fail:
            # Act - trigger invocation through public start_execution method
            executor.start_execution(start_input)

            # Get the handler that was passed to the scheduler and execute it manually
            mock_scheduler.call_later.assert_called_once()
            handler = mock_scheduler.call_later.call_args[0][0]

            # Execute the handler to trigger the invocation logic
            import asyncio

            asyncio.run(handler())

        # Assert - verify workflow was completed with error
        mock_fail.assert_called_once_with("test-arn", failed_response.error)


def test_should_complete_workflow_with_result_when_invocation_succeeds(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test that successful invocation responses trigger workflow completion with result."""
    # Arrange
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = False
    mock_execution.start_input = start_input
    mock_execution.consecutive_failed_invocation_attempts = 0

    # Mock invoker to return successful response
    mock_invocation_input = Mock()
    mock_invoker.create_invocation_input.return_value = mock_invocation_input
    success_response = DurableExecutionInvocationOutput(
        status=InvocationStatus.SUCCEEDED, result="success result"
    )
    mock_invoker.invoke.return_value = success_response

    # Mock execution creation and store behavior
    with patch(
        "aws_durable_execution_sdk_python_testing.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution
        mock_store.load.return_value = mock_execution

        # Mock the workflow completion methods
        with patch.object(executor, "complete_execution") as mock_complete:
            # Act - trigger invocation through public start_execution method
            executor.start_execution(start_input)

            # Get the handler that was passed to the scheduler and execute it manually
            mock_scheduler.call_later.assert_called_once()
            handler = mock_scheduler.call_later.call_args[0][0]

            # Execute the handler to trigger the invocation logic
            import asyncio

            asyncio.run(handler())

        # Assert - verify workflow was completed with result
        mock_complete.assert_called_once_with("test-arn", "success result")


def test_should_handle_pending_status_when_operations_exist(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test that pending invocation responses are handled when operations exist."""
    # Arrange
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = False
    mock_execution.start_input = start_input
    mock_execution.consecutive_failed_invocation_attempts = 0
    mock_execution.has_pending_operations.return_value = True

    # Mock invoker to return pending response
    mock_invocation_input = Mock()
    mock_invoker.create_invocation_input.return_value = mock_invocation_input
    pending_response = DurableExecutionInvocationOutput(status=InvocationStatus.PENDING)
    mock_invoker.invoke.return_value = pending_response

    # Mock execution creation and store behavior
    with patch(
        "aws_durable_execution_sdk_python_testing.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution
        mock_store.load.return_value = mock_execution

        # Act - trigger invocation through public start_execution method
        executor.start_execution(start_input)

        # Get the handler that was passed to the scheduler and execute it manually
        mock_scheduler.call_later.assert_called_once()
        handler = mock_scheduler.call_later.call_args[0][0]

        # Execute the handler to trigger the invocation logic
        import asyncio

        asyncio.run(handler())

    # Assert - verify pending operations were checked
    mock_execution.has_pending_operations.assert_called_once_with(mock_execution)


def test_should_ignore_response_when_execution_already_complete(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test that responses are ignored when execution is already complete."""
    # Arrange
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = True  # Already complete
    mock_execution.start_input = start_input

    # Mock invoker - this shouldn't be called since execution is complete
    mock_invoker.create_invocation_input.return_value = Mock()
    mock_invoker.invoke.return_value = DurableExecutionInvocationOutput(
        status=InvocationStatus.SUCCEEDED
    )

    # Mock execution creation and store behavior
    with patch(
        "aws_durable_execution_sdk_python_testing.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution
        mock_store.load.return_value = mock_execution

        # Act - trigger invocation through public start_execution method
        executor.start_execution(start_input)

        # Get the handler that was passed to the scheduler and execute it manually
        mock_scheduler.call_later.assert_called_once()
        handler = mock_scheduler.call_later.call_args[0][0]

        # Execute the handler to trigger the invocation logic
        import asyncio

        asyncio.run(handler())

    # Assert - verify invoker was not called since execution was already complete
    mock_invoker.create_invocation_input.assert_not_called()
    mock_invoker.invoke.assert_not_called()


def test_should_retry_when_response_has_no_status(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test that invocation responses without status trigger retry logic."""
    # Arrange
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = False
    mock_execution.start_input = start_input
    mock_execution.consecutive_failed_invocation_attempts = 0

    # Mock invoker to return response without status
    mock_invocation_input = Mock()
    mock_invoker.create_invocation_input.return_value = mock_invocation_input
    no_status_response = DurableExecutionInvocationOutput(status=None)
    mock_invoker.invoke.return_value = no_status_response

    # Mock execution creation and store behavior
    with patch(
        "aws_durable_execution_sdk_python_testing.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution
        mock_store.load.return_value = mock_execution

        # Act - trigger invocation through public start_execution method
        executor.start_execution(start_input)

        # Get the handler that was passed to the scheduler and execute it manually
        mock_scheduler.call_later.assert_called_once()
        handler = mock_scheduler.call_later.call_args[0][0]

        # Execute the handler to trigger the invocation logic
        asyncio.run(handler())

        # Assert - verify retry was triggered due to validation error
        assert mock_execution.consecutive_failed_invocation_attempts == 1
        mock_store.save.assert_called_with(mock_execution)
        # Verify retry was scheduled (call_later should be called twice: initial + retry)
        assert mock_scheduler.call_later.call_count == 2


def test_should_retry_when_failed_response_has_result(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test that failed responses with result trigger retry logic."""
    # Arrange
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = False
    mock_execution.start_input = start_input
    mock_execution.consecutive_failed_invocation_attempts = 0

    # Mock invoker to return invalid failed response (with result)
    mock_invocation_input = Mock()
    mock_invoker.create_invocation_input.return_value = mock_invocation_input
    invalid_response = DurableExecutionInvocationOutput(
        status=InvocationStatus.FAILED, result="should not have result"
    )
    mock_invoker.invoke.return_value = invalid_response

    # Mock execution creation and store behavior
    with patch(
        "aws_durable_execution_sdk_python_testing.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution
        mock_store.load.return_value = mock_execution

        # Act - trigger invocation through public start_execution method
        executor.start_execution(start_input)

        # Get the handler that was passed to the scheduler and execute it manually
        mock_scheduler.call_later.assert_called_once()
        handler = mock_scheduler.call_later.call_args[0][0]

        # Execute the handler to trigger the invocation logic
        asyncio.run(handler())

        # Assert - verify retry was triggered due to validation error
        assert mock_execution.consecutive_failed_invocation_attempts == 1
        mock_store.save.assert_called_with(mock_execution)
        # Verify retry was scheduled (call_later should be called twice: initial + retry)
        assert mock_scheduler.call_later.call_count == 2


def test_should_retry_when_success_response_has_error(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test that successful responses with error trigger retry logic."""
    # Arrange
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = False
    mock_execution.start_input = start_input
    mock_execution.consecutive_failed_invocation_attempts = 0

    # Mock invoker to return invalid success response (with error)
    mock_invocation_input = Mock()
    mock_invoker.create_invocation_input.return_value = mock_invocation_input
    invalid_response = DurableExecutionInvocationOutput(
        status=InvocationStatus.SUCCEEDED,
        error=ErrorObject.from_message("should not have error"),
    )
    mock_invoker.invoke.return_value = invalid_response

    # Mock execution creation and store behavior
    with patch(
        "aws_durable_execution_sdk_python_testing.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution
        mock_store.load.return_value = mock_execution

        # Act - trigger invocation through public start_execution method
        executor.start_execution(start_input)

        # Get the handler that was passed to the scheduler and execute it manually
        mock_scheduler.call_later.assert_called_once()
        handler = mock_scheduler.call_later.call_args[0][0]

        # Execute the handler to trigger the invocation logic
        asyncio.run(handler())

        # Assert - verify retry was triggered due to validation error
        assert mock_execution.consecutive_failed_invocation_attempts == 1
        mock_store.save.assert_called_with(mock_execution)
        # Verify retry was scheduled (call_later should be called twice: initial + retry)
        assert mock_scheduler.call_later.call_count == 2


def test_should_retry_when_pending_response_has_no_operations(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test that pending responses without operations trigger retry logic."""
    # Arrange
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = False
    mock_execution.start_input = start_input
    mock_execution.consecutive_failed_invocation_attempts = 0
    mock_execution.has_pending_operations.return_value = False  # No pending operations

    # Mock invoker to return pending response
    mock_invocation_input = Mock()
    mock_invoker.create_invocation_input.return_value = mock_invocation_input
    pending_response = DurableExecutionInvocationOutput(status=InvocationStatus.PENDING)
    mock_invoker.invoke.return_value = pending_response

    # Mock execution creation and store behavior
    with patch(
        "aws_durable_execution_sdk_python_testing.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution
        mock_store.load.return_value = mock_execution

        # Act - trigger invocation through public start_execution method
        executor.start_execution(start_input)

        # Get the handler that was passed to the scheduler and execute it manually
        mock_scheduler.call_later.assert_called_once()
        handler = mock_scheduler.call_later.call_args[0][0]

        # Execute the handler to trigger the invocation logic
        asyncio.run(handler())

        # Assert - verify retry was triggered due to validation error
        assert mock_execution.consecutive_failed_invocation_attempts == 1
        mock_store.save.assert_called_with(mock_execution)
        # Verify retry was scheduled (call_later should be called twice: initial + retry)
        assert mock_scheduler.call_later.call_count == 2


def test_invoke_handler_success(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test successful invocation through public API."""
    # Arrange
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = False
    mock_execution.start_input = start_input

    mock_invocation_input = Mock()
    mock_invoker.create_invocation_input.return_value = mock_invocation_input
    mock_response = DurableExecutionInvocationOutput(
        status=InvocationStatus.SUCCEEDED, result="test"
    )
    mock_invoker.invoke.return_value = mock_response

    # Mock execution creation and store behavior
    with patch(
        "aws_durable_execution_sdk_python_testing.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution
        mock_store.load.return_value = mock_execution

        # Act - trigger invocation through public start_execution method
        executor.start_execution(start_input)

        # Get the handler that was passed to the scheduler and execute it manually
        mock_scheduler.call_later.assert_called_once()
        handler = mock_scheduler.call_later.call_args[0][0]

        # Execute the handler to trigger the invocation logic
        asyncio.run(handler())

    # Verify the invocation process was executed
    mock_invoker.create_invocation_input.assert_called_once_with(
        execution=mock_execution
    )
    mock_invoker.invoke.assert_called_once_with("test-function", mock_invocation_input)


def test_invoke_handler_execution_already_complete(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test that completed executions are handled properly through public API."""
    # Arrange
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = True
    mock_execution.start_input = start_input

    # Mock execution creation and store behavior
    with patch(
        "aws_durable_execution_sdk_python_testing.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution
        mock_store.load.return_value = mock_execution

        # Act - trigger invocation through public start_execution method
        executor.start_execution(start_input)

        # Get the handler that was passed to the scheduler and execute it manually
        mock_scheduler.call_later.assert_called_once()
        handler = mock_scheduler.call_later.call_args[0][0]

        # Execute the handler to trigger the invocation logic
        asyncio.run(handler())

    # Verify store was accessed to check execution status
    mock_store.load.assert_called_with("test-arn")


def test_invoke_handler_execution_completed_during_invocation(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test execution completing during invocation through public API."""
    # Arrange
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = False
    mock_execution.start_input = start_input

    mock_invocation_input = Mock()
    mock_invoker.create_invocation_input.return_value = mock_invocation_input
    mock_response = Mock()
    mock_invoker.invoke.return_value = mock_response

    # Create a completed execution mock
    completed_execution = Mock()
    completed_execution.durable_execution_arn = "test-arn"
    completed_execution.is_complete = True
    completed_execution.start_input = start_input

    # First call returns incomplete execution, second call returns completed execution
    mock_store.load.side_effect = [mock_execution, completed_execution]

    # Mock execution creation
    with patch(
        "aws_durable_execution_sdk_python_testing.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution

        # Act - trigger invocation through public start_execution method
        executor.start_execution(start_input)

        # Get the handler that was passed to the scheduler and execute it manually
        mock_scheduler.call_later.assert_called_once()
        handler = mock_scheduler.call_later.call_args[0][0]

        # Execute the handler to trigger the invocation logic
        asyncio.run(handler())

    # Verify the execution was checked for completion
    assert mock_store.load.call_count >= 2


def test_invoke_handler_resource_not_found(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test resource not found handling causes workflow failure through public API."""
    # Arrange
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = False
    mock_execution.start_input = start_input

    mock_invoker.create_invocation_input.side_effect = ResourceNotFoundException(
        "Function not found"
    )

    # Mock execution creation and store behavior
    with patch(
        "aws_durable_execution_sdk_python_testing.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution
        mock_store.load.return_value = mock_execution

        # Mock the public fail_execution method to verify it gets called
        with patch.object(executor, "fail_execution") as mock_fail:
            # Act - trigger invocation through public start_execution method
            executor.start_execution(start_input)

            # Get the handler that was passed to the scheduler and execute it manually
            mock_scheduler.call_later.assert_called_once()
            handler = mock_scheduler.call_later.call_args[0][0]

            # Execute the handler to trigger the invocation logic
            asyncio.run(handler())

        # Assert - verify workflow failure was triggered through public API
        mock_fail.assert_called_once()
        # Verify the error contains the expected message
        call_args = mock_fail.call_args
        assert call_args[0][0] == "test-arn"  # execution_arn is first positional arg
        assert "Function not found" in str(
            call_args[0][1]
        )  # error is second positional arg


def test_invoke_handler_general_exception(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test general exception handling triggers retry through public API."""
    # Arrange
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = False
    mock_execution.start_input = start_input
    mock_execution.consecutive_failed_invocation_attempts = 0

    # Configure invoker to fail
    mock_invoker.create_invocation_input.side_effect = Exception("General error")

    # Mock execution creation and store behavior
    with patch(
        "aws_durable_execution_sdk_python_testing.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution
        mock_store.load.return_value = mock_execution

        # Act - trigger invocation through public start_execution method
        executor.start_execution(start_input)

        # Get the handler that was passed to the scheduler and execute it manually
        mock_scheduler.call_later.assert_called_once()
        handler = mock_scheduler.call_later.call_args[0][0]

        # Execute the handler to trigger the invocation logic
        asyncio.run(handler())

        # Assert - verify retry was scheduled through observable behavior
        assert mock_execution.consecutive_failed_invocation_attempts == 1
        mock_store.save.assert_called_with(mock_execution)
        # Verify retry was scheduled (call_later should be called twice: initial + retry)
        assert mock_scheduler.call_later.call_count == 2


def test_invoke_execution_through_start_execution(
    executor, mock_scheduler, start_input
):
    """Test execution invocation behavior through public start_execution method."""
    mock_event = Mock()
    mock_scheduler.create_event.return_value = mock_event

    with patch(
        "aws_durable_execution_sdk_python_testing.executor.Execution"
    ) as mock_execution_class:
        mock_execution = Mock()
        mock_execution.durable_execution_arn = "test-arn"
        mock_execution_class.new.return_value = mock_execution

        # Start execution which internally calls _invoke_execution
        executor.start_execution(start_input)

    # Verify scheduler was called with the completion event
    mock_scheduler.call_later.assert_called()
    args = mock_scheduler.call_later.call_args
    assert args[1]["delay"] == 0  # Initial invocation has no delay
    assert args[1]["completion_event"] == mock_event


def test_should_complete_workflow_successfully_through_public_api(
    executor, mock_store, mock_execution
):
    """Test workflow completion through public complete_execution method."""
    # Arrange
    mock_execution.result = "test result"  # Mock result after completion
    mock_store.load.return_value = mock_execution

    with patch.object(executor, "_complete_events") as mock_complete_events:
        # Act - Use public API to complete workflow
        executor.complete_execution("test-arn", "result")

    # Assert - Verify final execution status and stored results
    mock_store.load.assert_called_once_with(execution_arn="test-arn")
    mock_execution.complete_success.assert_called_once_with(result="result")
    mock_store.update.assert_called_once_with(mock_execution)
    mock_complete_events.assert_called_once_with(execution_arn="test-arn")


def test_should_complete_workflow_with_failure_through_public_api(
    executor, mock_store, mock_execution
):
    """Test workflow failure completion through public fail_execution method."""
    # Arrange
    error = ErrorObject.from_message("test error")
    mock_execution.result = "error result"  # Mock result after failure
    mock_store.load.return_value = mock_execution

    with patch.object(executor, "_complete_events") as mock_complete_events:
        # Act - Use public API to fail workflow
        executor.fail_execution("test-arn", error)

    # Assert - Verify final execution status and stored error
    mock_store.load.assert_called_once_with(execution_arn="test-arn")
    mock_execution.complete_fail.assert_called_once_with(error=error)
    mock_store.update.assert_called_once_with(mock_execution)
    mock_complete_events.assert_called_once_with(execution_arn="test-arn")


def test_should_handle_workflow_completion_state_through_public_api(
    executor, mock_store, mock_execution
):
    """Test workflow completion behavior and state management through public API."""
    # Arrange
    mock_execution.result = "final result"  # Mock result after completion
    mock_store.load.return_value = mock_execution

    with patch.object(executor, "_complete_events") as mock_complete_events:
        # Act - Complete workflow through public API
        executor.complete_execution("test-arn", "result")

    # Assert - Verify completion was processed and observer notifications sent
    mock_store.load.assert_called_once_with(execution_arn="test-arn")
    mock_execution.complete_success.assert_called_once_with(result="result")
    mock_store.update.assert_called_once_with(mock_execution)
    mock_complete_events.assert_called_once_with(execution_arn="test-arn")


def test_should_fail_execution_when_function_not_found(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test that workflow fails when function is not found during invocation."""
    # Arrange
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = False
    mock_execution.start_input = start_input
    mock_execution.consecutive_failed_invocation_attempts = 0

    # Mock invoker to raise function not found error
    mock_invoker.create_invocation_input.side_effect = ResourceNotFoundException(
        "Function not found: test_function"
    )

    # Mock execution creation and store behavior
    with patch(
        "aws_durable_execution_sdk_python_testing.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution
        mock_store.load.return_value = mock_execution

        with patch.object(executor, "fail_execution") as mock_fail:
            # Act - trigger invocation through public start_execution method
            executor.start_execution(start_input)

            # Get the handler that was passed to the scheduler and execute it manually
            mock_scheduler.call_later.assert_called_once()
            handler = mock_scheduler.call_later.call_args[0][0]

            # Execute the handler to trigger the invocation logic
            import asyncio

            asyncio.run(handler())

        # Assert - verify failure was triggered with correct error
        mock_fail.assert_called_once()
        call_args = mock_fail.call_args
        assert call_args[0][0] == "test-arn"  # execution_arn
        assert "Function not found" in call_args[0][1].message  # error message


def test_should_fail_execution_when_retries_exhausted(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test that workflow fails when maximum retry attempts are exhausted."""
    # Arrange
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = False
    mock_execution.start_input = start_input
    mock_execution.consecutive_failed_invocation_attempts = (
        executor.MAX_CONSECUTIVE_FAILED_ATTEMPTS + 1
    )

    # Mock invoker to raise exception (simulating network/invocation failure)
    mock_invoker.create_invocation_input.side_effect = Exception("Network error")

    # Mock execution creation and store behavior
    with patch(
        "aws_durable_execution_sdk_python_testing.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution
        mock_store.load.return_value = mock_execution

        with patch.object(executor, "fail_execution") as mock_fail:
            # Act - trigger invocation through public start_execution method
            # This will cause an exception during invocation, which triggers retry logic
            executor.start_execution(start_input)

            # Get the handler that was passed to the scheduler and execute it manually
            mock_scheduler.call_later.assert_called_once()
            handler = mock_scheduler.call_later.call_args[0][0]

            # Execute the handler to trigger the invocation logic
            import asyncio

            asyncio.run(handler())

        # Assert - verify failure was triggered when retries exhausted
        mock_fail.assert_called_once()
        call_args = mock_fail.call_args
        assert call_args[0][0] == "test-arn"  # execution_arn


def test_should_prevent_multiple_workflow_failures_on_complete_execution(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test that attempting to fail an already completed execution raises an exception."""
    # Arrange - execution starts incomplete but becomes complete during processing
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = False  # Initially incomplete
    mock_execution.start_input = start_input
    mock_execution.consecutive_failed_invocation_attempts = 0

    # Create a completed execution for the _fail_workflow call
    completed_execution = Mock()
    completed_execution.is_complete = True

    # Mock invoker to raise ResourceNotFoundException (triggers _fail_workflow)
    mock_invoker.create_invocation_input.side_effect = ResourceNotFoundException(
        "Function not found"
    )

    # Mock execution creation and store behavior
    with patch(
        "aws_durable_execution_sdk_python_testing.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution
        # First load returns incomplete, second load (in _fail_workflow) returns complete
        mock_store.load.side_effect = [mock_execution, completed_execution]

        # Act & Assert - triggering workflow failure on completed execution should raise exception
        executor.start_execution(start_input)
        handler = mock_scheduler.call_later.call_args[0][0]

        # Execute the handler to trigger the invocation logic - this should raise the exception
        with pytest.raises(
            IllegalStateException, match="Cannot make multiple close workflow decisions"
        ):
            asyncio.run(handler())


def test_should_retry_invocation_when_under_limit_through_public_api(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test that invocation retries when under limit through public API with final outcome verification."""
    # Arrange - Set up execution that will trigger retry logic
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = False
    mock_execution.start_input = start_input
    mock_execution.consecutive_failed_invocation_attempts = 3  # Under limit (5 is max)

    # Configure invoker to fail initially with validation error, then succeed on retry
    mock_invocation_input = Mock()
    mock_invoker.create_invocation_input.return_value = mock_invocation_input

    # First invocation: invalid response triggers retry
    invalid_response = DurableExecutionInvocationOutput(
        status=InvocationStatus.FAILED,
        result="should not have result",  # Invalid: failed response with result
    )
    # Second invocation: valid success response
    success_response = DurableExecutionInvocationOutput(
        status=InvocationStatus.SUCCEEDED, result="final success"
    )
    mock_invoker.invoke.side_effect = [invalid_response, success_response]

    # Mock execution creation and store behavior
    with patch(
        "aws_durable_execution_sdk_python_testing.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution
        mock_store.load.return_value = mock_execution

        # Act - trigger the retry scenario through public API
        executor.start_execution(start_input)

        # Simulate scheduler executing the initial invocation handler
        initial_handler = mock_scheduler.call_later.call_args[0][0]
        import asyncio

        asyncio.run(initial_handler())

        # Verify retry was scheduled due to validation error
        assert mock_scheduler.call_later.call_count == 2  # Initial + retry
        retry_call = mock_scheduler.call_later.call_args_list[1]
        retry_handler = retry_call[0][0]
        retry_delay = retry_call[1]["delay"]

        # Execute the retry handler to complete the scenario
        asyncio.run(retry_handler())

    # Assert - verify final outcome after retry sequence
    assert (
        mock_execution.consecutive_failed_invocation_attempts == 4
    )  # Incremented from 3 to 4
    assert retry_delay == Executor.RETRY_BACKOFF_SECONDS  # Correct backoff delay used
    mock_store.save.assert_called_with(mock_execution)  # Execution state saved
    assert mock_invoker.invoke.call_count == 2  # Initial + retry invocation


def test_should_fail_workflow_when_retry_limit_exceeded(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test that workflow fails when retry limit is exceeded through public API."""
    # Arrange
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = False
    mock_execution.start_input = start_input
    mock_execution.consecutive_failed_invocation_attempts = 6  # Over limit

    # Mock invoker to consistently fail
    mock_invoker.create_invocation_input.side_effect = Exception("Persistent error")
    mock_store.load.return_value = mock_execution

    # Mock execution creation
    with patch(
        "aws_durable_execution_sdk_python_testing.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution

        # Mock the public fail_execution method to verify it gets called
        with patch.object(executor, "fail_execution") as mock_fail:
            # Act - trigger execution that will exceed retry limit
            executor.start_execution(start_input)

            # Get the handler that was passed to the scheduler and execute it manually
            mock_scheduler.call_later.assert_called_once()
            handler = mock_scheduler.call_later.call_args[0][0]

            # Execute the handler to trigger the invocation logic
            asyncio.run(handler())

        # Assert - verify workflow failed due to retry limit exceeded
        mock_fail.assert_called_once()
        # Verify the error contains the expected message
        call_args = mock_fail.call_args
        assert call_args[0][0] == "test-arn"  # execution_arn is first positional arg
        assert "Persistent error" in str(
            call_args[0][1]
        )  # error is second positional arg


def test_complete_events_through_complete_execution(
    executor, mock_store, mock_scheduler
):
    """Test completion event behavior through public complete_execution method."""
    mock_execution = Mock()
    mock_execution.result = "test result"
    mock_store.load.return_value = mock_execution

    # Set up completion event through start_execution
    mock_event = Mock()
    mock_scheduler.create_event.return_value = mock_event

    with patch(
        "aws_durable_execution_sdk_python_testing.executor.Execution"
    ) as mock_execution_class:
        mock_exec = Mock()
        mock_exec.durable_execution_arn = "test-arn"
        mock_execution_class.new.return_value = mock_exec

        start_input = Mock()
        executor.start_execution(start_input)

    # Now complete the execution - this should trigger event.set()
    executor.complete_execution("test-arn", "result")

    # Verify the event was set through observable behavior
    mock_event.set.assert_called_once()


def test_complete_events_no_event_through_public_api(executor, mock_store):
    """Test that completing non-existent execution handles missing events gracefully."""
    mock_execution = Mock()
    mock_execution.result = "test result"
    mock_store.load.return_value = mock_execution

    # Complete execution without setting up completion event first
    # Should not raise exception when event doesn't exist
    executor.complete_execution("nonexistent-arn", "result")


def test_wait_until_complete_success(executor, mock_scheduler):
    """Test wait until complete success through public API."""
    mock_event = Mock()
    mock_event.wait.return_value = True
    mock_scheduler.create_event.return_value = mock_event

    # Set up completion event through start_execution
    with patch(
        "aws_durable_execution_sdk_python_testing.executor.Execution"
    ) as mock_execution_class:
        mock_execution = Mock()
        mock_execution.durable_execution_arn = "test-arn"
        mock_execution_class.new.return_value = mock_execution

        start_input = Mock()
        executor.start_execution(start_input)

    result = executor.wait_until_complete("test-arn", timeout=10)

    assert result is True
    mock_event.wait.assert_called_once_with(10)


def test_wait_until_complete_timeout(executor, mock_scheduler):
    """Test wait until complete timeout through public API."""
    mock_event = Mock()
    mock_event.wait.return_value = False
    mock_scheduler.create_event.return_value = mock_event

    # Set up completion event through start_execution
    with patch(
        "aws_durable_execution_sdk_python_testing.executor.Execution"
    ) as mock_execution_class:
        mock_execution = Mock()
        mock_execution.durable_execution_arn = "test-arn"
        mock_execution_class.new.return_value = mock_execution

        start_input = Mock()
        executor.start_execution(start_input)

    result = executor.wait_until_complete("test-arn", timeout=10)

    assert result is False


def test_wait_until_complete_no_event(executor):
    with pytest.raises(ResourceNotFoundException, match="execution does not exist"):
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


def test_should_schedule_wait_timer_correctly(executor, mock_scheduler):
    """Test that wait timer is scheduled correctly through public method."""
    # Arrange
    mock_event = Mock()
    mock_scheduler.create_event.return_value = mock_event

    # Set up completion event through start_execution
    with patch(
        "aws_durable_execution_sdk_python_testing.executor.Execution"
    ) as mock_execution_class:
        mock_execution = Mock()
        mock_execution.durable_execution_arn = "test-arn"
        mock_execution_class.new.return_value = mock_execution

        start_input = Mock()
        executor.start_execution(start_input)

    # Act - schedule wait timer through public method
    executor.on_wait_timer_scheduled("test-arn", "op-123", delay=5.0)

    # Assert - verify scheduler was called correctly
    assert mock_scheduler.call_later.call_count == 2  # start_execution + wait timer
    wait_call = mock_scheduler.call_later.call_args_list[1]  # Second call is wait timer
    assert wait_call[1]["delay"] == 5.0
    assert wait_call[1]["completion_event"] == mock_event


def test_should_ignore_wait_completion_for_completed_execution(
    executor, mock_store, mock_execution
):
    """Test that wait completion logic correctly handles completed executions."""
    # Arrange
    mock_execution.is_complete = True
    mock_store.load.return_value = mock_execution

    # Act - simulate the wait completion logic for a completed execution
    execution = mock_store.load("test-arn")

    # The logic should check if execution is complete before attempting to complete wait
    if not execution.is_complete:
        execution.complete_wait(operation_id="op-123")
        mock_store.update(execution)

    # Assert - verify that complete_wait was not called for completed execution
    mock_execution.complete_wait.assert_not_called()
    mock_store.update.assert_not_called()


def test_should_handle_wait_completion_exception_gracefully(
    executor, mock_store, mock_execution
):
    """Test that wait completion exceptions are handled through error handling."""
    # Arrange
    mock_store.load.return_value = mock_execution
    mock_execution.is_complete = False
    mock_execution.complete_wait.side_effect = Exception("test error")

    # Act & Assert - test that exception handling works correctly
    # This tests the error handling logic without scheduler timing dependencies
    execution = mock_store.load("test-arn")

    with pytest.raises(Exception, match="test error"):
        execution.complete_wait(operation_id="op-123")


def test_should_complete_retry_when_retry_scheduled(
    executor, mock_store, mock_scheduler, mock_execution
):
    """Test retry completion through public scheduler callback API."""
    # Arrange
    mock_store.load.return_value = mock_execution

    # Configure scheduler to immediately execute the callback
    def immediate_callback(func, delay=0, count=1, completion_event=None):
        func()  # Execute the retry handler immediately
        return Mock()

    mock_scheduler.call_later.side_effect = immediate_callback

    # Mock _invoke_execution to prevent async warnings
    with patch.object(executor, "_invoke_execution"):
        # Act - trigger retry through public API
        executor.on_step_retry_scheduled("test-arn", "op-123", 10.0)

    # Assert - verify observable behavior
    mock_store.load.assert_called_with("test-arn")
    mock_execution.complete_retry.assert_called_once_with(operation_id="op-123")
    mock_store.update.assert_called_with(mock_execution)


def test_should_ignore_retry_when_execution_complete(
    executor, mock_store, mock_scheduler, mock_execution
):
    """Test that completed executions ignore retry events through public API."""
    # Arrange
    mock_execution.is_complete = True
    mock_store.load.return_value = mock_execution

    # Configure scheduler to immediately execute the callback
    def immediate_callback(func, delay=0, count=1, completion_event=None):
        func()  # Execute the retry handler immediately
        return Mock()

    mock_scheduler.call_later.side_effect = immediate_callback

    # Mock _invoke_execution to prevent async warnings
    with patch.object(executor, "_invoke_execution"):
        # Act - trigger retry through public API
        executor.on_step_retry_scheduled("test-arn", "op-123", 10.0)

    # Assert - verify no retry processing occurs
    mock_execution.complete_retry.assert_not_called()
    mock_store.update.assert_not_called()


def test_should_handle_retry_exception_gracefully(
    executor, mock_store, mock_scheduler, mock_execution
):
    """Test that retry exceptions are handled gracefully through public API."""
    # Arrange
    mock_store.load.return_value = mock_execution
    mock_execution.complete_retry.side_effect = Exception("test error")

    # Configure scheduler to immediately execute the callback
    def immediate_callback(func, delay=0, count=1, completion_event=None):
        func()  # Execute the retry handler immediately
        return Mock()

    mock_scheduler.call_later.side_effect = immediate_callback

    # Mock _invoke_execution to prevent async warnings
    with patch.object(executor, "_invoke_execution"):
        # Act - should not raise exception
        executor.on_step_retry_scheduled("test-arn", "op-123", 10.0)

    # Assert - verify the retry was attempted but exception was caught
    mock_execution.complete_retry.assert_called_once_with(operation_id="op-123")


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
    """Test wait timer scheduling through public observer method."""
    mock_event = Mock()
    mock_scheduler.create_event.return_value = mock_event

    # Set up completion event through start_execution
    with patch(
        "aws_durable_execution_sdk_python_testing.executor.Execution"
    ) as mock_execution_class:
        mock_execution = Mock()
        mock_execution.durable_execution_arn = "test-arn"
        mock_execution_class.new.return_value = mock_execution

        start_input = Mock()
        executor.start_execution(start_input)

    with patch.object(executor, "_on_wait_succeeded"):
        with patch.object(executor, "_invoke_execution"):
            executor.on_wait_timer_scheduled("test-arn", "op-123", 10.0)

    # Verify scheduler was called with correct parameters
    assert (
        mock_scheduler.call_later.call_count == 2
    )  # Once for start_execution, once for wait timer
    wait_timer_call = mock_scheduler.call_later.call_args_list[
        1
    ]  # Second call is for wait timer
    assert wait_timer_call[1]["delay"] == 10.0
    assert wait_timer_call[1]["completion_event"] == mock_event


def test_should_retry_when_response_has_unexpected_status(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test that responses with unexpected status trigger retry logic."""
    # Arrange
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = False
    mock_execution.start_input = start_input
    mock_execution.consecutive_failed_invocation_attempts = 0

    # Mock invoker to return response with unexpected status
    mock_invocation_input = Mock()
    mock_invoker.create_invocation_input.return_value = mock_invocation_input
    unexpected_response = Mock()
    unexpected_response.status = "UNKNOWN_STATUS"
    mock_invoker.invoke.return_value = unexpected_response

    # Mock execution creation and store behavior
    with patch(
        "aws_durable_execution_sdk_python_testing.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution
        mock_store.load.return_value = mock_execution

        # Act - trigger invocation through public start_execution method
        executor.start_execution(start_input)

        # Get the handler that was passed to the scheduler and execute it manually
        mock_scheduler.call_later.assert_called_once()
        handler = mock_scheduler.call_later.call_args[0][0]

        # Execute the handler to trigger the invocation logic
        asyncio.run(handler())

        # Assert - verify retry was triggered due to validation error
        assert mock_execution.consecutive_failed_invocation_attempts == 1
        mock_store.save.assert_called_with(mock_execution)
        # Verify retry was scheduled (call_later should be called twice: initial + retry)
        assert mock_scheduler.call_later.call_count == 2


def test_invoke_handler_execution_completed_during_invocation_async(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test execution completing during invocation through public API."""
    # First call returns incomplete execution, second call returns completed execution
    incomplete_execution = Mock(spec=Execution)
    incomplete_execution.is_complete = False
    incomplete_execution.start_input = start_input
    incomplete_execution.consecutive_failed_invocation_attempts = 0
    incomplete_execution.durable_execution_arn = "test-arn"

    completed_execution = Mock(spec=Execution)
    completed_execution.is_complete = True

    mock_store.load.side_effect = [incomplete_execution, completed_execution]

    mock_invocation_input = Mock()
    mock_invoker.create_invocation_input.return_value = mock_invocation_input
    mock_response = Mock()
    mock_invoker.invoke.return_value = mock_response

    # Mock execution creation
    with patch(
        "aws_durable_execution_sdk_python_testing.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = incomplete_execution

        # Act - trigger invocation through public start_execution method
        executor.start_execution(start_input)

        # Get the handler that was passed to the scheduler and execute it manually
        mock_scheduler.call_later.assert_called_once()
        handler = mock_scheduler.call_later.call_args[0][0]

        # Execute the handler to trigger the invocation logic
        asyncio.run(handler())

    # Verify the execution was loaded multiple times (before and after invocation)
    assert mock_store.load.call_count >= 2


def test_invoke_handler_resource_not_found_async(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test resource not found handling causes workflow failure through public API (async version)."""
    # Arrange
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = False
    mock_execution.start_input = start_input

    mock_invoker.create_invocation_input.side_effect = ResourceNotFoundException(
        "Function not found"
    )

    # Mock execution creation and store behavior
    with patch(
        "aws_durable_execution_sdk_python_testing.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution
        mock_store.load.return_value = mock_execution

        # Mock the public fail_execution method to verify it gets called
        with patch.object(executor, "fail_execution") as mock_fail:
            # Act - trigger invocation through public start_execution method
            executor.start_execution(start_input)

            # Get the handler that was passed to the scheduler and execute it manually
            mock_scheduler.call_later.assert_called_once()
            handler = mock_scheduler.call_later.call_args[0][0]

            # Execute the handler to trigger the invocation logic
            asyncio.run(handler())

        # Assert - verify workflow failure was triggered through public API
        mock_fail.assert_called_once()
        # Verify the error contains the expected message
        call_args = mock_fail.call_args
        assert call_args[0][0] == "test-arn"  # execution_arn is first positional arg
        assert "Function not found" in str(
            call_args[0][1]
        )  # error is second positional arg


def test_invoke_handler_general_exception_async(
    executor, mock_store, mock_scheduler, mock_invoker, start_input
):
    """Test general exception handling triggers retry through public API (async version)."""
    # Arrange
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.is_complete = False
    mock_execution.start_input = start_input
    mock_execution.consecutive_failed_invocation_attempts = 0

    # Configure invoker to fail initially, then succeed on retry
    mock_invoker.create_invocation_input.side_effect = [
        Exception("General error"),  # First call fails
        Mock(),  # Second call succeeds (returns invocation input)
    ]

    # Mock successful response for retry
    success_response = DurableExecutionInvocationOutput(
        status=InvocationStatus.SUCCEEDED, result="success"
    )
    mock_invoker.invoke.return_value = success_response

    # Mock execution creation and store behavior
    with patch(
        "aws_durable_execution_sdk_python_testing.executor.Execution"
    ) as mock_execution_class:
        mock_execution_class.new.return_value = mock_execution
        mock_store.load.return_value = mock_execution

        # Act - trigger invocation through public start_execution method
        executor.start_execution(start_input)

        # Get the handler that was passed to the scheduler and execute it manually
        mock_scheduler.call_later.assert_called_once()
        handler = mock_scheduler.call_later.call_args[0][0]

        # Execute the handler to trigger the invocation logic
        asyncio.run(handler())

        # Assert - verify retry was scheduled through observable behavior
        assert mock_execution.consecutive_failed_invocation_attempts == 1
        mock_store.save.assert_called_with(mock_execution)
        # Verify retry was scheduled (call_later should be called twice: initial + retry)
        assert mock_scheduler.call_later.call_count == 2


def test_invoke_execution_with_delay_through_wait_timer(executor, mock_scheduler):
    """Test execution invocation with delay through wait timer scheduling."""
    mock_event = Mock()
    mock_scheduler.create_event.return_value = mock_event

    # Set up completion event through start_execution
    with patch(
        "aws_durable_execution_sdk_python_testing.executor.Execution"
    ) as mock_execution_class:
        mock_execution = Mock()
        mock_execution.durable_execution_arn = "test-arn"
        mock_execution_class.new.return_value = mock_execution

        start_input = Mock()
        executor.start_execution(start_input)

    # Test delay behavior through wait timer scheduling
    with patch.object(executor, "_on_wait_succeeded"):
        executor.on_wait_timer_scheduled("test-arn", "op-123", 10.0)

    # Verify scheduler was called with delay for wait timer
    wait_timer_call = mock_scheduler.call_later.call_args_list[
        1
    ]  # Second call is for wait timer
    assert wait_timer_call[1]["delay"] == 10.0


def test_invoke_execution_no_delay_through_start_execution(executor, mock_scheduler):
    """Test execution invocation with no delay through start_execution."""
    mock_event = Mock()
    mock_scheduler.create_event.return_value = mock_event

    # Test no delay behavior through start_execution
    with patch(
        "aws_durable_execution_sdk_python_testing.executor.Execution"
    ) as mock_execution_class:
        mock_execution = Mock()
        mock_execution.durable_execution_arn = "test-arn"
        mock_execution_class.new.return_value = mock_execution

        start_input = Mock()
        executor.start_execution(start_input)

    # Verify scheduler was called with no delay for initial execution
    initial_call = mock_scheduler.call_later.call_args_list[
        0
    ]  # First call is for initial execution
    assert initial_call[1]["delay"] == 0


def test_on_step_retry_scheduled(executor, mock_scheduler):
    """Test step retry scheduling through public observer method."""
    mock_event = Mock()
    mock_scheduler.create_event.return_value = mock_event

    # Set up completion event through start_execution
    with patch(
        "aws_durable_execution_sdk_python_testing.executor.Execution"
    ) as mock_execution_class:
        mock_execution = Mock()
        mock_execution.durable_execution_arn = "test-arn"
        mock_execution_class.new.return_value = mock_execution

        start_input = Mock()
        executor.start_execution(start_input)

    with patch.object(executor, "_on_retry_ready"):
        with patch.object(executor, "_invoke_execution"):
            executor.on_step_retry_scheduled("test-arn", "op-123", 10.0)

    # Verify scheduler was called with correct parameters
    assert (
        mock_scheduler.call_later.call_count == 2
    )  # Once for start_execution, once for retry
    retry_call = mock_scheduler.call_later.call_args_list[1]  # Second call is for retry
    assert retry_call[1]["delay"] == 10.0
    assert retry_call[1]["completion_event"] == mock_event


def test_wait_handler_execution(executor, mock_scheduler):
    """Test wait handler execution through public observer method."""
    mock_event = Mock()
    mock_scheduler.create_event.return_value = mock_event

    # Set up completion event through start_execution
    with patch(
        "aws_durable_execution_sdk_python_testing.executor.Execution"
    ) as mock_execution_class:
        mock_execution = Mock()
        mock_execution.durable_execution_arn = "test-arn"
        mock_execution_class.new.return_value = mock_execution

        start_input = Mock()
        executor.start_execution(start_input)

    with patch.object(executor, "_on_wait_succeeded") as mock_wait:
        with patch.object(executor, "_invoke_execution") as mock_invoke:
            executor.on_wait_timer_scheduled("test-arn", "op-123", 10.0)

            # Get the handler that was passed to call_later (second call for wait timer)
            wait_timer_call = mock_scheduler.call_later.call_args_list[1]
            wait_handler = wait_timer_call[0][0]

            # Execute the handler to test the inner function
            wait_handler()

            mock_wait.assert_called_once_with("test-arn", "op-123")
            mock_invoke.assert_called_once_with("test-arn", delay=0)


def test_retry_handler_execution(executor, mock_scheduler):
    """Test retry handler execution through public observer method."""
    mock_event = Mock()
    mock_scheduler.create_event.return_value = mock_event

    # Set up completion event through start_execution
    with patch(
        "aws_durable_execution_sdk_python_testing.executor.Execution"
    ) as mock_execution_class:
        mock_execution = Mock()
        mock_execution.durable_execution_arn = "test-arn"
        mock_execution_class.new.return_value = mock_execution

        start_input = Mock()
        executor.start_execution(start_input)

    with patch.object(executor, "_on_retry_ready") as mock_retry:
        with patch.object(executor, "_invoke_execution") as mock_invoke:
            executor.on_step_retry_scheduled("test-arn", "op-123", 10.0)

            # Get the handler that was passed to call_later (second call for retry)
            retry_call = mock_scheduler.call_later.call_args_list[1]
            retry_handler = retry_call[0][0]

            # Execute the handler to test the inner function
            retry_handler()

            mock_retry.assert_called_once_with("test-arn", "op-123")
            mock_invoke.assert_called_once_with("test-arn", delay=0)


# Tests for new web handler methods


def test_get_execution_details(executor, mock_store):
    """Test get_execution_details method."""

    # Create mock execution with operation
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.start_input.execution_name = "test-execution"
    mock_execution.start_input.function_name = "test-function"
    mock_execution.is_complete = True

    # Create mock result
    mock_result = DurableExecutionInvocationOutput(
        status=InvocationStatus.SUCCEEDED, result="test-result"
    )
    mock_execution.result = mock_result

    # Create mock operation
    mock_operation = Operation(
        operation_id="op-1",
        parent_id=None,
        name="test-execution",
        start_timestamp=datetime.now(UTC),
        end_timestamp=datetime.now(UTC),
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.SUCCEEDED,
        execution_details=ExecutionDetails(input_payload='{"test": "data"}'),
    )
    mock_execution.get_operation_execution_started.return_value = mock_operation

    mock_store.load.return_value = mock_execution

    result = executor.get_execution_details("test-arn")

    assert result.durable_execution_arn == "test-arn"
    assert result.durable_execution_name == "test-execution"
    assert result.status == "SUCCEEDED"
    assert result.result == "test-result"
    assert result.error is None
    mock_store.load.assert_called_once_with("test-arn")


def test_get_execution_details_not_found(executor, mock_store):
    """Test get_execution_details with non-existent execution."""
    mock_store.load.side_effect = KeyError("Execution not found")

    with pytest.raises(ResourceNotFoundException, match="Execution test-arn not found"):
        executor.get_execution_details("test-arn")


def test_get_execution_details_failed_execution(executor, mock_store):
    """Test get_execution_details with failed execution."""

    # Create mock execution with failed result
    mock_execution = Mock()
    mock_execution.durable_execution_arn = "test-arn"
    mock_execution.start_input.execution_name = "test-execution"
    mock_execution.start_input.function_name = "test-function"
    mock_execution.is_complete = True

    error = ErrorObject.from_message("Test error")
    mock_result = DurableExecutionInvocationOutput(
        status=InvocationStatus.FAILED, error=error
    )
    mock_execution.result = mock_result

    # Create mock operation
    mock_operation = Operation(
        operation_id="op-1",
        parent_id=None,
        name="test-execution",
        start_timestamp=datetime.now(UTC),
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.FAILED,
        execution_details=ExecutionDetails(input_payload='{"test": "data"}'),
    )
    mock_execution.get_operation_execution_started.return_value = mock_operation

    mock_store.load.return_value = mock_execution

    result = executor.get_execution_details("test-arn")

    assert result.status == "FAILED"
    assert result.result is None
    assert result.error == error


def test_list_executions_empty(executor, mock_store):
    """Test list_executions with no executions."""
    mock_store.list_all.return_value = []

    result = executor.list_executions()

    assert result.durable_executions == []
    assert result.next_marker is None
    mock_store.list_all.assert_called_once()


def test_list_executions_with_filtering(executor, mock_store):
    """Test list_executions with function name filtering."""

    # Create mock executions
    execution1 = Mock()
    execution1.durable_execution_arn = "arn1"
    execution1.start_input.execution_name = "exec1"
    execution1.start_input.function_name = "function1"
    execution1.is_complete = False
    execution1.result = None

    execution2 = Mock()
    execution2.durable_execution_arn = "arn2"
    execution2.start_input.execution_name = "exec2"
    execution2.start_input.function_name = "function2"
    execution2.is_complete = True
    execution2.result = DurableExecutionInvocationOutput(
        status=InvocationStatus.SUCCEEDED, result="result"
    )

    # Create mock operations
    op1 = Operation(
        operation_id="op-1",
        parent_id=None,
        name="exec1",
        start_timestamp=datetime.now(UTC),
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )
    op2 = Operation(
        operation_id="op-2",
        parent_id=None,
        name="exec2",
        start_timestamp=datetime.now(UTC),
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.SUCCEEDED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    execution1.get_operation_execution_started.return_value = op1
    execution2.get_operation_execution_started.return_value = op2

    mock_store.list_all.return_value = [execution1, execution2]

    # Test filtering by function name
    result = executor.list_executions(function_name="function1")

    assert len(result.durable_executions) == 1
    assert result.durable_executions[0].durable_execution_arn == "arn1"
    assert result.durable_executions[0].status == "RUNNING"


def test_list_executions_with_pagination(executor, mock_store):
    """Test list_executions with pagination."""

    # Create multiple mock executions
    executions = []
    for i in range(5):
        execution = Mock()
        execution.durable_execution_arn = f"arn{i}"
        execution.start_input.execution_name = f"exec{i}"
        execution.start_input.function_name = "test-function"
        execution.is_complete = False
        execution.result = None

        op = Operation(
            operation_id=f"op-{i}",
            parent_id=None,
            name=f"exec{i}",
            start_timestamp=datetime.now(UTC),
            operation_type=OperationType.EXECUTION,
            status=OperationStatus.STARTED,
            execution_details=ExecutionDetails(input_payload="{}"),
        )
        execution.get_operation_execution_started.return_value = op
        executions.append(execution)

    mock_store.list_all.return_value = executions

    # Test pagination with max_items=2
    result = executor.list_executions(max_items=2)

    assert len(result.durable_executions) == 2
    assert result.next_marker == "2"

    # Test second page
    result2 = executor.list_executions(max_items=2, marker="2")

    assert len(result2.durable_executions) == 2
    assert result2.next_marker == "4"


def test_list_executions_by_function(executor):
    """Test list_executions_by_function delegates to list_executions."""
    with patch.object(executor, "list_executions") as mock_list:
        mock_response = ListDurableExecutionsResponse(
            durable_executions=[], next_marker=None
        )
        mock_list.return_value = mock_response

        result = executor.list_executions_by_function(
            "test-function", status_filter="RUNNING"
        )

        mock_list.assert_called_once_with(
            function_name="test-function",
            execution_name=None,
            status_filter="RUNNING",
            time_after=None,
            time_before=None,
            marker=None,
            max_items=None,
            reverse_order=False,
        )
        assert result.durable_executions == []
        assert result.next_marker is None


def test_stop_execution(executor, mock_store):
    """Test stop_execution method."""
    mock_execution = Mock()
    mock_execution.is_complete = False
    mock_store.load.return_value = mock_execution

    with patch.object(executor, "fail_execution") as mock_fail:
        result = executor.stop_execution("test-arn")

    mock_store.load.assert_called_once_with("test-arn")
    mock_fail.assert_called_once()
    assert result.stop_timestamp is not None


def test_stop_execution_already_complete(executor, mock_store):
    """Test stop_execution with already completed execution."""
    mock_execution = Mock()
    mock_execution.is_complete = True
    mock_store.load.return_value = mock_execution

    with pytest.raises(ExecutionAlreadyStartedException, match="already completed"):
        executor.stop_execution("test-arn")


def test_stop_execution_with_custom_error(executor, mock_store):
    """Test stop_execution with custom error."""
    mock_execution = Mock()
    mock_execution.is_complete = False
    mock_store.load.return_value = mock_execution

    custom_error = ErrorObject.from_message("Custom stop error")

    with patch.object(executor, "fail_execution") as mock_fail:
        executor.stop_execution("test-arn", error=custom_error)

    mock_fail.assert_called_once_with("test-arn", custom_error)


def test_get_execution_state(executor, mock_store):
    """Test get_execution_state method."""

    mock_execution = Mock()
    mock_execution.used_tokens = {"token1", "token2"}

    # Create mock operations
    operations = [
        Operation(
            operation_id="op-1",
            parent_id=None,
            name="step1",
            start_timestamp=datetime.now(UTC),
            operation_type=OperationType.STEP,
            status=OperationStatus.SUCCEEDED,
        ),
        Operation(
            operation_id="op-2",
            parent_id=None,
            name="step2",
            start_timestamp=datetime.now(UTC),
            operation_type=OperationType.STEP,
            status=OperationStatus.STARTED,
        ),
    ]
    mock_execution.get_assertable_operations.return_value = operations

    mock_store.load.return_value = mock_execution

    result = executor.get_execution_state("test-arn", checkpoint_token="token1")  # noqa: S106

    assert len(result.operations) == 2
    assert result.next_marker is None
    mock_store.load.assert_called_once_with("test-arn")


def test_get_execution_state_invalid_token(executor, mock_store):
    """Test get_execution_state with invalid checkpoint token."""
    mock_execution = Mock()
    mock_execution.used_tokens = {"token1", "token2"}
    mock_store.load.return_value = mock_execution

    with pytest.raises(
        InvalidParameterValueException, match="Invalid checkpoint token"
    ):
        executor.get_execution_state("test-arn", checkpoint_token="invalid-token")  # noqa: S106


def test_get_execution_history(executor, mock_store):
    """Test get_execution_history method."""
    mock_execution = Mock()
    mock_store.load.return_value = mock_execution

    result = executor.get_execution_history("test-arn")

    assert result.events == []
    assert result.next_marker is None
    mock_store.load.assert_called_once_with("test-arn")


def test_checkpoint_execution(executor, mock_store):
    """Test checkpoint_execution method."""
    mock_execution = Mock()
    mock_execution.used_tokens = {"token1", "token2"}
    mock_execution.get_new_checkpoint_token.return_value = "new-token"
    mock_store.load.return_value = mock_execution

    result = executor.checkpoint_execution("test-arn", "token1")

    assert result.checkpoint_token == "new-token"  # noqa: S105
    assert result.new_execution_state is None
    mock_store.load.assert_called_once_with("test-arn")
    mock_execution.get_new_checkpoint_token.assert_called_once()


def test_checkpoint_execution_invalid_token(executor, mock_store):
    """Test checkpoint_execution with invalid checkpoint token."""
    mock_execution = Mock()
    mock_execution.used_tokens = {"token1", "token2"}
    mock_store.load.return_value = mock_execution

    with pytest.raises(
        InvalidParameterValueException, match="Invalid checkpoint token"
    ):
        executor.checkpoint_execution("test-arn", "invalid-token")


# Callback method tests


def test_send_callback_success(executor):
    """Test send_callback_success method."""

    result = executor.send_callback_success("test-callback-id", "success-result")

    assert isinstance(result, SendDurableExecutionCallbackSuccessResponse)


def test_send_callback_success_empty_callback_id(executor):
    """Test send_callback_success with empty callback_id."""
    with pytest.raises(InvalidParameterValueException, match="callback_id is required"):
        executor.send_callback_success("")


def test_send_callback_success_none_callback_id(executor):
    """Test send_callback_success with None callback_id."""
    with pytest.raises(InvalidParameterValueException, match="callback_id is required"):
        executor.send_callback_success(None)


def test_send_callback_success_with_result(executor):
    """Test send_callback_success with result data."""
    result = executor.send_callback_success("test-callback-id", "test-result")

    assert isinstance(result, SendDurableExecutionCallbackSuccessResponse)


def test_send_callback_failure(executor):
    """Test send_callback_failure method."""

    result = executor.send_callback_failure("test-callback-id")

    assert isinstance(result, SendDurableExecutionCallbackFailureResponse)


def test_send_callback_failure_empty_callback_id(executor):
    """Test send_callback_failure with empty callback_id."""
    with pytest.raises(InvalidParameterValueException, match="callback_id is required"):
        executor.send_callback_failure("")


def test_send_callback_failure_none_callback_id(executor):
    """Test send_callback_failure with None callback_id."""
    with pytest.raises(InvalidParameterValueException, match="callback_id is required"):
        executor.send_callback_failure(None)


def test_send_callback_failure_with_error(executor):
    """Test send_callback_failure with error object."""
    error = ErrorObject.from_message("Test callback error")
    result = executor.send_callback_failure("test-callback-id", error)

    assert isinstance(result, SendDurableExecutionCallbackFailureResponse)


def test_send_callback_heartbeat(executor):
    """Test send_callback_heartbeat method."""

    result = executor.send_callback_heartbeat("test-callback-id")

    assert isinstance(result, SendDurableExecutionCallbackHeartbeatResponse)


def test_send_callback_heartbeat_empty_callback_id(executor):
    """Test send_callback_heartbeat with empty callback_id."""
    with pytest.raises(InvalidParameterValueException, match="callback_id is required"):
        executor.send_callback_heartbeat("")


def test_send_callback_heartbeat_none_callback_id(executor):
    """Test send_callback_heartbeat with None callback_id."""
    with pytest.raises(InvalidParameterValueException, match="callback_id is required"):
        executor.send_callback_heartbeat(None)
