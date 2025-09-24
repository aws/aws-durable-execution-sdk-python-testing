"""Unit tests for execution module."""

from datetime import UTC, datetime
from unittest.mock import patch

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    Operation,
    OperationStatus,
    OperationType,
    StepDetails,
)

from aws_durable_execution_sdk_python_testing.exceptions import IllegalStateError
from aws_durable_execution_sdk_python_testing.execution import Execution
from aws_durable_execution_sdk_python_testing.model import StartDurableExecutionInput


def test_execution_init():
    """Test Execution initialization."""
    arn = "test-arn"
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
    )
    operations = []

    execution = Execution(arn, start_input, operations)

    assert execution.durable_execution_arn == arn
    assert execution.start_input == start_input
    assert execution.operations == operations
    assert execution.updates == []
    assert execution.used_tokens == set()
    assert execution.token_sequence == 0
    assert execution.is_complete is False
    assert execution.consecutive_failed_invocation_attempts == 0


@patch("aws_durable_execution_sdk_python_testing.execution.uuid4")
def test_execution_new(mock_uuid4):
    """Test Execution.new static method."""
    mock_uuid = "test-uuid-123"
    mock_uuid4.return_value = mock_uuid

    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
    )

    execution = Execution.new(start_input)

    assert execution.durable_execution_arn == str(mock_uuid)
    assert execution.start_input == start_input
    assert execution.operations == []


@patch("aws_durable_execution_sdk_python_testing.execution.datetime")
def test_execution_start(mock_datetime):
    """Test Execution.start method."""
    mock_now = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)
    mock_datetime.now.return_value = mock_now

    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
        input='{"key": "value"}',
    )
    execution = Execution("test-arn", start_input, [])

    execution.start()

    assert len(execution.operations) == 1
    operation = execution.operations[0]
    assert operation.operation_id == "test-invocation-id"
    assert operation.parent_id is None
    assert operation.name == "test-execution"
    assert operation.start_timestamp == mock_now
    assert operation.operation_type == OperationType.EXECUTION
    assert operation.status == OperationStatus.STARTED
    assert operation.execution_details.input_payload == '"{\\"key\\": \\"value\\"}"'


def test_get_operation_execution_started():
    """Test get_operation_execution_started method."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    execution = Execution("test-arn", start_input, [])
    execution.start()

    result = execution.get_operation_execution_started()

    assert result == execution.operations[0]
    assert result.operation_type == OperationType.EXECUTION


def test_get_operation_execution_started_not_started():
    """Test get_operation_execution_started raises error when not started."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
    )
    execution = Execution("test-arn", start_input, [])

    with pytest.raises(ValueError, match="execution not started"):
        execution.get_operation_execution_started()


def test_get_new_checkpoint_token():
    """Test get_new_checkpoint_token method."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
    )
    execution = Execution("test-arn", start_input, [])

    token1 = execution.get_new_checkpoint_token()
    token2 = execution.get_new_checkpoint_token()

    assert execution.token_sequence == 2
    assert token1 in execution.used_tokens
    assert token2 in execution.used_tokens
    assert token1 != token2


def test_get_navigable_operations():
    """Test get_navigable_operations method."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
    )
    operations = [
        Operation(
            operation_id="op1",
            parent_id=None,
            name="test",
            start_timestamp=datetime.now(UTC),
            operation_type=OperationType.EXECUTION,
            status=OperationStatus.STARTED,
        )
    ]
    execution = Execution("test-arn", start_input, operations)

    result = execution.get_navigable_operations()

    assert result == operations


def test_get_assertable_operations():
    """Test get_assertable_operations method."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
    )
    execution_op = Operation(
        operation_id="exec-op",
        parent_id=None,
        name="execution",
        start_timestamp=datetime.now(UTC),
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
    )
    step_op = Operation(
        operation_id="step-op",
        parent_id=None,
        name="step",
        start_timestamp=datetime.now(UTC),
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
    )
    operations = [execution_op, step_op]
    execution = Execution("test-arn", start_input, operations)

    result = execution.get_assertable_operations()

    assert len(result) == 1
    assert result[0] == step_op


def test_has_pending_operations_with_pending_step():
    """Test has_pending_operations returns True for pending STEP operations."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
    )
    operations = [
        Operation(
            operation_id="op1",
            parent_id=None,
            name="test",
            start_timestamp=datetime.now(UTC),
            operation_type=OperationType.STEP,
            status=OperationStatus.PENDING,
        )
    ]
    execution = Execution("test-arn", start_input, operations)

    result = execution.has_pending_operations(execution)

    assert result is True


def test_has_pending_operations_with_started_wait():
    """Test has_pending_operations returns True for started WAIT operations."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
    )
    operations = [
        Operation(
            operation_id="op1",
            parent_id=None,
            name="test",
            start_timestamp=datetime.now(UTC),
            operation_type=OperationType.WAIT,
            status=OperationStatus.STARTED,
        )
    ]
    execution = Execution("test-arn", start_input, operations)

    result = execution.has_pending_operations(execution)

    assert result is True


def test_has_pending_operations_with_started_callback():
    """Test has_pending_operations returns True for started CALLBACK operations."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
    )
    operations = [
        Operation(
            operation_id="op1",
            parent_id=None,
            name="test",
            start_timestamp=datetime.now(UTC),
            operation_type=OperationType.CALLBACK,
            status=OperationStatus.STARTED,
        )
    ]
    execution = Execution("test-arn", start_input, operations)

    result = execution.has_pending_operations(execution)

    assert result is True


def test_has_pending_operations_with_started_invoke():
    """Test has_pending_operations returns True for started INVOKE operations."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
    )
    operations = [
        Operation(
            operation_id="op1",
            parent_id=None,
            name="test",
            start_timestamp=datetime.now(UTC),
            operation_type=OperationType.INVOKE,
            status=OperationStatus.STARTED,
        )
    ]
    execution = Execution("test-arn", start_input, operations)

    result = execution.has_pending_operations(execution)

    assert result is True


def test_has_pending_operations_no_pending():
    """Test has_pending_operations returns False when no pending operations."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
    )
    operations = [
        Operation(
            operation_id="op1",
            parent_id=None,
            name="test",
            start_timestamp=datetime.now(UTC),
            operation_type=OperationType.STEP,
            status=OperationStatus.SUCCEEDED,
        )
    ]
    execution = Execution("test-arn", start_input, operations)

    result = execution.has_pending_operations(execution)

    assert result is False


def test_complete_success_with_string_result():
    """Test complete_success method with string result."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
    )
    execution = Execution("test-arn", start_input, [])

    execution.complete_success("success result")

    assert execution.is_complete is True
    assert execution.result.status == InvocationStatus.SUCCEEDED
    assert execution.result.result == "success result"


def test_complete_success_with_none_result():
    """Test complete_success method with None result."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
    )
    execution = Execution("test-arn", start_input, [])

    execution.complete_success(None)

    assert execution.is_complete is True
    assert execution.result.status == InvocationStatus.SUCCEEDED
    assert execution.result.result is None


def test_complete_fail():
    """Test complete_fail method."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
    )
    execution = Execution("test-arn", start_input, [])
    error = ErrorObject.from_message("Test error message")

    execution.complete_fail(error)

    assert execution.is_complete is True
    assert execution.result.status == InvocationStatus.FAILED
    assert execution.result.error == error


def test_find_operation_exists():
    """Test _find_operation method when operation exists."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
    )
    operation = Operation(
        operation_id="test-op-id",
        parent_id=None,
        name="test",
        start_timestamp=datetime.now(UTC),
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
    )
    execution = Execution("test-arn", start_input, [operation])

    index, found_operation = execution._find_operation("test-op-id")  # noqa: SLF001

    assert index == 0
    assert found_operation == operation


def test_find_operation_not_exists():
    """Test _find_operation method when operation doesn't exist."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
    )
    execution = Execution("test-arn", start_input, [])

    with pytest.raises(
        IllegalStateError, match="Attempting to update state of an Operation"
    ):
        execution._find_operation("non-existent-id")  # noqa: SLF001


@patch("aws_durable_execution_sdk_python_testing.execution.datetime")
def test_complete_wait_success(mock_datetime):
    """Test complete_wait method successful completion."""
    mock_now = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)
    mock_datetime.now.return_value = mock_now

    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
    )
    operation = Operation(
        operation_id="wait-op-id",
        parent_id=None,
        name="test-wait",
        start_timestamp=datetime.now(UTC),
        operation_type=OperationType.WAIT,
        status=OperationStatus.STARTED,
    )
    execution = Execution("test-arn", start_input, [operation])

    result = execution.complete_wait("wait-op-id")

    assert result.status == OperationStatus.SUCCEEDED
    assert result.end_timestamp == mock_now
    assert execution.token_sequence == 1
    assert execution.operations[0] == result


def test_complete_wait_wrong_status():
    """Test complete_wait method with wrong operation status."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
    )
    operation = Operation(
        operation_id="wait-op-id",
        parent_id=None,
        name="test-wait",
        start_timestamp=datetime.now(UTC),
        operation_type=OperationType.WAIT,
        status=OperationStatus.SUCCEEDED,
    )
    execution = Execution("test-arn", start_input, [operation])

    with pytest.raises(
        IllegalStateError, match="Attempting to transition a Wait Operation"
    ):
        execution.complete_wait("wait-op-id")


def test_complete_wait_wrong_type():
    """Test complete_wait method with wrong operation type."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
    )
    operation = Operation(
        operation_id="step-op-id",
        parent_id=None,
        name="test-step",
        start_timestamp=datetime.now(UTC),
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
    )
    execution = Execution("test-arn", start_input, [operation])

    with pytest.raises(IllegalStateError, match="Expected WAIT operation"):
        execution.complete_wait("step-op-id")


def test_complete_retry_success():
    """Test complete_retry method successful completion."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
    )
    step_details = StepDetails(
        next_attempt_timestamp=str(datetime.now(UTC)),
        attempt=1,
    )
    operation = Operation(
        operation_id="step-op-id",
        parent_id=None,
        name="test-step",
        start_timestamp=datetime.now(UTC),
        operation_type=OperationType.STEP,
        status=OperationStatus.PENDING,
        step_details=step_details,
    )
    execution = Execution("test-arn", start_input, [operation])

    result = execution.complete_retry("step-op-id")

    assert result.status == OperationStatus.READY
    assert result.step_details.next_attempt_timestamp is None
    assert execution.token_sequence == 1
    assert execution.operations[0] == result


def test_complete_retry_no_step_details():
    """Test complete_retry method with no step_details."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
    )
    operation = Operation(
        operation_id="step-op-id",
        parent_id=None,
        name="test-step",
        start_timestamp=datetime.now(UTC),
        operation_type=OperationType.STEP,
        status=OperationStatus.PENDING,
    )
    execution = Execution("test-arn", start_input, [operation])

    result = execution.complete_retry("step-op-id")

    assert result.status == OperationStatus.READY
    assert result.step_details is None
    assert execution.token_sequence == 1


def test_complete_retry_wrong_status():
    """Test complete_retry method with wrong operation status."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
    )
    operation = Operation(
        operation_id="step-op-id",
        parent_id=None,
        name="test-step",
        start_timestamp=datetime.now(UTC),
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
    )
    execution = Execution("test-arn", start_input, [operation])

    with pytest.raises(
        IllegalStateError, match="Attempting to transition a Step Operation"
    ):
        execution.complete_retry("step-op-id")


def test_complete_retry_wrong_type():
    """Test complete_retry method with wrong operation type."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
    )
    operation = Operation(
        operation_id="wait-op-id",
        parent_id=None,
        name="test-wait",
        start_timestamp=datetime.now(UTC),
        operation_type=OperationType.WAIT,
        status=OperationStatus.PENDING,
    )
    execution = Execution("test-arn", start_input, [operation])

    with pytest.raises(IllegalStateError, match="Expected STEP operation"):
        execution.complete_retry("wait-op-id")
