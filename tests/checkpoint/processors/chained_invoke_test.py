"""Tests for ChainedInvoke operation processor."""

import pytest
from aws_durable_execution_sdk_python.lambda_service import (
    ChainedInvokeOptions,
    ErrorObject,
    Operation,
    OperationAction,
    OperationStatus,
    OperationType,
    OperationUpdate,
)

from aws_durable_execution_sdk_python_testing.checkpoint.processors.chained_invoke import (
    ChainedInvokeProcessor,
)
from aws_durable_execution_sdk_python_testing.exceptions import (
    InvalidParameterValueException,
)
from aws_durable_execution_sdk_python_testing.observer import ExecutionNotifier


class MockNotifier(ExecutionNotifier):
    """Mock notifier for testing."""

    def __init__(self):
        super().__init__()
        self.chained_invoke_started_calls = []

    def notify_chained_invoke_started(
        self,
        execution_arn: str,
        operation_id: str,
        function_name: str,
        payload: str | None,
    ) -> None:
        self.chained_invoke_started_calls.append(
            (execution_arn, operation_id, function_name, payload)
        )


# Property-based tests for chain-invokes feature


@pytest.mark.parametrize(
    "operation_id,function_name,payload,execution_arn",
    [
        # Basic case with payload
        (
            "op-1",
            "child-function",
            '{"key": "value"}',
            "arn:aws:lambda:us-east-1:123456789012:execution:test",
        ),
        # No payload
        (
            "op-2",
            "handler-fn",
            None,
            "arn:aws:lambda:us-west-2:987654321098:execution:parent",
        ),
        # Empty payload
        ("op-3", "my-function", "", "test-arn"),
        # Complex payload
        ("op-complex", "complex-fn", '{"nested": {"array": [1, 2, 3]}}', "complex-arn"),
        # Long function name
        (
            "op-long",
            "very-long-function-name-with-many-characters",
            '{"data": 123}',
            "long-arn",
        ),
    ],
)
def test_property_start_creates_pending_operation(
    operation_id: str,
    function_name: str,
    payload: str | None,
    execution_arn: str,
):
    """
    **Feature: chain-invokes, Property 6: ChainedInvokeProcessor START Creates PENDING Operation**

    *For any* CHAINED_INVOKE operation update with action START, processing should create
    an Operation with status PENDING and notify observers with the function name and payload.

    **Validates: Requirements 5.1, 5.4, 9.1**
    """
    # Arrange
    processor = ChainedInvokeProcessor()
    notifier = MockNotifier()

    chained_invoke_options = ChainedInvokeOptions(function_name=function_name)

    update = OperationUpdate(
        operation_id=operation_id,
        operation_type=OperationType.CHAINED_INVOKE,
        action=OperationAction.START,
        name="test-invoke",
        payload=payload,
        chained_invoke_options=chained_invoke_options,
    )

    # Act
    result = processor.process(update, None, notifier, execution_arn)

    # Assert: Operation has status PENDING
    assert isinstance(result, Operation)
    assert result.operation_id == operation_id
    assert result.operation_type == OperationType.CHAINED_INVOKE
    assert result.status == OperationStatus.PENDING
    assert result.chained_invoke_details is not None

    # Assert: Observer was notified with correct parameters
    assert len(notifier.chained_invoke_started_calls) == 1
    call = notifier.chained_invoke_started_calls[0]
    assert call[0] == execution_arn
    assert call[1] == operation_id
    assert call[2] == function_name
    assert call[3] == payload


@pytest.mark.parametrize(
    "operation_id,result_payload,execution_arn",
    [
        # Basic case with result
        (
            "op-1",
            '{"result": "success"}',
            "arn:aws:lambda:us-east-1:123456789012:execution:test",
        ),
        # No result
        ("op-2", None, "arn:aws:lambda:us-west-2:987654321098:execution:parent"),
        # Empty result
        ("op-3", "", "test-arn"),
        # Complex result
        ("op-complex", '{"data": {"items": [1, 2, 3], "status": "ok"}}', "complex-arn"),
    ],
)
def test_property_succeed_updates_to_succeeded(
    operation_id: str,
    result_payload: str | None,
    execution_arn: str,
):
    """
    **Feature: chain-invokes, Property 7: ChainedInvokeProcessor SUCCEED Updates to SUCCEEDED**

    *For any* CHAINED_INVOKE operation update with action SUCCEED, processing should update
    the Operation status to SUCCEEDED and store the result payload.

    **Validates: Requirements 5.2**
    """
    # Arrange
    processor = ChainedInvokeProcessor()
    notifier = MockNotifier()

    update = OperationUpdate(
        operation_id=operation_id,
        operation_type=OperationType.CHAINED_INVOKE,
        action=OperationAction.SUCCEED,
        name="test-invoke",
        payload=result_payload,
    )

    # Act
    result = processor.process(update, None, notifier, execution_arn)

    # Assert: Operation has status SUCCEEDED
    assert isinstance(result, Operation)
    assert result.operation_id == operation_id
    assert result.operation_type == OperationType.CHAINED_INVOKE
    assert result.status == OperationStatus.SUCCEEDED
    assert result.end_timestamp is not None

    # Assert: Result is stored in chained_invoke_details
    assert result.chained_invoke_details is not None
    assert result.chained_invoke_details.result == result_payload
    assert result.chained_invoke_details.error is None


@pytest.mark.parametrize(
    "operation_id,error_type,error_message,execution_arn",
    [
        # Basic error
        (
            "op-1",
            "RuntimeError",
            "Something went wrong",
            "arn:aws:lambda:us-east-1:123456789012:execution:test",
        ),
        # Timeout error
        (
            "op-2",
            "TimeoutError",
            "Function timed out",
            "arn:aws:lambda:us-west-2:987654321098:execution:parent",
        ),
        # Resource not found
        ("op-3", "ResourceNotFoundException", "Handler not found", "test-arn"),
        # Generic error
        ("op-generic", "Error", "Generic error message", "generic-arn"),
    ],
)
def test_property_fail_updates_to_failed(
    operation_id: str,
    error_type: str,
    error_message: str,
    execution_arn: str,
):
    """
    **Feature: chain-invokes, Property 8: ChainedInvokeProcessor FAIL Updates to FAILED**

    *For any* CHAINED_INVOKE operation update with action FAIL, processing should update
    the Operation status to FAILED and store the error.

    **Validates: Requirements 5.3**
    """
    # Arrange
    processor = ChainedInvokeProcessor()
    notifier = MockNotifier()

    error = ErrorObject(error_type, error_message, None, None)

    update = OperationUpdate(
        operation_id=operation_id,
        operation_type=OperationType.CHAINED_INVOKE,
        action=OperationAction.FAIL,
        name="test-invoke",
        error=error,
    )

    # Act
    result = processor.process(update, None, notifier, execution_arn)

    # Assert: Operation has status FAILED
    assert isinstance(result, Operation)
    assert result.operation_id == operation_id
    assert result.operation_type == OperationType.CHAINED_INVOKE
    assert result.status == OperationStatus.FAILED
    assert result.end_timestamp is not None

    # Assert: Error is stored in chained_invoke_details
    assert result.chained_invoke_details is not None
    assert result.chained_invoke_details.result is None
    assert result.chained_invoke_details.error == error


# Unit tests for edge cases and error conditions


def test_process_invalid_action():
    """Test that invalid actions raise InvalidParameterValueException."""
    processor = ChainedInvokeProcessor()
    notifier = MockNotifier()

    update = OperationUpdate(
        operation_id="op-123",
        operation_type=OperationType.CHAINED_INVOKE,
        action=OperationAction.RETRY,
        name="test-invoke",
    )

    with pytest.raises(
        InvalidParameterValueException, match="Invalid action for CHAINED_INVOKE"
    ):
        processor.process(update, None, notifier, "test-arn")


def test_process_cancel_action_invalid():
    """Test that CANCEL action raises InvalidParameterValueException."""
    processor = ChainedInvokeProcessor()
    notifier = MockNotifier()

    update = OperationUpdate(
        operation_id="op-123",
        operation_type=OperationType.CHAINED_INVOKE,
        action=OperationAction.CANCEL,
        name="test-invoke",
    )

    with pytest.raises(
        InvalidParameterValueException, match="Invalid action for CHAINED_INVOKE"
    ):
        processor.process(update, None, notifier, "test-arn")


def test_start_without_chained_invoke_options():
    """Test START action without chained_invoke_options uses empty function name."""
    processor = ChainedInvokeProcessor()
    notifier = MockNotifier()

    update = OperationUpdate(
        operation_id="op-123",
        operation_type=OperationType.CHAINED_INVOKE,
        action=OperationAction.START,
        name="test-invoke",
        payload='{"test": true}',
    )

    result = processor.process(update, None, notifier, "test-arn")

    assert result.status == OperationStatus.PENDING
    assert len(notifier.chained_invoke_started_calls) == 1
    # Function name should be empty string when not provided
    assert notifier.chained_invoke_started_calls[0][2] == ""


def test_succeed_with_current_operation():
    """Test SUCCEED action preserves start_timestamp from current operation."""
    processor = ChainedInvokeProcessor()
    notifier = MockNotifier()

    import datetime

    current_op = Operation(
        operation_id="op-123",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.PENDING,
        start_timestamp=datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.UTC),
        name="test-invoke",
        parent_id="parent-op",
        sub_type="invoke",
    )

    update = OperationUpdate(
        operation_id="op-123",
        operation_type=OperationType.CHAINED_INVOKE,
        action=OperationAction.SUCCEED,
        payload='{"result": "done"}',
    )

    result = processor.process(update, current_op, notifier, "test-arn")

    assert result.status == OperationStatus.SUCCEEDED
    assert result.start_timestamp == current_op.start_timestamp
    assert result.parent_id == "parent-op"
    assert result.sub_type == "invoke"


def test_fail_with_current_operation():
    """Test FAIL action preserves start_timestamp from current operation."""
    processor = ChainedInvokeProcessor()
    notifier = MockNotifier()

    import datetime

    current_op = Operation(
        operation_id="op-123",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.PENDING,
        start_timestamp=datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.UTC),
        name="test-invoke",
        parent_id="parent-op",
    )

    error = ErrorObject("TestError", "Test error", None, None)

    update = OperationUpdate(
        operation_id="op-123",
        operation_type=OperationType.CHAINED_INVOKE,
        action=OperationAction.FAIL,
        error=error,
    )

    result = processor.process(update, current_op, notifier, "test-arn")

    assert result.status == OperationStatus.FAILED
    assert result.start_timestamp == current_op.start_timestamp
    assert result.parent_id == "parent-op"
