"""Unit tests for checkpoint validator."""

import json

import pytest
from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    Operation,
    OperationAction,
    OperationStatus,
    OperationType,
    OperationUpdate,
)

from aws_durable_execution_sdk_python_testing.checkpoint.validators.checkpoint import (
    MAX_ERROR_PAYLOAD_SIZE_BYTES,
    CheckpointValidator,
)
from aws_durable_execution_sdk_python_testing.exceptions import (
    InvalidParameterValueException,
)
from aws_durable_execution_sdk_python_testing.execution import Execution
from aws_durable_execution_sdk_python_testing.model import StartDurableExecutionInput


def _create_test_execution() -> Execution:
    """Create a test execution with basic setup."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=900,
        execution_retention_period_days=7,
        input=json.dumps({"test": "data"}),
        invocation_id="test-invocation-id",
    )
    execution = Execution.new(start_input)
    execution.start()
    return execution


def test_validate_input_empty_updates():
    """Test validation with empty updates list."""
    execution = _create_test_execution()
    CheckpointValidator.validate_input([], execution)


def test_validate_input_single_valid_update():
    """Test validation with single valid update."""
    execution = _create_test_execution()
    updates = [
        OperationUpdate(
            operation_id="test-step-id",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
        )
    ]
    CheckpointValidator.validate_input(updates, execution)


def test_validate_conflicting_execution_update_multiple():
    """Test validation fails with multiple execution updates."""
    execution = _create_test_execution()
    updates = [
        OperationUpdate(
            operation_id="exec-1",
            operation_type=OperationType.EXECUTION,
            action=OperationAction.SUCCEED,
        ),
        OperationUpdate(
            operation_id="exec-2",
            operation_type=OperationType.EXECUTION,
            action=OperationAction.FAIL,
        ),
    ]

    with pytest.raises(
        InvalidParameterValueException,
        match="Cannot checkpoint multiple EXECUTION updates",
    ):
        CheckpointValidator.validate_input(updates, execution)


def test_validate_conflicting_execution_update_not_last():
    """Test validation fails when execution update is not last."""
    execution = _create_test_execution()
    updates = [
        OperationUpdate(
            operation_id="exec-1",
            operation_type=OperationType.EXECUTION,
            action=OperationAction.SUCCEED,
        ),
        OperationUpdate(
            operation_id="step-1",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
        ),
    ]

    with pytest.raises(
        InvalidParameterValueException,
        match="EXECUTION checkpoint must be the last update",
    ):
        CheckpointValidator.validate_input(updates, execution)


def test_validate_execution_update_as_last():
    """Test validation passes when execution update is last."""
    execution = _create_test_execution()
    updates = [
        OperationUpdate(
            operation_id="step-1",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
        ),
        OperationUpdate(
            operation_id="exec-1",
            operation_type=OperationType.EXECUTION,
            action=OperationAction.SUCCEED,
        ),
    ]
    CheckpointValidator.validate_input(updates, execution)


def test_validate_payload_sizes_error_too_large():
    """Test validation fails when error payload is too large."""
    execution = _create_test_execution()

    large_message = "x" * (MAX_ERROR_PAYLOAD_SIZE_BYTES + 1)
    large_error = ErrorObject(
        message=large_message, type="TestError", data=None, stack_trace=None
    )

    updates = [
        OperationUpdate(
            operation_id="step-1",
            operation_type=OperationType.STEP,
            action=OperationAction.FAIL,
            error=large_error,
        )
    ]

    with pytest.raises(
        InvalidParameterValueException,
        match=f"Error object size must be less than {MAX_ERROR_PAYLOAD_SIZE_BYTES} bytes",
    ):
        CheckpointValidator.validate_input(updates, execution)


def test_validate_payload_sizes_error_within_limit():
    """Test validation passes when error payload is within limit."""
    execution = _create_test_execution()

    small_error = ErrorObject(
        message="Small error", type="TestError", data=None, stack_trace=None
    )
    updates = [
        OperationUpdate(
            operation_id="step-1",
            operation_type=OperationType.STEP,
            action=OperationAction.FAIL,
            error=small_error,
        )
    ]
    CheckpointValidator.validate_input(updates, execution)


def test_validate_duplicate_operation_ids():
    """Test validation fails with duplicate operation IDs."""
    execution = _create_test_execution()
    updates = [
        OperationUpdate(
            operation_id="duplicate-id",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
        ),
        OperationUpdate(
            operation_id="duplicate-id",
            operation_type=OperationType.STEP,
            action=OperationAction.SUCCEED,
        ),
    ]

    with pytest.raises(
        InvalidParameterValueException,
        match="Cannot update the same operation twice in a single request",
    ):
        CheckpointValidator.validate_input(updates, execution)


def test_validate_valid_parent_id_in_execution():
    """Test validation passes with valid parent ID from execution."""
    execution = _create_test_execution()

    context_op = Operation(
        operation_id="context-1",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.STARTED,
    )
    execution.operations.append(context_op)

    updates = [
        OperationUpdate(
            operation_id="step-1",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
            parent_id="context-1",
        )
    ]
    CheckpointValidator.validate_input(updates, execution)


def test_validate_valid_parent_id_in_updates():
    """Test validation passes with valid parent ID from updates."""
    execution = _create_test_execution()
    updates = [
        OperationUpdate(
            operation_id="context-1",
            operation_type=OperationType.CONTEXT,
            action=OperationAction.START,
        ),
        OperationUpdate(
            operation_id="step-1",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
            parent_id="context-1",
        ),
    ]
    CheckpointValidator.validate_input(updates, execution)


def test_validate_invalid_parent_id_wrong_type():
    """Test validation fails with parent ID of wrong operation type."""
    execution = _create_test_execution()

    step_op = Operation(
        operation_id="step-parent",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
    )
    execution.operations.append(step_op)

    updates = [
        OperationUpdate(
            operation_id="step-1",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
            parent_id="step-parent",
        )
    ]

    with pytest.raises(
        InvalidParameterValueException, match="Invalid parent operation id"
    ):
        CheckpointValidator.validate_input(updates, execution)


def test_validate_invalid_parent_id_not_found():
    """Test validation fails with parent ID that doesn't exist."""
    execution = _create_test_execution()
    updates = [
        OperationUpdate(
            operation_id="step-1",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
            parent_id="non-existent-parent",
        )
    ]

    with pytest.raises(
        InvalidParameterValueException, match="Invalid parent operation id"
    ):
        CheckpointValidator.validate_input(updates, execution)


def test_validate_no_parent_id():
    """Test validation passes with no parent ID."""
    execution = _create_test_execution()
    updates = [
        OperationUpdate(
            operation_id="step-1",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
            parent_id=None,
        )
    ]
    CheckpointValidator.validate_input(updates, execution)


def test_validate_operation_status_transition_step():
    """Test validation calls step validator for STEP operations."""
    execution = _create_test_execution()

    step_op = Operation(
        operation_id="step-1",
        operation_type=OperationType.STEP,
        status=OperationStatus.READY,
    )
    execution.operations.append(step_op)

    updates = [
        OperationUpdate(
            operation_id="step-1",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
        )
    ]
    CheckpointValidator.validate_input(updates, execution)


def test_validate_operation_status_transition_context():
    """Test validation calls context validator for CONTEXT operations."""
    execution = _create_test_execution()

    context_op = Operation(
        operation_id="context-1",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.STARTED,
    )
    execution.operations.append(context_op)

    updates = [
        OperationUpdate(
            operation_id="context-1",
            operation_type=OperationType.CONTEXT,
            action=OperationAction.SUCCEED,
        )
    ]
    CheckpointValidator.validate_input(updates, execution)


def test_validate_operation_status_transition_wait():
    """Test validation calls wait validator for WAIT operations."""
    execution = _create_test_execution()

    wait_op = Operation(
        operation_id="wait-1",
        operation_type=OperationType.WAIT,
        status=OperationStatus.STARTED,
    )
    execution.operations.append(wait_op)

    updates = [
        OperationUpdate(
            operation_id="wait-1",
            operation_type=OperationType.WAIT,
            action=OperationAction.CANCEL,
        )
    ]
    CheckpointValidator.validate_input(updates, execution)


def test_validate_operation_status_transition_callback():
    """Test validation calls callback validator for CALLBACK operations."""
    execution = _create_test_execution()

    callback_op = Operation(
        operation_id="callback-1",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
    )
    execution.operations.append(callback_op)

    updates = [
        OperationUpdate(
            operation_id="callback-1",
            operation_type=OperationType.CALLBACK,
            action=OperationAction.CANCEL,
        )
    ]
    CheckpointValidator.validate_input(updates, execution)


def test_validate_operation_status_transition_invoke():
    """Test validation calls invoke validator for INVOKE operations."""
    execution = _create_test_execution()

    invoke_op = Operation(
        operation_id="invoke-1",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.STARTED,
    )
    execution.operations.append(invoke_op)

    updates = [
        OperationUpdate(
            operation_id="invoke-1",
            operation_type=OperationType.CHAINED_INVOKE,
            action=OperationAction.CANCEL,
        )
    ]
    CheckpointValidator.validate_input(updates, execution)


def test_validate_operation_status_transition_execution():
    """Test validation calls execution validator for EXECUTION operations."""
    execution = _create_test_execution()
    updates = [
        OperationUpdate(
            operation_id="exec-1",
            operation_type=OperationType.EXECUTION,
            action=OperationAction.SUCCEED,
        )
    ]
    CheckpointValidator.validate_input(updates, execution)
