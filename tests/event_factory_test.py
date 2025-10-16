"""Tests for Event factory methods.

This module tests all the event creation factory methods in the Event class.
"""

from datetime import UTC, datetime

import pytest
from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    OperationStatus,
)

from aws_durable_execution_sdk_python_testing.model import Event


# region execution-tests
def test_create_execution_started():
    event = Event.create_execution_event(
        operation_status=OperationStatus.STARTED,
        event_timestamp="2024-01-01T12:00:00Z",
        event_id=1,
        operation_id="op-1",
        name="test_execution",
        input_payload='{"test": "data"}',
        execution_timeout=300,
        include_execution_data=True,
    )

    assert event.event_type == "ExecutionStarted"
    assert event.operation_id == "op-1"
    assert event.name == "test_execution"
    assert event.execution_started_details.input["Payload"] == '{"test": "data"}'
    assert event.execution_started_details.execution_timeout == 300


def test_create_execution_succeeded():
    event = Event.create_execution_event(
        operation_status=OperationStatus.SUCCEEDED,
        event_timestamp="2024-01-01T12:00:00Z",
        event_id=2,
        operation_id="op-1",
        result_payload='{"result": "success"}',
        include_execution_data=True,
    )

    assert event.event_type == "ExecutionSucceeded"
    assert (
        event.execution_succeeded_details.result["Payload"] == '{"result": "success"}'
    )


def test_create_execution_failed():
    error = ErrorObject.from_message("Execution failed")
    event = Event.create_execution_event(
        operation_status=OperationStatus.FAILED,
        event_timestamp="2024-01-01T12:00:00Z",
        event_id=3,
        operation_id="op-1",
        error=error,
    )

    assert event.event_type == "ExecutionFailed"
    assert (
        event.execution_failed_details.error["Payload"]["ErrorMessage"]
        == "Execution failed"
    )


def test_create_execution_timed_out():
    error = ErrorObject.from_message("Execution timed out")
    event = Event.create_execution_event(
        operation_status=OperationStatus.TIMED_OUT,
        event_timestamp="2024-01-01T12:00:00Z",
        event_id=4,
        operation_id="op-1",
        error=error,
    )

    assert event.event_type == "ExecutionTimedOut"
    assert (
        event.execution_timed_out_details.error["Payload"]["ErrorMessage"]
        == "Execution timed out"
    )


def test_create_execution_stopped():
    error = ErrorObject.from_message("Execution stopped")
    event = Event.create_execution_event(
        operation_status=OperationStatus.STOPPED,
        event_timestamp="2024-01-01T12:00:00Z",
        event_id=5,
        operation_id="op-1",
        error=error,
    )

    assert event.event_type == "ExecutionStopped"
    assert (
        event.execution_stopped_details.error["Payload"]["ErrorMessage"]
        == "Execution stopped"
    )


def test_create_execution_invalid_status():
    with pytest.raises(
        ValueError, match="Operation status .* is not valid for execution operations"
    ):
        Event.create_execution_event(
            operation_status=OperationStatus.CANCELLED,
            event_timestamp="2024-01-01T12:00:00Z",
            event_id=1,
            operation_id="op-1",
        )


# endregion execution-tests


# region context-tests
def test_create_context_started():
    event = Event.create_context_event(
        operation_status=OperationStatus.STARTED,
        event_timestamp="2024-01-01T12:00:00Z",
        event_id=1,
        operation_id="ctx-1",
        name="test_context",
    )

    assert event.event_type == "ContextStarted"
    assert event.operation_id == "ctx-1"
    assert event.name == "test_context"
    assert event.context_started_details is not None


def test_create_context_succeeded():
    event = Event.create_context_event(
        operation_status=OperationStatus.SUCCEEDED,
        event_timestamp="2024-01-01T12:00:00Z",
        event_id=2,
        operation_id="ctx-1",
        result_payload='{"context": "result"}',
        include_execution_data=True,
    )

    assert event.event_type == "ContextSucceeded"
    assert event.context_succeeded_details.result["Payload"] == '{"context": "result"}'


def test_create_context_failed():
    error = ErrorObject.from_message("Context failed")
    event = Event.create_context_event(
        operation_status=OperationStatus.FAILED,
        event_timestamp="2024-01-01T12:00:00Z",
        event_id=3,
        operation_id="ctx-1",
        error=error,
    )

    assert event.event_type == "ContextFailed"
    assert (
        event.context_failed_details.error["Payload"]["ErrorMessage"]
        == "Context failed"
    )


def test_create_context_invalid_status():
    with pytest.raises(
        ValueError, match="Operation status .* is not valid for context operations"
    ):
        Event.create_context_event(
            operation_status=OperationStatus.TIMED_OUT,
            event_timestamp="2024-01-01T12:00:00Z",
            event_id=1,
            operation_id="ctx-1",
        )


# endregion context-tests


# region wait-tests
def test_create_wait_started():
    event = Event.create_wait_event(
        operation_status=OperationStatus.STARTED,
        event_id=1,
        event_timestamp="2024-01-01T12:00:00Z",
        operation_id="wait-1",
        duration=300,
        scheduled_end_timestamp="2024-01-01T12:05:00Z",
    )

    assert event.event_type == "WaitStarted"
    assert event.wait_started_details.duration == 300
    assert event.wait_started_details.scheduled_end_timestamp == "2024-01-01T12:05:00Z"


def test_create_wait_succeeded():
    event = Event.create_wait_event(
        operation_status=OperationStatus.SUCCEEDED,
        event_id=2,
        event_timestamp="2024-01-01T12:05:00Z",
        operation_id="wait-1",
        duration=300,
    )

    assert event.event_type == "WaitSucceeded"
    assert event.wait_succeeded_details.duration == 300


def test_create_wait_cancelled():
    event = Event.create_wait_event(
        operation_status=OperationStatus.CANCELLED,
        event_id=3,
        event_timestamp="2024-01-01T12:03:00Z",
        operation_id="wait-1",
    )

    assert event.event_type == "WaitCancelled"
    assert event.wait_cancelled_details is not None


def test_create_wait_invalid_status():
    with pytest.raises(
        ValueError, match="Operation status .* is not valid for wait operations"
    ):
        Event.create_wait_event(
            operation_status=OperationStatus.FAILED,
            event_id=1,
            event_timestamp="2024-01-01T12:00:00Z",
            operation_id="wait-1",
        )


# endregion wait-tests


# region step-tests
def test_create_step_started():
    event = Event.create_step_event(
        operation_status=OperationStatus.STARTED,
        event_timestamp="2024-01-01T12:00:00Z",
        event_id=1,
        operation_id="step-1",
        name="test_step",
    )

    assert event.event_type == "StepStarted"
    assert event.operation_id == "step-1"
    assert event.name == "test_step"
    assert event.step_started_details is not None


def test_create_step_succeeded():
    event = Event.create_step_event(
        operation_status=OperationStatus.SUCCEEDED,
        event_timestamp="2024-01-01T12:00:00Z",
        event_id=2,
        operation_id="step-1",
        result_payload='{"step": "result"}',
        include_execution_data=True,
    )

    assert event.event_type == "StepSucceeded"
    assert event.step_succeeded_details.result["Payload"] == '{"step": "result"}'


def test_create_step_failed():
    error = ErrorObject.from_message("Step failed")
    event = Event.create_step_event(
        operation_status=OperationStatus.FAILED,
        event_timestamp="2024-01-01T12:00:00Z",
        event_id=3,
        operation_id="step-1",
        error=error,
    )

    assert event.event_type == "StepFailed"
    assert event.step_failed_details.error["Payload"]["ErrorMessage"] == "Step failed"


def test_create_step_invalid_status():
    with pytest.raises(
        ValueError, match="Operation status .* is not valid for step operations"
    ):
        Event.create_step_event(
            operation_status=OperationStatus.TIMED_OUT,
            event_timestamp="2024-01-01T12:00:00Z",
            event_id=1,
            operation_id="step-1",
        )


# endregion step-tests


# region chained_invoke
def test_create_chained_invoke_started():
    event = Event.create_chained_invoke_event(
        operation_status=OperationStatus.STARTED,
        event_timestamp="2024-01-01T12:00:00Z",
        event_id=1,
        operation_id="invoke-1",
        name="test_invoke",
    )

    assert event.event_type == "ChainedInvokeStarted"
    assert event.operation_id == "invoke-1"
    assert event.name == "test_invoke"
    assert event.chained_invoke_started_details is not None


def test_create_chained_invoke_succeeded():
    event = Event.create_chained_invoke_event(
        operation_status=OperationStatus.SUCCEEDED,
        event_timestamp="2024-01-01T12:00:00Z",
        event_id=2,
        operation_id="invoke-1",
        result_payload='{"invoke": "result"}',
        include_execution_data=True,
    )

    assert event.event_type == "ChainedInvokeSucceeded"
    assert (
        event.chained_invoke_succeeded_details.result["Payload"]
        == '{"invoke": "result"}'
    )


def test_create_chained_invoke_failed():
    error = ErrorObject.from_message("Invoke failed")
    event = Event.create_chained_invoke_event(
        operation_status=OperationStatus.FAILED,
        event_timestamp="2024-01-01T12:00:00Z",
        event_id=3,
        operation_id="invoke-1",
        error=error,
    )

    assert event.event_type == "ChainedInvokeFailed"
    assert (
        event.chained_invoke_failed_details.error["Payload"]["ErrorMessage"]
        == "Invoke failed"
    )


def test_create_chained_invoke_timed_out():
    error = ErrorObject.from_message("Invoke timed out")
    event = Event.create_chained_invoke_event(
        operation_status=OperationStatus.TIMED_OUT,
        event_timestamp="2024-01-01T12:00:00Z",
        event_id=4,
        operation_id="invoke-1",
        error=error,
    )

    assert event.event_type == "ChainedInvokeTimedOut"
    assert (
        event.chained_invoke_timed_out_details.error["Payload"]["ErrorMessage"]
        == "Invoke timed out"
    )


def test_create_chained_invoke_stopped():
    error = ErrorObject.from_message("Invoke stopped")
    event = Event.create_chained_invoke_event(
        operation_status=OperationStatus.STOPPED,
        event_timestamp="2024-01-01T12:00:00Z",
        event_id=5,
        operation_id="invoke-1",
        error=error,
    )

    assert event.event_type == "ChainedInvokeStopped"
    assert (
        event.chained_invoke_stopped_details.error["Payload"]["ErrorMessage"]
        == "Invoke stopped"
    )


def test_create_chained_invoke_invalid_status():
    with pytest.raises(
        ValueError,
        match="Operation status .* is not valid for chained invoke operations",
    ):
        Event.create_chained_invoke_event(
            operation_status=OperationStatus.CANCELLED,
            event_timestamp="2024-01-01T12:00:00Z",
            event_id=1,
            operation_id="invoke-1",
        )


# endregion chained_invoke


# region callback-tests
def test_create_callback_started():
    event = Event.create_callback_event(
        operation_status=OperationStatus.STARTED,
        event_timestamp="2024-01-01T12:00:00Z",
        event_id=1,
        operation_id="callback-1",
        name="test_callback",
        callback_id="cb-123",
    )

    assert event.event_type == "CallbackStarted"
    assert event.operation_id == "callback-1"
    assert event.name == "test_callback"
    assert event.callback_started_details.callback_id == "cb-123"


def test_create_callback_succeeded():
    event = Event.create_callback_event(
        operation_status=OperationStatus.SUCCEEDED,
        event_timestamp="2024-01-01T12:00:00Z",
        event_id=2,
        operation_id="callback-1",
        result_payload='{"callback": "result"}',
        include_execution_data=True,
    )

    assert event.event_type == "CallbackSucceeded"
    assert (
        event.callback_succeeded_details.result["Payload"] == '{"callback": "result"}'
    )


def test_create_callback_failed():
    error = ErrorObject.from_message("Callback failed")
    event = Event.create_callback_event(
        operation_status=OperationStatus.FAILED,
        event_timestamp="2024-01-01T12:00:00Z",
        event_id=3,
        operation_id="callback-1",
        error=error,
    )

    assert event.event_type == "CallbackFailed"
    assert (
        event.callback_failed_details.error["Payload"]["ErrorMessage"]
        == "Callback failed"
    )


def test_create_callback_timed_out():
    error = ErrorObject.from_message("Callback timed out")
    event = Event.create_callback_event(
        operation_status=OperationStatus.TIMED_OUT,
        event_timestamp="2024-01-01T12:00:00Z",
        event_id=4,
        operation_id="callback-1",
        error=error,
    )

    assert event.event_type == "CallbackTimedOut"
    assert (
        event.callback_timed_out_details.error["Payload"]["ErrorMessage"]
        == "Callback timed out"
    )


def test_create_callback_invalid_status():
    with pytest.raises(
        ValueError, match="Operation status .* is not valid for callback operations"
    ):
        Event.create_callback_event(
            operation_status=OperationStatus.STOPPED,
            event_timestamp="2024-01-01T12:00:00Z",
            event_id=1,
            operation_id="callback-1",
        )


# endregion callback-tests


# region from_operation
def test_from_operation_started_execution():
    from unittest.mock import Mock

    from aws_durable_execution_sdk_python.lambda_service import (
        ExecutionDetails,
        OperationType,
    )

    operation = Mock()
    operation.operation_id = "op-1"
    operation.operation_type = OperationType.EXECUTION
    operation.name = "test"
    operation.parent_id = None
    operation.start_timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
    operation.execution_details = ExecutionDetails(input_payload='{"test": "data"}')

    execution = Mock()
    execution.start_input.execution_timeout_seconds = 300

    event = Event.from_operation_started(operation, 1, execution, True)

    assert event.event_type == "ExecutionStarted"
    assert event.operation_id == "op-1"
    assert event.execution_started_details.input["Payload"] == '{"test": "data"}'
    assert event.execution_started_details.execution_timeout == 300


def test_from_operation_finished_execution_succeeded():
    from unittest.mock import Mock

    from aws_durable_execution_sdk_python.lambda_service import OperationType

    operation = Mock()
    operation.operation_id = "op-1"
    operation.operation_type = OperationType.EXECUTION
    operation.status = OperationStatus.SUCCEEDED
    operation.name = "test"
    operation.parent_id = None
    operation.end_timestamp = datetime(2024, 1, 1, 12, 5, 0, tzinfo=UTC)

    execution = Mock()
    execution.result = "success_result"

    event = Event.from_operation_finished(operation, 2, execution, True)

    assert event.event_type == "ExecutionSucceeded"
    assert event.operation_id == "op-1"
    assert event.execution_succeeded_details.result["Payload"] == "success_result"


def test_from_operation_started_no_timestamp():
    from unittest.mock import Mock

    operation = Mock()
    operation.start_timestamp = None

    with pytest.raises(ValueError, match="Operation start timestamp cannot be None"):
        Event.from_operation_started(operation, 1)


def test_from_operation_finished_no_timestamp():
    from unittest.mock import Mock

    operation = Mock()
    operation.end_timestamp = None

    with pytest.raises(ValueError, match="Operation end timestamp cannot be None"):
        Event.from_operation_finished(operation, 1)


def test_from_operation_finished_invalid_status():
    from unittest.mock import Mock

    from aws_durable_execution_sdk_python.lambda_service import OperationType

    operation = Mock()
    operation.operation_type = OperationType.EXECUTION
    operation.status = OperationStatus.STARTED
    operation.end_timestamp = datetime(2024, 1, 1, 12, 5, 0, tzinfo=UTC)

    with pytest.raises(ValueError, match="Operation status must be one of"):
        Event.from_operation_finished(operation, 1)


def test_from_operation_started_all_types():
    from unittest.mock import Mock

    from aws_durable_execution_sdk_python.lambda_service import (
        OperationType,
        WaitDetails,
    )

    # Test STEP
    operation = Mock()
    operation.operation_id = "step-1"
    operation.operation_type = OperationType.STEP
    operation.name = "test_step"
    operation.parent_id = None
    operation.start_timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

    event = Event.from_operation_started(operation, 1)
    assert event.event_type == "StepStarted"

    # Test WAIT with options and details
    operation = Mock()
    operation.operation_id = "wait-1"
    operation.operation_type = OperationType.WAIT
    operation.name = "test_wait"
    operation.parent_id = None
    operation.start_timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
    operation.wait_options = {"wait_seconds": 300}
    operation.wait_details = WaitDetails(
        scheduled_timestamp=datetime(2024, 1, 1, 12, 5, 0, tzinfo=UTC)
    )

    event = Event.from_operation_started(operation, 1)
    assert event.event_type == "WaitStarted"

    # Test CALLBACK with details
    operation = Mock()
    operation.operation_id = "cb-1"
    operation.operation_type = OperationType.CALLBACK
    operation.name = "test_callback"
    operation.parent_id = None
    operation.start_timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
    callback_details = Mock()
    callback_details.callback_id = "cb-123"
    operation.callback_details = callback_details

    event = Event.from_operation_started(operation, 1)
    assert event.event_type == "CallbackStarted"

    # Test CHAINED_INVOKE
    operation = Mock()
    operation.operation_id = "invoke-1"
    operation.operation_type = OperationType.CHAINED_INVOKE
    operation.name = "test_invoke"
    operation.parent_id = None
    operation.start_timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

    event = Event.from_operation_started(operation, 1)
    assert event.event_type == "ChainedInvokeStarted"

    # Test CONTEXT
    operation = Mock()
    operation.operation_id = "ctx-1"
    operation.operation_type = OperationType.CONTEXT
    operation.name = "test_context"
    operation.parent_id = None
    operation.start_timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

    event = Event.from_operation_started(operation, 1)
    assert event.event_type == "ContextStarted"


def test_from_operation_finished_all_types():
    from unittest.mock import Mock

    from aws_durable_execution_sdk_python.lambda_service import OperationType

    # Test WAIT
    operation = Mock()
    operation.operation_id = "wait-1"
    operation.operation_type = OperationType.WAIT
    operation.status = OperationStatus.SUCCEEDED
    operation.name = "test_wait"
    operation.parent_id = None
    operation.end_timestamp = datetime(2024, 1, 1, 12, 5, 0, tzinfo=UTC)
    operation.wait_options = {"wait_seconds": 300}
    operation.wait_details = None

    event = Event.from_operation_finished(operation, 1)
    assert event.event_type == "WaitSucceeded"

    # Test STEP with details
    operation = Mock()
    operation.operation_id = "step-1"
    operation.operation_type = OperationType.STEP
    operation.status = OperationStatus.SUCCEEDED
    operation.name = "test_step"
    operation.parent_id = None
    operation.end_timestamp = datetime(2024, 1, 1, 12, 5, 0, tzinfo=UTC)
    step_details = Mock()
    step_details.result = "step_result"
    step_details.error = None
    operation.step_details = step_details

    event = Event.from_operation_finished(operation, 1, include_execution_data=True)
    assert event.event_type == "StepSucceeded"

    # Test CALLBACK with details
    operation = Mock()
    operation.operation_id = "cb-1"
    operation.operation_type = OperationType.CALLBACK
    operation.status = OperationStatus.SUCCEEDED
    operation.name = "test_callback"
    operation.parent_id = None
    operation.end_timestamp = datetime(2024, 1, 1, 12, 5, 0, tzinfo=UTC)
    callback_details = Mock()
    callback_details.result = "callback_result"
    callback_details.error = None
    operation.callback_details = callback_details

    event = Event.from_operation_finished(operation, 1, include_execution_data=True)
    assert event.event_type == "CallbackSucceeded"

    # Test CHAINED_INVOKE with details
    operation = Mock()
    operation.operation_id = "invoke-1"
    operation.operation_type = OperationType.CHAINED_INVOKE
    operation.status = OperationStatus.SUCCEEDED
    operation.name = "test_invoke"
    operation.parent_id = None
    operation.end_timestamp = datetime(2024, 1, 1, 12, 5, 0, tzinfo=UTC)
    chained_invoke_details = Mock()
    chained_invoke_details.result = "invoke_result"
    chained_invoke_details.error = None
    operation.chained_invoke_details = chained_invoke_details

    event = Event.from_operation_finished(operation, 1, include_execution_data=True)
    assert event.event_type == "ChainedInvokeSucceeded"

    # Test CONTEXT
    operation = Mock()
    operation.operation_id = "ctx-1"
    operation.operation_type = OperationType.CONTEXT
    operation.status = OperationStatus.SUCCEEDED
    operation.name = "test_context"
    operation.parent_id = None
    operation.end_timestamp = datetime(2024, 1, 1, 12, 5, 0, tzinfo=UTC)
    operation.result = "context_result"
    operation.error = None

    event = Event.from_operation_finished(operation, 1, include_execution_data=True)
    assert event.event_type == "ContextSucceeded"


def test_from_operation_unknown_type():
    from unittest.mock import Mock

    operation = Mock()
    operation.operation_type = "UNKNOWN"
    operation.start_timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

    with pytest.raises(ValueError, match="Unknown operation type"):
        Event.from_operation_started(operation, 1)

    operation.end_timestamp = datetime(2024, 1, 1, 12, 5, 0, tzinfo=UTC)
    operation.status = OperationStatus.SUCCEEDED

    with pytest.raises(ValueError, match="Unknown operation type"):
        Event.from_operation_finished(operation, 1)


# endregion from_operation


# region helpers-test
def test_iso_with_timezone():
    ts = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
    result = Event._iso(ts)  # noqa: SLF001
    assert result == "2024-01-01T12:00:00+00:00"


def test_iso_without_timezone():
    ts = datetime(2024, 1, 1, 12, 0, 0)  # noqa: DTZ001
    result = Event._iso(ts)  # noqa: SLF001
    assert result == "2024-01-01T12:00:00+00:00"


def test_iso_none_with_default():
    result = Event._iso(None, default_now=True)  # noqa: SLF001
    assert isinstance(result, str)
    assert "T" in result


def test_iso_none_without_default():
    with pytest.raises(ValueError, match="Timestamp is required"):
        Event._iso(None)  # noqa: SLF001


def test_payload_envelope_with_data():
    result = Event._payload_envelope("test_payload", True)  # noqa: SLF001
    assert result == {"Payload": "test_payload", "Truncated": False}


def test_payload_envelope_without_data():
    result = Event._payload_envelope("test_payload", False)  # noqa: SLF001
    assert result == {"Payload": None, "Truncated": False}


def test_payload_envelope_none():
    result = Event._payload_envelope(None, True)  # noqa: SLF001
    assert result is None


def test_error_envelope_with_error_object():
    error = ErrorObject.from_message("test error")
    result = Event._error_envelope(error)  # noqa: SLF001
    assert result == {"Payload": error.to_dict(), "Truncated": False}


def test_error_envelope_with_dict():
    error = {"message": "test error"}
    result = Event._error_envelope(error)  # noqa: SLF001
    assert result == {"Payload": error, "Truncated": False}


def test_error_envelope_none():
    result = Event._error_envelope(None)  # noqa: SLF001
    assert result is None


def test_wait_seconds_from_timestamps():
    from datetime import datetime
    from unittest.mock import Mock

    op = Mock()
    op.start_timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

    wait_details = Mock()
    wait_details.scheduled_timestamp = datetime(
        2024, 1, 1, 12, 5, 0, tzinfo=UTC
    )  # 5 minutes later
    op.wait_details = wait_details

    result = Event._wait_seconds_from(op)  # noqa: SLF001
    assert result == 300  # 5 minutes = 300 seconds


def test_wait_seconds_from_no_details():
    from unittest.mock import Mock

    op = Mock()
    op.wait_details = None

    result = Event._wait_seconds_from(op)  # noqa: SLF001
    assert result is None


def test_wait_seconds_from_no_timestamps():
    from unittest.mock import Mock

    op = Mock()
    op.start_timestamp = None
    wait_details = Mock()
    wait_details.scheduled_timestamp = None
    op.wait_details = wait_details

    result = Event._wait_seconds_from(op)  # noqa: SLF001
    assert result is None


def test_wait_seconds_from_none():
    from unittest.mock import Mock

    op = Mock()
    op.wait_options = None
    op.wait_details = None

    result = Event._wait_seconds_from(op)  # noqa: SLF001
    assert result is None


def test_scheduled_end_iso_with_timestamp():
    from unittest.mock import Mock

    op = Mock()
    wait_details = Mock()
    wait_details.scheduled_timestamp = datetime(2024, 1, 1, 12, 5, 0, tzinfo=UTC)
    op.wait_details = wait_details

    result = Event._scheduled_end_iso(op)  # noqa: SLF001
    assert result == "2024-01-01T12:05:00+00:00"


def test_scheduled_end_iso_none():
    from unittest.mock import Mock

    op = Mock()
    op.wait_details = None

    result = Event._scheduled_end_iso(op)  # noqa: SLF001
    assert result is None


# endregion helpers-test
