"""Model classes for the web API."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# Import existing types from the main SDK - REUSE EVERYTHING POSSIBLE
from aws_durable_execution_sdk_python.lambda_service import (
    CallbackOptions,
    ContextOptions,
    ErrorObject,
    InvokeOptions,
    Operation,
    OperationAction,
    OperationSubType,
    OperationType,
    OperationUpdate,
    StepOptions,
    WaitOptions,
)

from aws_durable_execution_sdk_python_testing.exceptions import (
    InvalidParameterValueException,
)


# Web API specific models (not in Smithy but needed for web interface)
@dataclass(frozen=True)
class StartDurableExecutionInput:
    """Input for starting a durable execution via web API."""

    account_id: str
    function_name: str
    function_qualifier: str
    execution_name: str
    execution_timeout_seconds: int
    execution_retention_period_days: int
    invocation_id: str | None = None
    trace_fields: dict | None = None
    tenant_id: str | None = None
    input: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> StartDurableExecutionInput:
        # Validate required fields and raise AWS-compliant exceptions
        required_fields = [
            "AccountId",
            "FunctionName",
            "FunctionQualifier",
            "ExecutionName",
            "ExecutionTimeoutSeconds",
            "ExecutionRetentionPeriodDays",
        ]

        for field in required_fields:
            if field not in data:
                msg: str = f"Missing required field: {field}"
                raise InvalidParameterValueException(msg)

        return cls(
            account_id=data["AccountId"],
            function_name=data["FunctionName"],
            function_qualifier=data["FunctionQualifier"],
            execution_name=data["ExecutionName"],
            execution_timeout_seconds=data["ExecutionTimeoutSeconds"],
            execution_retention_period_days=data["ExecutionRetentionPeriodDays"],
            invocation_id=data.get("InvocationId"),
            trace_fields=data.get("TraceFields"),
            tenant_id=data.get("TenantId"),
            input=data.get("Input"),
        )

    def to_dict(self) -> dict[str, Any]:
        result = {
            "AccountId": self.account_id,
            "FunctionName": self.function_name,
            "FunctionQualifier": self.function_qualifier,
            "ExecutionName": self.execution_name,
            "ExecutionTimeoutSeconds": self.execution_timeout_seconds,
            "ExecutionRetentionPeriodDays": self.execution_retention_period_days,
        }
        if self.invocation_id is not None:
            result["InvocationId"] = self.invocation_id
        if self.trace_fields is not None:
            result["TraceFields"] = self.trace_fields
        if self.tenant_id is not None:
            result["TenantId"] = self.tenant_id
        if self.input is not None:
            result["Input"] = self.input
        return result


@dataclass(frozen=True)
class StartDurableExecutionOutput:
    """Output from starting a durable execution via web API."""

    execution_arn: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> StartDurableExecutionOutput:
        return cls(execution_arn=data.get("ExecutionArn"))

    def to_dict(self) -> dict[str, Any]:
        result = {}
        if self.execution_arn is not None:
            result["ExecutionArn"] = self.execution_arn
        return result


# Smithy-based API models
@dataclass(frozen=True)
class GetDurableExecutionRequest:
    """Request to get durable execution details."""

    durable_execution_arn: str

    @classmethod
    def from_dict(cls, data: dict) -> GetDurableExecutionRequest:
        return cls(durable_execution_arn=data["DurableExecutionArn"])

    def to_dict(self) -> dict[str, Any]:
        return {"DurableExecutionArn": self.durable_execution_arn}


@dataclass(frozen=True)
class GetDurableExecutionResponse:
    """Response containing durable execution details."""

    durable_execution_arn: str
    durable_execution_name: str
    function_arn: str
    status: str
    start_date: str
    input_payload: str | None = None
    result: str | None = None
    error: ErrorObject | None = None
    stop_date: str | None = None
    version: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> GetDurableExecutionResponse:
        error = None
        if error_data := data.get("Error"):
            error = ErrorObject.from_dict(error_data)

        return cls(
            durable_execution_arn=data["DurableExecutionArn"],
            durable_execution_name=data["DurableExecutionName"],
            function_arn=data["FunctionArn"],
            status=data["Status"],
            start_date=data["StartDate"],
            input_payload=data.get("InputPayload"),
            result=data.get("Result"),
            error=error,
            stop_date=data.get("StopDate"),
            version=data.get("Version"),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "DurableExecutionArn": self.durable_execution_arn,
            "DurableExecutionName": self.durable_execution_name,
            "FunctionArn": self.function_arn,
            "Status": self.status,
            "StartDate": self.start_date,
        }
        if self.input_payload is not None:
            result["InputPayload"] = self.input_payload
        if self.result is not None:
            result["Result"] = self.result
        if self.error is not None:
            result["Error"] = self.error.to_dict()
        if self.stop_date is not None:
            result["StopDate"] = self.stop_date
        if self.version is not None:
            result["Version"] = self.version
        return result


@dataclass(frozen=True)
class Execution:
    """Execution summary structure from Smithy model."""

    durable_execution_arn: str
    durable_execution_name: str
    function_arn: str
    status: str
    start_date: str
    stop_date: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> Execution:
        return cls(
            durable_execution_arn=data["DurableExecutionArn"],
            durable_execution_name=data["DurableExecutionName"],
            function_arn=data.get(
                "FunctionArn", ""
            ),  # Make optional for backward compatibility
            status=data["Status"],
            start_date=data["StartDate"],
            stop_date=data.get("StopDate"),
        )

    def to_dict(self) -> dict[str, Any]:
        result = {
            "DurableExecutionArn": self.durable_execution_arn,
            "DurableExecutionName": self.durable_execution_name,
            "Status": self.status,
            "StartDate": self.start_date,
        }
        if self.function_arn:  # Only include if not empty
            result["FunctionArn"] = self.function_arn
        if self.stop_date is not None:
            result["StopDate"] = self.stop_date
        return result


@dataclass(frozen=True)
class ListDurableExecutionsRequest:
    """Request to list durable executions."""

    function_name: str | None = None
    function_version: str | None = None
    durable_execution_name: str | None = None
    status_filter: list[str] | None = None
    time_after: str | None = None
    time_before: str | None = None
    marker: str | None = None
    max_items: int = 0
    reverse_order: bool | None = None

    @classmethod
    def from_dict(cls, data: dict) -> ListDurableExecutionsRequest:
        return cls(
            function_name=data.get("FunctionName"),
            function_version=data.get("FunctionVersion"),
            durable_execution_name=data.get("DurableExecutionName"),
            status_filter=data.get("StatusFilter"),
            time_after=data.get("TimeAfter"),
            time_before=data.get("TimeBefore"),
            marker=data.get("Marker"),
            max_items=data.get("MaxItems", 0),
            reverse_order=data.get("ReverseOrder"),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.function_name is not None:
            result["FunctionName"] = self.function_name
        if self.function_version is not None:
            result["FunctionVersion"] = self.function_version
        if self.durable_execution_name is not None:
            result["DurableExecutionName"] = self.durable_execution_name
        if self.status_filter is not None:
            result["StatusFilter"] = self.status_filter
        if self.time_after is not None:
            result["TimeAfter"] = self.time_after
        if self.time_before is not None:
            result["TimeBefore"] = self.time_before
        if self.marker is not None:
            result["Marker"] = self.marker
        if self.max_items is not None:
            result["MaxItems"] = self.max_items
        if self.reverse_order is not None:
            result["ReverseOrder"] = self.reverse_order
        return result


@dataclass(frozen=True)
class ListDurableExecutionsResponse:
    """Response containing list of durable executions."""

    durable_executions: list[Execution]
    next_marker: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> ListDurableExecutionsResponse:
        executions = [
            Execution.from_dict(exec_data)
            for exec_data in data.get("DurableExecutions", [])
        ]
        return cls(
            durable_executions=executions,
            next_marker=data.get("NextMarker"),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "DurableExecutions": [exe.to_dict() for exe in self.durable_executions]
        }
        if self.next_marker is not None:
            result["NextMarker"] = self.next_marker
        return result


@dataclass(frozen=True)
class StopDurableExecutionRequest:
    """Request to stop a durable execution."""

    durable_execution_arn: str
    error: ErrorObject | None = None

    @classmethod
    def from_dict(cls, data: dict) -> StopDurableExecutionRequest:
        error = None
        if error_data := data.get("Error"):
            error = ErrorObject.from_dict(error_data)

        return cls(
            durable_execution_arn=data["DurableExecutionArn"],
            error=error,
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"DurableExecutionArn": self.durable_execution_arn}
        if self.error is not None:
            result["Error"] = self.error.to_dict()
        return result


@dataclass(frozen=True)
class StopDurableExecutionResponse:
    """Response from stopping a durable execution."""

    stop_date: str

    @classmethod
    def from_dict(cls, data: dict) -> StopDurableExecutionResponse:
        return cls(stop_date=data["StopDate"])

    def to_dict(self) -> dict[str, Any]:
        return {"StopDate": self.stop_date}


@dataclass(frozen=True)
class GetDurableExecutionStateRequest:
    """Request to get durable execution state."""

    durable_execution_arn: str
    checkpoint_token: str
    marker: str | None = None
    max_items: int = 0

    @classmethod
    def from_dict(cls, data: dict) -> GetDurableExecutionStateRequest:
        return cls(
            durable_execution_arn=data["DurableExecutionArn"],
            checkpoint_token=data["CheckpointToken"],
            marker=data.get("Marker"),
            max_items=data.get("MaxItems", 0),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "DurableExecutionArn": self.durable_execution_arn,
            "CheckpointToken": self.checkpoint_token,
        }
        if self.marker is not None:
            result["Marker"] = self.marker
        if self.max_items is not None:
            result["MaxItems"] = self.max_items
        return result


@dataclass(frozen=True)
class GetDurableExecutionStateResponse:
    """Response containing durable execution state operations."""

    operations: list[Operation]
    next_marker: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> GetDurableExecutionStateResponse:
        operations = [
            Operation.from_dict(op_data) for op_data in data.get("Operations", [])
        ]
        return cls(
            operations=operations,
            next_marker=data.get("NextMarker"),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "Operations": [op.to_dict() for op in self.operations]
        }
        if self.next_marker is not None:
            result["NextMarker"] = self.next_marker
        return result


# Event-related structures from Smithy model
@dataclass(frozen=True)
class EventInput:
    """Event input structure."""

    payload: str | None = None
    truncated: bool = False

    @classmethod
    def from_dict(cls, data: dict) -> EventInput:
        return cls(
            payload=data.get("Payload"),
            truncated=data.get("Truncated", False),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"Truncated": self.truncated}
        if self.payload is not None:
            result["Payload"] = self.payload
        return result


@dataclass(frozen=True)
class EventResult:
    """Event result structure."""

    payload: str | None = None
    truncated: bool = False

    @classmethod
    def from_dict(cls, data: dict) -> EventResult:
        return cls(
            payload=data.get("Payload"),
            truncated=data.get("Truncated", False),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"Truncated": self.truncated}
        if self.payload is not None:
            result["Payload"] = self.payload
        return result


@dataclass(frozen=True)
class EventError:
    """Event error structure."""

    payload: ErrorObject | None = None
    truncated: bool = False

    @classmethod
    def from_dict(cls, data: dict) -> EventError:
        payload = None
        if payload_data := data.get("Payload"):
            payload = ErrorObject.from_dict(payload_data)

        return cls(
            payload=payload,
            truncated=data.get("Truncated", False),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"Truncated": self.truncated}
        if self.payload is not None:
            result["Payload"] = self.payload.to_dict()
        return result


@dataclass(frozen=True)
class RetryDetails:
    """Retry details structure."""

    current_attempt: int = 0
    next_attempt_delay_seconds: int | None = None

    @classmethod
    def from_dict(cls, data: dict) -> RetryDetails:
        return cls(
            current_attempt=data.get("CurrentAttempt", 0),
            next_attempt_delay_seconds=data.get("NextAttemptDelaySeconds"),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"CurrentAttempt": self.current_attempt}
        if self.next_attempt_delay_seconds is not None:
            result["NextAttemptDelaySeconds"] = self.next_attempt_delay_seconds
        return result


# Event detail structures
@dataclass(frozen=True)
class ExecutionStartedDetails:
    """Execution started event details."""

    input: EventInput | None = None
    execution_timeout: int | None = None

    @classmethod
    def from_dict(cls, data: dict) -> ExecutionStartedDetails:
        input_data = None
        if input_dict := data.get("Input"):
            input_data = EventInput.from_dict(input_dict)

        return cls(
            input=input_data,
            execution_timeout=data.get("ExecutionTimeout"),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.input is not None:
            result["Input"] = self.input.to_dict()
        if self.execution_timeout is not None:
            result["ExecutionTimeout"] = self.execution_timeout
        return result


@dataclass(frozen=True)
class ExecutionSucceededDetails:
    """Execution succeeded event details."""

    result: EventResult | None = None

    @classmethod
    def from_dict(cls, data: dict) -> ExecutionSucceededDetails:
        result_data = None
        if result_dict := data.get("Result"):
            result_data = EventResult.from_dict(result_dict)

        return cls(result=result_data)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.result is not None:
            result["Result"] = self.result.to_dict()
        return result


@dataclass(frozen=True)
class ExecutionFailedDetails:
    """Execution failed event details."""

    error: EventError | None = None

    @classmethod
    def from_dict(cls, data: dict) -> ExecutionFailedDetails:
        error_data = None
        if error_dict := data.get("Error"):
            error_data = EventError.from_dict(error_dict)

        return cls(error=error_data)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.error is not None:
            result["Error"] = self.error.to_dict()
        return result


@dataclass(frozen=True)
class ExecutionTimedOutDetails:
    """Execution timed out event details."""

    error: EventError | None = None

    @classmethod
    def from_dict(cls, data: dict) -> ExecutionTimedOutDetails:
        error_data = None
        if error_dict := data.get("Error"):
            error_data = EventError.from_dict(error_dict)

        return cls(error=error_data)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.error is not None:
            result["Error"] = self.error.to_dict()
        return result


@dataclass(frozen=True)
class ExecutionStoppedDetails:
    """Execution stopped event details."""

    error: EventError | None = None

    @classmethod
    def from_dict(cls, data: dict) -> ExecutionStoppedDetails:
        error_data = None
        if error_dict := data.get("Error"):
            error_data = EventError.from_dict(error_dict)

        return cls(error=error_data)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.error is not None:
            result["Error"] = self.error.to_dict()
        return result


@dataclass(frozen=True)
class ContextStartedDetails:
    """Context started event details."""

    @classmethod
    def from_dict(cls, data: dict) -> ContextStartedDetails:  # noqa: ARG003
        return cls()

    def to_dict(self) -> dict[str, Any]:
        return {}


@dataclass(frozen=True)
class ContextSucceededDetails:
    """Context succeeded event details."""

    result: EventResult | None = None

    @classmethod
    def from_dict(cls, data: dict) -> ContextSucceededDetails:
        result_data = None
        if result_dict := data.get("Result"):
            result_data = EventResult.from_dict(result_dict)

        return cls(result=result_data)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.result is not None:
            result["Result"] = self.result.to_dict()
        return result


@dataclass(frozen=True)
class ContextFailedDetails:
    """Context failed event details."""

    error: EventError | None = None

    @classmethod
    def from_dict(cls, data: dict) -> ContextFailedDetails:
        error_data = None
        if error_dict := data.get("Error"):
            error_data = EventError.from_dict(error_dict)

        return cls(error=error_data)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.error is not None:
            result["Error"] = self.error.to_dict()
        return result


@dataclass(frozen=True)
class WaitStartedDetails:
    """Wait started event details."""

    duration: int | None = None
    scheduled_end_timestamp: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> WaitStartedDetails:
        return cls(
            duration=data.get("Duration"),
            scheduled_end_timestamp=data.get("ScheduledEndTimestamp"),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.duration is not None:
            result["Duration"] = self.duration
        if self.scheduled_end_timestamp is not None:
            result["ScheduledEndTimestamp"] = self.scheduled_end_timestamp
        return result


@dataclass(frozen=True)
class WaitSucceededDetails:
    """Wait succeeded event details."""

    duration: int | None = None

    @classmethod
    def from_dict(cls, data: dict) -> WaitSucceededDetails:
        return cls(duration=data.get("Duration"))

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.duration is not None:
            result["Duration"] = self.duration
        return result


@dataclass(frozen=True)
class WaitCancelledDetails:
    """Wait cancelled event details."""

    error: EventError | None = None

    @classmethod
    def from_dict(cls, data: dict) -> WaitCancelledDetails:
        error_data = None
        if error_dict := data.get("Error"):
            error_data = EventError.from_dict(error_dict)

        return cls(error=error_data)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.error is not None:
            result["Error"] = self.error.to_dict()
        return result


@dataclass(frozen=True)
class StepStartedDetails:
    """Step started event details."""

    @classmethod
    def from_dict(cls, data: dict) -> StepStartedDetails:  # noqa: ARG003
        return cls()

    def to_dict(self) -> dict[str, Any]:
        return {}


@dataclass(frozen=True)
class StepSucceededDetails:
    """Step succeeded event details."""

    result: EventResult | None = None
    retry_details: RetryDetails | None = None

    @classmethod
    def from_dict(cls, data: dict) -> StepSucceededDetails:
        result_data = None
        if result_dict := data.get("Result"):
            result_data = EventResult.from_dict(result_dict)

        retry_details_data = None
        if retry_dict := data.get("RetryDetails"):
            retry_details_data = RetryDetails.from_dict(retry_dict)

        return cls(result=result_data, retry_details=retry_details_data)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.result is not None:
            result["Result"] = self.result.to_dict()
        if self.retry_details is not None:
            result["RetryDetails"] = self.retry_details.to_dict()
        return result


@dataclass(frozen=True)
class StepFailedDetails:
    """Step failed event details."""

    error: EventError | None = None
    retry_details: RetryDetails | None = None

    @classmethod
    def from_dict(cls, data: dict) -> StepFailedDetails:
        error_data = None
        if error_dict := data.get("Error"):
            error_data = EventError.from_dict(error_dict)

        retry_details_data = None
        if retry_dict := data.get("RetryDetails"):
            retry_details_data = RetryDetails.from_dict(retry_dict)

        return cls(error=error_data, retry_details=retry_details_data)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.error is not None:
            result["Error"] = self.error.to_dict()
        if self.retry_details is not None:
            result["RetryDetails"] = self.retry_details.to_dict()
        return result


@dataclass(frozen=True)
class InvokeStartedDetails:
    """Invoke started event details."""

    input: EventInput | None = None
    function_arn: str | None = None
    durable_execution_arn: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> InvokeStartedDetails:
        input_data = None
        if input_dict := data.get("Input"):
            input_data = EventInput.from_dict(input_dict)

        return cls(
            input=input_data,
            function_arn=data.get("FunctionArn"),
            durable_execution_arn=data.get("DurableExecutionArn"),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.input is not None:
            result["Input"] = self.input.to_dict()
        if self.function_arn is not None:
            result["FunctionArn"] = self.function_arn
        if self.durable_execution_arn is not None:
            result["DurableExecutionArn"] = self.durable_execution_arn
        return result


@dataclass(frozen=True)
class InvokeSucceededDetails:
    """Invoke succeeded event details."""

    result: EventResult | None = None

    @classmethod
    def from_dict(cls, data: dict) -> InvokeSucceededDetails:
        result_data = None
        if result_dict := data.get("Result"):
            result_data = EventResult.from_dict(result_dict)

        return cls(result=result_data)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.result is not None:
            result["Result"] = self.result.to_dict()
        return result


@dataclass(frozen=True)
class InvokeFailedDetails:
    """Invoke failed event details."""

    error: EventError | None = None

    @classmethod
    def from_dict(cls, data: dict) -> InvokeFailedDetails:
        error_data = None
        if error_dict := data.get("Error"):
            error_data = EventError.from_dict(error_dict)

        return cls(error=error_data)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.error is not None:
            result["Error"] = self.error.to_dict()
        return result


@dataclass(frozen=True)
class InvokeTimedOutDetails:
    """Invoke timed out event details."""

    error: EventError | None = None

    @classmethod
    def from_dict(cls, data: dict) -> InvokeTimedOutDetails:
        error_data = None
        if error_dict := data.get("Error"):
            error_data = EventError.from_dict(error_dict)

        return cls(error=error_data)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.error is not None:
            result["Error"] = self.error.to_dict()
        return result


@dataclass(frozen=True)
class InvokeStoppedDetails:
    """Invoke stopped event details."""

    error: EventError | None = None

    @classmethod
    def from_dict(cls, data: dict) -> InvokeStoppedDetails:
        error_data = None
        if error_dict := data.get("Error"):
            error_data = EventError.from_dict(error_dict)

        return cls(error=error_data)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.error is not None:
            result["Error"] = self.error.to_dict()
        return result


@dataclass(frozen=True)
class CallbackStartedDetails:
    """Callback started event details."""

    callback_id: str | None = None
    heartbeat_timeout: int | None = None
    timeout: int | None = None

    @classmethod
    def from_dict(cls, data: dict) -> CallbackStartedDetails:
        return cls(
            callback_id=data.get("CallbackId"),
            heartbeat_timeout=data.get("HeartbeatTimeout"),
            timeout=data.get("Timeout"),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.callback_id is not None:
            result["CallbackId"] = self.callback_id
        if self.heartbeat_timeout is not None:
            result["HeartbeatTimeout"] = self.heartbeat_timeout
        if self.timeout is not None:
            result["Timeout"] = self.timeout
        return result


@dataclass(frozen=True)
class CallbackSucceededDetails:
    """Callback succeeded event details."""

    result: EventResult | None = None

    @classmethod
    def from_dict(cls, data: dict) -> CallbackSucceededDetails:
        result_data = None
        if result_dict := data.get("Result"):
            result_data = EventResult.from_dict(result_dict)

        return cls(result=result_data)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.result is not None:
            result["Result"] = self.result.to_dict()
        return result


@dataclass(frozen=True)
class CallbackFailedDetails:
    """Callback failed event details."""

    error: EventError | None = None

    @classmethod
    def from_dict(cls, data: dict) -> CallbackFailedDetails:
        error_data = None
        if error_dict := data.get("Error"):
            error_data = EventError.from_dict(error_dict)

        return cls(error=error_data)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.error is not None:
            result["Error"] = self.error.to_dict()
        return result


@dataclass(frozen=True)
class CallbackTimedOutDetails:
    """Callback timed out event details."""

    error: EventError | None = None

    @classmethod
    def from_dict(cls, data: dict) -> CallbackTimedOutDetails:
        error_data = None
        if error_dict := data.get("Error"):
            error_data = EventError.from_dict(error_dict)

        return cls(error=error_data)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.error is not None:
            result["Error"] = self.error.to_dict()
        return result


@dataclass(frozen=True)
class Event:
    """Event structure from Smithy model."""

    event_type: str
    event_timestamp: str
    sub_type: str | None = None
    event_id: int = 1
    operation_id: str | None = None
    name: str | None = None
    parent_id: str | None = None
    execution_started_details: ExecutionStartedDetails | None = None
    execution_succeeded_details: ExecutionSucceededDetails | None = None
    execution_failed_details: ExecutionFailedDetails | None = None
    execution_timed_out_details: ExecutionTimedOutDetails | None = None
    execution_stopped_details: ExecutionStoppedDetails | None = None
    context_started_details: ContextStartedDetails | None = None
    context_succeeded_details: ContextSucceededDetails | None = None
    context_failed_details: ContextFailedDetails | None = None
    wait_started_details: WaitStartedDetails | None = None
    wait_succeeded_details: WaitSucceededDetails | None = None
    wait_cancelled_details: WaitCancelledDetails | None = None
    step_started_details: StepStartedDetails | None = None
    step_succeeded_details: StepSucceededDetails | None = None
    step_failed_details: StepFailedDetails | None = None
    invoke_started_details: InvokeStartedDetails | None = None
    invoke_succeeded_details: InvokeSucceededDetails | None = None
    invoke_failed_details: InvokeFailedDetails | None = None
    invoke_timed_out_details: InvokeTimedOutDetails | None = None
    invoke_stopped_details: InvokeStoppedDetails | None = None
    callback_started_details: CallbackStartedDetails | None = None
    callback_succeeded_details: CallbackSucceededDetails | None = None
    callback_failed_details: CallbackFailedDetails | None = None
    callback_timed_out_details: CallbackTimedOutDetails | None = None

    @classmethod
    def from_dict(cls, data: dict) -> Event:
        # Parse all the detail structures
        execution_started_details = None
        if details_data := data.get("ExecutionStartedDetails"):
            execution_started_details = ExecutionStartedDetails.from_dict(details_data)

        execution_succeeded_details = None
        if details_data := data.get("ExecutionSucceededDetails"):
            execution_succeeded_details = ExecutionSucceededDetails.from_dict(
                details_data
            )

        execution_failed_details = None
        if details_data := data.get("ExecutionFailedDetails"):
            execution_failed_details = ExecutionFailedDetails.from_dict(details_data)

        execution_timed_out_details = None
        if details_data := data.get("ExecutionTimedOutDetails"):
            execution_timed_out_details = ExecutionTimedOutDetails.from_dict(
                details_data
            )

        execution_stopped_details = None
        if details_data := data.get("ExecutionStoppedDetails"):
            execution_stopped_details = ExecutionStoppedDetails.from_dict(details_data)

        context_started_details = None
        if details_data := data.get("ContextStartedDetails"):
            context_started_details = ContextStartedDetails.from_dict(details_data)

        context_succeeded_details = None
        if details_data := data.get("ContextSucceededDetails"):
            context_succeeded_details = ContextSucceededDetails.from_dict(details_data)

        context_failed_details = None
        if details_data := data.get("ContextFailedDetails"):
            context_failed_details = ContextFailedDetails.from_dict(details_data)

        wait_started_details = None
        if details_data := data.get("WaitStartedDetails"):
            wait_started_details = WaitStartedDetails.from_dict(details_data)

        wait_succeeded_details = None
        if details_data := data.get("WaitSucceededDetails"):
            wait_succeeded_details = WaitSucceededDetails.from_dict(details_data)

        wait_cancelled_details = None
        if details_data := data.get("WaitCancelledDetails"):
            wait_cancelled_details = WaitCancelledDetails.from_dict(details_data)

        step_started_details = None
        if details_data := data.get("StepStartedDetails"):
            step_started_details = StepStartedDetails.from_dict(details_data)

        step_succeeded_details = None
        if details_data := data.get("StepSucceededDetails"):
            step_succeeded_details = StepSucceededDetails.from_dict(details_data)

        step_failed_details = None
        if details_data := data.get("StepFailedDetails"):
            step_failed_details = StepFailedDetails.from_dict(details_data)

        invoke_started_details = None
        if details_data := data.get("InvokeStartedDetails"):
            invoke_started_details = InvokeStartedDetails.from_dict(details_data)

        invoke_succeeded_details = None
        if details_data := data.get("InvokeSucceededDetails"):
            invoke_succeeded_details = InvokeSucceededDetails.from_dict(details_data)

        invoke_failed_details = None
        if details_data := data.get("InvokeFailedDetails"):
            invoke_failed_details = InvokeFailedDetails.from_dict(details_data)

        invoke_timed_out_details = None
        if details_data := data.get("InvokeTimedOutDetails"):
            invoke_timed_out_details = InvokeTimedOutDetails.from_dict(details_data)

        invoke_stopped_details = None
        if details_data := data.get("InvokeStoppedDetails"):
            invoke_stopped_details = InvokeStoppedDetails.from_dict(details_data)

        callback_started_details = None
        if details_data := data.get("CallbackStartedDetails"):
            callback_started_details = CallbackStartedDetails.from_dict(details_data)

        callback_succeeded_details = None
        if details_data := data.get("CallbackSucceededDetails"):
            callback_succeeded_details = CallbackSucceededDetails.from_dict(
                details_data
            )

        callback_failed_details = None
        if details_data := data.get("CallbackFailedDetails"):
            callback_failed_details = CallbackFailedDetails.from_dict(details_data)

        callback_timed_out_details = None
        if details_data := data.get("CallbackTimedOutDetails"):
            callback_timed_out_details = CallbackTimedOutDetails.from_dict(details_data)

        return cls(
            event_type=data["EventType"],
            event_timestamp=data["EventTimestamp"],
            sub_type=data.get("SubType"),
            event_id=data.get("EventId", 1),
            operation_id=data.get("Id"),
            name=data.get("Name"),
            parent_id=data.get("ParentId"),
            execution_started_details=execution_started_details,
            execution_succeeded_details=execution_succeeded_details,
            execution_failed_details=execution_failed_details,
            execution_timed_out_details=execution_timed_out_details,
            execution_stopped_details=execution_stopped_details,
            context_started_details=context_started_details,
            context_succeeded_details=context_succeeded_details,
            context_failed_details=context_failed_details,
            wait_started_details=wait_started_details,
            wait_succeeded_details=wait_succeeded_details,
            wait_cancelled_details=wait_cancelled_details,
            step_started_details=step_started_details,
            step_succeeded_details=step_succeeded_details,
            step_failed_details=step_failed_details,
            invoke_started_details=invoke_started_details,
            invoke_succeeded_details=invoke_succeeded_details,
            invoke_failed_details=invoke_failed_details,
            invoke_timed_out_details=invoke_timed_out_details,
            invoke_stopped_details=invoke_stopped_details,
            callback_started_details=callback_started_details,
            callback_succeeded_details=callback_succeeded_details,
            callback_failed_details=callback_failed_details,
            callback_timed_out_details=callback_timed_out_details,
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "EventType": self.event_type,
            "EventTimestamp": self.event_timestamp,
            "EventId": self.event_id,
        }
        if self.sub_type is not None:
            result["SubType"] = self.sub_type
        if self.operation_id is not None:
            result["Id"] = self.operation_id
        if self.name is not None:
            result["Name"] = self.name
        if self.parent_id is not None:
            result["ParentId"] = self.parent_id
        if self.execution_started_details is not None:
            result["ExecutionStartedDetails"] = self.execution_started_details.to_dict()
        if self.execution_succeeded_details is not None:
            result["ExecutionSucceededDetails"] = (
                self.execution_succeeded_details.to_dict()
            )
        if self.execution_failed_details is not None:
            result["ExecutionFailedDetails"] = self.execution_failed_details.to_dict()
        if self.execution_timed_out_details is not None:
            result["ExecutionTimedOutDetails"] = (
                self.execution_timed_out_details.to_dict()
            )
        if self.execution_stopped_details is not None:
            result["ExecutionStoppedDetails"] = self.execution_stopped_details.to_dict()
        if self.context_started_details is not None:
            result["ContextStartedDetails"] = self.context_started_details.to_dict()
        if self.context_succeeded_details is not None:
            result["ContextSucceededDetails"] = self.context_succeeded_details.to_dict()
        if self.context_failed_details is not None:
            result["ContextFailedDetails"] = self.context_failed_details.to_dict()
        if self.wait_started_details is not None:
            result["WaitStartedDetails"] = self.wait_started_details.to_dict()
        if self.wait_succeeded_details is not None:
            result["WaitSucceededDetails"] = self.wait_succeeded_details.to_dict()
        if self.wait_cancelled_details is not None:
            result["WaitCancelledDetails"] = self.wait_cancelled_details.to_dict()
        if self.step_started_details is not None:
            result["StepStartedDetails"] = self.step_started_details.to_dict()
        if self.step_succeeded_details is not None:
            result["StepSucceededDetails"] = self.step_succeeded_details.to_dict()
        if self.step_failed_details is not None:
            result["StepFailedDetails"] = self.step_failed_details.to_dict()
        if self.invoke_started_details is not None:
            result["InvokeStartedDetails"] = self.invoke_started_details.to_dict()
        if self.invoke_succeeded_details is not None:
            result["InvokeSucceededDetails"] = self.invoke_succeeded_details.to_dict()
        if self.invoke_failed_details is not None:
            result["InvokeFailedDetails"] = self.invoke_failed_details.to_dict()
        if self.invoke_timed_out_details is not None:
            result["InvokeTimedOutDetails"] = self.invoke_timed_out_details.to_dict()
        if self.invoke_stopped_details is not None:
            result["InvokeStoppedDetails"] = self.invoke_stopped_details.to_dict()
        if self.callback_started_details is not None:
            result["CallbackStartedDetails"] = self.callback_started_details.to_dict()
        if self.callback_succeeded_details is not None:
            result["CallbackSucceededDetails"] = (
                self.callback_succeeded_details.to_dict()
            )
        if self.callback_failed_details is not None:
            result["CallbackFailedDetails"] = self.callback_failed_details.to_dict()
        if self.callback_timed_out_details is not None:
            result["CallbackTimedOutDetails"] = (
                self.callback_timed_out_details.to_dict()
            )
        return result


@dataclass(frozen=True)
class GetDurableExecutionHistoryRequest:
    """Request to get durable execution history."""

    durable_execution_arn: str
    include_execution_data: bool | None = None
    reverse_order: bool | None = None
    marker: str | None = None
    max_items: int = 0

    @classmethod
    def from_dict(cls, data: dict) -> GetDurableExecutionHistoryRequest:
        return cls(
            durable_execution_arn=data["DurableExecutionArn"],
            include_execution_data=data.get("IncludeExecutionData"),
            reverse_order=data.get("ReverseOrder"),
            marker=data.get("Marker"),
            max_items=data.get("MaxItems", 0),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"DurableExecutionArn": self.durable_execution_arn}
        if self.include_execution_data is not None:
            result["IncludeExecutionData"] = self.include_execution_data
        if self.reverse_order is not None:
            result["ReverseOrder"] = self.reverse_order
        if self.marker is not None:
            result["Marker"] = self.marker
        if self.max_items is not None:
            result["MaxItems"] = self.max_items
        return result


@dataclass(frozen=True)
class GetDurableExecutionHistoryResponse:
    """Response containing durable execution history events."""

    events: list[Event]
    next_marker: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> GetDurableExecutionHistoryResponse:
        events = [Event.from_dict(event_data) for event_data in data.get("Events", [])]
        return cls(
            events=events,
            next_marker=data.get("NextMarker"),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"Events": [event.to_dict() for event in self.events]}
        if self.next_marker is not None:
            result["NextMarker"] = self.next_marker
        return result


@dataclass(frozen=True)
class ListDurableExecutionsByFunctionRequest:
    """Request to list durable executions by function."""

    function_name: str
    qualifier: str | None = None
    durable_execution_name: str | None = None
    status_filter: list[str] | None = None
    time_after: str | None = None
    time_before: str | None = None
    marker: str | None = None
    max_items: int = 0
    reverse_order: bool | None = None

    @classmethod
    def from_dict(cls, data: dict) -> ListDurableExecutionsByFunctionRequest:
        return cls(
            function_name=data["FunctionName"],
            qualifier=data.get("Qualifier"),
            durable_execution_name=data.get("DurableExecutionName"),
            status_filter=data.get("StatusFilter"),
            time_after=data.get("TimeAfter"),
            time_before=data.get("TimeBefore"),
            marker=data.get("Marker"),
            max_items=data.get("MaxItems", 0),
            reverse_order=data.get("ReverseOrder"),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"FunctionName": self.function_name}
        if self.qualifier is not None:
            result["Qualifier"] = self.qualifier
        if self.durable_execution_name is not None:
            result["DurableExecutionName"] = self.durable_execution_name
        if self.status_filter is not None:
            result["StatusFilter"] = self.status_filter
        if self.time_after is not None:
            result["TimeAfter"] = self.time_after
        if self.time_before is not None:
            result["TimeBefore"] = self.time_before
        if self.marker is not None:
            result["Marker"] = self.marker
        if self.max_items is not None:
            result["MaxItems"] = self.max_items
        if self.reverse_order is not None:
            result["ReverseOrder"] = self.reverse_order
        return result


@dataclass(frozen=True)
class ListDurableExecutionsByFunctionResponse:
    """Response containing list of durable executions by function."""

    durable_executions: list[Execution]
    next_marker: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> ListDurableExecutionsByFunctionResponse:
        executions = [
            Execution.from_dict(exec_data)
            for exec_data in data.get("DurableExecutions", [])
        ]
        return cls(
            durable_executions=executions,
            next_marker=data.get("NextMarker"),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "DurableExecutions": [exe.to_dict() for exe in self.durable_executions]
        }
        if self.next_marker is not None:
            result["NextMarker"] = self.next_marker
        return result


# Callback-related models
@dataclass(frozen=True)
class SendDurableExecutionCallbackSuccessRequest:
    """Request to send callback success."""

    callback_id: str
    result: bytes | None = None

    @classmethod
    def from_dict(cls, data: dict) -> SendDurableExecutionCallbackSuccessRequest:
        return cls(
            callback_id=data["CallbackId"],
            result=data.get("Result"),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"CallbackId": self.callback_id}
        if self.result is not None:
            result["Result"] = self.result
        return result


@dataclass(frozen=True)
class SendDurableExecutionCallbackSuccessResponse:
    """Response from sending callback success."""


@dataclass(frozen=True)
class SendDurableExecutionCallbackFailureRequest:
    """Request to send callback failure."""

    callback_id: str
    error: ErrorObject | None = None

    @classmethod
    def from_dict(cls, data: dict) -> SendDurableExecutionCallbackFailureRequest:
        error = None
        if error_data := data.get("Error"):
            error = ErrorObject.from_dict(error_data)

        return cls(
            callback_id=data["CallbackId"],
            error=error,
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"CallbackId": self.callback_id}
        if self.error is not None:
            result["Error"] = self.error.to_dict()
        return result


@dataclass(frozen=True)
class SendDurableExecutionCallbackFailureResponse:
    """Response from sending callback failure."""


@dataclass(frozen=True)
class SendDurableExecutionCallbackHeartbeatRequest:
    """Request to send callback heartbeat."""

    callback_id: str

    @classmethod
    def from_dict(cls, data: dict) -> SendDurableExecutionCallbackHeartbeatRequest:
        return cls(callback_id=data["CallbackId"])

    def to_dict(self) -> dict[str, Any]:
        return {"CallbackId": self.callback_id}


@dataclass(frozen=True)
class SendDurableExecutionCallbackHeartbeatResponse:
    """Response from sending callback heartbeat."""


# Checkpoint-related models
@dataclass(frozen=True)
class CheckpointUpdatedExecutionState:
    """Updated execution state from checkpoint."""

    operations: list[Operation]
    next_marker: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> CheckpointUpdatedExecutionState:
        operations = [
            Operation.from_dict(op_data) for op_data in data.get("Operations", [])
        ]
        return cls(
            operations=operations,
            next_marker=data.get("NextMarker"),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "Operations": [op.to_dict() for op in self.operations]
        }
        if self.next_marker is not None:
            result["NextMarker"] = self.next_marker
        return result


@dataclass(frozen=True)
class CheckpointDurableExecutionRequest:
    """Request to checkpoint a durable execution."""

    durable_execution_arn: str
    checkpoint_token: str
    updates: list[OperationUpdate] | None = None
    client_token: str | None = None

    @classmethod
    def from_dict(
        cls, data: dict, durable_execution_arn: str
    ) -> CheckpointDurableExecutionRequest:
        updates = None
        if updates_data := data.get("Updates"):
            updates = []
            for update_data in updates_data:
                # Map dictionary fields to OperationUpdate constructor parameters
                operation_update = OperationUpdate(
                    operation_id=update_data["Id"],
                    operation_type=OperationType(update_data["Type"]),
                    action=OperationAction(update_data["Action"]),
                    parent_id=update_data.get("ParentId"),
                    name=update_data.get("Name"),
                    sub_type=OperationSubType(update_data["SubType"])
                    if update_data.get("SubType")
                    else None,
                    payload=update_data.get("Payload"),
                    error=ErrorObject.from_dict(update_data["Error"])
                    if update_data.get("Error")
                    else None,
                    context_options=ContextOptions(**update_data["ContextOptions"])
                    if update_data.get("ContextOptions")
                    else None,
                    step_options=StepOptions(**update_data["StepOptions"])
                    if update_data.get("StepOptions")
                    else None,
                    wait_options=WaitOptions(**update_data["WaitOptions"])
                    if update_data.get("WaitOptions")
                    else None,
                    callback_options=CallbackOptions(**update_data["CallbackOptions"])
                    if update_data.get("CallbackOptions")
                    else None,
                    invoke_options=InvokeOptions(**update_data["InvokeOptions"])
                    if update_data.get("InvokeOptions")
                    else None,
                )
                updates.append(operation_update)

        return cls(
            durable_execution_arn=durable_execution_arn,
            checkpoint_token=data["CheckpointToken"],
            updates=updates,
            client_token=data.get("ClientToken"),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "DurableExecutionArn": self.durable_execution_arn,
            "CheckpointToken": self.checkpoint_token,
        }
        if self.updates is not None:
            result["Updates"] = [update.to_dict() for update in self.updates]
        if self.client_token is not None:
            result["ClientToken"] = self.client_token
        return result


@dataclass(frozen=True)
class CheckpointDurableExecutionResponse:
    """Response from checkpointing a durable execution."""

    checkpoint_token: str
    new_execution_state: CheckpointUpdatedExecutionState | None = None

    @classmethod
    def from_dict(cls, data: dict) -> CheckpointDurableExecutionResponse:
        new_execution_state = None
        if state_data := data.get("NewExecutionState"):
            new_execution_state = CheckpointUpdatedExecutionState.from_dict(state_data)

        return cls(
            checkpoint_token=data["CheckpointToken"],
            new_execution_state=new_execution_state,
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"CheckpointToken": self.checkpoint_token}
        if self.new_execution_state is not None:
            result["NewExecutionState"] = self.new_execution_state.to_dict()
        return result


# Error response structure for consistent error handling
@dataclass(frozen=True)
class ErrorResponse:
    """Structured error response for web service operations."""

    error_type: str
    error_message: str
    error_code: str | None = None
    request_id: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> ErrorResponse:
        """Create ErrorResponse from dictionary.

        Args:
            data: Dictionary containing error data

        Returns:
            ErrorResponse: The error response object
        """
        error_data = data.get("error", data)  # Support both nested and flat structures
        return cls(
            error_type=error_data["type"],
            error_message=error_data["message"],
            error_code=error_data.get("code"),
            request_id=error_data.get("requestId"),
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert ErrorResponse to dictionary.

        Returns:
            dict: Dictionary representation of the error response
        """
        error_data: dict[str, Any] = {
            "type": self.error_type,
            "message": self.error_message,
        }

        if self.error_code is not None:
            error_data["code"] = self.error_code
        if self.request_id is not None:
            error_data["requestId"] = self.request_id

        return {"error": error_data}
