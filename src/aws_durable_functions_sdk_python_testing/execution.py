from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import uuid4

from aws_durable_functions_sdk_python.execution import (
    DurableExecutionInvocationOutput,
    InvocationStatus,
)
from aws_durable_functions_sdk_python.lambda_service import (
    ErrorObject,
    ExecutionDetails,
    Operation,
    OperationStatus,
    OperationType,
    OperationUpdate,
)

from aws_durable_functions_sdk_python_testing.exceptions import (
    IllegalStateError,
    InvalidParameterError,
)
from aws_durable_functions_sdk_python_testing.token import CheckpointToken

if TYPE_CHECKING:
    from aws_durable_functions_sdk_python_testing.model import (
        StartDurableExecutionInput,
    )


class Execution:
    """Execution state."""

    def __init__(
        self,
        durable_execution_arn: str,
        start_input: StartDurableExecutionInput,
        operations: list[Operation],
    ):
        self.durable_execution_arn: str = durable_execution_arn
        # operation is frozen, it won't mutate - no need to clone/deep-copy
        self.start_input: StartDurableExecutionInput = start_input
        self.operations: list[Operation] = operations
        self.updates: list[OperationUpdate] = []
        self.used_tokens: set[str] = set()
        # TODO: this will need to persist/rehydrate depending on inmemory vs sqllite store
        self.token_sequence: int = 0
        self.is_complete: bool = False
        self.result: DurableExecutionInvocationOutput | None
        self.consecutive_failed_invocation_attempts: int = 0

    @staticmethod
    def new(input: StartDurableExecutionInput) -> Execution:  # noqa: A002
        # make a nicer arn
        # Pattern: arn:(aws[a-zA-Z-]*)?:lambda:[a-z]{2}(-gov)?-[a-z]+-\d{1}:\d{12}:durable-execution:[a-zA-Z0-9-_\.]+:[a-zA-Z0-9-_\.]+:[a-zA-Z0-9-_\.]+
        # Example: arn:aws:lambda:us-east-1:123456789012:durable-execution:myDurableFunction:myDurableExecutionName:ce67da72-3701-4f83-9174-f4189d27b0a5
        return Execution(
            durable_execution_arn=str(uuid4()), start_input=input, operations=[]
        )

    def start(self) -> None:
        # not thread safe, prob should be
        if self.start_input.invocation_id is None:
            msg: str = "invocation_id is required"
            raise InvalidParameterError(msg)
        self.operations.append(
            Operation(
                operation_id=self.start_input.invocation_id,
                parent_id=None,
                name=self.start_input.execution_name,
                start_timestamp=datetime.now(UTC),
                operation_type=OperationType.EXECUTION,
                status=OperationStatus.STARTED,
                execution_details=ExecutionDetails(
                    input_payload=json.dumps(self.start_input.input)
                ),
            )
        )

    def get_operation_execution_started(self) -> Operation:
        if not self.operations:
            msg: str = "execution not started."

            raise ValueError(msg)

        return self.operations[0]

    def get_new_checkpoint_token(self) -> str:
        """Generate a new checkpoint token with incremented sequence"""
        # TODO: not thread safe and it should be
        self.token_sequence += 1
        new_token_sequence = self.token_sequence
        token = CheckpointToken(
            execution_arn=self.durable_execution_arn, token_sequence=new_token_sequence
        )
        token_str = token.to_str()
        self.used_tokens.add(token_str)
        return token_str

    def get_navigable_operations(self) -> list[Operation]:
        """Get list of operations, but exclude child operations where the parent has already completed."""
        return self.operations

    def get_assertable_operations(self) -> list[Operation]:
        """Get list of operations, but exclude the EXECUTION operations"""
        # TODO: this excludes EXECUTION at start, but can there be an EXECUTION at the end if there was a checkpoint with large payload?
        return self.operations[1:]

    def has_pending_operations(self, execution: Execution) -> bool:
        """True if execution has pending operations."""

        for operation in execution.operations:
            if (
                operation.operation_type == OperationType.STEP
                and operation.status == OperationStatus.PENDING
            ) or (
                operation.operation_type
                in [OperationType.WAIT, OperationType.CALLBACK, OperationType.INVOKE]
                and operation.status == OperationStatus.STARTED
            ):
                return True
        return False

    def complete_success(self, result: str | None) -> None:
        self.result = DurableExecutionInvocationOutput(
            status=InvocationStatus.SUCCEEDED, result=result
        )
        self.is_complete = True

    def complete_fail(self, error: ErrorObject) -> None:
        self.result = DurableExecutionInvocationOutput(
            status=InvocationStatus.FAILED, error=error
        )
        self.is_complete = True

    def _find_operation(self, operation_id: str) -> tuple[int, Operation]:
        """Find operation by ID, return index and operation."""
        for i, operation in enumerate(self.operations):
            if operation.operation_id == operation_id:
                return i, operation
        msg: str = f"Attempting to update state of an Operation [{operation_id}] that doesn't exist"
        raise IllegalStateError(msg)

    def complete_wait(self, operation_id: str) -> Operation:
        """Complete WAIT operation when timer fires."""
        index, operation = self._find_operation(operation_id)

        # Validate
        if operation.status != OperationStatus.STARTED:
            msg_wait_not_started: str = f"Attempting to transition a Wait Operation[{operation_id}] to SUCCEEDED when it's not STARTED"
            raise IllegalStateError(msg_wait_not_started)
        if operation.operation_type != OperationType.WAIT:
            msg_not_wait: str = (
                f"Expected WAIT operation, got {operation.operation_type}"
            )
            raise IllegalStateError(msg_not_wait)

        # TODO: make thread-safe. Increment sequence
        self.token_sequence += 1

        # Build and assign updated operation
        self.operations[index] = replace(
            operation,
            status=OperationStatus.SUCCEEDED,
            end_timestamp=datetime.now(UTC),
        )

        return self.operations[index]

    def complete_retry(self, operation_id: str) -> Operation:
        """Complete STEP retry when timer fires."""
        index, operation = self._find_operation(operation_id)

        # Validate
        if operation.status != OperationStatus.PENDING:
            msg_step_not_pending: str = f"Attempting to transition a Step Operation[{operation_id}] to READY when it's not PENDING"
            raise IllegalStateError(msg_step_not_pending)
        if operation.operation_type != OperationType.STEP:
            msg_not_step: str = (
                f"Expected STEP operation, got {operation.operation_type}"
            )
            raise IllegalStateError(msg_not_step)

        # TODO: make thread-safe. Increment sequence
        self.token_sequence += 1

        # Build updated step_details with cleared next_attempt_timestamp
        new_step_details = None
        if operation.step_details:
            new_step_details = replace(
                operation.step_details, next_attempt_timestamp=None
            )

        # Build updated operation
        updated_operation = replace(
            operation, status=OperationStatus.READY, step_details=new_step_details
        )

        # Assign
        self.operations[index] = updated_operation
        return updated_operation
