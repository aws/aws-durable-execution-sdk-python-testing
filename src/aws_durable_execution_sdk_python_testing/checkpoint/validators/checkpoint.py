"""Main checkpoint input validator."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from aws_durable_execution_sdk_python.lambda_service import (
    OperationType,
    OperationUpdate,
)

from aws_durable_execution_sdk_python_testing.checkpoint.validators.operations.callback import (
    CallbackOperationValidator,
)
from aws_durable_execution_sdk_python_testing.checkpoint.validators.operations.context import (
    ContextOperationValidator,
)
from aws_durable_execution_sdk_python_testing.checkpoint.validators.operations.execution import (
    ExecutionOperationValidator,
)
from aws_durable_execution_sdk_python_testing.checkpoint.validators.operations.invoke import (
    InvokeOperationValidator,
)
from aws_durable_execution_sdk_python_testing.checkpoint.validators.operations.step import (
    StepOperationValidator,
)
from aws_durable_execution_sdk_python_testing.checkpoint.validators.operations.wait import (
    WaitOperationValidator,
)
from aws_durable_execution_sdk_python_testing.checkpoint.validators.transitions import (
    ValidActionsByOperationTypeValidator,
)
from aws_durable_execution_sdk_python_testing.exceptions import InvalidParameterError

if TYPE_CHECKING:
    from collections.abc import MutableMapping

    from aws_durable_execution_sdk_python_testing.execution import Execution

MAX_ERROR_PAYLOAD_SIZE_BYTES = 32768


class CheckpointValidator:
    """Validates checkpoint input based on current state."""

    @staticmethod
    def validate_input(updates: list[OperationUpdate], execution: Execution) -> None:
        """Perform validation on the given input based on the current state."""
        if not updates:
            return

        CheckpointValidator._validate_conflicting_execution_update(updates)
        CheckpointValidator._validate_parent_id_and_duplicate_id(updates, execution)

        for update in updates:
            CheckpointValidator._validate_operation_update(update, execution)

    @staticmethod
    def _validate_conflicting_execution_update(updates: list[OperationUpdate]) -> None:
        """Validate that there are no conflicting execution updates."""
        execution_updates = [
            update
            for update in updates
            if update.operation_type == OperationType.EXECUTION
        ]

        if len(execution_updates) > 1:
            msg_multiple_exec: str = "Cannot checkpoint multiple EXECUTION updates."

            raise InvalidParameterError(msg_multiple_exec)

        if execution_updates and updates[-1].operation_type != OperationType.EXECUTION:
            msg_exec_last: str = "EXECUTION checkpoint must be the last update."

            raise InvalidParameterError(msg_exec_last)

    @staticmethod
    def _validate_operation_update(
        update: OperationUpdate, execution: Execution
    ) -> None:
        """Validate a single operation update."""
        CheckpointValidator._validate_payload_sizes(update)
        ValidActionsByOperationTypeValidator.validate(
            update.operation_type, update.action
        )
        CheckpointValidator._validate_operation_status_transition(update, execution)

    @staticmethod
    def _validate_payload_sizes(update: OperationUpdate) -> None:
        """Validate that operation payload sizes are not too large."""
        if update.error is not None:
            payload = json.dumps(update.error.to_dict())
            if len(payload) > MAX_ERROR_PAYLOAD_SIZE_BYTES:
                msg: str = f"Error object size must be less than {MAX_ERROR_PAYLOAD_SIZE_BYTES} bytes."
                raise InvalidParameterError(msg)

    @staticmethod
    def _validate_operation_status_transition(
        update: OperationUpdate, execution: Execution
    ) -> None:
        """Validate that the operation status transition is valid."""
        current_state = None
        for operation in execution.operations:
            if operation.operation_id == update.operation_id:
                current_state = operation
                break

        match update.operation_type:
            case OperationType.STEP:
                StepOperationValidator.validate(current_state, update)
            case OperationType.CONTEXT:
                ContextOperationValidator.validate(current_state, update)
            case OperationType.WAIT:
                WaitOperationValidator.validate(current_state, update)
            case OperationType.CALLBACK:
                CallbackOperationValidator.validate(current_state, update)
            case OperationType.INVOKE:
                InvokeOperationValidator.validate(current_state, update)
            case OperationType.EXECUTION:
                ExecutionOperationValidator.validate(update)
            case _:  # pragma: no cover
                msg: str = "Invalid operation type."

                raise InvalidParameterError(msg)

    @staticmethod
    def _validate_parent_id_and_duplicate_id(
        updates: list[OperationUpdate], execution: Execution
    ) -> None:
        """Validate parent IDs and check for duplicate operation IDs."""
        operations_seen: MutableMapping[str, OperationUpdate] = {}

        for update in updates:
            if update.operation_id in operations_seen:
                msg: str = "Cannot update the same operation twice in a single request."
                raise InvalidParameterError(msg)

            if not CheckpointValidator._is_valid_parent_for_update(
                execution, update, operations_seen
            ):
                msg_invalid_parent: str = "Invalid parent operation id."

                raise InvalidParameterError(msg_invalid_parent)

            operations_seen[update.operation_id] = update

    @staticmethod
    def _is_valid_parent_for_update(
        execution: Execution,
        update: OperationUpdate,
        operations_seen: MutableMapping[str, OperationUpdate],
    ) -> bool:
        """Check if the parent ID is valid for the update."""
        parent_id = update.parent_id

        if parent_id is None:
            return True

        if parent_id in operations_seen:
            parent_update = operations_seen[parent_id]
            return parent_update.operation_type == OperationType.CONTEXT

        for operation in execution.operations:
            if operation.operation_id == parent_id:
                return operation.operation_type == OperationType.CONTEXT

        return False
