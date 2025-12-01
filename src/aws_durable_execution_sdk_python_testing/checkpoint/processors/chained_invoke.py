"""ChainedInvoke operation processor for handling CHAINED_INVOKE operation updates."""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from aws_durable_execution_sdk_python.lambda_service import (
    ChainedInvokeDetails,
    Operation,
    OperationAction,
    OperationStatus,
    OperationType,
    OperationUpdate,
)

from aws_durable_execution_sdk_python_testing.checkpoint.processors.base import (
    OperationProcessor,
)
from aws_durable_execution_sdk_python_testing.exceptions import (
    InvalidParameterValueException,
)

if TYPE_CHECKING:
    from aws_durable_execution_sdk_python_testing.observer import ExecutionNotifier


class ChainedInvokeProcessor(OperationProcessor):
    """Processes CHAINED_INVOKE operation updates."""

    def process(
        self,
        update: OperationUpdate,
        current_op: Operation | None,
        notifier: ExecutionNotifier,
        execution_arn: str,
    ) -> Operation:
        """Process CHAINED_INVOKE operation update."""
        match update.action:
            case OperationAction.START:
                return self._process_start(update, current_op, notifier, execution_arn)
            case OperationAction.SUCCEED:
                return self._process_succeed(update, current_op)
            case OperationAction.FAIL:
                return self._process_fail(update, current_op)
            case _:
                msg: str = f"Invalid action for CHAINED_INVOKE: {update.action}"
                raise InvalidParameterValueException(msg)

    def _process_start(
        self,
        update: OperationUpdate,
        current_op: Operation | None,
        notifier: ExecutionNotifier,
        execution_arn: str,
    ) -> Operation:
        """Process START action - create Operation with status PENDING and notify observers."""
        # Extract function_name and payload from chained_invoke_options
        function_name: str | None = None
        payload: str | None = update.payload

        if update.chained_invoke_options:
            function_name = update.chained_invoke_options.function_name

        # Create ChainedInvokeDetails
        chained_invoke_details = ChainedInvokeDetails(
            result=None,
            error=None,
        )

        start_time: datetime.datetime | None = self._get_start_time(current_op)

        operation = Operation(
            operation_id=update.operation_id,
            parent_id=update.parent_id,
            name=update.name,
            start_timestamp=start_time,
            end_timestamp=None,
            operation_type=OperationType.CHAINED_INVOKE,
            status=OperationStatus.PENDING,
            sub_type=update.sub_type,
            chained_invoke_details=chained_invoke_details,
        )

        # Notify observers about chained invoke start
        notifier.notify_chained_invoke_started(
            execution_arn=execution_arn,
            operation_id=update.operation_id,
            function_name=function_name or "",
            payload=payload,
        )

        return operation

    def _process_succeed(
        self,
        update: OperationUpdate,
        current_op: Operation | None,
    ) -> Operation:
        """Process SUCCEED action - update Operation status to SUCCEEDED and store result."""
        # Create ChainedInvokeDetails with result
        chained_invoke_details = ChainedInvokeDetails(
            result=update.payload,
            error=None,
        )

        start_time: datetime.datetime | None = self._get_start_time(current_op)
        end_time: datetime.datetime = datetime.datetime.now(tz=datetime.UTC)

        return Operation(
            operation_id=update.operation_id,
            parent_id=update.parent_id
            if update.parent_id
            else (current_op.parent_id if current_op else None),
            name=update.name
            if update.name
            else (current_op.name if current_op else None),
            start_timestamp=start_time,
            end_timestamp=end_time,
            operation_type=OperationType.CHAINED_INVOKE,
            status=OperationStatus.SUCCEEDED,
            sub_type=update.sub_type
            if update.sub_type
            else (current_op.sub_type if current_op else None),
            chained_invoke_details=chained_invoke_details,
        )

    def _process_fail(
        self,
        update: OperationUpdate,
        current_op: Operation | None,
    ) -> Operation:
        """Process FAIL action - update Operation status to FAILED and store error."""
        # Create ChainedInvokeDetails with error
        chained_invoke_details = ChainedInvokeDetails(
            result=None,
            error=update.error,
        )

        start_time: datetime.datetime | None = self._get_start_time(current_op)
        end_time: datetime.datetime = datetime.datetime.now(tz=datetime.UTC)

        return Operation(
            operation_id=update.operation_id,
            parent_id=update.parent_id
            if update.parent_id
            else (current_op.parent_id if current_op else None),
            name=update.name
            if update.name
            else (current_op.name if current_op else None),
            start_timestamp=start_time,
            end_timestamp=end_time,
            operation_type=OperationType.CHAINED_INVOKE,
            status=OperationStatus.FAILED,
            sub_type=update.sub_type
            if update.sub_type
            else (current_op.sub_type if current_op else None),
            chained_invoke_details=chained_invoke_details,
        )
